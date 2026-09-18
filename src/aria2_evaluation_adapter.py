#!/usr/bin/env python3
"""Controller-facing adapter that drives the real per-URL aria2 downloader
against the local evaluation fixture server (see acquisition_evaluation.py).

This module never contacts an acquisition source. It shells out to the real
`tod-dl.py` CLI, unmodified, against loopback fixture URLs only. It does not
reimplement transfer, resume, or finalization logic.

Every URL the downloader accepts must match `relative_path()` in tod-dl.py:
exactly `/<source>/data/<tail...>`. The fixture server's `path_prefix` is set
to `<source>/data` so generated fixture URLs satisfy that shape.

Tor routing note: `tod-dl.py` always wraps aria2c in `torsocks -i`. Real
torsocks refuses, by default, to proxy connections to loopback destinations
(verified empirically in this evaluation; see the sanitized report). For the
`local_fixture` scenarios below, the adapter points `TORSOCKS_CONF_FILE` at a
test-only config with `AllowOutboundLocalhost 1`, which makes torsocks
connect to the loopback fixture directly instead of through Tor. This is the
exemption the specification's "Tor and process model" section grants to a
scenario that explicitly declares `local_fixture`; it must never be read as
evidence of Tor routing. E14 (the Tor-admission guard) instead uses the
downloader's real, unmodified Tor control-port preflight and does not use
this bypass.
"""

from __future__ import annotations

import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
from acquisition_evaluation import Fixture, FixtureServer  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[1]
DOWNLOADER = REPO_ROOT / "src" / "tod-dl.py"
FIXTURE_SOURCE = "fixture-source"
FIXTURE_PREFIX = f"{FIXTURE_SOURCE}/data"

# Test-only torsocks override: bypass Tor for loopback destinations so the
# unmodified downloader can reach the local fixture server. Never used for
# E14, which must exercise the real Tor control-port preflight.
LOCAL_FIXTURE_TORSOCKS_CONF = """\
TorAddress 127.0.0.1
TorPort 9050
AllowOutboundLocalhost 1
"""


class AdapterError(RuntimeError):
    """Report an adapter-level failure without touching evidence files."""


def write_local_fixture_torsocks_conf(directory: Path) -> Path:
    path = directory / "torsocks-local-fixture.conf"
    path.write_text(LOCAL_FIXTURE_TORSOCKS_CONF, encoding="utf-8")
    return path


def fixture_server(fixtures: list[Fixture], event_log: Path,
                   scripts: dict | None = None) -> FixtureServer:
    """Build the fixture server, prefixed so URLs satisfy relative_path()."""
    return FixtureServer(fixtures, event_log, scripts=scripts, path_prefix=FIXTURE_PREFIX)


def write_queue_file(path: Path, urls: list[str]) -> Path:
    path.write_text("\n".join(urls) + "\n", encoding="utf-8")
    return path


def run_downloader(*, queue: Path, destination: Path, state: Path,
                    workers: int = 1, max_files: int = 0,
                    reserve_bytes: int = 0, time_limit: float = 0,
                    tor_control_address: str = "127.0.0.1:9051",
                    tor_control_cookie: Path = Path("/run/tor/control.authcookie"),
                    torsocks_conf: Path | None = None,
                    extra_args: list[str] | None = None,
                    timeout: float = 30) -> subprocess.CompletedProcess:
    """Invoke the real downloader CLI as a subprocess and return its result."""
    command = [
        sys.executable, str(DOWNLOADER),
        "--queue", str(queue),
        "--destination", str(destination),
        "--state", str(state),
        "--workers", str(workers),
        "--max-files", str(max_files),
        "--reserve-bytes", str(reserve_bytes),
        "--tor-control-address", tor_control_address,
        "--tor-control-cookie", str(tor_control_cookie),
        "--worker-stagger", "0.1",
        "--connect-timeout", "5",
        "--timeout", "5",
        "--socks-backoff", "1",
        "--tor-newnym-interval", "0",
    ]
    if time_limit:
        command += ["--time-limit", str(time_limit)]
    command += extra_args or []
    env = None
    if torsocks_conf is not None:
        import os
        env = dict(os.environ)
        env["TORSOCKS_CONF_FILE"] = str(torsocks_conf)
    try:
        return subprocess.run(command, capture_output=True, text=True,
                              timeout=timeout, cwd=str(REPO_ROOT), env=env)
    except subprocess.TimeoutExpired as exc:
        raise AdapterError(f"downloader did not exit within {timeout}s: {exc}") from exc


def read_downloads(state: Path) -> dict[str, dict[str, Any]]:
    """Return the durable per-URL rows the adapter needs to judge a scenario."""
    database = state / "manifest.sqlite"
    if not database.exists():
        return {}
    with sqlite3.connect(database) as db:
        db.row_factory = sqlite3.Row
        rows = db.execute(
            "SELECT url, relative_path, storage_path, staging_path, status, "
            "attempts, bytes, sha256, last_error, review_code FROM downloads"
        ).fetchall()
    return {row["url"]: dict(row) for row in rows}


def isolated_dirs(root: Path) -> tuple[Path, Path]:
    """Create fresh, isolated destination/state directories for one scenario."""
    destination = root / "destination"
    state = root / "state"
    destination.mkdir(parents=True)
    state.mkdir(parents=True)
    return destination, state


def clean(root: Path) -> None:
    if root.exists():
        shutil.rmtree(root)
