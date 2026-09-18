#!/usr/bin/env python3
"""Local fixtures and records for acquisition-engine evaluation.

This module never contacts an acquisition source. Its HTTP server binds only
to loopback and serves synthetic deterministic content.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import tempfile
import threading
import time
from dataclasses import asdict, dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import unquote
from urllib.request import Request, urlopen


GENERATOR_ALGORITHM = "blake2b-64-counter-v1"
GENERATOR_BLOCK_SIZE = 64
MANIFEST_VERSION = 1
REPORT_VERSION = 1
SCENARIO_STATES = frozenset({"pass", "fail", "blocked", "not run"})
REQUIRED_SCENARIOS = frozenset(f"E{number:02d}" for number in range(1, 15))


class EvaluationError(ValueError):
    """Report invalid evaluation input without changing evidence files."""


class DeterministicBytes:
    """Serve repeatable byte ranges without holding an entire fixture in memory."""

    def __init__(self, seed: str, length: int) -> None:
        if not seed:
            raise EvaluationError("fixture seed must not be empty")
        if length < 0:
            raise EvaluationError("fixture length must not be negative")
        self.seed = seed.encode("utf-8")
        self.length = length

    def _block(self, index: int) -> bytes:
        return hashlib.blake2b(
            self.seed + index.to_bytes(16, "big"), digest_size=GENERATOR_BLOCK_SIZE
        ).digest()

    def read(self, offset: int, length: int) -> bytes:
        """Return a bounded byte range from the generated representation."""
        if offset < 0 or length < 0 or offset + length > self.length:
            raise EvaluationError("requested fixture range is outside its representation")
        result = bytearray()
        position = offset
        remaining = length
        while remaining:
            block_index, block_offset = divmod(position, GENERATOR_BLOCK_SIZE)
            block = self._block(block_index)
            take = min(remaining, GENERATOR_BLOCK_SIZE - block_offset)
            result.extend(block[block_offset:block_offset + take])
            position += take
            remaining -= take
        return bytes(result)

    def sha256(self, chunk_size: int = 1024 * 1024) -> str:
        """Calculate a digest in a separate bounded-memory streaming pass."""
        if chunk_size <= 0:
            raise EvaluationError("digest chunk size must be positive")
        digest = hashlib.sha256()
        offset = 0
        while offset < self.length:
            size = min(chunk_size, self.length - offset)
            digest.update(self.read(offset, size))
            offset += size
        return digest.hexdigest()


@dataclass(frozen=True)
class Fixture:
    """One synthetic representation in an immutable fixture manifest."""

    name: str
    seed: str
    length: int
    sha256: str
    content_type: str = "application/octet-stream"
    etag: str | None = None

    @classmethod
    def create(cls, name: str, seed: str, length: int,
               content_type: str = "application/octet-stream") -> "Fixture":
        segments = name.split("/") if name else []
        if not segments or any(not part or part in {".", ".."} for part in segments):
            raise EvaluationError("fixture name must be a non-empty relative path "
                                  "with no empty, '.', or '..' segments")
        generator = DeterministicBytes(seed, length)
        digest = generator.sha256()
        return cls(name, seed, length, digest, content_type,
                   f'"fixture-{digest[:24]}"')

    @property
    def generator(self) -> DeterministicBytes:
        return DeterministicBytes(self.seed, self.length)


@dataclass(frozen=True)
class ResponseScript:
    """A deterministic response override for one fixture request."""

    status: int | None = None
    delay_seconds: float = 0.0
    short_body_bytes: int | None = None
    ignore_range: bool = False
    redirect_path: str | None = None
    terminate_after_bytes: int | None = None
    omit_validators: bool = False

    def validate(self) -> None:
        if self.status is not None and not 100 <= self.status <= 599:
            raise EvaluationError("script status must be an HTTP status code")
        if self.delay_seconds < 0:
            raise EvaluationError("script delay must not be negative")
        for value in (self.short_body_bytes, self.terminate_after_bytes):
            if value is not None and value < 0:
                raise EvaluationError("script body limit must not be negative")
        if self.redirect_path is not None and not self.redirect_path.startswith("/"):
            raise EvaluationError("script redirect path must be absolute")


@dataclass(frozen=True)
class ScenarioResult:
    """One sanitized scenario result in an evaluation report."""

    scenario_id: str
    outcome: str
    requirement: str
    detail: str
    event_log: str | None = None

    def validate(self) -> None:
        if self.outcome not in SCENARIO_STATES:
            raise EvaluationError("scenario outcome is invalid")
        if not self.scenario_id.startswith("E"):
            raise EvaluationError("scenario ID must start with E")
        if not self.requirement:
            raise EvaluationError("scenario requirement must not be empty")


def process_tree_pids(root_pid: int) -> list[int]:
    """Return every PID in the process tree rooted at root_pid, root included.

    Walks /proc's PPid links rather than direct children only: a supervisor's
    `torsocks -i aria2c` engine process may be a grandchild instead of a
    direct child, depending on whether torsocks exec-replaces itself.
    """
    parents: dict[int, int] = {}
    for entry in Path("/proc").iterdir():
        if not entry.name.isdecimal():
            continue
        try:
            status = (entry / "status").read_text(encoding="utf-8")
        except OSError:
            continue
        for line in status.splitlines():
            if line.startswith("PPid:"):
                parents[int(entry.name)] = int(line.split(":", 1)[1].strip())
                break
    tree = {root_pid}
    changed = True
    while changed:
        changed = False
        for pid, ppid in parents.items():
            if ppid in tree and pid not in tree:
                tree.add(pid)
                changed = True
    return sorted(tree)


def process_command(pid: int) -> str:
    """Return a PID's command line, or an empty string if it already exited."""
    try:
        raw = (Path("/proc") / str(pid) / "cmdline").read_bytes()
    except OSError:
        return ""
    return raw.decode("utf-8", "replace").replace("\0", " ").strip()


def process_rss_bytes(pid: int) -> int:
    """Return one process's resident set size in bytes, or 0 if it already exited."""
    try:
        status = (Path("/proc") / str(pid) / "status").read_text(encoding="utf-8")
    except OSError:
        return 0
    for line in status.splitlines():
        if line.startswith("VmRSS:"):
            return int(line.split()[1]) * 1024
    return 0


def find_engine_pid(controller_pid: int) -> int | None:
    """Return the aria2c process PID in the controller's tree, if it is running."""
    for pid in process_tree_pids(controller_pid):
        if pid != controller_pid and "aria2c" in process_command(pid):
            return pid
    return None


def wait_for_bytes_written(path: Path, threshold: int, timeout: float,
                           poll_interval: float = 0.05) -> bool:
    """Poll a file's size until it reaches threshold bytes; return whether it did.

    Used to trigger a reproducible kill point by observed bytes transferred,
    not wall-clock delay: the fixture server's event log has no entry for an
    in-progress transfer, so on-disk staging size is the only observable
    signal during the transfer.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if path.stat().st_size >= threshold:
                return True
        except FileNotFoundError:
            pass
        time.sleep(poll_interval)
    return False


class ResourceSampler:
    """Sample a process tree's summed RSS at a fixed interval, in a thread.

    The interval is documented here (not standardized elsewhere) per the
    parent specification's "document the interval" instruction: 1.0 second.
    """

    DEFAULT_INTERVAL_SECONDS = 1.0

    def __init__(self, root_pid: int, interval_seconds: float | None = None) -> None:
        self.root_pid = root_pid
        self.interval_seconds = interval_seconds or self.DEFAULT_INTERVAL_SECONDS
        self.samples: list[int] = []
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def _run(self) -> None:
        while not self._stop.is_set():
            total = sum(process_rss_bytes(pid) for pid in process_tree_pids(self.root_pid))
            if total:
                self.samples.append(total)
            self._stop.wait(self.interval_seconds)

    def start(self) -> "ResourceSampler":
        self._thread = threading.Thread(target=self._run, name="rss-sampler", daemon=True)
        self._thread.start()
        return self

    def stop(self) -> int:
        """Stop sampling and return the maximum summed RSS observed, in bytes."""
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)
        return max(self.samples, default=0)


def generate_synthetic_queue_rows(count: int, body_length: int = 32,
                                  seed_prefix: str = "e10") -> list[Fixture]:
    """Build `count` syntactic fixture rows without materializing any body.

    Each body is tiny; the fixture server synthesizes it on demand from the
    same deterministic generator smaller fixtures use. This scenario tests
    admission and resource bounds, not per-row transfer bandwidth, so a full
    transfer of every row is not the point (parent specification, "do not
    request every URL").
    """
    return [Fixture.create(f"{seed_prefix}-{index:07d}.bin", f"{seed_prefix}-{index}",
                           body_length)
           for index in range(count)]


def canonical_json(value: Any) -> bytes:
    """Return stable JSON bytes for hashes and durable evaluation records."""
    return (json.dumps(value, sort_keys=True, separators=(",", ":"),
                       ensure_ascii=True) + "\n").encode("utf-8")


def build_manifest(fixtures: list[Fixture]) -> dict[str, Any]:
    """Build a manifest that records the generator and all expected digests."""
    names = [fixture.name for fixture in fixtures]
    if len(names) != len(set(names)):
        raise EvaluationError("fixture names must be unique")
    return {
        "schema_version": MANIFEST_VERSION,
        "generator": {
            "algorithm": GENERATOR_ALGORITHM,
            "block_size": GENERATOR_BLOCK_SIZE,
        },
        "fixtures": [asdict(fixture) for fixture in sorted(fixtures,
                                                              key=lambda item: item.name)],
    }


def manifest_hash(manifest: dict[str, Any]) -> str:
    """Return the SHA-256 hash of canonical manifest bytes."""
    return hashlib.sha256(canonical_json(manifest)).hexdigest()


def write_json(path: Path, value: dict[str, Any]) -> None:
    """Write a small evaluation record atomically with restrictive permissions."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("x", encoding="utf-8") as handle:
            handle.write(canonical_json(value).decode("utf-8"))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        os.chmod(path, 0o600)
    finally:
        temporary.unlink(missing_ok=True)


def write_manifest(path: Path, fixtures: list[Fixture]) -> str:
    """Write an immutable fixture manifest and return its canonical hash."""
    if path.exists():
        raise EvaluationError("fixture manifest already exists")
    manifest = build_manifest(fixtures)
    digest = manifest_hash(manifest)
    write_json(path, {**manifest, "manifest_sha256": digest})
    return digest


class FixtureServer:
    """Serve deterministic local HTTP fixtures and record each request."""

    def __init__(self, fixtures: list[Fixture], event_log: Path,
                 scripts: dict[str, list[ResponseScript]] | None = None,
                 path_prefix: str = "fixtures") -> None:
        self.fixtures = {fixture.name: fixture for fixture in fixtures}
        if len(self.fixtures) != len(fixtures):
            raise EvaluationError("fixture names must be unique")
        if not path_prefix or path_prefix.startswith("/") or path_prefix.endswith("/"):
            raise EvaluationError("fixture path prefix must be a bare relative path")
        self.path_prefix = path_prefix
        self.event_log = event_log
        self.scripts = scripts or {}
        for fixture_scripts in self.scripts.values():
            for script in fixture_scripts:
                script.validate()
        self.events: list[dict[str, Any]] = []
        self.request_counts: dict[str, int] = {}
        self.active_connections = 0
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), self._handler())

    def _handler(self):
        owner = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, format: str, *args: object) -> None:
                return

            def do_GET(self) -> None:
                owner._serve(self)

        return Handler

    @property
    def base_url(self) -> str:
        host, port = self._server.server_address
        return f"http://{host}:{port}"

    def url(self, name: str) -> str:
        if name not in self.fixtures:
            raise EvaluationError("unknown fixture")
        return f"{self.base_url}/{self.path_prefix}/{name}"

    def _script(self, name: str, count: int) -> ResponseScript:
        values = self.scripts.get(name, [])
        return values[min(count - 1, len(values) - 1)] if values else ResponseScript()

    @staticmethod
    def _parse_range(value: str | None, length: int) -> tuple[int, int] | None:
        if not value or not value.startswith("bytes=") or "," in value:
            return None
        start_text, separator, end_text = value[6:].partition("-")
        if not separator or not start_text.isdecimal():
            return None
        start = int(start_text)
        end = length - 1 if not end_text else int(end_text) if end_text.isdecimal() else -1
        if start >= length or end < start:
            return (-1, -1)
        return start, min(end, length - 1)

    def _record(self, event: dict[str, Any]) -> None:
        with self._lock:
            self.events.append(event)
            write_json(self.event_log, {"schema_version": 1, "events": self.events})

    def _serve(self, handler: BaseHTTPRequestHandler) -> None:
        prefix = f"/{self.path_prefix}/"
        if not handler.path.startswith(prefix):
            handler.send_error(404)
            return
        # Decode each raw path segment on its own, after splitting on literal
        # "/", so a percent-encoded "/" cannot be mistaken for a separator.
        name = "/".join(unquote(part) for part in
                        handler.path.removeprefix(prefix).split("/"))
        fixture = self.fixtures.get(name)
        if fixture is None:
            handler.send_error(404)
            return
        with self._lock:
            self.request_counts[name] = self.request_counts.get(name, 0) + 1
            count = self.request_counts[name]
            self.active_connections += 1
            concurrent = self.active_connections
        script = self._script(name, count)
        requested_range = handler.headers.get("Range")
        status = script.status
        start, end = 0, fixture.length - 1
        range_value = self._parse_range(requested_range, fixture.length)
        if status is None and range_value == (-1, -1):
            status = 416
        elif status is None and range_value and not script.ignore_range:
            status = 206
            start, end = range_value
        elif status is None:
            status = 200
        sent = 0
        try:
            if script.delay_seconds:
                time.sleep(script.delay_seconds)
            if script.redirect_path is not None:
                handler.send_response(status if script.status else 302)
                handler.send_header("Location", script.redirect_path)
                handler.send_header("Content-Length", "0")
                handler.end_headers()
                return
            if status == 416:
                handler.send_response(status)
                handler.send_header("Content-Range", f"bytes */{fixture.length}")
                handler.send_header("Content-Length", "0")
                handler.end_headers()
                return
            body_length = max(0, end - start + 1)
            advertised_length = body_length
            if script.short_body_bytes is not None:
                body_length = min(body_length, script.short_body_bytes)
            handler.send_response(status)
            handler.send_header("Content-Type", fixture.content_type)
            handler.send_header("Content-Length", str(advertised_length))
            handler.send_header("Accept-Ranges", "bytes")
            if body_length < advertised_length:
                handler.send_header("Connection", "close")
            if status == 206:
                handler.send_header("Content-Range", f"bytes {start}-{end}/{fixture.length}")
            if not script.omit_validators:
                handler.send_header("ETag", fixture.etag or "")
                handler.send_header("Last-Modified", "Mon, 01 Jan 2024 00:00:00 GMT")
            handler.end_headers()
            position = start
            remaining = body_length
            while remaining:
                chunk = fixture.generator.read(position, min(64 * 1024, remaining))
                if script.terminate_after_bytes is not None:
                    available = script.terminate_after_bytes - sent
                    if available <= 0:
                        handler.close_connection = True
                        break
                    chunk = chunk[:available]
                handler.wfile.write(chunk)
                sent += len(chunk)
                position += len(chunk)
                remaining -= len(chunk)
            if body_length < advertised_length:
                handler.close_connection = True
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            self._record({
                "fixture": name,
                "range": requested_range,
                "status": status,
                "transmitted_bytes": sent,
                "simultaneous_connections": concurrent,
            })
            with self._lock:
                self.active_connections -= 1

    def start(self) -> "FixtureServer":
        self.event_log.parent.mkdir(parents=True, exist_ok=True)
        self._thread = threading.Thread(target=self._server.serve_forever,
                                        name="fixture-server", daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        if self._thread:
            self._thread.join(timeout=5)

    def __enter__(self) -> "FixtureServer":
        return self.start()

    def __exit__(self, exc_type: object, exc_value: object,
                 traceback: object) -> None:
        self.stop()


def build_report(candidate: str, command: list[str], manifest_sha256: str,
                 results: list[ScenarioResult], adapter_revision: str,
                 binary_version: str = "not run") -> dict[str, Any]:
    """Build a structured report without treating blocked work as a pass."""
    for result in results:
        result.validate()
    reported_scenarios = {result.scenario_id for result in results}
    return {
        "schema_version": REPORT_VERSION,
        "candidate": candidate,
        "command": command,
        "binary_version": binary_version,
        "adapter_revision": adapter_revision,
        "fixture_manifest_sha256": manifest_sha256,
        "operating_system": {
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
            "python": platform.python_version(),
        },
        "results": [asdict(result) for result in results],
        "selection_eligible": reported_scenarios == REQUIRED_SCENARIOS and all(
            result.outcome == "pass" for result in results
        ),
    }


def self_test(output: Path) -> int:
    """Run a small local HTTP fixture check without transfer engines or Tor."""
    fixture = Fixture.create("self-test.bin", "tod-dl-evaluation", 4096)
    manifest_digest = write_manifest(output / "fixture-manifest.json", [fixture])
    with FixtureServer([fixture], output / "fixture-events.json") as server:
        request = Request(server.url(fixture.name), headers={"Range": "bytes=128-383"})
        with urlopen(request, timeout=5) as response:
            received = response.read()
            if response.status != 206 or received != fixture.generator.read(128, 256):
                raise EvaluationError("fixture range self-test failed")
    result = ScenarioResult("E01", "pass", "fixture server range handling",
                            "local self-test passed", "fixture-events.json")
    write_json(output / "evaluation-report.json", build_report(
        "fixture-self-test", [], manifest_digest, [result], "local"
    ))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True,
                        help="empty local directory for generated evaluation records")
    parser.add_argument("--self-test", action="store_true",
                        help="run the loopback fixture-server self-test")
    args = parser.parse_args()
    if not args.self_test:
        parser.error("--self-test is required; engine scenarios need explicit adapters")
    if args.output.exists() and any(args.output.iterdir()):
        parser.error("--output must be empty")
    args.output.mkdir(parents=True, exist_ok=True)
    try:
        return self_test(args.output)
    except EvaluationError as exc:
        parser.error(str(exc))
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
