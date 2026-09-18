#!/usr/bin/env python3
"""Run the tractable E01-E14 subset against the current per-URL aria2 process
configuration and write a sanitized selection report.

See specs/SPEC-acquisition-tool-evaluation.md. This script drives the real,
unmodified `tod-dl.py` CLI through `aria2_evaluation_adapter.py` against a
local loopback fixture server only. It never contacts an acquisition source
and never runs a source pilot.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import signal
import sqlite3
import stat
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from acquisition_evaluation import (Fixture, ResourceSampler, ResponseScript,
                                    ScenarioResult, build_report,
                                    find_engine_pid, generate_synthetic_queue_rows,
                                    wait_for_bytes_written, write_json, write_manifest)
from aria2_evaluation_adapter import (FIXTURE_PREFIX, fixture_server, isolated_dirs,
                                      read_downloads, read_transitions, run_downloader,
                                      start_downloader,
                                      write_local_fixture_torsocks_conf,
                                      write_queue_file)
from inspection import inspection_request

TOR_CONTROL_ADDRESS = "127.0.0.1:9051"
TOR_CONTROL_COOKIE = Path("/run/tor/control.authcookie")


def scenario_root(base: Path, scenario_id: str) -> Path:
    root = base / scenario_id.lower()
    root.mkdir(parents=True)
    return root


def e01_small_and_empty(base: Path, torsocks_conf: Path) -> ScenarioResult:
    root = scenario_root(base, "E01")
    small = Fixture.create("small.bin", "e01-small", 4096)
    empty = Fixture.create("empty.bin", "e01-empty", 0)
    manifest_digest = write_manifest(root / "fixture-manifest.json", [small, empty])
    events = root / "fixture-events.json"
    destination, state = isolated_dirs(root)
    with fixture_server([small, empty], events) as server:
        queue = write_queue_file(root / "queue.txt", [server.url(small.name),
                                                       server.url(empty.name)])
        result = run_downloader(queue=queue, destination=destination, state=state,
                                reserve_bytes=0, tor_control_address=TOR_CONTROL_ADDRESS,
                                tor_control_cookie=TOR_CONTROL_COOKIE,
                                torsocks_conf=torsocks_conf, timeout=30)
    rows = read_downloads(state)
    detail = f"exit={result.returncode} stdout_tail={result.stdout.splitlines()[-3:]}"
    for fixture in (small, empty):
        row = next((r for r in rows.values() if r["relative_path"].endswith(fixture.name)), None)
        if row is None or row["status"] != "complete":
            return ScenarioResult("E01", "fail", "E01: exact bytes and expected hashes",
                                  f"{fixture.name} did not reach complete: {row} | {detail}",
                                  str(events))
        stored = destination / row["storage_path"]
        if stored.read_bytes() != fixture.generator.read(0, fixture.length):
            return ScenarioResult("E01", "fail", "E01: exact bytes and expected hashes",
                                  f"{fixture.name} stored bytes mismatch", str(events))
        if row["sha256"] != fixture.sha256:
            return ScenarioResult("E01", "fail", "E01: exact bytes and expected hashes",
                                  f"{fixture.name} recorded hash mismatch", str(events))
    return ScenarioResult("E01", "pass", "E01: exact bytes and expected hashes",
                          f"small and empty fixtures completed with matching hashes; {detail}",
                          str(events))


def e03_range_ignored(base: Path, torsocks_conf: Path) -> ScenarioResult:
    """Interrupt a partial transfer, then have the server ignore Range on retry.

    A single aria2 invocation runs with --max-tries=1 (one HTTP request per
    invocation); the adapter's engine attempt therefore ends after the first
    interrupted request, and the controller schedules a retry with its normal
    (>=60s) backoff. This scenario waits out that one real backoff interval
    rather than a multi-day soak, per the spec's engine-integration guidance.
    """
    root = scenario_root(base, "E03")
    body = Fixture.create("e03.bin", "e03-body", 300_000)
    write_manifest(root / "fixture-manifest.json", [body])
    events = root / "fixture-events.json"
    destination, state = isolated_dirs(root)
    scripts = {body.name: [
        ResponseScript(terminate_after_bytes=100_000),
        ResponseScript(ignore_range=True),
    ]}
    with fixture_server([body], events, scripts=scripts) as server:
        queue = write_queue_file(root / "queue.txt", [server.url(body.name)])
        result1 = run_downloader(queue=queue, destination=destination, state=state,
                                 reserve_bytes=0, tor_control_address=TOR_CONTROL_ADDRESS,
                                 tor_control_cookie=TOR_CONTROL_COOKIE,
                                 torsocks_conf=torsocks_conf, time_limit=5, timeout=20)
        time.sleep(65)
        result2 = run_downloader(queue=queue, destination=destination, state=state,
                                 reserve_bytes=0, tor_control_address=TOR_CONTROL_ADDRESS,
                                 tor_control_cookie=TOR_CONTROL_COOKIE,
                                 torsocks_conf=torsocks_conf, time_limit=15, timeout=30)
        result = result2
    rows = read_downloads(state)
    row = next(iter(rows.values()), None)
    detail = (f"run1_exit={result1.returncode} run2_exit={result2.returncode} row={row} "
             f"events={json.loads(events.read_text())['events']}")
    if row is None:
        return ScenarioResult("E03", "fail",
                              "E03: preserve old partial; never combine versions",
                              f"no download row recorded | {detail}", str(events))
    staging = Path(row["staging_path"]) if row["staging_path"] else None
    if row["status"] == "complete":
        stored = destination / row["storage_path"]
        if stored.read_bytes() == body.generator.read(0, body.length):
            return ScenarioResult("E03", "fail",
                                  "E03: preserve old partial; never combine versions",
                                  f"promotion succeeded despite an ignored-Range full "
                                  f"resend after a partial transfer; a naive engine could "
                                  f"have concatenated versions | {detail}", str(events))
        return ScenarioResult("E03", "fail",
                              "E03: preserve old partial; never combine versions",
                              f"promoted with mismatched bytes | {detail}", str(events))
    if row["status"] in {"retry_wait", "failed", "review_required"} and staging and staging.exists():
        return ScenarioResult("E03", "pass",
                              "E03: preserve old partial; never combine versions",
                              f"partial preserved, not promoted, after ignored-Range "
                              f"resend; status={row['status']} | {detail}", str(events))
    return ScenarioResult("E03", "fail", "E03: preserve old partial; never combine versions",
                          f"unexpected terminal state | {detail}", str(events))


def e04_outage_then_recovery(base: Path, torsocks_conf: Path) -> ScenarioResult:
    root = scenario_root(base, "E04")
    body = Fixture.create("e04.bin", "e04-body", 4096)
    write_manifest(root / "fixture-manifest.json", [body])
    events = root / "fixture-events.json"
    destination, state = isolated_dirs(root)
    scripts = {body.name: [ResponseScript(status=503)]}
    with fixture_server([body], events, scripts=scripts) as server:
        queue = write_queue_file(root / "queue.txt", [server.url(body.name)])
        result1 = run_downloader(queue=queue, destination=destination, state=state,
                                 reserve_bytes=0, tor_control_address=TOR_CONTROL_ADDRESS,
                                 tor_control_cookie=TOR_CONTROL_COOKIE,
                                 torsocks_conf=torsocks_conf, time_limit=5, timeout=20)
        rows_after_outage = read_downloads(state)
        row = next(iter(rows_after_outage.values()), None)
        if row is None or row["status"] not in {"retry_wait", "queued", "pending"}:
            return ScenarioResult("E04", "fail", "E04: apply outage policy; resume on recovery",
                                  f"503 did not produce a retryable state: {row}", str(events))
        server.scripts = {}
        time.sleep(65)
        result2 = run_downloader(queue=queue, destination=destination, state=state,
                                 reserve_bytes=0, tor_control_address=TOR_CONTROL_ADDRESS,
                                 tor_control_cookie=TOR_CONTROL_COOKIE,
                                 torsocks_conf=torsocks_conf, time_limit=15, timeout=30)
    rows_after_recovery = read_downloads(state)
    row = next(iter(rows_after_recovery.values()), None)
    detail = (f"run1_exit={result1.returncode} run2_exit={result2.returncode} "
             f"final_row={row}")
    if row and row["status"] == "complete":
        return ScenarioResult("E04", "pass", "E04: apply outage policy; resume on recovery",
                              f"503 backed off, then completed once the fixture recovered; "
                              f"{detail}", str(events))
    return ScenarioResult("E04", "fail", "E04: apply outage policy; resume on recovery",
                          f"did not complete after recovery | {detail}", str(events))


def e07_existing_final(base: Path, torsocks_conf: Path) -> ScenarioResult:
    root = scenario_root(base, "E07")
    body = Fixture.create("e07.bin", "e07-body", 2048)
    write_manifest(root / "fixture-manifest.json", [body])
    events = root / "fixture-events.json"
    destination, state = isolated_dirs(root)
    final_path = destination / FIXTURE_PREFIX.split("/")[0] / "data" / body.name
    final_path.parent.mkdir(parents=True, exist_ok=True)
    final_path.write_bytes(b"pre-existing evidence, must not change")
    original = final_path.read_bytes()
    with fixture_server([body], events) as server:
        queue = write_queue_file(root / "queue.txt", [server.url(body.name)])
        result = run_downloader(queue=queue, destination=destination, state=state,
                                reserve_bytes=0, tor_control_address=TOR_CONTROL_ADDRESS,
                                tor_control_cookie=TOR_CONTROL_COOKIE,
                                torsocks_conf=torsocks_conf, timeout=30)
    unchanged = final_path.read_bytes() == original
    rows = read_downloads(state)
    row = next(iter(rows.values()), None)
    detail = f"exit={result.returncode} row={row}"
    if not unchanged:
        return ScenarioResult("E07", "fail", "E07: no existing bytes change",
                              f"existing final file was modified | {detail}", str(events))
    if row and row["status"] == "existing_unverified":
        return ScenarioResult("E07", "pass", "E07: no existing bytes change; collision recorded",
                              f"existing final preserved and recorded existing_unverified; "
                              f"{detail}", str(events))
    return ScenarioResult("E07", "fail", "E07: no existing bytes change; collision recorded",
                          f"existing final preserved but collision not recorded as expected; "
                          f"{detail}", str(events))


def e08_checksum_mismatch_review(base: Path, torsocks_conf: Path) -> ScenarioResult:
    """Short body: a first EOF retries once (may be a resumable cut); only a

    second, no-growth EOF gives up to review_required
    (is_incomplete_body_failure()'s retry-once-then-review rule, src/tod-dl.py). A
    genuinely short body never grows on retry, so drive two attempts, forcing
    the second's retry due now instead of waiting out RETRY_DELAYS[0] (60s).
    """
    root = scenario_root(base, "E08")
    body = Fixture.create("e08.bin", "e08-body", 4096)
    write_manifest(root / "fixture-manifest.json", [body])
    events = root / "fixture-events.json"
    destination, state = isolated_dirs(root)
    scripts = {body.name: [ResponseScript(short_body_bytes=100)]}
    with fixture_server([body], events, scripts=scripts) as server:
        queue = write_queue_file(root / "queue.txt", [server.url(body.name)])
        result1 = run_downloader(queue=queue, destination=destination, state=state,
                                 reserve_bytes=0, tor_control_address=TOR_CONTROL_ADDRESS,
                                 tor_control_cookie=TOR_CONTROL_COOKIE,
                                 torsocks_conf=torsocks_conf, time_limit=8, timeout=25)
        with sqlite3.connect(state / "manifest.sqlite") as db:
            db.execute("UPDATE downloads SET next_retry_at=0")
            db.commit()
        result = run_downloader(queue=queue, destination=destination, state=state,
                                reserve_bytes=0, tor_control_address=TOR_CONTROL_ADDRESS,
                                tor_control_cookie=TOR_CONTROL_COOKIE,
                                torsocks_conf=torsocks_conf, time_limit=8, timeout=25)
    rows = read_downloads(state)
    row = next(iter(rows.values()), None)
    detail = (f"exit1={result1.returncode} exit2={result.returncode} row={row} note='only "
             f"the short-body sub-case was exercised this session, not the HTML-200-error "
             f"or post-hash-mismatch cases'")
    if row and row["status"] == "review_required":
        return ScenarioResult("E08", "pass", "E08: block automatic promotion; retain review candidate",
                              f"short body was not auto-promoted and was recorded as a "
                              f"review candidate | {detail}", str(events))
    if row and row["status"] == "complete":
        return ScenarioResult("E08", "fail", "E08: block automatic promotion; retain review candidate",
                              f"short body was promoted despite an incomplete transfer | "
                              f"{detail}", str(events))
    return ScenarioResult("E08", "fail", "E08: block automatic promotion; retain review candidate",
                          f"transfer was not promoted but also not classified as "
                          f"review_required (status={row['status'] if row else None}); an "
                          f"aria2-level short-read is treated as a retryable engine failure, "
                          f"not a review candidate, in the current adapter/controller | "
                          f"{detail}", str(events))


def e09_disk_exhaustion(base: Path, torsocks_conf: Path) -> ScenarioResult:
    root = scenario_root(base, "E09")
    body = Fixture.create("e09.bin", "e09-body", 4096)
    write_manifest(root / "fixture-manifest.json", [body])
    events = root / "fixture-events.json"
    destination, state = isolated_dirs(root)
    with fixture_server([body], events) as server:
        queue = write_queue_file(root / "queue.txt", [server.url(body.name)])
        free = os.statvfs(destination).f_frsize * os.statvfs(destination).f_bavail
        result = run_downloader(queue=queue, destination=destination, state=state,
                                reserve_bytes=free + 10 * 1024**3,
                                tor_control_address=TOR_CONTROL_ADDRESS,
                                tor_control_cookie=TOR_CONTROL_COOKIE,
                                torsocks_conf=torsocks_conf, time_limit=5, timeout=20)
        admitted = (json.loads(events.read_text())["events"] if events.exists() else [])
    rows = read_downloads(state)
    row = next(iter(rows.values()), None)
    detail = f"exit={result.returncode} row={row} fixture_requests={len(admitted)}"
    method = ("adapter-side --reserve-bytes set above actual free space, "
             "per SPEC-acquisition-tool-evaluation.md's controlled-quota option")
    if admitted:
        return ScenarioResult("E09", "fail", "E09: stop admission; report local storage failure",
                              f"admission was not stopped; fixture was contacted | "
                              f"method={method} | {detail}", str(events))
    if "free-space reserve reached" in result.stdout or (row and row["status"] not in
                                                          {"complete", "active"}):
        return ScenarioResult("E09", "pass", "E09: stop admission; report local storage failure",
                              f"admission stopped before any transfer request; "
                              f"method={method} | {detail}", str(events))
    return ScenarioResult("E09", "fail", "E09: stop admission; report local storage failure",
                          f"local storage failure not clearly reported | method={method} | "
                          f"{detail}", str(events))


def e12_encoded_names_and_duplicates(base: Path, torsocks_conf: Path) -> ScenarioResult:
    root = scenario_root(base, "E12")
    plain = Fixture.create("plain-name.bin", "e12-plain", 512)
    unicode_name = Fixture.create("café-résumé.bin", "e12-unicode", 512)
    manifest_digest = write_manifest(root / "fixture-manifest.json", [plain, unicode_name])
    events = root / "fixture-events.json"
    destination, state = isolated_dirs(root)
    with fixture_server([plain, unicode_name], events) as server:
        plain_url = server.url(plain.name)
        unicode_url = server.url(unicode_name.name)
        traversal_url = server.base_url + f"/{FIXTURE_PREFIX}/..%2f..%2fplain-name.bin"
        queue = write_queue_file(root / "queue.txt", [
            plain_url, plain_url, unicode_url, traversal_url,
        ])
        result = run_downloader(queue=queue, destination=destination, state=state,
                                reserve_bytes=0, tor_control_address=TOR_CONTROL_ADDRESS,
                                tor_control_cookie=TOR_CONTROL_COOKIE,
                                torsocks_conf=torsocks_conf, timeout=30)
    rows = read_downloads(state)
    detail = f"exit={result.returncode} rows={list(rows.keys())}"
    if len(rows) != 2:
        return ScenarioResult("E12", "fail",
                              "E12: stable mapping; explicit rejection; enforced source scope",
                              f"expected 2 distinct accepted URLs (duplicate and unsafe "
                              f"traversal path rejected before queueing), got {len(rows)} | "
                              f"{detail}", str(events))
    unicode_row = rows.get(unicode_url)
    if unicode_row is None or unicode_row["status"] != "complete":
        return ScenarioResult("E12", "fail",
                              "E12: stable mapping; explicit rejection; enforced source scope",
                              f"Unicode-named fixture did not complete | {detail}", str(events))
    rejected = [line for line in result.stdout.splitlines() if line.startswith("[queue-rejected]")]
    reported_duplicate = any(plain_url in line for line in rejected)
    reported_traversal = any(traversal_url in line for line in rejected)
    detail = f"{detail} rejected_lines={rejected}"
    if reported_duplicate and reported_traversal:
        return ScenarioResult("E12", "pass",
                              "E12: stable mapping; explicit rejection; enforced source scope",
                              f"duplicate URL and unsafe traversal path both rejected before "
                              f"queueing with an explicit report, Unicode name transferred "
                              f"correctly | {detail}", str(events))
    return ScenarioResult("E12", "fail",
                          "E12: stable mapping; explicit rejection; enforced source scope",
                          f"duplicate URL de-duplicated and unsafe traversal path rejected "
                          f"before queueing, and the Unicode name transferred correctly, but "
                          f"the explicit rejection report this requirement calls for is "
                          f"missing (duplicate reported={reported_duplicate}, traversal "
                          f"reported={reported_traversal}) | {detail}", str(events))


def e14_tor_admission_guard(base: Path, torsocks_conf: Path) -> ScenarioResult:
    root = scenario_root(base, "E14")
    body = Fixture.create("e14.bin", "e14-body", 4096)
    write_manifest(root / "fixture-manifest.json", [body])
    events = root / "fixture-events.json"
    destination, state = isolated_dirs(root)
    with fixture_server([body], events) as server:
        queue = write_queue_file(root / "queue.txt", [server.url(body.name)])
        # Deliberately wrong Tor control-port address: nothing listens on 9.
        result = run_downloader(queue=queue, destination=destination, state=state,
                                reserve_bytes=0, tor_control_address="127.0.0.1:9",
                                tor_control_cookie=TOR_CONTROL_COOKIE,
                                torsocks_conf=torsocks_conf, timeout=15)
        admitted = (json.loads(events.read_text())["events"] if events.exists() else [])
    detail = f"exit={result.returncode} stderr={result.stderr.strip()!r}"
    if admitted:
        return ScenarioResult("E14", "fail", "E14: refuse source traffic when Tor is absent",
                              f"fixture was contacted despite a broken Tor control-port "
                              f"address | {detail}", str(events))
    if result.returncode == 1 and "Tor isolation preflight failed" in result.stderr:
        return ScenarioResult("E14", "pass", "E14: refuse source traffic when Tor is absent",
                              f"downloader refused to start before any fixture request; "
                              f"{detail}", str(events))
    return ScenarioResult("E14", "fail", "E14: refuse source traffic when Tor is absent",
                          f"unexpected outcome | {detail}", str(events))


def e02_large_file_interrupted_resume(base: Path, torsocks_conf: Path) -> ScenarioResult:
    """Interrupt an 8 GiB streamed transfer at exactly 256 MiB, then resume.

    Runs once per candidate, not part of the repeated failure-boundary set,
    per the parent specification's large-file-fixture guidance. The fixture
    server never materializes the representation; it streams bytes from the
    deterministic generator at the requested offset, and the expected digest
    comes from a second, independent streaming pass over the same generator
    and seed (Fixture.create).

    """
    root = scenario_root(base, "E02")
    body = Fixture.create("e02.bin", "e02-body", 8 * 1024**3)
    write_manifest(root / "fixture-manifest.json", [body])
    events = root / "fixture-events.json"
    destination, state = isolated_dirs(root)
    scripts = {body.name: [ResponseScript(terminate_after_bytes=256 * 1024**2)]}
    with fixture_server([body], events, scripts=scripts) as server:
        queue = write_queue_file(root / "queue.txt", [server.url(body.name)])
        result1 = run_downloader(queue=queue, destination=destination, state=state,
                                 reserve_bytes=0, tor_control_address=TOR_CONTROL_ADDRESS,
                                 tor_control_cookie=TOR_CONTROL_COOKIE,
                                 torsocks_conf=torsocks_conf, time_limit=600, timeout=900)
        result2 = run_downloader(queue=queue, destination=destination, state=state,
                                 reserve_bytes=0, tor_control_address=TOR_CONTROL_ADDRESS,
                                 tor_control_cookie=TOR_CONTROL_COOKIE,
                                 torsocks_conf=torsocks_conf, time_limit=600, timeout=900)
    rows = read_downloads(state)
    row = next(iter(rows.values()), None)
    logged = json.loads(events.read_text())["events"] if events.exists() else []
    second_range = next((e["range"] for e in logged[1:] if e["fixture"] == body.name), None)
    detail = (f"run1_exit={result1.returncode} run2_exit={result2.returncode} row={row} "
             f"second_request_range={second_range}")
    if (row and row["status"] == "complete" and row["sha256"] == body.sha256
            and second_range and not second_range.startswith("bytes=0-")):
        return ScenarioResult("E02", "pass", "E02: resume a large interrupted transfer",
                              f"resumed with a nonzero Range and a matching digest, no full "
                              f"restart from byte 0 | {detail}", str(events))
    return ScenarioResult("E02", "fail", "E02: resume a large interrupted transfer",
                          f"did not resume and complete with a matching digest | {detail}",
                          str(events))


def e05_kill_injection(base: Path, torsocks_conf: Path) -> ScenarioResult:
    """Kill the engine, controller, and both mid-transfer; restart; verify durable state.

    Triggers each kill by polling the staging file's on-disk size (never by
    wall-clock delay), so the kill point is reproducible across runs; the
    fixture server's event log has no entry for an in-progress transfer. The
    "both" sub-case sends simultaneous SIGKILLs, per this specification's
    resolved open question (the harder case first).
    """
    root = scenario_root(base, "E05")
    kill_threshold = 4 * 1024**2
    details = []
    for sub_case in ("engine", "controller", "both"):
        sub_root = root / sub_case
        sub_root.mkdir(parents=True)
        body = Fixture.create(f"e05-{sub_case}.bin", f"e05-{sub_case}", 32 * 1024**2)
        write_manifest(sub_root / "fixture-manifest.json", [body])
        events = sub_root / "fixture-events.json"
        destination, state = isolated_dirs(sub_root)
        scripts = {body.name: [ResponseScript(delay_seconds=0.01)]}
        with fixture_server([body], events, scripts=scripts) as server:
            queue = write_queue_file(sub_root / "queue.txt", [server.url(body.name)])
            process = start_downloader(queue=queue, destination=destination, state=state,
                                       reserve_bytes=0, tor_control_address=TOR_CONTROL_ADDRESS,
                                       tor_control_cookie=TOR_CONTROL_COOKIE,
                                       torsocks_conf=torsocks_conf)
            staging = None
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline and staging is None:
                rows = read_downloads(state)
                row = next(iter(rows.values()), None)
                if row and row.get("staging_path"):
                    staging = Path(row["staging_path"])
                else:
                    time.sleep(0.1)
            reached = staging is not None and wait_for_bytes_written(
                staging, kill_threshold, timeout=30)
            engine_pid = find_engine_pid(process.pid)
            if sub_case in ("engine", "both") and engine_pid:
                os.kill(engine_pid, signal.SIGKILL)
            if sub_case in ("controller", "both"):
                process.kill()
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=15)
            result2 = run_downloader(queue=queue, destination=destination, state=state,
                                     reserve_bytes=0, tor_control_address=TOR_CONTROL_ADDRESS,
                                     tor_control_cookie=TOR_CONTROL_COOKIE,
                                     torsocks_conf=torsocks_conf, time_limit=30, timeout=60)
        rows = read_downloads(state)
        row = next(iter(rows.values()), None)
        ok = (reached and row is not None and row["status"] == "complete"
              and row["sha256"] == body.sha256)
        if ok:
            stored = destination / row["storage_path"]
            ok = stored.read_bytes() == body.generator.read(0, body.length)
        details.append(f"{sub_case}: reached_threshold={reached} exit2={result2.returncode} "
                       f"ok={ok} row={row}")
        if not ok:
            return ScenarioResult("E05", "fail", "E05: survive engine/controller/both kills",
                                  f"{sub_case} did not recover to a complete, matching file | "
                                  f"{' | '.join(details)}", None)
    return ScenarioResult("E05", "pass", "E05: survive engine/controller/both kills",
                          f"engine, controller, and both kills all recovered to a complete, "
                          f"matching file on restart | {' | '.join(details)}", None)


def e06_controller_failpoints(base: Path, torsocks_conf: Path) -> ScenarioResult:
    """Stop the controller at each of the three named finalization failpoints.

    Sets TOD_DL_FAILPOINT (via run_downloader's `failpoint` parameter), which
    only this evaluation runner sets; the environment variable is never
    reachable in normal operation (Downloader.hit_failpoint, tod-dl.py).
    """
    root = scenario_root(base, "E06")
    details = []
    for failpoint in ("post_validation_intent", "post_final_file_creation",
                      "post_completion_commit"):
        sub_root = root / failpoint
        sub_root.mkdir(parents=True)
        body = Fixture.create(f"e06-{failpoint}.bin", f"e06-{failpoint}", 4096)
        write_manifest(sub_root / "fixture-manifest.json", [body])
        events = sub_root / "fixture-events.json"
        destination, state = isolated_dirs(sub_root)
        with fixture_server([body], events) as server:
            queue = write_queue_file(sub_root / "queue.txt", [server.url(body.name)])
            result1 = run_downloader(queue=queue, destination=destination, state=state,
                                     reserve_bytes=0, tor_control_address=TOR_CONTROL_ADDRESS,
                                     tor_control_cookie=TOR_CONTROL_COOKIE,
                                     torsocks_conf=torsocks_conf, failpoint=failpoint,
                                     time_limit=15, timeout=30)
            result2 = run_downloader(queue=queue, destination=destination, state=state,
                                     reserve_bytes=0, tor_control_address=TOR_CONTROL_ADDRESS,
                                     tor_control_cookie=TOR_CONTROL_COOKIE,
                                     torsocks_conf=torsocks_conf, time_limit=15, timeout=30)
        rows = read_downloads(state)
        row = next(iter(rows.values()), None)
        ok = row is not None and row["status"] == "complete" and row["sha256"] == body.sha256
        details.append(f"{failpoint}: exit1={result1.returncode} exit2={result2.returncode} "
                       f"ok={ok} row={row}")
        if not ok:
            return ScenarioResult("E06", "fail",
                                  "E06: unambiguous process boundaries at finalization",
                                  f"{failpoint} did not reconcile to a complete, matching "
                                  f"record | {' | '.join(details)}", None)
    return ScenarioResult("E06", "pass",
                          "E06: unambiguous process boundaries at finalization",
                          f"all three finalization failpoints reconciled idempotently on "
                          f"restart | {' | '.join(details)}", None)


def e10_million_row_admission_and_resources(base: Path, torsocks_conf: Path) -> ScenarioResult:
    """One million rows; assert resource, admission, latency, and shutdown targets.

    Generates the queue file fresh in this isolated run directory, per this
    specification's resolved open question. The fixture server synthesizes
    each tiny body on demand; nothing is requested for most of the million
    rows, per the parent specification's "do not request every URL."
    """
    root = scenario_root(base, "E10")
    run_id = "e10-admission-run"
    fixtures = generate_synthetic_queue_rows(1_000_000)
    events = root / "fixture-events.json"
    destination, state = isolated_dirs(root)
    with fixture_server(fixtures, events) as server:
        urls = [server.url(fixture.name) for fixture in fixtures]
        queue = write_queue_file(root / "queue.txt", urls)
        process = start_downloader(queue=queue, destination=destination, state=state,
                                   workers=4, reserve_bytes=0,
                                   tor_control_address=TOR_CONTROL_ADDRESS,
                                   tor_control_cookie=TOR_CONTROL_COOKIE,
                                   torsocks_conf=torsocks_conf, time_limit=120,
                                   extra_args=["--run-id", run_id])
        sampler = ResourceSampler(process.pid).start()
        status_latencies = []
        run_deadline = time.monotonic() + 90
        while time.monotonic() < run_deadline and process.poll() is None:
            try:
                started = time.monotonic()
                inspection_request(state, run_id, "list_queue", {"limit": 1})
                status_latencies.append(time.monotonic() - started)
            except Exception:  # noqa: BLE001 - socket may not be up yet
                pass
            time.sleep(5)
        peak_rss = sampler.stop()
        shutdown_started = time.monotonic()
        process.send_signal(signal.SIGTERM)
        try:
            process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=15)
        shutdown_seconds = time.monotonic() - shutdown_started
        logged = json.loads(events.read_text())["events"] if events.exists() else []
    requested_urls = {event["fixture"] for event in logged}
    max_status_latency = max(status_latencies, default=0.0)
    rss_target = 512 * 1024**2
    latency_target = 2.0
    shutdown_target = 30.0
    admission_bound = 100  # far below 1,000,000; demonstrates a bounded, not exhaustive, admit
    checks = {
        "peak_rss_within_target": peak_rss < rss_target,
        "status_latency_within_target": max_status_latency < latency_target,
        "shutdown_within_target": shutdown_seconds < shutdown_target,
        "admission_bounded": len(requested_urls) <= admission_bound,
    }
    detail = (f"requested_urls={len(requested_urls)} (bound<={admission_bound}) "
             f"peak_rss_bytes={peak_rss} (target<{rss_target}) "
             f"max_status_latency_s={max_status_latency:.2f} (target<{latency_target}s) "
             f"shutdown_seconds={shutdown_seconds:.1f} (target<{shutdown_target}s) "
             f"checks={checks}")
    if all(checks.values()):
        return ScenarioResult("E10", "pass",
                              "E10: one million rows within resource and responsiveness targets",
                              detail, str(events))
    return ScenarioResult("E10", "fail",
                          "E10: one million rows within resource and responsiveness targets",
                          detail, str(events))


def e13_concurrency_timing(base: Path, torsocks_conf: Path) -> ScenarioResult:
    """Slow large file alongside small files and a due retry; assert prompt refill.

    Records admission (`active`) and completion (`complete`) timestamps from
    download_transitions, since the fixture event log carries no timestamp.
    """
    root = scenario_root(base, "E13")
    large = Fixture.create("e13-large.bin", "e13-large", 8 * 1024**2)
    retry_body = Fixture.create("e13-retry.bin", "e13-retry", 4096)
    small = [Fixture.create(f"e13-small-{i}.bin", f"e13-small-{i}", 4096) for i in range(4)]
    fixtures = [large, retry_body, *small]
    write_manifest(root / "fixture-manifest.json", fixtures)
    events = root / "fixture-events.json"
    destination, state = isolated_dirs(root)
    # Stay under _downloader_command()'s fixed --timeout=5 (aria2_evaluation_adapter.py),
    # or aria2 itself times out the response wait before it ever starts.
    scripts = {
        large.name: [ResponseScript(delay_seconds=3.0)],
        retry_body.name: [ResponseScript(status=503), ResponseScript()],
    }
    with fixture_server(fixtures, events, scripts=scripts) as server:
        retry_url = server.url(retry_body.name)
        urls = [server.url(f.name) for f in (large, retry_body, *small)]
        queue = write_queue_file(root / "queue.txt", urls)
        result1 = run_downloader(queue=queue, destination=destination, state=state,
                                 workers=4, reserve_bytes=0,
                                 tor_control_address=TOR_CONTROL_ADDRESS,
                                 tor_control_cookie=TOR_CONTROL_COOKIE,
                                 torsocks_conf=torsocks_conf, time_limit=20, timeout=35)
        # Force the retry due now instead of waiting out the normal >=60s backoff.
        due_at = dt.datetime.now(dt.timezone.utc)
        with sqlite3.connect(state / "manifest.sqlite") as db:
            db.execute("UPDATE downloads SET next_retry_at=0 WHERE url=?", (retry_url,))
            db.commit()
        result2 = run_downloader(queue=queue, destination=destination, state=state,
                                 workers=4, reserve_bytes=0,
                                 tor_control_address=TOR_CONTROL_ADDRESS,
                                 tor_control_cookie=TOR_CONTROL_COOKIE,
                                 torsocks_conf=torsocks_conf, time_limit=20, timeout=40)
    transitions = read_transitions(state)
    admissions = {t["url"]: t["recorded_at"] for t in transitions if t["to_status"] == "active"}
    completions = {t["url"]: t["recorded_at"] for t in transitions
                  if t["to_status"] == "complete" and t["from_status"] != "complete"}
    refill_target = 5.0

    def parse(value: str) -> dt.datetime:
        return dt.datetime.fromisoformat(value)

    small_urls = [server.url(f.name) for f in small]
    missing = [url for url in (*small_urls, retry_url) if url not in completions]
    large_url = server.url(large.name)
    retry_admission = admissions.get(retry_url)
    retry_refill_seconds = ((parse(retry_admission) - due_at).total_seconds()
                            if retry_admission else None)
    small_complete_before_large = (large_url in completions and all(
        parse(completions[url]) < parse(completions[large_url]) for url in small_urls
        if url in completions))
    # recorded_at has whole-second precision (now(), src/tod-dl.py) but due_at has
    # microsecond precision, so recorded_at can appear up to ~1s earlier than due_at
    # purely from truncation; allow that much slack without weakening the 5s target.
    checks = {
        "all_small_and_retry_completed": not missing,
        "retry_refilled_promptly": (retry_refill_seconds is not None
                                    and -1.0 <= retry_refill_seconds < refill_target),
        "small_files_not_blocked_by_large": small_complete_before_large,
    }
    detail = (f"exit1={result1.returncode} exit2={result2.returncode} "
             f"admissions={admissions} completions={completions} "
             f"retry_refill_seconds={retry_refill_seconds} checks={checks}")
    if all(checks.values()):
        return ScenarioResult("E13", "pass",
                              "E13: refill idle slots promptly under a slow transfer",
                              detail, str(events))
    return ScenarioResult("E13", "fail",
                          "E13: refill idle slots promptly under a slow transfer",
                          detail, str(events))


NOT_RUN = {
    "E02": "harness built and wired, but not run to completion this session. The "
          "deterministic fixture generator's throughput (Section 1's open question in "
          "specs/SPEC-acquisition-evaluation-infrastructure.md) is now measured and "
          "fixed: it used a 64-byte blake2b digest per block (134M hash calls for an "
          "8 GiB fixture, ~3.4 MB/s) and, through Fixture.generator, created a fresh "
          "DeterministicBytes instance per read, so no per-instance cache could ever "
          "hit. It now uses a shake_256 digest producing 1 MiB blocks (8192 calls for "
          "an 8 GiB fixture) via a module-level cache keyed on (seed, block index), "
          "measured at 8 GiB in ~16 s (>500 MB/s), well inside the 600 s attempt limit. "
          "The is_incomplete_body fix itself is confirmed working (the staging file "
          "grew well past the 256 MiB interrupt point, i.e. it resumed instead of "
          "routing to review_required). Run E02 to completion next.",
    "E11": "no harness built yet for a five-item run selected from a larger queue; "
          "see specs/SPEC-acquisition-tool-evaluation.md's scenario table.",
}


def main() -> int:
    if TOR_CONTROL_COOKIE.exists():
        try:
            TOR_CONTROL_COOKIE.read_bytes()
        except OSError as exc:
            print(f"cannot read Tor control cookie: {exc}", file=sys.stderr)
            return 2
    else:
        print("Tor control cookie not found; E14 needs a live system Tor control port",
              file=sys.stderr)
        return 2

    output = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("/tmp/tod-dl-eval-run")
    if output.exists():
        print(f"{output} already exists; refusing to overwrite", file=sys.stderr)
        return 2
    output.mkdir(parents=True)
    torsocks_conf = write_local_fixture_torsocks_conf(output)

    scenarios = [
        e01_small_and_empty,
        e03_range_ignored,
        e04_outage_then_recovery,
        e05_kill_injection,
        e06_controller_failpoints,
        e07_existing_final,
        e08_checksum_mismatch_review,
        e09_disk_exhaustion,
        e10_million_row_admission_and_resources,
        e12_encoded_names_and_duplicates,
        e13_concurrency_timing,
        e14_tor_admission_guard,
    ]
    results: list[ScenarioResult] = []
    for scenario in scenarios:
        print(f"running {scenario.__name__} ...", flush=True)
        try:
            result = scenario(output, torsocks_conf)
        except Exception as exc:  # noqa: BLE001 - record the failure, don't crash the run
            result = ScenarioResult(scenario.__name__[:4].upper(), "fail",
                                    "adapter execution", f"adapter raised {exc!r}")
        print(f"  {result.scenario_id}: {result.outcome}")
        results.append(result)
    for scenario_id, reason in NOT_RUN.items():
        results.append(ScenarioResult(scenario_id, "not run",
                                      f"{scenario_id}: see specs/SPEC-acquisition-tool-evaluation.md",
                                      reason))

    manifest_digest = json.loads(
        (output / "e01" / "fixture-manifest.json").read_text()
    )["manifest_sha256"]
    report = build_report(
        candidate="per-URL aria2 process through Tor (torsocks -i, one process per URL)",
        command=["python3", "src/tod-dl.py", "--queue", "<per-scenario>",
                "--torsocks", "/usr/bin/torsocks", "--aria2c", "/usr/bin/aria2c"],
        manifest_sha256=manifest_digest,
        results=results,
        adapter_revision="src/aria2_evaluation_adapter.py@initial",
        binary_version="aria2 1.37.0",
    )
    write_json(output / "evaluation-report.json", report)
    print(f"\nWrote {output / 'evaluation-report.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
