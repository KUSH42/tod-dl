#!/usr/bin/env python3
"""Run the tractable E01-E14 subset against the current per-URL aria2 process
configuration and write a sanitized selection report.

See specs/SPEC-acquisition-tool-evaluation.md. This script drives the real,
unmodified `tod-dl.py` CLI through `aria2_evaluation_adapter.py` against a
local loopback fixture server only. It never contacts an acquisition source
and never runs a source pilot.
"""

from __future__ import annotations

import json
import os
import stat
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from acquisition_evaluation import (Fixture, ResponseScript, ScenarioResult,
                                    build_report, write_json, write_manifest)
from aria2_evaluation_adapter import (FIXTURE_PREFIX, fixture_server, isolated_dirs,
                                      read_downloads, run_downloader,
                                      write_local_fixture_torsocks_conf,
                                      write_queue_file)

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
    root = scenario_root(base, "E08")
    body = Fixture.create("e08.bin", "e08-body", 4096)
    write_manifest(root / "fixture-manifest.json", [body])
    events = root / "fixture-events.json"
    destination, state = isolated_dirs(root)
    scripts = {body.name: [ResponseScript(short_body_bytes=100)]}
    with fixture_server([body], events, scripts=scripts) as server:
        queue = write_queue_file(root / "queue.txt", [server.url(body.name)])
        result = run_downloader(queue=queue, destination=destination, state=state,
                                reserve_bytes=0, tor_control_address=TOR_CONTROL_ADDRESS,
                                tor_control_cookie=TOR_CONTROL_COOKIE,
                                torsocks_conf=torsocks_conf, time_limit=8, timeout=25)
    rows = read_downloads(state)
    row = next(iter(rows.values()), None)
    detail = (f"exit={result.returncode} row={row} note='only the short-body sub-case was "
             f"exercised this session, not the HTML-200-error or post-hash-mismatch cases'")
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
    return ScenarioResult("E12", "fail",
                          "E12: stable mapping; explicit rejection; enforced source scope",
                          f"duplicate URL de-duplicated and unsafe traversal path rejected "
                          f"before queueing, and the Unicode name transferred correctly, but "
                          f"read_queues() drops the rejected URL silently (ValueError -> "
                          f"continue, no report) instead of emitting the explicit rejection "
                          f"report this requirement calls for | {detail}", str(events))


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


NOT_RUN = {
    "E02": "needs the 8 GiB generated-file interrupted-transfer scenario and a streaming "
          "digest pass; a later session should run it once, in isolation, given the runtime cost.",
    "E05": "needs process-kill timing around engine/controller/both mid-transfer; a later "
          "session needs a kill-injection harness around Downloader.transfer/run_aria2.",
    "E06": "needs the controller failpoints between validation, link creation, and DB commit "
          "that the spec asks the fixture harness to add; not yet implemented in this adapter.",
    "E10": "needs a one-million-row synthetic queue and RSS sampling at a fixed interval; "
          "a later session should build the queue generator and a sampler around the run.",
    "E13": "needs concurrent slow-large-file plus small-file admission-timing assertions "
          "(5-second idle-slot refill target); not attempted this session.",
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
        e07_existing_final,
        e08_checksum_mismatch_review,
        e09_disk_exhaustion,
        e12_encoded_names_and_duplicates,
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
