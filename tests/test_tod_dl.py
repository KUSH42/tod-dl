"""Regression tests for bounded, queue-ordered downloader admission."""

from __future__ import annotations

import tempfile
import unittest
import os
import fcntl
import hashlib
import errno
import importlib.util
import inspect
import itertools
import json
import re
import shutil
import sqlite3
import signal
import sys
import threading
import base64
import stat
import time
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from types import SimpleNamespace

from cryptography.hazmat.primitives import serialization

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
module_path = Path(__file__).resolve().parents[1] / "src" / "tod-dl.py"
module_spec = importlib.util.spec_from_file_location("tod_dl", module_path)
if module_spec is None or module_spec.loader is None:
    raise RuntimeError("cannot load tod-dl.py for tests")
tod_dl = importlib.util.module_from_spec(module_spec)
sys.modules["tod_dl"] = tod_dl
module_spec.loader.exec_module(tod_dl)

from tod_dl import (ADMISSION_POLL_SECONDS, FAILPOINTS, Downloader, RETRY_DELAYS,
                    TelemetryStateProjection,
                    aria2_log_terminal_status, aria2_rpc_job_status, hit_failpoint,
                    relative_path, sha256sum, storage_relative)
from download_telemetry import (PUBLISH_INTERVAL_SECONDS, TelemetryPublisher,
                                estimate_eta_seconds, reduce_metrics)
from provenance import ProvenanceError, ProvenanceWriter, canonical_json, digest as event_digest
from verify_provenance import verify


def make_args(root: Path, queue: Path, max_files: int = 0) -> SimpleNamespace:
    return SimpleNamespace(
        destination=root / "destination",
        state=root / "state",
        queue=[queue],
        workers=4,
        max_files=max_files,
        max_attempts=0,
        run_id="test-run",
        worker_stagger=0,
        socks_backoff=60,
    )


class RunSelectionTests(unittest.TestCase):
    def test_rpc_terminal_job_status_reads_known_gid(self):
        import tod_dl

        original = tod_dl.aria2_rpc_call
        calls = []

        def rpc_call(port, method, params):
            calls.append((port, method, params))
            return {"result": {"status": "error"}}

        tod_dl.aria2_rpc_call = rpc_call
        try:
            self.assertEqual(aria2_rpc_job_status(12345, "secret", "gid-1"), "error")
        finally:
            tod_dl.aria2_rpc_call = original
        self.assertEqual(calls, [(12345, "aria2.tellStatus",
                                  ["token:secret", "gid-1", ["status"]])])

    def test_rpc_terminal_job_status_uses_stopped_list_without_gid(self):
        import tod_dl

        original = tod_dl.aria2_rpc_call
        tod_dl.aria2_rpc_call = lambda *_: {"result": [{"status": "complete"}]}
        try:
            self.assertEqual(aria2_rpc_job_status(12345, "secret", None), "complete")
        finally:
            tod_dl.aria2_rpc_call = original

    def test_log_terminal_status_is_a_fallback_for_stalled_rpc(self):
        with tempfile.TemporaryDirectory() as temporary:
            log_path = Path(temporary) / "aria2.log"
            log_path.write_text("errorCode=1 Failed to connect to the host\n",
                                encoding="utf-8")
            self.assertEqual(aria2_log_terminal_status(log_path), "error")
            log_path.write_text("NOTICE Download complete: fixture.bin\n",
                                encoding="utf-8")
            self.assertEqual(aria2_log_terminal_status(log_path), "complete")

    def prepare(self, root: Path, urls: list[str], max_files: int = 0):
        queue = root / "queue.txt"
        queue.write_text("\n".join(urls) + "\n", encoding="utf-8")
        downloader = Downloader(make_args(root, queue, max_files))
        downloader.destination.mkdir()
        downloader.state.mkdir()
        db = downloader.open_db()
        downloader.import_queues(db)
        return downloader, db

    def test_import_reports_rejected_and_duplicate_queue_lines(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            good = "https://fixture.test/first/data/item.bin"
            invalid = "https://fixture.test/first/data/../escape.bin"
            urls = [good, invalid, good]
            queue = root / "queue.txt"
            queue.write_text("\n".join(urls) + "\n", encoding="utf-8")
            downloader = Downloader(make_args(root, queue))
            downloader.destination.mkdir()
            downloader.state.mkdir()
            db = downloader.open_db()
            capture = StringIO()
            with redirect_stdout(capture):
                imported = downloader.import_queues(db)
            output = capture.getvalue()
            self.assertEqual(imported, 1)
            self.assertIn(f"[queue-rejected] {invalid}: unsafe URL path", output)
            self.assertIn(f"[queue-rejected] {good}: duplicate URL", output)
            db.close()

    def test_later_queue_refreshes_size_and_checksum_of_an_existing_row(self):
        url = "https://fixture.test/first/data/item.bin"
        first, second = "a" * 64, "b" * 64
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            downloader, db = self.prepare(root, [f"{url} size=1K sha256={first}"])
            queue = root / "queue.txt"
            query = "SELECT inventory_size, expected_sha256 FROM downloads WHERE url=?"
            self.assertEqual(db.execute(query, (url,)).fetchone(), ("1K", first))
            queue.write_text(f"{url} size=2K sha256={second}\n", encoding="utf-8")
            downloader.import_queues(db)
            self.assertEqual(db.execute(query, (url,)).fetchone(), ("2K", second))
            db.close()

    def test_selection_skips_existing_before_applying_limit(self):
        urls = [
            "https://fixture.test/first/data/existing.bin",
            "https://fixture.test/second/data/second.bin",
            "https://fixture.test/third/data/third.bin",
        ]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            downloader, db = self.prepare(root, urls, max_files=2)
            existing = downloader.destination / "first" / "data" / "existing.bin"
            existing.parent.mkdir(parents=True)
            existing.write_bytes(b"evidence")

            selected, skipped = downloader.scope_run(db)

            self.assertEqual((selected, skipped), (2, 1))
            rows = db.execute("SELECT url, queue_rank FROM run_items "
                              "WHERE run_id='test-run' "
                              "ORDER BY queue_rank").fetchall()
            self.assertEqual(rows, [(urls[1], 0), (urls[2], 1)])
            self.assertEqual(db.execute("SELECT status FROM downloads WHERE url=?",
                                        (urls[0],)).fetchone()[0], "existing_unverified")
            db.close()

    def test_scope_run_completes_an_existing_file_whose_hash_matches_the_queue(self):
        url = "https://fixture.test/first/data/existing.bin"
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            queue = root / "queue.txt"
            content = b"evidence"
            digest = hashlib.sha256(content).hexdigest()
            queue.write_text(f"{url} sha256={digest}\n", encoding="utf-8")
            downloader = Downloader(make_args(root, queue))
            downloader.destination.mkdir()
            downloader.state.mkdir()
            db = downloader.open_db()
            downloader.import_queues(db)
            existing = downloader.destination / "first" / "data" / "existing.bin"
            existing.parent.mkdir(parents=True)
            existing.write_bytes(content)

            selected, skipped = downloader.scope_run(db)

            self.assertEqual((selected, skipped), (0, 1))
            row = db.execute("SELECT status, bytes, sha256 FROM downloads WHERE url=?",
                             (url,)).fetchone()
            self.assertEqual(row, ("complete", len(content), digest))
            db.close()

    def test_scope_run_sends_an_existing_file_with_a_mismatched_hash_to_review(self):
        url = "https://fixture.test/first/data/existing.bin"
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            queue = root / "queue.txt"
            expected_digest = hashlib.sha256(b"evidence").hexdigest()
            queue.write_text(f"{url} sha256={expected_digest}\n", encoding="utf-8")
            downloader = Downloader(make_args(root, queue))
            downloader.destination.mkdir()
            downloader.state.mkdir()
            db = downloader.open_db()
            downloader.import_queues(db)
            existing = downloader.destination / "first" / "data" / "existing.bin"
            existing.parent.mkdir(parents=True)
            existing.write_bytes(b"a different file entirely")

            selected, skipped = downloader.scope_run(db)

            self.assertEqual((selected, skipped), (0, 1))
            row = db.execute("SELECT status, last_error, review_code FROM downloads WHERE url=?",
                             (url,)).fetchone()
            self.assertEqual(row, ("review_required", "checksum mismatch", "checksum_mismatch"))
            db.close()

    def test_relative_path_decodes_percent_encoded_segments(self):
        path = relative_path("https://example.invalid/RUN1/data/a%20b/c%24d.txt")
        self.assertEqual(path.as_posix(), "RUN1/data/a b/c$d.txt")

    def test_relative_path_decodes_prefix_segment(self):
        path = relative_path("https://example.invalid/R%20UN1/data/file.txt")
        self.assertEqual(path.as_posix(), "R UN1/data/file.txt")

    def test_relative_path_accepts_a_url_without_a_data_segment(self):
        path = relative_path("https://example.invalid/RUN1/a%20b/c.txt")
        self.assertEqual(path.as_posix(), "RUN1/a b/c.txt")

    def test_relative_path_rejects_a_url_with_no_file_or_a_listing_file(self):
        for bad in ("https://example.invalid/RUN1", "https://example.invalid/RUN1/",
                    "https://example.invalid/RUN1/ALL_FILES", "https://example.invalid/RUN1/data/ALL_FILES",
                    "https://example.invalid/RUN1/../secret"):
            with self.assertRaises(ValueError, msg=bad):
                relative_path(bad)

    def test_relative_path_rejects_encoded_traversal(self):
        with self.assertRaises(ValueError):
            relative_path("https://example.invalid/RUN1/data/a%2F%2E%2E%2Fb")

    def test_relative_path_rejects_plain_traversal(self):
        with self.assertRaises(ValueError):
            relative_path("https://example.invalid/RUN1/data/../secret")

    def test_reimport_does_not_remap_an_already_acquired_encoded_path(self):
        """A corrected relative_path() must not orphan evidence already
        acquired under the previous, un-decoded storage path."""
        url = "https://fixture.test/first/data/a%20b.bin"
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            queue = root / "queue.txt"
            queue.write_text(url + "\n", encoding="utf-8")
            downloader = Downloader(make_args(root, queue))
            downloader.destination.mkdir()
            downloader.state.mkdir()
            acquired = downloader.destination / "first" / "data" / "a%20b.bin"
            acquired.parent.mkdir(parents=True)
            acquired.write_bytes(b"evidence")
            db = downloader.open_db()
            db.execute("INSERT INTO downloads (url, relative_path, storage_path, "
                       "status, updated_at) VALUES (?, ?, ?, 'complete', ?)",
                       (url, "first/data/a%20b.bin", "first/data/a%20b.bin", "now"))
            db.commit()

            downloader.import_queues(db)

            self.assertEqual(db.execute("SELECT storage_path FROM downloads WHERE url=?",
                                        (url,)).fetchone()[0], "first/data/a%20b.bin")
            selected, existing = downloader.scope_run(db)
            self.assertEqual((selected, existing), (0, 1))
            self.assertEqual(db.execute("SELECT status FROM downloads WHERE url=?",
                                        (url,)).fetchone()[0], "complete")
            db.close()

    def test_dry_run_does_not_report_an_already_acquired_encoded_path_as_missing(self):
        url = "https://fixture.test/first/data/a%20b.bin"
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            queue = root / "queue.txt"
            queue.write_text(url + "\n", encoding="utf-8")
            downloader = Downloader(make_args(root, queue))
            downloader.destination.mkdir()
            downloader.state.mkdir()
            acquired = downloader.destination / "first" / "data" / "a%20b.bin"
            acquired.parent.mkdir(parents=True)
            acquired.write_bytes(b"evidence")

            with redirect_stdout(StringIO()) as output:
                downloader.dry_run()

            self.assertIn("EXISTING", output.getvalue())
            self.assertNotIn("MISSING", output.getvalue())

    def test_retry_backoff_is_capped_at_five_minutes(self):
        self.assertEqual(max(RETRY_DELAYS), 300)

    def test_worker_slots_are_reused_after_a_retry(self):
        with tempfile.TemporaryDirectory() as temporary:
            queue = Path(temporary) / "queue.txt"
            queue.write_text("", encoding="utf-8")
            downloader = Downloader(make_args(Path(temporary), queue))
            downloader.args.workers = 2
            downloader.worker_ids = {"first": 1, "second": 2}
            with self.assertRaisesRegex(RuntimeError, "no worker slot"):
                downloader.reserve_worker_id()
            del downloader.worker_ids["first"]
            self.assertEqual(downloader.reserve_worker_id(), 1)

    def test_tor_circuit_renewal_is_rate_limited(self):
        import tod_dl

        with tempfile.TemporaryDirectory() as temporary:
            queue = Path(temporary) / "queue.txt"
            queue.write_text("", encoding="utf-8")
            downloader = Downloader(make_args(Path(temporary), queue))
            downloader.args.tor_newnym_interval = 60
            downloader.args.tor_control_address = "127.0.0.1:9051"
            downloader.args.tor_control_cookie = Path(temporary) / "cookie"
            original = tod_dl.send_tor_newnym
            requests = []
            tod_dl.send_tor_newnym = lambda *_: requests.append("newnym")
            try:
                self.assertTrue(downloader.request_newnym())
                self.assertFalse(downloader.request_newnym())
            finally:
                tod_dl.send_tor_newnym = original
            self.assertEqual(requests, ["newnym"])

    def test_operator_tor_renewal_is_audited_and_keeps_items_unchanged(self):
        import tod_dl

        url = "https://fixture.test/first/data/item.bin"
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            downloader, db = self.prepare(root, [url])
            downloader.scope_run(db)
            downloader.args.tor_newnym_interval = 60
            downloader.args.tor_control_address = "127.0.0.1:9051"
            downloader.args.tor_control_cookie = root / "cookie"
            original = tod_dl.send_tor_newnym
            request = {"request_id": "renew-1", "session_id": "session-1"}
            try:
                tod_dl.send_tor_newnym = lambda *_: (_ for _ in ()).throw(
                    RuntimeError("Tor rejected SIGNAL NEWNYM"))
                failed = downloader.control_renew_tor_circuits(db, request)
                tod_dl.send_tor_newnym = lambda *_: None
                first = downloader.control_renew_tor_circuits(
                    db, {"request_id": "renew-2", "session_id": "session-1"})
                second = downloader.control_renew_tor_circuits(
                    db, {"request_id": "renew-3", "session_id": "session-1"})
            finally:
                tod_dl.send_tor_newnym = original
            self.assertEqual(failed["outcome"], "failed")
            self.assertEqual(first["outcome"], "completed")
            self.assertEqual(second["outcome"], "rejected")
            self.assertIn("seconds", second["reason"])
            self.assertEqual(db.execute("SELECT status, next_retry_at FROM downloads WHERE url=?",
                                        (url,)).fetchone(), ("queued", 0))
            rows = db.execute("SELECT action, outcome, next_eligible_at FROM tor_renewals "
                              "ORDER BY requested_at").fetchall()
            self.assertEqual([row[0] for row in rows], ["renew_tor_circuits"] * 3)
            self.assertEqual([row[1] for row in rows], ["failed", "completed", "rejected"])
            self.assertIsNotNone(rows[1][2])
            self.assertEqual(db.execute("SELECT COUNT(*) FROM control_requests "
                                        "WHERE action='renew_tor_circuits'").fetchone()[0], 3)
            db.close()

    def test_retry_now_resets_retry_wait_items(self):
        url = "https://fixture.test/first/data/item.bin"
        with tempfile.TemporaryDirectory() as temporary:
            downloader, db = self.prepare(Path(temporary), [url])
            downloader.scope_run(db)
            downloader.update(db, "UPDATE downloads SET status='retry_wait', "
                              "next_retry_at=9999999999 WHERE url=?", (url,))
            downloader.reset_retry_now(db)
            db.commit()
            self.assertEqual(downloader.next_pending(db)[0], url)
            db.close()

    def test_retry_now_resets_automatically_queued_items(self):
        url = "https://fixture.test/first/data/item.bin"
        with tempfile.TemporaryDirectory() as temporary:
            downloader, db = self.prepare(Path(temporary), [url])
            downloader.scope_run(db)
            downloader.update(db, "UPDATE downloads SET status='queued', "
                              "next_retry_at=9999999999 WHERE url=?", (url,))

            downloader.reset_retry_now(db)
            db.commit()

            self.assertEqual(downloader.next_pending(db)[0], url)
            db.close()

    def test_control_retry_now_is_selected_and_idempotently_audited(self):
        url = "https://fixture.test/first/data/item.bin"
        with tempfile.TemporaryDirectory() as temporary:
            downloader, db = self.prepare(Path(temporary), [url])
            downloader.scope_run(db)
            downloader.update(db, "UPDATE downloads SET status='retry_wait', "
                              "next_retry_at=9999999999 WHERE url=?", (url,))
            request = {"request_id": "request-one", "session_id": "session-one"}

            first = downloader.control_retry_now(db, request)
            second = downloader.control_retry_now(db, request)

            self.assertEqual(first, second)
            self.assertEqual(downloader.next_pending(db)[0], url)
            self.assertTrue(downloader.control_wake.is_set())
            self.assertEqual(db.execute("SELECT COUNT(*) FROM control_requests").fetchone()[0], 1)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM download_transitions "
                                        "WHERE url='__control__'").fetchone()[0], 1)
            db.close()

    def test_control_retry_now_scopes_to_requested_item_ids(self):
        first_url = "https://fixture.test/first/data/item.bin"
        second_url = "https://fixture.test/first/data/other.bin"
        with tempfile.TemporaryDirectory() as temporary:
            downloader, db = self.prepare(Path(temporary), [first_url, second_url])
            downloader.scope_run(db)
            downloader.update(db, "UPDATE downloads SET status='retry_wait', "
                              "next_retry_at=9999999999", ())
            request = {"request_id": "request-one", "session_id": "session-one",
                      "parameters": {"item_ids": [downloader.item_id(first_url)]}}

            response = downloader.control_retry_now(db, request)

            self.assertEqual(response["outcome"], "completed")
            self.assertIn("made 1 selected", response["reason"])
            self.assertEqual(db.execute("SELECT next_retry_at FROM downloads WHERE url=?",
                                        (first_url,)).fetchone()[0], 0)
            self.assertEqual(db.execute("SELECT next_retry_at FROM downloads WHERE url=?",
                                        (second_url,)).fetchone()[0], 9999999999)
            db.close()

    def test_control_retry_now_rejects_malformed_item_ids(self):
        url = "https://fixture.test/first/data/item.bin"
        with tempfile.TemporaryDirectory() as temporary:
            downloader, db = self.prepare(Path(temporary), [url])
            downloader.scope_run(db)
            request = {"request_id": "request-one", "session_id": "session-one",
                      "parameters": {"item_ids": []}}

            response = downloader.control_retry_now(db, request)

            self.assertEqual(response["outcome"], "rejected")
            self.assertIn("item_ids", response["reason"])
            db.close()

    def test_control_pause_and_resume_admission_toggle_the_gate_and_are_audited(self):
        url = "https://fixture.test/first/data/item.bin"
        with tempfile.TemporaryDirectory() as temporary:
            downloader, db = self.prepare(Path(temporary), [url])
            downloader.scope_run(db)
            pause_request = {"request_id": "pause-1", "session_id": "session-1"}

            first = downloader.control_pause_admission(db, pause_request)
            second = downloader.control_pause_admission(db, pause_request)
            again_paused = downloader.control_pause_admission(
                db, {"request_id": "pause-2", "session_id": "session-1"})

            self.assertEqual(first["outcome"], "completed")
            self.assertEqual(first, second)
            self.assertTrue(downloader.admission_paused.is_set())
            self.assertEqual(again_paused["outcome"], "rejected")

            resume_request = {"request_id": "resume-1", "session_id": "session-1"}
            resumed = downloader.control_resume_admission(db, resume_request)
            not_paused = downloader.control_resume_admission(
                db, {"request_id": "resume-2", "session_id": "session-1"})

            self.assertEqual(resumed["outcome"], "completed")
            self.assertFalse(downloader.admission_paused.is_set())
            self.assertTrue(downloader.control_wake.is_set())
            self.assertEqual(not_paused["outcome"], "rejected")
            self.assertEqual(db.execute("SELECT COUNT(*) FROM control_requests "
                                        "WHERE action='pause_admission'").fetchone()[0], 2)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM control_requests "
                                        "WHERE action='resume_admission'").fetchone()[0], 2)
            db.close()

    def test_control_drain_and_stop_gates_admission_without_stopping_active_transfers(self):
        url = "https://fixture.test/first/data/item.bin"
        with tempfile.TemporaryDirectory() as temporary:
            downloader, db = self.prepare(Path(temporary), [url])
            downloader.scope_run(db)
            request = {"request_id": "drain-1", "session_id": "session-1"}

            first = downloader.control_drain_and_stop(db, request)
            second = downloader.control_drain_and_stop(db, request)
            rejected = downloader.control_drain_and_stop(
                db, {"request_id": "drain-2", "session_id": "session-1"})

            self.assertEqual(first["outcome"], "completed")
            self.assertEqual(first, second)
            self.assertTrue(downloader.drain_requested.is_set())
            self.assertFalse(downloader.stop_requested.is_set())
            self.assertEqual(rejected["outcome"], "rejected")
            self.assertEqual(db.execute("SELECT COUNT(*) FROM control_requests "
                                        "WHERE action='drain_and_stop'").fetchone()[0], 2)
            db.close()

    def test_control_checkpoint_stop_sets_stop_requested_and_is_audited(self):
        url = "https://fixture.test/first/data/item.bin"
        with tempfile.TemporaryDirectory() as temporary:
            downloader, db = self.prepare(Path(temporary), [url])
            downloader.scope_run(db)
            request = {"request_id": "checkpoint-1", "session_id": "session-1"}

            first = downloader.control_checkpoint_stop(db, request)
            second = downloader.control_checkpoint_stop(db, request)
            rejected = downloader.control_checkpoint_stop(
                db, {"request_id": "checkpoint-2", "session_id": "session-1"})

            self.assertEqual(first["outcome"], "completed")
            self.assertEqual(first, second)
            self.assertTrue(downloader.stop_requested.is_set())
            self.assertEqual(rejected["outcome"], "rejected")
            self.assertEqual(db.execute("SELECT COUNT(*) FROM control_requests "
                                        "WHERE action='checkpoint_stop'").fetchone()[0], 2)
            db.close()

    def test_control_state_reports_run_wide_action_availability(self):
        url = "https://fixture.test/first/data/item.bin"
        with tempfile.TemporaryDirectory() as temporary:
            downloader, db = self.prepare(Path(temporary), [url])
            downloader.scope_run(db)
            downloader.args.tor_newnym_interval = 60

            actions = downloader.control_state()["actions"]
            self.assertEqual(actions["pause_admission"], "available")
            self.assertEqual(actions["resume_admission"], "unavailable")
            self.assertEqual(actions["drain_and_stop"], "available")
            self.assertEqual(actions["checkpoint_stop"], "available")

            downloader.control_pause_admission(
                db, {"request_id": "pause-1", "session_id": "session-1"})
            paused_actions = downloader.control_state()["actions"]
            self.assertEqual(paused_actions["pause_admission"], "unavailable")
            self.assertEqual(paused_actions["resume_admission"], "available")
            db.close()

    def test_control_exclude_item_moves_selected_items_and_is_audited(self):
        first_url = "https://fixture.test/first/data/item.bin"
        second_url = "https://fixture.test/first/data/other.bin"
        with tempfile.TemporaryDirectory() as temporary:
            downloader, db = self.prepare(Path(temporary), [first_url, second_url])
            downloader.scope_run(db)
            downloader.update(db, "UPDATE downloads SET status='retry_wait' WHERE url=?",
                              (first_url,))
            request = {"request_id": "request-one", "session_id": "session-one",
                      "parameters": {"item_ids": [downloader.item_id(first_url)]}}

            first = downloader.control_exclude_item(db, request)
            second = downloader.control_exclude_item(db, request)

            self.assertEqual(first["outcome"], second["outcome"])
            self.assertEqual(first["reason"], second["reason"])
            self.assertEqual(first["state_revision"], second["state_revision"])
            self.assertEqual(first["outcome"], "completed")
            self.assertEqual(first["items"][downloader.item_id(first_url)], "excluded")
            self.assertEqual(db.execute("SELECT status FROM downloads WHERE url=?",
                                        (first_url,)).fetchone()[0], "excluded")
            self.assertEqual(db.execute("SELECT status FROM downloads WHERE url=?",
                                        (second_url,)).fetchone()[0], "queued")
            self.assertEqual(db.execute("SELECT COUNT(*) FROM control_requests").fetchone()[0], 1)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM download_transitions "
                                        "WHERE url='__control__'").fetchone()[0], 1)
            db.close()

    def test_control_exclude_item_rejects_out_of_scope_and_inapplicable_ids(self):
        first_url = "https://fixture.test/first/data/item.bin"
        with tempfile.TemporaryDirectory() as temporary:
            downloader, db = self.prepare(Path(temporary), [first_url])
            downloader.scope_run(db)
            downloader.update(db, "UPDATE downloads SET status='complete' WHERE url=?",
                              (first_url,))
            request = {"request_id": "request-one", "session_id": "session-one",
                      "parameters": {"item_ids": [downloader.item_id(first_url), "missing-item"]}}

            response = downloader.control_exclude_item(db, request)

            self.assertEqual(response["outcome"], "completed")
            self.assertIn("not excludable", response["items"][downloader.item_id(first_url)])
            self.assertIn("outside the selected set", response["items"]["missing-item"])
            self.assertEqual(db.execute("SELECT status FROM downloads WHERE url=?",
                                        (first_url,)).fetchone()[0], "complete")
            db.close()

    def test_control_exclude_item_from_every_reachable_state_retains_error(self):
        for source_status in sorted(tod_dl.EXCLUDABLE_STATUSES):
            with self.subTest(source_status=source_status):
                target_url = "https://fixture.test/first/data/item.bin"
                other_url = "https://fixture.test/first/data/other.bin"
                with tempfile.TemporaryDirectory() as temporary:
                    downloader, db = self.prepare(Path(temporary), [target_url, other_url])
                    downloader.scope_run(db)
                    downloader.update(
                        db, "UPDATE downloads SET status=?, last_error='connection reset', "
                            "priority=2 WHERE url=?", (source_status, target_url))
                    other_status, other_priority = db.execute(
                        "SELECT status, priority FROM downloads WHERE url=?",
                        (other_url,)).fetchone()
                    other_rank = db.execute(
                        "SELECT queue_rank FROM run_items WHERE url=?", (other_url,)).fetchone()[0]
                    request = {"request_id": f"request-{source_status}", "session_id": "session-one",
                              "parameters": {"item_ids": [downloader.item_id(target_url)]}}

                    response = downloader.control_exclude_item(db, request)

                    self.assertEqual(response["outcome"], "completed")
                    self.assertEqual(response["items"][downloader.item_id(target_url)], "excluded")
                    row = db.execute("SELECT status, last_error FROM downloads WHERE url=?",
                                     (target_url,)).fetchone()
                    self.assertEqual(row[0], "excluded")
                    self.assertEqual(row[1], "connection reset")
                    self.assertEqual(
                        db.execute("SELECT status, priority FROM downloads WHERE url=?",
                                  (other_url,)).fetchone(), (other_status, other_priority))
                    self.assertEqual(
                        db.execute("SELECT queue_rank FROM run_items WHERE url=?",
                                  (other_url,)).fetchone()[0], other_rank)
                    db.close()

    def test_only_control_exclude_item_ever_sets_excluded_status(self):
        source = Path(tod_dl.__file__).read_text(encoding="utf-8")
        assignments = re.findall(r"""status\s*=\s*['"]excluded['"]""", source)
        self.assertEqual(len(assignments), 2)
        method_source = inspect.getsource(tod_dl.Downloader.control_exclude_item)
        self.assertEqual(len(re.findall(r"""status\s*=\s*['"]excluded['"]""", method_source)), 2)

    def test_control_exclude_item_rejects_malformed_item_ids(self):
        url = "https://fixture.test/first/data/item.bin"
        with tempfile.TemporaryDirectory() as temporary:
            downloader, db = self.prepare(Path(temporary), [url])
            downloader.scope_run(db)
            request = {"request_id": "request-one", "session_id": "session-one",
                      "parameters": {"item_ids": []}}

            response = downloader.control_exclude_item(db, request)

            self.assertEqual(response["outcome"], "rejected")
            self.assertIn("item_ids", response["reason"])
            db.close()

    def test_control_set_item_priority_sets_hint_and_is_audited(self):
        first_url = "https://fixture.test/first/data/item.bin"
        second_url = "https://fixture.test/first/data/other.bin"
        with tempfile.TemporaryDirectory() as temporary:
            downloader, db = self.prepare(Path(temporary), [first_url, second_url])
            downloader.scope_run(db)
            downloader.update(db, "UPDATE downloads SET status='retry_wait' WHERE url=?",
                              (first_url,))
            request = {"request_id": "request-one", "session_id": "session-one",
                      "parameters": {"item_ids": [downloader.item_id(first_url)], "priority": 3}}

            first = downloader.control_set_item_priority(db, request)
            second = downloader.control_set_item_priority(db, request)

            self.assertEqual(first["outcome"], second["outcome"])
            self.assertEqual(first["reason"], second["reason"])
            self.assertEqual(first["state_revision"], second["state_revision"])
            self.assertEqual(first["outcome"], "completed")
            self.assertEqual(first["items"][downloader.item_id(first_url)], "prioritized")
            self.assertEqual(db.execute("SELECT priority FROM downloads WHERE url=?",
                                        (first_url,)).fetchone()[0], 3)
            self.assertEqual(db.execute("SELECT priority FROM downloads WHERE url=?",
                                        (second_url,)).fetchone()[0], 0)
            self.assertEqual(db.execute("SELECT status FROM downloads WHERE url=?",
                                        (first_url,)).fetchone()[0], "retry_wait")
            queue_rank = db.execute("SELECT queue_rank FROM run_items WHERE url=?",
                                    (first_url,)).fetchone()[0]
            self.assertEqual(queue_rank, 0)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM control_requests").fetchone()[0], 1)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM download_transitions "
                                        "WHERE url='__control__'").fetchone()[0], 1)
            db.close()

    def test_control_set_item_priority_rejects_out_of_scope_and_inapplicable_ids(self):
        first_url = "https://fixture.test/first/data/item.bin"
        with tempfile.TemporaryDirectory() as temporary:
            downloader, db = self.prepare(Path(temporary), [first_url])
            downloader.scope_run(db)
            downloader.update(db, "UPDATE downloads SET status='complete' WHERE url=?",
                              (first_url,))
            request = {"request_id": "request-one", "session_id": "session-one",
                      "parameters": {"item_ids": [downloader.item_id(first_url), "missing-item"],
                                    "priority": 1}}

            response = downloader.control_set_item_priority(db, request)

            self.assertEqual(response["outcome"], "completed")
            self.assertIn("not prioritizable", response["items"][downloader.item_id(first_url)])
            self.assertIn("outside the selected set", response["items"]["missing-item"])
            self.assertEqual(db.execute("SELECT priority FROM downloads WHERE url=?",
                                        (first_url,)).fetchone()[0], 0)
            db.close()

    def test_control_set_item_priority_rejects_malformed_item_ids_and_bounds(self):
        url = "https://fixture.test/first/data/item.bin"
        with tempfile.TemporaryDirectory() as temporary:
            downloader, db = self.prepare(Path(temporary), [url])
            downloader.scope_run(db)
            missing_ids = {"request_id": "request-one", "session_id": "session-one",
                          "parameters": {"item_ids": [], "priority": 1}}

            response = downloader.control_set_item_priority(db, missing_ids)

            self.assertEqual(response["outcome"], "rejected")
            self.assertIn("item_ids", response["reason"])

            out_of_bounds = {"request_id": "request-two", "session_id": "session-one",
                             "parameters": {"item_ids": [downloader.item_id(url)],
                                           "priority": 99}}

            response = downloader.control_set_item_priority(db, out_of_bounds)

            self.assertEqual(response["outcome"], "rejected")
            self.assertIn("priority", response["reason"])
            db.close()

    def test_control_set_retry_cooldown_overrides_deadline_and_is_audited(self):
        first_url = "https://fixture.test/first/data/item.bin"
        second_url = "https://fixture.test/first/data/other.bin"
        with tempfile.TemporaryDirectory() as temporary:
            downloader, db = self.prepare(Path(temporary), [first_url, second_url])
            downloader.scope_run(db)
            downloader.update(db, "UPDATE downloads SET status='retry_wait', "
                              "next_retry_at=9999999999 WHERE url=?", (first_url,))
            request = {"request_id": "request-one", "session_id": "session-one",
                      "parameters": {"item_ids": [downloader.item_id(first_url)],
                                    "cooldown_s": 120}}

            first = downloader.control_set_retry_cooldown(db, request)
            second = downloader.control_set_retry_cooldown(db, request)

            self.assertEqual(first["outcome"], second["outcome"])
            self.assertEqual(first["reason"], second["reason"])
            self.assertEqual(first["state_revision"], second["state_revision"])
            self.assertEqual(first["outcome"], "completed")
            self.assertEqual(first["items"][downloader.item_id(first_url)], "cooldown set")
            new_retry_at = db.execute("SELECT next_retry_at FROM downloads WHERE url=?",
                                      (first_url,)).fetchone()[0]
            self.assertLess(new_retry_at, 9999999999)
            self.assertGreater(new_retry_at, time.time())
            self.assertEqual(db.execute("SELECT next_retry_at FROM downloads WHERE url=?",
                                        (second_url,)).fetchone()[0], 0)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM control_requests").fetchone()[0], 1)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM download_transitions "
                                        "WHERE url='__control__'").fetchone()[0], 1)
            db.close()

    def test_control_set_retry_cooldown_rejects_out_of_scope_and_inapplicable_ids(self):
        first_url = "https://fixture.test/first/data/item.bin"
        with tempfile.TemporaryDirectory() as temporary:
            downloader, db = self.prepare(Path(temporary), [first_url])
            downloader.scope_run(db)
            downloader.update(db, "UPDATE downloads SET status='complete' WHERE url=?",
                              (first_url,))
            request = {"request_id": "request-one", "session_id": "session-one",
                      "parameters": {"item_ids": [downloader.item_id(first_url), "missing-item"],
                                    "cooldown_s": 60}}

            response = downloader.control_set_retry_cooldown(db, request)

            self.assertEqual(response["outcome"], "completed")
            self.assertIn("not cooldown-eligible", response["items"][downloader.item_id(first_url)])
            self.assertIn("outside the selected set", response["items"]["missing-item"])
            self.assertEqual(db.execute("SELECT next_retry_at FROM downloads WHERE url=?",
                                        (first_url,)).fetchone()[0], 0)
            db.close()

    def test_control_set_retry_cooldown_rejects_malformed_item_ids_and_bounds(self):
        url = "https://fixture.test/first/data/item.bin"
        with tempfile.TemporaryDirectory() as temporary:
            downloader, db = self.prepare(Path(temporary), [url])
            downloader.scope_run(db)
            missing_ids = {"request_id": "request-one", "session_id": "session-one",
                          "parameters": {"item_ids": [], "cooldown_s": 60}}

            response = downloader.control_set_retry_cooldown(db, missing_ids)

            self.assertEqual(response["outcome"], "rejected")
            self.assertIn("item_ids", response["reason"])

            out_of_bounds = {"request_id": "request-two", "session_id": "session-one",
                             "parameters": {"item_ids": [downloader.item_id(url)],
                                           "cooldown_s": 99999}}

            response = downloader.control_set_retry_cooldown(db, out_of_bounds)

            self.assertEqual(response["outcome"], "rejected")
            self.assertIn("cooldown_s", response["reason"])
            db.close()

    def test_control_wake_interrupts_active_transfer_wait(self):
        with tempfile.TemporaryDirectory() as temporary:
            queue = Path(temporary) / "queue.txt"
            queue.write_text("", encoding="utf-8")
            downloader = Downloader(make_args(Path(temporary), queue))

            self.assertEqual(downloader.admission_wait_timeout(), ADMISSION_POLL_SECONDS)
            downloader.control_wake.set()
            self.assertEqual(downloader.admission_wait_timeout(), 0)
            self.assertFalse(downloader.control_wake.is_set())

    def test_resume_requeues_an_interrupted_active_transfer(self):
        url = "https://fixture.test/first/data/item.bin"
        with tempfile.TemporaryDirectory() as temporary:
            downloader, db = self.prepare(Path(temporary), [url])
            downloader.scope_run(db)
            downloader.update(db, "UPDATE downloads SET status='active', next_retry_at=99 "
                              "WHERE url=?", (url,))

            downloader.requeue_interrupted_transfers(db)
            db.commit()

            self.assertEqual(downloader.next_pending(db)[0], url)
            self.assertEqual(db.execute("SELECT status FROM downloads WHERE url=?", (url,))
                             .fetchone()[0], "queued")
            db.close()

    def test_resume_terminates_a_surviving_writer_before_requeuing(self):
        """A crashed controller can leave an orphaned aria2 writer running.

        Requeuing the item without checking for it would let a new worker
        start a second writer against the same staging partial.
        """
        import subprocess
        url = "https://fixture.test/first/data/item.bin"
        with tempfile.TemporaryDirectory() as temporary:
            downloader, db = self.prepare(Path(temporary), [url])
            downloader.scope_run(db)
            downloader.update(db, "UPDATE downloads SET status='active', next_retry_at=99 "
                              "WHERE url=?", (url,))
            downloader.run_dir.mkdir(parents=True)
            downloader.manifest = {"workers": []}
            survivor = subprocess.Popen(["sleep", "5"])
            try:
                downloader.record_worker(url, survivor.pid)

                downloader.requeue_interrupted_transfers(db)
                db.commit()

                survivor.wait(timeout=5)
                self.assertEqual(survivor.returncode, -signal.SIGTERM)
                self.assertEqual(db.execute("SELECT status FROM downloads WHERE url=?",
                                            (url,)).fetchone()[0], "queued")
            finally:
                if survivor.poll() is None:
                    survivor.kill()
                    survivor.wait()
            db.close()

    def test_resume_does_not_treat_a_recycled_pid_as_a_survivor(self):
        url = "https://fixture.test/first/data/item.bin"
        with tempfile.TemporaryDirectory() as temporary:
            downloader, db = self.prepare(Path(temporary), [url])
            downloader.scope_run(db)
            downloader.update(db, "UPDATE downloads SET status='active', next_retry_at=99 "
                              "WHERE url=?", (url,))
            downloader.run_dir.mkdir(parents=True)
            downloader.manifest = {"workers": [
                {"url": url, "torsocks_pid": os.getpid(), "started_ticks": -1}]}
            downloader.write_manifest()

            downloader.requeue_interrupted_transfers(db)
            db.commit()

            self.assertEqual(db.execute("SELECT status FROM downloads WHERE url=?",
                                        (url,)).fetchone()[0], "queued")
            db.close()

    def test_existing_run_id_does_not_expand_when_queue_changes(self):
        initial = ["https://fixture.test/first/data/one.bin"]
        expanded = initial + ["https://fixture.test/second/data/two.bin"]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            downloader, db = self.prepare(root, initial)
            self.assertEqual(downloader.scope_run(db), (1, 0))
            queue = downloader.args.queue[0]
            queue.write_text("\n".join(expanded) + "\n", encoding="utf-8")
            downloader.import_queues(db)

            self.assertEqual(downloader.scope_run(db), (1, 0))
            rows = db.execute("SELECT url FROM run_items WHERE run_id='test-run'").fetchall()
            self.assertEqual(rows, [(initial[0],)])
            db.close()

    def test_scope_run_does_not_demote_a_row_already_tracked_as_terminal(self):
        """A second scope_run() over a final file that a prior run already
        settled to a terminal status must leave that status alone, instead
        of re-treating the file's presence as a brand-new collision."""
        url = "https://fixture.test/first/data/item.bin"
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            downloader, db = self.prepare(root, [url])
            target = downloader.destination / "first" / "data" / "item.bin"
            target.parent.mkdir(parents=True)
            target.write_bytes(b"evidence")
            for status in ("complete", "existing_unverified", "review_required",
                           "excluded", "unavailable"):
                db.execute("UPDATE downloads SET status=? WHERE url=?", (status, url))
                db.commit()
                db.execute("DELETE FROM run_items WHERE run_id='test-run'")
                db.commit()
                downloader.run_id = f"test-run-{status}"

                selected, existing = downloader.scope_run(db)

                self.assertEqual((selected, existing), (0, 1))
                self.assertEqual(db.execute("SELECT status FROM downloads WHERE url=?",
                                            (url,)).fetchone()[0], status)
            db.close()

    def test_open_db_migrates_legacy_pending_status_to_queued(self):
        url = "https://fixture.test/first/data/legacy.bin"
        with tempfile.TemporaryDirectory() as temporary:
            downloader, db = self.prepare(Path(temporary), [url])
            db.execute("UPDATE downloads SET status='pending' WHERE url=?", (url,))
            db.commit()
            db.close()
            db = downloader.open_db()
            self.assertEqual(db.execute("SELECT status FROM downloads WHERE url=?",
                                        (url,)).fetchone()[0], "queued")
            db.close()

    def test_next_pending_preserves_input_order_not_path_order(self):
        urls = [
            "https://fixture.test/first/data/z-last-path.bin",
            "https://fixture.test/second/data/a-first-path.bin",
        ]
        with tempfile.TemporaryDirectory() as temporary:
            downloader, db = self.prepare(Path(temporary), urls)
            downloader.scope_run(db)

            self.assertEqual(downloader.next_pending(db)[0], urls[0])
            downloader.update(db, "UPDATE downloads SET status='admitted' WHERE url=?",
                              (urls[0],))
            self.assertEqual(downloader.next_pending(db)[0], urls[1])
            db.close()

    def test_dry_run_excludes_existing_from_the_selection_limit(self):
        urls = [
            "https://fixture.test/first/data/existing.bin",
            "https://fixture.test/second/data/selected.bin",
            "https://fixture.test/third/data/not-selected.bin",
        ]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            downloader, db = self.prepare(root, urls, max_files=1)
            db.close()
            existing = downloader.destination / "first" / "data" / "existing.bin"
            existing.parent.mkdir(parents=True)
            existing.write_bytes(b"evidence")

            output = StringIO()
            with redirect_stdout(output):
                self.assertEqual(downloader.dry_run(), 0)

            self.assertIn("existing.bin", output.getvalue())
            self.assertIn("selected.bin", output.getvalue())
            self.assertNotIn("not-selected.bin", output.getvalue())

    def test_storage_mapping_respects_destination_name_limit(self):
        with tempfile.TemporaryDirectory() as temporary:
            destination = Path(temporary)
            name_max = os.pathconf(destination, "PC_NAME_MAX")
            component = "x" * (name_max + 1)

            stored = storage_relative(Path(component), destination)

            self.assertTrue(stored.name.startswith("__longname__"))

    def test_import_remaps_legacy_unsafe_storage_path(self):
        url = "https://fixture.test/first/data/" + "x" * 300
        with tempfile.TemporaryDirectory() as temporary:
            downloader, db = self.prepare(Path(temporary), [url])
            db.execute("UPDATE downloads SET storage_path=? WHERE url=?",
                       ("first/data/" + "x" * 300, url))
            db.commit()

            downloader.import_queues(db)

            stored = db.execute("SELECT storage_path FROM downloads WHERE url=?", (url,))
            self.assertTrue(stored.fetchone()[0].endswith("__longname__" +
                                                           hashlib.sha256(
                                                               ("x" * 300).encode()
                                                           ).hexdigest()))
            db.close()

    def test_promotion_records_intent_before_staging_cleanup(self):
        url = "https://fixture.test/first/data/item.bin"
        with tempfile.TemporaryDirectory() as temporary:
            downloader, db = self.prepare(Path(temporary), [url])
            downloader.scope_run(db)
            staging = downloader.staging_path(url)
            staging.parent.mkdir(parents=True)
            staging.write_bytes(b"fixture bytes")
            downloader.run_aria2 = lambda *_: (True, "complete")

            self.assertEqual(downloader.transfer(downloader.next_pending(db), db), "complete")

            states = [row[0] for row in db.execute(
                "SELECT to_status FROM download_transitions WHERE url=? ORDER BY id", (url,)
            )]
            self.assertIn("promoting", states)
            self.assertEqual(states[-2:], ["complete", "complete"])
            self.assertFalse(staging.exists())
            db.close()

    def test_admitted_transfer_reserves_a_worker_before_cooldown(self):
        url = "https://fixture.test/first/data/item.bin"
        with tempfile.TemporaryDirectory() as temporary:
            downloader, db = self.prepare(Path(temporary), [url])
            downloader.scope_run(db)
            observed_slots = []

            def observe_cooldown():
                observed_slots.append((dict(downloader.active), dict(downloader.worker_ids)))

            downloader.wait_for_cooldown = observe_cooldown
            downloader.run_aria2 = lambda *_: (True, "complete")
            staging = downloader.staging_path(url)
            staging.parent.mkdir(parents=True)
            staging.write_bytes(b"fixture bytes")

            self.assertEqual(downloader.transfer(downloader.next_pending(db), db), "complete")

            self.assertEqual(len(observed_slots), 2)
            for active, workers in observed_slots:
                self.assertEqual(active, {url: staging})
                self.assertEqual(workers, {url: 1})
            db.close()

    def test_promotion_os_error_requires_review_and_preserves_staging(self):
        url = "https://fixture.test/first/data/item.bin"
        with tempfile.TemporaryDirectory() as temporary:
            downloader, db = self.prepare(Path(temporary), [url])
            downloader.scope_run(db)
            staging = downloader.staging_path(url)
            staging.parent.mkdir(parents=True)
            staging.write_bytes(b"fixture bytes")
            downloader.run_aria2 = lambda *_: (True, "complete")
            original_link = os.link

            def fail_link(source, target):
                raise OSError(errno.ENAMETOOLONG, "File name too long", target)

            os.link = fail_link
            try:
                self.assertEqual(downloader.transfer(downloader.next_pending(db), db), "review")
            finally:
                os.link = original_link

            self.assertTrue(staging.exists())
            self.assertEqual(db.execute("SELECT status FROM downloads WHERE url=?", (url,))
                             .fetchone()[0], "review_required")
            self.assertEqual(db.execute("SELECT review_code FROM downloads WHERE url=?", (url,))
                             .fetchone()[0], "ENAMETOOLONG")
            db.close()

    def test_safe_path_remediation_promotes_verified_selected_legacy_item(self):
        component = "x" * 300
        url = "https://fixture.test/first/data/" + component
        payload = b"preserved fixture bytes"
        with tempfile.TemporaryDirectory() as temporary:
            downloader, db = self.prepare(Path(temporary), [url])
            downloader.scope_run(db)
            staging = downloader.staging_path(url)
            staging.parent.mkdir(parents=True)
            staging.write_bytes(payload)
            db.execute("UPDATE downloads SET status='review_required', sha256=?, "
                       "last_error=?, promotion_target=? WHERE url=?",
                       (hashlib.sha256(payload).hexdigest(),
                        "promotion reconciliation failed: [Errno 36] File name too long",
                        str(downloader.destination / "first" / "data" / component), url))
            db.commit()

            self.assertEqual(downloader.remediate_safe_paths(db), 1)

            stored, target = downloader.safe_remediation_target(
                "first/data/" + component)
            self.assertTrue(target.is_file())
            self.assertEqual(target.read_bytes(), payload)
            self.assertFalse(staging.exists())
            row = db.execute("SELECT status, storage_path, review_code, remediation_reason, "
                             "remediation_mapping_version, remediation_outcome FROM downloads "
                             "WHERE url=?", (url,)).fetchone()
            self.assertEqual(row, ("complete", stored.as_posix(), "ENAMETOOLONG",
                                   "automatic safe-path remediation", "v1", "complete"))
            transitions = [entry[0] for entry in db.execute(
                "SELECT detail FROM download_transitions WHERE url=? ORDER BY id", (url,)
            )]
            self.assertIn("automatic safe-path remediation intent", transitions)
            self.assertIn("automatic safe-path remediation complete", transitions)
            db.close()

    def test_safe_path_remediation_preserves_staging_on_mapped_collision(self):
        component = "x" * 300
        url = "https://fixture.test/first/data/" + component
        payload = b"preserved fixture bytes"
        with tempfile.TemporaryDirectory() as temporary:
            downloader, db = self.prepare(Path(temporary), [url])
            downloader.scope_run(db)
            staging = downloader.staging_path(url)
            staging.parent.mkdir(parents=True)
            staging.write_bytes(payload)
            _, target = downloader.safe_remediation_target("first/data/" + component)
            target.parent.mkdir(parents=True)
            target.write_bytes(b"existing evidence")
            db.execute("UPDATE downloads SET status='review_required', sha256=?, "
                       "review_code='ENAMETOOLONG' WHERE url=?",
                       (hashlib.sha256(payload).hexdigest(), url))
            db.commit()

            self.assertEqual(downloader.remediate_safe_paths(db), 0)

            self.assertTrue(staging.exists())
            self.assertEqual(target.read_bytes(), b"existing evidence")
            self.assertEqual(db.execute("SELECT status, remediation_outcome FROM downloads "
                                        "WHERE url=?", (url,)).fetchone(),
                             ("review_required", "manual_collision"))
            db.close()

    def test_safe_path_remediation_does_not_mutate_unselected_review_item(self):
        component = "x" * 300
        url = "https://fixture.test/first/data/" + component
        payload = b"preserved fixture bytes"
        with tempfile.TemporaryDirectory() as temporary:
            downloader, db = self.prepare(Path(temporary), [url])
            staging = downloader.staging_path(url)
            staging.parent.mkdir(parents=True)
            staging.write_bytes(payload)
            db.execute("UPDATE downloads SET status='review_required', sha256=?, "
                       "review_code='ENAMETOOLONG' WHERE url=?",
                       (hashlib.sha256(payload).hexdigest(), url))
            db.commit()

            self.assertEqual(downloader.remediate_safe_paths(db), 0)

            self.assertTrue(staging.exists())
            self.assertEqual(db.execute("SELECT status FROM downloads WHERE url=?", (url,))
                             .fetchone()[0], "review_required")
            db.close()

    def test_reconcile_promotion_marks_matching_final_complete(self):
        url = "https://fixture.test/first/data/item.bin"
        with tempfile.TemporaryDirectory() as temporary:
            downloader, db = self.prepare(Path(temporary), [url])
            target = downloader.destination / "first" / "data" / "item.bin"
            target.parent.mkdir(parents=True)
            target.write_bytes(b"fixture bytes")
            downloader.update(db, "UPDATE downloads SET status='promoting', sha256=?, "
                              "promotion_target=? WHERE url=?",
                              (hashlib.sha256(b"fixture bytes").hexdigest(),
                               str(target), url))

            downloader.reconcile_promotions(db)

            self.assertEqual(db.execute("SELECT status FROM downloads WHERE url=?", (url,))
                             .fetchone()[0], "complete")
            db.close()

    def test_reconcile_promotion_recreates_link_when_target_never_created(self):
        # Reproduces E06 failpoint (a): the durable promoting record and its
        # digest committed, but the process was killed before os.link()
        # created the final file. The digest is already trustworthy, so
        # reconciliation must complete automatically instead of discarding it
        # into review_required.
        url = "https://fixture.test/first/data/item.bin"
        payload = b"fixture bytes"
        with tempfile.TemporaryDirectory() as temporary:
            downloader, db = self.prepare(Path(temporary), [url])
            staging = downloader.staging_path(url)
            staging.parent.mkdir(parents=True)
            staging.write_bytes(payload)
            target = downloader.destination / "first" / "data" / "item.bin"
            # ensure_safe_parent() creates this directory before promoting is
            # ever recorded, so the parent must already exist here too.
            target.parent.mkdir(parents=True)
            downloader.update(db, "UPDATE downloads SET status='promoting', sha256=?, "
                              "staging_path=?, promotion_target=? WHERE url=?",
                              (hashlib.sha256(payload).hexdigest(), str(staging),
                               str(target), url))

            downloader.reconcile_promotions(db)

            self.assertEqual(db.execute("SELECT status FROM downloads WHERE url=?", (url,))
                             .fetchone()[0], "complete")
            self.assertEqual(target.read_bytes(), payload)
            self.assertFalse(staging.exists())
            db.close()

    def test_reconcile_promotion_with_oversized_target_requires_review(self):
        url = "https://fixture.test/first/data/item.bin"
        with tempfile.TemporaryDirectory() as temporary:
            downloader, db = self.prepare(Path(temporary), [url])
            oversized_target = downloader.destination / ("x" * 300)
            downloader.update(db, "UPDATE downloads SET status='promoting', sha256=?, "
                              "promotion_target=? WHERE url=?",
                              (hashlib.sha256(b"fixture bytes").hexdigest(),
                               str(oversized_target), url))

            downloader.reconcile_promotions(db)

            self.assertEqual(db.execute("SELECT status FROM downloads WHERE url=?", (url,))
                             .fetchone()[0], "review_required")
            db.close()

    def test_reconcile_completed_staging_cleanup_is_safe_to_repeat(self):
        url = "https://fixture.test/first/data/item.bin"
        payload = b"fixture bytes"
        with tempfile.TemporaryDirectory() as temporary:
            downloader, db = self.prepare(Path(temporary), [url])
            downloader.scope_run(db)
            target = downloader.destination / "first" / "data" / "item.bin"
            target.parent.mkdir(parents=True)
            target.write_bytes(payload)
            staging = downloader.staging_path(url)
            staging.parent.mkdir(parents=True)
            staging.write_bytes(payload)
            downloader.update(db, "UPDATE downloads SET status='complete', sha256=?, "
                              "bytes=? WHERE url=?",
                              (hashlib.sha256(payload).hexdigest(), len(payload), url))

            downloader.cleanup_completed_staging(db)
            downloader.cleanup_completed_staging(db)

            self.assertEqual(target.read_bytes(), payload)
            self.assertFalse(staging.exists())
            self.assertIsNotNone(db.execute("SELECT cleanup_completed_at FROM downloads "
                                             "WHERE url=?", (url,)).fetchone()[0])
            states = [row[0] for row in db.execute(
                "SELECT to_status FROM download_transitions WHERE url=? ORDER BY id", (url,)
            )]
            self.assertEqual(states.count("complete"), 1)
            db.close()

    def test_reconcile_does_not_hash_a_final_outside_destination(self):
        url = "https://fixture.test/first/data/item.bin"
        payload = b"fixture bytes"
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            downloader, db = self.prepare(root, [url])
            outside = root / "outside.bin"
            outside.write_bytes(payload)
            downloader.update(db, "UPDATE downloads SET status='promoting', sha256=?, "
                              "promotion_target=? WHERE url=?",
                              (hashlib.sha256(payload).hexdigest(), str(outside), url))

            downloader.reconcile_promotions(db)

            self.assertEqual(outside.read_bytes(), payload)
            self.assertEqual(db.execute("SELECT status FROM downloads WHERE url=?", (url,))
                             .fetchone()[0], "review_required")
            db.close()

    def test_completed_staging_cleanup_does_not_remove_an_outside_file(self):
        url = "https://fixture.test/first/data/item.bin"
        payload = b"fixture bytes"
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            downloader, db = self.prepare(root, [url])
            downloader.scope_run(db)
            outside = root / "outside.bin"
            outside.write_bytes(payload)
            downloader.update(db, "UPDATE downloads SET status='complete', staging_path=? "
                              "WHERE url=?", (str(outside), url))

            downloader.cleanup_completed_staging(db)

            self.assertEqual(outside.read_bytes(), payload)
            self.assertEqual(db.execute("SELECT status FROM downloads WHERE url=?", (url,))
                             .fetchone()[0], "review_required")
            db.close()


class FaultRoot:
    """Keep fault artifacts only when the test body raises an exception."""

    def __enter__(self):
        self.path = Path(tempfile.mkdtemp(prefix="tod-dl-fault-"))
        return self.path

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is not None:
            print(f"Retained fault-test artifacts: {self.path}")
            return False
        shutil.rmtree(self.path)
        return False


class LocalFakeTransferEngine:
    """A local fixture engine that never starts a process or requests a source."""

    def __init__(self, payload: bytes):
        self.payload = payload
        self.calls = 0

    def partial(self, staging: Path) -> None:
        staging.parent.mkdir(parents=True, exist_ok=True)
        staging.write_bytes(self.payload[:len(self.payload) // 2])
        Path(str(staging) + ".aria2").write_text("resume metadata", encoding="utf-8")

    def complete(self, staging: Path) -> tuple[bool, str]:
        self.calls += 1
        staging.parent.mkdir(parents=True, exist_ok=True)
        staging.write_bytes(self.payload)
        Path(str(staging) + ".aria2").unlink(missing_ok=True)
        return True, "local fixture complete"

    def terminal_failure(self, staging: Path) -> tuple[bool, str]:
        self.calls += 1
        staging.parent.mkdir(parents=True, exist_ok=True)
        staging.write_bytes(self.payload[:3])
        return False, "local fixture terminal failure"

    def stalled(self, staging: Path) -> tuple[bool, str]:
        self.calls += 1
        staging.parent.mkdir(parents=True, exist_ok=True)
        staging.write_bytes(b"")
        return False, "local fixture stalled with zero progress"

    def incomplete_body(self, staging: Path) -> tuple[bool, str]:
        self.calls += 1
        staging.parent.mkdir(parents=True, exist_ok=True)
        staging.write_bytes(self.payload[:len(self.payload) // 2])
        return False, "errorCode=1 Got EOF from the server"

    def changed_representation(self, staging: Path) -> tuple[bool, str]:
        self.calls += 1
        staging.parent.mkdir(parents=True, exist_ok=True)
        staging.write_bytes(self.payload + b" changed representation")
        Path(str(staging) + ".aria2").unlink(missing_ok=True)
        return True, "local fixture changed representation"


class FaultRootTests(unittest.TestCase):
    """Fault artifacts stay outside the repository and survive only a failed test."""

    def test_root_is_removed_after_a_passing_test(self):
        with FaultRoot() as root:
            (root / "artifact").write_bytes(b"x")
        self.assertFalse(root.exists())

    def test_root_is_kept_and_its_path_printed_after_a_failing_test(self):
        output = StringIO()
        with redirect_stdout(output):
            with self.assertRaises(RuntimeError):
                with FaultRoot() as root:
                    raise RuntimeError("fixture failure")
        try:
            self.assertTrue(root.is_dir())
            self.assertIn(str(root), output.getvalue())
        finally:
            shutil.rmtree(root)

    def test_root_is_outside_the_repository_and_its_protected_directories(self):
        repository = Path(__file__).resolve().parents[1]
        with FaultRoot() as root:
            self.assertNotIn(repository, root.resolve().parents)
            for name in ("downloaded_files", "archive", "pst_extracted", "msg_extracted", "output"):
                self.assertNotIn(repository / name, root.resolve().parents)


class FailpointGatingTests(unittest.TestCase):
    """TOD_DL_FAILPOINT must gate exactly the named boundary, for E06."""

    def test_only_the_matching_failpoint_exits(self):
        original_exit = os.environ.get("TOD_DL_FAILPOINT")
        original_os_exit = tod_dl.os._exit
        calls = []
        tod_dl.os._exit = lambda code: calls.append(code)
        try:
            os.environ["TOD_DL_FAILPOINT"] = "post_final_file_creation"
            hit_failpoint("post_validation_intent")
            self.assertEqual(calls, [])
            hit_failpoint("post_final_file_creation")
            self.assertEqual(calls, [70])
            del os.environ["TOD_DL_FAILPOINT"]
            hit_failpoint("post_completion_commit")
            self.assertEqual(calls, [70])
        finally:
            tod_dl.os._exit = original_os_exit
            if original_exit is None:
                os.environ.pop("TOD_DL_FAILPOINT", None)
            else:
                os.environ["TOD_DL_FAILPOINT"] = original_exit

    def test_unknown_failpoint_name_is_rejected(self):
        with self.assertRaises(AssertionError):
            hit_failpoint("not_a_real_failpoint")

    def test_all_three_failpoints_are_named_distinctly(self):
        self.assertEqual(FAILPOINTS, {"post_validation_intent", "post_final_file_creation",
                                      "post_completion_commit"})


class AcquisitionFaultRecoveryTests(unittest.TestCase):
    """Restart recovery tests with only temporary files and a local fake engine."""

    payload = b"fault-recovery fixture bytes"
    url = "https://fixture.test/first/data/item.bin"

    def make_controller(self, root: Path, urls=None, max_files: int = 1):
        queue = root / "queue.txt"
        queue.write_text("\n".join(urls or [self.url]) + "\n", encoding="utf-8")
        downloader = Downloader(make_args(root, queue, max_files))
        downloader.destination.mkdir(exist_ok=True)
        downloader.state.mkdir(exist_ok=True)
        db = downloader.open_db()
        downloader.import_queues(db)
        downloader.scope_run(db)
        return downloader, db, queue

    def restart(self, root: Path, queue: Path, max_files: int = 1):
        downloader = Downloader(make_args(root, queue, max_files))
        db = downloader.open_db()
        downloader.import_queues(db)
        downloader.scope_run(db)
        return downloader, db

    def test_access_denied_item_pauses_only_its_own_origin_until_it_is_retried(self):
        denied = "https://denied.test/first/data/a.bin"
        same_origin = "https://denied.test/first/data/b.bin"
        other_origin = "https://open.test/first/data/c.bin"
        with FaultRoot() as root:
            downloader, db, queue = self.make_controller(
                root, [denied, same_origin, other_origin], max_files=3)
            downloader.update(db, "UPDATE downloads SET status='review_required', "
                              "review_code='access_denied' WHERE url=?", (denied,))
            db.commit()
            self.assertEqual(downloader.next_pending(db)[0], other_origin)
            downloader.update(db, "UPDATE downloads SET status='complete' WHERE url=?",
                              (other_origin,))
            db.commit()
            self.assertIsNone(downloader.next_pending(db))
            self.assertIsNone(downloader.retry_wait(db))
            downloader.update(db, "UPDATE downloads SET status='queued', review_code=NULL "
                              "WHERE url=?", (denied,))
            db.commit()
            self.assertEqual(downloader.next_pending(db)[0], denied)
            db.close()

    def test_retry_access_denied_action_lifts_the_origin_pause_and_rejects_other_states(self):
        denied = "https://denied.test/first/data/a.bin"
        same_origin = "https://denied.test/first/data/b.bin"
        with FaultRoot() as root:
            downloader, db, queue = self.make_controller(root, [denied, same_origin], max_files=2)
            downloader.update(db, "UPDATE downloads SET status='review_required', attempts=2, "
                              "review_code='access_denied', last_error='status=403' WHERE url=?",
                              (denied,))
            db.commit()
            self.assertIsNone(downloader.next_pending(db))
            denied_id = downloader.item_id(denied)
            queued_id = downloader.item_id(same_origin)
            response = downloader.control_retry_access_denied(db, {
                "request_id": "req-1", "session_id": "sess-1",
                "parameters": {"item_ids": [denied_id, queued_id, "missing"]},
            })
            self.assertEqual(response["outcome"], "completed")
            self.assertEqual(response["items"][denied_id], "returned to queued")
            self.assertIn("rejected: item is in queued", response["items"][queued_id])
            self.assertIn("outside the selected set", response["items"]["missing"])
            row = db.execute("SELECT status, review_code, last_error, attempts FROM downloads "
                             "WHERE url=?", (denied,)).fetchone()
            self.assertEqual(row, ("queued", None, None, 2))
            self.assertIsNotNone(downloader.next_pending(db))
            replay = downloader.control_retry_access_denied(db, {
                "request_id": "req-1", "session_id": "sess-1",
                "parameters": {"item_ids": [denied_id]},
            })
            self.assertEqual(replay["state_revision"], response["state_revision"])
            db.close()

    def test_retry_access_denied_action_rejects_other_review_codes(self):
        with FaultRoot() as root:
            downloader, db, _ = self.make_controller(root)
            downloader.transition(db, self.url, "review_required", "changed remote representation",
                                  bytes=None, review_code="changed_remote_representation")
            item_id = downloader.item_id(self.url)
            response = downloader.control_retry_access_denied(db, {
                "request_id": "req-2", "session_id": "sess-1",
                "parameters": {"item_ids": [item_id]},
            })
            self.assertIn("not an access-denied review", response["items"][item_id])
            self.assertEqual(db.execute("SELECT status, review_code FROM downloads WHERE url=?",
                                        (self.url,)).fetchone(),
                             ("review_required", "changed_remote_representation"))
            db.close()

    def selected_urls(self, db):
        return [row[0] for row in db.execute(
            "SELECT url FROM run_items WHERE run_id='test-run' ORDER BY queue_rank"
        )]

    def transition_states(self, db):
        return [row[0] for row in db.execute(
            "SELECT to_status FROM download_transitions WHERE url=? ORDER BY id", (self.url,))]

    def assert_final(self, downloader, db, payload=None):
        payload = self.payload if payload is None else payload
        target = downloader.destination / "first" / "data" / "item.bin"
        self.assertEqual(target.read_bytes(), payload)
        self.assertEqual(sha256sum(target), hashlib.sha256(payload).hexdigest())
        self.assertEqual(db.execute("SELECT status, sha256 FROM downloads WHERE url=?",
                                    (self.url,)).fetchone(),
                         ("complete", hashlib.sha256(payload).hexdigest()))

    def test_before_attempt_state_commits_requeues_only_selected_item(self):
        with FaultRoot() as root:
            downloader, db, queue = self.make_controller(root)
            downloader.update(db, "UPDATE downloads SET status='admitted' WHERE url=?",
                              (self.url,))
            db.close()  # Stop at the named failure point before the attempt transition.

            restarted, db = self.restart(root, queue)
            restarted.requeue_interrupted_transfers(db)
            self.assertEqual(restarted.next_pending(db)[0], self.url)
            self.assertEqual(self.selected_urls(db), [self.url])
            self.assertFalse((restarted.destination / "first" / "data" / "item.bin").exists())
            db.close()

    def test_during_active_partial_transfer_preserves_resume_files(self):
        with FaultRoot() as root:
            downloader, db, queue = self.make_controller(root)
            staging = downloader.staging_path(self.url)
            engine = LocalFakeTransferEngine(self.payload)
            engine.partial(staging)
            downloader.transition(db, self.url, "active", attempts=1)
            db.close()

            restarted, db = self.restart(root, queue)
            restarted.requeue_interrupted_transfers(db)
            self.assertEqual(staging.read_bytes(), self.payload[:len(self.payload) // 2])
            self.assertTrue(Path(str(staging) + ".aria2").is_file())
            self.assertEqual(restarted.next_pending(db)[0], self.url)
            self.assertEqual(self.selected_urls(db), [self.url])
            db.close()

    def test_after_engine_success_before_hashing_rechecks_staging(self):
        with FaultRoot() as root:
            downloader, db, queue = self.make_controller(root)
            engine = LocalFakeTransferEngine(self.payload)
            staging = downloader.staging_path(self.url)
            engine.complete(staging)
            downloader.transition(db, self.url, "active", attempts=1)
            db.close()

            restarted, db = self.restart(root, queue)
            restarted.requeue_interrupted_transfers(db)
            restarted.run_aria2 = lambda *_: engine.complete(staging)
            self.assertEqual(restarted.transfer(restarted.next_pending(db), db), "complete")
            self.assert_final(restarted, db)
            self.assertEqual(self.selected_urls(db), [self.url])
            db.close()

    def test_unconfirmed_representation_on_resume_enters_review_without_retrying(self):
        # Spec: a resumed transfer that had a recorded validator but cannot
        # reconfirm it now has neither reliable version protection nor an
        # expected checksum, so it must move to review_required rather than
        # blindly retry into the same partial bytes.
        with FaultRoot() as root:
            downloader, db, _ = self.make_controller(root)
            downloader.transition(db, self.url, "queued", staging_generation=1,
                                  etag='"known-etag"')
            engine = LocalFakeTransferEngine(self.payload)
            staging = downloader.staging_path(self.url)
            engine.partial(staging)
            downloader.run_aria2 = lambda *_: engine.changed_representation(staging)
            downloader.probe_representation = lambda url: None

            self.assertEqual(downloader.transfer(downloader.next_pending(db), db), "review")
            self.assertEqual(engine.calls, 0)
            row = db.execute(
                "SELECT status, review_code, bytes FROM downloads WHERE url=?",
                (self.url,)).fetchone()
            self.assertEqual(row, ("review_required", "no_reliable_version_protection", None))
            db.close()

    def test_changed_etag_on_resume_enters_review_without_retrying(self):
        with FaultRoot() as root:
            downloader, db, _ = self.make_controller(root)
            downloader.transition(db, self.url, "queued", staging_generation=1, etag='"old-etag"')
            engine = LocalFakeTransferEngine(self.payload)
            staging = downloader.staging_path(self.url)
            engine.partial(staging)
            downloader.run_aria2 = lambda *_: engine.changed_representation(staging)
            downloader.probe_representation = lambda url: {"etag": '"new-etag"', "last_modified": None}

            self.assertEqual(downloader.transfer(downloader.next_pending(db), db), "review")
            self.assertEqual(engine.calls, 0)
            row = db.execute(
                "SELECT status, review_code, bytes, etag FROM downloads WHERE url=?",
                (self.url,)).fetchone()
            self.assertEqual(row, ("review_required", "changed_remote_representation", None, '"new-etag"'))
            db.close()

    def test_confirmed_unchanged_etag_resumes_the_existing_partial(self):
        with FaultRoot() as root:
            downloader, db, _ = self.make_controller(root)
            downloader.transition(db, self.url, "queued", staging_generation=1, etag='"same-etag"')
            engine = LocalFakeTransferEngine(self.payload)
            staging = downloader.staging_path(self.url)
            engine.partial(staging)
            downloader.run_aria2 = lambda *_: engine.complete(staging)
            downloader.probe_representation = lambda url: {"etag": '"same-etag"', "last_modified": None}

            self.assertEqual(downloader.transfer(downloader.next_pending(db), db), "complete")
            self.assert_final(downloader, db)
            self.assertEqual(engine.calls, 1)
            db.close()

    def test_resume_new_generation_control_action_restarts_a_representation_review(self):
        with FaultRoot() as root:
            downloader, db, _ = self.make_controller(root)
            downloader.transition(db, self.url, "review_required", "changed remote representation",
                                  bytes=None, review_code="changed_remote_representation",
                                  staging_generation=1, etag='"old-etag"')
            item_id = downloader.item_id(self.url)
            response = downloader.control_resume_new_generation(db, {
                "request_id": "req-1", "session_id": "sess-1",
                "parameters": {"item_ids": [item_id]},
            })
            self.assertEqual(response["outcome"], "completed")
            self.assertEqual(response["items"][item_id], "resumed under a new staging generation")
            row = db.execute(
                "SELECT status, review_code, etag, last_modified, staging_generation "
                "FROM downloads WHERE url=?", (self.url,)).fetchone()
            self.assertEqual(row, ("queued", None, None, None, 2))
            db.close()

    not_found_error = ("[HttpSkipResponseCommand.cc:218] errorCode=3 Resource not found")
    gone_error = ("[HttpSkipResponseCommand.cc:239] errorCode=22 "
                  "The response status is not successful. status=410")

    def make_unavailable(self, downloader, db, partial=True, **fields):
        if partial:
            LocalFakeTransferEngine(self.payload).partial(downloader.staging_path(self.url))
        downloader.transition(db, self.url, "unavailable", "HTTP 404",
                              recheck_at=time.time() - 1, **fields)

    def recheck(self, downloader, db, response):
        downloader.head_request = lambda url: response
        row = downloader.next_recheck(db)
        self.assertIsNotNone(row)
        with redirect_stdout(StringIO()):
            return downloader.recheck_unavailable(row, db)

    def test_404_and_410_error_lines_are_recognised_and_other_errors_are_not(self):
        self.assertTrue(tod_dl.is_unavailable_failure(self.not_found_error))
        self.assertTrue(tod_dl.is_unavailable_failure(self.gone_error))
        self.assertFalse(tod_dl.is_unavailable_failure(
            "errorCode=22 The response status is not successful. status=4040"))
        self.assertFalse(tod_dl.is_unavailable_failure(
            "errorCode=22 The response status is not successful. status=503"))

    def test_404_on_transfer_records_unavailable_and_keeps_the_partial(self):
        for error in (self.not_found_error, self.gone_error):
            with FaultRoot() as root:
                downloader, db, _ = self.make_controller(root)
                engine = LocalFakeTransferEngine(self.payload)
                staging = downloader.staging_path(self.url)
                engine.partial(staging)
                downloader.run_aria2 = lambda *_: (False, error)
                downloader.probe_representation = lambda url: None
                before = time.time()

                with redirect_stdout(StringIO()):
                    self.assertEqual(downloader.transfer(downloader.next_pending(db), db),
                                     "unavailable")
                status, attempts, recheck_at, last_error = db.execute(
                    "SELECT status, attempts, recheck_at, last_error FROM downloads WHERE url=?",
                    (self.url,)).fetchone()
                self.assertEqual((status, attempts, last_error), ("unavailable", 1, error))
                self.assertAlmostEqual(recheck_at - before, 86400, delta=60)
                self.assertTrue(staging.exists())
                self.assertIsNone(downloader.next_pending(db))
                self.assertIsNone(downloader.next_recheck(db))
                db.close()

    def test_recheck_is_claimed_for_a_full_day_before_the_request_runs(self):
        with FaultRoot() as root:
            downloader, db, _ = self.make_controller(root)
            self.make_unavailable(downloader, db)
            due = downloader.next_recheck(db)
            self.assertEqual(due[0], self.url)
            before = time.time()
            downloader.claim_recheck(db, self.url)
            self.assertIsNone(downloader.next_recheck(db))
            recheck_at = db.execute("SELECT recheck_at FROM downloads WHERE url=?",
                                    (self.url,)).fetchone()[0]
            self.assertAlmostEqual(recheck_at - before, 86400, delta=60)
            db.close()

    def make_generation_item(self, root, generation, recheck_in=3600):
        downloader, db, queue = self.make_controller(
            root, urls=[f"{self.url} generation={generation}"])
        downloader.transition(db, self.url, "unavailable", "HTTP 404",
                              recheck_at=time.time() + recheck_in, early_recheck=0)
        return downloader, db, queue

    def import_generation(self, downloader, db, queue, generation):
        queue.write_text(f"{self.url} generation={generation}\n", encoding="utf-8")
        with redirect_stdout(StringIO()):
            downloader.import_queues(db)

    def recheck_state(self, db):
        return db.execute("SELECT source_generation, early_recheck, recheck_at "
                          "FROM downloads WHERE url=?", (self.url,)).fetchone()

    def test_queue_line_generation_token_is_stored_and_a_repeated_one_is_rejected(self):
        with FaultRoot() as root:
            downloader, db, queue = self.make_controller(
                root, urls=[f"{self.url} size=1K generation=Gen-7"])
            self.assertEqual(self.recheck_state(db)[0], "Gen-7")
            self.assertEqual(list(tod_dl.read_queues([queue]))[0][4], "Gen-7")
            queue.write_text(f"{self.url} generation=a generation=b\n", encoding="utf-8")
            rejected = []
            self.assertEqual(list(tod_dl.read_queues(
                [queue], on_reject=lambda url, reason: rejected.append(reason))), [])
            self.assertEqual(rejected, ["duplicate queue line token: generation"])
            db.close()

    def test_differing_source_generation_makes_a_not_yet_due_recheck_due_early(self):
        with FaultRoot() as root:
            downloader, db, queue = self.make_generation_item(root, "g1")
            self.assertIsNone(downloader.next_recheck(db))
            self.import_generation(downloader, db, queue, "g2")
            self.assertEqual(self.recheck_state(db)[:2], ("g2", 1))
            self.assertEqual(downloader.next_recheck(db)[0], self.url)
            db.close()

    def test_early_recheck_counts_as_the_days_recheck_and_only_a_later_differing_id_repeats_it(self):
        with FaultRoot() as root:
            downloader, db, queue = self.make_generation_item(root, "g1")
            self.import_generation(downloader, db, queue, "g2")
            before = time.time()
            downloader.claim_recheck(db, self.url)
            generation, early, recheck_at = self.recheck_state(db)
            self.assertEqual(early, 2)
            self.assertAlmostEqual(recheck_at - before, 86400, delta=60)
            self.assertIsNone(downloader.next_recheck(db))
            self.import_generation(downloader, db, queue, "g2")
            self.assertEqual(self.recheck_state(db)[:2], ("g2", 2))
            self.assertIsNone(downloader.next_recheck(db))
            self.import_generation(downloader, db, queue, "g3")
            self.assertEqual(self.recheck_state(db)[:2], ("g3", 1))
            self.assertEqual(downloader.next_recheck(db)[0], self.url)
            downloader.claim_recheck(db, self.url)
            self.assertEqual(self.recheck_state(db)[1], 2)
            db.execute("UPDATE downloads SET recheck_at=?", (time.time() - 1,))
            downloader.claim_recheck(db, self.url)
            self.assertEqual(self.recheck_state(db)[1], 0)
            downloader.transition(db, self.url, "unavailable", "HTTP 404",
                                  recheck_at=time.time() + 3600)
            self.import_generation(downloader, db, queue, "g4")
            self.assertEqual(self.recheck_state(db)[1], 1)
            db.close()

    def test_source_generation_does_not_trigger_when_unknown_equal_or_already_due(self):
        with FaultRoot() as root:
            downloader, db, queue = self.make_generation_item(root, "g1")
            self.import_generation(downloader, db, queue, "g1")
            self.assertEqual(self.recheck_state(db)[:2], ("g1", 0))
            queue.write_text(self.url + "\n", encoding="utf-8")
            with redirect_stdout(StringIO()):
                downloader.import_queues(db)
            self.assertEqual(self.recheck_state(db)[:2], (None, 0))
            self.import_generation(downloader, db, queue, "g5")
            self.assertEqual(self.recheck_state(db)[:2], ("g5", 0))
            self.assertIsNone(downloader.next_recheck(db))
            db.execute("UPDATE downloads SET recheck_at=?", (time.time() - 1,))
            self.import_generation(downloader, db, queue, "g6")
            self.assertEqual(self.recheck_state(db)[:2], ("g6", 0))
            db.close()

    def test_source_generation_does_not_trigger_a_recheck_for_an_available_item(self):
        with FaultRoot() as root:
            downloader, db, queue = self.make_controller(
                root, urls=[f"{self.url} generation=g1"])
            self.import_generation(downloader, db, queue, "g2")
            self.assertEqual(self.recheck_state(db)[:2], ("g2", 0))
            self.assertIsNone(downloader.next_recheck(db))
            db.close()

    def test_recheck_that_is_not_a_success_leaves_the_item_unavailable(self):
        for response in ({"status": 404, "etag": None, "last_modified": None},
                         {"status": 410, "etag": None, "last_modified": None},
                         {"status": 503, "etag": None, "last_modified": None},
                         {"status": 403, "etag": None, "last_modified": None},
                         None):
            with FaultRoot() as root:
                downloader, db, _ = self.make_controller(root)
                self.make_unavailable(downloader, db, etag='"e"', staging_generation=1)
                self.assertEqual(self.recheck(downloader, db, response), "still_unavailable")
                self.assertEqual(db.execute("SELECT status FROM downloads WHERE url=?",
                                            (self.url,)).fetchone()[0], "unavailable")
                self.assertTrue(downloader.staging_path(self.url).exists())
                db.close()

    def test_recheck_success_without_a_staged_partial_admits_fresh_work(self):
        with FaultRoot() as root:
            downloader, db, _ = self.make_controller(root)
            self.make_unavailable(downloader, db, partial=False)
            result = self.recheck(downloader, db, {"status": 200, "etag": '"n"',
                                                   "last_modified": None})
            self.assertEqual(result, "recheck_available")
            self.assertEqual(db.execute(
                "SELECT status, recheck_at, next_retry_at FROM downloads WHERE url=?",
                (self.url,)).fetchone(), ("queued", None, 0))
            self.assertEqual(downloader.next_pending(db)[0], self.url)
            db.close()

    def test_recheck_success_with_unchanged_etag_resumes_under_the_same_generation(self):
        with FaultRoot() as root:
            downloader, db, _ = self.make_controller(root)
            self.make_unavailable(downloader, db, etag='"same"', staging_generation=1)
            result = self.recheck(downloader, db, {"status": 206, "etag": '"same"',
                                                   "last_modified": None})
            self.assertEqual(result, "recheck_available")
            self.assertEqual(db.execute(
                "SELECT status, staging_generation FROM downloads WHERE url=?",
                (self.url,)).fetchone(), ("queued", 1))
            self.assertTrue(downloader.staging_path(self.url).exists())
            db.close()

    def test_recheck_success_with_changed_etag_enters_review_and_keeps_old_bytes(self):
        with FaultRoot() as root:
            downloader, db, _ = self.make_controller(root)
            self.make_unavailable(downloader, db, etag='"old"', staging_generation=1)
            staging = downloader.staging_path(self.url)
            result = self.recheck(downloader, db, {"status": 200, "etag": '"new"',
                                                   "last_modified": None})
            self.assertEqual(result, "recheck_review")
            self.assertEqual(db.execute(
                "SELECT status, review_code, staging_generation, etag, recheck_at "
                "FROM downloads WHERE url=?", (self.url,)).fetchone(),
                ("review_required", "changed_remote_representation", 1, '"new"', None))
            self.assertFalse(staging.exists())
            candidates = list(downloader.candidates.iterdir())
            self.assertTrue(any(c.read_bytes() == self.payload[:len(self.payload) // 2]
                                for c in candidates if c.suffix != ".aria2"))
            db.close()

    def test_recheck_success_without_any_recorded_validator_is_unconfirmed_review(self):
        with FaultRoot() as root:
            downloader, db, _ = self.make_controller(root)
            self.make_unavailable(downloader, db, staging_generation=1)
            result = self.recheck(downloader, db, {"status": 200, "etag": '"n"',
                                                   "last_modified": None})
            self.assertEqual(result, "recheck_review")
            self.assertEqual(db.execute("SELECT status, review_code FROM downloads WHERE url=?",
                                        (self.url,)).fetchone(),
                             ("review_required", "no_reliable_version_protection"))
            db.close()

    def test_recheck_success_with_expected_checksum_resumes_despite_a_new_etag(self):
        with FaultRoot() as root:
            downloader, db, _ = self.make_controller(root)
            self.make_unavailable(downloader, db, etag='"old"', staging_generation=1,
                                  expected_sha256=hashlib.sha256(self.payload).hexdigest())
            result = self.recheck(downloader, db, {"status": 200, "etag": '"new"',
                                                   "last_modified": None})
            self.assertEqual(result, "recheck_available")
            self.assertEqual(db.execute("SELECT status FROM downloads WHERE url=?",
                                        (self.url,)).fetchone()[0], "queued")
            db.close()

    def test_run_rechecks_a_due_unavailable_item_and_completes_it(self):
        with FaultRoot() as root:
            queue = root / "queue.txt"
            queue.write_text(self.url + "\n", encoding="utf-8")

            def build():
                args = make_args(root, queue)
                args.tor_control_address = "127.0.0.1:9051"
                args.tor_control_cookie = root / "control.authcookie"
                args.retry_now = False
                args.time_limit = 0
                args.progress_interval = 30
                args.reserve_bytes = 0
                return Downloader(args)

            def run(downloader):
                original = tod_dl.verify_tor_isolation
                tod_dl.verify_tor_isolation = lambda address, cookie: ["9050"]
                try:
                    with redirect_stdout(StringIO()):
                        return downloader.run()
                finally:
                    tod_dl.verify_tor_isolation = original

            first = build()
            first.probe_representation = lambda url: {"etag": '"v1"', "last_modified": None}
            first.run_aria2 = lambda *_: (False, self.not_found_error)
            self.assertEqual(run(first), 1)
            db = sqlite3.connect(root / "state" / "manifest.sqlite")
            self.assertEqual(db.execute("SELECT status FROM downloads").fetchone()[0],
                             "unavailable")

            second = build()
            second.head_request = lambda url: {"status": 200, "etag": '"v1"',
                                               "last_modified": None}
            second.probe_representation = lambda url: {"etag": '"v1"', "last_modified": None}
            second.run_aria2 = lambda *_: (False, self.not_found_error)
            self.assertEqual(run(second), 1)
            self.assertEqual(db.execute("SELECT status FROM downloads").fetchone()[0],
                             "unavailable")
            self.assertEqual(db.execute("SELECT COUNT(*) FROM download_attempts").fetchone()[0],
                             1)

            db.execute("UPDATE downloads SET recheck_at=?", (time.time() - 1,))
            db.commit()
            third = build()
            engine = LocalFakeTransferEngine(self.payload)
            third.head_request = lambda url: {"status": 200, "etag": '"v1"',
                                              "last_modified": None}
            third.probe_representation = lambda url: {"etag": '"v1"', "last_modified": None}
            third.run_aria2 = lambda _url, staging, _attempt: engine.complete(staging)
            self.assertEqual(run(third), 0)
            self.assertEqual(db.execute("SELECT status FROM downloads").fetchone()[0],
                             "complete")
            db.close()

    def test_run_with_a_new_source_generation_rechecks_an_unavailable_item_early(self):
        with FaultRoot() as root:
            queue = root / "queue.txt"

            def run(run_id, generation, head_response):
                queue.write_text(f"{self.url} generation={generation}\n", encoding="utf-8")
                args = make_args(root, queue)
                args.run_id = run_id
                args.tor_control_address = "127.0.0.1:9051"
                args.tor_control_cookie = root / "control.authcookie"
                args.retry_now = False
                args.time_limit = 0
                args.progress_interval = 30
                args.reserve_bytes = 0
                downloader = Downloader(args)
                heads = []
                downloader.head_request = lambda url: heads.append(url) or head_response
                downloader.probe_representation = lambda url: {"etag": '"v1"',
                                                               "last_modified": None}
                downloader.run_aria2 = lambda *_: (False, self.not_found_error)
                original = tod_dl.verify_tor_isolation
                tod_dl.verify_tor_isolation = lambda address, cookie: ["9050"]
                try:
                    with redirect_stdout(StringIO()):
                        downloader.run()
                finally:
                    tod_dl.verify_tor_isolation = original
                return heads

            gone = {"status": 404, "etag": None, "last_modified": None}
            self.assertEqual(run("run-a", "g1", gone), [])
            self.assertEqual(run("run-b", "g1", gone), [])
            self.assertEqual(run("run-c", "g2", gone), [self.url])
            self.assertEqual(run("run-d", "g2", gone), [])
            self.assertEqual(run("run-e", "g3", gone), [self.url])
            self.assertEqual(run("run-f", "g3", gone), [])
            db = sqlite3.connect(root / "state" / "manifest.sqlite")
            self.assertEqual(db.execute("SELECT status, source_generation, early_recheck "
                                        "FROM downloads").fetchone(),
                             ("unavailable", "g3", 2))
            db.close()

    def test_during_file_flush_preserves_staging_and_does_not_create_a_final(self):
        with FaultRoot() as root:
            downloader, db, _ = self.make_controller(root)
            staging = downloader.staging_path(self.url)
            engine = LocalFakeTransferEngine(self.payload)
            engine.complete(staging)
            downloader.run_aria2 = lambda *_: (True, "local fixture complete")
            original_flush = downloader.flush_file
            downloader.flush_file = lambda _: (_ for _ in ()).throw(
                OSError(errno.EIO, "local fixture flush failure"))
            try:
                self.assertEqual(downloader.transfer(downloader.next_pending(db), db), "review")
            finally:
                downloader.flush_file = original_flush
            self.assertEqual(staging.read_bytes(), self.payload)
            self.assertFalse((downloader.destination / "first" / "data" / "item.bin").exists())
            self.assertEqual(db.execute("SELECT status FROM downloads WHERE url=?", (self.url,))
                             .fetchone()[0], "review_required")
            db.close()

    def test_after_hash_and_promotion_intent_commit_reconciles_final_digest(self):
        with FaultRoot() as root:
            downloader, db, queue = self.make_controller(root)
            staging = downloader.staging_path(self.url)
            staging.parent.mkdir(parents=True)
            staging.write_bytes(self.payload)
            target = downloader.destination / "first" / "data" / "item.bin"
            target.parent.mkdir(parents=True)
            digest = hashlib.sha256(self.payload).hexdigest()
            downloader.transition(db, self.url, "promoting", sha256=digest,
                                  bytes=len(self.payload), promotion_target=str(target))
            os.link(staging, target)
            db.close()

            restarted, db = self.restart(root, queue)
            restarted.reconcile_promotions(db)
            restarted.cleanup_completed_staging(db)
            self.assert_final(restarted, db)
            self.assertFalse(staging.exists())
            self.assertEqual(self.transition_states(db)[:2], ["promoting", "complete"])
            self.assertEqual(self.selected_urls(db), [self.url])
            db.close()

    def test_after_exclusive_final_creation_before_completion_commit_hashes_final(self):
        with FaultRoot() as root:
            downloader, db, queue = self.make_controller(root)
            staging = downloader.staging_path(self.url)
            staging.parent.mkdir(parents=True)
            staging.write_bytes(self.payload)
            target = downloader.destination / "first" / "data" / "item.bin"
            target.parent.mkdir(parents=True)
            digest = hashlib.sha256(self.payload).hexdigest()
            downloader.transition(db, self.url, "promoting", sha256=digest,
                                  promotion_target=str(target))
            os.link(staging, target)
            db.close()

            restarted, db = self.restart(root, queue)
            restarted.reconcile_promotions(db)
            self.assert_final(restarted, db)
            self.assertFalse(staging.exists())
            self.assertEqual(self.transition_states(db)[:2], ["promoting", "complete"])
            self.assertEqual(self.selected_urls(db), [self.url])
            db.close()

    def test_after_completion_commit_before_staging_cleanup_repeats_cleanup_safely(self):
        with FaultRoot() as root:
            downloader, db, queue = self.make_controller(root)
            staging = downloader.staging_path(self.url)
            staging.parent.mkdir(parents=True)
            staging.write_bytes(self.payload)
            target = downloader.destination / "first" / "data" / "item.bin"
            target.parent.mkdir(parents=True)
            os.link(staging, target)
            digest = hashlib.sha256(self.payload).hexdigest()
            downloader.transition(db, self.url, "complete", sha256=digest,
                                  bytes=len(self.payload))
            db.close()

            restarted, db = self.restart(root, queue)
            restarted.cleanup_completed_staging(db)
            restarted.cleanup_completed_staging(db)
            self.assert_final(restarted, db)
            self.assertFalse(staging.exists())
            self.assertIsNotNone(db.execute("SELECT cleanup_completed_at FROM downloads "
                                             "WHERE url=?", (self.url,)).fetchone()[0])
            self.assertEqual(self.selected_urls(db), [self.url])
            db.close()

    def test_during_candidate_creation_preserves_existing_final_and_incoming_bytes(self):
        with FaultRoot() as root:
            downloader, db, queue = self.make_controller(root)
            prior = b"existing evidence"
            target = downloader.destination / "first" / "data" / "item.bin"
            target.parent.mkdir(parents=True)
            target.write_bytes(prior)
            staging = downloader.staging_path(self.url)
            staging.parent.mkdir(parents=True)
            staging.write_bytes(self.payload)
            downloader.transition(db, self.url, "promoting",
                                  sha256=hashlib.sha256(self.payload).hexdigest(),
                                  promotion_target=str(target))
            candidate = downloader.move_candidate(staging)
            db.close()

            restarted, db = self.restart(root, queue)
            restarted.reconcile_promotions(db)
            self.assertEqual(target.read_bytes(), prior)
            self.assertEqual(candidate.read_bytes(), self.payload)
            self.assertEqual(sha256sum(candidate), hashlib.sha256(self.payload).hexdigest())
            self.assertEqual(db.execute("SELECT status FROM downloads WHERE url=?", (self.url,))
                             .fetchone()[0], "review_required")
            self.assertEqual(self.selected_urls(db), [self.url])
            db.close()

    def test_during_sqlite_commit_or_wal_checkpoint_stops_admission_until_recovery(self):
        with FaultRoot() as root:
            downloader, db, queue = self.make_controller(root)
            db.close()  # A closed connection simulates a local commit/checkpoint failure.
            with self.assertRaisesRegex(RuntimeError, "local SQLite failure"):
                downloader.transition(db, self.url, "admitted")
            self.assertTrue(downloader.stop_requested.is_set())
            self.assertFalse((downloader.destination / "first" / "data" / "item.bin").exists())

            restarted, db = self.restart(root, queue)
            self.assertEqual(restarted.next_pending(db)[0], self.url)
            self.assertEqual(self.selected_urls(db), [self.url])
            db.close()

    def test_terminal_failure_does_not_admit_outside_immutable_selection(self):
        urls = [self.url, "https://fixture.test/second/data/later.bin"]
        with FaultRoot() as root:
            downloader, db, _ = self.make_controller(root, urls, max_files=1)
            engine = LocalFakeTransferEngine(self.payload)
            staging = downloader.staging_path(self.url)
            downloader.run_aria2 = lambda *_: engine.terminal_failure(staging)
            self.assertEqual(downloader.transfer(downloader.next_pending(db), db), "failed")
            downloader.reset_retry_now(db)
            self.assertEqual(downloader.next_pending(db)[0], self.url)
            self.assertEqual(self.selected_urls(db), [self.url])
            db.close()

    def test_staging_cleanup_failure_keeps_the_final_complete_and_repeats_safely(self):
        with FaultRoot() as root:
            downloader, db, queue = self.make_controller(root)
            staging = downloader.staging_path(self.url)
            staging.parent.mkdir(parents=True)
            staging.write_bytes(self.payload)
            target = downloader.destination / "first" / "data" / "item.bin"
            target.parent.mkdir(parents=True)
            os.link(staging, target)
            digest = hashlib.sha256(self.payload).hexdigest()
            downloader.transition(db, self.url, "complete", sha256=digest,
                                  bytes=len(self.payload))
            original_unlink = os.unlink

            def failing_unlink(path, *args, **kwargs):
                raise OSError(errno.EIO, "local fixture cleanup failure")

            os.unlink = failing_unlink
            try:
                downloader.cleanup_completed_staging(db)
            finally:
                os.unlink = original_unlink
            self.assert_final(downloader, db)
            self.assertTrue(staging.exists())
            self.assertIsNone(db.execute("SELECT cleanup_completed_at FROM downloads "
                                         "WHERE url=?", (self.url,)).fetchone()[0])
            self.assertEqual(self.transition_states(db), ["complete", "complete"])
            db.close()

            restarted, db = self.restart(root, queue)
            restarted.cleanup_completed_staging(db)
            self.assert_final(restarted, db)
            self.assertFalse(staging.exists())
            self.assertEqual(target.read_bytes(), self.payload)
            self.assertIsNotNone(db.execute("SELECT cleanup_completed_at FROM downloads "
                                            "WHERE url=?", (self.url,)).fetchone()[0])
            self.assertEqual(self.selected_urls(db), [self.url])
            db.close()

    def test_controller_shutdown_during_transfer_keeps_the_partial_for_resume(self):
        with FaultRoot() as root:
            downloader, db, queue = self.make_controller(root)
            staging = downloader.staging_path(self.url)
            engine = LocalFakeTransferEngine(self.payload)

            def stopped_by_shutdown(*_):
                engine.partial(staging)
                downloader.stop_requested.set()
                return False, "local fixture stopped by controller shutdown"

            downloader.run_aria2 = stopped_by_shutdown
            self.assertEqual(downloader.transfer(downloader.next_pending(db), db), "failed")
            self.assertEqual(staging.read_bytes(), self.payload[:len(self.payload) // 2])
            self.assertTrue(Path(str(staging) + ".aria2").is_file())
            self.assertFalse((downloader.destination / "first" / "data" / "item.bin").exists())
            self.assertEqual(db.execute("SELECT status FROM downloads WHERE url=?", (self.url,))
                             .fetchone()[0], "retry_wait")
            self.assertEqual(db.execute("SELECT outcome FROM download_attempts WHERE url=?",
                                        (self.url,)).fetchone()[0], "stopped")
            db.close()

            restarted, db = self.restart(root, queue)
            restarted.requeue_interrupted_transfers(db)
            restarted.reset_retry_now(db)
            restarted.run_aria2 = lambda *_: engine.complete(staging)
            self.assertEqual(restarted.transfer(restarted.next_pending(db), db), "complete")
            self.assert_final(restarted, db)
            self.assertEqual(self.selected_urls(db), [self.url])
            db.close()

    def run_with_time_limit(self, root: Path, time_limit: float, ticking_clock: bool = False):
        queue = root / "queue.txt"
        queue.write_text(self.url + "\n", encoding="utf-8")
        args = make_args(root, queue)
        args.tor_control_address = "127.0.0.1:9051"
        args.tor_control_cookie = root / "control.authcookie"
        args.retry_now = False
        args.time_limit = time_limit
        args.progress_interval = 30
        args.reserve_bytes = 0
        downloader = Downloader(args)
        engine = LocalFakeTransferEngine(self.payload)
        downloader.run_aria2 = lambda _url, staging, _attempt: engine.complete(staging)
        original_verify = tod_dl.verify_tor_isolation
        original_monotonic = time.monotonic
        tod_dl.verify_tor_isolation = lambda address, cookie: ["9050"]
        if ticking_clock:
            # Each clock read advances one second, so the deadline has always expired
            # by the next read. This removes the race between setup time and a tiny limit.
            ticks = itertools.count()
            time.monotonic = lambda: float(next(ticks))
        try:
            with redirect_stdout(StringIO()):
                exit_code = downloader.run()
        finally:
            time.monotonic = original_monotonic
            tod_dl.verify_tor_isolation = original_verify
        closed = [json.loads(line) for line in
                  downloader.provenance.events_path.read_text(encoding="utf-8").splitlines()][-1]
        return exit_code, closed["close_reason"], downloader.destination / "first" / "data" / "item.bin"

    def test_time_limit_does_not_stop_admission_before_expiry(self):
        with FaultRoot() as root:
            exit_code, close_reason, target = self.run_with_time_limit(root, 3600)
            self.assertEqual((exit_code, close_reason), (0, "finished"))
            self.assertEqual(target.read_bytes(), self.payload)

    def test_time_limit_stops_admission_at_expiry_without_touching_finals(self):
        with FaultRoot() as root:
            exit_code, close_reason, target = self.run_with_time_limit(root, 1, ticking_clock=True)
            self.assertEqual((exit_code, close_reason), (1, "time_limit"))
            self.assertFalse(target.exists())
            db = sqlite3.connect(root / "state" / "manifest.sqlite")
            self.assertEqual(db.execute("SELECT status FROM downloads").fetchall(), [("queued",)])
            db.close()

    def test_incomplete_body_retries_once_before_review(self):
        """A single EOF failure may be a resumable mid-transfer cut; retry it."""
        with FaultRoot() as root:
            downloader, db, _ = self.make_controller(root)
            engine = LocalFakeTransferEngine(self.payload)
            staging = downloader.staging_path(self.url)
            downloader.run_aria2 = lambda *_: engine.incomplete_body(staging)
            self.assertEqual(downloader.transfer(downloader.next_pending(db), db), "failed")
            row = db.execute("SELECT status, last_error FROM downloads WHERE url=?",
                             (self.url,)).fetchone()
            self.assertEqual(row[0], "retry_wait")
            self.assertIn("Got EOF from the server", row[1])
            self.assertTrue(staging.exists())  # kept in place for --continue=true
            self.assertFalse(list(downloader.candidates.glob("*")))
            db.close()

    def test_incomplete_body_resumes_to_completion_on_retry(self):
        """Growth on the retry means the cut was resumable, not a short body."""
        with FaultRoot() as root:
            downloader, db, _ = self.make_controller(root)
            engine = LocalFakeTransferEngine(self.payload)
            staging = downloader.staging_path(self.url)
            downloader.run_aria2 = lambda *_: engine.incomplete_body(staging)
            self.assertEqual(downloader.transfer(downloader.next_pending(db), db), "failed")
            downloader.reset_retry_now(db)
            downloader.run_aria2 = lambda *_: engine.complete(staging)
            self.assertEqual(downloader.transfer(downloader.next_pending(db), db), "complete")
            db.close()

    def test_incomplete_body_blocks_promotion_and_retains_review_candidate(self):
        """No growth on the retry means a genuinely short body; give up to review."""
        with FaultRoot() as root:
            downloader, db, _ = self.make_controller(root)
            engine = LocalFakeTransferEngine(self.payload)
            staging = downloader.staging_path(self.url)
            downloader.run_aria2 = lambda *_: engine.incomplete_body(staging)
            self.assertEqual(downloader.transfer(downloader.next_pending(db), db), "failed")
            downloader.reset_retry_now(db)
            self.assertEqual(downloader.transfer(downloader.next_pending(db), db), "review")
            row = db.execute("SELECT status, last_error FROM downloads WHERE url=?",
                             (self.url,)).fetchone()
            self.assertEqual(row[0], "review_required")
            self.assertIn("Got EOF from the server", row[1])
            self.assertFalse(staging.exists())
            candidates = list(downloader.candidates.glob("*"))
            self.assertEqual(len(candidates), 1)
            self.assertEqual(candidates[0].read_bytes(), self.payload[:len(self.payload) // 2])
            db.close()

    def test_disk_full_stops_admission_without_consuming_a_retry_attempt(self):
        """ENOSPC must stop admission and not count as a network retry."""
        with FaultRoot() as root:
            downloader, db, _ = self.make_controller(root)
            staging = downloader.staging_path(self.url)

            def enospc(*_args):
                staging.parent.mkdir(parents=True, exist_ok=True)
                staging.write_bytes(self.payload[:3])
                return False, "errorCode=13 No space left on device"

            downloader.run_aria2 = enospc
            row = downloader.next_pending(db)
            attempts_before = row[4]
            self.assertEqual(downloader.transfer(row, db), "local_failure")
            self.assertTrue(downloader.admission_paused.is_set())
            updated = db.execute(
                "SELECT status, attempts, last_error FROM downloads WHERE url=?",
                (self.url,)).fetchone()
            self.assertEqual(updated[0], "queued")
            self.assertEqual(updated[1], attempts_before)
            self.assertIn("No space left on device", updated[2])
            db.close()

    def test_retry_now_wakes_admission_with_an_active_transfer(self):
        with FaultRoot() as root:
            downloader, db, _ = self.make_controller(root)
            downloader.update(db, "UPDATE downloads SET status='retry_wait', next_retry_at=? "
                              "WHERE url=?", (time.time() + 3600, self.url))
            downloader.active["active-fixture"] = root / "active-partial"
            response = downloader.control_retry_now(
                db, {"request_id": "fault-retry", "session_id": "fault-session"})
            self.assertEqual(response["outcome"], "completed")
            self.assertEqual(len(downloader.active), 1)
            self.assertEqual(downloader.admission_wait_timeout(), 0)
            self.assertEqual(downloader.next_pending(db)[0], self.url)
            db.close()

    def test_eligible_retry_fills_idle_slot_within_one_admission_poll(self):
        with FaultRoot() as root:
            downloader, db, _ = self.make_controller(root)
            downloader.update(db, "UPDATE downloads SET status='retry_wait', next_retry_at=? "
                              "WHERE url=?", (time.time() + 1, self.url))
            downloader.reset_retry_now(db)
            self.assertLessEqual(downloader.admission_wait_timeout(), ADMISSION_POLL_SECONDS)
            self.assertEqual(downloader.next_pending(db)[0], self.url)
            db.close()

    def test_storage_reserve_failure_keeps_final_bytes_unchanged(self):
        with FaultRoot() as root:
            downloader, db, _ = self.make_controller(root)
            target = downloader.destination / "first" / "data" / "item.bin"
            target.parent.mkdir(parents=True)
            target.write_bytes(b"existing final")
            downloader.has_storage_reserve = lambda: False
            self.assertFalse(downloader.has_storage_reserve())
            self.assertEqual(target.read_bytes(), b"existing final")
            self.assertEqual(db.execute("SELECT status FROM downloads WHERE url=?", (self.url,))
                             .fetchone()[0], "queued")
            db.close()

    def test_storage_reserve_accounts_for_admitted_transfers_remaining_bytes(self):
        with FaultRoot() as root:
            downloader, db, _ = self.make_controller(root)
            free = shutil.disk_usage(downloader.destination).free
            downloader.args.reserve_bytes = 1

            class FakeTelemetry:
                def runtime_copy(self):
                    return [{"total_bytes": free, "received_bytes": 0}]

            downloader.telemetry = FakeTelemetry()
            self.assertFalse(downloader.has_storage_reserve())
            downloader.telemetry = None
            self.assertTrue(downloader.has_storage_reserve())
            db.close()

    def test_zero_progress_transfer_uses_one_terminal_stall_result_without_busy_loop(self):
        with FaultRoot() as root:
            downloader, db, _ = self.make_controller(root)
            engine = LocalFakeTransferEngine(self.payload)
            staging = downloader.staging_path(self.url)
            downloader.run_aria2 = lambda *_: engine.stalled(staging)
            started = time.monotonic()
            self.assertEqual(downloader.transfer(downloader.next_pending(db), db), "failed")
            self.assertLess(time.monotonic() - started, ADMISSION_POLL_SECONDS)
            self.assertEqual(engine.calls, 1)
            self.assertEqual(db.execute("SELECT status FROM downloads WHERE url=?", (self.url,))
                             .fetchone()[0], "retry_wait")
            db.close()

    def test_hashing_serializes_across_concurrent_workers(self):
        """Spec: hash one file at a time; concurrent workers must not overlap hashing."""
        first_url = "https://fixture.test/first/data/one.bin"
        second_url = "https://fixture.test/second/data/two.bin"
        with FaultRoot() as root:
            downloader, db, _ = self.make_controller(
                root, urls=[first_url, second_url], max_files=2)
            engine = LocalFakeTransferEngine(self.payload)
            downloader.run_aria2 = lambda url, staging, attempt: engine.complete(staging)
            active = 0
            peak = 0
            guard = threading.Lock()

            def tracking_sha256sum(path, progress=None):
                nonlocal active, peak
                with guard:
                    active += 1
                    peak = max(peak, active)
                time.sleep(0.05)
                digest = tod_dl.hashlib.sha256(path.read_bytes()).hexdigest()
                with guard:
                    active -= 1
                return digest

            original_sha256sum = tod_dl.sha256sum
            tod_dl.sha256sum = tracking_sha256sum
            try:
                first_row = downloader.next_pending(db)
                downloader.transition(db, first_row[0], "admitted", "transfer admitted")
                second_row = downloader.next_pending(db)
                rows = [first_row, second_row]
                results = []
                workers = [threading.Thread(target=lambda r=row: results.append(
                    downloader.transfer(r, db))) for row in rows]
                for worker in workers:
                    worker.start()
                for worker in workers:
                    worker.join()
            finally:
                tod_dl.sha256sum = original_sha256sum
            self.assertEqual(results, ["complete", "complete"])
            self.assertEqual(peak, 1)
            db.close()


class ProvenanceTests(unittest.TestCase):
    def make_record(self, root: Path):
        state = root / "state"
        destination = root / "destination"
        final = destination / "fixture" / "data" / "item.bin"
        final.parent.mkdir(parents=True)
        final.write_bytes(b"provenance fixture")
        writer = ProvenanceWriter(state, "fixture-run")
        queue = [{"path": "/fixture/queue.txt", "sha256": "0" * 64}]
        writer.event("run_started", queue_input_digests=queue,
                     selection_settings={"max_files": 1}, selected_item_count=1)
        writer.event("attempt_finished", item_id="a" * 64,
                     source_url="https://fixture.test/fixture/data/item.bin", attempt_number=1,
                     request_started_at="2026-09-16T00:00:00Z",
                     request_finished_at="2026-09-16T00:00:01Z", final_url=None,
                     redirect_chain=[], http_status=None, outcome="success",
                     response_content_length=None, response_content_range=None,
                     response_content_type=None, response_etag=None,
                     response_last_modified=None)
        digest = hashlib.sha256(final.read_bytes()).hexdigest()
        unavailable = {"available": False, "compared": False, "value": None}
        writer.event("finalized", item_id="a" * 64,
                     source_url="https://fixture.test/fixture/data/item.bin",
                     logical_relative_path="fixture/data/item.bin",
                     final_relative_path="fixture/data/item.bin", byte_count=final.stat().st_size,
                     sha256=digest, validation_method_version="1",
                     finalized_at="2026-09-16T00:00:01Z", expected_size=unavailable,
                     expected_checksum=unavailable, etag=unavailable, last_modified=unavailable)
        writer.close(queue, {"max_files": 1}, 1, {"complete": 1}, "finished")
        public = root / "trusted-public.pem"
        public.write_bytes(writer.private_key.public_key().public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo,
        ))
        return writer, destination, final, public

    def test_valid_record_and_required_tampering_fail(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            writer, destination, final, public = self.make_record(root)
            self.assertEqual(verify(writer.directory, destination, public, None), [])
            final.write_bytes(b"changed")
            self.assertTrue(verify(writer.directory, destination, public, None))

    def test_event_and_summary_tampering_fail(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            writer, destination, final, public = self.make_record(root)
            events = writer.directory / "events.jsonl"
            lines = events.read_text(encoding="utf-8").splitlines()
            events.write_text("\n".join(reversed(lines)) + "\n", encoding="utf-8")
            self.assertTrue(verify(writer.directory, destination, public, None))
            writer, destination, final, public = self.make_record(root / "second")
            summary = writer.directory / "summary.json"
            value = json.loads(summary.read_text(encoding="utf-8"))
            value["final_event_digest"] = "0" * 64
            summary.write_text(json.dumps(value), encoding="utf-8")
            self.assertTrue(verify(writer.directory, destination, public, None))
            writer, destination, final, public = self.make_record(root / "third")
            (writer.directory / "schema.json").write_text("{}", encoding="utf-8")
            self.assertTrue(verify(writer.directory, destination, public, None))

    def test_missing_final_fails_after_a_replaced_signed_summary(self):
        with tempfile.TemporaryDirectory() as temporary:
            writer, destination, final, public = self.make_record(Path(temporary))
            final.unlink()
            self.assertTrue(verify(writer.directory, destination, public, None))


class ProvenanceAcceptanceTests(unittest.TestCase):
    """Each tampering case must fail for its own named reason, not only fail."""

    make_record = ProvenanceTests.make_record

    def assert_fails_with(self, writer, destination, public, text):
        errors = verify(writer.directory, destination, public, None)
        self.assertTrue(any(text in error for error in errors), errors)
        return errors

    def read_events(self, writer):
        return [json.loads(line) for line in
                writer.events_path.read_text(encoding="utf-8").splitlines()]

    def resign(self, writer, change=lambda summary: None):
        summary_path = writer.directory / "summary.json"
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        summary.pop("signature")
        change(summary)
        signature = writer.private_key.sign(canonical_json(summary))
        summary["signature"] = base64.urlsafe_b64encode(signature).decode("ascii").rstrip("=")
        summary_path.write_text(json.dumps(summary), encoding="utf-8")

    def rewrite_events(self, writer, change):
        """Apply a change, then rebuild the digest chain and re-sign the summary.

        The result isolates one rule: only the changed field can fail.
        """
        events = self.read_events(writer)
        change(events)
        previous = None
        for event in events:
            event["previous_digest"] = previous
            event.pop("event_digest", None)
            event["event_digest"] = previous = event_digest(event)
        writer.events_path.write_text(
            "".join(json.dumps(event, sort_keys=True) + "\n" for event in events),
            encoding="utf-8")
        self.resign(writer, lambda summary: summary.update(
            event_count=len(events), final_event_digest=previous))

    def test_changed_final_file_reports_the_digest_mismatch(self):
        with tempfile.TemporaryDirectory() as temporary:
            writer, destination, final, public = self.make_record(Path(temporary))
            final.write_bytes(b"provenance fixtur3")  # same length, different bytes
            errors = self.assert_fails_with(writer, destination, public, "final SHA-256 differs")
            self.assertFalse(any("byte count" in error for error in errors))

    def test_missing_final_file_is_reported_by_path(self):
        with tempfile.TemporaryDirectory() as temporary:
            writer, destination, final, public = self.make_record(Path(temporary))
            final.unlink()
            self.assert_fails_with(writer, destination, public,
                                   "final file is missing: fixture/data/item.bin")

    def test_removed_event_line_reports_a_sequence_gap(self):
        with tempfile.TemporaryDirectory() as temporary:
            writer, destination, final, public = self.make_record(Path(temporary))
            lines = writer.events_path.read_text(encoding="utf-8").splitlines()
            del lines[1]
            writer.events_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
            self.assert_fails_with(writer, destination, public, "sequence gap or reorder")

    def test_reordered_event_lines_report_a_sequence_error(self):
        with tempfile.TemporaryDirectory() as temporary:
            writer, destination, final, public = self.make_record(Path(temporary))
            lines = writer.events_path.read_text(encoding="utf-8").splitlines()
            lines[1], lines[2] = lines[2], lines[1]
            writer.events_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
            self.assert_fails_with(writer, destination, public, "sequence gap or reorder")

    def test_changed_event_field_reports_an_invalid_event_digest(self):
        with tempfile.TemporaryDirectory() as temporary:
            writer, destination, final, public = self.make_record(Path(temporary))
            events = self.read_events(writer)
            events[2]["source_url"] = "https://fixture.test/other/item.bin"
            writer.events_path.write_text(
                "".join(json.dumps(event, sort_keys=True) + "\n" for event in events),
                encoding="utf-8")
            self.assert_fails_with(writer, destination, public, "invalid event digest")

    def test_wrong_summary_final_digest_is_reported_when_correctly_signed(self):
        with tempfile.TemporaryDirectory() as temporary:
            writer, destination, final, public = self.make_record(Path(temporary))
            self.resign(writer, lambda summary: summary.update(final_event_digest="0" * 64))
            errors = self.assert_fails_with(writer, destination, public,
                                            "final event digest mismatch")
            self.assertFalse(any("invalid signature" in error for error in errors))

    def test_changed_summary_signature_is_reported(self):
        with tempfile.TemporaryDirectory() as temporary:
            writer, destination, final, public = self.make_record(Path(temporary))
            summary_path = writer.directory / "summary.json"
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            summary["signature"] = "A" * 86
            summary_path.write_text(json.dumps(summary), encoding="utf-8")
            self.assert_fails_with(writer, destination, public, "invalid signature")

    def test_changed_schema_file_is_reported(self):
        with tempfile.TemporaryDirectory() as temporary:
            writer, destination, final, public = self.make_record(Path(temporary))
            (writer.directory / "schema.json").write_text("{}", encoding="utf-8")
            errors = self.assert_fails_with(writer, destination, public, "schema digest mismatch")
            self.assertTrue(any("schema content changed" in error for error in errors))

    def test_removed_final_event_with_a_matching_signed_summary_fails(self):
        with tempfile.TemporaryDirectory() as temporary:
            writer, destination, final, public = self.make_record(Path(temporary))
            self.rewrite_events(writer, lambda events: events.pop())
            errors = self.assert_fails_with(writer, destination, public, "not a matching run_closed")
            self.assertFalse(any("final event digest" in error for error in errors))

    def test_removed_finalized_event_with_a_matching_signed_summary_fails(self):
        with tempfile.TemporaryDirectory() as temporary:
            writer, destination, final, public = self.make_record(Path(temporary))
            self.rewrite_events(writer, lambda events: events.pop(2))
            self.assert_fails_with(writer, destination, public,
                                   "completed-item count does not match finalized events")

    def test_summary_signed_by_an_untrusted_key_fails(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            writer, destination, final, public = self.make_record(root)
            other, _, _, other_public = self.make_record(root / "other")
            self.assert_fails_with(writer, destination, other_public, "fingerprint mismatch")

    def test_verifier_requires_a_trust_anchor(self):
        with tempfile.TemporaryDirectory() as temporary:
            writer, destination, final, public = self.make_record(Path(temporary))
            errors = verify(writer.directory, destination, None, None)
            self.assertIn("a trusted public key or expected fingerprint is required", errors)

    def test_a_fingerprint_alone_does_not_skip_the_signature_check(self):
        with tempfile.TemporaryDirectory() as temporary:
            writer, destination, final, public = self.make_record(Path(temporary))
            errors = verify(writer.directory, destination, None, writer.fingerprint)
            self.assertIn("a public key is required to verify the summary signature", errors)

    def test_an_expected_fingerprint_must_match_the_trusted_key(self):
        with tempfile.TemporaryDirectory() as temporary:
            writer, destination, final, public = self.make_record(Path(temporary))
            self.assertEqual(verify(writer.directory, destination, public, writer.fingerprint), [])
            errors = verify(writer.directory, destination, public, "1" * 64)
            self.assertIn("trusted public key fingerprint does not match expected fingerprint", errors)

    def test_each_consumer_rejection_rule_is_reported(self):
        cases = {
            "invalid event type": lambda events: events[1].update(event_type="bogus"),
            "missing or unknown event fields": lambda events: events[1].pop("outcome"),
            "duplicate event ID": lambda events: events[2].update(event_id=events[1]["event_id"]),
            "sequence gap or reorder": lambda events: events[2].update(sequence=9),
            "identifiers do not match record path": lambda events: events[1].update(run_id="other"),
            "is not an allowed value": lambda events: events[1].update(outcome="local_failure"),
        }
        for text, change in cases.items():
            with self.subTest(rule=text), tempfile.TemporaryDirectory() as temporary:
                writer, destination, final, public = self.make_record(Path(temporary))
                self.rewrite_events(writer, change)
                self.assert_fails_with(writer, destination, public, text)

    def test_invalid_json_and_duplicate_keys_are_rejected(self):
        for text, line in {"invalid JSON": "{not json", "duplicate key": '{"a":1,"a":2}'}.items():
            with self.subTest(rule=text), tempfile.TemporaryDirectory() as temporary:
                writer, destination, final, public = self.make_record(Path(temporary))
                with writer.events_path.open("a", encoding="utf-8") as handle:
                    handle.write(line + "\n")
                self.assert_fails_with(writer, destination, public, text)

    def test_invalid_digest_format_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            writer, destination, final, public = self.make_record(Path(temporary))
            self.rewrite_events(writer, lambda events: events[2].update(
                sha256=events[2]["sha256"].upper()))
            self.assert_fails_with(writer, destination, public, "does not match the required format")

    def test_unsafe_relative_path_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            writer, destination, final, public = self.make_record(Path(temporary))
            self.rewrite_events(writer, lambda events: events[2].update(
                final_relative_path="../escape.bin"))
            self.assert_fails_with(writer, destination, public, "finalized event is invalid")

    def test_an_unavailable_validation_object_must_not_claim_a_value(self):
        with tempfile.TemporaryDirectory() as temporary:
            writer, destination, final, public = self.make_record(Path(temporary))
            self.rewrite_events(writer, lambda events: events[2]["etag"].update(compared=True))
            self.assert_fails_with(writer, destination, public, "etag is unavailable")

    def test_verifier_never_modifies_evidence(self):
        with tempfile.TemporaryDirectory() as temporary:
            writer, destination, final, public = self.make_record(Path(temporary))
            final.write_bytes(b"tampered")
            paths = [*writer.directory.iterdir(), final]
            before = {path: (path.read_bytes(), path.stat().st_mtime_ns) for path in paths}
            self.assertTrue(verify(writer.directory, destination, public, None))
            after = {path: (path.read_bytes(), path.stat().st_mtime_ns) for path in paths}
            self.assertEqual(before, after)
            self.assertEqual(sorted(path.name for path in writer.directory.iterdir()),
                             ["events.jsonl", "schema.json", "summary.json"])

    def test_record_set_modes_and_line_format(self):
        with tempfile.TemporaryDirectory() as temporary:
            writer, destination, final, public = self.make_record(Path(temporary))
            self.assertEqual(stat.S_IMODE(writer.directory.stat().st_mode), 0o700)
            for name in ("events.jsonl", "schema.json", "summary.json"):
                self.assertEqual(stat.S_IMODE((writer.directory / name).stat().st_mode), 0o600, name)
            raw = writer.events_path.read_bytes()
            self.assertTrue(raw.endswith(b"\n"))
            self.assertNotIn(b"\r", raw)
            self.assertEqual(len(raw.decode("utf-8").split("\n")), 5)

    def test_writer_rejects_credentialed_urls_without_appending(self):
        with tempfile.TemporaryDirectory() as temporary:
            writer, destination, final, public = self.make_record(Path(temporary))
            before = writer.events_path.read_bytes()
            for url in ("https://user:secret@fixture.test/item.bin", "https://user@fixture.test/item.bin"):
                with self.assertRaises(ProvenanceError):
                    writer.event("candidate_created", item_id="a" * 64, source_url=url)
            with self.assertRaises(ProvenanceError):
                writer.event("attempt_finished", item_id="a" * 64,
                             source_url="https://fixture.test/item.bin", attempt_number=1,
                             request_started_at="2026-09-16T00:00:00Z",
                             request_finished_at="2026-09-16T00:00:01Z",
                             final_url="https://fixture.test/item.bin",
                             redirect_chain=["https://u:p@fixture.test/hop"], http_status=200,
                             outcome="success", response_content_length=None,
                             response_content_range=None, response_content_type=None,
                             response_etag=None, response_last_modified=None)
            self.assertEqual(writer.events_path.read_bytes(), before)
            self.assertEqual(writer.sequence, 4)

    def test_writer_rejects_an_event_that_breaks_the_schema(self):
        with tempfile.TemporaryDirectory() as temporary:
            writer, destination, final, public = self.make_record(Path(temporary))
            before = writer.events_path.read_bytes()
            with self.assertRaisesRegex(ProvenanceError, "schema"):
                writer.event("local_failure", failure_code="bogus", failure_detail="x")
            self.assertEqual(writer.events_path.read_bytes(), before)
            self.assertEqual(writer.sequence, 4)


class ProvenanceControllerTests(unittest.TestCase):
    """The controller must record durable provenance before it records completion."""

    url = "https://fixture.test/first/data/item.bin"
    payload = b"provenance controller fixture"

    def make_controller(self, root: Path):
        queue = root / "queue.txt"
        queue.write_text(self.url + "\n", encoding="utf-8")
        downloader = Downloader(make_args(root, queue, 1))
        downloader.destination.mkdir(exist_ok=True)
        downloader.state.mkdir(exist_ok=True)
        db = downloader.open_db()
        downloader.import_queues(db)
        downloader.scope_run(db)
        downloader.provenance = ProvenanceWriter(downloader.state, downloader.run_id)
        downloader.provenance_event("run_started", queue_input_digests=[],
                                    selection_settings={"max_files": 1}, selected_item_count=1)
        engine = LocalFakeTransferEngine(self.payload)
        staging = downloader.staging_path(self.url)
        downloader.run_aria2 = lambda *_: engine.complete(staging)
        return downloader, db, engine, staging

    def events(self, downloader):
        return [json.loads(line) for line in
                downloader.provenance.events_path.read_text(encoding="utf-8").splitlines()]

    def public_key(self, root: Path, downloader) -> Path:
        public = root / "trusted.pem"
        public.write_bytes(downloader.provenance.private_key.public_key().public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo))
        return public

    def test_completed_transfer_records_a_finalized_event_that_verifies(self):
        with FaultRoot() as root:
            downloader, db, engine, staging = self.make_controller(root)
            self.assertEqual(downloader.transfer(downloader.next_pending(db), db), "complete")
            events = self.events(downloader)
            self.assertEqual([event["event_type"] for event in events],
                             ["run_started", "attempt_finished", "finalized"])
            finalized = events[2]
            self.assertEqual(finalized["item_id"], downloader.item_id(self.url))
            self.assertEqual(finalized["sha256"], hashlib.sha256(self.payload).hexdigest())
            downloader.provenance.close([], {"max_files": 1}, 1, {"complete": 1}, "finished")
            self.assertEqual(verify(downloader.provenance.directory, downloader.destination,
                                    self.public_key(root, downloader), None), [])
            db.close()

    REDIRECT_LOG = (
        "2026-09-18 20:36:58.901715 [INFO] [HttpConnection.cc:163] CUID#7 - Response received:\n"
        "HTTP/1.0 302 Found\nLocation: /hop\n\n\n"
        "2026-09-18 20:36:58.901759 [NOTICE] [HttpResponse.cc:168] CUID#7 - "
        "Redirecting to http://example.test/hop\n"
        "2026-09-18 20:36:58.902361 [INFO] [HttpConnection.cc:163] CUID#7 - Response received:\n"
        "HTTP/1.1 302 Found\nLocation: /f\n\n\n"
        "2026-09-18 20:36:58.902400 [NOTICE] [HttpResponse.cc:168] CUID#7 - "
        "Redirecting to http://example.test/f\n"
        "2026-09-18 20:36:58.903000 [INFO] [HttpConnection.cc:163] CUID#7 - Response received:\n"
        "HTTP/1.1 206 Partial Content\nContent-Range: bytes 10-99/100\nContent-Length: 90\n"
        "ETag: \"v1\"\nLast-Modified: Wed, 01 Jan 2025 00:00:00 GMT\n"
        "Content-Type: application/octet-stream\nSet-Cookie: secret=1\n\n\n")

    def test_response_log_yields_the_last_response_and_the_intermediate_redirects(self):
        parsed = tod_dl.parse_aria2_response_log(self.REDIRECT_LOG, "http://example.test/a")
        self.assertEqual(parsed["final_url"], "http://example.test/f")
        self.assertEqual(parsed["redirect_chain"], ["http://example.test/hop"])
        self.assertEqual(parsed["http_status"], 206)
        self.assertEqual(parsed["response_content_range"], "bytes 10-99/100")
        self.assertEqual(parsed["response_content_length"], "90")
        self.assertEqual(parsed["response_etag"], '"v1"')
        self.assertEqual(parsed["response_last_modified"], "Wed, 01 Jan 2025 00:00:00 GMT")
        self.assertNotIn("secret", json.dumps(parsed))
        self.assertEqual(tod_dl.expected_size_from_response(parsed), 100)

    def test_response_log_never_returns_a_url_with_credentials(self):
        log = self.REDIRECT_LOG.replace("http://example.test/hop", "http://user:pw@example.test/hop")
        parsed = tod_dl.parse_aria2_response_log(log, "http://example.test/a")
        self.assertIsNone(parsed["final_url"])
        self.assertEqual(parsed["redirect_chain"], [])
        self.assertNotIn("pw", json.dumps(parsed))

    def test_response_log_without_a_response_is_empty(self):
        self.assertEqual(tod_dl.parse_aria2_response_log("", "http://example.test/a"), {})

    def test_full_body_length_is_the_expected_size_only_for_a_200_response(self):
        self.assertEqual(tod_dl.expected_size_from_response(
            {"http_status": 200, "response_content_length": "12"}), 12)
        self.assertIsNone(tod_dl.expected_size_from_response(
            {"http_status": 206, "response_content_length": "12"}))
        self.assertIsNone(tod_dl.expected_size_from_response({}))

    def test_capture_response_keeps_parsed_fields_and_deletes_the_raw_log(self):
        with FaultRoot() as root:
            downloader, db, engine, staging = self.make_controller(root)
            raw = root / "aria2.response.log"
            raw.write_text(self.REDIRECT_LOG, encoding="utf-8")
            downloader.capture_response(self.url, raw)
            self.assertFalse(raw.exists())
            self.assertEqual(downloader.attempt_responses[self.url]["http_status"], 206)
            downloader.capture_response(self.url, raw)
            self.assertEqual(downloader.attempt_responses[self.url], {})
            db.close()

    def test_attempt_and_finalized_events_record_the_observed_response(self):
        with FaultRoot() as root:
            downloader, db, engine, staging = self.make_controller(root)

            def run(*_):
                downloader.attempt_responses[self.url] = tod_dl.parse_aria2_response_log(
                    self.REDIRECT_LOG.replace("100", str(len(self.payload))).replace(
                        "bytes 10-", "bytes 0-"), self.url)
                return engine.complete(staging)
            downloader.run_aria2 = run
            self.assertEqual(downloader.transfer(downloader.next_pending(db), db), "complete")
            attempt, finalized = self.events(downloader)[1:]
            self.assertEqual(attempt["http_status"], 206)
            self.assertEqual(attempt["response_etag"], '"v1"')
            self.assertEqual(attempt["redirect_chain"], ["http://example.test/hop"])
            self.assertEqual(finalized["etag"], {"available": True, "compared": False,
                                                 "value": '"v1"'})
            self.assertEqual(finalized["expected_size"],
                             {"available": True, "compared": True, "value": len(self.payload)})
            self.assertEqual(finalized["last_modified"]["value"], "Wed, 01 Jan 2025 00:00:00 GMT")
            downloader.provenance.close([], {"max_files": 1}, 1, {"complete": 1}, "finished")
            self.assertEqual(verify(downloader.provenance.directory, downloader.destination,
                                    self.public_key(root, downloader), None), [])
            db.close()

    def test_announced_size_that_differs_from_the_staged_bytes_blocks_promotion(self):
        with FaultRoot() as root:
            downloader, db, engine, staging = self.make_controller(root)

            def run(*_):
                downloader.attempt_responses[self.url] = {
                    "http_status": 200, "response_content_length": str(len(self.payload) + 1)}
                return engine.complete(staging)
            downloader.run_aria2 = run
            with redirect_stdout(StringIO()):
                self.assertEqual(downloader.transfer(downloader.next_pending(db), db), "review")
            status, code = db.execute("SELECT status, review_code FROM downloads WHERE url=?",
                                      (self.url,)).fetchone()
            self.assertEqual((status, code), ("review_required", "size_mismatch"))
            self.assertFalse((downloader.destination / "first" / "data" / "item.bin").exists())
            self.assertFalse(staging.exists())
            candidates = [p for p in downloader.candidates.rglob("*") if p.is_file()]
            self.assertEqual([p.read_bytes() for p in candidates], [self.payload])
            events = self.events(downloader)
            self.assertEqual(events[-2]["outcome"], "validation_failed")
            self.assertEqual(events[-1]["finalization_failure_reason"], "validation_failed")
            db.close()

    def test_finalized_event_marks_the_etag_compared_only_after_a_confirmed_resume(self):
        with FaultRoot() as root:
            downloader, db, engine, staging = self.make_controller(root)
            engine.partial(staging)
            db.execute("UPDATE downloads SET etag='\"v1\"' WHERE url=?", (self.url,))
            db.commit()
            downloader.check_representation = lambda *_: ("confirmed_unchanged", {"etag": '"v1"'})
            self.assertEqual(downloader.transfer(downloader.next_pending(db), db), "complete")
            finalized = self.events(downloader)[-1]
            self.assertEqual(finalized["etag"], {"available": True, "compared": True,
                                                 "value": '"v1"'})
            self.assertEqual(finalized["expected_size"]["available"], False)
            db.close()

    def test_finalized_event_reports_unavailable_validators_when_none_were_observed(self):
        with FaultRoot() as root:
            downloader, db, engine, staging = self.make_controller(root)
            self.assertEqual(downloader.transfer(downloader.next_pending(db), db), "complete")
            finalized = self.events(downloader)[-1]
            unavailable = {"available": False, "compared": False, "value": None}
            for field in ("expected_size", "etag", "last_modified"):
                self.assertEqual(finalized[field], unavailable)
            db.close()

    def test_403_and_401_are_access_denied_and_are_not_retried(self):
        self.assertTrue(tod_dl.is_access_denied_failure(
            "errorCode=22 The response status is not successful. status=403"))
        self.assertTrue(tod_dl.is_access_denied_failure("errorCode=24 Authorization failed."))
        self.assertFalse(tod_dl.is_access_denied_failure(
            "errorCode=22 The response status is not successful. status=4030"))
        for error in ("errorCode=22 The response status is not successful. status=403",
                      "errorCode=24 Authorization failed."):
            with FaultRoot() as root:
                downloader, db, engine, staging = self.make_controller(root)

                def run(*_):
                    downloader.attempt_responses[self.url] = {"http_status": 403,
                                                              "final_url": self.url}
                    return False, error
                downloader.run_aria2 = run
                with redirect_stdout(StringIO()):
                    self.assertEqual(downloader.transfer(downloader.next_pending(db), db),
                                     "review")
                status, code, retry_at = db.execute(
                    "SELECT status, review_code, next_retry_at FROM downloads WHERE url=?",
                    (self.url,)).fetchone()
                self.assertEqual((status, code), ("review_required", "access_denied"))
                self.assertFalse(retry_at)
                attempt = self.events(downloader)[-1]
                self.assertEqual((attempt["outcome"], attempt["http_status"]), ("access_denied", 403))
                db.close()

    def test_unavailable_attempt_records_the_response_status(self):
        with FaultRoot() as root:
            downloader, db, engine, staging = self.make_controller(root)

            def run(*_):
                downloader.attempt_responses[self.url] = {"http_status": 404}
                return False, "errorCode=3 Resource not found"
            downloader.run_aria2 = run
            with redirect_stdout(StringIO()):
                self.assertEqual(downloader.transfer(downloader.next_pending(db), db),
                                 "unavailable")
            attempt = self.events(downloader)[-1]
            self.assertEqual((attempt["outcome"], attempt["http_status"]), ("unavailable", 404))
            self.assertIsNone(attempt["final_url"])
            db.close()

    def test_failed_record_write_stops_admission_and_leaves_the_item_unresolved(self):
        with FaultRoot() as root:
            downloader, db, engine, staging = self.make_controller(root)
            downloader.provenance.events_path = root / "missing" / "events.jsonl"
            with self.assertRaisesRegex(RuntimeError, "local provenance failure"):
                downloader.transfer(downloader.next_pending(db), db)
            self.assertTrue(downloader.stop_requested.is_set())
            self.assertEqual(downloader.local_failure_code, "record_write")
            self.assertNotEqual(db.execute("SELECT status FROM downloads WHERE url=?",
                                           (self.url,)).fetchone()[0], "complete")
            db.close()

    def test_candidate_reasons_use_only_the_specified_values(self):
        with FaultRoot() as root:
            downloader, db, engine, staging = self.make_controller(root)
            staging.parent.mkdir(parents=True, exist_ok=True)
            staging.write_bytes(self.payload)
            candidate = downloader.candidates / "first" / "item.bin"
            candidate.parent.mkdir(parents=True, exist_ok=True)
            candidate.write_bytes(self.payload)
            expected = {"incomplete_body": "validation_failed",
                        "checksum_mismatch": "validation_failed",
                        "changed_remote_representation": "recovery_review",
                        "no_reliable_version_protection": "recovery_review",
                        "destination_collision": "destination_collision"}
            for cause, reason in expected.items():
                downloader.candidate_event(self.url, staging, candidate, None, cause)
                self.assertEqual(self.events(downloader)[-1]["finalization_failure_reason"], reason)
            db.close()

    def test_disk_full_records_a_stopped_attempt_and_a_storage_local_failure(self):
        with FaultRoot() as root:
            downloader, db, engine, staging = self.make_controller(root)
            downloader.run_aria2 = lambda *_: (False, "errorCode=9 No space left on device")
            self.assertEqual(downloader.transfer(downloader.next_pending(db), db), "local_failure")
            events = self.events(downloader)
            self.assertEqual([event["event_type"] for event in events],
                             ["run_started", "attempt_finished", "local_failure"])
            self.assertEqual(events[1]["outcome"], "stopped")
            self.assertEqual(events[2]["failure_code"], "storage")
            self.assertEqual(downloader.local_failure_code, "storage")
            db.close()

    def test_sqlite_failure_records_a_database_write_local_failure(self):
        with FaultRoot() as root:
            downloader, db, engine, staging = self.make_controller(root)
            db.close()
            with self.assertRaisesRegex(RuntimeError, "local SQLite failure"):
                downloader.transition(db, self.url, "admitted")
            failure = self.events(downloader)[-1]
            self.assertEqual((failure["event_type"], failure["failure_code"]),
                             ("local_failure", "database_write"))


class TelemetryTests(unittest.TestCase):
    def test_projection_hydrates_and_updates_without_snapshot_sql(self):
        url = "https://fixture.test/first/data/item.bin"
        with tempfile.TemporaryDirectory() as temporary:
            downloader, db = RunSelectionTests().prepare(Path(temporary), [url])
            downloader.scope_run(db)
            downloader.update(db, "UPDATE downloads SET status='retry_wait', bytes=3, "
                              "next_retry_at=123, last_error=? WHERE url=?",
                              ("bad\x1b  retry", url))

            projection = TelemetryStateProjection.hydrate(db, downloader.run_id, 0)
            before = projection.copy()
            self.assertEqual(before["counts"]["retry"], 1)
            self.assertEqual(before["retry_error"], "bad? retry")
            self.assertEqual(before["retry_count"], 1)

            downloader.projection = projection
            downloader.transition(db, url, "complete", bytes=7)
            after = projection.copy()
            self.assertEqual(after["counts"]["retry"], 0)
            self.assertEqual(after["counts"]["complete"], 1)
            self.assertEqual(after["complete_bytes"], 7)
            self.assertGreater(after["revision"], before["revision"])
            db.close()

    def test_reducer_does_not_count_resume_baseline_as_session_progress(self):
        metrics = reduce_metrics(
            [{"url": "one", "status": "complete", "bytes": 10},
             {"url": "two", "status": "active", "bytes": None}],
            [{"url": "two", "received_bytes": 5, "total_bytes": 20,
              "speed_bps": 2}],
            100.0, 110.0,
        )
        self.assertEqual(metrics["complete_bytes"]["value"], 10)
        self.assertEqual(metrics["retained_bytes"]["value"], 15)
        self.assertEqual(metrics["known_remaining_bytes"]["value"], 15)
        self.assertIsNone(metrics["session_received_bytes"]["value"])
        self.assertEqual(metrics["eta_seconds"], {"value": 7.5, "quality": "exact"})
        self.assertEqual(metrics["eta_reason"], "based on current aria2 RPC speed")

    def test_eta_requires_total_received_and_positive_speed(self):
        self.assertEqual(estimate_eta_seconds(20, 100, 10), 8)
        self.assertIsNone(estimate_eta_seconds(20, None, 10))
        self.assertIsNone(estimate_eta_seconds(20, 100, 0))

    def test_publisher_writes_bounded_atomic_snapshot(self):
        with tempfile.TemporaryDirectory() as temporary:
            publisher = TelemetryPublisher(Path(temporary), "test-run", 1)
            publisher.start(lambda lifecycle, reason, active, validation, events: {
                "state_revision": 0, "run": {"lifecycle": lifecycle, "reason": reason},
                "workers": active, "validation": validation, "health": {},
                "recent_events": events,
            })
            publisher.event("info", "test", "a\x1b message")
            publisher.set_lifecycle("finished")
            publisher.stop()
            publisher.publish()
            snapshot = json.loads(publisher.path.read_text(encoding="utf-8"))
            self.assertEqual(snapshot["schema_version"], 2)
            self.assertEqual(snapshot["run_id"], "test-run")
            self.assertEqual(snapshot["run"]["lifecycle"], "finished")
            self.assertNotIn("\x1b", snapshot["recent_events"][0]["message"])

    def test_publisher_refreshes_idle_countdowns_twice_per_second(self):
        with tempfile.TemporaryDirectory() as temporary:
            publisher = TelemetryPublisher(Path(temporary), "test-run", 1)
            publisher.start(lambda lifecycle, reason, active, validation, events: {
                "state_revision": 0, "run": {"lifecycle": lifecycle, "reason": reason},
                "workers": active, "validation": validation, "health": {},
                "recent_events": events,
            })
            time.sleep(PUBLISH_INTERVAL_SECONDS * 2.4)
            publisher.stop()
            snapshot = json.loads(publisher.path.read_text(encoding="utf-8"))
            self.assertGreaterEqual(snapshot["sequence"], 3)

    def test_publisher_records_only_received_byte_growth_as_payload_progress(self):
        with tempfile.TemporaryDirectory() as temporary:
            publisher = TelemetryPublisher(Path(temporary), "test-run", 1)
            publisher.set_active("https://fixture.test/item", 1, 1, "downloading")
            publisher.update_sample("https://fixture.test/item", 10, 20, 1, 1)
            recorded, completed, stopped = publisher.time_status_copy()
            self.assertIsNotNone(recorded)
            self.assertIsNone(completed)
            self.assertIsNone(stopped)
            publisher.update_sample("https://fixture.test/item", 10, 20, 1, 1)
            self.assertEqual(publisher.time_status_copy()[0], recorded)


class ShutdownSignalTests(unittest.TestCase):
    def test_sigterm_during_run_exits_143_and_records_close_reason(self):
        url = "https://fixture.test/first/data/item.bin"
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            queue = root / "queue.txt"
            queue.write_text(url + "\n", encoding="utf-8")
            args = make_args(root, queue)
            args.tor_control_address = "127.0.0.1:9051"
            args.tor_control_cookie = root / "control.authcookie"
            args.retry_now = False
            args.time_limit = 0
            args.progress_interval = 30
            args.reserve_bytes = 0
            downloader = Downloader(args)

            original_verify = tod_dl.verify_tor_isolation

            def raise_sigterm_then_verify(address, cookie):
                os.kill(os.getpid(), signal.SIGTERM)
                return ["9050"]

            tod_dl.verify_tor_isolation = raise_sigterm_then_verify
            try:
                exit_code = downloader.run()
            finally:
                tod_dl.verify_tor_isolation = original_verify

            self.assertEqual(exit_code, 143)
            self.assertEqual(signal.getsignal(signal.SIGTERM), signal.SIG_DFL)
            events = downloader.provenance.events_path.read_text(encoding="utf-8").splitlines()
            closed = [json.loads(line) for line in events if '"run_closed"' in line][0]
            self.assertEqual(closed["close_reason"], "interrupted")

    def test_sigint_during_run_exits_130_and_records_close_reason(self):
        url = "https://fixture.test/first/data/item.bin"
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            queue = root / "queue.txt"
            queue.write_text(url + "\n", encoding="utf-8")
            args = make_args(root, queue)
            args.tor_control_address = "127.0.0.1:9051"
            args.tor_control_cookie = root / "control.authcookie"
            args.retry_now = False
            args.time_limit = 0
            args.progress_interval = 30
            args.reserve_bytes = 0
            downloader = Downloader(args)

            original_verify = tod_dl.verify_tor_isolation

            def raise_sigint_then_verify(address, cookie):
                os.kill(os.getpid(), signal.SIGINT)
                return ["9050"]

            tod_dl.verify_tor_isolation = raise_sigint_then_verify
            try:
                exit_code = downloader.run()
            finally:
                tod_dl.verify_tor_isolation = original_verify

            self.assertEqual(exit_code, 130)
            self.assertEqual(signal.getsignal(signal.SIGINT), signal.default_int_handler)
            events = downloader.provenance.events_path.read_text(encoding="utf-8").splitlines()
            closed = [json.loads(line) for line in events if '"run_closed"' in line][0]
            self.assertEqual(closed["close_reason"], "interrupted")

    def test_exclusive_ownership_conflicts_across_state_dirs_sharing_destination(self):
        url = "https://fixture.test/first/data/item.bin"
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            queue = root / "queue.txt"
            queue.write_text(url + "\n", encoding="utf-8")
            args_second = make_args(root, queue)
            args_second.state = root / "state-b"
            downloader_second = Downloader(args_second)
            root.joinpath("destination").mkdir(parents=True)
            held_lock = (root / "destination" / "acquisition.lock").open("a+")
            fcntl.flock(held_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            try:
                with self.assertRaisesRegex(
                        RuntimeError, "another acquisition supervisor holds the lock"):
                    downloader_second.run()
            finally:
                fcntl.flock(held_lock, fcntl.LOCK_UN)
                held_lock.close()


if __name__ == "__main__":
    unittest.main()
