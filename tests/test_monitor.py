"""Tests for the read-only version-1 telemetry monitor."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from controller import (ControlError, ControlServer, control_request,
                        get_control_state, read_control_session)
from inspection import (InspectionError, InspectionServer, inspection_request,
                        read_inspection_session)
from monitor import (SnapshotError, event_item_path, event_message_style,
                     event_timestamp, freshness, literal_text, read_snapshot,
                     marquee_filename, retry_summary, event_severity_style,
                     event_worker_label, format_countdown, freshness_style,
                     lifecycle_style, select_snapshot, worker_phase_label,
                     truncate_filename, SpeedTrend, disk_status, progress_status,
                     screen_summary, validate_snapshot, detail_bytes, item_details_text,
                     worker_details_text, format_retry_deadline)


def snapshot(lifecycle: str = "running") -> dict:
    return {
        "schema_version": 1,
        "run_id": "run-one",
        "session_id": "session-one",
        "sequence": 1,
        "published_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "session_elapsed_s": 1,
        "state_revision": 1,
        "run": {
            "lifecycle": lifecycle,
            "selected_count": 1,
            "counts": {"queued": 1, "busy": 0, "retry": 0, "exhausted": 0,
                       "complete": 0, "existing_unverified": 0,
                       "review_required": 0, "unavailable": 0, "unknown": 0},
            "metrics": {},
        },
        "workers": [], "validation": [], "health": {}, "recent_events": [],
    }


class MonitorTests(unittest.TestCase):
    def test_inspection_endpoint_reads_redacted_item_records(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            database = root / "manifest.sqlite"
            db = sqlite3.connect(database)
            db.executescript("""
                CREATE TABLE downloads (
                    url TEXT PRIMARY KEY, relative_path TEXT NOT NULL,
                    storage_path TEXT, staging_path TEXT, inventory_size TEXT,
                    status TEXT NOT NULL, attempts INTEGER NOT NULL, bytes INTEGER,
                    sha256 TEXT, last_error TEXT, next_retry_at REAL NOT NULL,
                    promotion_target TEXT, updated_at TEXT NOT NULL, review_code TEXT
                );
                CREATE TABLE run_items (run_id TEXT NOT NULL, url TEXT NOT NULL,
                                        queue_rank INTEGER NOT NULL);
                CREATE TABLE telemetry_revisions (run_id TEXT PRIMARY KEY,
                                                  revision INTEGER NOT NULL);
                CREATE TABLE download_transitions (id INTEGER PRIMARY KEY,
                    url TEXT, run_id TEXT, to_status TEXT, detail TEXT,
                    recorded_at TEXT);
            """)
            source = "http://user:secret@example.onion/a/file.txt?token=secret"
            db.execute("INSERT INTO downloads VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                       (source, "a/file.txt", "safe/file.txt", None, None, "pending",
                        0, None, None, None, 0, None, "2026-09-17T00:00:00Z", None))
            db.execute("INSERT INTO run_items VALUES (?, ?, ?)",
                       ("run-one", source, 1))
            db.execute("INSERT INTO telemetry_revisions VALUES (?, ?)",
                       ("run-one", 7))
            db.commit()
            db.close()
            server = InspectionServer(
                root, "run-one", "session-one", database, 3,
                runtime_provider=lambda: [{"url": source, "worker_id": 1,
                                           "phase": "downloading", "generation": 1,
                                           "attempt_id": "attempt-1", "attempt_number": 1,
                                           "received_bytes": 0, "total_bytes": 10,
                                           "sample_age_s": 0, "sample_sequence": 3,
                                           "progress_samples": []}])
            server.start()
            try:
                descriptor = read_inspection_session(root, "run-one")
                self.assertNotIn("token", descriptor)
                self.assertEqual((root / "telemetry" / "run-one" /
                                  "inspection-session.json").stat().st_mode & 0o777, 0o600)
                page = inspection_request(root, "run-one", "list_queue")
                item_id = page["data"]["rows"][0]["item_id"]
                hidden = inspection_request(root, "run-one", "get_item",
                                            {"item_id": item_id})
                self.assertEqual(hidden["state_revision"], 7)
                self.assertEqual(hidden["data"]["item"]["source"], None)
                shown = inspection_request(root, "run-one", "get_item",
                                           {"item_id": item_id,
                                            "reveal_source": True})
                self.assertEqual(shown["data"]["item"]["source"],
                                 "http://example.onion/a/file.txt")
                worker = inspection_request(root, "run-one", "get_worker",
                                            {"worker_id": 1})["data"]["worker"]
                self.assertEqual(worker["assignment"]["item_id"], item_id)
                self.assertEqual(worker["assignment"]["basename"], "file.txt")
                self.assertEqual(worker["received_bytes"], 0)
                self.assertEqual(worker["quality"], "exact")
                with self.assertRaisesRegex(InspectionError, "not found"):
                    inspection_request(root, "run-one", "get_item",
                                       {"item_id": "0" * 64})
            finally:
                server.stop()

    def test_local_control_endpoint_requires_its_session_capability(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            server = ControlServer(root, "run-one", "session-one",
                                   lambda: {"actions": {"get_control_state": "available"}})
            server.start()
            try:
                self.assertEqual(get_control_state(root, "run-one")["actions"],
                                 {"get_control_state": "available"})
                session_path = root / "control" / "run-one" / "session.json"
                self.assertEqual(session_path.stat().st_mode & 0o777, 0o600)
                session = read_control_session(root, "run-one")
                session["token"] = "wrong"
                session_path.write_text(json.dumps(session), encoding="utf-8")
                with self.assertRaisesRegex(ControlError, "invalid"):
                    get_control_state(root, "run-one")
            finally:
                server.stop()

    def test_retry_now_requires_confirmation_and_replays_request_id(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            commands = []
            server = ControlServer(root, "run-one", "session-one", lambda: {},
                                   lambda request: commands.append(request) or {
                                       "outcome": "completed", "reason": "accepted",
                                       "state_revision": 3,
                                   })
            server.start()
            try:
                prepared = control_request(root, "run-one", "prepare_confirmation",
                                           {"action": "retry_now"})["confirmation"]
                with self.assertRaisesRegex(ControlError, "invalid or expired"):
                    control_request(root, "run-one", "retry_now", {"nonce": prepared["nonce"],
                                                                       "confirmation": "wrong"})
                prepared = control_request(root, "run-one", "prepare_confirmation",
                                           {"action": "retry_now"})["confirmation"]
                parameters = {"nonce": prepared["nonce"], "confirmation": "retry_now"}
                first = control_request(root, "run-one", "retry_now", parameters,
                                        request_id="request-one")
                second = control_request(root, "run-one", "retry_now", parameters,
                                         request_id="request-one")
                self.assertEqual(first, second)
                self.assertEqual(len(commands), 1)
            finally:
                server.stop()

    def test_tor_renewal_requires_confirmation_and_replays_request_id(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            commands = []
            server = ControlServer(root, "run-one", "session-one", lambda: {},
                                   lambda request: commands.append(request) or {
                                       "outcome": "completed",
                                       "reason": "Tor accepted a request for new future streams",
                                   })
            server.start()
            try:
                prepared = control_request(root, "run-one", "prepare_confirmation",
                                           {"action": "renew_tor_circuits"})["confirmation"]
                parameters = {"nonce": prepared["nonce"],
                              "confirmation": "renew_tor_circuits"}
                first = control_request(root, "run-one", "renew_tor_circuits", parameters,
                                        request_id="renew-one")
                second = control_request(root, "run-one", "renew_tor_circuits", parameters,
                                         request_id="renew-one")
                self.assertEqual(first, second)
                self.assertEqual(len(commands), 1)
                self.assertEqual(commands[0]["action"], "renew_tor_circuits")
            finally:
                server.stop()

    def test_rejects_counts_that_do_not_reconcile(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "bad.json"
            value = snapshot()
            value["run"]["counts"]["queued"] = 2
            path.write_text(json.dumps(value), encoding="utf-8")
            with self.assertRaisesRegex(SnapshotError, "must sum"):
                read_snapshot(path)

    def test_marks_old_running_snapshot_disconnected(self):
        value = snapshot()
        value["published_at"] = "2000-01-01T00:00:00+00:00"
        self.assertEqual(freshness(value), "disconnected")

    def test_selects_only_one_valid_live_snapshot(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "telemetry" / "run-one"
            root.mkdir(parents=True)
            (root / "snapshot.json").write_text(json.dumps(snapshot()), encoding="utf-8")
            self.assertEqual(select_snapshot(Path(temporary), None)["run_id"], "run-one")

    def test_rejects_negative_worker_counter_and_neutralizes_controls(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "bad.json"
            value = snapshot()
            value["workers"] = [{"worker_id": 1, "received_bytes": -1}]
            path.write_text(json.dumps(value), encoding="utf-8")
            with self.assertRaisesRegex(SnapshotError, "nonnegative"):
                read_snapshot(path)
        self.assertEqual(literal_text("bad\x1b[31m"), "bad\\x1b[31m")

    def test_displays_retry_deadline_and_reason(self):
        value = snapshot()
        value["health"] = {"retry_remaining_s": 125, "cooldown_remaining_s": 0,
                           "retry_error": "connection timed out"}
        self.assertIn("~2m 5s", retry_summary(value))
        self.assertIn("connection timed out", retry_summary(value))

    def test_labels_an_immediately_eligible_retry(self):
        value = snapshot()
        value["health"] = {"retry_pending_count": 1, "retry_remaining_s": 0,
                           "cooldown_remaining_s": 0}
        self.assertEqual(retry_summary(value),
                         "Retry eligible now; waiting for controller admission")

    def test_cooldown_is_explained_in_retry_and_worker_labels(self):
        value = snapshot()
        value["health"] = {"retry_pending_count": 1, "retry_remaining_s": 0,
                           "cooldown_remaining_s": 12}
        self.assertEqual(retry_summary(value), "Retry eligible now; SOCKS cooldown 12s")
        self.assertEqual(worker_phase_label({"phase": "cooldown"}, value),
                         "cooldown 12s")
        self.assertEqual(format_countdown(72), "1m 12s")
        self.assertEqual(format_countdown(3660), "1h 1m")

    def test_downloading_worker_becomes_stalled_after_sixty_seconds_without_progress(self):
        value = snapshot()
        self.assertEqual(worker_phase_label(
            {"phase": "downloading", "last_progress_age_s": 59}, value), "downloading")
        self.assertEqual(worker_phase_label(
            {"phase": "downloading", "last_progress_age_s": 60}, value), "stalled 1m 0s")

    def test_finished_run_without_retries_has_a_final_summary(self):
        value = snapshot("finished")
        self.assertEqual(retry_summary(value), "No retries pending")

    def test_version_two_payload_progress_and_disk_status_are_displayed(self):
        value = snapshot()
        value["schema_version"] = 2
        value["run"].update({
            "last_payload_progress_at": (dt.datetime.now(dt.timezone.utc)
                                        - dt.timedelta(seconds=8)).isoformat(),
            "last_completion_at": (dt.datetime.now(dt.timezone.utc)
                                   - dt.timedelta(seconds=4)).isoformat(),
            "completed_at": None,
            "stopped_at": None,
            "metrics": {"speed_bps": {"value": 100, "quality": "exact"}},
        })
        value["workers"] = [{"worker_id": 1, "phase": "downloading",
                             "received_bytes": 1, "total_bytes": 2}]
        value["health"] = {"filesystems": [{
            "filesystem_id": "100", "roles": ["destination", "state"],
            "free_bytes": 1000, "reserve_bytes": 500, "headroom_bytes": 500,
        }]}
        validate_snapshot(value)
        self.assertIn("Last complete", progress_status(value))
        self.assertIn("Disk  ", disk_status(value))
        self.assertIn("Session 00:00:01", screen_summary(value, "Collecting"))

    def test_idle_run_prefers_last_payload_progress_over_last_complete(self):
        value = snapshot()
        value["run"].update({
            "last_payload_progress_at": (dt.datetime.now(dt.timezone.utc)
                                        - dt.timedelta(seconds=30)).isoformat(),
            "last_completion_at": (dt.datetime.now(dt.timezone.utc)
                                   - dt.timedelta(seconds=10)).isoformat(),
        })
        self.assertIn("Last payload progress", progress_status(value))

    def test_cooldown_and_unknown_worker_counters_prefer_last_payload_progress(self):
        value = snapshot()
        value["run"].update({
            "last_payload_progress_at": (dt.datetime.now(dt.timezone.utc)
                                        - dt.timedelta(seconds=30)).isoformat(),
            "last_completion_at": (dt.datetime.now(dt.timezone.utc)
                                   - dt.timedelta(seconds=10)).isoformat(),
        })
        value["workers"] = [{"worker_id": 1, "phase": "downloading",
                             "received_bytes": 1, "total_bytes": 2}]
        value["health"] = {"cooldown_remaining_s": 1}
        self.assertIn("Last payload progress", progress_status(value))
        value["health"] = {}
        value["workers"][0].update({"received_bytes": None, "total_bytes": None})
        self.assertIn("Last payload progress", progress_status(value))
        value["run"]["last_payload_progress_at"] = None
        self.assertIn("Last complete", progress_status(value))

    def test_speed_trend_labels_a_rising_exact_series(self):
        trend = SpeedTrend()
        for sequence in range(1, 11):
            value = snapshot()
            value["sequence"] = sequence
            value["run"]["metrics"] = {
                "speed_bps": {"value": sequence * 100, "quality": "exact"},
            }
            trend.observe(value, observed_at=float(sequence * 4))
        self.assertEqual(trend.label(), "Rising")

    def test_event_severity_styles_distinguish_log_levels(self):
        self.assertEqual(event_severity_style("info"), "cyan")
        self.assertEqual(event_severity_style("warning"), "bold yellow")
        self.assertEqual(event_severity_style("error"), "bold red")
        self.assertEqual(event_severity_style("other"), "dim")

    def test_header_status_styles_distinguish_lifecycle_and_freshness(self):
        self.assertEqual(lifecycle_style("running"), "bold green")
        self.assertEqual(lifecycle_style("stopped"), "bold yellow")
        self.assertEqual(lifecycle_style("unexpected"), "bold red")
        self.assertEqual(freshness_style("live"), "green")
        self.assertEqual(freshness_style("disconnected"), "red")

    def test_event_worker_label_includes_valid_worker_id_only(self):
        self.assertEqual(event_worker_label({"worker_id": 3}), "W3")
        self.assertEqual(event_worker_label({"worker_id": None}), "")
        self.assertEqual(event_worker_label({"worker_id": 0}), "")
        self.assertEqual(event_worker_label({"worker_id": True}), "")

    def test_completion_event_message_is_green(self):
        self.assertEqual(event_message_style({"category": "complete"}), "green")
        self.assertEqual(event_message_style({"category": "retry"}), "")

    def test_transfer_events_show_only_the_decoded_item_path(self):
        event = {"category": "complete",
                 "item_id": "http://example.onion/root/Some%20File%20%26%20Notes.pdf"}
        self.assertEqual(event_item_path(event), "/root/Some File & Notes.pdf")
        self.assertEqual(event_item_path({"category": "retry", "item_id": event["item_id"]}), "")

    def test_event_timestamp_uses_local_clock_and_rejects_invalid_values(self):
        recorded = "2026-09-15T19:10:26+00:00"
        expected = dt.datetime.fromisoformat(recorded).astimezone().strftime("%H:%M:%S")
        self.assertEqual(event_timestamp({"at": recorded}), expected)
        self.assertEqual(event_timestamp({"at": "not-a-time"}), "--:--:--")
        self.assertEqual(event_timestamp({}), "--:--:--")

    def test_truncates_filename_without_losing_extension(self):
        name = "a-very-long-attachment-name-that-must-not-fill-the-table.pdf"
        self.assertEqual(truncate_filename(name, 24), "a-very-long-attach...pdf")

    def test_marquee_rotates_long_basename_and_neutralizes_controls(self):
        name = "long-name-with-control\x1b-and-extension.pdf"
        self.assertEqual(marquee_filename(name, 0, 12), "long-name-wi")
        self.assertIn("\\x1b", marquee_filename(name, 16, 12))
        self.assertNotEqual(marquee_filename(name, 0, 12), marquee_filename(name, 1, 12))

    def test_inspection_attempt_history_is_ordered_and_paged(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            database = root / "manifest.sqlite"
            db = sqlite3.connect(database)
            db.executescript("""
                CREATE TABLE downloads (url TEXT PRIMARY KEY, relative_path TEXT NOT NULL, storage_path TEXT, staging_path TEXT, inventory_size TEXT, status TEXT NOT NULL, attempts INTEGER NOT NULL, bytes INTEGER, sha256 TEXT, last_error TEXT, next_retry_at REAL NOT NULL, promotion_target TEXT, updated_at TEXT NOT NULL, review_code TEXT);
                CREATE TABLE run_items (run_id TEXT, url TEXT, queue_rank INTEGER);
                CREATE TABLE telemetry_revisions (run_id TEXT PRIMARY KEY, revision INTEGER);
                CREATE TABLE download_attempts (run_id TEXT, url TEXT, attempt_number INTEGER, attempt_id TEXT, generation TEXT, started_at TEXT, ended_at TEXT, outcome TEXT, error_category TEXT, error_message TEXT, retry_at TEXT);
            """)
            url = "http://example.onion/a"
            db.execute("INSERT INTO downloads VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                       (url, "a", None, None, None, "retry_wait", 3, 9, None, None, 0, None, "2026-09-17T00:00:00Z", None))
            db.execute("INSERT INTO run_items VALUES (?, ?, ?)", ("run-one", url, 1))
            db.execute("INSERT INTO telemetry_revisions VALUES (?, ?)", ("run-one", 4))
            for number, identifier in ((3, "b"), (3, "a"), (2, "z")):
                db.execute("INSERT INTO download_attempts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                           ("run-one", url, number, identifier, None, None, None, "failed", None, None, None))
            db.commit()
            db.close()
            server = InspectionServer(root, "run-one", "session-one", database, 3)
            server.start()
            try:
                item_id = hashlib.sha256(url.encode()).hexdigest()
                first = inspection_request(root, "run-one", "list_attempts", {"item_id": item_id, "page_size": 2})
                self.assertEqual([(row["attempt_number"], row["attempt_id"]) for row in first["data"]["attempts"]], [(3, "a"), (3, "b")])
                second = inspection_request(root, "run-one", "list_attempts", {"item_id": item_id, "page_size": 2, "cursor": first["data"]["next_cursor"]})
                self.assertEqual(second["data"]["attempts"][0]["attempt_number"], 2)
            finally:
                server.stop()

    def test_item_details_render_literal_values_and_exact_bytes(self):
        item = {"item_id": "a" * 64, "logical_path": "<tag>\x1b[31m", "received_bytes": 0, "source_label": "Source hidden", "source": None}
        rendered = item_details_text(item, "2026-09-17T00:00:00Z", 2)
        self.assertIn("<tag>\\x1b[31m", rendered)
        self.assertIn("0 B (0 bytes)", rendered)
        self.assertEqual(detail_bytes(None), "? (not recorded)")

    def test_worker_details_keep_assignment_identity_and_clear_idle_values(self):
        worker = {"worker_id": 2, "phase": "downloading", "last_progress_age_s": 60,
                  "received_bytes": 0, "total_bytes": None, "quality": "exact",
                  "assignment": {"run_id": "run-one", "session_id": "session-one",
                                 "worker_id": 2, "item_id": "a<id>\x1b[31m",
                                 "basename": "<file>", "generation": 1,
                                 "attempt_id": "attempt", "attempt_number": 1}}
        rendered = worker_details_text(worker, "read", 4, 3, "live")
        self.assertIn("details differ", rendered)
        self.assertIn("No progress for 60s", rendered)
        self.assertIn("a<id>\\x1b[31m", rendered)
        self.assertIn("\n\nSource\n", rendered)
        self.assertIn("Source hidden", rendered)
        idle = worker_details_text({"worker_id": 2, "assignment": None,
                                    "reason": "Reason unavailable"})
        self.assertIn("No item assigned", idle)
        self.assertNotIn("Received:", idle)

    def test_retry_deadline_hides_the_durable_epoch_value(self):
        self.assertEqual(format_retry_deadline(0), "Eligible; awaiting controller")
        rendered = item_details_text({"retry_at": 0})
        self.assertIn("Retry deadline: Eligible; awaiting controller", rendered)


if __name__ == "__main__":
    unittest.main()
