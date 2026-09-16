"""Tests for the read-only version-1 telemetry monitor."""

from __future__ import annotations

import datetime as dt
import json
import tempfile
import unittest
from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from controller import (ControlError, ControlServer, control_request,
                        get_control_state, read_control_session)
from monitor import (SnapshotError, event_item_path, event_message_style,
                     event_timestamp, freshness, literal_text, read_snapshot,
                     marquee_filename, retry_summary, event_severity_style,
                     event_worker_label, format_countdown, freshness_style,
                     lifecycle_style, select_snapshot, worker_phase_label,
                     truncate_filename)


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

    def test_downloading_worker_becomes_stalled_after_five_seconds_without_progress(self):
        value = snapshot()
        self.assertEqual(worker_phase_label(
            {"phase": "downloading", "last_progress_age_s": 5}, value), "downloading")
        self.assertEqual(worker_phase_label(
            {"phase": "downloading", "last_progress_age_s": 6.2}, value), "stalled 6s")

    def test_finished_run_without_retries_has_a_final_summary(self):
        value = snapshot("finished")
        self.assertEqual(retry_summary(value), "No retries pending")

    def test_event_severity_styles_distinguish_log_levels(self):
        self.assertEqual(event_severity_style("info"), "cyan")
        self.assertEqual(event_severity_style("warning"), "bold yellow")
        self.assertEqual(event_severity_style("error"), "bold red")
        self.assertEqual(event_severity_style("other"), "dim")

    def test_header_status_styles_distinguish_lifecycle_and_freshness(self):
        self.assertEqual(lifecycle_style("running"), "bold green")
        self.assertEqual(lifecycle_style("stopped"), "bold yellow")
        self.assertEqual(lifecycle_style("unexpected"), "bold red")
        self.assertEqual(freshness_style("live"), "bold green")
        self.assertEqual(freshness_style("disconnected"), "bold red")

    def test_event_worker_label_includes_valid_worker_id_only(self):
        self.assertEqual(event_worker_label({"worker_id": 3}), "W3")
        self.assertEqual(event_worker_label({"worker_id": None}), "")
        self.assertEqual(event_worker_label({"worker_id": 0}), "")
        self.assertEqual(event_worker_label({"worker_id": True}), "")

    def test_completion_event_message_is_green(self):
        self.assertEqual(event_message_style({"category": "complete"}), "bold green")
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


if __name__ == "__main__":
    unittest.main()
