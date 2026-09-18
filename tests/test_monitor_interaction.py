"""Headless-Textual interaction tests for the monitor's control command set.

These tests drive the real Monitor App through Textual's run_test() Pilot,
against a real ControlServer over a temporary Unix socket (the same fake
controller pattern test_monitor.py uses for contract tests). They never
contact a source, Tor, or aria2.
"""

from __future__ import annotations

import datetime as dt
import tempfile
import threading
import unittest
from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

try:
    import textual  # noqa: F401
    TEXTUAL_AVAILABLE = True
except ImportError:
    TEXTUAL_AVAILABLE = False

from controller import ControlServer
from monitor import build_monitor_app


def snapshot() -> dict:
    return {
        "schema_version": 1,
        "run_id": "run-one",
        "session_id": "session-one",
        "sequence": 1,
        "published_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "session_elapsed_s": 1,
        "state_revision": 1,
        "run": {
            "lifecycle": "running",
            "selected_count": 1,
            "counts": {"queued": 1, "busy": 0, "retry": 0, "exhausted": 0,
                       "complete": 0, "existing_unverified": 0,
                       "review_required": 0, "unavailable": 0, "excluded": 0,
                       "unknown": 0},
            "metrics": {},
        },
        "workers": [], "validation": [], "health": {}, "recent_events": [],
    }


def available_actions(*names: str) -> dict:
    return {"actions": {name: "available" for name in names}}


def snapshot_with_worker() -> dict:
    value = snapshot()
    value["workers"] = [{"worker_id": 1, "item_id": "item-1", "basename": "file.txt",
                         "received_bytes": 0, "total_bytes": 10, "speed_bps": None,
                         "eta_seconds": None, "phase": "downloading"}]
    return value


@unittest.skipUnless(TEXTUAL_AVAILABLE, "Textual is optional")
class MonitorInteractionTests(unittest.IsolatedAsyncioTestCase):
    def make_app(self, root: Path, state_provider, executor, fps: int = 20,
                 snapshot_value: dict | None = None):
        server = ControlServer(root, "run-one", "session-one", state_provider, executor)
        server.start()
        self.addCleanup(server.stop)
        app_class = build_monitor_app(snapshot_value or snapshot(), None, root, True, fps)
        self.assertIsNotNone(app_class, "Textual is installed; build_monitor_app must succeed")
        return app_class(), server

    async def test_run_wide_action_confirms_and_sends_request(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            commands = []
            app, _server = self.make_app(
                root,
                lambda: available_actions("pause_admission"),
                lambda request: commands.append(request) or {
                    "outcome": "completed", "reason": "admission paused",
                    "state_revision": 2,
                },
            )
            async with app.run_test() as pilot:
                await pilot.pause(0.3)
                await pilot.press("p")
                await pilot.pause(0.2)
                self.assertEqual(app.screen.__class__.__name__, "ActionConfirmation")
                await pilot.press("y")
                await pilot.pause(0.2)
            self.assertEqual(len(commands), 1)
            self.assertEqual(commands[0]["action"], "pause_admission")

    async def test_run_wide_action_cancel_sends_no_request(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            commands = []
            app, _server = self.make_app(
                root,
                lambda: available_actions("drain_and_stop"),
                lambda request: commands.append(request) or {"outcome": "completed",
                                                              "reason": "draining"},
            )
            async with app.run_test() as pilot:
                await pilot.pause(0.3)
                await pilot.press("d")
                await pilot.pause(0.2)
                self.assertEqual(app.screen.__class__.__name__, "ActionConfirmation")
                await pilot.press("n")
                await pilot.pause(0.2)
                self.assertEqual(len(app.screen_stack), 1)
            self.assertEqual(commands, [])

    async def test_ineligible_action_opens_no_confirmation(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            commands = []
            app, _server = self.make_app(
                root,
                lambda: available_actions("pause_admission"),  # checkpoint_stop absent
                lambda request: commands.append(request) or {"outcome": "completed"},
            )
            async with app.run_test() as pilot:
                await pilot.pause(0.3)
                await pilot.press("k")  # checkpoint_stop: not in the available set
                await pilot.pause(0.2)
                self.assertEqual(len(app.screen_stack), 1)
            self.assertEqual(commands, [])

    async def test_worker_details_disables_run_controls(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            commands = []
            app, _server = self.make_app(
                root,
                lambda: available_actions("pause_admission", "resume_admission",
                                          "drain_and_stop", "checkpoint_stop"),
                lambda request: commands.append(request) or {"outcome": "completed"},
                snapshot_value=snapshot_with_worker(),
            )
            async with app.run_test() as pilot:
                await pilot.pause(0.3)
                app.open_worker_details("1")
                await pilot.pause(0.1)
                self.assertEqual(app.screen.__class__.__name__, "WorkerDetails")
                for key in ("p", "u", "d", "k"):
                    await pilot.press(key)
                    await pilot.pause(0.1)
                    self.assertEqual(app.screen.__class__.__name__, "WorkerDetails")
            self.assertEqual(commands, [])

    async def test_control_state_polling_is_throttled_to_twice_per_second(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            lock = threading.Lock()
            poll_count = 0

            def state_provider():
                nonlocal poll_count
                with lock:
                    poll_count += 1
                return available_actions("pause_admission")

            app, _server = self.make_app(root, state_provider, lambda request: {}, fps=30)
            async with app.run_test() as pilot:
                await pilot.pause(1.0)
            with lock:
                final_count = poll_count
            # 1.0s at a 0.5s minimum gap between polls allows at most 3 polls
            # (t=0, t=0.5, t=1.0); the 30 FPS render loop must not poll on
            # every frame.
            self.assertLessEqual(final_count, 3)
            self.assertGreaterEqual(final_count, 1)

    async def test_activity_scroll_position_survives_snapshot_update(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            app, _server = self.make_app(
                root, lambda: available_actions(), lambda request: {}, fps=20)
            async with app.run_test() as pilot:
                await pilot.pause(0.2)
                current = dict(app.current)
                current["recent_events"] = [
                    {"timestamp": dt.datetime.now(dt.timezone.utc).isoformat(),
                     "severity": "info", "message": f"event {index}"}
                    for index in range(50)
                ]
                app.current = current
                app.populate(current, force=True)
                await pilot.pause(0.1)
                pane = app.query_one("#activity-pane")
                pane.scroll_home(animate=False)
                await pilot.pause(0.1)
                scrolled_y = pane.scroll_y
                current2 = dict(current)
                current2["recent_events"] = current["recent_events"] + [
                    {"timestamp": dt.datetime.now(dt.timezone.utc).isoformat(),
                     "severity": "info", "message": "one more event"}]
                app.populate(current2, force=False)
                await pilot.pause(0.1)
                self.assertEqual(pane.scroll_y, scrolled_y)

    async def test_resize_keeps_app_responsive(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            commands = []
            app, _server = self.make_app(
                root, lambda: available_actions("pause_admission"),
                lambda request: commands.append(request) or {"outcome": "completed"})
            async with app.run_test(size=(80, 24)) as pilot:
                await pilot.pause(0.3)
                await pilot.resize_terminal(120, 40)
                await pilot.pause(0.2)
                await pilot.press("p")
                await pilot.pause(0.2)
                self.assertEqual(app.screen.__class__.__name__, "ActionConfirmation")
                await pilot.press("y")
                await pilot.pause(0.2)
            self.assertEqual(len(commands), 1)

    async def test_reconnect_after_controller_restart_recovers_control_state(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            server = ControlServer(root, "run-one", "session-one",
                                   lambda: available_actions("pause_admission"),
                                   lambda request: {"outcome": "completed"})
            server.start()
            app_class = build_monitor_app(snapshot(), None, root, True, 30)
            app = app_class()
            async with app.run_test() as pilot:
                await pilot.pause(0.3)
                self.assertEqual(
                    app.control_state["actions"].get("pause_admission"), "available")
                server.stop()
                await pilot.pause(0.7)
                # The socket is gone; the UI must keep running with a recorded
                # error rather than crash the render loop.
                self.assertIsNotNone(app.control_error)
                server = ControlServer(root, "run-one", "session-two",
                                       lambda: available_actions("resume_admission"),
                                       lambda request: {"outcome": "completed"})
                server.start()
                try:
                    await pilot.pause(0.7)
                    self.assertEqual(
                        app.control_state["actions"].get("resume_admission"), "available")
                finally:
                    server.stop()


if __name__ == "__main__":
    unittest.main()
