"""Headless-Textual interaction tests for the monitor's control command set.

These tests drive the real Monitor App through Textual's run_test() Pilot,
against a real ControlServer over a temporary Unix socket (the same fake
controller pattern test_monitor.py uses for contract tests). They never
contact a source, Tor, or aria2.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import os
import sqlite3
import tempfile
import threading
import time
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
from inspection import InspectionServer
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


def item_id_for(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()


def build_item_database(root: Path, with_attempts_table: bool = True) -> Path:
    """Create a manifest database with the production item-related schema."""
    database = root / "manifest.sqlite"
    db = sqlite3.connect(database)
    db.executescript("""
        CREATE TABLE downloads (
            url TEXT PRIMARY KEY, relative_path TEXT NOT NULL, storage_path TEXT,
            staging_path TEXT, inventory_size TEXT, status TEXT NOT NULL DEFAULT 'queued',
            attempts INTEGER NOT NULL DEFAULT 0, bytes INTEGER, sha256 TEXT,
            last_error TEXT, next_retry_at REAL NOT NULL DEFAULT 0, promotion_target TEXT,
            updated_at TEXT NOT NULL, review_code TEXT, priority INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE run_items (
            run_id TEXT NOT NULL, url TEXT NOT NULL, queue_rank INTEGER NOT NULL,
            PRIMARY KEY (run_id, url)
        );
        CREATE TABLE telemetry_revisions (run_id TEXT PRIMARY KEY, revision INTEGER NOT NULL);
    """)
    if with_attempts_table:
        db.execute("""
            CREATE TABLE download_attempts (
                run_id TEXT NOT NULL, url TEXT NOT NULL, attempt_number INTEGER NOT NULL,
                attempt_id TEXT NOT NULL, generation TEXT, started_at TEXT, ended_at TEXT,
                outcome TEXT, error_category TEXT, error_message TEXT, retry_at TEXT
            )
        """)
    db.execute("INSERT INTO telemetry_revisions VALUES (?, ?)", ("run-one", 1))
    db.commit()
    db.close()
    return database


def insert_item(database: Path, url: str, rank: int, relative_path: str,
                status: str = "queued", **fields) -> None:
    db = sqlite3.connect(database)
    db.execute(
        "INSERT INTO downloads (url, relative_path, storage_path, staging_path, "
        "inventory_size, status, attempts, bytes, sha256, last_error, next_retry_at, "
        "promotion_target, updated_at, review_code, priority) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (url, relative_path, fields.get("storage_path"), fields.get("staging_path"),
         fields.get("inventory_size"), status, fields.get("attempts", 0),
         fields.get("bytes"), fields.get("sha256"), fields.get("last_error"),
         fields.get("next_retry_at", 0), fields.get("promotion_target"),
         fields.get("updated_at", "2026-09-18T00:00:00Z"), fields.get("review_code"),
         fields.get("priority", 0)))
    db.execute("INSERT INTO run_items (run_id, url, queue_rank) VALUES (?,?,?)",
              ("run-one", url, rank))
    db.commit()
    db.close()


def set_revision(database: Path, revision: int) -> None:
    db = sqlite3.connect(database)
    db.execute("UPDATE telemetry_revisions SET revision=? WHERE run_id=?", (revision, "run-one"))
    db.commit()
    db.close()


def delete_run_item(database: Path, url: str) -> None:
    db = sqlite3.connect(database)
    db.execute("DELETE FROM run_items WHERE url=?", (url,))
    db.commit()
    db.close()


def insert_attempts(database: Path, url: str, rows: list[dict]) -> None:
    db = sqlite3.connect(database)
    db.executemany(
        "INSERT INTO download_attempts (run_id, url, attempt_number, attempt_id, generation, "
        "started_at, ended_at, outcome, error_category, error_message, retry_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        [("run-one", url, row["attempt_number"], row["attempt_id"], row.get("generation"),
          row.get("started_at"), row.get("ended_at"), row.get("outcome"),
          row.get("error_category"), row.get("error_message"), row.get("retry_at"))
         for row in rows])
    db.commit()
    db.close()


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

    async def test_resume_admission_confirms_and_sends_request(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            commands = []
            app, _server = self.make_app(
                root,
                lambda: available_actions("resume_admission"),
                lambda request: commands.append(request) or {
                    "outcome": "completed", "reason": "admission resumed",
                    "state_revision": 2,
                },
            )
            async with app.run_test() as pilot:
                await pilot.pause(0.3)
                await pilot.press("u")
                await pilot.pause(0.2)
                self.assertEqual(app.screen.__class__.__name__, "ActionConfirmation")
                await pilot.press("y")
                await pilot.pause(0.2)
            self.assertEqual(len(commands), 1)
            self.assertEqual(commands[0]["action"], "resume_admission")

    async def test_checkpoint_stop_confirms_and_sends_request(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            commands = []
            app, _server = self.make_app(
                root,
                lambda: available_actions("checkpoint_stop"),
                lambda request: commands.append(request) or {
                    "outcome": "completed", "reason": "checkpointing and stopping",
                    "state_revision": 2,
                },
            )
            async with app.run_test() as pilot:
                await pilot.pause(0.3)
                await pilot.press("k")
                await pilot.pause(0.2)
                self.assertEqual(app.screen.__class__.__name__, "ActionConfirmation")
                await pilot.press("y")
                await pilot.pause(0.2)
            self.assertEqual(len(commands), 1)
            self.assertEqual(commands[0]["action"], "checkpoint_stop")

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

    async def test_stale_source_response_cannot_replace_a_newer_selection(self):
        # A source-reveal reply that arrives after the worker moved on to a
        # different item must be discarded, not applied to the screen -- a
        # late race here would show one item's revealed source under
        # another item's row, a real information-leak vector.
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            app, _server = self.make_app(
                root, lambda: available_actions(), lambda request: {},
                snapshot_value=snapshot_with_worker(),
            )
            async with app.run_test() as pilot:
                await pilot.pause(0.3)
                app.open_worker_details("1")
                await pilot.pause(0.1)
                screen = app.screen
                self.assertEqual(screen.__class__.__name__, "WorkerDetails")
                screen.displayed_item_id = "item-2"
                screen.revealed = True
                screen.source = None
                screen.source_label = "Source hidden"
                stale_response = {"data": {"item": {
                    "source": "http://a.onion/should-not-appear",
                    "source_label": "Source redacted"}}}
                screen.apply_source("item-1", stale_response, None)
                self.assertIsNone(screen.source)
                self.assertEqual(screen.source_label, "Source hidden")

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

    async def test_activity_time_is_local_and_dim_never_bold_dim(self):
        # Terminals render bold plus dim inconsistently, and the operator reads
        # event times against their own clock.
        previous = os.environ.get("TZ")
        os.environ["TZ"] = "TST-5"
        time.tzset()

        def restore() -> None:
            if previous is None:
                os.environ.pop("TZ", None)
            else:
                os.environ["TZ"] = previous
            time.tzset()

        self.addCleanup(restore)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            app, _server = self.make_app(
                root, lambda: available_actions(), lambda request: {}, fps=20)
            async with app.run_test() as pilot:
                await pilot.pause(0.2)
                current = dict(app.current)
                current["recent_events"] = [
                    {"at": "2026-09-19T16:31:08+02:00", "severity": "info",
                     "message": "resumed"}]
                app.current = current
                app.populate(current, force=True)
                await pilot.pause(0.1)
                content = app.query_one("#activity").content
                self.assertTrue(content.plain.startswith("19:31:08 "))
                styles = [str(span.style) for span in content.spans]
                self.assertEqual(styles[0], "dim")
                self.assertNotIn("bold dim", styles)

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


@unittest.skipUnless(TEXTUAL_AVAILABLE, "Textual is optional")
class ItemDetailsInteractionTests(unittest.IsolatedAsyncioTestCase):
    """Headless coverage for specs/SPEC-console-item-details.md's acceptance criteria."""

    def make_item_app(self, root: Path, database: Path, runtime_provider=None,
                      snapshot_value: dict | None = None, fps: int = 20):
        control = ControlServer(root, "run-one", "session-one",
                                lambda: available_actions(),
                                lambda request: {"outcome": "completed"})
        control.start()
        self.addCleanup(control.stop)
        inspection = InspectionServer(root, "run-one", "session-one", database, 3,
                                      runtime_provider)
        inspection.start()
        self.addCleanup(inspection.stop)
        app_class = build_monitor_app(snapshot_value or snapshot(), None, root, True, fps)
        self.assertIsNotNone(app_class, "Textual is installed; build_monitor_app must succeed")
        return app_class()

    async def open_queue_row(self, pilot, index: int = 0):
        app = pilot.app
        app.query_one("TabbedContent").active = "queue-tab"
        await pilot.pause(0.3)
        pane = app.query_one("#queue-pane")
        table = app.query_one("#queue-table")
        while not pane.rows:
            await pilot.pause(0.1)
        table.move_cursor(row=index)
        await pilot.pause(0.05)
        await pilot.press("enter")
        await pilot.pause(0.3)

    async def test_entry_from_queue_and_return_navigation_with_duplicate_basenames(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            database = build_item_database(root)
            url_one, url_two = "http://a.onion/x/dup.bin", "http://a.onion/y/dup.bin"
            insert_item(database, url_one, 1, "x/dup.bin")
            insert_item(database, url_two, 2, "y/dup.bin")
            app = self.make_item_app(root, database)
            async with app.run_test() as pilot:
                await self.open_queue_row(pilot, 0)
                self.assertEqual(app.screen.__class__.__name__, "ItemDetails")
                text_one = app.screen.query_one("#item-details-text").content.plain
                self.assertIn("dup.bin", text_one)
                self.assertRegex(text_one.splitlines()[1], r"ID [0-9a-f]{10}")
                pane = app.query_one("#queue-pane")
                previous_selection = pane.selected_item_id
                await pilot.press("escape")
                await pilot.pause(0.2)
                self.assertEqual(len(app.screen_stack), 1)
                self.assertEqual(pane.selected_item_id, previous_selection)
                await self.open_queue_row(pilot, 1)
                text_two = app.screen.query_one("#item-details-text").content.plain
                self.assertIn("dup.bin", text_two)
                self.assertNotEqual(text_one.splitlines()[1], text_two.splitlines()[1])

    async def test_section_headers_are_bold_without_a_fixed_foreground_color(self):
        # A fixed white foreground is invisible on a light terminal theme; the
        # visual-style spec allows bold or dim only for structural text.
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            database = build_item_database(root)
            insert_item(database, "http://a.onion/x/head.bin", 1, "x/head.bin")
            app = self.make_item_app(root, database)
            async with app.run_test() as pilot:
                await self.open_queue_row(pilot, 0)
                content = app.screen.query_one("#item-details-text").content
                styles = [str(span.style) for span in content.spans
                          if content.plain[span.start:span.end] == "State"]
                self.assertEqual(styles, ["bold"])

    async def test_entry_from_worker_details_and_return_navigation(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            database = build_item_database(root)
            url = "http://a.onion/worker-item.bin"
            insert_item(database, url, 1, "worker-item.bin", status="active")
            item_id = item_id_for(url)
            runtime = [{"worker_id": 1, "url": url, "phase": "downloading"}]
            app = self.make_item_app(root, database, runtime_provider=lambda: runtime,
                                     snapshot_value=snapshot_with_worker())
            async with app.run_test() as pilot:
                await pilot.pause(0.3)
                app.open_worker_details("1")
                await pilot.pause(0.2)
                self.assertEqual(app.screen.__class__.__name__, "WorkerDetails")
                app.screen.displayed_item_id = item_id
                await pilot.press("i")
                await pilot.pause(0.3)
                self.assertEqual(app.screen.__class__.__name__, "ItemDetails")
                await pilot.press("escape")
                await pilot.pause(0.2)
                self.assertEqual(app.screen.__class__.__name__, "WorkerDetails")

    async def test_source_reveal_hide_and_clearing_on_close_and_session_change(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            database = build_item_database(root)
            url = "http://a.onion/secret-source.bin?token=1"
            insert_item(database, url, 1, "secret-source.bin", status="queued")
            app = self.make_item_app(root, database)
            async with app.run_test() as pilot:
                await self.open_queue_row(pilot, 0)
                screen = app.screen
                self.assertIn("Source hidden",
                             screen.query_one("#item-details-text").content.plain)
                await pilot.press("r")
                await pilot.pause(0.2)
                text = screen.query_one("#item-details-text").content.plain
                self.assertIn("Source redacted", text)
                self.assertNotIn("token=1", text)
                await pilot.press("r")  # now bound to hide_source
                await pilot.pause(0.1)
                self.assertIn("Source hidden",
                             screen.query_one("#item-details-text").content.plain)
                # Reveal again, then close: reopening must show it hidden again.
                await pilot.press("r")
                await pilot.pause(0.2)
                self.assertIn("Source redacted",
                             screen.query_one("#item-details-text").content.plain)
                await pilot.press("escape")
                await pilot.pause(0.2)
                await self.open_queue_row(pilot, 0)
                self.assertIn("Source hidden",
                             app.screen.query_one("#item-details-text").content.plain)
                # Reveal, then simulate a session change: reveal must clear and reload.
                await pilot.press("r")
                await pilot.pause(0.2)
                self.assertTrue(app.screen.revealed)
                current = dict(app.current)
                current["session_id"] = "session-two"
                app.current = current
                app.screen.check_session()
                await pilot.pause(0.2)
                self.assertFalse(app.screen.revealed)
                self.assertIn("Source hidden",
                             app.screen.query_one("#item-details-text").content.plain)

    async def test_session_change_with_no_record_shows_details_unavailable(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            database = build_item_database(root)
            url = "http://a.onion/vanishing.bin"
            insert_item(database, url, 1, "vanishing.bin", status="queued")
            app = self.make_item_app(root, database)
            async with app.run_test() as pilot:
                await self.open_queue_row(pilot, 0)
                delete_run_item(database, url)
                current = dict(app.current)
                current["session_id"] = "session-two"
                app.current = current
                app.screen.check_session()
                await pilot.pause(0.3)
                text = app.screen.query_one("#item-details-text").content.plain
                self.assertIn("Details unavailable", text)
                self.assertIn("item was not found", text)

    async def test_paginated_attempts_next_page_and_revision_change_does_not_mix_pages(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            database = build_item_database(root)
            url = "http://a.onion/many-attempts.bin"
            insert_item(database, url, 1, "many-attempts.bin", status="retry_wait", attempts=250)
            rows = [{"attempt_number": number, "attempt_id": f"a{number:04d}",
                    "generation": "gen-1", "started_at": "2026-09-18T00:00:00Z",
                    "ended_at": "2026-09-18T00:01:00Z", "outcome": "failed",
                    "error_category": "network", "error_message": "connection reset"}
                   for number in range(250, 0, -1)]
            insert_attempts(database, url, rows)
            app = self.make_item_app(root, database)
            async with app.run_test() as pilot:
                await self.open_queue_row(pilot, 0)
                screen = app.screen
                await pilot.press("n")
                await pilot.pause(0.3)
                self.assertEqual(len(screen.attempts), 200)
                self.assertEqual(screen.attempts[0]["attempt_number"], 250)
                text = screen.query_one("#item-details-text").content.plain
                self.assertIn("#250 a0250 Generation: gen-1", text)
                self.assertIn("Started: 2026-09-18T00:00:00Z", text)
                self.assertIn("Ended: 2026-09-18T00:01:00Z", text)
                self.assertIn("Error: network connection reset", text)
                set_revision(database, 2)
                await pilot.press("n")
                await pilot.pause(0.3)
                # A revision change must not mix pages: the count stays at the first page.
                self.assertEqual(len(screen.attempts), 200)

    async def test_arrow_keys_select_an_attempt_without_scrolling_other_sections(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            database = build_item_database(root)
            url = "http://a.onion/three-attempts.bin"
            insert_item(database, url, 1, "three-attempts.bin", status="retry_wait", attempts=3)
            rows = [{"attempt_number": number, "attempt_id": f"a{number:04d}",
                    "generation": "gen-1", "started_at": "2026-09-18T00:00:00Z",
                    "ended_at": "2026-09-18T00:01:00Z", "outcome": "failed",
                    "error_category": "network", "error_message": "connection reset"}
                   for number in range(3, 0, -1)]
            insert_attempts(database, url, rows)
            app = self.make_item_app(root, database)
            async with app.run_test() as pilot:
                await self.open_queue_row(pilot, 0)
                screen = app.screen
                await pilot.press("n")
                await pilot.pause(0.3)
                self.assertEqual(screen.selected_attempt_index, 0)
                await pilot.press("down")
                await pilot.pause(0.1)
                self.assertEqual(screen.selected_attempt_index, 1)
                text = screen.query_one("#item-details-text").content.plain
                self.assertIn("→ #2 a0002", text)
                await pilot.press("down")
                await pilot.press("down")  # clamps at the last attempt, no wraparound
                await pilot.pause(0.1)
                self.assertEqual(screen.selected_attempt_index, 2)
                await pilot.press("up")
                await pilot.pause(0.1)
                self.assertEqual(screen.selected_attempt_index, 1)

    async def test_missing_attempt_history_leaves_no_attempts_shown(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            database = build_item_database(root, with_attempts_table=False)
            url = "http://a.onion/no-history.bin"
            insert_item(database, url, 1, "no-history.bin", status="retry_wait")
            app = self.make_item_app(root, database)
            async with app.run_test() as pilot:
                await self.open_queue_row(pilot, 0)
                screen = app.screen
                await pilot.press("n")
                await pilot.pause(0.3)
                self.assertEqual(screen.attempts, [])
                text = screen.query_one("#item-details-text").content.plain
                self.assertNotIn("Recorded attempts\n#", text)

    async def test_literal_rendering_of_markup_and_unicode_paths(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            database = build_item_database(root)
            url = "http://a.onion/literal.bin"
            path = "café/日本語/<b>bold</b>\x1b[31mred/name.bin"
            insert_item(database, url, 1, path, status="queued")
            app = self.make_item_app(root, database)
            async with app.run_test() as pilot:
                await self.open_queue_row(pilot, 0)
                text = app.screen.query_one("#item-details-text").content.plain
                self.assertIn("café", text)
                self.assertIn("日本語", text)
                self.assertIn("<b>bold</b>", text)
                self.assertIn("\\x1b[31m", text)
                self.assertNotIn("\x1b[31m", text)

    async def test_resize_keeps_item_details_responsive(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            database = build_item_database(root)
            url = "http://a.onion/resize.bin"
            insert_item(database, url, 1, "resize.bin", status="queued")
            app = self.make_item_app(root, database)
            async with app.run_test(size=(80, 24)) as pilot:
                await self.open_queue_row(pilot, 0)
                self.assertEqual(app.screen.__class__.__name__, "ItemDetails")
                focused_before = app.focused
                await pilot.resize_terminal(120, 40)
                await pilot.pause(0.2)
                self.assertEqual(app.screen.__class__.__name__, "ItemDetails")
                self.assertIs(app.focused, focused_before)
                await pilot.press("n")
                await pilot.pause(0.2)
                self.assertEqual(app.screen.__class__.__name__, "ItemDetails")

    async def test_no_evidence_read_or_mutation_action_is_reachable(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            database = build_item_database(root)
            url = "http://a.onion/readonly.bin"
            insert_item(database, url, 1, "readonly.bin", status="retry_wait")
            app = self.make_item_app(root, database)
            async with app.run_test() as pilot:
                await self.open_queue_row(pilot, 0)
                binding_keys = {binding[0] if isinstance(binding, tuple) else binding.key
                               for binding in type(app.screen).BINDINGS}
                self.assertEqual(binding_keys, {"escape", "r", "n", "l", "up", "down",
                                                "q", "t", "p", "u", "d", "k"})
                for key in ("q", "t", "p", "u", "d", "k"):
                    await pilot.press(key)
                    await pilot.pause(0.1)
                    self.assertEqual(app.screen.__class__.__name__, "ItemDetails")
                await pilot.press("l")
                await pilot.pause(0.1)
                self.assertEqual(app.screen.__class__.__name__, "ItemDetails")

    async def test_revealed_source_over_the_byte_bound_is_labeled_truncated(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            database = build_item_database(root)
            long_path = "/" + ("segment/" * 200) + "file.bin"
            url = "http://a.onion" + long_path
            insert_item(database, url, 1, "long.bin", status="queued")
            app = self.make_item_app(root, database)
            async with app.run_test() as pilot:
                await self.open_queue_row(pilot, 0)
                await pilot.press("r")
                await pilot.pause(0.2)
                text = app.screen.query_one("#item-details-text").content.plain
                self.assertIn("Source truncated", text)

    async def test_session_change_clears_attempts_without_mixing_pages(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            database = build_item_database(root)
            url = "http://a.onion/session-attempts.bin"
            insert_item(database, url, 1, "session-attempts.bin", status="retry_wait")
            insert_attempts(database, url, [
                {"attempt_number": 1, "attempt_id": "a1", "outcome": "failed"}])
            app = self.make_item_app(root, database)
            async with app.run_test() as pilot:
                await self.open_queue_row(pilot, 0)
                await pilot.press("n")
                await pilot.pause(0.2)
                self.assertEqual(len(app.screen.attempts), 1)
                current = dict(app.current)
                current["session_id"] = "session-two"
                app.current = current
                app.screen.check_session()
                await pilot.pause(0.2)
                # A session change must clear the previously loaded attempt page.
                self.assertEqual(app.screen.attempts, [])
                self.assertEqual(app.screen.cursor, None)

    async def test_inspection_outage_retains_last_known_data_then_recovers(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            database = build_item_database(root)
            url = "http://a.onion/reconnect.bin"
            insert_item(database, url, 1, "reconnect.bin", status="queued")
            control = ControlServer(root, "run-one", "session-one",
                                    lambda: available_actions(),
                                    lambda request: {"outcome": "completed"})
            control.start()
            self.addCleanup(control.stop)
            inspection = InspectionServer(root, "run-one", "session-one", database, 3)
            inspection.start()
            app_class = build_monitor_app(snapshot(), None, root, True, 20)
            app = app_class()
            async with app.run_test() as pilot:
                await self.open_queue_row(pilot, 0)
                text = app.screen.query_one("#item-details-text").content.plain
                self.assertIn("reconnect.bin", text)
                inspection.stop()
                app.screen.load_item()
                await pilot.pause(0.3)
                text = app.screen.query_one("#item-details-text").content.plain
                # A plain inspection failure must retain last-known labeled data,
                # not claim the item vanished (that is reserved for a session
                # change that finds no record).
                self.assertIn("Last-known values retained", text)
                self.assertIn("reconnect.bin", text)
                self.assertNotIn("Details unavailable", text)
                inspection = InspectionServer(root, "run-one", "session-one", database, 3)
                inspection.start()
                self.addCleanup(inspection.stop)
                app.screen.load_item()
                await pilot.pause(0.3)
                text = app.screen.query_one("#item-details-text").content.plain
                self.assertIn("reconnect.bin", text)
                self.assertNotIn("Details unavailable", text)
                self.assertNotIn("Last-known values retained", text)


@unittest.skipUnless(TEXTUAL_AVAILABLE, "Textual is optional")
class QueueRowScopedActionInteractionTests(unittest.IsolatedAsyncioTestCase):
    """Headless coverage for row-scoped queue actions (SPEC-controller-control-ui.md)."""

    def make_queue_app(self, root: Path, database: Path, executor, fps: int = 20):
        control = ControlServer(root, "run-one", "session-one", lambda: available_actions(),
                                executor)
        control.start()
        self.addCleanup(control.stop)
        inspection = InspectionServer(root, "run-one", "session-one", database, 3)
        inspection.start()
        self.addCleanup(inspection.stop)
        app_class = build_monitor_app(snapshot(), None, root, True, fps)
        self.assertIsNotNone(app_class, "Textual is installed; build_monitor_app must succeed")
        return app_class()

    async def open_queue_tab(self, pilot, index: int = 0):
        app = pilot.app
        app.query_one("TabbedContent").active = "queue-tab"
        await pilot.pause(0.3)
        pane = app.query_one("#queue-pane")
        table = app.query_one("#queue-table")
        while not pane.rows:
            await pilot.pause(0.1)
        table.move_cursor(row=index)
        await pilot.pause(0.05)
        return pane

    async def test_row_scoped_retry_sends_only_the_focused_item(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            database = build_item_database(root)
            first, second = "http://a.onion/first.bin", "http://a.onion/second.bin"
            insert_item(database, first, 1, "first.bin", status="retry_wait")
            insert_item(database, second, 2, "second.bin", status="retry_wait")
            commands = []
            app = self.make_queue_app(
                root, database,
                lambda request: commands.append(request) or {"outcome": "completed"})
            async with app.run_test() as pilot:
                pane = await self.open_queue_tab(pilot, 1)
                self.assertEqual(pane.selected_item_id, item_id_for(second))
                await pilot.press("R")
                await pilot.pause(0.2)
                self.assertEqual(app.screen.__class__.__name__, "ActionConfirmation")
                await pilot.press("y")
                await pilot.pause(0.2)
            self.assertEqual(len(commands), 1)
            self.assertEqual(commands[0]["action"], "retry_now")
            self.assertEqual(commands[0]["parameters"]["item_ids"], [item_id_for(second)])

    async def test_row_scoped_exclude_sends_only_the_focused_item(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            database = build_item_database(root)
            first, second = "http://a.onion/first.bin", "http://a.onion/second.bin"
            insert_item(database, first, 1, "first.bin", status="queued")
            insert_item(database, second, 2, "second.bin", status="queued")
            commands = []
            app = self.make_queue_app(
                root, database,
                lambda request: commands.append(request) or {"outcome": "completed"})
            async with app.run_test() as pilot:
                pane = await self.open_queue_tab(pilot, 0)
                self.assertEqual(pane.selected_item_id, item_id_for(first))
                await pilot.press("x")
                await pilot.pause(0.2)
                self.assertEqual(app.screen.__class__.__name__, "ActionConfirmation")
                await pilot.press("y")
                await pilot.pause(0.2)
            self.assertEqual(len(commands), 1)
            self.assertEqual(commands[0]["action"], "exclude_item")
            self.assertEqual(commands[0]["parameters"]["item_ids"], [item_id_for(first)])

    async def test_row_scoped_retry_access_denied_sends_only_the_focused_denied_item(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            database = build_item_database(root)
            first, second = "http://a.onion/first.bin", "http://a.onion/second.bin"
            insert_item(database, first, 1, "first.bin", status="queued")
            insert_item(database, second, 2, "second.bin", status="review_required",
                        review_code="access_denied")
            commands = []
            app = self.make_queue_app(
                root, database,
                lambda request: commands.append(request) or {"outcome": "completed"})
            async with app.run_test() as pilot:
                pane = await self.open_queue_tab(pilot, 0)
                self.assertEqual(pane.selected_item_id, item_id_for(first))
                await pilot.press("A")
                await pilot.pause(0.2)
                self.assertNotEqual(app.screen.__class__.__name__, "ActionConfirmation")
                pane = await self.open_queue_tab(pilot, 1)
                self.assertEqual(pane.selected_item_id, item_id_for(second))
                await pilot.press("A")
                await pilot.pause(0.2)
                self.assertEqual(app.screen.__class__.__name__, "ActionConfirmation")
                await pilot.press("y")
                await pilot.pause(0.2)
            self.assertEqual(len(commands), 1)
            self.assertEqual(commands[0]["action"], "retry_access_denied")
            self.assertEqual(commands[0]["parameters"]["item_ids"], [item_id_for(second)])

    async def test_row_scoped_resume_new_generation_sends_only_the_focused_review_item(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            database = build_item_database(root)
            first, second = "http://a.onion/first.bin", "http://a.onion/second.bin"
            insert_item(database, first, 1, "first.bin", status="queued")
            insert_item(database, second, 2, "second.bin", status="review_required",
                        review_code="changed_remote_representation")
            commands = []
            app = self.make_queue_app(
                root, database,
                lambda request: commands.append(request) or {"outcome": "completed"})
            async with app.run_test() as pilot:
                pane = await self.open_queue_tab(pilot, 0)
                self.assertEqual(pane.selected_item_id, item_id_for(first))
                await pilot.press("N")
                await pilot.pause(0.2)
                self.assertNotEqual(app.screen.__class__.__name__, "ActionConfirmation")
                pane = await self.open_queue_tab(pilot, 1)
                self.assertEqual(pane.selected_item_id, item_id_for(second))
                await pilot.press("N")
                await pilot.pause(0.2)
                self.assertEqual(app.screen.__class__.__name__, "ActionConfirmation")
                await pilot.press("y")
                await pilot.pause(0.2)
            self.assertEqual(len(commands), 1)
            self.assertEqual(commands[0]["action"], "resume_new_generation")
            self.assertEqual(commands[0]["parameters"]["item_ids"], [item_id_for(second)])

    async def test_row_scoped_priority_raise_sends_only_the_focused_item(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            database = build_item_database(root)
            first, second = "http://a.onion/first.bin", "http://a.onion/second.bin"
            insert_item(database, first, 1, "first.bin", status="queued")
            insert_item(database, second, 2, "second.bin", status="queued")
            commands = []
            app = self.make_queue_app(
                root, database,
                lambda request: commands.append(request) or {"outcome": "completed"})
            async with app.run_test() as pilot:
                pane = await self.open_queue_tab(pilot, 0)
                self.assertEqual(pane.selected_item_id, item_id_for(first))
                await pilot.press("]")
                await pilot.pause(0.2)
                self.assertEqual(app.screen.__class__.__name__, "ActionConfirmation")
                await pilot.press("y")
                await pilot.pause(0.2)
            self.assertEqual(len(commands), 1)
            self.assertEqual(commands[0]["action"], "set_item_priority")
            self.assertEqual(commands[0]["parameters"]["item_ids"], [item_id_for(first)])
            self.assertEqual(commands[0]["parameters"]["priority"], 1)

    async def test_row_scoped_cooldown_lower_sends_only_the_focused_item(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            database = build_item_database(root)
            first, second = "http://a.onion/first.bin", "http://a.onion/second.bin"
            insert_item(database, first, 1, "first.bin", status="retry_wait",
                       next_retry_at=time.time() + 60)
            insert_item(database, second, 2, "second.bin", status="retry_wait",
                       next_retry_at=time.time() + 60)
            commands = []
            app = self.make_queue_app(
                root, database,
                lambda request: commands.append(request) or {"outcome": "completed"})
            async with app.run_test() as pilot:
                pane = await self.open_queue_tab(pilot, 1)
                self.assertEqual(pane.selected_item_id, item_id_for(second))
                await pilot.press("{")
                await pilot.pause(0.2)
                self.assertEqual(app.screen.__class__.__name__, "ActionConfirmation")
                await pilot.press("y")
                await pilot.pause(0.2)
            self.assertEqual(len(commands), 1)
            self.assertEqual(commands[0]["action"], "set_retry_cooldown")
            self.assertEqual(commands[0]["parameters"]["item_ids"], [item_id_for(second)])

    async def test_retry_countdown_row_dims_its_labels_and_keeps_the_values_default(self):
        # The queue spec dims retry status text but keeps the deadline and the
        # countdown value default style; a fully default line hid the labels.
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            database = build_item_database(root)
            insert_item(database, "http://a.onion/wait.bin", 1, "wait.bin",
                        status="retry_wait", next_retry_at=time.time() + 3600)
            app = self.make_queue_app(root, database, lambda request: {"outcome": "completed"})
            async with app.run_test() as pilot:
                await self.open_queue_tab(pilot, 0)
                cell = app.query_one("#queue-table").get_row_at(0)[-1]
                self.assertRegex(cell.plain,
                                 r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} \(.+ remaining\)$")
                dim = "".join(cell.plain[span.start:span.end]
                              for span in cell.spans if str(span.style) == "dim")
                self.assertEqual(dim, " ( remaining)")

    async def test_row_scoped_action_ignores_filtered_out_rows(self):
        # A bucket filter must not let the row-scoped action reach beyond the
        # explicitly focused, visible row -- even though a second, filtered-out
        # item also exists in the selected set.
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            database = build_item_database(root)
            visible, hidden = "http://a.onion/visible.bin", "http://a.onion/hidden.bin"
            insert_item(database, visible, 1, "visible.bin", status="retry_wait")
            insert_item(database, hidden, 2, "hidden.bin", status="complete")
            commands = []
            app = self.make_queue_app(
                root, database,
                lambda request: commands.append(request) or {"outcome": "completed"})
            async with app.run_test() as pilot:
                app.query_one("TabbedContent").active = "queue-tab"
                await pilot.pause(0.3)
                pane = app.query_one("#queue-pane")
                pane.bucket = "retry"
                app.query_one("#queue-bucket").value = "retry"
                pane.reload(reset=True)
                await pilot.pause(0.3)
                self.assertEqual(len(pane.rows), 1)
                self.assertEqual(pane.selected_item_id, item_id_for(visible))
                await pilot.press("R")
                await pilot.pause(0.2)
                await pilot.press("y")
                await pilot.pause(0.2)
            self.assertEqual(len(commands), 1)
            self.assertEqual(commands[0]["parameters"]["item_ids"], [item_id_for(visible)])


class QueueMarqueeInteractionTests(unittest.IsolatedAsyncioTestCase):
    """Headless coverage for marquee scrolling on the selected queue row."""

    def make_queue_app(self, root: Path, database: Path, fps: int = 20):
        control = ControlServer(root, "run-one", "session-one", lambda: available_actions(),
                                lambda request: {"outcome": "completed"})
        control.start()
        self.addCleanup(control.stop)
        inspection = InspectionServer(root, "run-one", "session-one", database, 3)
        inspection.start()
        self.addCleanup(inspection.stop)
        app_class = build_monitor_app(snapshot(), None, root, True, fps)
        self.assertIsNotNone(app_class, "Textual is installed; build_monitor_app must succeed")
        return app_class()

    async def open_queue_tab(self, pilot, index: int = 0):
        app = pilot.app
        app.query_one("TabbedContent").active = "queue-tab"
        await pilot.pause(0.3)
        pane = app.query_one("#queue-pane")
        table = app.query_one("#queue-table")
        while not pane.rows:
            await pilot.pause(0.1)
        table.move_cursor(row=index)
        await pilot.pause(0.05)
        return pane

    async def test_selected_row_basename_scrolls_over_time(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            database = build_item_database(root)
            long_name = "a-very-long-filename-that-does-not-fit-the-column.bin"
            url = "http://a.onion/long.bin"
            insert_item(database, url, 1, long_name, status="queued")
            app = self.make_queue_app(root, database)
            async with app.run_test() as pilot:
                pane = await self.open_queue_tab(pilot, 0)
                table = app.query_one("#queue-table")
                item_id = item_id_for(url)
                first = table.get_cell(item_id, "basename")
                await pilot.pause(0.6)
                second = table.get_cell(item_id, "basename")
                self.assertNotEqual(first, second)
                self.assertEqual(len(str(first)), 32)

    async def test_deselected_row_stops_scrolling(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            database = build_item_database(root)
            long_name = "a-very-long-filename-that-does-not-fit-the-column.bin"
            first_url = "http://a.onion/long.bin"
            second_url = "http://a.onion/short.bin"
            insert_item(database, first_url, 1, long_name, status="queued")
            insert_item(database, second_url, 2, "short.bin", status="queued")
            app = self.make_queue_app(root, database)
            async with app.run_test() as pilot:
                pane = await self.open_queue_tab(pilot, 0)
                table = app.query_one("#queue-table")
                first_item_id = item_id_for(first_url)
                await pilot.pause(0.6)
                table.move_cursor(row=1)
                await pilot.pause(0.05)
                reset_value = table.get_cell(first_item_id, "basename")
                await pilot.pause(0.6)
                self.assertEqual(table.get_cell(first_item_id, "basename"), reset_value)
                self.assertIn("...", reset_value)


if __name__ == "__main__":
    unittest.main()
