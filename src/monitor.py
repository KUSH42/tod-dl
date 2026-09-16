#!/usr/bin/env python3
"""Read-only terminal monitor for version-1 acquisition telemetry."""

from __future__ import annotations

import argparse
from collections import deque
import datetime as dt
import hashlib
import json
import sys
import threading
import time
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlsplit

from controller import ControlError, control_request, get_control_state


FINAL_LIFECYCLES = {"finished", "stopped"}
REQUIRED_COUNTS = {
    "queued", "busy", "retry", "exhausted", "complete", "existing_unverified",
    "review_required", "unavailable", "unknown",
}
EVENT_SEVERITY_STYLES = {
    "info": "cyan",
    "warning": "bold yellow",
    "error": "bold red",
}
LIFECYCLE_STYLES = {
    "running": "bold green",
    "finished": "bold green",
    "stopped": "bold yellow",
}
FRESHNESS_STYLES = {
    "live": "bold green",
    "stale": "bold yellow",
    "disconnected": "bold red",
    "recorded": "dim",
}


class SnapshotError(ValueError):
    """A snapshot is absent, malformed, or incompatible."""


def require_string(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise SnapshotError(f"{name} must be a nonempty string")
    return value


def require_nonnegative(value: Any, name: str) -> int | float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
        raise SnapshotError(f"{name} must be a nonnegative number")
    return value


def validate_snapshot(snapshot: Any) -> dict[str, Any]:
    """Validate untrusted version-1 data before it reaches a terminal UI."""
    if not isinstance(snapshot, dict):
        raise SnapshotError("snapshot must be a JSON object")
    if snapshot.get("schema_version") not in {1, 2}:
        raise SnapshotError("unsupported telemetry schema version")
    for name in ("run_id", "session_id", "published_at"):
        require_string(snapshot.get(name), name)
    for name in ("sequence", "session_elapsed_s", "state_revision"):
        require_nonnegative(snapshot.get(name), name)
    run = snapshot.get("run")
    if not isinstance(run, dict):
        raise SnapshotError("run must be an object")
    require_string(run.get("lifecycle"), "run.lifecycle")
    selected = require_nonnegative(run.get("selected_count"), "run.selected_count")
    counts = run.get("counts")
    if not isinstance(counts, dict) or set(counts) != REQUIRED_COUNTS:
        raise SnapshotError("run.counts must contain every version-1 bucket")
    if sum(require_nonnegative(counts[name], f"run.counts.{name}")
           for name in REQUIRED_COUNTS) != selected:
        raise SnapshotError("run.counts must sum to run.selected_count")
    for collection in ("workers", "validation", "recent_events"):
        if not isinstance(snapshot.get(collection), list):
            raise SnapshotError(f"{collection} must be an array")
    worker_ids = set()
    for index, worker in enumerate(snapshot["workers"]):
        if not isinstance(worker, dict):
            raise SnapshotError(f"workers[{index}] must be an object")
        worker_id = require_nonnegative(worker.get("worker_id"),
                                        f"workers[{index}].worker_id")
        if worker_id in worker_ids:
            raise SnapshotError("worker IDs must be unique")
        worker_ids.add(worker_id)
        for name in ("received_bytes", "total_bytes", "speed_bps", "eta_seconds"):
            if worker.get(name) is not None:
                require_nonnegative(worker[name], f"workers[{index}].{name}")
    if not isinstance(snapshot.get("health"), dict):
        raise SnapshotError("health must be an object")
    if snapshot["schema_version"] == 2:
        for name in ("last_payload_progress_at", "completed_at", "stopped_at"):
            if run.get(name) is not None:
                require_string(run[name], f"run.{name}")
        filesystems = snapshot["health"].get("filesystems")
        if not isinstance(filesystems, list):
            raise SnapshotError("health.filesystems must be an array")
        for index, filesystem in enumerate(filesystems):
            if not isinstance(filesystem, dict):
                raise SnapshotError(f"health.filesystems[{index}] must be an object")
            require_string(filesystem.get("filesystem_id"),
                           f"health.filesystems[{index}].filesystem_id")
            roles = filesystem.get("roles")
            if not isinstance(roles, list) or not roles:
                raise SnapshotError(f"health.filesystems[{index}].roles must be nonempty")
    return snapshot


def read_snapshot(path: Path) -> dict[str, Any]:
    try:
        return validate_snapshot(json.loads(path.read_text(encoding="utf-8")))
    except OSError as exc:
        raise SnapshotError(f"cannot read snapshot: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise SnapshotError(f"malformed JSON: {exc.msg}") from exc


def published_age(snapshot: dict[str, Any], now: dt.datetime | None = None) -> float | None:
    try:
        published = dt.datetime.fromisoformat(snapshot["published_at"])
    except ValueError:
        return None
    if published.tzinfo is None:
        return None
    current = now or dt.datetime.now(dt.timezone.utc)
    return max(0.0, (current - published).total_seconds())


def freshness(snapshot: dict[str, Any]) -> str:
    if snapshot["run"]["lifecycle"] in FINAL_LIFECYCLES:
        return "recorded"
    age = published_age(snapshot)
    if age is None or age > 15:
        return "disconnected"
    if age > 5:
        return "stale"
    return "live"


def select_snapshot(state: Path, run_id: str | None) -> dict[str, Any]:
    root = state / "telemetry"
    if run_id:
        return read_snapshot(root / run_id / "snapshot.json")
    candidates = []
    for path in root.glob("*/snapshot.json") if root.exists() else ():
        try:
            snapshot = read_snapshot(path)
        except SnapshotError:
            continue
        if snapshot["run"]["lifecycle"] not in FINAL_LIFECYCLES:
            candidates.append(snapshot)
    if len(candidates) != 1:
        raise SnapshotError("select --run-id; no unique live telemetry session exists")
    return candidates[0]


def format_bytes(value: Any) -> str:
    if value is None:
        return "?"
    number = int(value)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if number < 1024 or unit == "TiB":
            return f"{number} {unit}" if unit == "B" else f"{number / 1:,.1f} {unit}"
        number /= 1024
    return "?"


def format_duration(value: Any) -> str:
    if value is None:
        return "—"
    seconds = int(value)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    return f"~{hours}h {minutes}m" if hours else f"~{minutes}m {seconds}s"


def format_elapsed_duration(value: Any) -> str:
    """Format a session duration without an ETA approximation marker."""
    if not isinstance(value, (int, float)) or isinstance(value, bool) or value < 0:
        return "?"
    seconds = int(value)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def format_countdown(value: Any) -> str:
    """Format an exact short countdown without ETA's approximate marker."""
    seconds = max(0, int(value or 0))
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    if hours:
        return f"{hours}h {minutes}m"
    return f"{minutes}m {seconds}s" if minutes else f"{seconds}s"


def literal_text(value: Any) -> str:
    """Neutralize terminal controls before displaying untrusted data."""
    return "".join(character if ord(character) >= 32 and ord(character) != 127
                   else f"\\x{ord(character):02x}" for character in str(value))


def short_item_id(value: Any) -> str:
    return hashlib.sha256(str(value).encode("utf-8", "replace")).hexdigest()[:10]


def truncate_filename(value: Any, width: int = 28) -> str:
    """Keep a useful filename suffix without exposing its containing path."""
    name = literal_text(value)
    if len(name) <= width:
        return name
    suffix = Path(str(value)).suffix
    tail = suffix.lstrip(".") if len(suffix) < width - 4 else ""
    head = max(1, width - len(tail) - 3)
    return name[:head] + "..." + tail


def marquee_filename(value: Any, offset: int, width: int = 28) -> str:
    """Return one literal-safe scrolling window over a long basename."""
    name = literal_text(value)
    if len(name) <= width:
        return name
    loop = name + "   ·   "
    start = offset % len(loop)
    return (loop + loop)[start:start + width]


def metric_value(snapshot: dict[str, Any], name: str) -> Any:
    value = snapshot["run"].get("metrics", {}).get(name)
    if isinstance(value, dict):
        return value.get("value")
    return value if isinstance(value, (int, float)) and not isinstance(value, bool) else None


def metric_quality(snapshot: dict[str, Any], name: str) -> str:
    value = snapshot["run"].get("metrics", {}).get(name)
    return value.get("quality", "unavailable") if isinstance(value, dict) else "unavailable"


def recorded_age(value: Any, snapshot: dict[str, Any]) -> str:
    """Return an age relative to the published snapshot, or an unknown marker."""
    if not isinstance(value, str):
        return "?"
    try:
        recorded = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        published = dt.datetime.fromisoformat(snapshot["published_at"].replace("Z", "+00:00"))
    except ValueError:
        return "?"
    if recorded.tzinfo is None or published.tzinfo is None or recorded > published:
        return "?"
    return format_countdown((published - recorded).total_seconds()) + " ago"


def progress_status(snapshot: dict[str, Any]) -> str:
    """Describe recorded transfer progress without claiming host reachability."""
    run = snapshot["run"]
    lifecycle = str(run.get("lifecycle", "")).lower()
    if lifecycle == "finished":
        return "Completed at " + (literal_text(run.get("completed_at"))
                                  if run.get("completed_at") else "?")
    if lifecycle == "stopped":
        reason = literal_text(run.get("reason")) if run.get("reason") else "unknown reason"
        when = literal_text(run.get("stopped_at")) if run.get("stopped_at") else "?"
        return f"Stopped at {when}: {reason}"
    active_downloads = [worker for worker in snapshot["workers"]
                        if worker.get("phase") == "downloading"]
    payload_age = recorded_age(run.get("last_payload_progress_at"), snapshot)
    completion_age = recorded_age(run.get("last_completion_at"), snapshot)
    cooldown_remaining = snapshot["health"].get("cooldown_remaining_s")
    cooldown_active = ((isinstance(cooldown_remaining, (int, float))
                        and not isinstance(cooldown_remaining, bool)
                        and cooldown_remaining > 0)
                       or any(worker.get("phase") == "cooldown"
                              for worker in snapshot["workers"]))
    all_counters_unknown = (bool(snapshot["workers"])
                            and all(worker.get("received_bytes") is None
                                    and worker.get("total_bytes") is None
                                    for worker in snapshot["workers"]))
    if (cooldown_active or all_counters_unknown or not active_downloads) and payload_age != "?":
        return "Last payload progress " + payload_age
    if completion_age != "?":
        return "Last complete " + completion_age
    if payload_age != "?":
        return "Last payload progress " + payload_age
    return "No payload progress recorded"


def disk_status(snapshot: dict[str, Any]) -> str:
    """Render safe filesystem capacity fields without exposing local paths."""
    if freshness(snapshot) != "live":
        return "Disk status unavailable"
    filesystems = snapshot["health"].get("filesystems")
    if not isinstance(filesystems, list) or not filesystems:
        return "Disk status unavailable"
    rendered = []
    for filesystem in filesystems:
        if not isinstance(filesystem, dict):
            return "Disk status unavailable"
        roles = filesystem.get("roles")
        free = filesystem.get("free_bytes")
        reserve = filesystem.get("reserve_bytes")
        headroom = filesystem.get("headroom_bytes")
        if (not isinstance(roles, list) or not roles or any(not isinstance(role, str)
                for role in roles) or any(not isinstance(value, int)
                for value in (free, reserve, headroom))):
            return "Disk status unavailable"
        status = " storage risk" if headroom < 0 else " storage stop" if headroom == 0 else ""
        rendered.append(f"Disk: {format_bytes(free)} free | "
                        f"{format_bytes(reserve)} reserve | "
                        f"{format_bytes(abs(headroom))} headroom{status}")
    return "\n".join(rendered)


class SpeedTrend:
    """Keep bounded display-only aggregate-speed history for one monitor."""

    def __init__(self) -> None:
        self.samples: deque[tuple[float, float | None]] = deque()
        self.session_id: str | None = None
        self.sequence: int | None = None

    def observe(self, snapshot: dict[str, Any], observed_at: float | None = None) -> None:
        current = time.monotonic() if observed_at is None else observed_at
        session_id = snapshot["session_id"]
        sequence = snapshot["sequence"]
        if self.session_id != session_id or (self.sequence is not None and sequence < self.sequence):
            self.samples.clear()
        if self.sequence == sequence and self.session_id == session_id:
            return
        if (self.samples and self.session_id == session_id
                and current - self.samples[-1][0] < 1):
            self.sequence = sequence
            return
        quality = metric_quality(snapshot, "speed_bps")
        value = metric_value(snapshot, "speed_bps")
        sample = float(value) if quality in {"exact", "estimated"} and value is not None else None
        self.samples.append((current, sample))
        self.session_id, self.sequence = session_id, sequence
        while self.samples and current - self.samples[0][0] > 300:
            self.samples.popleft()

    def label(self) -> str:
        valid = [(at, value) for at, value in self.samples if value is not None]
        if not valid:
            return "No data"
        latest_at = valid[-1][0]
        window = [(at, value) for at, value in valid if latest_at - at <= 60]
        if len(window) < 10 or window[-1][0] - window[0][0] < 30:
            return "Collecting"
        third = max(1, len(window) // 3)
        early = sum(value for _, value in window[:third]) / third
        late = sum(value for _, value in window[-third:]) / third
        if early <= 0:
            return "Rising" if late > 0 else "Steady"
        if late >= early * 1.1:
            return "Rising"
        if late <= early * 0.9:
            return "Falling"
        return "Steady"


def concise_status(snapshot: dict[str, Any]) -> str:
    run = snapshot["run"]
    counts = run["counts"]
    return "\n".join((
        f"TOD-DL {snapshot['run_id']} {run['lifecycle'].upper()} ({freshness(snapshot)})",
        "Files " + " | ".join(f"{name}={counts[name]}" for name in sorted(counts)
                                   if counts[name]),
        f"Data retained={format_bytes(metric_value(snapshot, 'retained_bytes'))} "
        f"speed={format_bytes(metric_value(snapshot, 'speed_bps'))}/s",
        retry_summary(snapshot),
    ))


def retry_summary(snapshot: dict[str, Any]) -> str:
    health = snapshot["health"]
    remaining = health.get("retry_remaining_s")
    cooldown = health.get("cooldown_remaining_s")
    if (snapshot["run"]["lifecycle"] == "finished"
            and not health.get("retry_pending_count", 0)):
        return "No retries pending"
    if health.get("retry_pending_count", 0) and remaining == 0:
        if cooldown:
            return f"Retry eligible now; SOCKS cooldown {format_countdown(cooldown)}"
        return "Retry eligible now; waiting for controller admission"
    if remaining is not None:
        return f"Retry eligible in {format_duration(remaining)}: " \
               f"{literal_text(health.get('retry_error') or 'waiting')[:120]}"
    if cooldown:
        return f"SOCKS cooldown {format_duration(cooldown)}"
    return "Retry: no pending deadline"


def worker_phase_label(worker: dict[str, Any], snapshot: dict[str, Any]) -> str:
    phase = literal_text(worker.get("phase", "unknown"))
    if phase == "downloading":
        progress_age = worker.get("last_progress_age_s")
        if (isinstance(progress_age, (int, float)) and not isinstance(progress_age, bool)
                and progress_age >= 60):
            return f"stalled {format_countdown(progress_age)}"
    if phase == "cooldown":
        remaining = snapshot["health"].get("cooldown_remaining_s")
        return f"cooldown {format_countdown(remaining)}" if remaining else "cooldown"
    return phase


def event_severity_style(value: Any) -> str:
    """Select a stable, readable style for an event severity label."""
    return EVENT_SEVERITY_STYLES.get(str(value).lower(), "dim")


def event_worker_label(event: dict[str, Any]) -> str:
    worker_id = event.get("worker_id")
    return (f"W{worker_id}" if isinstance(worker_id, int) and not isinstance(worker_id, bool)
            and worker_id > 0 else "")


def event_message_style(event: dict[str, Any]) -> str:
    """Emphasize a successful completed transfer without recoloring its level."""
    return "bold green" if event.get("category") == "complete" else ""


def event_item_path(event: dict[str, Any]) -> str:
    """Return a literal-safe decoded path for transfer lifecycle events only."""
    if event.get("category") not in {"attempt", "complete"}:
        return ""
    item_id = event.get("item_id")
    if not isinstance(item_id, str):
        return ""
    path = urlsplit(item_id).path
    return literal_text(unquote(path)) if path else ""


def event_timestamp(event: dict[str, Any]) -> str:
    """Render an event's recorded instant as a local, compact clock time."""
    value = event.get("at")
    if not isinstance(value, str):
        return "--:--:--"
    try:
        recorded = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return "--:--:--"
    if recorded.tzinfo is None:
        return "--:--:--"
    return recorded.astimezone().strftime("%H:%M:%S")


def lifecycle_style(value: Any) -> str:
    return LIFECYCLE_STYLES.get(str(value).lower(), "bold red")


def freshness_style(value: Any) -> str:
    return FRESHNESS_STYLES.get(str(value).lower(), "dim")


def screen_summary(snapshot: dict[str, Any], trend_label: str = "No data",
                   session_elapsed_s: float | None = None) -> str:
    run = snapshot["run"]
    counts = run["counts"]
    elapsed = snapshot["session_elapsed_s"] if session_elapsed_s is None else session_elapsed_s
    unknown_size_items = metric_value(snapshot, "unknown_size_items")
    eta = format_duration(metric_value(snapshot, "eta_seconds"))
    if eta == "—" and unknown_size_items:
        item_label = "item size" if unknown_size_items == 1 else "item sizes"
        eta += f" ({unknown_size_items} {item_label} unknown)"
    return "\n".join((
        f"TOD-DL  {literal_text(snapshot['run_id'])}  "
        f"{literal_text(run['lifecycle']).upper()}  Session "
        f"{format_elapsed_duration(elapsed)}  {progress_status(snapshot)}",
        "Files  " + " | ".join((
            f"{counts['complete']}/{run['selected_count']} complete",
            f"{counts['busy']} busy", f"{counts['retry']} retry",
            f"{counts['review_required']} review", f"{counts['queued']} queued",
        )),
        f"Data   {format_bytes(metric_value(snapshot, 'retained_bytes'))} retained | "
        f"{format_bytes(metric_value(snapshot, 'known_remaining_bytes'))} remaining",
        f"Speed  {format_bytes(metric_value(snapshot, 'speed_bps'))}/s | "
        f"{trend_label} | ETA {eta}",
        disk_status(snapshot),
        retry_summary(snapshot),
    ))


def run_textual(snapshot: dict[str, Any], snapshot_path: Path | None = None,
                state: Path | None = None, control: bool = False, fps: int = 30) -> int:
    try:
        from textual.app import App, ComposeResult
        from textual.containers import Horizontal, Vertical, VerticalScroll
        from rich.text import Text
        from textual.screen import ModalScreen
        from textual.widgets import Button, DataTable, Footer, Static
    except ImportError:
        print("Textual is optional. Install requirements-monitor.txt, or use "
              "./run.sh --status.", file=sys.stderr)
        return 2

    class ActionConfirmation(ModalScreen[bool]):
        BINDINGS = [("y", "confirm", "Yes"), ("n", "dismiss", "No"),
                    ("escape", "dismiss", "Cancel")]

        CSS = """
        ActionConfirmation {
            align: center middle;
        }
        #retry-confirmation {
            width: 58;
            height: auto;
            border: round $warning;
            background: $surface;
            padding: 1 2;
        }
        #retry-confirmation-buttons {
            align: center middle;
            height: auto;
            margin-top: 1;
        }
        #retry-confirmation-buttons Button {
            margin: 0 1;
        }
        """

        def __init__(self, action: str) -> None:
            super().__init__()
            self.action = action

        def compose(self) -> ComposeResult:
            message = ("Make all retryable selected items eligible now?\n"
                       "This does not add files or expand the selected run."
                       if self.action == "retry_now" else
                       "Ask Tor to use new circuits for future streams?\n"
                       "This does not prove a new route or affect active transfers.")
            with Vertical(id="retry-confirmation"):
                yield Static(message)
                with Horizontal(id="retry-confirmation-buttons"):
                    yield Button("Yes [y]", id="confirm", variant="warning")
                    yield Button("No [n]", id="cancel")

        def action_confirm(self) -> None:
            self.dismiss(True)

        def action_dismiss(self) -> None:
            self.dismiss(False)

        def on_button_pressed(self, event: Button.Pressed) -> None:
            self.dismiss(event.button.id == "confirm")

    class Monitor(App):
        BINDINGS = [("q", "quit", "Close"), ("r", "prepare_retry_now", "Retry now"),
                    ("t", "prepare_renew_tor_circuits", "Renew Tor")]
        CSS = """
        #activity-pane {
            height: 1fr;
            min-height: 5;
            overflow-y: auto;
            scrollbar-size: 1 1;
            border: round $accent;
            padding: 0 1;
        }
        #activity {
            height: auto;
        }
        """

        def summary_text(self, current: dict[str, Any]) -> Text:
            run = current["run"]
            state = literal_text(run["lifecycle"])
            connection = freshness(current)
            rendered = Text()
            rendered.append("TOD-DL", style="bold cyan")
            rendered.append("  ")
            rendered.append(literal_text(current["run_id"]), style="bold white")
            rendered.append("  ")
            rendered.append(state.upper(), style=lifecycle_style(state))
            rendered.append("  ")
            rendered.append(connection, style=freshness_style(connection))
            rendered.append("  Session ")
            rendered.append(format_elapsed_duration(self.session_elapsed(current)))
            rendered.append("  ")
            rendered.append(progress_status(current))
            trend_label = self.trend.label() if connection == "live" else "No data"
            remainder = screen_summary(current, trend_label,
                                       self.session_elapsed(current)).split("\n", 1)
            if len(remainder) == 2:
                for line in remainder[1].splitlines():
                    if line.startswith("Disk:"):
                        continue
                    rendered.append("\n")
                    label, separator, value = line.partition("  ")
                    if label in {"Files", "Data", "Speed"} and separator:
                        rendered.append(label, style="bold")
                        rendered.append(separator + value)
                    else:
                        rendered.append(line)
            return rendered

        @staticmethod
        def activity_text(events: list[dict[str, Any]]) -> Text:
            rendered = Text()
            for index, event in enumerate(events):
                rendered.append(f"{event_timestamp(event)} ", style="bold dim")
                severity = literal_text(event.get("severity", "?")).upper()
                rendered.append(f"{severity:7}", style=event_severity_style(severity))
                worker = event_worker_label(event)
                if worker:
                    rendered.append(f" {worker}", style="bold")
                rendered.append(f" {literal_text(event.get('message', ''))}",
                                style=event_message_style(event))
                item_path = event_item_path(event)
                if item_path:
                    rendered.append(f"\n  {item_path}", style="dim")
                if index < len(events) - 1:
                    rendered.append("\n")
            return rendered

        def compose(self) -> ComposeResult:
            yield Static("", id="summary")
            yield DataTable(id="workers")
            yield Static("", id="disk")
            with VerticalScroll(id="activity-pane", classes="event-log"):
                yield Static(self.activity_text(snapshot["recent_events"]), id="activity")
            yield Footer()

        def on_mount(self) -> None:
            table = self.query_one("#workers", DataTable)
            table.add_column("#", key="worker", width=3)
            table.add_column("Item", key="item", width=40)
            table.add_column("Phase", key="phase", width=13)
            table.add_column("Received / total", key="progress", width=21)
            table.add_column("Speed", key="speed", width=11)
            table.add_column("ETA", key="eta", width=9)
            self.query_one("#activity-pane", VerticalScroll).border_title = "Event log"
            self.current = snapshot
            self.trend = SpeedTrend()
            self.snapshot_sequence = None
            self.snapshot_elapsed_s = 0.0
            self.snapshot_observed_monotonic = time.monotonic()
            self.observe_snapshot(snapshot)
            self.last_summary_signature: str | None = None
            self.last_disk_signature: str | None = None
            self.last_event_signature: str | None = None
            self.rendered_rows: dict[str, tuple[str, ...]] = {}
            self.last_control_poll = 0.0
            self.control_state: dict[str, Any] | None = None
            self.control_error: str | None = None
            self.action_confirmation: dict[str, Any] | None = None
            self.control_request_active = False
            self.populate(snapshot, force=True)
            self.set_interval(1 / fps, self.render_frame)
            if snapshot_path:
                self.set_interval(1 / 2, self.refresh_snapshot)

        def populate(self, current: dict[str, Any], force: bool = False) -> None:
            summary_signature = self.summary_text(current).plain
            if force or summary_signature != self.last_summary_signature:
                self.query_one("#summary", Static).update(self.summary_text(current))
                self.last_summary_signature = summary_signature
            disk = disk_status(current)
            if force or disk != self.last_disk_signature:
                self.query_one("#disk", Static).update(disk)
                self.last_disk_signature = disk
            activity_pane = self.query_one("#activity-pane", VerticalScroll)
            follow_events = activity_pane.scroll_y >= activity_pane.max_scroll_y
            event_signature = json.dumps(current["recent_events"], sort_keys=True,
                                         separators=(",", ":"))
            if force or event_signature != self.last_event_signature:
                self.query_one("#activity", Static).update(
                    self.activity_text(current["recent_events"]))
                self.last_event_signature = event_signature
                if follow_events:
                    activity_pane.scroll_end(animate=False)
            table = self.query_one("#workers", DataTable)
            if force:
                table.clear(columns=False)
            for worker in sorted(current["workers"],
                                 key=lambda item: item.get("worker_id", 0)):
                item_id = worker.get("item_id")
                values = (
                    str(worker.get("worker_id", "?")),
                    marquee_filename(worker.get("basename"), int(time.monotonic() * 3), width=36)
                    if item_id else "idle",
                    worker_phase_label(worker, current),
                    f"{format_bytes(worker.get('received_bytes'))} / "
                    f"{format_bytes(worker.get('total_bytes'))}",
                    f"{format_bytes(worker.get('speed_bps'))}/s"
                    if worker.get("speed_bps") is not None else "—",
                    format_duration(worker.get("eta_seconds")),
                )
                row_key = str(worker.get("worker_id", "?"))
                if force:
                    table.add_row(*values, key=row_key)
                else:
                    previous = self.rendered_rows.get(row_key, ())
                    for column, value, prior in zip(("worker", "item", "phase", "progress",
                                                     "speed", "eta"), values, previous):
                        if value != prior:
                            table.update_cell(row_key, column, value, update_width=False)
                self.rendered_rows[row_key] = values

        def retry_now_eligible(self) -> bool:
            """Return whether the current immutable run has retryable items."""
            return bool(control and state and self.current["run"]["counts"]["retry"])

        def tor_renewal_eligible(self) -> bool:
            return bool(control and state and self.control_state
                        and self.control_state.get("actions", {}).get(
                            "renew_tor_circuits") == "available")

        def check_action(self, action: str, parameters: tuple[object, ...]) -> bool | None:
            if action == "prepare_retry_now" and not self.retry_now_eligible():
                return None
            if action == "prepare_renew_tor_circuits" and not self.tor_renewal_eligible():
                return None
            return True

        def render_frame(self) -> None:
            self.populate(self.current)
            if not control or not state or time.monotonic() - self.last_control_poll < 0.5:
                return
            self.last_control_poll = time.monotonic()
            self.submit_control_request(
                lambda: get_control_state(state, self.current["run_id"]),
                self.apply_control_state,
            )

        def observe_snapshot(self, current: dict[str, Any]) -> None:
            if current["sequence"] != self.snapshot_sequence:
                self.snapshot_sequence = current["sequence"]
                self.snapshot_elapsed_s = float(current["session_elapsed_s"])
                self.snapshot_observed_monotonic = time.monotonic()
                self.trend.observe(current, self.snapshot_observed_monotonic)

        def session_elapsed(self, current: dict[str, Any]) -> float:
            if freshness(current) != "live":
                return self.snapshot_elapsed_s
            return self.snapshot_elapsed_s + max(
                0.0, time.monotonic() - self.snapshot_observed_monotonic)

        def submit_control_request(self, operation, completed) -> None:
            """Run bounded socket I/O away from Textual's render and input loop."""
            if self.control_request_active:
                return
            self.control_request_active = True

            def worker() -> None:
                try:
                    result, error = operation(), None
                except ControlError as exc:
                    result, error = None, str(exc)
                self.call_from_thread(completed, result, error)

            threading.Thread(target=worker, name="monitor-control", daemon=True).start()

        def apply_control_state(self, result, error) -> None:
            self.control_request_active = False
            self.control_state = result if isinstance(result, dict) else None
            self.control_error = error

        def action_prepare_retry_now(self) -> None:
            if not self.retry_now_eligible():
                return
            self.prepare_action("retry_now")

        def action_prepare_renew_tor_circuits(self) -> None:
            if not self.tor_renewal_eligible():
                return
            self.prepare_action("renew_tor_circuits")

        def prepare_action(self, action: str) -> None:
            self.submit_control_request(
                lambda: control_request(state, self.current["run_id"], "prepare_confirmation",
                                        {"action": action}),
                self.apply_action_preparation,
            )

        def apply_action_preparation(self, payload, error) -> None:
            if error:
                self.control_request_active = False
                self.notify("Control action unavailable: " + literal_text(error), severity="error")
                return
            confirmation = payload.get("confirmation") if isinstance(payload, dict) else None
            if not isinstance(confirmation, dict):
                self.control_request_active = False
                self.notify("Control action unavailable: invalid controller response", severity="error")
                return
            self.control_request_active = False
            self.action_confirmation = confirmation
            self.push_screen(ActionConfirmation(str(confirmation.get("action"))),
                             self.action_confirmation_complete)

        def action_confirmation_complete(self, confirmed: bool | None) -> None:
            if not confirmed or not self.action_confirmation or not state:
                self.action_confirmation = None
                return
            confirmation = self.action_confirmation
            self.action_confirmation = None
            action = confirmation.get("action")
            if not isinstance(action, str):
                self.notify("Control action unavailable: invalid controller response", severity="error")
                return
            self.submit_control_request(
                lambda: control_request(
                    state, self.current["run_id"], action,
                    {"nonce": confirmation.get("nonce"), "confirmation": action},
                ),
                self.apply_action_result,
            )

        def apply_action_result(self, payload, error) -> None:
            self.control_request_active = False
            if error:
                self.notify("Control action rejected: " + literal_text(error), severity="error")
                return
            reason = payload.get("reason", "control action completed") if isinstance(payload, dict) else ""
            self.notify(literal_text(str(reason)), severity="information")

        def refresh_snapshot(self) -> None:
            try:
                was_retryable = self.retry_now_eligible()
                self.current = read_snapshot(snapshot_path)
                self.observe_snapshot(self.current)
                if was_retryable != self.retry_now_eligible():
                    self.refresh_bindings()
                self.populate(self.current)
            except SnapshotError:
                self.query_one("#summary", Static).update(
                    Text("Telemetry stale or unreadable; showing last valid display."))

    Monitor().run()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--demo", action="store_true", help="show the bundled synthetic fixture")
    source.add_argument("--fixture", type=Path, help="read a version-1 JSON fixture")
    parser.add_argument("--state", type=Path, default=Path("download-state"))
    parser.add_argument("--run-id", help="read this run's live snapshot")
    parser.add_argument("--control", action="store_true",
                        help="attach to the controller's local command endpoint")
    parser.add_argument("--fps", type=int, default=30,
                        help="Textual render frames per second (10 through 60, default: 30)")
    args = parser.parse_args()
    if not 10 <= args.fps <= 60:
        parser.error("--fps must be between 10 and 60")
    if args.demo:
        path = (Path(__file__).resolve().parents[1] / "tests" / "fixtures" /
                "monitor-v1-demo.json")
        snapshot = read_snapshot(path)
        snapshot_path = None
    elif args.fixture:
        snapshot = read_snapshot(args.fixture)
        snapshot_path = None
    else:
        root = args.state / "telemetry"
        snapshot = select_snapshot(args.state, args.run_id)
        snapshot_path = root / args.run_id / "snapshot.json" if args.run_id else None
    if not sys.stdout.isatty():
        print(concise_status(snapshot))
        return 1 if freshness(snapshot) == "disconnected" else 0
    return run_textual(snapshot, snapshot_path, args.state, args.control, args.fps)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SnapshotError as exc:
        print(f"Telemetry unavailable: {exc}", file=sys.stderr)
        raise SystemExit(1)
