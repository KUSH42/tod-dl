#!/usr/bin/env python3
"""Read-only terminal monitor for version-1 acquisition telemetry."""

from __future__ import annotations

import argparse
import csv
from collections import deque
import datetime as dt
import hashlib
import json
import logging
import re
import sys
import threading
import time
from pathlib import Path
from typing import Any, NamedTuple
from urllib.parse import unquote, urlsplit

from rich.cells import cell_len
from rich.text import Text

from controller import (COOLDOWN_OVERRIDE_MAX_S, COOLDOWN_OVERRIDE_MIN_S, ControlError,
                        PRIORITY_MAX, PRIORITY_MIN, control_request, get_control_state)
from inspection import InspectionError, inspection_request


FINAL_LIFECYCLES = {"finished", "stopped"}
ACTIVITY_MESSAGE_MAX = 120
ACTIVITY_MESSAGE_MIN = 20
ACTIVITY_NAME_MAX = 40
ACTIVITY_NAME_MIN = 12
ACTIVITY_SEVERITY_COLUMNS = 7
CATEGORY_SLUG = re.compile(r"[a-z0-9_]+")
MARQUEE_STEPS_PER_SECOND = 3
MARQUEE_END_PAUSE_STEPS = 2 * MARQUEE_STEPS_PER_SECOND
STALLED_AFTER_S = 60
ENGINE_SAMPLE_STALE_AFTER_S = 5
WORKER_ITEM_WIDTH = 36
REQUIRED_COUNTS = {
    "queued", "busy", "retry", "exhausted", "complete", "existing_unverified",
    "review_required", "unavailable", "excluded", "unknown",
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
    "live": "green",
    "stale": "yellow",
    "disconnected": "red",
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
        # a malformed timestamp gives an unknown age; the caller shows an unknown marker
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
            # an unreadable or partial snapshot is skipped; the next poll reads it again
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
    """Keep the head and the tail, with the extension, around one `…`."""
    return middle_truncate(literal_text(value), width)


def marquee_filename(value: Any, offset: int, width: int = 28) -> str:
    """Return one literal-safe scrolling window over a long basename.

    `offset` counts scroll steps. The window holds at the end of the name for
    `MARQUEE_END_PAUSE_STEPS` steps, then restarts from the beginning.
    """
    name = literal_text(value)
    if cell_len(name) <= width:
        return name
    last_start = next((index for index in range(len(name)) if cell_len(name[index:]) <= width),
                      len(name) - 1)
    start = min(offset % (last_start + MARQUEE_END_PAUSE_STEPS), last_start)
    window = ""
    for character in name[start:]:
        if cell_len(window + character) > width:
            break
        window += character
    return window


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
        # a malformed timestamp gives an unknown age; the display shows '?'
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
    if (cooldown_active or all_counters_unknown) and payload_age != "?":
        return "Last payload progress " + payload_age
    if not active_downloads and payload_age != "?":
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
        rendered.append(f"Disk  {format_bytes(free)} free | "
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


def is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def worker_stalled(worker: dict[str, Any]) -> bool:
    """Return whether this worker's own transfer has had no payload progress for 60 s."""
    progress_age = worker.get("last_progress_age_s")
    return (worker.get("phase") == "downloading" and is_number(progress_age)
            and progress_age >= STALLED_AFTER_S)


def worker_rate_cells(worker: dict[str, Any]) -> tuple[str, str]:
    """Render the speed and ETA cells; a stalled row or a stale engine sample has neither."""
    sample_age = worker.get("sample_age_s")
    if worker_stalled(worker) or (is_number(sample_age)
                                  and sample_age > ENGINE_SAMPLE_STALE_AFTER_S):
        return "—", "—"
    speed = worker.get("speed_bps")
    return (f"{format_bytes(speed)}/s" if speed is not None else "—",
            format_duration(worker.get("eta_seconds")))


def worker_progress_text(worker: dict[str, Any]) -> str:
    """Render received / total; `?` is an unknown total and only a known zero is an empty file."""
    total = worker.get("total_bytes")
    text = f"{format_bytes(worker.get('received_bytes'))} / {format_bytes(total)}"
    return text + " (empty file)" if total == 0 else text


def worker_item_cell(basename: Any, item_id: Any, width: int, duplicate: bool,
                     offset: int | None) -> str:
    """Render the item cell: a scrolled basename if `offset` is set, else a middle cut.

    A duplicate basename gets its short item ID, and the name takes the width left over.
    """
    suffix = f" [{short_item_id(item_id)}]" if duplicate else ""
    room = width - cell_len(suffix)
    if offset is None:
        return middle_truncate(literal_text(basename), room) + suffix
    return marquee_filename(basename, offset, width=room) + suffix


def worker_phase_label(worker: dict[str, Any], snapshot: dict[str, Any]) -> str:
    phase = literal_text(worker.get("phase", "unknown"))
    if worker_stalled(worker):
        return f"stalled {format_countdown(worker['last_progress_age_s'])}"
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
    """Dim an event message, except a completed transfer, which stays green."""
    return "green" if event.get("category") == "complete" else "dim"


def truncate_end(value: str, width: int) -> str:
    """Cut a string to at most `width` display columns, marking the cut with `…`."""
    if cell_len(value) <= width:
        return value
    kept = ""
    for character in value:
        if cell_len(kept + character) > width - 1:
            break
        kept += character
    return kept + "…" if width >= 1 else ""


def middle_truncate(value: str, width: int) -> str:
    """Cut a string to `width` display columns around one `…`, keeping head and tail."""
    if cell_len(value) <= width:
        return value
    tail_width = (width - 1) // 2
    head_width = width - 1 - tail_width
    head = ""
    for character in value:
        if cell_len(head + character) > head_width:
            break
        head += character
    tail = ""
    for character in reversed(value):
        if cell_len(character + tail) > tail_width:
            break
        tail = character + tail
    return head + "…" + tail


def event_basename(event: dict[str, Any]) -> str:
    """Return the literal-safe decoded basename of an item-bound event, never a directory."""
    item_id = event.get("item_id")
    if not isinstance(item_id, str):
        return ""
    return literal_text(unquote(urlsplit(item_id).path.rsplit("/", 1)[-1]))


def event_short_id(event: dict[str, Any]) -> str:
    item_id = event.get("item_id")
    return f"[{short_item_id(item_id)}]" if isinstance(item_id, str) and item_id else ""


def event_message(event: dict[str, Any]) -> str:
    """Return one literal line with paths masked, from the controller category if it has one.

    A category that is a machine slug such as `attempt` names a kind of event, not a
    phrase for the operator, so the message is the fallback for it.
    """
    category = event.get("category")
    if isinstance(category, str) and category and not CATEGORY_SLUG.fullmatch(category):
        text = category
    else:
        text = str(event.get("message", ""))
    tokens = ["[path]" if "/" in token or "\\" in token else token for token in text.split()]
    return truncate_end(literal_text(" ".join(tokens)), ACTIVITY_MESSAGE_MAX)


def collapse_repeated_events(events: list[dict[str, Any]]) -> list[tuple[dict[str, Any], int]]:
    """Merge consecutive events that read the same, keeping the latest one and a count."""
    collapsed: list[tuple[dict[str, Any], int, tuple[Any, ...]]] = []
    for event in events:
        key = (str(event.get("severity")), event.get("worker_id"), event.get("item_id"),
               event_message(event))
        if collapsed and collapsed[-1][2] == key:
            collapsed[-1] = (event, collapsed[-1][1] + 1, key)
        else:
            collapsed.append((event, 1, key))
    return [(event, count) for event, count, _key in collapsed]


def activity_segments(event: dict[str, Any], count: int = 1,
                      width: int | None = None) -> list[tuple[str, str]]:
    """Lay out one event as (text, style) pieces that fit `width` display columns."""
    severity = literal_text(event.get("severity", "?")).upper()
    worker = event_worker_label(event)
    short_id = event_short_id(event)
    name = middle_truncate(event_basename(event), ACTIVITY_NAME_MAX)
    base = event_message(event)
    suffix = f" ×{count}" if count > 1 else ""
    padded_severity = severity + " " * max(0, ACTIVITY_SEVERITY_COLUMNS - cell_len(severity))
    fixed = 8 + 2 + cell_len(padded_severity) + (2 + len(worker) if worker else 0)
    fixed += 2 + cell_len(short_id) if short_id else 0

    def room(shown_name: str) -> int:
        return (width - fixed - 2 - (2 + cell_len(shown_name) if shown_name else 0)
                if width is not None else ACTIVITY_MESSAGE_MAX)

    if width is not None and name:
        wanted = min(cell_len(base + suffix), ACTIVITY_MESSAGE_MIN)
        if room(name) < wanted:
            shorter = max(ACTIVITY_NAME_MIN, cell_len(name) - (wanted - room(name)))
            if shorter < cell_len(name):
                name = middle_truncate(name, shorter)
            if room(name) < wanted:
                name = ""
    limit = min(max(room(name), 1), ACTIVITY_MESSAGE_MAX)
    message = (base + suffix if cell_len(base + suffix) <= limit
               else truncate_end(base, limit - cell_len(suffix)) + suffix)
    segments = [(event_timestamp(event), "dim"), ("  ", ""),
                (padded_severity, event_severity_style(severity))]
    if worker:
        segments += [("  ", ""), (worker, "bold")]
    segments += [("  ", ""), (message, event_message_style(event))]
    if name:
        segments += [("  ", ""), (name, "")]
    if short_id:
        segments += [("  ", ""), (short_id, "white")]
    return segments


def event_timestamp(event: dict[str, Any]) -> str:
    """Render an event's recorded instant as a local clock time without a zone."""
    value = event.get("at")
    if not isinstance(value, str):
        return "--:--:--"
    try:
        recorded = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        # a malformed timestamp gives an unknown age; the display shows '--:--:--'
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


def detail_value(value: Any, reason: str = "not recorded") -> str:
    """Render an inspection value without treating an unknown as a zero."""
    if value is None:
        return f"? ({literal_text(reason)})"
    return literal_text(value)


def detail_bytes(value: Any, reason: str = "not recorded") -> str:
    """Show binary units and the exact byte value for one item field."""
    if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
        return f"{format_bytes(value)} ({value:,} bytes)"
    return detail_value(None, reason)


def detail_rate(value: Any, reason: str = "not recorded") -> str:
    """Show a nonnegative byte rate without converting an unknown to zero."""
    if isinstance(value, (int, float)) and not isinstance(value, bool) and value >= 0:
        return f"{format_bytes(int(value))}/s"
    return detail_value(None, reason)


def local_deadline(epoch: float, remaining: float) -> str:
    """Format a retry deadline in the operator's local time, with its countdown."""
    deadline = dt.datetime.fromtimestamp(epoch)
    return f"{deadline:%Y-%m-%d %H:%M:%S} ({format_countdown(remaining)} remaining)"


def format_retry_deadline(value: Any) -> str:
    """Format a durable retry deadline without exposing its epoch value."""
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return detail_value(value, "not recorded")
    remaining = value - time.time()
    if remaining <= 0:
        return "Eligible; awaiting controller"
    return local_deadline(value, remaining)


QUEUE_STATE_FILTERS = ("all", "queued", "busy", "retry", "exhausted", "complete",
                      "existing_unverified", "review_required", "unavailable",
                      "excluded", "unknown")
QUEUE_NOT_ELIGIBLE_BUCKETS = {"exhausted", "review_required", "excluded"}
COOLDOWN_STEP_S = 30
QUEUE_EXPORT_FIELDS = ("queue_rank", "item_id", "basename", "bucket", "priority",
                      "phase", "received_bytes", "total_bytes", "retry_at")


def queue_export_row(row: dict[str, Any]) -> dict[str, Any]:
    """Return one queue row's exportable fields only.

    This allowlist excludes the source URL and mapped storage path that
    `list_queue` never returns, keeping export consistent if a caller ever
    passes a richer row.
    """
    return {field: row.get(field) for field in QUEUE_EXPORT_FIELDS}


def queue_retry_status(bucket: Any, retry_at: Any, cooldown_active: bool = False,
                       reference_time: float | None = None) -> str:
    """Format one queue row's retry status without implying controller admission."""
    if bucket in QUEUE_NOT_ELIGIBLE_BUCKETS:
        return "Not eligible"
    if not isinstance(retry_at, (int, float)) or isinstance(retry_at, bool):
        return detail_value(retry_at, "not recorded")
    now = time.time() if reference_time is None else reference_time
    remaining = retry_at - now
    if remaining <= 0:
        return "Eligible; cooldown active" if cooldown_active else "Eligible; awaiting controller"
    return local_deadline(retry_at, remaining)


def queue_row_cells(row: dict[str, Any], cooldown_active: bool = False,
                    reference_time: float | None = None
                    ) -> tuple[str, str, str, str, str, str, str, str]:
    """Render one queue row's literal-safe display cells in column order."""
    bucket = row.get("bucket", "unknown")
    priority = row.get("priority", 0)
    return (
        str(row.get("queue_rank", "?")),
        truncate_filename(row.get("basename"), 32),
        short_item_id(row.get("item_id")),
        literal_text(bucket),
        f"{priority:+d}" if isinstance(priority, int) else "0",
        literal_text(row.get("phase")) if row.get("phase") else "—",
        f"{format_bytes(row.get('received_bytes'))} / {format_bytes(row.get('total_bytes'))}",
        queue_retry_status(bucket, row.get("retry_at"), cooldown_active, reference_time),
    )


def queue_header_text(run_id: Any, selected_count: Any, loaded_count: int,
                      active_filters: str, read_at: Any, revision: Any) -> str:
    """Build the queue tab's literal-safe header line."""
    return (f"Run: {detail_value(run_id)}  Selected: {detail_value(selected_count)}  "
            f"Loaded rows: {loaded_count}  Filters: {literal_text(active_filters)}  "
            f"Matching count unavailable  "
            f"Read: {detail_value(read_at, 'read time unavailable')}  "
            f"Revision: {detail_value(revision, 'revision unavailable')}")


# Detail views build a render model first, then render it at a width. A model is
# a list of DetailHeader, DetailSection, DetailField, and DetailNote rows.
UNAVAILABLE_REASONS = ("not in sample", "sample stale", "not applicable",
                       "controller did not report", "unsupported by engine")
DETAIL_LABEL_WIDE = 24
DETAIL_LABEL_NARROW = 18
DETAIL_NARROW_COLUMNS = 80
DETAIL_GAP = 2
DETAIL_STACK_INDENT = 2
DETAIL_MIN_VALUE_WIDTH = 10
NO_PROGRESS_SECONDS = 60
DETAIL_GUTTER = 2  # scrollbar columns, taken from the wrap width but not the layout tier
DETAIL_DEFAULT_WIDTH = 100


class DetailHeader(NamedTuple):
    """The first line: subject and status words on the left, read metadata on the right."""
    subject: str
    status: tuple[str, ...]
    freshness: str
    read_at: Any
    revision: Any


class DetailSection(NamedTuple):
    title: str
    dim: bool = False


class DetailField(NamedTuple):
    """One grid row: one label cell and one value cell."""
    label: str
    value: str
    dim: bool = False


class DetailNote(NamedTuple):
    text: str
    style: str | None = None


def detail_reason(reasons: Any, key: str) -> str:
    """Return the service's reason for an unavailable field.

    The view does not choose a reason. When the service sent none, or one
    outside the fixed set, the controller did not report it.
    """
    reason = reasons.get(key) if isinstance(reasons, dict) else None
    return reason if reason in UNAVAILABLE_REASONS else "controller did not report"


def detail_clock(value: Any, reason: str) -> str:
    """Show an instant as a local clock time, or an unknown value with its reason."""
    text = event_timestamp({"at": value})
    return detail_value(None if text == "--:--:--" else text, reason)


def decoded_basename(value: Any) -> str:
    """Percent-decode a basename, then neutralize terminal controls in the result."""
    return literal_text(unquote(str(value)))


def detail_header_line(header: DetailHeader, width: int) -> list[list[tuple[str, str | None]]]:
    """Lay out the header as parts per line, right-aligning the read metadata when it fits."""
    left: list[tuple[str, str | None]] = [(header.subject, "bold")]
    for word in header.status:
        left.append(("  " + word, "dim" if word.startswith("?") else None))
    left.append(("  " + header.freshness, freshness_style(header.freshness)))
    read = (f"Read {event_timestamp({'at': header.read_at})}  "
            f"Rev {'?' if header.revision is None else literal_text(header.revision)}")
    used = sum(cell_len(part) for part, _style in left)
    padding = width - DETAIL_GUTTER - used - cell_len(read)
    if padding >= DETAIL_GAP:
        return [left + [(" " * padding, None), (read, "dim")]]
    return [left, [(read, "dim")]]


def detail_grid_lines(row: DetailField, width: int) -> list[list[tuple[str, str | None]]]:
    """Wrap one field into label and value cells, or stack them below 80 columns."""
    dim_value = row.dim or row.value.startswith("?")
    label_style = "dim"
    value_style = "dim" if dim_value else None
    usable = width - DETAIL_GUTTER
    if width < DETAIL_NARROW_COLUMNS:
        label_width = max(usable, DETAIL_MIN_VALUE_WIDTH)
        value_width = max(usable - DETAIL_STACK_INDENT, DETAIL_MIN_VALUE_WIDTH)
        lines: list[list[tuple[str, str | None]]] = [
            [(part, label_style)] for part in wrap_cells(row.label, label_width)]
        indent = " " * DETAIL_STACK_INDENT
        lines.extend([(indent, None), (part, value_style)]
                     for part in wrap_cells(row.value, value_width))
        return lines
    column = DETAIL_LABEL_NARROW if width <= DETAIL_NARROW_COLUMNS else DETAIL_LABEL_WIDE
    labels = wrap_cells(row.label, column - DETAIL_GAP)
    values = wrap_cells(row.value, max(usable - column, DETAIL_MIN_VALUE_WIDTH))
    lines = []
    for index in range(max(len(labels), len(values))):
        label = labels[index] if index < len(labels) else ""
        value = values[index] if index < len(values) else ""
        pad = " " * (column - cell_len(label))
        lines.append([(label + pad, label_style), (value, value_style)])
    return lines


def wrap_cells(value: str, width: int) -> list[str]:
    """Wrap text to display columns at spaces, and split a word only when it cannot fit."""
    lines: list[str] = []
    current = ""
    for word in value.split(" "):
        while cell_len(word) > width:
            room = width - (cell_len(current) + 1 if current else 0)
            if room <= 0:
                lines.append(current)
                current, room = "", width
            head = ""
            for character in word:
                if cell_len(head + character) > room:
                    break
                head += character
            lines.append((current + " " + head) if current else head)
            current, word = "", word[len(head):]
        if not current:
            current = word
        elif cell_len(current) + 1 + cell_len(word) <= width:
            current += " " + word
        else:
            lines.append(current)
            current = word
    lines.append(current)
    return lines


def render_detail(model: list[Any], width: int) -> Text:
    """Render a detail model at a width, with one blank line before each section."""
    lines: list[list[tuple[str, str | None]]] = []
    for row in model:
        if isinstance(row, DetailHeader):
            lines.extend(detail_header_line(row, width))
        elif isinstance(row, DetailSection):
            if lines:
                lines.append([])
            lines.append([(row.title, "dim" if row.dim else "bold")])
        elif isinstance(row, DetailField):
            lines.extend(detail_grid_lines(row, width))
        else:
            lines.append([(row.text, row.style)])
    visual = Text()
    for index, line in enumerate(lines):
        for part, style in line:
            visual.append(part, style=style)
        if index < len(lines) - 1:
            visual.append("\n")
    return visual


def detail_no_progress(worker: dict[str, Any]) -> bool:
    age = worker.get("last_progress_age_s")
    return (worker.get("phase") == "downloading" and isinstance(age, (int, float))
            and not isinstance(age, bool) and age >= NO_PROGRESS_SECONDS)


def worker_details_model(worker: dict[str, Any], read_at: Any = None, revision: Any = None,
                         dashboard_revision: Any = None, sample_freshness: str = "?",
                         banner: tuple[str, ...] = ()) -> list[Any]:
    """Build the worker details model from one slot record."""
    reasons = worker.get("unavailable_reason")
    def why(key: str) -> str:
        return detail_reason(reasons, key)
    assignment = worker.get("assignment") if isinstance(worker.get("assignment"), dict) else None
    phase = worker.get("phase")
    model: list[Any] = [DetailHeader(
        f"Worker {detail_value(worker.get('worker_id'), why('worker_id'))}",
        (detail_value(phase, why("phase")),), literal_text(sample_freshness),
        read_at, revision)]
    if dashboard_revision is not None and revision != dashboard_revision:
        model.append(DetailNote(f"Dashboard revision {literal_text(dashboard_revision)}; "
                                "details differ"))
    model.extend(DetailNote(literal_text(line)) for line in banner)
    model.append(DetailSection("Assignment"))
    if not assignment or not assignment.get("item_id"):
        model.extend([DetailNote("No item assigned"),
                      DetailField("Reason", literal_text(worker.get("reason")
                                                         or "Reason unavailable"))])
        return model
    def value(source: dict[str, Any], key: str) -> str:
        return detail_value(source.get(key), why(key))
    def size(key: str) -> str:
        return detail_bytes(worker.get(key), why(key))
    basename = assignment.get("basename")
    progress_age = detail_age(worker.get("last_progress_age_s"), why("last_progress_age_s"))
    if not progress_age.startswith("?"):
        progress_age += " ago"
    model.extend([
        DetailField("Run", value(assignment, "run_id")),
        DetailField("Session", value(assignment, "session_id")),
        DetailField("Item ID", value(assignment, "item_id")),
        DetailField("Basename", decoded_basename(basename) if basename is not None
                    else detail_value(None, why("basename"))),
        DetailField("Generation", value(assignment, "generation")),
        DetailField("Attempt", value(assignment, "attempt_number")),
        DetailField("Engine instance", value(assignment, "engine_instance_id")),
        DetailField("Engine job", value(assignment, "engine_job_id")),
        DetailField("PID", value(assignment, "pid")),
        DetailSection("Activity"),
        DetailField("Reason", value(worker, "reason")),
        DetailField("Phase elapsed", detail_elapsed(worker.get("phase_elapsed_s"),
                                                    why("phase_elapsed_s"))),
        DetailField("Last transition", detail_clock(worker.get("last_transition_at"),
                                                    why("last_transition_at"))),
        DetailField("Attempt elapsed", detail_elapsed(worker.get("attempt_elapsed_s"),
                                                      why("attempt_elapsed_s"))),
        DetailField("Last payload progress", progress_age),
    ])
    if detail_no_progress(worker):
        model.append(DetailNote(f"No progress for {NO_PROGRESS_SECONDS}s"))
    eta = worker.get("eta_seconds")
    eta_reason = ("Estimating" if worker.get("estimator") == "Estimating"
                  and why("eta_seconds") != "sample stale" else why("eta_seconds"))
    model.extend([
        DetailSection("Transfer"),
        DetailField("Received", size("received_bytes")),
        DetailField("Total", size("total_bytes")),
        DetailField("Total source", value(worker, "total_source")),
        DetailField("Resume baseline", size("resume_baseline_bytes")),
        DetailField("Speed", detail_rate(worker.get("speed_bps"), why("speed_bps"))),
        DetailField("Smoothed speed", detail_rate(worker.get("smoothed_speed_bps"),
                                                  why("smoothed_speed_bps"))),
        DetailField("ETA (approximate)",
                    detail_value(format_duration(eta) if isinstance(eta, (int, float))
                                 and not isinstance(eta, bool) else None, eta_reason)),
        DetailField("Connections", value(worker, "connections")),
        DetailSection("Telemetry sample", dim=True),
        DetailField("Sample sequence", value(worker, "sample_sequence"), dim=True),
        DetailField("Sample age", detail_age(worker.get("sample_age_s"), why("sample_age_s")),
                    dim=True),
        DetailField("Quality", value(worker, "quality"), dim=True),
        DetailSection("Admission"),
    ])
    conditions = worker.get("admission")
    if isinstance(conditions, dict) and conditions:
        model.extend(DetailField(literal_text(name), literal_text(state))
                     for name, state in conditions.items())
    elif conditions:
        model.append(DetailField("Controller conditions", literal_text(conditions)))
    else:
        model.append(DetailField("Next eligible start", detail_value(None, why("admission"))))
    model.extend([DetailSection("Validation"),
                  DetailField("Validation", value(worker, "validation"))])
    return model


def detail_elapsed(value: Any, reason: str) -> str:
    """Show an elapsed time with units, or an unknown value with its reason."""
    if isinstance(value, (int, float)) and not isinstance(value, bool) and value >= 0:
        return format_elapsed_duration(value)
    return detail_value(None, reason)


def detail_age(value: Any, reason: str) -> str:
    """Show an age with a unit, or an unknown value with its reason."""
    if isinstance(value, (int, float)) and not isinstance(value, bool) and value >= 0:
        return format_countdown(value)
    return detail_value(None, reason)


def item_details_model(item: dict[str, Any], read_at: Any = None, revision: Any = None,
                       sample_freshness: str = "?", banner: tuple[str, ...] = (),
                       dashboard_revision: Any = None) -> list[Any]:
    """Build the item details model from one item record."""
    reasons = item.get("unavailable_reason")
    sections = {name: item.get(name, {}) for name in
                ("identity", "state", "engine", "bytes", "validation")}
    identity, state = sections["identity"], sections["state"]
    def field(section: Any, name: str, fallback: Any = None) -> Any:
        return section.get(name, fallback) if isinstance(section, dict) else fallback
    def shown(section: str, key: str, fallback: Any = None) -> str:
        return detail_value(field(sections[section], key, fallback),
                            detail_reason(reasons, f"{section}.{key}"))
    def sized(section: str, key: str, fallback: Any = None) -> str:
        return detail_bytes(field(sections[section], key, fallback),
                            detail_reason(reasons, f"{section}.{key}"))
    basename = field(identity, "basename", item.get("basename"))
    item_id = field(identity, "item_id", item.get("item_id"))
    bucket = field(state, "bucket", item.get("bucket"))
    phase = field(state, "phase")
    unknown_basename = detail_value(None, detail_reason(reasons, "identity.basename"))
    model: list[Any] = [DetailHeader(
        truncate_filename(unquote(str(basename)), 40) if basename is not None else unknown_basename,
        tuple(([f"[{short_item_id(item_id)}]"] if item_id is not None else [])
              + [detail_value(bucket, detail_reason(reasons, "state.bucket")),
                 detail_value(phase, detail_reason(reasons, "state.phase"))]),
        literal_text(sample_freshness), read_at, revision)]
    if dashboard_revision is not None and revision != dashboard_revision:
        model.append(DetailNote(f"Dashboard revision {literal_text(dashboard_revision)}; "
                                "details differ"))
    model.extend(DetailNote(literal_text(line)) for line in banner)
    reason_of = lambda key: detail_reason(reasons, key)
    retry_at = field(state, "retry_at", item.get("retry_at"))
    model.extend([
        DetailSection("Identity and paths"),
        DetailField("Item ID", shown("identity", "item_id", item.get("item_id"))),
        DetailField("Run ID", shown("identity", "run_id")),
        DetailField("Original path", shown("identity", "logical_path", item.get("logical_path"))),
        DetailField("Mapped storage path", shown("identity", "storage_path", item.get("storage_path"))),
        DetailField("Staging path", detail_value(item.get("staging_path"), reason_of("staging_path"))),
        DetailField("Candidate path", detail_value(item.get("candidate_path"), reason_of("candidate_path"))),
        DetailField("Queue rank", shown("identity", "queue_rank", item.get("queue_rank"))),
        DetailField("Generation", shown("identity", "generation")),
        DetailField("Attempt ID", shown("identity", "attempt_id")),
        DetailField("Mapping reason", shown("identity", "mapping_reason")),
        DetailField("Mapping version", shown("identity", "mapping_version")),
        DetailSection("State"),
        DetailField("Durable state", shown("state", "durable_state", item.get("durable_state"))),
        DetailField("Bucket", shown("state", "bucket", item.get("bucket"))),
        DetailField("Reason", shown("state", "phase_reason")),
        DetailField("Worker", shown("state", "worker_id")),
        DetailField("Last transition", shown("state", "last_transition_at", item.get("updated_at"))),
        DetailField("Attempts", f"{shown("state", 'attempt_count', item.get('attempt_count'))} / "
                                f"{shown("state", 'attempt_ceiling', item.get('attempt_ceiling'))}"),
        DetailField("Retry deadline", format_retry_deadline(retry_at)
                    if retry_at is not None else detail_value(None, reason_of("state.retry_at"))),
        DetailField("Blocking condition", shown("state", "blocking_condition")),
        DetailSection("Engine"),
        DetailField("Engine", f"{shown("engine", 'name')} {shown("engine", 'version')}"),
        DetailField("Instance", shown("engine", "instance_id")),
        DetailField("Job", shown("engine", "job_id")),
        DetailField("PID", shown("engine", "pid")),
        DetailField("Runtime sample", shown("engine", "sample_at")),
        DetailSection("Bytes"),
        DetailField("Received", sized("bytes", "received", item.get("received_bytes"))),
        DetailField("Resume baseline", sized("bytes", "resume_baseline")),
        DetailField("Transfer total", sized("bytes", "transfer_total")),
        DetailField("Total source", shown("bytes", "transfer_total_source")),
        DetailField("Trusted expected size", sized("bytes", "trusted_expected")),
        DetailField("Inventory size", shown("bytes", "inventory_size", item.get("inventory_size"))),
        DetailField("Retained item bytes", sized("bytes", "retained_item_bytes")),
        DetailField("Committed item completion bytes", sized("bytes", "committed_completion_bytes")),
        DetailSection("Telemetry sample", dim=True),
        DetailField("Sample sequence", shown("engine", "sample_sequence"), dim=True),
        DetailField("Sample age", detail_age(field(sections["engine"], "sample_age_s"),
                                             reason_of("engine.sample_age_s")), dim=True),
        DetailField("Quality", shown("engine", "quality"), dim=True),
        DetailSection("Validation"),
        DetailField("Method", shown("validation", "method")),
        DetailField("Processed bytes", sized("validation", "processed_bytes")),
        DetailField("Result", shown("validation", "result")),
        DetailField("Recorded at", shown("validation", "recorded_at")),
        DetailField("Expected SHA-256", shown("validation", "expected_sha256")),
        DetailField("Observed SHA-256", shown("validation", "observed_sha256", item.get("sha256"))),
        DetailField("Mismatch reason", shown("validation", "mismatch_reason")),
        DetailField("Promotion", shown("validation", "promotion_status")),
        DetailField("Staging cleanup", shown("validation", "staging_cleanup_at")),
        DetailSection("Source"),
        DetailField("Source", item_source_value(item)),
        DetailSection("Attempts and errors"),
        DetailNote("Select Next attempts to load another recorded page."),
    ])
    return model


def item_source_value(item: dict[str, Any]) -> str:
    """Show the source only after an explicit reveal, with its redaction label."""
    label = literal_text(item.get("source_label", "Source hidden"))
    if item.get("source") is not None:
        return f"{label}: {literal_text(item['source'])}"
    return f"{label}; press s to reveal" if label == "Source hidden" else label


def worker_details_text(worker: dict[str, Any], read_at: Any = None, revision: Any = None,
                        dashboard_revision: Any = None, sample_freshness: str = "?",
                        width: int = DETAIL_DEFAULT_WIDTH) -> str:
    """Render worker details as plain text, for tests and text export."""
    return render_detail(worker_details_model(worker, read_at, revision, dashboard_revision,
                                              sample_freshness), width).plain


def item_details_text(item: dict[str, Any], read_at: Any = None, revision: Any = None,
                      sample_freshness: str = "?",
                      width: int = DETAIL_DEFAULT_WIDTH) -> str:
    """Render item details as plain text, for tests and text export."""
    return render_detail(item_details_model(item, read_at, revision, sample_freshness),
                         width).plain


# Footer entries per screen, in the order SPEC-console-keymap.md requires:
# navigation keys, screen-local actions, then command keys with `q` last.
FOOTER_ENTRIES: dict[str, tuple[tuple[str, str], ...]] = {
    "dashboard": (("↑↓", "Select"), ("Enter", "Details"), ("Tab", "Pane"), ("l", "Logs"),
                  ("?", "Help"), ("r", "Retry now"), ("t", "Renew Tor"), ("q", "Close")),
    "queue": (("Tab", "Focus"), ("↑↓", "Select"), ("Enter", "Details"), ("/", "Search"),
              ("l", "Logs"), ("?", "Help"), ("R", "Retry row"), ("x", "Exclude row")),
    "worker": (("Esc", "Back"), ("↑↓", "Scroll"), ("i", "Item details"), ("l", "Logs"),
               ("?", "Help")),
    "item": (("Esc", "Back"), ("↑↓", "Attempt"), ("n", "Next attempts"), ("l", "Logs"),
             ("s", "Source"), ("?", "Help")),
}
FOOTER_WIDE_COLUMNS = 120
FOOTER_WIDE_LIMIT = 8
FOOTER_NARROW_LIMIT = 5
DISABLED_CONTROL_NOTICE = "Not available here; press Escape to return"
DISABLED_CONTROL_NOTICE_S = 3
# Keys that a detail screen binds to a no-op so they cannot reach the dashboard.
DISABLED_CONTROL_KEYS = ("q", "r", "t", "p", "u", "d", "k")
# Footer command keys whose action `check_action` can disable; the footer dims them.
FOOTER_GATED_KEYS = ("r", "t", "R", "x")
KEY_DISPLAY = {
    "question_mark": "?", "slash": "/", "right_square_bracket": "]",
    "left_square_bracket": "[", "right_curly_bracket": "}", "left_curly_bracket": "{",
    "escape": "Esc", "pageup": "PageUp", "pagedown": "PageDown", "up": "↑", "down": "↓",
    "ctrl+c": "Ctrl+C", "tab": "Tab", "shift+tab": "Shift+Tab", "enter": "Enter",
}


def footer_entries(screen: str, width: int) -> list[tuple[str, str]]:
    """Return the footer entries for a screen at a terminal width.

    Wide terminals (120 columns or more) show up to 8 entries and every other
    width shows up to 5. `?` is always kept: when it falls outside the first 5
    entries, it replaces the fifth.
    """
    entries = list(FOOTER_ENTRIES[screen])
    if width >= FOOTER_WIDE_COLUMNS:
        return entries[:FOOTER_WIDE_LIMIT]
    shown = entries[:FOOTER_NARROW_LIMIT]
    if not any(key == "?" for key, _label in shown):
        shown[-1] = next(entry for entry in entries if entry[0] == "?")
    return shown


def footer_text(screen: str, width: int) -> str:
    return "  ".join(f"{key} {label}" for key, label in footer_entries(screen, width))


def binding_key_label(key: str) -> str:
    return KEY_DISPLAY.get(key, key)


def binding_kind(action: str) -> str:
    """Name how the help screen classifies a bound action.

    Every `prepare_*` action opens a confirmation, except the export prompt,
    which asks for a local file path and sends no controller request.
    """
    if action.startswith("prepare_") and action != "prepare_export":
        return "confirmed"
    return "read-only"


def help_rows(binding_tables: list[list[Any]]) -> list[tuple[str, str, str]]:
    """Return (keys, meaning, kind) rows for bindings, keeping each key once.

    `binding_tables` holds Textual BINDINGS lists. A binding without a
    description is a hidden no-op; the disabled-control keys collapse into one row.
    """
    from textual.binding import Binding
    rows: list[tuple[str, str, str]] = []
    seen: set[str] = set()
    hidden: list[str] = []
    for table in binding_tables:
        for binding in Binding.make_bindings(table):
            if binding.key in seen:
                continue
            seen.add(binding.key)
            if binding.action == "disabled_control":
                hidden.append(binding_key_label(binding.key))
            elif binding.description:
                rows.append((binding_key_label(binding.key), binding.description,
                             binding_kind(binding.action)))
    if hidden:
        rows.append((" ".join(hidden), DISABLED_CONTROL_NOTICE, "read-only"))
    return rows


def build_monitor_app(snapshot: dict[str, Any], snapshot_path: Path | None = None,
                      state: Path | None = None, control: bool = False,
                      fps: int = 30) -> type | None:
    """Build the Monitor Textual App class without running it.

    Shared by run_textual (which calls .run()) and headless interaction
    tests (which call .run_test()); returns None if Textual is not
    installed.
    """
    try:
        from textual.app import App, ComposeResult
        from textual.binding import Binding
        from textual.containers import Horizontal, Vertical, VerticalScroll
        from rich.text import Text
        from textual.screen import ModalScreen, Screen
        from textual.widgets import (Button, DataTable, Input, Select, Static,
                                     TabbedContent, TabPane)
    except ImportError:
        print("Textual is optional. Install requirements-monitor.txt, or use "
              "./run.sh --status.", file=sys.stderr)
        return None

    section_labels = {"Identity and paths", "State", "Engine", "Bytes", "Validation",
                      "Source", "Attempts and errors", "Assignment", "Activity",
                      "Transfer", "Admission", "Worker details", "Item details"}

    def detail_visual(output: str) -> Text:
        """Style trusted labels without parsing untrusted values as Rich markup.

        Unavailable values ("? (...)") are dimmed so recorded data stands
        out against the repeated placeholder text.
        """
        visual = Text()
        placeholder = "Matching count unavailable"

        def append_value(text: str) -> None:
            index = text.find(placeholder)
            if index == -1:
                visual.append(text, style="dim" if text.lstrip().startswith("?") else None)
                return
            visual.append(text[:index], style="dim" if text[:index].lstrip().startswith("?") else None)
            visual.append(placeholder, style="dim")
            visual.append(text[index + len(placeholder):])

        for line_number, line in enumerate(output.splitlines()):
            if line in section_labels:
                visual.append(line, style="bold")
            elif line.startswith("→ "):
                visual.append(line, style="reverse")
            else:
                position = 0
                for match in re.finditer(r"(?:^|  )([^:\n]{1,40}:)", line):
                    append_value(line[position:match.start(1)])
                    visual.append(match.group(1), style="dim")
                    position = match.end(1)
                append_value(line[position:])
            if line_number < len(output.splitlines()) - 1:
                visual.append("\n")
        return visual

    def retry_status_visual(text: str) -> Text:
        """Dim retry status text; the deadline and countdown values stay default style."""
        match = re.fullmatch(r"(.+) \((.+) remaining\)", text)
        if not match:
            return Text(text, style="dim")
        visual = Text(match.group(1))
        visual.append(" (", style="dim")
        visual.append(match.group(2))
        visual.append(" remaining)", style="dim")
        return visual

    class KeyFooter(Static):
        """One-line footer drawn from FOOTER_ENTRIES, with a timed notice slot."""
        DEFAULT_CSS = "KeyFooter { dock: bottom; height: 1; background: $panel; }"

        def __init__(self, screen_name: str) -> None:
            super().__init__("", id="key-footer")
            self.screen_name = screen_name
            self.notice: str | None = None
            self.notice_timer: Any = None

        def on_mount(self) -> None:
            self.screen.bindings_updated_signal.subscribe(self, lambda _screen: self.render_footer())
            self.render_footer()

        def on_resize(self, event: Any) -> None:
            self.render_footer()

        def entry_available(self, key: str) -> bool:
            """Report whether a command key can act now, using the same gate as the bindings."""
            if key not in FOOTER_GATED_KEYS:
                return True
            active = self.screen.active_bindings.get(key)
            return bool(active and active.enabled)

        def set_screen_name(self, screen_name: str) -> None:
            self.screen_name = screen_name
            self.render_footer()

        def show_notice(self, message: str, seconds: float) -> None:
            if self.notice_timer is not None:
                self.notice_timer.stop()
            self.notice = message
            self.notice_timer = self.set_timer(seconds, self.clear_notice)
            self.render_footer()

        def clear_notice(self) -> None:
            self.notice, self.notice_timer = None, None
            self.render_footer()

        def render_footer(self) -> None:
            rendered = Text(no_wrap=True, overflow="ellipsis")
            if self.notice:
                rendered.append(self.notice, style="bold")
            else:
                for index, (key, label) in enumerate(
                        footer_entries(self.screen_name, self.app.size.width)):
                    if index:
                        rendered.append("  ")
                    if self.entry_available(key):
                        rendered.append(key, style="bold")
                        rendered.append(" " + label)
                    else:
                        rendered.append(f"{key} {label}", style="dim")
            self.update(rendered)

    class HelpScreen(ModalScreen[None]):
        """List the bindings of the screen that opened it; sends no request."""
        BINDINGS = [Binding("escape", "dismiss", "Close")]

        DEFAULT_CSS = """
        HelpScreen {
            align: center middle;
        }
        #help-panel {
            width: 76;
            height: auto;
            max-height: 90%;
            border: round $accent;
            background: $surface;
            padding: 1 2;
        }
        """

        def __init__(self, title: str, rows: list[tuple[str, str, str]]) -> None:
            super().__init__()
            self.title_text = title
            self.rows = rows

        def compose(self) -> ComposeResult:
            with Vertical(id="help-panel"):
                yield Static(self.title_text, id="help-title")
                with VerticalScroll():
                    yield Static("\n".join(
                        f"{keys:<14}{meaning}  [{kind}]" for keys, meaning, kind in self.rows),
                        id="help-rows")
                yield Static("Escape closes this help.")

        def action_dismiss(self) -> None:
            self.dismiss(None)

    class ActionConfirmation(ModalScreen[bool]):
        BINDINGS = [("y", "confirm", "Yes"), ("n", "dismiss", "No"),
                    ("escape", "dismiss", "Cancel")]

        DEFAULT_CSS = """
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

        def __init__(self, action: str, item_count: int | None = None) -> None:
            super().__init__()
            self.action = action
            self.item_count = item_count

        def compose(self) -> ComposeResult:
            if self.action == "retry_now":
                message = (f"Make {self.item_count} selected item(s) eligible now?\n"
                           "This does not add files or expand the selected run."
                           if self.item_count is not None else
                           "Make all retryable selected items eligible now?\n"
                           "This does not add files or expand the selected run.")
            elif self.action == "exclude_item":
                message = (f"Exclude {self.item_count} selected item(s)?\n"
                           "This stops future admission and retry for them. It cannot "
                           "be undone in this release and does not change queue rank.")
            elif self.action == "retry_access_denied":
                message = (f"Retry {self.item_count} selected access-denied item(s)?\n"
                           "This returns them to queued and lifts the origin pause. "
                           "A repeated 401 or 403 pauses the origin again.")
            elif self.action == "resume_new_generation":
                message = (f"Restart {self.item_count} selected item(s) under a new generation?\n"
                           "This returns them to queued and clears their review code. "
                           "The next attempt starts a fresh download instead of "
                           "resuming staged bytes.")
            elif self.action == "pause_admission":
                message = ("Pause admission of new transfers?\n"
                           "Active transfers keep running; nothing new is admitted "
                           "until admission is resumed.")
            elif self.action == "resume_admission":
                message = ("Resume admission for this run?\n"
                           "This reopens admission only for the immutable selected run.")
            elif self.action == "drain_and_stop":
                message = ("Drain and stop this run?\n"
                           "Admission stops now; active transfers run to a durable "
                           "state, then the run exits. This cannot be undone.")
            elif self.action == "checkpoint_stop":
                message = ("Checkpoint and stop this run?\n"
                           "Active transfers are checkpointed and terminated safely, "
                           "then the run exits. This cannot be undone.")
            else:
                message = ("Ask Tor to use new circuits for future streams?\n"
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

    class ExportDestination(ModalScreen[str | None]):
        """Prompt for a local file path; triggers no controller action."""
        BINDINGS = [Binding("escape", "cancel", "Cancel", show=False)]

        DEFAULT_CSS = """
        ExportDestination {
            align: center middle;
        }
        #export-destination {
            width: 64;
            height: auto;
            border: round $accent;
            background: $surface;
            padding: 1 2;
        }
        """

        def compose(self) -> ComposeResult:
            with Vertical(id="export-destination"):
                yield Static("Export queue rows to file (Enter to confirm, Escape to cancel):")
                yield Input(placeholder="queue-export.csv", id="export-path")

        def on_mount(self) -> None:
            self.query_one("#export-path", Input).focus()

        def on_input_submitted(self, event: Input.Submitted) -> None:
            self.dismiss(event.value.strip() or None)

        def action_cancel(self) -> None:
            self.dismiss(None)

    class ItemDetails(Screen[None]):
        """Read-only view bound to one immutable run and item identity."""
        BINDINGS = [("escape", "dismiss", "Back"), ("s", "toggle_source", "Reveal or hide source"),
                    ("n", "next_attempts", "Next attempts"), ("l", "logs", "Logs"),
                    Binding("up", "select_previous_attempt", "Previous attempt", show=False,
                           priority=True),
                    Binding("down", "select_next_attempt", "Next attempt", show=False,
                           priority=True),
                    *[Binding(key, "disabled_control", show=False)
                      for key in DISABLED_CONTROL_KEYS]]
        DEFAULT_CSS = "#item-details { height: 1fr; overflow-y: auto; }"

        def __init__(self, run_id: str, item_id: str, dashboard_revision: Any = None) -> None:
            super().__init__()
            self.run_id = run_id
            self.item_id = item_id
            self.dashboard_revision = dashboard_revision
            self.item: dict[str, Any] | None = None
            self.read_at: Any = None
            self.revision: Any = None
            self.cursor: str | None = None
            self.attempts: list[dict[str, Any]] = []
            self.selected_attempt_index = 0
            self.revealed = False
            self.session_id: Any = None
            self.session_reload_pending = False
            self.last_update: tuple[str | None, bool] = (None, False)

        def compose(self) -> ComposeResult:
            with VerticalScroll(id="item-details"):
                yield Static("Loading item details…", id="item-details-text")
            yield KeyFooter("item")

        def on_mount(self) -> None:
            self.session_id = self.app.current.get("session_id")
            self.load_item()
            self.set_interval(2, self.check_session)

        def check_session(self) -> None:
            current_session_id = self.app.current.get("session_id")
            if current_session_id == self.session_id:
                return
            self.session_id = current_session_id
            self.item = None
            self.attempts = []
            self.selected_attempt_index = 0
            self.cursor = None
            self.revealed = False
            self.session_reload_pending = True
            self.load_item()

        def detail_width(self) -> int:
            """Return the terminal columns the layout tier and wrapping use."""
            return self.size.width or DETAIL_DEFAULT_WIDTH

        def on_resize(self, event: Any) -> None:
            self.update_details(*self.last_update)

        def update_details(self, error: str | None = None, retain: bool = False) -> None:
            self.last_update = (error, retain)
            freshness_label = freshness(self.app.current)
            if error and not (retain and self.item):
                model: list[Any] = [DetailNote("Details unavailable", "bold"),
                                    DetailNote(literal_text(error))]
            else:
                banner = ("Last-known values retained", error) if error else ()
                model = item_details_model(self.item or {}, self.read_at, self.revision,
                                           freshness_label, banner, self.dashboard_revision)
            if self.attempts:
                marker = "→"
                model.append(DetailSection("Recorded attempts"))
                model.extend(DetailNote(
                    f"{marker if index == self.selected_attempt_index else ' '} "
                    f"#{detail_value(row.get('attempt_number'))} {detail_value(row.get('attempt_id'))} "
                    f"Generation: {detail_value(row.get('generation'))}  "
                    f"Started: {detail_value(row.get('started_at'))}  "
                    f"Ended: {detail_value(row.get('ended_at'))}  "
                    f"Outcome: {detail_value(row.get('outcome'))}  "
                    f"Error: {detail_value(row.get('error_category'))} "
                    f"{detail_value(row.get('error_message'))}  "
                    f"Retry deadline: {format_retry_deadline(row.get('retry_at'))}",
                    "reverse" if index == self.selected_attempt_index else None)
                    for index, row in enumerate(self.attempts))
            self.query_one("#item-details-text", Static).update(
                render_detail(model, self.detail_width()))

        def load_item(self) -> None:
            self.app.submit_inspection(
                lambda: inspection_request(state, self.run_id, "get_item",
                                            {"item_id": self.item_id,
                                             "reveal_source": self.revealed}),
                self.apply_item)

        def apply_item(self, response: dict[str, Any] | None, error: str | None) -> None:
            session_reload = self.session_reload_pending
            self.session_reload_pending = False
            if error or not response:
                self.update_details(error or "inspection returned no record", retain=not session_reload)
                return
            data = response.get("data", {})
            item = data.get("item") if isinstance(data, dict) else None
            if not isinstance(item, dict):
                self.update_details("inspection returned an invalid item", retain=not session_reload)
                return
            self.item = item
            self.read_at = response.get("read_at")
            self.revision = response.get("state_revision")
            self.update_details()

        def action_toggle_source(self) -> None:
            if not self.revealed:
                self.revealed = True
                self.load_item()
                return
            self.revealed = False
            if self.item:
                self.item["source"] = None
                self.item["source_label"] = "Source hidden"
            self.update_details()

        def action_next_attempts(self) -> None:
            parameters: dict[str, Any] = {"item_id": self.item_id, "page_size": 200}
            if self.cursor:
                parameters["cursor"] = self.cursor
            self.app.submit_inspection(
                lambda: inspection_request(state, self.run_id, "list_attempts", parameters),
                self.apply_attempts)

        def apply_attempts(self, response: dict[str, Any] | None, error: str | None) -> None:
            if error or not response:
                self.app.notify("Results changed: " + literal_text(error or "request failed"), severity="warning")
                return
            if self.revision is not None and response.get("state_revision") != self.revision:
                self.app.notify("Results changed; restart attempts from the first page.", severity="warning")
                return
            data = response.get("data", {})
            rows = data.get("attempts") if isinstance(data, dict) else None
            if not isinstance(rows, list):
                self.app.notify("Attempt history is unavailable.", severity="warning")
                return
            self.attempts.extend(row for row in rows if isinstance(row, dict))
            self.cursor = data.get("next_cursor") if isinstance(data.get("next_cursor"), str) else None
            self.selected_attempt_index = min(self.selected_attempt_index, len(self.attempts) - 1)
            self.update_details()

        def action_select_previous_attempt(self) -> None:
            if not self.attempts:
                self.query_one("#item-details", VerticalScroll).scroll_up()
                return
            self.selected_attempt_index = max(0, self.selected_attempt_index - 1)
            self.update_details()

        def action_select_next_attempt(self) -> None:
            if not self.attempts:
                self.query_one("#item-details", VerticalScroll).scroll_down()
                return
            self.selected_attempt_index = min(len(self.attempts) - 1, self.selected_attempt_index + 1)
            self.update_details()

        def action_logs(self) -> None:
            self.app.notify("Item logs are unavailable", severity="warning")

        def action_dismiss(self) -> None:
            self.revealed = False
            self.app.pop_screen()

        def action_disabled_control(self) -> None:
            self.query_one(KeyFooter).show_notice(
                DISABLED_CONTROL_NOTICE, DISABLED_CONTROL_NOTICE_S)

    class WorkerDetails(Screen[None]):
        """Read-only view bound to one worker slot in one controller session."""
        BINDINGS = [("escape", "dismiss", "Back"), ("i", "item_details", "Item details"),
                    ("l", "logs", "Logs"),
                    *[Binding(key, "disabled_control", show=False)
                      for key in DISABLED_CONTROL_KEYS]]
        DEFAULT_CSS = "#worker-details { height: 1fr; overflow-y: auto; }"

        def __init__(self, run_id: str, session_id: str, worker_id: int,
                     dashboard_revision: Any) -> None:
            super().__init__()
            self.run_id, self.session_id, self.worker_id = run_id, session_id, worker_id
            self.dashboard_revision = dashboard_revision
            self.worker: dict[str, Any] | None = None
            self.read_at: Any = None
            self.revision: Any = None
            self.displayed_item_id: str | None = None
            self.session_ended = False
            self.last_update: str | None = None

        def compose(self) -> ComposeResult:
            with VerticalScroll(id="worker-details"):
                yield Static("Loading worker details…", id="worker-details-text")
            yield KeyFooter("worker")

        def on_mount(self) -> None:
            self.load_worker()
            self.set_interval(2, self.refresh_worker)

        def refresh_worker(self) -> None:
            if self.app.current.get("session_id") != self.session_id:
                self.session_ended = True
                self.update_details()
                return
            self.load_worker()

        def load_worker(self) -> None:
            if self.session_ended:
                return
            self.app.submit_inspection(
                lambda: inspection_request(state, self.run_id, "get_worker",
                                            {"worker_id": self.worker_id}), self.apply_worker)

        def apply_worker(self, response: dict[str, Any] | None, error: str | None) -> None:
            if error or not response:
                if error and "session" in error:
                    self.session_ended = True
                self.update_details(error or "inspection returned no worker")
                return
            data = response.get("data", {})
            worker = data.get("worker") if isinstance(data, dict) else None
            if not isinstance(worker, dict):
                self.update_details("inspection returned an invalid worker")
                return
            self.worker, self.read_at, self.revision = worker, response.get("read_at"), response.get("state_revision")
            assignment = worker.get("assignment")
            item_id = (assignment.get("item_id") if isinstance(assignment, dict)
                       and isinstance(assignment.get("item_id"), str) else None)
            self.displayed_item_id = item_id
            self.update_details()

        def detail_width(self) -> int:
            """Return the terminal columns the layout tier and wrapping use."""
            return self.size.width or DETAIL_DEFAULT_WIDTH

        def on_resize(self, event: Any) -> None:
            self.update_details(self.last_update)

        def update_details(self, error: str | None = None) -> None:
            self.last_update = error
            if self.session_ended:
                model: list[Any] = [DetailNote("Worker details", "bold"),
                                    DetailNote("Session ended"),
                                    DetailNote("Return to the current dashboard.")]
            else:
                banner = ("Last-known values retained", error) if error else ()
                if error and not self.worker:
                    model = [DetailNote("Worker details", "bold"), *map(DetailNote, banner)]
                else:
                    model = worker_details_model(
                        self.worker or {"worker_id": self.worker_id}, self.read_at,
                        self.revision, self.dashboard_revision, freshness(self.app.current),
                        banner)
            self.query_one("#worker-details-text", Static).update(
                render_detail(model, self.detail_width()))

        def action_item_details(self) -> None:
            if not self.displayed_item_id:
                self.app.notify("No item assigned", severity="warning")
                return
            self.app.push_screen(ItemDetails(self.run_id, self.displayed_item_id,
                                             self.dashboard_revision))

        def action_logs(self) -> None:
            self.app.notify("No item logs" if not self.displayed_item_id else "Item logs are unavailable", severity="warning")

        def action_dismiss(self) -> None:
            self.app.pop_screen()

        def action_disabled_control(self) -> None:
            self.query_one(KeyFooter).show_notice(
                DISABLED_CONTROL_NOTICE, DISABLED_CONTROL_NOTICE_S)

    class QueueSearchInput(Input):
        """A literal-substring search box that cancels unsubmitted edits on Escape."""
        BINDINGS = [Binding("escape", "cancel", "Cancel", show=False)]

        def __init__(self, pane: "QueuePane", **kwargs: Any) -> None:
            super().__init__(**kwargs)
            self.pane = pane

        def action_cancel(self) -> None:
            self.value = self.pane.search_text
            self.blur()

    class QueuePane(Vertical):
        """Paginated, filtered browsing of the immutable selected queue."""
        BINDINGS = [
            Binding("slash", "focus_search", "Search", show=True),
            Binding("pagedown", "next_page", "Next page", priority=True),
            Binding("pageup", "previous_page", "Prev page", priority=True),
            Binding("c", "clear_filters", "Clear filters"),
            Binding("f", "refresh_results", "Refresh results", show=False),
            Binding("g", "first_page", "First page", show=False),
            Binding("R", "prepare_retry_selected", "Retry row", show=True),
            Binding("x", "prepare_exclude_selected", "Exclude row", show=True),
            Binding("A", "prepare_retry_access_denied_selected", "Retry denied row", show=True),
            Binding("N", "prepare_resume_new_generation_selected", "New generation row",
                    show=True),
            Binding("right_square_bracket", "prepare_raise_priority_selected",
                   "Raise priority", show=True),
            Binding("left_square_bracket", "prepare_lower_priority_selected",
                   "Lower priority", show=True),
            Binding("right_curly_bracket", "prepare_raise_cooldown_selected",
                   "Raise cooldown", show=True),
            Binding("left_curly_bracket", "prepare_lower_cooldown_selected",
                   "Lower cooldown", show=True),
            Binding("e", "prepare_export", "Export queue to a local file", show=True),
            Binding("l", "logs", "Logs"),
        ]
        DEFAULT_CSS = """
        QueuePane { height: 1fr; }
        #queue-filters { height: 3; }
        #queue-filters Select { width: 26; }
        #queue-filters Input { width: 1fr; }
        #queue-table { height: 1fr; }
        """

        def __init__(self) -> None:
            super().__init__(id="queue-pane")
            self.bucket = "all"
            self.search_text = ""
            self.cursor_stack: list[str | None] = [None]
            self.page_index = 0
            self.next_cursor: str | None = None
            self.rows: list[dict[str, Any]] = []
            self.read_at: Any = None
            self.revision: Any = None
            self.selected_item_id: str | None = None
            self.marquee_item_id: str | None = None
            self.marquee_started_at: float = 0.0
            self.request_active = False
            self.export_active = False
            self.export_path: str | None = None
            self.export_bucket = "all"
            self.export_query = ""
            self.export_collected: list[dict[str, Any]] = []
            self.wide = True

        def compose(self) -> ComposeResult:
            yield Static("", id="queue-header")
            with Horizontal(id="queue-filters"):
                yield Select([(name.capitalize() if name != "all" else "All", name)
                             for name in QUEUE_STATE_FILTERS], value="all",
                             allow_blank=False, id="queue-bucket")
                yield QueueSearchInput(self, placeholder="/ to search, Enter to apply",
                                       id="queue-search")
            yield Static("", id="queue-banner")
            yield DataTable(id="queue-table")

        def on_mount(self) -> None:
            self.query_one("#queue-table", DataTable).cursor_type = "row"
            self.set_wide(self.size.width >= 100)
            self.reload(reset=True)
            self.set_interval(3, self.poll_revision)
            self.set_interval(1 / 3, self.animate_marquee)

        def focus_default(self) -> None:
            """Move focus inside the pane so its own bindings receive keys."""
            self.query_one("#queue-table", DataTable).focus()

        def on_resize(self, event: Any) -> None:
            self.set_wide(self.size.width >= 100)

        def set_wide(self, wide: bool) -> None:
            table = self.query_one("#queue-table", DataTable)
            if wide == self.wide and table.columns:
                return
            self.wide = wide
            table.clear(columns=True)
            table.add_column("Rank", key="rank", width=6)
            table.add_column("Basename", key="basename", width=32)
            table.add_column("Item ID", key="item_id", width=12)
            table.add_column("Bucket", key="bucket", width=18)
            table.add_column("Priority", key="priority", width=9)
            if wide:
                table.add_column("Phase", key="phase", width=13)
                table.add_column("Received / total", key="bytes", width=21)
            table.add_column("Retry deadline", key="retry", width=34)
            self.render_rows()

        def active_filters_label(self) -> str:
            parts = []
            if self.bucket != "all":
                parts.append(f"state={self.bucket}")
            if self.search_text:
                parts.append(f"search={self.search_text!r}")
            return ", ".join(parts) if parts else "none"

        def update_header(self) -> None:
            run = self.app.current
            self.query_one("#queue-header", Static).update(detail_visual(
                queue_header_text(run.get("run_id"), run["run"].get("selected_count"),
                                  len(self.rows), self.active_filters_label(),
                                  self.read_at, self.revision)))

        def set_banner(self, text: str) -> None:
            style = "bold" if text == "Results changed" else None
            self.query_one("#queue-banner", Static).update(Text(literal_text(text), style=style))

        def reload(self, reset: bool) -> None:
            if reset:
                self.cursor_stack = [None]
                self.page_index = 0
                self.next_cursor = None
            if not self.app.current["run"].get("selected_count", 0):
                self.rows = []
                self.set_banner("No selected items")
                self.update_header()
                self.render_rows()
                return
            self.request_page(self.cursor_stack[self.page_index])

        def request_page(self, cursor: str | None) -> None:
            if self.request_active:
                return
            self.request_active = True
            run_id = self.app.current["run_id"]
            parameters: dict[str, Any] = {"bucket": self.bucket, "query": self.search_text,
                                          "page_size": 100}
            if cursor:
                parameters["cursor"] = cursor
            self.app.submit_inspection(
                lambda: inspection_request(state, run_id, "list_queue", parameters),
                lambda response, error: self.apply_page(cursor, response, error))

        def apply_page(self, cursor: str | None, response: dict[str, Any] | None,
                       error: str | None) -> None:
            self.request_active = False
            if error or not response:
                self.set_banner("Queue unavailable: " + literal_text(error or "request failed"))
                self.update_header()
                self.refresh_bindings()
                return
            data = response.get("data", {})
            rows = data.get("rows")
            if not isinstance(rows, list):
                self.set_banner("Queue unavailable: invalid response")
                self.update_header()
                self.refresh_bindings()
                return
            if not rows:
                if cursor is None:
                    self.rows = []
                    self.read_at = response.get("read_at")
                    self.revision = response.get("state_revision")
                    self.set_banner("No matching items")
                else:
                    self.next_cursor = None
                    self.set_banner("")
                self.update_header()
                self.render_rows()
                self.refresh_bindings()
                return
            self.rows = rows
            self.next_cursor = (data.get("next_cursor")
                                if isinstance(data.get("next_cursor"), str) else None)
            self.read_at = response.get("read_at")
            self.revision = response.get("state_revision")
            if self.page_index + 1 >= len(self.cursor_stack):
                self.cursor_stack.append(self.next_cursor)
            else:
                self.cursor_stack[self.page_index + 1] = self.next_cursor
            if len(self.cursor_stack) > 100:
                excess = len(self.cursor_stack) - 100
                del self.cursor_stack[:excess]
                self.page_index -= excess
            self.set_banner("")
            self.update_header()
            self.render_rows()
            self.refresh_bindings()

        def render_rows(self) -> None:
            table = self.query_one("#queue-table", DataTable)
            table.clear()
            health = self.app.current.get("health", {})
            cooldown = health.get("cooldown_remaining_s")
            cooldown_active = (isinstance(cooldown, (int, float))
                              and not isinstance(cooldown, bool) and cooldown > 0)
            for row in self.rows:
                cells = queue_row_cells(row, cooldown_active)
                if not self.wide:
                    cells = (cells[0], cells[1], cells[2], cells[3], cells[4], cells[7])
                cells = cells[:-1] + (retry_status_visual(cells[-1]),)
                table.add_row(*cells, key=row["item_id"])
            if self.rows and any(row["item_id"] == self.selected_item_id for row in self.rows):
                index = next(i for i, row in enumerate(self.rows)
                            if row["item_id"] == self.selected_item_id)
                table.move_cursor(row=index)
            elif self.rows:
                if self.selected_item_id is not None:
                    self.app.notify("Selection changed; the previous item left the page.",
                                    severity="warning")
                self.selected_item_id = self.rows[0]["item_id"]
                table.move_cursor(row=0)
            else:
                self.selected_item_id = None

        def on_select_changed(self, event: Select.Changed) -> None:
            if event.select.id != "queue-bucket":
                return
            self.bucket = event.value
            self.reload(reset=True)

        def on_input_submitted(self, event: Input.Submitted) -> None:
            if event.input.id != "queue-search":
                return
            self.search_text = event.value
            self.reload(reset=True)

        def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
            event.stop()
            item_id = event.row_key.value
            if any(row["item_id"] == item_id for row in self.rows):
                self.selected_item_id = item_id
                self.app.push_screen(ItemDetails(self.app.current["run_id"], item_id,
                                                 self.app.current.get("state_revision")))

        def on_data_table_row_highlighted(self, event: DataTable.RowHighlighted) -> None:
            event.stop()
            if event.row_key and event.row_key.value:
                previous_item_id = self.selected_item_id
                self.selected_item_id = event.row_key.value
                if previous_item_id and previous_item_id != self.selected_item_id:
                    self.reset_marquee_cell(previous_item_id)

        def reset_marquee_cell(self, item_id: str) -> None:
            """Stop scrolling a basename cell that lost row selection."""
            row = next((row for row in self.rows if row["item_id"] == item_id), None)
            if row is None:
                return
            self.query_one("#queue-table", DataTable).update_cell(
                item_id, "basename", truncate_filename(row.get("basename"), 32),
                update_width=False)

        def animate_marquee(self) -> None:
            """Scroll the selected row's basename in place while it stays long."""
            if not self.selected_item_id:
                self.marquee_item_id = None
                return
            if self.selected_item_id != self.marquee_item_id:
                self.marquee_item_id = self.selected_item_id
                self.marquee_started_at = time.monotonic()
            row = next((row for row in self.rows if row["item_id"] == self.selected_item_id),
                      None)
            if row is None or len(literal_text(row.get("basename"))) <= 32:
                return
            offset = int((time.monotonic() - self.marquee_started_at) * 3)
            self.query_one("#queue-table", DataTable).update_cell(
                self.selected_item_id, "basename",
                marquee_filename(row.get("basename"), offset, width=32),
                update_width=False)

        def action_focus_search(self) -> None:
            self.query_one("#queue-search", Input).focus()

        def action_next_page(self) -> None:
            if not self.next_cursor:
                return
            self.page_index += 1
            if self.page_index >= len(self.cursor_stack):
                self.cursor_stack.append(self.next_cursor)
            self.request_page(self.cursor_stack[self.page_index])

        def action_previous_page(self) -> None:
            if self.page_index == 0:
                return
            self.page_index -= 1
            self.request_page(self.cursor_stack[self.page_index])

        def action_first_page(self) -> None:
            self.page_index = 0
            self.cursor_stack = [None]
            self.next_cursor = None
            self.request_page(None)

        def action_clear_filters(self) -> None:
            self.bucket, self.search_text = "all", ""
            self.query_one("#queue-bucket", Select).value = "all"
            self.query_one("#queue-search", Input).value = ""
            self.reload(reset=True)

        def action_refresh_results(self) -> None:
            self.reload(reset=True)

        def action_logs(self) -> None:
            self.app.notify("Item logs are unavailable", severity="warning")

        def retry_selected_eligible(self) -> bool:
            """Return whether the focused row can be offered for row-scoped retry.

            Eligibility is advisory only; the controller validates the item's
            actual retryable state at execution time.
            """
            return bool(control and state and self.selected_item_id)

        def exclude_selected_eligible(self) -> bool:
            """Return whether the focused row can be offered for row-scoped exclusion.

            Eligibility is advisory only; the controller validates the item's
            actual excludable state at execution time.
            """
            return bool(control and state and self.selected_item_id)

        def review_required_selected_eligible(self) -> bool:
            """Return whether the focused row is offered for a review-item action.

            Covers `retry_access_denied` and `resume_new_generation`. Eligibility
            is advisory only; the controller validates the item's actual review
            code at execution time.
            """
            if not (control and state and self.selected_item_id):
                return False
            row = next((row for row in self.rows if row["item_id"] == self.selected_item_id), None)
            return bool(row and row.get("bucket") == "review_required")

        def action_prepare_resume_new_generation_selected(self) -> None:
            if not self.review_required_selected_eligible():
                return
            self.app.prepare_row_scoped_resume_new_generation(self.selected_item_id)

        def action_prepare_retry_access_denied_selected(self) -> None:
            if not self.review_required_selected_eligible():
                return
            self.app.prepare_row_scoped_retry_access_denied(self.selected_item_id)

        def action_prepare_retry_selected(self) -> None:
            if not self.retry_selected_eligible():
                return
            self.app.prepare_row_scoped_retry(self.selected_item_id)

        def action_prepare_exclude_selected(self) -> None:
            if not self.exclude_selected_eligible():
                return
            self.app.prepare_row_scoped_exclude(self.selected_item_id)

        def priority_selected_eligible(self, delta: int) -> bool:
            """Return whether the focused row's priority can move by `delta`.

            Eligibility is advisory only; the controller validates the item's
            actual prioritizable state and bounds at execution time.
            """
            if not (control and state and self.selected_item_id):
                return False
            row = next((row for row in self.rows if row["item_id"] == self.selected_item_id), None)
            current = row.get("priority", 0) if row else 0
            return PRIORITY_MIN <= current + delta <= PRIORITY_MAX

        def selected_priority(self) -> int:
            row = next((row for row in self.rows if row["item_id"] == self.selected_item_id), None)
            return row.get("priority", 0) if row else 0

        def action_prepare_raise_priority_selected(self) -> None:
            if not self.priority_selected_eligible(1):
                return
            self.app.prepare_row_scoped_priority(self.selected_item_id, self.selected_priority() + 1)

        def action_prepare_lower_priority_selected(self) -> None:
            if not self.priority_selected_eligible(-1):
                return
            self.app.prepare_row_scoped_priority(self.selected_item_id, self.selected_priority() - 1)

        def selected_cooldown_s(self) -> int:
            row = next((row for row in self.rows if row["item_id"] == self.selected_item_id), None)
            retry_at = row.get("retry_at") if row else None
            if not isinstance(retry_at, (int, float)):
                return 0
            return max(0, int(retry_at - time.time()))

        def cooldown_selected_eligible(self, delta: int) -> bool:
            """Return whether the focused row's retry cooldown can move by `delta` seconds.

            Eligibility is advisory only; the controller validates the item's
            actual cooldown-eligible state and bounds at execution time.
            """
            if not (control and state and self.selected_item_id):
                return False
            row = next((row for row in self.rows if row["item_id"] == self.selected_item_id), None)
            if not row or row.get("bucket") != "retry":
                return False
            current = self.selected_cooldown_s()
            return COOLDOWN_OVERRIDE_MIN_S <= current + delta <= COOLDOWN_OVERRIDE_MAX_S

        def action_prepare_raise_cooldown_selected(self) -> None:
            if not self.cooldown_selected_eligible(COOLDOWN_STEP_S):
                return
            self.app.prepare_row_scoped_cooldown(
                self.selected_item_id, self.selected_cooldown_s() + COOLDOWN_STEP_S)

        def action_prepare_lower_cooldown_selected(self) -> None:
            if not self.cooldown_selected_eligible(-COOLDOWN_STEP_S):
                return
            self.app.prepare_row_scoped_cooldown(
                self.selected_item_id, self.selected_cooldown_s() - COOLDOWN_STEP_S)

        def export_eligible(self) -> bool:
            return bool(state) and not self.export_active

        def action_prepare_export(self) -> None:
            if not self.export_eligible():
                return
            self.app.push_screen(ExportDestination(), self.handle_export_destination)

        def handle_export_destination(self, path: str | None) -> None:
            if not path:
                return
            self.export_active = True
            self.export_path = path
            self.export_bucket = self.bucket
            self.export_query = self.search_text
            self.export_collected: list[dict[str, Any]] = []
            self.refresh_bindings()
            self.export_scan(None)

        def export_scan(self, cursor: str | None) -> None:
            run_id = self.app.current["run_id"]
            parameters: dict[str, Any] = {"bucket": self.export_bucket,
                                          "query": self.export_query, "page_size": 200}
            if cursor:
                parameters["cursor"] = cursor
            self.app.submit_inspection(
                lambda: inspection_request(state, run_id, "list_queue", parameters),
                self.apply_export_page)

        def apply_export_page(self, response: dict[str, Any] | None, error: str | None) -> None:
            if error or not response:
                self.export_active = False
                self.refresh_bindings()
                self.app.notify("Export failed: " + literal_text(error or "request failed"),
                                severity="error")
                return
            data = response.get("data", {})
            rows = data.get("rows")
            if not isinstance(rows, list):
                self.export_active = False
                self.refresh_bindings()
                self.app.notify("Export failed: invalid response", severity="error")
                return
            self.export_collected.extend(queue_export_row(row) for row in rows)
            next_cursor = data.get("next_cursor") if isinstance(data.get("next_cursor"), str) else None
            if next_cursor:
                self.export_scan(next_cursor)
                return
            self.export_active = False
            self.refresh_bindings()
            try:
                with open(self.export_path, "w", newline="", encoding="utf-8") as handle:
                    writer = csv.DictWriter(handle, fieldnames=QUEUE_EXPORT_FIELDS)
                    writer.writeheader()
                    writer.writerows(self.export_collected)
            except OSError as exc:
                self.app.notify(f"Export failed: {exc}", severity="error")
                return
            self.app.notify(
                f"Exported {len(self.export_collected)} row(s) to " + literal_text(self.export_path),
                severity="information")

        def check_action(self, action: str, parameters: tuple[object, ...]) -> bool | None:
            if action == "next_page" and not self.next_cursor:
                return None
            if action == "previous_page" and self.page_index == 0:
                return None
            if action == "clear_filters" and self.bucket == "all" and not self.search_text:
                return None
            if (action == "first_page" and self.page_index == 0
                    and self.cursor_stack[0] is None):
                return None
            if action == "prepare_retry_selected" and not self.retry_selected_eligible():
                return None
            if action == "prepare_exclude_selected" and not self.exclude_selected_eligible():
                return None
            if (action == "prepare_retry_access_denied_selected"
                    and not self.review_required_selected_eligible()):
                return None
            if (action == "prepare_resume_new_generation_selected"
                    and not self.review_required_selected_eligible()):
                return None
            if action == "prepare_raise_priority_selected" and not self.priority_selected_eligible(1):
                return None
            if action == "prepare_lower_priority_selected" and not self.priority_selected_eligible(-1):
                return None
            if (action == "prepare_raise_cooldown_selected"
                    and not self.cooldown_selected_eligible(COOLDOWN_STEP_S)):
                return None
            if (action == "prepare_lower_cooldown_selected"
                    and not self.cooldown_selected_eligible(-COOLDOWN_STEP_S)):
                return None
            if action == "prepare_export" and not self.export_eligible():
                return None
            return True

        def poll_revision(self) -> None:
            if self.request_active or not self.rows:
                return
            run_id = self.app.current["run_id"]
            parameters: dict[str, Any] = {"bucket": self.bucket, "query": self.search_text,
                                          "page_size": 100}
            cursor = self.cursor_stack[self.page_index]
            if cursor:
                parameters["cursor"] = cursor

            def completed(response: dict[str, Any] | None, error: str | None) -> None:
                if error or not response or self.revision is None:
                    return
                revision = response.get("state_revision")
                if revision is not None and revision != self.revision:
                    self.set_banner("Results changed")

            self.app.submit_inspection(
                lambda: inspection_request(state, run_id, "list_queue", parameters), completed)

    class Monitor(App):
        BINDINGS = [("q", "quit", "Close the monitor"),
                    Binding("ctrl+c", "quit", "Close the monitor", show=False),
                    ("r", "prepare_retry_now", "Retry now"),
                    ("t", "prepare_renew_tor_circuits", "Renew Tor"),
                    ("p", "prepare_pause_admission", "Pause admission"),
                    ("u", "prepare_resume_admission", "Resume admission"),
                    ("d", "prepare_drain_and_stop", "Drain and stop"),
                    ("k", "prepare_checkpoint_stop", "Checkpoint and stop"),
                    ("l", "logs", "Logs"),
                    ("question_mark", "help", "Help"),
                    Binding("tab", "focus_region(1)", priority=True, show=False),
                    Binding("shift+tab", "focus_region(-1)", priority=True, show=False)]

        def __init__(self) -> None:
            super().__init__()
            # Set before compose() so a child's own on_mount (e.g. QueuePane) can
            # already read self.app.current and submit_inspection; App.on_mount
            # fires after children mount.
            self.current = snapshot
            self.inspection_request_active = False
            self.control_state: dict[str, Any] | None = None

        DEFAULT_CSS = """
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
            rendered.append("  Session ", style="dim")
            rendered.append(format_elapsed_duration(self.session_elapsed(current)))
            rendered.append("  ")
            self.append_progress_status(rendered, progress_status(current))
            trend_label = self.trend.label() if connection == "live" else "No data"
            remainder = screen_summary(current, trend_label,
                                       self.session_elapsed(current)).split("\n", 1)
            if len(remainder) == 2:
                for line in remainder[1].splitlines():
                    if line.startswith("Disk  "):
                        continue
                    rendered.append("\n")
                    label, separator, value = line.partition("  ")
                    if label in {"Files", "Data", "Speed"} and separator:
                        rendered.append(label, style="bold")
                        rendered.append(separator)
                        if label == "Files":
                            self.append_summary_labels(
                                rendered, value,
                                ("complete", "busy", "retry", "review", "queued"),
                            )
                        elif label == "Data":
                            self.append_summary_labels(rendered, value,
                                                       ("retained", "remaining"))
                        else:
                            rendered.append(value)
                    elif line == retry_summary(current):
                        rendered.append(line, style="dim")
                    else:
                        rendered.append(line)
            return rendered

        @staticmethod
        def append_progress_status(rendered: Text, status: str) -> None:
            for label in ("Last complete ", "Last payload progress "):
                if not status.startswith(label):
                    continue
                rendered.append(label, style="dim")
                value = status[len(label):]
                if value.endswith(" ago"):
                    rendered.append(value[:-4])
                    rendered.append(" ago", style="dim")
                else:
                    rendered.append(value)
                return
            rendered.append(status)

        @staticmethod
        def append_summary_labels(rendered: Text, value: str,
                                  labels: tuple[str, ...]) -> None:
            for index, segment in enumerate(value.split(" | ")):
                if index:
                    rendered.append(" | ")
                matched = next((label for label in labels
                                if segment.endswith(" " + label)), None)
                if matched is None:
                    rendered.append(segment)
                    continue
                rendered.append(segment[:-(len(matched) + 1)])
                rendered.append(" " + matched, style="dim")

        @staticmethod
        def disk_text(value: str) -> Text:
            rendered = Text()
            lines = value.splitlines()
            for index, line in enumerate(lines):
                if line.startswith("Disk  "):
                    rendered.append("Disk", style="bold")
                    for segment_index, segment in enumerate(line[4:].split(" | ")):
                        if segment_index:
                            rendered.append(" | ")
                        label = ("free", "reserve", "headroom")[segment_index]
                        prefix, separator, suffix = segment.partition(" " + label)
                        rendered.append(prefix)
                        if separator:
                            rendered.append(separator, style="dim")
                            rendered.append(suffix)
                else:
                    rendered.append(line)
                if index < len(lines) - 1:
                    rendered.append("\n")
            return rendered

        @staticmethod
        def activity_text(events: list[dict[str, Any]], width: int | None = None) -> Text:
            rendered = Text()
            collapsed = collapse_repeated_events(events)
            for index, (event, count) in enumerate(collapsed):
                for text, style in activity_segments(event, count, width):
                    rendered.append(text, style=style)
                if index < len(collapsed) - 1:
                    rendered.append("\n")
            return rendered

        def activity_width(self) -> int | None:
            """Return the columns the activity log can fill, or None before layout."""
            width = self.query_one("#activity-pane", VerticalScroll).scrollable_content_region.width
            return width or None

        def compose(self) -> ComposeResult:
            with TabbedContent(initial="activity-tab"):
                with TabPane("Activity", id="activity-tab"):
                    yield Static("", id="summary")
                    yield DataTable(id="workers")
                    yield Static("", id="disk")
                    with VerticalScroll(id="activity-pane", classes="event-log"):
                        yield Static(self.activity_text(snapshot["recent_events"]), id="activity")
                with TabPane("Queue", id="queue-tab"):
                    yield QueuePane()
            yield KeyFooter("dashboard")

        def on_mount(self) -> None:
            table = self.query_one("#workers", DataTable)
            table.add_column("#", key="worker", width=3)
            table.add_column("Item", key="item", width=40)
            table.add_column("Phase", key="phase", width=13)
            table.add_column("Received / total", key="progress", width=22)
            table.add_column("Speed", key="speed", width=11)
            table.add_column("ETA", key="eta", width=9)
            self.query_one("#activity-pane", VerticalScroll).border_title = "Event log"
            self.trend = SpeedTrend()
            self.snapshot_sequence = None
            self.snapshot_elapsed_s = 0.0
            self.snapshot_observed_monotonic = time.monotonic()
            self.observe_snapshot(snapshot)
            self.last_summary_signature: str | None = None
            self.last_disk_signature: str | None = None
            self.last_event_signature: str | None = None
            self.rendered_rows: dict[str, tuple[str, ...]] = {}
            self.marquee_focus: tuple[Any, Any] | None = None
            self.marquee_started_at = 0.0
            self.last_control_poll = 0.0
            self.control_error: str | None = None
            self.action_confirmation: dict[str, Any] | None = None
            self.pending_item_ids: list[str] | None = None
            self.control_request_active = False
            self.inspection_request_active = False
            self.populate(snapshot, force=True)
            self.set_interval(1 / fps, self.render_frame)
            if snapshot_path:
                self.set_interval(1 / 2, self.refresh_snapshot)

        def open_worker_details(self, row_key: Any) -> None:
            """Open the selected stable worker slot from either table event."""
            worker = next((row for row in self.current["workers"]
                           if str(row.get("worker_id")) == str(row_key)), None)
            if not worker:
                return
            self.push_screen(WorkerDetails(self.current["run_id"], self.current["session_id"],
                                           int(worker["worker_id"]), self.current["state_revision"]))

        def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
            self.open_worker_details(event.row_key.value)

        def on_data_table_cell_selected(self, event: DataTable.CellSelected) -> None:
            self.open_worker_details(event.cell_key.row_key.value)

        def on_tabbed_content_tab_activated(self, event: TabbedContent.TabActivated) -> None:
            queue_active = event.pane.id == "queue-tab"
            self.query_one(KeyFooter).set_screen_name("queue" if queue_active else "dashboard")
            if queue_active:
                self.query_one(QueuePane).focus_default()

        def focus_regions(self) -> list[Any]:
            """Return the widgets Tab visits on the dashboard, in order."""
            if self.query_one(TabbedContent).active == "queue-tab":
                return [self.query_one("#queue-bucket"), self.query_one("#queue-search"),
                        self.query_one("#queue-table")]
            return [self.query_one("#workers"), self.query_one("#activity-pane")]

        def action_focus_region(self, step: int) -> None:
            regions = self.focus_regions()
            focused = self.focused
            index = next((position for position, region in enumerate(regions)
                          if focused is not None and region in focused.ancestors_with_self), None)
            if index is None:
                regions[0 if step > 0 else -1].focus()
            else:
                regions[(index + step) % len(regions)].focus()

        def action_logs(self) -> None:
            self.notify("Item logs are unavailable", severity="warning")

        def action_help(self) -> None:
            screen = type(self.screen).__name__
            navigation = {
                "ItemDetails": ("Item details help", ItemDetails.BINDINGS, [
                    ("↑ ↓", "Select an attempt, or scroll", "read-only")]),
                "WorkerDetails": ("Worker details help", WorkerDetails.BINDINGS, [
                    ("↑ ↓", "Scroll", "read-only")]),
            }.get(screen)
            if navigation:
                title, tables, extra = navigation[0], [navigation[1], self.BINDINGS], navigation[2]
                extra = extra + [("Tab Shift+Tab", "Move focus between sections", "read-only")]
            elif isinstance(self.screen, ModalScreen):
                return
            elif self.query_one(TabbedContent).active == "queue-tab":
                title, tables = "Queue help", [QueuePane.BINDINGS, self.BINDINGS]
                extra = [("↑ ↓", "Move selection", "read-only"),
                         ("Enter", "Open details for the focused row", "read-only"),
                         ("Tab Shift+Tab", "Move focus between filters, search, and rows",
                          "read-only")]
            else:
                title, tables = "Dashboard help", [self.BINDINGS]
                extra = [("↑ ↓", "Move selection", "read-only"),
                         ("Enter", "Open details for the focused row", "read-only"),
                         ("Tab Shift+Tab", "Move focus between worker table and event log",
                          "read-only")]
            self.push_screen(HelpScreen(title, extra + help_rows(tables)))

        def submit_inspection(self, operation, completed) -> None:
            """Run one inspection request away from the render and input loop."""
            if self.inspection_request_active or state is None:
                completed(None, "inspection endpoint unavailable")
                return
            self.inspection_request_active = True
            def worker() -> None:
                try:
                    result, error = operation(), None
                except InspectionError as exc:
                    result, error = None, str(exc)
                except Exception:
                    logging.getLogger(__name__).exception("inspection request raised an unexpected error")
                    result, error = None, "inspection request raised an unexpected error"
                def finish() -> None:
                    self.inspection_request_active = False
                    try:
                        completed(result, error)
                    except Exception:
                        logging.getLogger(__name__).exception("inspection callback raised an unexpected error")
                self.call_from_thread(finish)
            threading.Thread(target=worker, name="monitor-inspection", daemon=True).start()

        def focused_worker_offset(self, table: DataTable, workers: list[dict[str, Any]]
                                  ) -> tuple[int | None, int | None]:
            """Return the focused worker ID and its scroll step, restarting on a new focus.

            Only the row under the table cursor scrolls. Scrolling starts from the name's
            beginning each time the cursor or the item in that row changes.
            """
            if not table.row_count:
                return None, None
            focused = next((worker for worker in workers
                            if str(worker.get("worker_id")) == str(
                                table.coordinate_to_cell_key(table.cursor_coordinate).row_key.value)),
                           None)
            if focused is None or not focused.get("item_id"):
                self.marquee_focus = None
                return None, None
            focus = (focused["worker_id"], focused["item_id"])
            if focus != self.marquee_focus:
                self.marquee_focus = focus
                self.marquee_started_at = time.monotonic()
            offset = int((time.monotonic() - self.marquee_started_at) * MARQUEE_STEPS_PER_SECOND)
            return focused["worker_id"], offset

        def populate(self, current: dict[str, Any], force: bool = False) -> None:
            if not self.query("#summary"):
                # A queued render tick can still fire while the app is
                # mid-shutdown (screen already torn down); skip this frame
                # instead of raising through the render loop.
                return
            summary_signature = self.summary_text(current).plain
            if force or summary_signature != self.last_summary_signature:
                self.query_one("#summary", Static).update(self.summary_text(current))
                self.last_summary_signature = summary_signature
            disk = disk_status(current)
            if force or disk != self.last_disk_signature:
                self.query_one("#disk", Static).update(self.disk_text(disk))
                self.last_disk_signature = disk
            activity_pane = self.query_one("#activity-pane", VerticalScroll)
            follow_events = activity_pane.scroll_y >= activity_pane.max_scroll_y
            activity_width = self.activity_width()
            event_signature = json.dumps([activity_width, current["recent_events"]],
                                         sort_keys=True, separators=(",", ":"))
            if force or event_signature != self.last_event_signature:
                self.query_one("#activity", Static).update(
                    self.activity_text(current["recent_events"], activity_width))
                self.last_event_signature = event_signature
                if follow_events:
                    activity_pane.scroll_end(animate=False)
            table = self.query_one("#workers", DataTable)
            if force:
                table.clear(columns=False)
            workers = sorted(current["workers"], key=lambda item: item.get("worker_id", 0))
            focused_id, offset = self.focused_worker_offset(table, workers)
            basename_counts: dict[str, int] = {}
            for worker in workers:
                if worker.get("item_id"):
                    name = literal_text(worker.get("basename"))
                    basename_counts[name] = basename_counts.get(name, 0) + 1
            for worker in workers:
                item_id = worker.get("item_id")
                speed, eta = worker_rate_cells(worker)
                values = (
                    str(worker.get("worker_id", "?")),
                    worker_item_cell(
                        worker.get("basename"), item_id, WORKER_ITEM_WIDTH,
                        basename_counts.get(literal_text(worker.get("basename")), 0) > 1,
                        offset if worker.get("worker_id") == focused_id else None)
                    if item_id else "idle",
                    worker_phase_label(worker, current),
                    worker_progress_text(worker),
                    speed,
                    eta,
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

        def control_action_eligible(self, action: str) -> bool:
            return bool(control and state and self.control_state
                        and self.control_state.get("actions", {}).get(action) == "available")

        def check_action(self, action: str, parameters: tuple[object, ...]) -> bool | None:
            if action == "focus_region" and self.screen is not self.screen_stack[0]:
                return False
            if action == "prepare_retry_now" and not self.retry_now_eligible():
                return None
            if action == "prepare_renew_tor_circuits" and not self.tor_renewal_eligible():
                return None
            if action == "prepare_pause_admission" and not self.control_action_eligible(
                    "pause_admission"):
                return None
            if action == "prepare_resume_admission" and not self.control_action_eligible(
                    "resume_admission"):
                return None
            if action == "prepare_drain_and_stop" and not self.control_action_eligible(
                    "drain_and_stop"):
                return None
            if action == "prepare_checkpoint_stop" and not self.control_action_eligible(
                    "checkpoint_stop"):
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
            previous = self.control_state
            self.control_state = result if isinstance(result, dict) else None
            self.control_error = error
            if self.control_state != previous:
                self.refresh_bindings()

        def action_prepare_retry_now(self) -> None:
            if not self.retry_now_eligible():
                return
            self.pending_item_ids = None
            self.prepare_action("retry_now")

        def action_prepare_renew_tor_circuits(self) -> None:
            if not self.tor_renewal_eligible():
                return
            self.pending_item_ids = None
            self.prepare_action("renew_tor_circuits")

        def action_prepare_pause_admission(self) -> None:
            if not self.control_action_eligible("pause_admission"):
                return
            self.pending_item_ids = None
            self.prepare_action("pause_admission")

        def action_prepare_resume_admission(self) -> None:
            if not self.control_action_eligible("resume_admission"):
                return
            self.pending_item_ids = None
            self.prepare_action("resume_admission")

        def action_prepare_drain_and_stop(self) -> None:
            if not self.control_action_eligible("drain_and_stop"):
                return
            self.pending_item_ids = None
            self.prepare_action("drain_and_stop")

        def action_prepare_checkpoint_stop(self) -> None:
            if not self.control_action_eligible("checkpoint_stop"):
                return
            self.pending_item_ids = None
            self.prepare_action("checkpoint_stop")

        def prepare_row_scoped_retry(self, item_id: str) -> None:
            if not (control and state):
                return
            self.pending_item_ids = [item_id]
            self.prepare_action("retry_now", {"item_ids": [item_id]})

        def prepare_row_scoped_exclude(self, item_id: str) -> None:
            if not (control and state):
                return
            self.pending_item_ids = [item_id]
            self.prepare_action("exclude_item", {"item_ids": [item_id]})

        def prepare_row_scoped_retry_access_denied(self, item_id: str) -> None:
            if not (control and state):
                return
            self.pending_item_ids = [item_id]
            self.prepare_action("retry_access_denied", {"item_ids": [item_id]})

        def prepare_row_scoped_resume_new_generation(self, item_id: str) -> None:
            if not (control and state):
                return
            self.pending_item_ids = [item_id]
            self.prepare_action("resume_new_generation", {"item_ids": [item_id]})

        def prepare_row_scoped_priority(self, item_id: str, priority: int) -> None:
            if not (control and state):
                return
            self.pending_item_ids = [item_id]
            self.prepare_action("set_item_priority", {"item_ids": [item_id], "priority": priority})

        def prepare_row_scoped_cooldown(self, item_id: str, cooldown_s: int) -> None:
            if not (control and state):
                return
            self.pending_item_ids = [item_id]
            self.prepare_action("set_retry_cooldown",
                                {"item_ids": [item_id], "cooldown_s": cooldown_s})

        def prepare_action(self, action: str, parameters: dict[str, Any] | None = None) -> None:
            self.submit_control_request(
                lambda: control_request(state, self.current["run_id"], "prepare_confirmation",
                                        {"action": action, **(parameters or {})}),
                self.apply_action_preparation,
            )

        def apply_action_preparation(self, payload, error) -> None:
            if error:
                self.control_request_active = False
                self.pending_item_ids = None
                self.notify("Control action unavailable: " + literal_text(error), severity="error")
                return
            confirmation = payload.get("confirmation") if isinstance(payload, dict) else None
            if not isinstance(confirmation, dict):
                self.control_request_active = False
                self.pending_item_ids = None
                self.notify("Control action unavailable: invalid controller response", severity="error")
                return
            self.control_request_active = False
            self.action_confirmation = confirmation
            item_count = len(self.pending_item_ids) if self.pending_item_ids else None
            self.push_screen(ActionConfirmation(str(confirmation.get("action")), item_count),
                             self.action_confirmation_complete)

        def action_confirmation_complete(self, confirmed: bool | None) -> None:
            item_ids = self.pending_item_ids
            self.pending_item_ids = None
            if not confirmed or not self.action_confirmation or not state:
                self.action_confirmation = None
                return
            confirmation = self.action_confirmation
            self.action_confirmation = None
            action = confirmation.get("action")
            if not isinstance(action, str):
                self.notify("Control action unavailable: invalid controller response", severity="error")
                return
            parameters = {"nonce": confirmation.get("nonce"), "confirmation": action}
            if item_ids:
                parameters["item_ids"] = item_ids
            self.submit_control_request(
                lambda: control_request(state, self.current["run_id"], action, parameters),
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

    return Monitor


def run_textual(snapshot: dict[str, Any], snapshot_path: Path | None = None,
                state: Path | None = None, control: bool = False, fps: int = 30) -> int:
    monitor_class = build_monitor_app(snapshot, snapshot_path, state, control, fps)
    if monitor_class is None:
        return 2
    monitor_class().run()
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
