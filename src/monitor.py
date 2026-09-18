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
from typing import Any
from urllib.parse import unquote, urlsplit

from controller import (COOLDOWN_OVERRIDE_MAX_S, COOLDOWN_OVERRIDE_MIN_S, ControlError,
                        PRIORITY_MAX, PRIORITY_MIN, control_request, get_control_state)
from inspection import InspectionError, inspection_request


FINAL_LIFECYCLES = {"finished", "stopped"}
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
    """Color a successful completed transfer without recoloring its level."""
    return "green" if event.get("category") == "complete" else ""


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


def format_retry_deadline(value: Any) -> str:
    """Format a durable retry deadline without exposing its epoch value."""
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return detail_value(value, "not recorded")
    remaining = value - time.time()
    if remaining <= 0:
        return "Eligible; awaiting controller"
    deadline = dt.datetime.fromtimestamp(value, dt.timezone.utc)
    return f"{deadline:%Y-%m-%d %H:%M:%S UTC} ({format_countdown(remaining)} remaining)"


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
    deadline = dt.datetime.fromtimestamp(retry_at, dt.timezone.utc)
    return f"{deadline:%Y-%m-%d %H:%M:%S UTC} ({format_countdown(remaining)} remaining)"


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


def item_details_text(item: dict[str, Any], read_at: Any = None,
                      revision: Any = None, sample_freshness: str = "?") -> str:
    """Build a literal-safe, scrollable item-details display from one record."""
    unavailable = item.get("unavailable", {})
    reason = unavailable.get("reason", "not recorded") if isinstance(unavailable, dict) else "not recorded"
    identity = item.get("identity", {})
    state = item.get("state", {})
    engine = item.get("engine", {})
    byte_values = item.get("bytes", {})
    validation = item.get("validation", {})
    def field(section: dict[str, Any], name: str, fallback: Any = None) -> Any:
        return section.get(name, fallback) if isinstance(section, dict) else fallback
    basename = field(identity, 'basename', item.get('basename'))
    item_id_value = field(identity, 'item_id', item.get('item_id'))
    lines = [
        "Item details",
        f"{truncate_filename(basename, 40) if basename is not None else '? (' + literal_text(reason) + ')'}  "
        f"ID {short_item_id(item_id_value) if item_id_value is not None else '? (' + literal_text(reason) + ')'}  "
        f"State: {detail_value(field(state, 'durable_state', item.get('durable_state')), reason)}  "
        f"Phase: {detail_value(field(state, 'phase'), reason)}  "
        f"Freshness: {literal_text(sample_freshness)}",
        f"Read: {detail_value(read_at, 'read time unavailable')}  Revision: {detail_value(revision, 'revision unavailable')}  Freshness: {literal_text(sample_freshness)}",
        "", "Identity and paths",
        f"Item ID: {detail_value(item_id_value, reason)}",
        f"Run ID: {detail_value(field(identity, 'run_id'), reason)}",
        f"Original path: {detail_value(field(identity, 'logical_path', item.get('logical_path')), reason)}",
        f"Mapped storage path: {detail_value(field(identity, 'storage_path', item.get('storage_path')), reason)}",
        f"Staging path: {detail_value(item.get('staging_path'), reason)}",
        f"Candidate path: {detail_value(item.get('candidate_path'), reason)}",
        f"Queue rank: {detail_value(field(identity, 'queue_rank', item.get('queue_rank')), reason)}",
        f"Generation: {detail_value(field(identity, 'generation'), reason)}  Attempt ID: {detail_value(field(identity, 'attempt_id'), reason)}",
        f"Mapping reason: {detail_value(field(identity, 'mapping_reason'), reason)}  Mapping version: {detail_value(field(identity, 'mapping_version'), reason)}",
        "", "State",
        f"Durable state: {detail_value(field(state, 'durable_state', item.get('durable_state')), reason)}  Bucket: {detail_value(field(state, 'bucket', item.get('bucket')), reason)}",
        f"Phase: {detail_value(field(state, 'phase'), reason)}  Reason: {detail_value(field(state, 'phase_reason'), reason)}",
        f"Worker: {detail_value(field(state, 'worker_id'), reason)}  Last transition: {detail_value(field(state, 'last_transition_at', item.get('updated_at')), reason)}",
        f"Attempts: {detail_value(field(state, 'attempt_count', item.get('attempt_count')), reason)} / {detail_value(field(state, 'attempt_ceiling', item.get('attempt_ceiling')), reason)}",
        f"Retry deadline: {format_retry_deadline(field(state, 'retry_at', item.get('retry_at')))}",
        f"Blocking condition: {detail_value(field(state, 'blocking_condition'), 'none reported')}",
        "", "Engine",
        f"Engine: {detail_value(field(engine, 'name'), reason)} {detail_value(field(engine, 'version'), reason)}",
        f"Instance: {detail_value(field(engine, 'instance_id'), reason)}  Job: {detail_value(field(engine, 'job_id'), reason)}  PID: {detail_value(field(engine, 'pid'), reason)}",
        f"Runtime sample: {detail_value(field(engine, 'sample_at'), reason)}",
        "", "Bytes",
        f"Received: {detail_bytes(field(byte_values, 'received', item.get('received_bytes')), reason)}",
        f"Resume baseline: {detail_bytes(field(byte_values, 'resume_baseline'), reason)}",
        f"Transfer total: {detail_bytes(field(byte_values, 'transfer_total'), reason)}  Source: {detail_value(field(byte_values, 'transfer_total_source'), reason)}",
        f"Trusted expected size: {detail_bytes(field(byte_values, 'trusted_expected'), reason)}",
        f"Inventory size: {detail_value(field(byte_values, 'inventory_size', item.get('inventory_size')), reason)}",
        f"Retained item bytes: {detail_bytes(field(byte_values, 'retained_item_bytes'), reason)}",
        f"Committed item completion bytes: {detail_bytes(field(byte_values, 'committed_completion_bytes'), reason)}",
        "", "Validation",
        f"Method: {detail_value(field(validation, 'method'), reason)}  Processed bytes: {detail_bytes(field(validation, 'processed_bytes'), reason)}",
        f"Result: {detail_value(field(validation, 'result'), reason)}  Recorded at: {detail_value(field(validation, 'recorded_at'), reason)}",
        f"Expected SHA-256: {detail_value(field(validation, 'expected_sha256'), reason)}",
        f"Observed SHA-256: {detail_value(field(validation, 'observed_sha256', item.get('sha256')), reason)}",
        f"Mismatch reason: {detail_value(field(validation, 'mismatch_reason'), reason)}",
        f"Promotion: {detail_value(field(validation, 'promotion_status'), reason)}  Staging cleanup: {detail_value(field(validation, 'staging_cleanup_at'), reason)}",
        "", "Source",
        f"{literal_text(item.get('source_label', 'Source hidden'))}: {detail_value(item.get('source'), 'hidden until you select Reveal source')}",
        "", "Attempts and errors", "Select Next attempts to load another recorded page.",
    ]
    return "\n".join(lines)


def worker_details_text(worker: dict[str, Any], read_at: Any = None,
                        revision: Any = None, dashboard_revision: Any = None,
                        sample_freshness: str = "?", source_label: str = "Source hidden",
                        source: Any = None) -> str:
    """Build a literal-safe worker details display from one slot record."""
    assignment = worker.get("assignment") if isinstance(worker.get("assignment"), dict) else None
    reason = "not recorded"
    lines = ["Worker details",
             f"Read: {detail_value(read_at, 'read time unavailable')}  Revision: {detail_value(revision, 'revision unavailable')}  Freshness: {literal_text(sample_freshness)}"]
    if dashboard_revision is not None and revision != dashboard_revision:
        lines.append(f"Dashboard revision: {detail_value(dashboard_revision)} (details differ)")
    lines.extend(["", "Assignment"])
    if not assignment or not assignment.get("item_id"):
        lines.extend(["No item assigned", f"Reason: {detail_value(worker.get('reason'), 'Reason unavailable')}"])
        return "\n".join(lines)
    lines.extend([
        f"Run: {detail_value(assignment.get('run_id'), reason)}  Session: {detail_value(assignment.get('session_id'), reason)}  Worker: {detail_value(assignment.get('worker_id'), reason)}",
        f"Item ID: {detail_value(assignment.get('item_id'), reason)}  Basename: {detail_value(assignment.get('basename'), reason)}",
        f"Generation: {detail_value(assignment.get('generation'), reason)}  Attempt: {detail_value(assignment.get('attempt_number'), reason)}",
        f"Engine instance: {detail_value(assignment.get('engine_instance_id'), reason)}  Job: {detail_value(assignment.get('engine_job_id'), reason)}  PID: {detail_value(assignment.get('pid'), reason)}",
        "", "Activity",
        f"Phase: {detail_value(worker.get('phase'), reason)}  Reason: {detail_value(worker.get('reason'), reason)}",
        f"Phase elapsed: {format_elapsed_duration(worker.get('phase_elapsed_s'))}  Attempt elapsed: {format_elapsed_duration(worker.get('attempt_elapsed_s'))}",
        f"Last payload progress: {format_countdown(worker.get('last_progress_age_s')) if worker.get('last_progress_age_s') is not None else '?'} ago",
        "No progress for 60s" if worker.get('phase') == 'downloading' and isinstance(worker.get('last_progress_age_s'), (int, float)) and worker['last_progress_age_s'] >= 60 else "",
        "", "Transfer",
        f"Received: {detail_bytes(worker.get('received_bytes'), reason)}  Total: {detail_bytes(worker.get('total_bytes'), reason)} ({detail_value(worker.get('total_source'), reason)})",
        f"Resume baseline: {detail_bytes(worker.get('resume_baseline_bytes'), reason)}",
        f"Speed: {detail_rate(worker.get('speed_bps'), reason)}  Smoothed: {detail_rate(worker.get('smoothed_speed_bps'), reason)}",
        f"ETA (approximate): {detail_value(format_countdown(worker.get('eta_seconds')) if isinstance(worker.get('eta_seconds'), (int, float)) and not isinstance(worker.get('eta_seconds'), bool) else None, 'Estimating' if worker.get('estimator') == 'Estimating' else reason)}  Connections: {detail_value(worker.get('connections'), reason)}",
        f"Sample sequence: {detail_value(worker.get('sample_sequence'), reason)}  Sample age: {detail_value(worker.get('sample_age_s'), reason)}  Quality: {detail_value(worker.get('quality'), reason)}",
        "", "Admission",
        f"Controller conditions: {detail_value(worker.get('admission'), 'not reported')}",
        "", "Validation",
        f"Validation: {detail_value(worker.get('validation'), 'not owned by this slot')}",
        "", "Source",
        f"{literal_text(source_label)}: {detail_value(source, 'hidden until you select Reveal source')}",
    ])
    return "\n".join(lines)


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
        from textual.widgets import (Button, DataTable, Footer, Input, Select, Static,
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
                visual.append(line, style="bold white")
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
        """Dim a retry status message; a retry countdown value stays default style."""
        return Text(text, style="dim" if "remaining)" not in text else None)

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

        CSS = """
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
        BINDINGS = [("escape", "dismiss", "Back"), ("r", "reveal_source", "Reveal source"),
                    ("n", "next_attempts", "Next attempts"), ("l", "logs", "Logs"),
                    Binding("up", "select_previous_attempt", "Prev attempt", show=False,
                           priority=True),
                    Binding("down", "select_next_attempt", "Next attempt", show=False,
                           priority=True),
                    Binding("q", "disabled_control", show=False),
                    Binding("t", "disabled_control", show=False),
                    Binding("p", "disabled_control", show=False),
                    Binding("u", "disabled_control", show=False),
                    Binding("d", "disabled_control", show=False),
                    Binding("k", "disabled_control", show=False)]
        CSS = "#item-details { height: 1fr; overflow-y: auto; }"

        def __init__(self, run_id: str, item_id: str) -> None:
            super().__init__()
            self.run_id = run_id
            self.item_id = item_id
            self.item: dict[str, Any] | None = None
            self.read_at: Any = None
            self.revision: Any = None
            self.cursor: str | None = None
            self.attempts: list[dict[str, Any]] = []
            self.selected_attempt_index = 0
            self.revealed = False
            self.session_id: Any = None
            self.session_reload_pending = False

        def compose(self) -> ComposeResult:
            with VerticalScroll(id="item-details"):
                yield Static("Loading item details…", id="item-details-text")
            yield Footer()

        def on_mount(self) -> None:
            self.session_id = self.app.current.get("session_id")
            self.set_source_binding()
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
            self.set_source_binding()
            self.load_item()

        def set_source_binding(self) -> None:
            action = "hide_source" if self.revealed else "reveal_source"
            label = "Hide source" if self.revealed else "Reveal source"
            self._bindings.key_to_bindings["r"] = [Binding("r", action, label)]
            self.refresh_bindings()

        def update_details(self, error: str | None = None, retain: bool = False) -> None:
            if error and retain and self.item:
                output = ("Last-known values retained\n" + literal_text(error) + "\n\n"
                          + item_details_text(self.item, self.read_at, self.revision,
                                              freshness(self.app.current)))
            elif error:
                output = "Details unavailable\n" + literal_text(error)
            else:
                output = item_details_text(self.item or {}, self.read_at, self.revision,
                                           freshness(self.app.current))
            if self.attempts:
                selection_marker = "→"
                output += "\n\nRecorded attempts\n" + "\n".join(
                    f"{selection_marker if index == self.selected_attempt_index else ' '} "
                    f"#{detail_value(row.get('attempt_number'))} {detail_value(row.get('attempt_id'))} "
                    f"Generation: {detail_value(row.get('generation'))}  "
                    f"Started: {detail_value(row.get('started_at'))}  "
                    f"Ended: {detail_value(row.get('ended_at'))}  "
                    f"Outcome: {detail_value(row.get('outcome'))}  "
                    f"Error: {detail_value(row.get('error_category'))} "
                    f"{detail_value(row.get('error_message'))}  "
                    f"Retry deadline: {format_retry_deadline(row.get('retry_at'))}"
                    for index, row in enumerate(self.attempts))
            self.query_one("#item-details-text", Static).update(detail_visual(output))

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

        def action_reveal_source(self) -> None:
            self.revealed = True
            self.set_source_binding()
            self.load_item()

        def action_hide_source(self) -> None:
            self.revealed = False
            if self.item:
                self.item["source"] = None
                self.item["source_label"] = "Source hidden"
            self.set_source_binding()
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
            return

    class WorkerDetails(Screen[None]):
        """Read-only view bound to one worker slot in one controller session."""
        BINDINGS = [("escape", "dismiss", "Back"), ("i", "item_details", "Item details"),
                    ("l", "logs", "Logs"), ("r", "reveal_source", "Reveal source"),
                    Binding("q", "disabled_control", show=False),
                    Binding("t", "disabled_control", show=False),
                    Binding("p", "disabled_control", show=False),
                    Binding("u", "disabled_control", show=False),
                    Binding("d", "disabled_control", show=False),
                    Binding("k", "disabled_control", show=False)]
        CSS = "#worker-details { height: 1fr; overflow-y: auto; }"

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
            self.revealed = False
            self.source: Any = None
            self.source_label = "Source hidden"

        def compose(self) -> ComposeResult:
            with VerticalScroll(id="worker-details"):
                yield Static("Loading worker details…", id="worker-details-text")
            yield Footer()

        def on_mount(self) -> None:
            self.set_source_binding()
            self.load_worker()
            self.set_interval(2, self.refresh_worker)

        def set_source_binding(self) -> None:
            action = "hide_source" if self.revealed else "reveal_source"
            label = "Hide source" if self.revealed else "Reveal source"
            self._bindings.key_to_bindings["r"] = [Binding("r", action, label)]
            self.refresh_bindings()

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
            if item_id != self.displayed_item_id:
                self.displayed_item_id = item_id
                self.revealed = False
                self.source = None
                self.source_label = "Source hidden"
                self.set_source_binding()
            self.update_details()

        def update_details(self, error: str | None = None) -> None:
            if self.session_ended:
                output = "Worker details\nSession ended\nReturn to the current dashboard."
            elif error:
                output = "Worker details\nLast-known values retained\n" + literal_text(error)
                if self.worker:
                    output += "\n\n" + worker_details_text(
                        self.worker, self.read_at, self.revision,
                        self.dashboard_revision, freshness(self.app.current),
                        self.source_label, self.source)
            else:
                output = worker_details_text(
                    self.worker or {"worker_id": self.worker_id}, self.read_at,
                    self.revision, self.dashboard_revision, freshness(self.app.current),
                    self.source_label, self.source)
            self.query_one("#worker-details-text", Static).update(detail_visual(output))

        def action_reveal_source(self) -> None:
            if not self.displayed_item_id:
                self.app.notify("No item assigned", severity="warning")
                return
            self.revealed = True
            self.set_source_binding()
            target = self.displayed_item_id
            self.app.submit_inspection(
                lambda: inspection_request(state, self.run_id, "get_item",
                                            {"item_id": target, "reveal_source": True}),
                lambda response, error: self.apply_source(target, response, error))

        def apply_source(self, target: str, response: dict[str, Any] | None,
                         error: str | None) -> None:
            if target != self.displayed_item_id or not self.revealed:
                return
            data = response.get("data", {}) if response else {}
            item = data.get("item") if isinstance(data, dict) else None
            if error or not isinstance(item, dict):
                self.source = None
                self.source_label = "Source unavailable"
            else:
                self.source = item.get("source")
                self.source_label = literal_text(item.get("source_label", "Source unavailable"))
            self.update_details()

        def action_hide_source(self) -> None:
            self.revealed = False
            self.source = None
            self.source_label = "Source hidden"
            self.set_source_binding()
            self.update_details()

        def action_item_details(self) -> None:
            if not self.displayed_item_id:
                self.app.notify("No item assigned", severity="warning")
                return
            self.app.push_screen(ItemDetails(self.run_id, self.displayed_item_id))

        def action_logs(self) -> None:
            self.app.notify("No item logs" if not self.displayed_item_id else "Item logs are unavailable", severity="warning")

        def action_dismiss(self) -> None:
            self.revealed = False
            self.source = None
            self.app.pop_screen()

        def action_disabled_control(self) -> None:
            return

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
            Binding("right_square_bracket", "prepare_raise_priority_selected",
                   "Raise priority", show=True),
            Binding("left_square_bracket", "prepare_lower_priority_selected",
                   "Lower priority", show=True),
            Binding("right_curly_bracket", "prepare_raise_cooldown_selected",
                   "Raise cooldown", show=True),
            Binding("left_curly_bracket", "prepare_lower_cooldown_selected",
                   "Lower cooldown", show=True),
            Binding("e", "prepare_export", "Export queue", show=True),
            Binding("l", "logs", "Logs"),
            Binding("question_mark", "help", "Help", show=True),
        ]
        CSS = """
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
                self.app.push_screen(ItemDetails(self.app.current["run_id"], item_id))

        def on_data_table_row_highlighted(self, event: DataTable.RowHighlighted) -> None:
            event.stop()
            if event.row_key and event.row_key.value:
                self.selected_item_id = event.row_key.value

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

        def action_help(self) -> None:
            self.app.notify(
                "/ search   Enter apply   Escape cancel   PageUp/PageDown page   "
                "Enter opens item details   R retry row   x exclude row   "
                "] raise priority   [ lower priority   } raise cooldown   "
                "{ lower cooldown   e export queue   l logs   "
                "c clear filters   f refresh results   g first page",
                title="Queue help")

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
        BINDINGS = [("q", "quit", "Close"), ("r", "prepare_retry_now", "Retry now"),
                    ("t", "prepare_renew_tor_circuits", "Renew Tor"),
                    ("p", "prepare_pause_admission", "Pause admission"),
                    ("u", "prepare_resume_admission", "Resume admission"),
                    ("d", "prepare_drain_and_stop", "Drain and stop"),
                    ("k", "prepare_checkpoint_stop", "Checkpoint and stop")]

        def __init__(self) -> None:
            super().__init__()
            # Set before compose() so a child's own on_mount (e.g. QueuePane) can
            # already read self.app.current and submit_inspection; App.on_mount
            # fires after children mount.
            self.current = snapshot
            self.inspection_request_active = False

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
            with TabbedContent(initial="activity-tab"):
                with TabPane("Activity", id="activity-tab"):
                    yield Static("", id="summary")
                    yield DataTable(id="workers")
                    yield Static("", id="disk")
                    with VerticalScroll(id="activity-pane", classes="event-log"):
                        yield Static(self.activity_text(snapshot["recent_events"]), id="activity")
                with TabPane("Queue", id="queue-tab"):
                    yield QueuePane()
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
            if event.pane.id == "queue-tab":
                self.query_one(QueuePane).focus_default()

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

        def control_action_eligible(self, action: str) -> bool:
            return bool(control and state and self.control_state
                        and self.control_state.get("actions", {}).get(action) == "available")

        def check_action(self, action: str, parameters: tuple[object, ...]) -> bool | None:
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
            self.control_state = result if isinstance(result, dict) else None
            self.control_error = error

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
