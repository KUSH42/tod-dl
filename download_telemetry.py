"""Bounded, disposable telemetry for the acquisition controller.

This module deliberately has no dependency on aria2 or the UI.  An engine
adapter may supply exact transfer counters later; until then, unavailable is
more truthful than deriving progress from a staging file's length.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import threading
import time
import uuid
from collections import deque
from pathlib import Path
from typing import Any


SCHEMA_VERSION = 1
MAX_EVENTS = 100
MAX_MESSAGE = 512
PUBLISH_INTERVAL_SECONDS = 0.5


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def metric(value: Any, quality: str) -> dict[str, Any]:
    return {"value": value, "quality": quality}


def estimate_eta_seconds(received_bytes: int | None, total_bytes: int | None,
                         speed_bps: int | None) -> float | None:
    """Estimate remaining transfer time only from one complete live sample."""
    if (received_bytes is None or total_bytes is None or speed_bps is None
            or received_bytes < 0 or total_bytes < received_bytes or speed_bps <= 0):
        return None
    return (total_bytes - received_bytes) / speed_bps


def reduce_metrics(items: list[dict[str, Any]], active: list[dict[str, Any]],
                   started_monotonic: float, sampled_monotonic: float) -> dict[str, Any]:
    """Reduce durable rows and exact engine samples without double counting.

    ``items`` contains only the immutable selected run.  Active counters are
    used only when an adapter has explicitly supplied them.
    """
    complete = sum(row["bytes"] or 0 for row in items if row["status"] == "complete")
    retained = complete
    known_total = complete
    known_remaining = 0
    unknown = 0
    speed = 0
    speed_quality = "exact"
    active_by_url = {row["url"]: row for row in active}
    for item in items:
        sample = active_by_url.get(item["url"])
        if item["status"] == "complete":
            continue
        total = sample.get("total_bytes") if sample else None
        received = sample.get("received_bytes") if sample else None
        if total is None:
            unknown += 1
        else:
            known_total += total
            known_remaining += max(0, total - (received or 0))
        if received is not None:
            retained += received
        if sample and sample.get("speed_bps") is not None:
            speed += sample["speed_bps"]
        elif sample:
            speed_quality = "partial"
    elapsed = max(0.0, sampled_monotonic - started_monotonic)
    eta = (known_remaining / speed if active and unknown == 0 and speed > 0
           and speed_quality == "exact" else None)
    finish_at = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=eta)).isoformat(
        timespec="seconds") if eta is not None else None
    return {
        "retained_bytes": metric(retained, "exact" if not active else "partial"),
        "complete_bytes": metric(complete, "exact"),
        "session_received_bytes": metric(None, "unavailable"),
        "known_total_bytes": metric(known_total, "partial" if unknown else "exact"),
        "known_remaining_bytes": metric(known_remaining,
                                         "partial" if unknown else "exact"),
        "unknown_size_items": metric(unknown, "exact"),
        "speed_bps": metric(speed if active else None,
                            speed_quality if active else "unavailable"),
        "average_speed_bps": metric(None, "unavailable"),
        "eta_seconds": metric(eta, "exact" if eta is not None else "unavailable"),
        "eta_reason": ("based on current aria2 RPC speed" if eta is not None
                       else "engine counters unavailable" if active else "no active transfer"),
        "estimated_finish_at": metric(finish_at,
                                       "exact" if finish_at is not None else "unavailable"),
        "session_elapsed_s": metric(elapsed, "exact"),
    }


class TelemetryPublisher:
    """Publish atomic snapshots without turning them into controller state."""

    def __init__(self, state: Path, run_id: str, workers: int) -> None:
        self.directory = state / "telemetry" / run_id
        self.path = self.directory / "snapshot.json"
        self.run_id = run_id
        self.session_id = uuid.uuid4().hex
        self.workers = workers
        self.started_monotonic = time.monotonic()
        self.sequence = 0
        self.lifecycle = "starting"
        self.reason: str | None = None
        self.active: dict[str, dict[str, Any]] = {}
        self.validation: dict[str, dict[str, Any]] = {}
        self.events: deque[dict[str, Any]] = deque(maxlen=MAX_EVENTS)
        self.errors: deque[str] = deque(maxlen=20)
        self._lock = threading.Lock()
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._snapshot_builder = None

    def start(self, snapshot_builder) -> None:
        self.directory.mkdir(parents=True, exist_ok=True)
        os.chmod(self.directory, 0o700)
        self._snapshot_builder = snapshot_builder
        self._thread = threading.Thread(target=self._loop, name="telemetry", daemon=True)
        self._thread.start()
        self.request_publish()

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()
        if self._thread:
            self._thread.join(timeout=2)

    def request_publish(self) -> None:
        self._wake.set()

    def event(self, severity: str, category: str, message: str,
              url: str | None = None, worker_id: int | None = None) -> None:
        clean = " ".join(message.replace("\x1b", "?").split())[:MAX_MESSAGE]
        with self._lock:
            self.events.append({"id": uuid.uuid4().hex, "at": utc_now(),
                                "severity": severity, "category": category,
                                "item_id": url, "worker_id": worker_id,
                                "message": clean})
        self.request_publish()

    def set_lifecycle(self, lifecycle: str, reason: str | None = None) -> None:
        with self._lock:
            self.lifecycle, self.reason = lifecycle, reason
        self.request_publish()

    def set_active(self, url: str, worker_id: int, attempt: int, phase: str,
                   pid: int | None = None) -> None:
        with self._lock:
            self.active[url] = {"url": url, "worker_id": worker_id,
                                "generation": attempt, "attempt_id": f"{url}:{attempt}",
                                "attempt_number": attempt, "phase": phase,
                                "reason": None, "phase_started": time.monotonic(), "pid": pid,
                                "engine_instance_id": None, "engine_job_id": None,
                                "received_bytes": None, "total_bytes": None,
                                "total_source": "unavailable", "speed_bps": None,
                                "connections": None, "sample_age_s": None,
                                "last_progress_monotonic": None,
                                "sample_sequence": None}
        self.request_publish()

    def update_phase(self, url: str, phase: str) -> None:
        with self._lock:
            if url in self.active:
                self.active[url]["phase"] = phase
                self.active[url]["phase_started"] = time.monotonic()
        self.request_publish()

    def update_sample(self, url: str, received_bytes: int | None,
                      total_bytes: int | None, speed_bps: int | None,
                      connections: int | None) -> None:
        with self._lock:
            if url not in self.active:
                return
            sample = self.active[url]
            previous = sample.get("received_bytes")
            if (received_bytes is not None
                    and (previous is None or received_bytes > previous)):
                sample["last_progress_monotonic"] = time.monotonic()
            sample.update({"received_bytes": received_bytes, "total_bytes": total_bytes,
                           "speed_bps": speed_bps, "connections": connections,
                           "total_source": "aria2_rpc", "sample_age_s": 0,
                           "sample_sequence": (sample.get("sample_sequence") or 0) + 1})
        self.request_publish()

    def set_validation(self, url: str, processed: int, total: int) -> None:
        with self._lock:
            self.validation[url] = {"item_id": url, "method": "sha256",
                                    "phase": "hashing", "processed_bytes": processed,
                                    "total_bytes": total, "sample_age_s": 0}
        self.request_publish()

    def clear_validation(self, url: str) -> None:
        with self._lock:
            self.validation.pop(url, None)
        self.request_publish()

    def errors_copy(self) -> list[str]:
        with self._lock:
            return list(self.errors)

    def clear_active(self, url: str) -> None:
        with self._lock:
            self.active.pop(url, None)
        self.request_publish()

    def _copy_runtime(self) -> tuple[str, str | None, list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
        with self._lock:
            return (self.lifecycle, self.reason, [dict(x) for x in self.active.values()],
                    [dict(x) for x in self.validation.values()], list(self.events))

    def _loop(self) -> None:
        """Publish shared runtime state twice per second while the run is live."""
        next_publish = time.monotonic()
        while not self._stop.is_set():
            wait_seconds = max(0.0, next_publish - time.monotonic())
            self._wake.wait(wait_seconds)
            self._wake.clear()
            if self._stop.is_set():
                break
            if time.monotonic() < next_publish:
                continue
            try:
                self.publish()
            except (OSError, ValueError, RuntimeError) as exc:
                with self._lock:
                    self.errors.append(f"telemetry write failed: {exc}")
            next_publish += PUBLISH_INTERVAL_SECONDS
            if next_publish < time.monotonic():
                next_publish = time.monotonic() + PUBLISH_INTERVAL_SECONDS

    def publish(self) -> None:
        if self._snapshot_builder is None:
            return
        lifecycle, reason, active, validation, events = self._copy_runtime()
        snapshot = self._snapshot_builder(lifecycle, reason, active, validation, events)
        self.sequence += 1
        snapshot.update({"schema_version": SCHEMA_VERSION, "run_id": self.run_id,
                         "session_id": self.session_id, "sequence": self.sequence,
                         "published_at": utc_now(),
                         "session_elapsed_s": max(0.0, time.monotonic() - self.started_monotonic)})
        temporary = self.path.with_suffix(".tmp")
        data = json.dumps(snapshot, separators=(",", ":"), sort_keys=True).encode() + b"\n"
        if len(data) > 256 * 1024:
            raise OSError("telemetry snapshot exceeds 256 KiB")
        with open(temporary, "wb") as handle:
            os.fchmod(handle.fileno(), 0o600)
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, self.path)
