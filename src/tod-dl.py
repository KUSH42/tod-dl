#!/usr/bin/env python3
"""Resumable, no-overwrite aria2 downloader for priority URL queues."""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import errno
import fcntl
import hashlib
import http.client
import json
import os
import shutil
import socket
import sqlite3
import stat
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path, PurePosixPath
from urllib.parse import unquote, urlsplit

from controller import ControlServer
from inspection import InspectionServer
from download_telemetry import (TelemetryPublisher, estimate_eta_seconds,
                                reduce_projected_metrics, sanitize_message)
from provenance import ProvenanceError, ProvenanceWriter, utc_now

SCHEMA = """CREATE TABLE IF NOT EXISTS downloads (
url TEXT PRIMARY KEY, relative_path TEXT NOT NULL, storage_path TEXT,
staging_path TEXT, inventory_size TEXT, status TEXT NOT NULL DEFAULT 'pending',
attempts INTEGER NOT NULL DEFAULT 0, bytes INTEGER, sha256 TEXT,
last_error TEXT, next_retry_at REAL NOT NULL DEFAULT 0, promotion_target TEXT,
promotion_intent_at TEXT, cleanup_completed_at TEXT, review_code TEXT,
remediation_reason TEXT, remediation_mapping_version TEXT,
remediation_outcome TEXT, remediation_started_at TEXT,
remediation_completed_at TEXT, priority INTEGER NOT NULL DEFAULT 0,
updated_at TEXT NOT NULL)"""
RUN_SCHEMA = """
CREATE TABLE IF NOT EXISTS run_items (
    run_id TEXT NOT NULL,
    url TEXT NOT NULL,
    queue_rank INTEGER NOT NULL,
    PRIMARY KEY (run_id, url)
);
CREATE INDEX IF NOT EXISTS run_items_order
    ON run_items (run_id, queue_rank);
CREATE TABLE IF NOT EXISTS download_transitions (
    id INTEGER PRIMARY KEY,
    url TEXT NOT NULL, run_id TEXT NOT NULL, from_status TEXT,
    to_status TEXT NOT NULL, detail TEXT, recorded_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS download_transitions_url
    ON download_transitions (url, recorded_at);
CREATE TABLE IF NOT EXISTS download_attempts (
    run_id TEXT NOT NULL, url TEXT NOT NULL, attempt_number INTEGER NOT NULL,
    attempt_id TEXT NOT NULL, generation TEXT, started_at TEXT,
    ended_at TEXT, outcome TEXT, error_category TEXT, error_message TEXT,
    retry_at TEXT, PRIMARY KEY (run_id, url, attempt_id)
);
CREATE INDEX IF NOT EXISTS download_attempts_order
    ON download_attempts (run_id, url, attempt_number DESC, attempt_id);
CREATE TABLE IF NOT EXISTS control_requests (
    request_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, session_id TEXT NOT NULL,
    action TEXT NOT NULL, outcome TEXT NOT NULL, reason TEXT NOT NULL,
    state_revision INTEGER NOT NULL, recorded_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS tor_renewals (
    request_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, session_id TEXT NOT NULL,
    action TEXT NOT NULL DEFAULT 'renew_tor_circuits', requested_at TEXT NOT NULL,
    interval_seconds INTEGER NOT NULL,
    previous_success_at REAL, outcome TEXT NOT NULL, failure_reason TEXT,
    successful_at REAL, next_eligible_at REAL
);
CREATE TABLE IF NOT EXISTS telemetry_revisions (
    run_id TEXT PRIMARY KEY, revision INTEGER NOT NULL
);
"""
RETRY_DELAYS = (60, 120, 240, 300, 300, 300)
FAILPOINTS = frozenset({"post_validation_intent", "post_final_file_creation",
                        "post_completion_commit"})
EXCLUDABLE_STATUSES = frozenset({"pending", "queued", "active", "admitted",
                                 "retry_wait", "failed", "review_required",
                                 "unavailable"})
PRIORITIZABLE_STATUSES = EXCLUDABLE_STATUSES
PRIORITY_MIN, PRIORITY_MAX = -5, 5
COOLDOWN_ELIGIBLE_STATUSES = frozenset({"retry_wait", "failed"})
COOLDOWN_OVERRIDE_MIN_S, COOLDOWN_OVERRIDE_MAX_S = 0, 3600
ADMISSION_POLL_SECONDS = 0.25
SAFE_PATH_MAPPING_VERSION = "v1"
DISPLAY_BUCKETS = ("queued", "busy", "retry", "exhausted", "complete",
                   "existing_unverified", "review_required", "unavailable",
                   "excluded", "unknown")


def now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def display_bucket(status: str, attempts: int, max_attempts: int) -> str:
    """Map durable state to one mutually exclusive telemetry bucket."""
    if status == "complete":
        return "complete"
    if status in {"active", "admitted", "promoting"}:
        return "busy"
    if status in {"retry_wait", "failed"}:
        return "exhausted" if max_attempts and attempts >= max_attempts else "retry"
    if status in {"existing_unverified", "review_required", "unavailable", "excluded"}:
        return status
    if status in {"pending", "queued"}:
        return "queued"
    return "unknown"


def allocate_loopback_port() -> int:
    """Reserve a loopback-selected port long enough to choose an RPC endpoint."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def aria2_rpc_status(port: int, secret: str) -> dict | None:
    """Read one active aria2 job without exposing its credential to consumers."""
    request = {"jsonrpc": "2.0", "id": "telemetry", "method": "aria2.tellActive",
               "params": [f"token:{secret}", ["gid", "completedLength", "totalLength",
                                                   "downloadSpeed", "connections"]]}
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=1)
    try:
        connection.request("POST", "/jsonrpc", json.dumps(request),
                           {"Content-Type": "application/json"})
        response = connection.getresponse()
        payload = json.loads(response.read())
        active = payload.get("result", [])
        return active[0] if active else None
    except (OSError, ValueError, http.client.HTTPException):
        return None
    finally:
        connection.close()


def aria2_rpc_call(port: int, method: str, params: list) -> dict | None:
    """Issue one bounded local JSON-RPC request, returning no data on failure."""
    request = {"jsonrpc": "2.0", "id": "evaluation", "method": method,
               "params": params}
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=1)
    try:
        connection.request("POST", "/jsonrpc", json.dumps(request),
                           {"Content-Type": "application/json"})
        return json.loads(connection.getresponse().read())
    except (OSError, ValueError, http.client.HTTPException):
        return None
    finally:
        connection.close()


def aria2_rpc_job_status(port: int, secret: str, gid: str | None) -> str | None:
    """Return a known terminal job status, without controlling the engine.

    aria2 stays alive to serve RPC after a download reaches ``complete`` or
    ``error``. The controller must therefore inspect the job, rather than
    infer transfer completion from the process lifetime.
    """
    fields = ["status"]
    if gid:
        response = aria2_rpc_call(port, "aria2.tellStatus",
                                  [f"token:{secret}", gid, fields])
        result = response.get("result") if response else None
        status = result.get("status") if isinstance(result, dict) else None
        if status in {"complete", "error", "removed"}:
            return status
        return None
    response = aria2_rpc_call(port, "aria2.tellStopped",
                              [f"token:{secret}", 0, 1, fields])
    result = response.get("result") if response else None
    if not isinstance(result, list) or not result:
        return None
    status = result[0].get("status") if isinstance(result[0], dict) else None
    return status if status in {"complete", "error", "removed"} else None


def aria2_log_terminal_status(log_path: Path) -> str | None:
    """Recognize a terminal one-item aria2 result when its RPC server stalls."""
    try:
        lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return None
    # The adapter sets --max-tries=1, so aria2's recorded errorCode is final
    # for this one-job process, not an intermediate retry.
    if any("errorCode=" in line for line in lines):
        return "error"
    if any("Download complete:" in line for line in lines):
        return "complete"
    return None


def evaluate_aria2_rpc(args: argparse.Namespace) -> int:
    """Exercise local RPC authentication without admitting a source transfer."""
    port = allocate_loopback_port()
    secret = uuid.uuid4().hex
    command = [args.torsocks, "-i", args.aria2c, "--enable-rpc=true",
               "--rpc-listen-all=false", "--disable-ipv6=true",
               f"--rpc-listen-port={port}", f"--rpc-secret={secret}"]
    process = subprocess.Popen(command, stdout=subprocess.DEVNULL,
                               stderr=subprocess.DEVNULL, text=True)
    try:
        deadline = time.monotonic() + 5
        authenticated = None
        while time.monotonic() < deadline and process.poll() is None:
            authenticated = aria2_rpc_call(port, "aria2.getVersion", [f"token:{secret}"])
            if authenticated and "result" in authenticated:
                break
            time.sleep(0.1)
        if not authenticated or "result" not in authenticated:
            print("RPC evaluation failed: authenticated loopback request was unavailable")
            return 1
        unauthenticated = aria2_rpc_call(port, "aria2.getVersion", [])
        if unauthenticated and "result" in unauthenticated:
            print("RPC evaluation failed: unauthenticated request returned data")
            return 1
        outcome = "error response" if unauthenticated else "no response"
        print("RPC evaluation passed: authenticated loopback access; "
              f"unauthenticated access produced {outcome}.")
        return 0
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


def relative_path(url: str) -> PurePosixPath:
    parsed = urlsplit(url)
    if parsed.username or parsed.password or parsed.query:
        raise ValueError("URL contains credentials or query parameters")
    parts = parsed.path.lstrip("/").split("/", 2)
    if len(parts) != 3 or parts[1] != "data" or parts[2] in {"", "ALL_FILES"}:
        raise ValueError("URL is not a data-file URL")
    # Decode each URL path segment on its own, after splitting on literal "/",
    # so a percent-encoded "/" cannot be mistaken for a path separator.
    tail = [unquote(part) for part in PurePosixPath(parts[2]).parts]
    path = PurePosixPath(unquote(parts[0])) / "data" / PurePosixPath(*tail)
    if any(part == ".." for part in path.parts):
        raise ValueError("unsafe URL path")
    return path


def legacy_relative_path(url: str) -> PurePosixPath | None:
    """Return the un-decoded path a pre-fix relative_path() would compute.

    Multiple independent controllers can share one destination tree and
    tell an already-acquired final apart from a missing one purely by
    filesystem existence. Decoding percent-encoded segments changed the
    computed path for evidence acquired before this fix; this reconstructs
    the old path so that existing evidence is still found, regardless of
    which controller's database (if any) recorded it.
    """
    try:
        parsed = urlsplit(url)
        if parsed.username or parsed.password or parsed.query:
            return None
        parts = parsed.path.lstrip("/").split("/", 2)
        if len(parts) != 3 or parts[1] != "data" or parts[2] in {"", "ALL_FILES"}:
            return None
        path = PurePosixPath(parts[0]) / "data" / PurePosixPath(parts[2])
        if any(part == ".." for part in path.parts):
            return None
        return path
    except ValueError:
        return None


def read_queues(paths: list[Path], on_reject=None):
    seen: set[str] = set()
    for path in paths:
        with path.open(encoding="utf-8", errors="replace") as handle:
            for line in handle:
                url = line.strip()
                if not url or url.startswith("#"):
                    continue
                if url in seen:
                    if on_reject:
                        on_reject(url, "duplicate URL")
                    continue
                seen.add(url)
                try:
                    yield url, relative_path(url)
                except ValueError as exc:
                    if on_reject:
                        on_reject(url, str(exc))
                    continue


def sha256sum(path: Path, progress=None) -> str:
    digest = hashlib.sha256()
    processed = 0
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
            processed += len(block)
            if progress:
                progress(processed)
    return digest.hexdigest()


def is_connectivity_failure(error: str) -> bool:
    """Identify errors that merit a global Tor connection cooldown."""
    upper = error.upper()
    return any(marker in upper for marker in (
        "SOCKS",
        "NAME RESOLUTION",
        "FAILED TO CONNECT TO THE HOST",
        "CONNECTION REFUSED",
        "NO ROUTE TO HOST",
        "CONNECTION TIMED OUT",
    ))


def is_incomplete_body_failure(error: str) -> bool:
    """Identify a source that closed the stream before sending the full body.

    aria2 reports this as errorCode=1 ("Got EOF from the server"), the same
    generic code it uses for other unrelated failures, so match on the
    specific message rather than the code alone.
    """
    return "GOT EOF FROM THE SERVER" in error.upper()


def hit_failpoint(name: str) -> None:
    """Exit immediately if TOD_DL_FAILPOINT names this boundary (test only).

    Simulates a process kill at an exact finalization step, for E06. Never
    reachable in normal operation: the evaluation runner is the only caller
    that sets TOD_DL_FAILPOINT.
    """
    assert name in FAILPOINTS, f"unknown failpoint {name!r}"
    if os.environ.get("TOD_DL_FAILPOINT") == name:
        os._exit(70)


def review_code_for_error(error: OSError) -> str | None:
    """Classify only promotion errors that have an unambiguous recovery rule."""
    return "ENAMETOOLONG" if error.errno == errno.ENAMETOOLONG else None


def legacy_name_too_long_error(error: str | None) -> bool:
    """Recognize the historic Linux error text without broad error matching."""
    return bool(error and "Errno 36" in error and "File name too long" in error)


def storage_relative(path: PurePosixPath, destination: Path) -> PurePosixPath:
    try:
        name_max = os.pathconf(destination, "PC_NAME_MAX")
    except OSError:
        name_max = 240
    safe_component_bytes = min(240, max(1, name_max - 1))
    parts = []
    for component in path.parts:
        if len(component.encode("utf-8")) > safe_component_bytes:
            parts.append("__longname__" + hashlib.sha256(component.encode()).hexdigest())
        else:
            parts.append(component)
    stored = PurePosixPath(*parts)
    if len(os.fsencode(destination / Path(stored))) > 3800:
        return PurePosixPath("__longpath__" + hashlib.sha256(
            path.as_posix().encode()).hexdigest())
    return stored


def control_command(control: socket.socket, command: str) -> list[str]:
    control.sendall((command + "\r\n").encode("ascii"))
    response = []
    buffer = b""
    while True:
        while b"\n" not in buffer:
            block = control.recv(4096)
            if not block:
                raise RuntimeError("Tor ControlPort closed the connection")
            buffer += block
        line, buffer = buffer.split(b"\n", 1)
        text = line.decode("utf-8", errors="replace").rstrip("\r")
        response.append(text)
        if text.startswith("250 ") or text.startswith("5"):
            return response


def verify_tor_isolation(address: str, cookie: Path) -> list[str]:
    host, separator, port = address.rpartition(":")
    if not separator or not host or not port.isdecimal():
        raise RuntimeError("--tor-control-address must be HOST:PORT")
    try:
        token = cookie.read_bytes().hex()
        with socket.create_connection((host, int(port)), timeout=10) as control:
            if not control_command(control, f"AUTHENTICATE {token}")[-1].startswith("250"):
                raise RuntimeError("Tor ControlPort authentication failed")
            response = control_command(control, "GETCONF SocksPort")
    except OSError as exc:
        raise RuntimeError(f"Tor isolation preflight failed: {exc}") from exc
    ports = [
        line.partition("=")[2]
        for line in response
        if line.startswith("250-") or line.startswith("250 SocksPort=")
    ]
    if not ports or not any("IsolateSOCKSAuth" in port.split() for port in ports):
        raise RuntimeError("Tor SocksPort is not configured with IsolateSOCKSAuth")
    return ports


def send_tor_newnym(address: str, cookie: Path) -> None:
    """Ask Tor to use fresh circuits for future streams."""
    host, separator, port = address.rpartition(":")
    if not separator or not host or not port.isdecimal():
        raise RuntimeError("--tor-control-address must be HOST:PORT")
    try:
        token = cookie.read_bytes().hex()
        with socket.create_connection((host, int(port)), timeout=10) as control:
            if not control_command(control, f"AUTHENTICATE {token}")[-1].startswith("250"):
                raise RuntimeError("Tor ControlPort authentication failed")
            if not control_command(control, "SIGNAL NEWNYM")[-1].startswith("250"):
                raise RuntimeError("Tor rejected SIGNAL NEWNYM")
    except OSError as exc:
        raise RuntimeError(f"Tor circuit renewal failed: {exc}") from exc


class TelemetryStateProjection:
    """Lock-protected, disposable summaries for one selected run."""

    def __init__(self, selected_count: int, skipped_existing: int, revision: int,
                 max_attempts: int) -> None:
        self._lock = threading.Lock()
        self.selected_count = selected_count
        self.skipped_existing = skipped_existing
        self.revision = revision
        self.max_attempts = max_attempts
        self.counts = {bucket: 0 for bucket in DISPLAY_BUCKETS}
        self.complete_bytes = 0
        self.last_completion_at: str | None = None
        self._retries: dict[str, tuple[float, str]] = {}

    @classmethod
    def hydrate(cls, db: sqlite3.Connection, run_id: str,
                max_attempts: int) -> "TelemetryStateProjection":
        """Build a projection in bounded row batches before publication starts."""
        selected_count = db.execute("SELECT COUNT(*) FROM run_items WHERE run_id=?",
                                    (run_id,)).fetchone()[0]
        skipped_existing = db.execute(
            "SELECT COUNT(DISTINCT url) FROM download_transitions "
            "WHERE run_id=? AND to_status='existing_unverified' AND url NOT IN "
            "(SELECT url FROM run_items WHERE run_id=?)", (run_id, run_id)
        ).fetchone()[0]
        row = db.execute("SELECT revision FROM telemetry_revisions WHERE run_id=?",
                         (run_id,)).fetchone()
        projection = cls(selected_count, skipped_existing, row[0] if row else 0,
                         max_attempts)
        rank = -1
        while True:
            rows = db.execute(
                "SELECT run_items.queue_rank, downloads.url, downloads.status, "
                "downloads.attempts, downloads.bytes, downloads.next_retry_at, "
                "downloads.last_error, downloads.updated_at FROM downloads JOIN run_items "
                "ON run_items.url=downloads.url WHERE run_items.run_id=? "
                "AND run_items.queue_rank>? ORDER BY run_items.queue_rank LIMIT 1000",
                (run_id, rank),
            ).fetchall()
            if not rows:
                break
            for rank, url, status, attempts, byte_count, retry_at, error, updated_at in rows:
                projection._add_row({"url": url, "status": status, "attempts": attempts,
                                     "bytes": byte_count, "next_retry_at": retry_at,
                                     "last_error": error, "updated_at": updated_at})
        completion_row = db.execute(
            "SELECT MAX(recorded_at) FROM download_transitions WHERE run_id=? "
            "AND to_status='complete' AND (from_status IS NULL OR from_status != 'complete')",
            (run_id,),
        ).fetchone()
        projection.last_completion_at = completion_row[0] if completion_row else None
        return projection

    def _retry_key(self, url: str) -> str:
        return hashlib.sha256(url.encode("utf-8")).hexdigest()

    def _add_row(self, row: dict) -> None:
        bucket = display_bucket(row["status"], row["attempts"], self.max_attempts)
        self.counts[bucket] += 1
        if bucket == "complete":
            self.complete_bytes += row["bytes"] or 0
            completion_at = row.get("updated_at")
            if completion_at and (self.last_completion_at is None
                                  or completion_at > self.last_completion_at):
                self.last_completion_at = completion_at
        if bucket == "retry":
            self._retries[self._retry_key(row["url"])] = (
                row["next_retry_at"], sanitize_message(row.get("last_error") or ""),
            )

    def apply(self, old: dict, new: dict, revision: int,
              completion_at: str | None = None) -> None:
        """Apply one committed item preimage and postimage."""
        with self._lock:
            old_bucket = display_bucket(old["status"], old["attempts"], self.max_attempts)
            new_bucket = display_bucket(new["status"], new["attempts"], self.max_attempts)
            self.counts[old_bucket] -= 1
            self.counts[new_bucket] += 1
            if old_bucket == "complete":
                self.complete_bytes -= old["bytes"] or 0
            if new_bucket == "complete":
                self.complete_bytes += new["bytes"] or 0
            key = self._retry_key(new["url"])
            self._retries.pop(key, None)
            if new_bucket == "retry":
                self._retries[key] = (new["next_retry_at"],
                                      sanitize_message(new.get("last_error") or ""))
            if old_bucket != "complete" and new_bucket == "complete" and completion_at:
                self.last_completion_at = completion_at
            self.revision = revision

    def set_revision(self, revision: int) -> None:
        """Apply a committed non-item audit revision."""
        with self._lock:
            self.revision = revision

    def copy(self) -> dict:
        """Copy only durable summary data for one snapshot build."""
        with self._lock:
            first_retry = min(((deadline, key, error) for key, (deadline, error)
                               in self._retries.items()), default=None)
            return {
                "selected_count": self.selected_count,
                "skipped_existing": self.skipped_existing,
                "counts": dict(self.counts),
                "complete_bytes": self.complete_bytes,
                "last_completion_at": self.last_completion_at,
                "revision": self.revision,
                "retry_at": first_retry[0] if first_retry else None,
                "retry_error": first_retry[2] if first_retry else None,
                "retry_count": len(self._retries),
            }


class Downloader:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.destination = args.destination.resolve()
        self.state = args.state.resolve()
        self.incoming = self.state / "incoming"
        self.candidates = self.state / "redownload-candidates"
        self.run_id = args.run_id or dt.datetime.now(dt.timezone.utc).strftime(
            "%Y%m%dT%H%M%S%fZ"
        )
        self.run_dir = self.state / "aria2" / self.run_id
        self.manifest_path = self.run_dir / "manifest.json"
        self.db_lock = threading.Lock()
        self.active_lock = threading.Lock()
        self.start_lock = threading.Lock()
        self.active: dict[str, Path] = {}
        self.worker_ids: dict[str, int] = {}
        self.active_attempts: dict[str, int] = {}
        self.telemetry: TelemetryPublisher | None = None
        self.projection: TelemetryStateProjection | None = None
        self.cooldown_lock = threading.Lock()
        self.cooldown_until = 0.0
        self.newnym_lock = threading.Lock()
        self.next_newnym_at = 0.0
        self.next_worker_start = 0.0
        self.stop_requested = threading.Event()
        self.admission_paused = threading.Event()
        self.drain_requested = threading.Event()
        self.control_wake = threading.Event()
        self.deadline: float | None = None
        self.pending_remediation_events: list[tuple[str, str, str]] = []
        self.provenance: ProvenanceWriter | None = None
        self.queue_inputs: list[dict] = []
        self.selected_item_count = 0

    @staticmethod
    def item_id(url: str) -> str:
        """Return the stable selected-manifest identifier for one URL."""
        return hashlib.sha256(url.encode("utf-8")).hexdigest()

    def provenance_event(self, event_type: str, **fields: object) -> None:
        """Write and fsync evidence provenance before related state changes."""
        if not self.provenance:
            return
        try:
            self.provenance.event(event_type, **fields)
        except ProvenanceError as exc:
            self.stop_requested.set()
            raise RuntimeError(f"local provenance failure: {exc}") from exc

    def attempt_event(self, db: sqlite3.Connection, url: str, number: int,
                      started_at: str, outcome: str) -> None:
        self.provenance_event(
            "attempt_finished", item_id=self.item_id(url), source_url=url,
            attempt_number=number, request_started_at=started_at,
            request_finished_at=utc_now(), final_url=None, redirect_chain=[],
            http_status=None, outcome=outcome, response_content_length=None,
            response_content_range=None, response_content_type=None,
            response_etag=None, response_last_modified=None,
        )
        attempt_id = hashlib.sha256(
            f"{self.run_id}:{url}:{number}".encode("utf-8")).hexdigest()
        with self.db_lock:
            db.execute("UPDATE download_attempts SET ended_at=?, outcome=? "
                       "WHERE run_id=? AND url=? AND attempt_id=?",
                       (utc_now(), outcome, self.run_id, url, attempt_id))
            db.commit()

    def record_attempt_start(self, db: sqlite3.Connection, url: str, number: int,
                             started_at: str) -> None:
        """Record a durable attempt identity without deriving historic attempts."""
        attempt_id = hashlib.sha256(
            f"{self.run_id}:{url}:{number}".encode("utf-8")).hexdigest()
        with self.db_lock:
            db.execute("INSERT OR IGNORE INTO download_attempts "
                       "(run_id, url, attempt_number, attempt_id, started_at) "
                       "VALUES (?, ?, ?, ?, ?)",
                       (self.run_id, url, number, attempt_id, started_at))
            db.commit()

    def finalized_event(self, url: str, logical: str, stored: str,
                        byte_count: int, sha256: str) -> None:
        unavailable = {"available": False, "compared": False, "value": None}
        self.provenance_event(
            "finalized", item_id=self.item_id(url), source_url=url,
            logical_relative_path=logical, final_relative_path=stored,
            byte_count=byte_count, sha256=sha256, validation_method_version="1",
            finalized_at=utc_now(), expected_size=unavailable,
            expected_checksum=unavailable, etag=unavailable, last_modified=unavailable,
        )

    def candidate_event(self, url: str, staging: Path, candidate: Path | None,
                        sha256: str | None, reason: str) -> None:
        staging_relative = staging.relative_to(self.state).as_posix()
        candidate_relative = (candidate.relative_to(self.candidates).as_posix()
                              if candidate else None)
        self.provenance_event(
            "candidate_created", item_id=self.item_id(url),
            staging_relative_path=staging_relative,
            candidate_relative_path=candidate_relative, sha256=sha256,
            finalization_failure_reason=reason,
        )

    def control_state(self) -> dict[str, object]:
        """Return bounded controller-owned information for an opted-in monitor."""
        with self.active_lock:
            active_workers = len(self.active)
        renewal_available = (self.args.tor_newnym_interval > 0
                             and not self.stop_requested.is_set())
        stopping = self.stop_requested.is_set()
        draining = self.drain_requested.is_set()
        paused = self.admission_paused.is_set()
        return {
            "state_revision": (self.projection.copy()["revision"]
                               if self.projection else 0),
            "lifecycle": "stopping" if stopping else "running",
            "active_workers": active_workers,
            "actions": {
                "get_control_state": "available",
                "retry_now": "available",
                "exclude_item": "available",
                "set_item_priority": "available",
                "set_retry_cooldown": "available",
                "renew_tor_circuits": "available" if renewal_available else "unavailable",
                "pause_admission": ("available" if not stopping and not draining and not paused
                                    else "unavailable"),
                "resume_admission": ("available" if not stopping and not draining and paused
                                     else "unavailable"),
                "drain_and_stop": "available" if not stopping and not draining else "unavailable",
                "checkpoint_stop": "available" if not stopping else "unavailable",
            },
        }

    def control_action(self, db: sqlite3.Connection,
                       request: dict[str, object]) -> dict[str, object]:
        """Run one confirmed controller action through the command channel."""
        if request.get("action") == "retry_now":
            return self.control_retry_now(db, request)
        if request.get("action") == "exclude_item":
            return self.control_exclude_item(db, request)
        if request.get("action") == "set_item_priority":
            return self.control_set_item_priority(db, request)
        if request.get("action") == "set_retry_cooldown":
            return self.control_set_retry_cooldown(db, request)
        if request.get("action") == "renew_tor_circuits":
            return self.control_renew_tor_circuits(db, request)
        if request.get("action") == "pause_admission":
            return self.control_pause_admission(db, request)
        if request.get("action") == "resume_admission":
            return self.control_resume_admission(db, request)
        if request.get("action") == "drain_and_stop":
            return self.control_drain_and_stop(db, request)
        if request.get("action") == "checkpoint_stop":
            return self.control_checkpoint_stop(db, request)
        return {"outcome": "rejected", "reason": "action is unavailable"}

    def control_retry_now(self, db: sqlite3.Connection,
                          request: dict[str, object]) -> dict[str, object]:
        """Durably make only selected retryable work eligible immediately."""
        request_id = request["request_id"]
        session_id = request["session_id"]
        if not isinstance(request_id, str) or not isinstance(session_id, str):
            return {"outcome": "rejected", "reason": "invalid control request"}
        parameters = request.get("parameters")
        item_ids = parameters.get("item_ids") if isinstance(parameters, dict) else None
        if item_ids is not None and (
                not isinstance(item_ids, list) or not item_ids
                or not all(isinstance(item_id, str) and item_id for item_id in item_ids)):
            return {"outcome": "rejected", "reason": "item_ids must be a non-empty list of strings"}
        with self.db_lock:
            existing = db.execute("SELECT outcome, reason, state_revision FROM control_requests "
                                  "WHERE request_id=?", (request_id,)).fetchone()
            if existing:
                outcome, reason, revision = existing
                return {"outcome": outcome, "reason": reason, "state_revision": revision}
            candidate_rows = db.execute(
                "SELECT downloads.url, downloads.status, downloads.attempts, downloads.bytes, "
                "downloads.next_retry_at, downloads.last_error FROM downloads JOIN run_items "
                "ON run_items.url=downloads.url WHERE run_items.run_id=? AND "
                "downloads.status IN ('queued', 'retry_wait', 'pending', 'failed')",
                (self.run_id,),
            ).fetchall()
            if item_ids is not None:
                wanted = set(item_ids)
                changed_rows = [row for row in candidate_rows if self.item_id(row[0]) in wanted]
            else:
                changed_rows = candidate_rows
            urls = [row[0] for row in changed_rows]
            if urls:
                placeholders = ",".join("?" * len(urls))
                changed = db.execute(
                    f"UPDATE downloads SET next_retry_at=0, updated_at=? "
                    f"WHERE url IN ({placeholders})",
                    (now(), *urls),
                ).rowcount
            else:
                changed = 0
            reason = f"made {changed} selected retryable item(s) eligible now"
            db.execute("INSERT INTO download_transitions "
                       "(url, run_id, from_status, to_status, detail, recorded_at) "
                       "VALUES (?, ?, ?, ?, ?, ?)",
                       ("__control__", self.run_id, "control", "control", reason, now()))
            revision = self.advance_telemetry_revision(db)
            db.execute("INSERT INTO control_requests "
                       "(request_id, run_id, session_id, action, outcome, reason, "
                       "state_revision, recorded_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                       (request_id, self.run_id, session_id, "retry_now", "completed",
                        reason, revision, now()))
            db.commit()
        for url, status, attempts, byte_count, retry_at, error in changed_rows:
            old = {"url": url, "status": status, "attempts": attempts,
                   "bytes": byte_count, "next_retry_at": retry_at, "last_error": error}
            new = dict(old, next_retry_at=0)
            self.apply_projection_change(old, new, revision)
        if self.projection and not changed_rows:
            self.projection.set_revision(revision)
        if self.telemetry:
            self.telemetry.event("info", "control", "retry now accepted")
        self.control_wake.set()
        return {"outcome": "completed", "reason": reason, "state_revision": revision}

    def control_exclude_item(self, db: sqlite3.Connection,
                             request: dict[str, object]) -> dict[str, object]:
        """Durably move only the requested selected items to `excluded`."""
        request_id = request["request_id"]
        session_id = request["session_id"]
        if not isinstance(request_id, str) or not isinstance(session_id, str):
            return {"outcome": "rejected", "reason": "invalid control request"}
        parameters = request.get("parameters")
        item_ids = parameters.get("item_ids") if isinstance(parameters, dict) else None
        if (not isinstance(item_ids, list) or not item_ids
                or not all(isinstance(item_id, str) and item_id for item_id in item_ids)):
            return {"outcome": "rejected", "reason": "item_ids must be a non-empty list of strings"}
        with self.db_lock:
            existing = db.execute("SELECT outcome, reason, state_revision FROM control_requests "
                                  "WHERE request_id=?", (request_id,)).fetchone()
            if existing:
                outcome, reason, revision = existing
                return {"outcome": outcome, "reason": reason, "state_revision": revision}
            selected_rows = db.execute(
                "SELECT downloads.url, downloads.status, downloads.attempts, downloads.bytes, "
                "downloads.next_retry_at, downloads.last_error FROM downloads JOIN run_items "
                "ON run_items.url=downloads.url WHERE run_items.run_id=?",
                (self.run_id,),
            ).fetchall()
            by_id = {self.item_id(row[0]): row for row in selected_rows}
            item_outcomes: dict[str, str] = {}
            changed_rows = []
            for item_id in item_ids:
                row = by_id.get(item_id)
                if row is None:
                    item_outcomes[item_id] = "rejected: item is outside the selected set"
                elif row[1] not in EXCLUDABLE_STATUSES:
                    item_outcomes[item_id] = f"rejected: item is in {row[1]}, not excludable"
                else:
                    item_outcomes[item_id] = "excluded"
                    changed_rows.append(row)
            urls = [row[0] for row in changed_rows]
            if urls:
                placeholders = ",".join("?" * len(urls))
                db.execute(
                    f"UPDATE downloads SET status='excluded', updated_at=? "
                    f"WHERE url IN ({placeholders})",
                    (now(), *urls),
                )
            reason = f"excluded {len(changed_rows)} of {len(item_ids)} requested item(s)"
            db.execute("INSERT INTO download_transitions "
                       "(url, run_id, from_status, to_status, detail, recorded_at) "
                       "VALUES (?, ?, ?, ?, ?, ?)",
                       ("__control__", self.run_id, "control", "control", reason, now()))
            revision = self.advance_telemetry_revision(db)
            db.execute("INSERT INTO control_requests "
                       "(request_id, run_id, session_id, action, outcome, reason, "
                       "state_revision, recorded_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                       (request_id, self.run_id, session_id, "exclude_item", "completed",
                        reason, revision, now()))
            db.commit()
        for url, status, attempts, byte_count, retry_at, error in changed_rows:
            old = {"url": url, "status": status, "attempts": attempts,
                   "bytes": byte_count, "next_retry_at": retry_at, "last_error": error}
            new = dict(old, status="excluded")
            self.apply_projection_change(old, new, revision)
        if self.projection and not changed_rows:
            self.projection.set_revision(revision)
        if self.telemetry:
            self.telemetry.event("info", "control", "exclude item accepted")
        return {"outcome": "completed", "reason": reason, "state_revision": revision,
                "items": item_outcomes}

    def control_set_item_priority(self, db: sqlite3.Connection,
                                  request: dict[str, object]) -> dict[str, object]:
        """Durably set a scheduler-preference hint; never renumbers queue rank."""
        request_id = request["request_id"]
        session_id = request["session_id"]
        if not isinstance(request_id, str) or not isinstance(session_id, str):
            return {"outcome": "rejected", "reason": "invalid control request"}
        parameters = request.get("parameters")
        item_ids = parameters.get("item_ids") if isinstance(parameters, dict) else None
        priority = parameters.get("priority") if isinstance(parameters, dict) else None
        if (not isinstance(item_ids, list) or not item_ids
                or not all(isinstance(item_id, str) and item_id for item_id in item_ids)):
            return {"outcome": "rejected", "reason": "item_ids must be a non-empty list of strings"}
        if (isinstance(priority, bool) or not isinstance(priority, int)
                or not PRIORITY_MIN <= priority <= PRIORITY_MAX):
            return {"outcome": "rejected",
                    "reason": f"priority must be an integer between {PRIORITY_MIN} and {PRIORITY_MAX}"}
        with self.db_lock:
            existing = db.execute("SELECT outcome, reason, state_revision FROM control_requests "
                                  "WHERE request_id=?", (request_id,)).fetchone()
            if existing:
                outcome, reason, revision = existing
                return {"outcome": outcome, "reason": reason, "state_revision": revision}
            selected_rows = db.execute(
                "SELECT downloads.url, downloads.status FROM downloads JOIN run_items "
                "ON run_items.url=downloads.url WHERE run_items.run_id=?",
                (self.run_id,),
            ).fetchall()
            by_id = {self.item_id(row[0]): row for row in selected_rows}
            item_outcomes: dict[str, str] = {}
            changed_urls = []
            for item_id in item_ids:
                row = by_id.get(item_id)
                if row is None:
                    item_outcomes[item_id] = "rejected: item is outside the selected set"
                elif row[1] not in PRIORITIZABLE_STATUSES:
                    item_outcomes[item_id] = f"rejected: item is in {row[1]}, not prioritizable"
                else:
                    item_outcomes[item_id] = "prioritized"
                    changed_urls.append(row[0])
            if changed_urls:
                placeholders = ",".join("?" * len(changed_urls))
                db.execute(
                    f"UPDATE downloads SET priority=?, updated_at=? WHERE url IN ({placeholders})",
                    (priority, now(), *changed_urls),
                )
            reason = (f"set priority to {priority} for {len(changed_urls)} of "
                      f"{len(item_ids)} requested item(s)")
            db.execute("INSERT INTO download_transitions "
                       "(url, run_id, from_status, to_status, detail, recorded_at) "
                       "VALUES (?, ?, ?, ?, ?, ?)",
                       ("__control__", self.run_id, "control", "control", reason, now()))
            revision = self.advance_telemetry_revision(db)
            db.execute("INSERT INTO control_requests "
                       "(request_id, run_id, session_id, action, outcome, reason, "
                       "state_revision, recorded_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                       (request_id, self.run_id, session_id, "set_item_priority", "completed",
                        reason, revision, now()))
            db.commit()
        if self.projection:
            self.projection.set_revision(revision)
        if self.telemetry:
            self.telemetry.event("info", "control", "set item priority accepted")
        return {"outcome": "completed", "reason": reason, "state_revision": revision,
                "items": item_outcomes}

    def control_set_retry_cooldown(self, db: sqlite3.Connection,
                                   request: dict[str, object]) -> dict[str, object]:
        """Durably override a selected item's retry-cooldown deadline."""
        request_id = request["request_id"]
        session_id = request["session_id"]
        if not isinstance(request_id, str) or not isinstance(session_id, str):
            return {"outcome": "rejected", "reason": "invalid control request"}
        parameters = request.get("parameters")
        item_ids = parameters.get("item_ids") if isinstance(parameters, dict) else None
        cooldown_s = parameters.get("cooldown_s") if isinstance(parameters, dict) else None
        if (not isinstance(item_ids, list) or not item_ids
                or not all(isinstance(item_id, str) and item_id for item_id in item_ids)):
            return {"outcome": "rejected", "reason": "item_ids must be a non-empty list of strings"}
        if (isinstance(cooldown_s, bool) or not isinstance(cooldown_s, int)
                or not COOLDOWN_OVERRIDE_MIN_S <= cooldown_s <= COOLDOWN_OVERRIDE_MAX_S):
            return {"outcome": "rejected",
                    "reason": (f"cooldown_s must be an integer between "
                               f"{COOLDOWN_OVERRIDE_MIN_S} and {COOLDOWN_OVERRIDE_MAX_S}")}
        with self.db_lock:
            existing = db.execute("SELECT outcome, reason, state_revision FROM control_requests "
                                  "WHERE request_id=?", (request_id,)).fetchone()
            if existing:
                outcome, reason, revision = existing
                return {"outcome": outcome, "reason": reason, "state_revision": revision}
            selected_rows = db.execute(
                "SELECT downloads.url, downloads.status, downloads.attempts, downloads.bytes, "
                "downloads.next_retry_at, downloads.last_error FROM downloads JOIN run_items "
                "ON run_items.url=downloads.url WHERE run_items.run_id=?",
                (self.run_id,),
            ).fetchall()
            by_id = {self.item_id(row[0]): row for row in selected_rows}
            item_outcomes: dict[str, str] = {}
            changed_rows = []
            new_retry_at = time.time() + cooldown_s
            for item_id in item_ids:
                row = by_id.get(item_id)
                if row is None:
                    item_outcomes[item_id] = "rejected: item is outside the selected set"
                elif row[1] not in COOLDOWN_ELIGIBLE_STATUSES:
                    item_outcomes[item_id] = f"rejected: item is in {row[1]}, not cooldown-eligible"
                else:
                    item_outcomes[item_id] = "cooldown set"
                    changed_rows.append(row)
            urls = [row[0] for row in changed_rows]
            if urls:
                placeholders = ",".join("?" * len(urls))
                db.execute(
                    f"UPDATE downloads SET next_retry_at=?, updated_at=? "
                    f"WHERE url IN ({placeholders})",
                    (new_retry_at, now(), *urls),
                )
            reason = (f"set retry cooldown to {cooldown_s}s for {len(changed_rows)} of "
                      f"{len(item_ids)} requested item(s)")
            db.execute("INSERT INTO download_transitions "
                       "(url, run_id, from_status, to_status, detail, recorded_at) "
                       "VALUES (?, ?, ?, ?, ?, ?)",
                       ("__control__", self.run_id, "control", "control", reason, now()))
            revision = self.advance_telemetry_revision(db)
            db.execute("INSERT INTO control_requests "
                       "(request_id, run_id, session_id, action, outcome, reason, "
                       "state_revision, recorded_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                       (request_id, self.run_id, session_id, "set_retry_cooldown", "completed",
                        reason, revision, now()))
            db.commit()
        for url, status, attempts, byte_count, retry_at, error in changed_rows:
            old = {"url": url, "status": status, "attempts": attempts,
                   "bytes": byte_count, "next_retry_at": retry_at, "last_error": error}
            new = dict(old, next_retry_at=new_retry_at)
            self.apply_projection_change(old, new, revision)
        if self.projection and not changed_rows:
            self.projection.set_revision(revision)
        if self.telemetry:
            self.telemetry.event("info", "control", "set retry cooldown accepted")
        return {"outcome": "completed", "reason": reason, "state_revision": revision,
                "items": item_outcomes}

    def admission_wait_timeout(self) -> float:
        """Return a short wait so eligible retries can fill idle worker slots."""
        timeout = ADMISSION_POLL_SECONDS
        if self.deadline is not None:
            timeout = min(timeout, max(0.0, self.deadline - time.monotonic()))
        if self.control_wake.is_set():
            self.control_wake.clear()
            return 0
        return timeout

    def control_renew_tor_circuits(self, db: sqlite3.Connection,
                                   request: dict[str, object]) -> dict[str, object]:
        """Persist and request NEWNYM without changing acquisition item state."""
        request_id = request.get("request_id")
        session_id = request.get("session_id")
        if not isinstance(request_id, str) or not isinstance(session_id, str):
            return {"outcome": "rejected", "reason": "invalid control request"}
        with self.db_lock:
            existing = db.execute("SELECT outcome, reason, state_revision FROM control_requests "
                                  "WHERE request_id=?", (request_id,)).fetchone()
            if existing:
                outcome, reason, revision = existing
                return {"outcome": outcome, "reason": reason, "state_revision": revision}
        result = self.request_newnym(db, request_id, session_id)
        assert isinstance(result, dict)
        with self.db_lock:
            revision = self.advance_telemetry_revision(db)
            db.execute("INSERT OR IGNORE INTO control_requests "
                       "(request_id, run_id, session_id, action, outcome, reason, "
                       "state_revision, recorded_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                       (request_id, self.run_id, session_id, "renew_tor_circuits",
                       result["outcome"], result["reason"], revision, now()))
            db.commit()
        if self.projection:
            self.projection.set_revision(revision)
        if self.telemetry:
            self.telemetry.request_publish()
        return result

    def control_pause_admission(self, db: sqlite3.Connection,
                               request: dict[str, object]) -> dict[str, object]:
        """Stop new admission after the current scheduler step; active transfers continue."""
        request_id = request.get("request_id")
        session_id = request.get("session_id")
        if not isinstance(request_id, str) or not isinstance(session_id, str):
            return {"outcome": "rejected", "reason": "invalid control request"}
        with self.db_lock:
            existing = db.execute("SELECT outcome, reason, state_revision FROM control_requests "
                                  "WHERE request_id=?", (request_id,)).fetchone()
            if existing:
                outcome, reason, revision = existing
                return {"outcome": outcome, "reason": reason, "state_revision": revision}
            if self.stop_requested.is_set() or self.drain_requested.is_set():
                outcome, reason = "rejected", "admission cannot be paused while the run is stopping"
            elif self.admission_paused.is_set():
                outcome, reason = "rejected", "admission is already paused"
            else:
                self.admission_paused.set()
                outcome, reason = "completed", "admission paused; active transfers continue"
            revision = self.advance_telemetry_revision(db)
            db.execute("INSERT INTO control_requests "
                       "(request_id, run_id, session_id, action, outcome, reason, "
                       "state_revision, recorded_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                       (request_id, self.run_id, session_id, "pause_admission", outcome,
                        reason, revision, now()))
            db.commit()
        if self.projection:
            self.projection.set_revision(revision)
        if outcome == "completed" and self.telemetry:
            self.telemetry.event("info", "control", reason)
        return {"outcome": outcome, "reason": reason, "state_revision": revision}

    def control_resume_admission(self, db: sqlite3.Connection,
                                 request: dict[str, object]) -> dict[str, object]:
        """Reopen admission for the immutable selected run without a restart."""
        request_id = request.get("request_id")
        session_id = request.get("session_id")
        if not isinstance(request_id, str) or not isinstance(session_id, str):
            return {"outcome": "rejected", "reason": "invalid control request"}
        with self.db_lock:
            existing = db.execute("SELECT outcome, reason, state_revision FROM control_requests "
                                  "WHERE request_id=?", (request_id,)).fetchone()
            if existing:
                outcome, reason, revision = existing
                return {"outcome": outcome, "reason": reason, "state_revision": revision}
            if not self.admission_paused.is_set():
                outcome, reason = "rejected", "admission is not paused"
            else:
                self.admission_paused.clear()
                outcome, reason = "completed", "admission resumed for the selected run"
            revision = self.advance_telemetry_revision(db)
            db.execute("INSERT INTO control_requests "
                       "(request_id, run_id, session_id, action, outcome, reason, "
                       "state_revision, recorded_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                       (request_id, self.run_id, session_id, "resume_admission", outcome,
                        reason, revision, now()))
            db.commit()
        if self.projection:
            self.projection.set_revision(revision)
        if outcome == "completed":
            if self.telemetry:
                self.telemetry.event("info", "control", reason)
            self.control_wake.set()
        return {"outcome": outcome, "reason": reason, "state_revision": revision}

    def control_drain_and_stop(self, db: sqlite3.Connection,
                               request: dict[str, object]) -> dict[str, object]:
        """Stop admission and let active transfers reach durable states before exit."""
        request_id = request.get("request_id")
        session_id = request.get("session_id")
        if not isinstance(request_id, str) or not isinstance(session_id, str):
            return {"outcome": "rejected", "reason": "invalid control request"}
        with self.db_lock:
            existing = db.execute("SELECT outcome, reason, state_revision FROM control_requests "
                                  "WHERE request_id=?", (request_id,)).fetchone()
            if existing:
                outcome, reason, revision = existing
                return {"outcome": outcome, "reason": reason, "state_revision": revision}
            if self.stop_requested.is_set() or self.drain_requested.is_set():
                outcome, reason = "rejected", "the run is already stopping"
            else:
                self.drain_requested.set()
                self.admission_paused.clear()
                outcome = "completed"
                reason = "draining active transfers to durable states; admission stopped"
            revision = self.advance_telemetry_revision(db)
            db.execute("INSERT INTO control_requests "
                       "(request_id, run_id, session_id, action, outcome, reason, "
                       "state_revision, recorded_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                       (request_id, self.run_id, session_id, "drain_and_stop", outcome,
                        reason, revision, now()))
            db.commit()
        if self.projection:
            self.projection.set_revision(revision)
        if outcome == "completed":
            if self.telemetry:
                self.telemetry.set_lifecycle("stopping", "drain and stop requested by operator")
                self.telemetry.event("info", "control", reason)
            self.control_wake.set()
        return {"outcome": outcome, "reason": reason, "state_revision": revision}

    def control_checkpoint_stop(self, db: sqlite3.Connection,
                                request: dict[str, object]) -> dict[str, object]:
        """Checkpoint and terminate active engines safely, then exit."""
        request_id = request.get("request_id")
        session_id = request.get("session_id")
        if not isinstance(request_id, str) or not isinstance(session_id, str):
            return {"outcome": "rejected", "reason": "invalid control request"}
        with self.db_lock:
            existing = db.execute("SELECT outcome, reason, state_revision FROM control_requests "
                                  "WHERE request_id=?", (request_id,)).fetchone()
            if existing:
                outcome, reason, revision = existing
                return {"outcome": outcome, "reason": reason, "state_revision": revision}
            if self.stop_requested.is_set():
                outcome, reason = "rejected", "the run is already stopping"
            else:
                self.stop_requested.set()
                outcome = "completed"
                reason = "checkpoint requested; terminating active transfers safely"
            revision = self.advance_telemetry_revision(db)
            db.execute("INSERT INTO control_requests "
                       "(request_id, run_id, session_id, action, outcome, reason, "
                       "state_revision, recorded_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                       (request_id, self.run_id, session_id, "checkpoint_stop", outcome,
                        reason, revision, now()))
            db.commit()
        if self.projection:
            self.projection.set_revision(revision)
        if outcome == "completed":
            if self.telemetry:
                self.telemetry.set_lifecycle("stopping", "checkpoint and stop requested by operator")
                self.telemetry.event("info", "control", reason)
            self.control_wake.set()
        return {"outcome": outcome, "reason": reason, "state_revision": revision}

    def request_newnym(self, db: sqlite3.Connection | None = None,
                       request_id: str | None = None, session_id: str | None = None) -> bool | dict[str, object]:
        """Request fresh circuits, with a shared durable success rate limit."""
        interval = getattr(self.args, "tor_newnym_interval", 60)
        if interval == 0:
            return {"outcome": "rejected", "reason": "Tor circuit renewal is disabled"} if db else False
        audit_id = request_id or str(uuid.uuid4())
        audit_session = session_id or (self.telemetry.session_id if self.telemetry else "automatic")
        with self.newnym_lock:
            current = time.time()
            previous = None
            if db:
                with self.db_lock:
                    existing = db.execute("SELECT outcome, failure_reason FROM tor_renewals "
                                          "WHERE request_id=?", (audit_id,)).fetchone()
                    if existing:
                        outcome, reason = existing
                        return {"outcome": outcome,
                                "reason": reason or "Tor accepted a request for new future streams"}
                    previous = db.execute("SELECT MAX(successful_at) FROM tor_renewals").fetchone()[0]
                    if previous is not None and current < previous + interval:
                        remaining = int(previous + interval - current + 0.999)
                        db.execute("INSERT INTO tor_renewals "
                                   "(request_id, run_id, session_id, requested_at, interval_seconds, "
                                   "previous_success_at, outcome, failure_reason) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                                   (audit_id, self.run_id, audit_session, now(), interval, previous,
                                    "rejected", f"renewal available in {remaining} seconds"))
                        db.commit()
                        return {"outcome": "rejected", "reason": f"renewal available in {remaining} seconds"}
                    db.execute("INSERT INTO tor_renewals "
                               "(request_id, run_id, session_id, requested_at, interval_seconds, "
                               "previous_success_at, outcome) VALUES (?, ?, ?, ?, ?, ?, ?)",
                               (audit_id, self.run_id, audit_session, now(), interval, previous, "pending"))
                    db.commit()
            elif current < self.next_newnym_at:
                return False
        try:
            send_tor_newnym(self.args.tor_control_address, self.args.tor_control_cookie)
        except RuntimeError as exc:
            if db:
                with self.db_lock:
                    db.execute("UPDATE tor_renewals SET outcome='failed', failure_reason=? WHERE request_id=?",
                               (str(exc)[:512], audit_id))
                    db.commit()
            print(f"[tor] circuit renewal failed: {exc}", file=sys.stderr, flush=True)
            return {"outcome": "failed", "reason": str(exc)[:512]} if db else False
        with self.newnym_lock:
            self.next_newnym_at = current + interval
        if db:
            with self.db_lock:
                db.execute("UPDATE tor_renewals SET outcome='completed', successful_at=?, "
                           "next_eligible_at=? WHERE request_id=?",
                           (current, current + interval, audit_id))
                db.commit()
        print("[tor] requested fresh circuits", flush=True)
        if db:
            return {"outcome": "completed",
                    "reason": "Tor accepted a request for new future streams"}
        return True

    def open_db(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.state / "manifest.sqlite", timeout=60,
                             check_same_thread=False)
        db.execute(SCHEMA)
        db.executescript(RUN_SCHEMA)
        columns = {row[1] for row in db.execute("PRAGMA table_info(downloads)")}
        for column in ("storage_path", "staging_path", "inventory_size",
                       "promotion_target", "promotion_intent_at",
                       "cleanup_completed_at", "review_code",
                       "remediation_reason", "remediation_mapping_version",
                       "remediation_outcome", "remediation_started_at",
                       "remediation_completed_at"):
            if column not in columns:
                db.execute(f"ALTER TABLE downloads ADD COLUMN {column} TEXT")
        if "next_retry_at" not in columns:
            db.execute("ALTER TABLE downloads ADD COLUMN next_retry_at REAL NOT NULL DEFAULT 0")
        if "priority" not in columns:
            db.execute("ALTER TABLE downloads ADD COLUMN priority INTEGER NOT NULL DEFAULT 0")
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA busy_timeout=60000")
        return db

    def update(self, db: sqlite3.Connection, query: str, values: tuple) -> None:
        if self.projection:
            raise RuntimeError("snapshot-relevant SQL must use transition")
        try:
            with self.db_lock:
                db.execute(query, values)
                db.commit()
        except sqlite3.Error as exc:
            # A commit error leaves the durable result unknown.  Stop admission
            # at once.  A later controller start reconciles the recorded state.
            self.stop_requested.set()
            raise RuntimeError(f"local SQLite failure: {exc}") from exc

    def advance_telemetry_revision(self, db: sqlite3.Connection) -> int:
        """Increment the durable revision in the active transaction."""
        db.execute("INSERT INTO telemetry_revisions (run_id, revision) VALUES (?, 1) "
                   "ON CONFLICT(run_id) DO UPDATE SET revision=revision+1",
                   (self.run_id,))
        return db.execute("SELECT revision FROM telemetry_revisions WHERE run_id=?",
                          (self.run_id,)).fetchone()[0]

    def apply_projection_change(self, old: dict, new: dict, revision: int,
                                completion_at: str | None = None) -> None:
        """Apply a committed durable change or stop future admission."""
        if not self.projection:
            return
        try:
            self.projection.apply(old, new, revision, completion_at)
        except Exception as exc:
            self.stop_requested.set()
            print(f"[telemetry] projection update failed: {exc}", file=sys.stderr,
                  flush=True)
            raise RuntimeError("telemetry projection update failed") from exc
        if self.telemetry:
            self.telemetry.request_publish()

    def transition(self, db: sqlite3.Connection, url: str, to_status: str,
                   detail: str | None = None, **fields) -> None:
        """Persist a state change and its audit record in one transaction."""
        try:
            with self.db_lock:
                row = db.execute("SELECT status, attempts, bytes, next_retry_at, last_error "
                                 "FROM downloads WHERE url=?", (url,)).fetchone()
                if not row:
                    raise RuntimeError(f"missing download row: {url}")
                old = {"url": url, "status": row[0], "attempts": row[1],
                       "bytes": row[2], "next_retry_at": row[3], "last_error": row[4]}
                recorded_at = now()
                assignments = ["status=?", "updated_at=?"]
                values: list[object] = [to_status, recorded_at]
                for name, value in fields.items():
                    assignments.append(f"{name}=?")
                    values.append(value)
                values.append(url)
                db.execute(f"UPDATE downloads SET {', '.join(assignments)} WHERE url=?", values)
                db.execute("INSERT INTO download_transitions "
                           "(url, run_id, from_status, to_status, detail, recorded_at) "
                           "VALUES (?, ?, ?, ?, ?, ?)",
                           (url, self.run_id, row[0], to_status,
                            detail, recorded_at))
                revision = self.advance_telemetry_revision(db)
                new_row = db.execute("SELECT status, attempts, bytes, next_retry_at, last_error "
                                     "FROM downloads WHERE url=?", (url,)).fetchone()
                db.commit()
            new = {"url": url, "status": new_row[0], "attempts": new_row[1],
                   "bytes": new_row[2], "next_retry_at": new_row[3],
                   "last_error": new_row[4]}
            self.apply_projection_change(old, new, revision,
                                         recorded_at if old["status"] != "complete"
                                         and new["status"] == "complete" else None)
        except sqlite3.Error as exc:
            self.stop_requested.set()
            raise RuntimeError(f"local SQLite failure: {exc}") from exc

    def staging_path(self, url: str) -> Path:
        digest = hashlib.sha256(url.encode()).hexdigest()
        return self.incoming / digest[:2] / digest[2:]

    def ensure_safe_parent(self, target: Path) -> None:
        """Create a destination parent only when no existing component is a symlink."""
        relative = target.relative_to(self.destination)
        current = self.destination
        if current.is_symlink():
            raise RuntimeError("destination must not be a symlink")
        for part in relative.parts[:-1]:
            current = current / part
            try:
                mode = current.lstat().st_mode
            except FileNotFoundError:
                current.mkdir()
                continue
            if os.path.islink(current):
                raise RuntimeError(f"unsafe symlink in destination path: {part}")
            if not os.path.isdir(current):
                raise RuntimeError(f"destination parent is not a directory: {part}")

    @staticmethod
    def flush_file(path: Path) -> None:
        with path.open("rb") as handle:
            os.fsync(handle.fileno())

    @staticmethod
    def flush_directory(path: Path) -> None:
        descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def resolved_storage_target(self, rel: PurePosixPath, url: str) -> tuple[PurePosixPath, Path]:
        """Return the storage path and destination target to use for url.

        Independent controllers share one destination tree and tell an
        already-acquired final apart from a missing one purely by filesystem
        existence. Prefer the current (decoded) path; if nothing sits there,
        fall back to where a pre-fix relative_path() would have placed it, so
        evidence acquired before the decoding fix is still found instead of
        being treated as missing.
        """
        stored = storage_relative(rel, self.destination)
        target = self.destination / Path(stored)
        if not target.exists():
            legacy_rel = legacy_relative_path(url)
            if legacy_rel is not None and legacy_rel != rel:
                legacy_stored = storage_relative(legacy_rel, self.destination)
                legacy_target = self.destination / Path(legacy_stored)
                if legacy_target.exists():
                    return legacy_stored, legacy_target
        return stored, target

    def import_queues(self, db: sqlite3.Connection) -> int:
        count = 0

        def report_rejection(url: str, reason: str) -> None:
            print(f"[queue-rejected] {url}: {reason}", flush=True)

        for url, rel in read_queues(self.args.queue, on_reject=report_rejection):
            stored, _ = self.resolved_storage_target(rel, url)
            db.execute("INSERT OR IGNORE INTO downloads (url, relative_path, "
                       "storage_path, staging_path, updated_at) VALUES (?, ?, ?, ?, ?)",
                       (url, rel.as_posix(), stored.as_posix(),
                        str(self.staging_path(url)), now()))
            db.execute("UPDATE downloads SET storage_path=?, staging_path=?, "
                       "updated_at=? WHERE url=? AND (storage_path IS NULL "
                       "OR storage_path != ? OR staging_path IS NULL)",
                       (stored.as_posix(), str(self.staging_path(url)), now(), url,
                        stored.as_posix()))
            count += 1
        db.commit()
        return count

    def scope_run(self, db: sqlite3.Connection) -> tuple[int, int]:
        """Persist this invocation's bounded selection in queue order.

        ``run_items`` is deliberately populated once, before workers start.
        Retried rows therefore cannot cause a later queue item to enter a
        --max-files run.
        """
        db.execute("INSERT OR IGNORE INTO telemetry_revisions (run_id, revision) VALUES (?, 0)",
                   (self.run_id,))
        previous = db.execute("SELECT COUNT(*) FROM run_items WHERE run_id=?",
                              (self.run_id,)).fetchone()[0]
        if previous:
            db.commit()
            return previous, 0
        selected = existing = 0
        for url, rel in read_queues(self.args.queue):
            if self.args.max_files and selected >= self.args.max_files:
                break
            stored, target = self.resolved_storage_target(rel, url)
            if target.exists():
                existing += 1
                self.transition(db, url, "existing_unverified", "final already exists",
                                bytes=target.stat().st_size)
                continue
            db.execute("INSERT INTO run_items (run_id, url, queue_rank) "
                       "VALUES (?, ?, ?)", (self.run_id, url, selected))
            selected += 1
        self.advance_telemetry_revision(db)
        db.commit()
        return selected, existing

    def dry_run(self) -> int:
        existing = missing = selected = 0
        for url, rel in read_queues(self.args.queue):
            if self.args.max_files and selected >= self.args.max_files:
                break
            stored, target = self.resolved_storage_target(rel, url)
            if target.exists():
                existing += 1
                print(f"EXISTING  {target}")
                continue
            selected += 1
            missing += 1
            print(f"MISSING   {target} <- {url}")
        print(f"Dry run: {existing} existing, {missing} missing")
        return 0

    def next_pending(self, db: sqlite3.Connection):
        query = """SELECT downloads.url, relative_path, storage_path, staging_path, attempts
            FROM downloads JOIN run_items ON run_items.url = downloads.url
            WHERE status IN ('queued', 'retry_wait', 'pending', 'failed')
            AND run_items.run_id = ?
            AND (? = 0 OR attempts < ?)
            AND next_retry_at <= ? ORDER BY run_items.queue_rank LIMIT 1"""
        return db.execute(query, (self.run_id, self.args.max_attempts, self.args.max_attempts,
                                  time.time())).fetchone()

    def retry_wait(self, db: sqlite3.Connection) -> float | None:
        row = db.execute(
            "SELECT MIN(next_retry_at) FROM downloads JOIN run_items "
            "ON run_items.url = downloads.url WHERE run_items.run_id=? "
            "AND status IN ('queued', 'retry_wait', 'pending', 'failed') "
            "AND (? = 0 OR attempts < ?)",
            (self.run_id, self.args.max_attempts, self.args.max_attempts),
        ).fetchone()
        if row[0] is None:
            return None
        return max(0, row[0] - time.time())

    def reset_retry_now(self, db: sqlite3.Connection) -> None:
        """Make persisted retryable selected items eligible immediately."""
        db.execute("UPDATE downloads SET next_retry_at=0 WHERE status IN "
                   "('queued', 'retry_wait', 'pending', 'failed') AND url IN "
                   "(SELECT url FROM run_items WHERE run_id=?)", (self.run_id,))

    def requeue_interrupted_transfers(self, db: sqlite3.Connection) -> None:
        """Return non-terminal selected work from a prior controller to queue."""
        db.execute("UPDATE downloads SET status='queued', next_retry_at=0, "
                   "updated_at=? WHERE status IN ('active', 'running', 'admitted') "
                   "AND url IN (SELECT url FROM run_items WHERE run_id=?)",
                   (now(), self.run_id))

    def remediation_event(self, severity: str, message: str, url: str) -> None:
        """Defer safe-path events until the read-only telemetry publisher exists."""
        if self.telemetry:
            self.telemetry.event(severity, "safe_path", message, url)
        else:
            self.pending_remediation_events.append((severity, message, url))

    def safe_remediation_target(self, relative_text: str) -> tuple[PurePosixPath, Path]:
        """Map a logical evidence path while rejecting paths outside the root."""
        logical = PurePosixPath(relative_text)
        if (logical.is_absolute() or not logical.parts
                or any(part in {"", ".", ".."} for part in logical.parts)):
            raise RuntimeError("unsafe logical path for safe-path remediation")
        stored = storage_relative(logical, self.destination)
        target = self.destination / Path(stored)
        try:
            target.relative_to(self.destination)
        except ValueError as exc:
            raise RuntimeError("mapped path escapes destination") from exc
        return stored, target

    def remediate_safe_paths(self, db: sqlite3.Connection) -> int:
        """Promote verified selected legacy ENAMETOOLONG staging artifacts.

        Every other review cause remains untouched.  A failed remediation records
        a terminal manual outcome, preventing automatic retries on later starts.
        """
        rows = db.execute(
            "SELECT downloads.url, downloads.relative_path, downloads.staging_path, "
            "downloads.sha256, downloads.review_code, downloads.last_error "
            "FROM downloads JOIN run_items ON run_items.url=downloads.url "
            "WHERE run_items.run_id=? AND downloads.status='review_required' "
            "AND downloads.remediation_outcome IS NULL",
            (self.run_id,),
        ).fetchall()
        resolved = 0
        for url, relative_text, staging_text, digest, review_code, last_error in rows:
            if review_code != "ENAMETOOLONG" and not legacy_name_too_long_error(last_error):
                continue
            staging = Path(staging_text) if staging_text else None
            try:
                if not staging or staging != self.staging_path(url):
                    raise RuntimeError("staging path is outside the approved incoming tree")
                if not stat.S_ISREG(staging.lstat().st_mode):
                    raise RuntimeError("staging is not a regular file")
                if not digest:
                    raise RuntimeError("recorded staging digest is absent")
                if sha256sum(staging) != digest:
                    raise RuntimeError("staging digest does not match recorded digest")
                stored, target = self.safe_remediation_target(relative_text)
                self.ensure_safe_parent(target)
                self.flush_file(staging)
            except (OSError, RuntimeError) as exc:
                self.transition(db, url, "review_required", str(exc),
                                last_error=str(exc), remediation_reason=
                                "automatic safe-path remediation",
                                remediation_mapping_version=SAFE_PATH_MAPPING_VERSION,
                                remediation_outcome="manual_error")
                self.remediation_event("warning", "safe-path remediation requires manual review", url)
                continue
            self.transition(db, url, "promoting", "automatic safe-path remediation intent",
                            storage_path=stored.as_posix(), promotion_target=str(target),
                            promotion_intent_at=now(), remediation_reason=
                            "automatic safe-path remediation",
                            remediation_mapping_version=SAFE_PATH_MAPPING_VERSION,
                            review_code="ENAMETOOLONG",
                            remediation_started_at=now())
            try:
                os.link(staging, target)
                self.flush_directory(target.parent)
            except FileExistsError:
                self.transition(db, url, "review_required", "mapped destination already exists",
                                last_error="mapped destination already exists",
                                remediation_outcome="manual_collision")
                self.remediation_event("warning", "safe-path remediation found an existing final", url)
                continue
            except OSError as exc:
                self.transition(db, url, "review_required", str(exc), last_error=str(exc),
                                remediation_outcome="manual_error")
                self.remediation_event("warning", "safe-path remediation requires manual review", url)
                continue
            self.finalized_event(url, relative_text, stored.as_posix(), target.stat().st_size, digest)
            self.transition(db, url, "complete", "automatic safe-path remediation complete",
                            bytes=target.stat().st_size, sha256=digest,
                            remediation_outcome="complete",
                            remediation_completed_at=now())
            os.unlink(staging)
            self.transition(db, url, "complete", "staging cleanup complete",
                            cleanup_completed_at=now())
            self.remediation_event("info", "safe-path remediation complete", url)
            print(f"[complete] safe-path remediation: {relative_text}", flush=True)
            resolved += 1
        return resolved

    def write_manifest(self) -> None:
        temporary = self.manifest_path.with_suffix(".tmp")
        temporary.write_text(json.dumps(self.manifest, indent=2) + "\n")
        os.replace(temporary, self.manifest_path)

    def record_worker(self, url: str, pid: int) -> None:
        with self.active_lock:
            self.manifest["workers"].append({"url": url, "torsocks_pid": pid})
            self.write_manifest()
        if self.telemetry:
            self.telemetry.set_active(url, self.worker_ids[url],
                                      self.active_attempts[url], "downloading", pid)

    def reserve_worker_id(self) -> int:
        """Return a reusable controller slot for a newly admitted transfer."""
        used = set(self.worker_ids.values())
        for worker_id in range(1, self.args.workers + 1):
            if worker_id not in used:
                return worker_id
        raise RuntimeError("no worker slot available for admitted transfer")

    def telemetry_snapshot(self, lifecycle: str,
                           reason: str | None, active: list[dict], validation: list[dict],
                           events: list[dict]) -> dict:
        """Build a bounded view from copied projection and runtime state."""
        if not self.projection:
            raise RuntimeError("telemetry projection is unavailable")
        durable = self.projection.copy()
        active_urls = {row["url"] for row in active}
        workers = []
        current = time.monotonic()
        for row in active:
            row = dict(row)
            source_url = row.pop("url")
            row["item_id"] = hashlib.sha256(source_url.encode()).hexdigest()
            row["basename"] = unquote(PurePosixPath(urlsplit(source_url).path).name)
            row["phase_age_s"] = max(0.0, current - row.pop("phase_started"))
            row["resume_baseline_bytes"] = None
            progress_at = row.pop("last_progress_monotonic", None)
            row["last_progress_age_s"] = (max(0.0, current - progress_at)
                                          if progress_at is not None else None)
            row["eta_seconds"] = estimate_eta_seconds(row.get("received_bytes"),
                                                       row.get("total_bytes"),
                                                       row.get("speed_bps"))
            workers.append(row)
        active_by_worker = {row["worker_id"] for row in workers}
        for worker_id in range(1, self.args.workers + 1):
            if worker_id not in active_by_worker:
                workers.append({"worker_id": worker_id, "item_id": None,
                                "generation": None, "attempt_id": None,
                                "attempt_number": None, "phase": "idle", "reason": None,
                                "phase_age_s": None, "engine_instance_id": None,
                                "engine_job_id": None, "pid": None, "sample_age_s": None,
                                "received_bytes": None, "total_bytes": None,
                                "total_source": "unavailable", "resume_baseline_bytes": None,
                                "speed_bps": None, "last_progress_age_s": None,
                                "sample_sequence": None, "connections": None,
                                "eta_seconds": None})
        workers.sort(key=lambda row: row["worker_id"])
        destination_usage = shutil.disk_usage(self.destination)
        state_usage = shutil.disk_usage(self.state)
        destination_device = self.destination.stat().st_dev
        state_device = self.state.stat().st_dev
        last_payload_progress_at, completed_at, stopped_at = self.telemetry.time_status_copy()
        filesystems = []
        for device, roles, usage in (
                (destination_device, ["destination"], destination_usage),
                (state_device, ["state"], state_usage)):
            existing = next((row for row in filesystems
                             if row["filesystem_id"] == str(device)), None)
            if existing is not None:
                existing["roles"].extend(roles)
                continue
            filesystems.append({"filesystem_id": str(device), "roles": roles,
                                "free_bytes": usage.free,
                                "reserve_bytes": self.args.reserve_bytes,
                                "headroom_bytes": usage.free - self.args.reserve_bytes})
        return {
            "state_revision": durable["revision"],
            "run": {"lifecycle": lifecycle, "reason": reason,
                    "created_at": self.manifest.get("started_at"),
                    "session_started_at": self.manifest.get("resumed_at",
                                                            self.manifest.get("started_at")),
                    "selected_count": durable["selected_count"], "counts": durable["counts"],
                    "skipped_existing_count": durable["skipped_existing"],
                    "last_completion_at": durable["last_completion_at"],
                    "last_payload_progress_at": last_payload_progress_at,
                    "completed_at": completed_at,
                    "stopped_at": stopped_at,
                    "engine_selection_status": (
                        "aria2 loopback RPC counters enabled"
                        if getattr(self.args, "aria2_rpc", False)
                        else "aria2 counters unavailable"
                    ),
                    "metrics": reduce_projected_metrics(
                        durable["selected_count"], durable["counts"],
                        durable["complete_bytes"], active,
                        self.telemetry.started_monotonic, current),
                    "remaining_run_time_s": (max(0.0, self.deadline - current)
                                             if self.deadline is not None else None)},
            "workers": workers,
            "validation": validation,
            "health": {"filesystems": filesystems,
                       "filesystem_layout_error": (
                           None if destination_device == state_device else
                           "destination and state use different filesystems"),
                       "tor_preflight": self.manifest.get("tor_isolation_preflight"),
                       "cooldown_remaining_s": max(0.0, self.cooldown_until - time.time()),
                       "retry_eligible_at": (dt.datetime.fromtimestamp(
                           durable["retry_at"], dt.timezone.utc).isoformat(timespec="seconds")
                                             if durable["retry_at"]
                                             and durable["retry_at"] > time.time() else None),
                       "retry_remaining_s": max(0.0, durable["retry_at"] - time.time())
                       if durable["retry_at"] is not None else None,
                       "retry_pending_count": durable["retry_count"],
                       "retry_error": durable["retry_error"],
                       "telemetry_errors": self.telemetry.errors_copy(),
                       "active_without_counters": len(active_urls)},
            "recent_events": events,
        }

    def wait_for_cooldown(self) -> None:
        while True:
            with self.cooldown_lock:
                remaining = self.cooldown_until - time.time()
            if remaining <= 0:
                return
            print(f"[cooldown] SOCKS5 failure; waiting {max(1, round(remaining))}s",
                  flush=True)
            time.sleep(min(remaining, 1))

    def wait_for_start_slot(self) -> None:
        """Stagger process starts without reducing the number of active workers."""
        with self.start_lock:
            delay = self.next_worker_start - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            self.next_worker_start = time.monotonic() + self.args.worker_stagger

    def run_aria2(self, url: str, staging: Path, attempt: int) -> tuple[bool, str]:
        staging.parent.mkdir(parents=True, exist_ok=True)
        name = hashlib.sha256(url.encode()).hexdigest()[:12]
        log_path = self.run_dir / f"aria2-{name}-{attempt}.log"
        rpc_port = allocate_loopback_port() if getattr(self.args, "aria2_rpc", False) else None
        rpc_secret = uuid.uuid4().hex if rpc_port else None
        command = [
            self.args.torsocks, "-i", self.args.aria2c, "--dir", str(staging.parent),
            "--out", staging.name, "--continue=true", "--allow-overwrite=false",
            "--auto-file-renaming=false", "--max-concurrent-downloads=1", "--split=1",
            "--max-connection-per-server=1", "--file-allocation=none", "--max-tries=1",
            "--async-dns=false",
            f"--connect-timeout={self.args.connect_timeout}",
            f"--timeout={self.args.timeout}", url,
        ]
        if rpc_port:
            command.extend(("--enable-rpc=true", "--rpc-listen-all=false",
                            "--disable-ipv6=true", f"--rpc-listen-port={rpc_port}",
                            f"--rpc-secret={rpc_secret}"))
        with log_path.open("w", encoding="utf-8") as log:
            process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT,
                                       text=True)
            self.record_worker(url, process.pid)
            next_sample = 0.0
            rpc_gid = None
            rpc_outcome = None
            while process.poll() is None:
                if rpc_port and time.monotonic() >= next_sample:
                    sample = aria2_rpc_status(rpc_port, rpc_secret)
                    if sample and self.telemetry:
                        def counter(name):
                            value = sample.get(name)
                            return int(value) if isinstance(value, str) and value.isdecimal() else None
                        self.telemetry.update_sample(url, counter("completedLength"),
                                                     counter("totalLength"),
                                                     counter("downloadSpeed"),
                                                     counter("connections"))
                    if sample:
                        candidate_gid = sample.get("gid")
                        if isinstance(candidate_gid, str):
                            rpc_gid = candidate_gid
                    rpc_status = (aria2_rpc_job_status(rpc_port, rpc_secret, rpc_gid)
                                  if sample or rpc_gid else None)
                    terminal_status = rpc_status or aria2_log_terminal_status(log_path)
                    if terminal_status:
                        rpc_outcome = terminal_status == "complete"
                        # With --enable-rpc aria2 remains available after its only
                        # job ends. It has no further acquisition work, so stop the
                        # local engine and let the controller promote or retry.
                        process.terminate()
                        try:
                            process.wait(timeout=10)
                        except subprocess.TimeoutExpired:
                            process.kill()
                            process.wait()
                        break
                    next_sample = time.monotonic() + 1
                if self.stop_requested.is_set() or (
                        self.deadline is not None and time.monotonic() >= self.deadline):
                    self.stop_requested.set()
                    process.terminate()
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
                    return False, "run time limit or stop requested"
                time.sleep(0.2)
            code = process.returncode
        lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
        diagnostic = next((line for line in reversed(lines) if "errorCode=" in line), None)
        if rpc_outcome is not None:
            return rpc_outcome, (diagnostic or f"aria2 RPC job {'complete' if rpc_outcome else 'failed'}")
        return code == 0, (diagnostic or lines[-1] if lines else f"aria2 exited {code}")

    def move_candidate(self, staging: Path) -> Path:
        self.candidates.mkdir(parents=True, exist_ok=True)
        while True:
            candidate = self.candidates / f"{staging.name}.{uuid.uuid4().hex}"
            try:
                os.link(staging, candidate)
            except FileExistsError:
                continue
            break
        os.unlink(staging)
        control = Path(str(staging) + ".aria2")
        if control.exists():
            os.replace(control, Path(str(candidate) + ".aria2"))
        return candidate

    def transfer(self, row, db: sqlite3.Connection) -> str:
        url, rel_text, stored_text, staging_text, attempts = row
        rel = PurePosixPath(rel_text)
        stored_text = stored_text or storage_relative(rel, self.destination).as_posix()
        staging_text = staging_text or str(self.staging_path(url))
        target = self.destination / Path(stored_text)
        staging = Path(staging_text)
        if target.exists():
            self.transition(db, url, "existing_unverified", "final already exists",
                            bytes=target.stat().st_size)
            return "existing"
        # Reserve the controller slot before a shared cooldown or stagger wait.
        # This makes the published worker state agree with the durable
        # ``admitted`` count instead of falsely rendering occupied slots idle.
        with self.active_lock:
            self.active[url] = staging
            worker_id = self.reserve_worker_id()
            self.worker_ids[url] = worker_id
            self.active_attempts[url] = attempts + 1
        if self.telemetry:
            self.telemetry.set_active(url, worker_id, attempts + 1, "cooldown")
            self.telemetry.event("info", "attempt", "transfer admitted", url, worker_id)
        self.wait_for_cooldown()
        self.wait_for_start_slot()
        self.wait_for_cooldown()
        attempt_started_at = utc_now()
        self.transition(db, url, "active", attempts=attempts + 1)
        self.record_attempt_start(db, url, attempts + 1, attempt_started_at)
        if self.telemetry:
            self.telemetry.update_phase(url, "connecting")
        try:
            ok, error = self.run_aria2(url, staging, attempts + 1)
        except BaseException:
            self.clear_transfer_telemetry(url)
            raise
        control = Path(str(staging) + ".aria2")
        if not ok or not staging.exists() or control.exists():
            size = staging.stat().st_size if staging.exists() else 0
            if staging.exists() and size > 0 and is_incomplete_body_failure(error):
                self.attempt_event(db, url, attempts + 1, attempt_started_at,
                                   "validation_failed")
                candidate = self.move_candidate(staging)
                self.candidate_event(url, staging, candidate, None, "incomplete_body")
                self.transition(db, url, "review_required", error,
                                bytes=candidate.stat().st_size, last_error=error)
                self.clear_transfer_telemetry(url)
                print(f"[review] {rel}: incomplete transfer: {error}", flush=True)
                return "review"
            if is_connectivity_failure(error):
                with self.cooldown_lock:
                    self.cooldown_until = max(self.cooldown_until,
                                              time.time() + self.args.socks_backoff)
                renewal = self.request_newnym(db)
                renewed = isinstance(renewal, dict) and renewal.get("outcome") == "completed"
                if renewed and self.telemetry:
                    self.telemetry.event("info", "tor", "requested fresh Tor circuits",
                                         url, worker_id)
            delay = RETRY_DELAYS[min(attempts, len(RETRY_DELAYS) - 1)]
            outcome = "stopped" if self.stop_requested.is_set() else "retryable_failure"
            self.attempt_event(db, url, attempts + 1, attempt_started_at, outcome)
            self.transition(db, url, "retry_wait", error, bytes=size,
                            last_error=error, next_retry_at=time.time() + delay)
            if self.telemetry:
                self.telemetry.event("warning", "retry", error, url, worker_id)
            self.clear_transfer_telemetry(url)
            print(f"[failed] {rel}: {error}", flush=True)
            return "failed"
        try:
            is_regular_staging = stat.S_ISREG(staging.lstat().st_mode)
        except FileNotFoundError:
            is_regular_staging = False
        if not is_regular_staging:
            self.attempt_event(db, url, attempts + 1, attempt_started_at, "validation_failed")
            self.transition(db, url, "review_required", "staging is not a regular file",
                            bytes=0,
                            last_error="non-regular staging file")
            self.clear_transfer_telemetry(url)
            return "review"
        if self.telemetry:
            self.telemetry.set_active(url, worker_id, attempts + 1, "hashing")
            self.telemetry.set_validation(url, 0, staging.stat().st_size)
        try:
            digest = sha256sum(
                staging,
                (lambda processed: self.telemetry.set_validation(
                    url, processed, staging.stat().st_size)) if self.telemetry else None,
            )
        except BaseException:
            self.clear_transfer_telemetry(url)
            raise
        if self.telemetry:
            self.telemetry.clear_validation(url)
        self.attempt_event(db, url, attempts + 1, attempt_started_at, "success")
        try:
            self.ensure_safe_parent(target)
            self.flush_file(staging)
            self.transition(db, url, "promoting", sha256=digest,
                            bytes=staging.stat().st_size, promotion_target=str(target),
                            promotion_intent_at=now())
            hit_failpoint("post_validation_intent")
        except (OSError, RuntimeError) as exc:
            self.transition(db, url, "review_required", str(exc), sha256=digest,
                            bytes=staging.stat().st_size, last_error=str(exc),
                            review_code=(review_code_for_error(exc)
                                         if isinstance(exc, OSError) else None))
            self.clear_transfer_telemetry(url)
            return "review"
        try:
            # link(2) exclusively creates target, closing the check/promote race.
            os.link(staging, target)
        except FileExistsError:
            candidate = self.move_candidate(staging)
            self.candidate_event(url, staging, candidate, digest, "destination_collision")
            self.transition(db, url, "review_required", "destination appeared during promotion",
                            bytes=candidate.stat().st_size, promotion_target=str(target))
            self.clear_transfer_telemetry(url)
            print(f"[candidate] {rel}", flush=True)
            return "candidate"
        except OSError as exc:
            self.transition(db, url, "review_required", str(exc), sha256=digest,
                            bytes=staging.stat().st_size, last_error=str(exc),
                            promotion_target=str(target),
                            review_code=review_code_for_error(exc))
            self.clear_transfer_telemetry(url)
            print(f"[review] {rel}: promotion failed: {exc}", flush=True)
            return "review"
        hit_failpoint("post_final_file_creation")
        self.flush_directory(target.parent)
        self.finalized_event(url, rel.as_posix(), stored_text, target.stat().st_size, digest)
        self.clear_transfer_telemetry(url)
        self.transition(db, url, "complete", bytes=target.stat().st_size, sha256=digest)
        hit_failpoint("post_completion_commit")
        os.unlink(staging)
        self.transition(db, url, "complete", "staging cleanup complete",
                        cleanup_completed_at=now())
        if self.telemetry:
            self.telemetry.event("info", "complete", "transfer complete", url, worker_id)
        print(f"[complete] {rel} ({target.stat().st_size:,} bytes)", flush=True)
        return "complete"

    def clear_transfer_telemetry(self, url: str) -> None:
        with self.active_lock:
            self.active.pop(url, None)
            self.worker_ids.pop(url, None)
            self.active_attempts.pop(url, None)
        if self.telemetry:
            self.telemetry.clear_validation(url)
            self.telemetry.clear_active(url)

    def progress_loop(self, stop: threading.Event) -> None:
        while not stop.wait(self.args.progress_interval):
            with self.active_lock:
                active = len(self.active)
            print(f"[progress] active={active} transfer counters unavailable", flush=True)

    def reconcile_promotions(self, db: sqlite3.Connection) -> None:
        """Resolve a durable promotion intent left by a killed supervisor."""
        rows = db.execute("SELECT url, relative_path, storage_path, staging_path, promotion_target, sha256 "
                          "FROM downloads WHERE status='promoting'").fetchall()
        for url, relative_text, stored_text, staging_text, target_text, digest in rows:
            staging = Path(staging_text) if staging_text else None
            target = Path(target_text) if target_text else None
            try:
                target_matches = bool(target and self.is_safe_final_path(target) and digest
                                      and sha256sum(target) == digest)
            except OSError as exc:
                self.transition(db, url, "review_required",
                                "promotion target is inaccessible",
                                last_error=f"promotion reconciliation failed: {exc}",
                                review_code=review_code_for_error(exc))
                continue
            if target_matches:
                stored = stored_text or Path(target).relative_to(self.destination).as_posix()
                self.finalized_event(url, relative_text, stored, target.stat().st_size, digest)
                self.transition(db, url, "complete", "reconciled promotion intent",
                                bytes=target.stat().st_size, sha256=digest)
                if staging and staging.exists() and staging.is_file():
                    os.unlink(staging)
                    self.transition(db, url, "complete", "reconciled staging cleanup",
                                    cleanup_completed_at=now())
                continue
            if (target and digest and staging and self.is_safe_staging_path(staging)
                    and self.is_promotable_final_path(target)):
                try:
                    staging_is_regular = stat.S_ISREG(staging.lstat().st_mode)
                except OSError:
                    staging_is_regular = False
                if staging_is_regular:
                    try:
                        os.link(staging, target)
                    except OSError as exc:
                        self.transition(db, url, "review_required",
                                        "promotion target could not be recreated",
                                        last_error=f"promotion reconciliation failed: {exc}",
                                        review_code=review_code_for_error(exc))
                        continue
                    self.flush_directory(target.parent)
                    stored = stored_text or Path(target).relative_to(self.destination).as_posix()
                    self.finalized_event(url, relative_text, stored, target.stat().st_size, digest)
                    self.transition(db, url, "complete", "reconciled promotion intent",
                                    bytes=target.stat().st_size, sha256=digest)
                    os.unlink(staging)
                    self.transition(db, url, "complete", "reconciled staging cleanup",
                                    cleanup_completed_at=now())
                    continue
            self.transition(db, url, "review_required", "incomplete or inconsistent promotion",
                            last_error="promotion reconciliation requires review")

    def is_promotable_final_path(self, target: Path) -> bool:
        """Return true only for a safe final path below destination with nothing at it yet."""
        try:
            relative = target.relative_to(self.destination)
            if not relative.parts:
                return False
            current = self.destination
            if os.path.islink(current):
                return False
            for part in relative.parts[:-1]:
                current = current / part
                if os.path.islink(current) or not current.is_dir():
                    return False
            target.lstat()
            return False
        except FileNotFoundError:
            return True
        except (OSError, ValueError):
            return False

    def is_safe_final_path(self, target: Path) -> bool:
        """Return true only for a regular final below a non-symlink destination path."""
        try:
            relative = target.relative_to(self.destination)
            if not relative.parts:
                return False
            current = self.destination
            if os.path.islink(current):
                return False
            for part in relative.parts[:-1]:
                current = current / part
                if os.path.islink(current) or not current.is_dir():
                    return False
            return stat.S_ISREG(target.lstat().st_mode)
        except (OSError, ValueError):
            return False

    def cleanup_completed_staging(self, db: sqlite3.Connection) -> None:
        """Finish idempotent staging cleanup after a committed final completion."""
        rows = db.execute(
            "SELECT downloads.url, downloads.staging_path FROM downloads JOIN run_items "
            "ON run_items.url=downloads.url WHERE run_items.run_id=? "
            "AND downloads.status='complete' AND downloads.cleanup_completed_at IS NULL",
            (self.run_id,),
        ).fetchall()
        for url, staging_text in rows:
            staging = Path(staging_text) if staging_text else self.staging_path(url)
            try:
                if not self.is_safe_staging_path(staging):
                    self.transition(db, url, "review_required",
                                    "completed staging path is unsafe",
                                    last_error="completed staging cleanup requires review")
                    continue
                try:
                    mode = staging.lstat().st_mode
                except FileNotFoundError:
                    mode = None
                if mode is not None:
                    if not stat.S_ISREG(mode):
                        self.transition(db, url, "review_required",
                                        "completed staging is not a regular file",
                                        last_error="completed staging cleanup requires review")
                        continue
                    os.unlink(staging)
                self.transition(db, url, "complete", "reconciled staging cleanup",
                                cleanup_completed_at=now())
            except OSError as exc:
                self.transition(db, url, "review_required",
                                "completed staging cleanup failed", last_error=str(exc),
                                review_code=review_code_for_error(exc))

    def is_safe_staging_path(self, staging: Path) -> bool:
        """Return true only for a path below the non-symlink staging directory."""
        try:
            relative = staging.relative_to(self.incoming)
            if not relative.parts or os.path.islink(self.incoming):
                return False
            current = self.incoming
            for part in relative.parts[:-1]:
                current = current / part
                if os.path.islink(current) or not current.is_dir():
                    return False
            return True
        except (OSError, ValueError):
            return False

    def has_storage_reserve(self) -> bool:
        return shutil.disk_usage(self.destination).free >= self.args.reserve_bytes

    def run(self) -> int:
        self.destination.mkdir(parents=True, exist_ok=True)
        self.state.mkdir(parents=True, exist_ok=True)
        if self.destination.stat().st_dev != self.state.stat().st_dev:
            raise RuntimeError("destination and state must share a filesystem")
        self.incoming.mkdir(parents=True, exist_ok=True)
        self.candidates.mkdir(parents=True, exist_ok=True)
        self.run_dir.mkdir(parents=True, exist_ok=True)
        lock = (self.state / "acquisition.lock").open("a+")
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("another acquisition supervisor holds the lock") from exc
        try:
            ports = verify_tor_isolation(self.args.tor_control_address,
                                         self.args.tor_control_cookie)
            queue_inputs = [
                {"path": str(path.resolve()), "sha256": sha256sum(path)}
                for path in self.args.queue
            ]
            self.queue_inputs = queue_inputs
            if self.manifest_path.exists():
                self.manifest = json.loads(self.manifest_path.read_text(encoding="utf-8"))
                if self.manifest.get("queue_inputs") != queue_inputs:
                    raise RuntimeError("run ID is bound to different queue inputs")
                if self.manifest.get("selection") != {"max_files": self.args.max_files}:
                    raise RuntimeError("run ID is bound to different selection settings")
                self.manifest["resumed_at"] = now()
            else:
                self.manifest = {"run_id": self.run_id, "started_at": now(),
                                 "queue_inputs": queue_inputs,
                                 "selection": {"max_files": self.args.max_files},
                                 "workers": []}
            self.manifest["tor_isolation_preflight"] = {"socks_ports": ports}
            self.write_manifest()
            db = self.open_db()
            print(f"Imported {self.import_queues(db):,} queue entries")
            selected, existing = self.scope_run(db)
            self.selected_item_count = selected
            print(f"Scoped this run to {selected:,} transfer entries; "
                  f"skipped {existing:,} existing finals")
            self.provenance = ProvenanceWriter(
                self.state, self.run_id, getattr(self.args, "provenance_signing_key", None)
            )
            self.provenance_event(
                "run_started", queue_input_digests=queue_inputs,
                selection_settings={"max_files": self.args.max_files},
                selected_item_count=selected,
            )
            self.reconcile_promotions(db)
            self.cleanup_completed_staging(db)
            resolved = self.remediate_safe_paths(db)
            if resolved:
                print(f"Resolved {resolved} safe-path review item(s)", flush=True)
            self.requeue_interrupted_transfers(db)
            if self.args.retry_now:
                self.reset_retry_now(db)
            db.commit()
            self.projection = TelemetryStateProjection.hydrate(
                db, self.run_id, self.args.max_attempts
            )
            self.deadline = (time.monotonic() + self.args.time_limit
                             if self.args.time_limit else None)
            self.telemetry = TelemetryPublisher(self.state, self.run_id, self.args.workers)
            self.telemetry.start(
                lambda lifecycle, reason, active, validation, events: self.telemetry_snapshot(
                    lifecycle, reason, active, validation, events)
            )
            control = ControlServer(self.state, self.run_id, self.telemetry.session_id,
                                    self.control_state,
                                    lambda request: self.control_action(db, request))
            control.start()
            inspection = InspectionServer(
                self.state, self.run_id, self.telemetry.session_id,
                self.state / "manifest.sqlite", self.args.max_attempts,
                self.telemetry.runtime_copy,
            )
            inspection.start()
            for severity, message, url in self.pending_remediation_events:
                self.telemetry.event(severity, "safe_path", message, url)
            self.pending_remediation_events.clear()
            self.telemetry.set_lifecycle("running")
            stop = threading.Event()
            progress = threading.Thread(target=self.progress_loop, args=(stop,), daemon=True)
            progress.start()
            results: dict[str, int] = {}
            try:
                with concurrent.futures.ThreadPoolExecutor(
                    max_workers=self.args.workers
                ) as pool:
                    futures: set[concurrent.futures.Future[str]] = set()
                    while True:
                        while (not self.stop_requested.is_set() and not self.drain_requested.is_set()
                               and not self.admission_paused.is_set()
                               and len(futures) < self.args.workers):
                            if self.deadline is not None and time.monotonic() >= self.deadline:
                                self.stop_requested.set()
                                self.telemetry.set_lifecycle("stopping", "run time limit reached")
                                print("[stopped] run time limit reached", flush=True)
                                break
                            if not self.has_storage_reserve():
                                self.stop_requested.set()
                                self.telemetry.set_lifecycle("stopping", "free-space reserve reached")
                                print("[stopped] free-space reserve reached", flush=True)
                                break
                            row = self.next_pending(db)
                            if row is None:
                                break
                            self.transition(db, row[0], "admitted", "transfer admitted")
                            futures.add(pool.submit(self.transfer, row, db))
                        if futures:
                            done, futures = concurrent.futures.wait(
                                futures,
                                timeout=self.admission_wait_timeout(),
                                return_when=concurrent.futures.FIRST_COMPLETED,
                            )
                            if (not done and self.deadline is not None
                                    and time.monotonic() >= self.deadline):
                                self.stop_requested.set()
                                continue
                            for future in done:
                                status = future.result()
                                results[status] = results.get(status, 0) + 1
                            continue
                        if self.stop_requested.is_set() or self.drain_requested.is_set():
                            break
                        wait = self.retry_wait(db)
                        if wait is None:
                            break
                        print(f"[retry] no eligible URL; waiting {max(1, round(wait))}s",
                              flush=True)
                        sleep_for = min(wait, 60)
                        if self.deadline is not None:
                            sleep_for = min(sleep_for,
                                            max(0, self.deadline - time.monotonic()))
                        if sleep_for <= 0:
                            self.stop_requested.set()
                        else:
                            self.control_wake.wait(sleep_for)
                            self.control_wake.clear()
            finally:
                stop.set()
                progress.join()
                self.manifest["finished_at"] = now()
                self.write_manifest()
                with self.db_lock:
                    unresolved = db.execute(
                        "SELECT COUNT(*) FROM downloads JOIN run_items "
                        "ON run_items.url = downloads.url WHERE run_items.run_id=? "
                        "AND status NOT IN ('complete', 'existing')", (self.run_id,)
                    ).fetchone()[0]
                    outcome_rows = db.execute(
                        "SELECT status, COUNT(*) FROM downloads JOIN run_items "
                        "ON run_items.url=downloads.url WHERE run_items.run_id=? "
                        "GROUP BY status", (self.run_id,)
                    ).fetchall()
                durable_outcomes = {status: count for status, count in outcome_rows}
                close_reason = ("time_limit" if self.deadline is not None and self.stop_requested.is_set()
                                else "finished" if not unresolved else "stopped")
                if self.provenance:
                    self.provenance.close(self.queue_inputs, {"max_files": self.args.max_files},
                                          self.selected_item_count, durable_outcomes, close_reason)
                if self.telemetry:
                    outcome = "finished" if not unresolved else "stopped"
                    self.telemetry.set_lifecycle(outcome,
                                                 None if not unresolved else "unresolved items remain")
                    self.telemetry.stop()
                    try:
                        self.telemetry.publish()
                    except OSError as exc:
                        print(f"[telemetry] final snapshot failed: {exc}", file=sys.stderr)
                control.stop()
                inspection.stop()
                db.close()
            print("Run summary: " + ", ".join(
                f"{key}={value}" for key, value in sorted(results.items())))
            return 1 if unresolved else 0
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)
            lock.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--queue", type=Path, action="append")
    parser.add_argument("--destination", type=Path, default=Path("downloaded_files"))
    parser.add_argument("--state", type=Path, default=Path("download-state"))
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--max-files", type=int, default=0)
    parser.add_argument("--run-id", help="resume this immutable selected run")
    parser.add_argument("--max-attempts", type=int, default=0,
                        help="attempt ceiling per URL (0 retries until resolved)")
    parser.add_argument("--aria2c", default=shutil.which("aria2c") or "aria2c")
    parser.add_argument("--aria2-rpc", action="store_true",
                        help="enable controller-only loopback aria2 RPC telemetry")
    parser.add_argument("--rpc-eval", action="store_true",
                        help="test authenticated loopback aria2 RPC without a transfer")
    parser.add_argument("--torsocks", default=shutil.which("torsocks") or "torsocks")
    parser.add_argument("--tor-control-address", default="127.0.0.1:9051")
    parser.add_argument("--tor-control-cookie", type=Path,
                        default=Path("/run/tor/control.authcookie"))
    parser.add_argument("--tor-newnym-interval", type=int, default=60,
                        help="minimum seconds between Tor circuit renewals (0 disables)")
    parser.add_argument("--connect-timeout", type=int, default=90)
    parser.add_argument("--timeout", type=int, default=300)
    parser.add_argument("--progress-interval", type=int, default=30)
    parser.add_argument("--socks-backoff", type=int, default=60)
    parser.add_argument("--worker-stagger", type=float, default=1,
                        help="seconds between worker process starts")
    parser.add_argument("--time-limit", type=float, default=0,
                        help="seconds before admission stops (0 disables the limit)")
    parser.add_argument("--reserve-bytes", type=int, default=10 * 1024 ** 3,
                        help="free-space reserve required before each admission")
    parser.add_argument("--status", action="store_true",
                        help="report persisted state without starting transfers")
    parser.add_argument("--retry-now", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--provenance-signing-key", type=Path,
                        help="Ed25519 private PEM key; default is state/provenance-signing-key.pem")
    args = parser.parse_args()
    if args.rpc_eval:
        return evaluate_aria2_rpc(args)
    if args.status:
        database = args.state / "manifest.sqlite"
        if not database.exists():
            print("No acquisition state exists.")
            return 0
        with sqlite3.connect(database) as db:
            rows = db.execute("SELECT status, COUNT(*) FROM downloads "
                              "GROUP BY status ORDER BY status").fetchall()
            last_success = db.execute("SELECT MAX(updated_at) FROM downloads "
                                      "WHERE status='complete'").fetchone()[0]
        free = shutil.disk_usage(args.destination).free if args.destination.exists() else None
        print("Status: " + ", ".join(f"{status}={count}" for status, count in rows))
        print(f"Last success: {last_success or 'none'}")
        print(f"Free space: {free if free is not None else 'destination missing'}")
        return 0
    if not args.queue:
        parser.error("--queue is required unless --status is used")
    if min(args.workers, args.connect_timeout, args.timeout,
           args.progress_interval, args.socks_backoff, args.tor_newnym_interval) < 0:
        parser.error("Tor circuit renewal interval must not be negative")
    if min(args.workers, args.connect_timeout, args.timeout,
           args.progress_interval, args.socks_backoff) < 1:
        parser.error("numeric settings must be positive")
    if args.workers > 4:
        parser.error("workers must not exceed four")
    if args.worker_stagger < 0 or args.time_limit < 0:
        parser.error("worker-stagger and time-limit must not be negative")
    if args.max_files < 0 or args.max_attempts < 0 or args.reserve_bytes < 0:
        parser.error("max-files, max-attempts, and reserve-bytes must not be negative")
    if args.run_id and not all(char.isalnum() or char in "-_" for char in args.run_id):
        parser.error("run-id may contain only letters, digits, hyphens, and underscores")
    try:
        return Downloader(args).dry_run() if args.dry_run else Downloader(args).run()
    except KeyboardInterrupt:
        print("Interrupted; final files and resumable staging were preserved.", file=sys.stderr)
        return 130
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
