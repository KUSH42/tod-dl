"""Read-only local inspection transport for controller-owned records.

The monitor uses this module for detail records.  It never opens the
acquisition database or reads a control capability.
"""

from __future__ import annotations

import base64
import datetime as dt
import hashlib
import json
import os
import re
import select
import socket
import sqlite3
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlsplit, urlunsplit


PROTOCOL_VERSION = 1
MAX_REQUEST_BYTES = 16 * 1024
MAX_RESPONSE_BYTES = 256 * 1024
MAX_REASON_BYTES = 512
MAX_SOURCE_URL_BYTES = 1024
MAX_PAGE_SIZE = 200
QUEUE_SCAN_BATCH = 500
# Fixed reasons a detail view may show beside an unavailable value. The service
# picks one for each unavailable field; the view never infers a reason.
REASON_NOT_IN_SAMPLE = "not in sample"
REASON_SAMPLE_STALE = "sample stale"
REASON_NOT_APPLICABLE = "not applicable"
REASON_NOT_REPORTED = "controller did not report"
STALE_SAMPLE_AGE_S = 5
# Worker fields that exist only in a live runtime sample.
WORKER_SAMPLE_FIELDS = ("generation", "attempt_id", "attempt_number", "engine_instance_id",
                        "engine_job_id", "pid", "phase", "reason", "phase_elapsed_s",
                        "attempt_elapsed_s", "received_bytes", "total_bytes", "total_source",
                        "resume_baseline_bytes", "sample_sequence", "sample_age_s",
                        "last_transition_at")
# Worker fields that have no meaning outside the downloading phase.
WORKER_DOWNLOAD_FIELDS = ("last_progress_age_s", "speed_bps", "smoothed_speed_bps",
                          "eta_seconds", "connections")
# Worker fields the stale-sample rule suppresses: a live rate or estimate.
WORKER_LIVE_FIELDS = ("speed_bps", "smoothed_speed_bps", "eta_seconds")
# Item fields that come from a runtime sample, keyed as `section.field`.
ITEM_SAMPLE_FIELDS = {"identity.generation", "identity.attempt_id", "state.phase",
                      "state.phase_reason", "state.worker_id", "engine.name",
                      "engine.version", "engine.instance_id", "engine.job_id", "engine.pid",
                      "engine.sample_at", "engine.sample_age_s", "bytes.resume_baseline",
                      "bytes.transfer_total", "bytes.transfer_total_source",
                      "validation.processed_bytes"}
# Item fields whose absence is a normal state of the item, not a missing report.
ITEM_NOT_APPLICABLE_FIELDS = {"identity.mapping_reason", "state.retry_at",
                              "bytes.committed_completion_bytes", "staging_path",
                              "candidate_path", "validation.mismatch_reason"}
ITEM_VALIDATION_FIELDS = {"validation.method", "validation.result", "validation.recorded_at",
                          "validation.observed_sha256"}
ITEM_ID = re.compile(r"[0-9a-f]{64}\Z")
ASCII_CURSOR = re.compile(r"[\x21-\x7e]{1,1024}\Z")
BUCKETS = {"queued", "busy", "retry", "exhausted", "complete",
           "existing_unverified", "review_required", "unavailable", "unknown"}


class InspectionError(RuntimeError):
    """The inspection endpoint is absent or rejected a request."""


def inspection_directory(state: Path, run_id: str) -> Path:
    return state / "telemetry" / run_id


def read_inspection_session(state: Path, run_id: str) -> dict[str, str | int]:
    """Read the non-secret inspection descriptor."""
    path = inspection_directory(state, run_id) / "inspection-session.json"
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise InspectionError(f"inspection endpoint unavailable: {exc}") from exc
    required = ("protocol_version", "run_id", "session_id", "socket_path")
    if (not isinstance(value, dict) or set(value) != set(required)
            or value.get("protocol_version") != PROTOCOL_VERSION
            or value.get("run_id") != run_id
            or any(not isinstance(value.get(name), str) or not value[name]
                   for name in required[1:])):
        raise InspectionError("inspection session is malformed or incompatible")
    return value


def _reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate JSON key")
        value[key] = item
    return value


def _json_object(raw: bytes) -> dict[str, Any]:
    value = json.loads(raw.decode("utf-8"), object_pairs_hook=_reject_duplicates)
    if not isinstance(value, dict):
        raise ValueError("message must be a JSON object")
    return value


def _read_line(connection: socket.socket, maximum: int) -> bytes:
    received = bytearray()
    while len(received) <= maximum:
        chunk = connection.recv(min(4096, maximum + 1 - len(received)))
        if not chunk:
            break
        received.extend(chunk)
        if b"\n" in received:
            line, _, remaining = received.partition(b"\n")
            if remaining:
                raise InspectionError("request contains more than one message")
            readable, _, _ = select.select([connection], [], [], 0.01)
            if readable and connection.recv(1, socket.MSG_PEEK):
                raise InspectionError("request contains bytes after its newline")
            return bytes(line)
    raise InspectionError("request exceeds 16 KiB or is incomplete")


def _request_id(value: Any) -> str | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        uuid.UUID(value)
    except ValueError:
        # a value that is not a UUID is invalid; the caller treats None as invalid
        return None
    return value


def _utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z")


def _cursor_encode(value: dict[str, Any]) -> str:
    return base64.urlsafe_b64encode(json.dumps(value, sort_keys=True,
                                                separators=(",", ":")).encode()).decode()


def _cursor_decode(value: Any) -> dict[str, Any]:
    if not isinstance(value, str) or not ASCII_CURSOR.fullmatch(value):
        raise InspectionError("cursor is invalid")
    try:
        result = json.loads(base64.urlsafe_b64decode(value.encode("ascii")))
    except (ValueError, json.JSONDecodeError) as exc:
        raise InspectionError("cursor is invalid") from exc
    if not isinstance(result, dict):
        raise InspectionError("cursor is invalid")
    return result


def _source_url(value: str) -> str | None:
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError:
        # an unparsable URL or port is invalid; the caller treats None as invalid
        return None
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        return None
    host = parsed.hostname
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    if port is not None:
        host = f"{host}:{port}"
    return urlunsplit((parsed.scheme, host, parsed.path, "", ""))


def inspection_request(state: Path, run_id: str, operation: str,
                       parameters: dict[str, Any] | None = None,
                       request_id: str | None = None,
                       timeout: float = 1.0) -> dict[str, Any]:
    """Send one unauthenticated same-user inspection request."""
    session = read_inspection_session(state, run_id)
    identifier = request_id or str(uuid.uuid4())
    request = {"protocol_version": PROTOCOL_VERSION, "request_id": identifier,
               "run_id": run_id, "session_id": session["session_id"],
               "operation": operation, "parameters": parameters or {}}
    encoded = (json.dumps(request, separators=(",", ":")) + "\n").encode("utf-8")
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
            connection.settimeout(timeout)
            connection.connect(str(session["socket_path"]))
            connection.sendall(encoded)
            raw = _read_line(connection, MAX_RESPONSE_BYTES)
    except OSError as exc:
        raise InspectionError(f"inspection endpoint unavailable: {exc}") from exc
    try:
        response = _json_object(raw)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise InspectionError("inspection response is malformed") from exc
    if response.get("request_id") != identifier:
        raise InspectionError("inspection response has a wrong request ID")
    if response.get("status") != "ok":
        raise InspectionError(str(response.get("reason", "inspection request failed")))
    return response


class InspectionServer:
    """Serve bounded durable records through a same-user Unix socket."""

    def __init__(self, state: Path, run_id: str, session_id: str, database: Path,
                 max_attempts: int, runtime_provider: Callable[[], list[dict[str, Any]]] | None = None) -> None:
        self.directory = inspection_directory(state, run_id)
        self.path = self.directory / "inspection.sock"
        self.session_path = self.directory / "inspection-session.json"
        self.run_id = run_id
        self.session_id = session_id
        self.database = database
        self.max_attempts = max_attempts
        self.runtime_provider = runtime_provider or (lambda: [])
        self.listener: socket.socket | None = None
        self.stop_requested = threading.Event()
        self.thread: threading.Thread | None = None
        self.queries = threading.BoundedSemaphore(4)

    def start(self) -> None:
        self.directory.mkdir(parents=True, exist_ok=True)
        os.chmod(self.directory, 0o700)
        self.path.unlink(missing_ok=True)
        descriptor = {"protocol_version": PROTOCOL_VERSION, "run_id": self.run_id,
                      "session_id": self.session_id, "socket_path": str(self.path)}
        temporary = self.session_path.with_suffix(".tmp")
        with temporary.open("w", encoding="utf-8") as handle:
            os.fchmod(handle.fileno(), 0o600)
            json.dump(descriptor, handle, separators=(",", ":"))
            handle.write("\n")
        os.replace(temporary, self.session_path)
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(str(self.path))
        os.chmod(self.path, 0o600)
        listener.listen(8)
        listener.settimeout(0.25)
        self.listener = listener
        self.thread = threading.Thread(target=self._serve, name="controller-inspection",
                                       daemon=True)
        self.thread.start()

    def stop(self) -> None:
        self.stop_requested.set()
        if self.listener:
            self.listener.close()
        if self.thread:
            self.thread.join(timeout=2)
        self.path.unlink(missing_ok=True)
        self.session_path.unlink(missing_ok=True)

    def _serve(self) -> None:
        while not self.stop_requested.is_set() and self.listener:
            try:
                connection, _ = self.listener.accept()
            except (OSError, TimeoutError):
                # accept timeout or a listener closed by stop(); the loop re-checks stop_requested
                continue
            threading.Thread(target=self._connection, args=(connection,), daemon=True).start()

    def _connection(self, connection: socket.socket) -> None:
        with connection:
            connection.settimeout(1)
            response = self._handle(connection)
            encoded = (json.dumps(response, separators=(",", ":")) + "\n").encode("utf-8")
            if len(encoded) > MAX_RESPONSE_BYTES:
                encoded = b'{"request_id":null,"status":"unavailable","reason":"response exceeds 256 KiB"}\n'
            try:
                connection.sendall(encoded)
            except OSError:
                # the client disconnected before the reply; nothing else can be sent
                pass
            self._drain(connection)

    def _drain(self, connection: socket.socket) -> None:
        """Discard unread bytes so the kernel closes with a FIN, not a
        RST that would corrupt the response already sent to the peer."""
        connection.settimeout(0.05)
        try:
            while connection.recv(65536):
                pass
        except OSError:
            # the client closed the socket while the server drained it; the connection is done
            pass

    def _error(self, request_id: str | None, status: str, reason: str) -> dict[str, Any]:
        return {"request_id": request_id, "status": status,
                "reason": reason.encode("utf-8", "replace")[:MAX_REASON_BYTES].decode("utf-8", "replace")}

    def _handle(self, connection: socket.socket) -> dict[str, Any]:
        request_id: str | None = None
        try:
            if hasattr(socket, "SO_PEERCRED"):
                credentials = connection.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12)
                if int.from_bytes(credentials[4:8], "little") != os.getuid():
                    raise InspectionError("peer UID is not authorized")
            raw = _read_line(connection, MAX_REQUEST_BYTES)
            request = _json_object(raw)
            request_id = _request_id(request.get("request_id"))
            if request_id is None:
                raise InspectionError("request ID must be a UUID")
            self._validate_request(request)
            if not self.queries.acquire(blocking=False):
                return self._error(request_id, "busy", "four inspection queries are already active")
            try:
                started = time.monotonic()
                result = self._query(request, started)
            finally:
                self.queries.release()
            return {"request_id": request_id, "status": "ok", "read_at": _utc_now(),
                    "state_revision": result.pop("state_revision"), "data": result}
        except InspectionError as exc:
            status = "invalid_request"
            message = str(exc)
            if "peer UID" in message:
                status = "unavailable"
            elif "not found" in message:
                status = "not_found"
            elif "session" in message:
                status = "session_changed"
            elif "protocol" in message:
                status = "incompatible"
            elif "cursor expired" in message:
                status = "cursor_expired"
            elif "exceeded one second" in message:
                status = "unavailable"
            return self._error(request_id, status, message)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            return self._error(request_id, "invalid_request", str(exc))
        except sqlite3.OperationalError as exc:
            return self._error(request_id, "unavailable", f"durable read unavailable: {exc}")
        except OSError as exc:
            return self._error(request_id, "unavailable", f"connection error: {exc}")

    def _validate_request(self, request: dict[str, Any]) -> None:
        required = {"protocol_version", "request_id", "run_id", "session_id", "operation", "parameters"}
        if set(request) != required:
            raise InspectionError("request fields are invalid")
        if request["protocol_version"] != PROTOCOL_VERSION:
            raise InspectionError("unsupported protocol version")
        if request["run_id"] != self.run_id or request["session_id"] != self.session_id:
            raise InspectionError("run or session changed")
        if request["operation"] not in {"get_item", "get_worker", "list_queue", "list_attempts"}:
            raise InspectionError("operation is unsupported")
        if not isinstance(request["parameters"], dict):
            raise InspectionError("parameters must be an object")

    def _database(self) -> sqlite3.Connection:
        connection = sqlite3.connect(f"file:{self.database}?mode=ro", uri=True,
                                     timeout=0.1, isolation_level=None)
        connection.create_function("item_id", 1,
                                   lambda value: hashlib.sha256(value.encode()).hexdigest())
        connection.execute("PRAGMA busy_timeout=100")
        return connection

    def _revision(self, db: sqlite3.Connection) -> int:
        row = db.execute("SELECT revision FROM telemetry_revisions WHERE run_id=?",
                         (self.run_id,)).fetchone()
        return int(row[0]) if row else 0

    def _check_deadline(self, started: float,
                        message: str = "durable read exceeded one second") -> None:
        if time.monotonic() - started > 1:
            raise InspectionError(message)

    def _query(self, request: dict[str, Any], started: float) -> dict[str, Any]:
        with self._database() as db:
            db.execute("BEGIN")
            revision = self._revision(db)
            operation = request["operation"]
            if operation == "get_item":
                result = self._get_item(db, request["parameters"])
            elif operation == "get_worker":
                result = self._get_worker(db, request["parameters"])
            elif operation == "list_queue":
                result = self._list_queue(db, request["parameters"], revision, started)
            else:
                result = self._list_attempts(db, request["parameters"], revision)
            self._check_deadline(started)
            db.rollback()
        result["state_revision"] = revision
        return result

    def _item_parameters(self, parameters: dict[str, Any], allowed: set[str]) -> str:
        if set(parameters) - allowed or "item_id" not in parameters:
            raise InspectionError("item parameters are invalid")
        item_id = parameters["item_id"]
        if not isinstance(item_id, str) or not ITEM_ID.fullmatch(item_id):
            raise InspectionError("item ID is invalid")
        return item_id

    def _item_row(self, db: sqlite3.Connection, item_id: str) -> tuple[Any, ...] | None:
        return db.execute(
            "SELECT d.url, r.queue_rank, d.relative_path, d.storage_path, d.staging_path, "
            "d.status, d.attempts, d.bytes, d.sha256, d.last_error, d.next_retry_at, "
            "d.promotion_target, d.updated_at, d.inventory_size, d.review_code "
            "FROM downloads d JOIN run_items r ON r.url=d.url WHERE r.run_id=? "
            "AND item_id(d.url)=?", (self.run_id, item_id)).fetchone()

    @staticmethod
    def _columns(db: sqlite3.Connection, table: str) -> set[str]:
        """Return columns without assuming that an older case database migrated."""
        return {str(row[1]) for row in db.execute(f"PRAGMA table_info({table})")}

    @staticmethod
    def _unavailable(reason: str) -> dict[str, Any]:
        return {"value": None, "reason": reason}

    @staticmethod
    def _item_reasons(data: dict[str, Any], status: str, bucket: str,
                      runtime: dict[str, Any]) -> dict[str, str]:
        """Return one fixed reason for each unavailable item field, keyed `section.field`."""
        sampled = bool(runtime) or bucket == "busy"
        validated = status in {"complete", "review_required"}
        reasons: dict[str, str] = {}
        def choose(key: str) -> str:
            if key in ITEM_SAMPLE_FIELDS:
                return REASON_NOT_IN_SAMPLE if sampled else REASON_NOT_APPLICABLE
            if key in ITEM_VALIDATION_FIELDS:
                return REASON_NOT_REPORTED if validated else REASON_NOT_APPLICABLE
            if key == "validation.mismatch_reason" and status == "review_required":
                return REASON_NOT_REPORTED
            if key in ITEM_NOT_APPLICABLE_FIELDS:
                return REASON_NOT_APPLICABLE
            return REASON_NOT_REPORTED
        for name in ("staging_path", "candidate_path"):
            if data.get(name) is None:
                reasons[name] = choose(name)
        for section in ("identity", "state", "engine", "bytes", "validation"):
            for field, value in data[section].items():
                if value is None:
                    reasons[f"{section}.{field}"] = choose(f"{section}.{field}")
        return reasons

    @staticmethod
    def _worker_reasons(live: dict[str, Any], assignment: dict[str, Any]) -> dict[str, str]:
        """Return one fixed reason for each unavailable worker field.

        A stale sample suppresses live speed and ETA in `live`; the value
        becomes unavailable with the `sample stale` reason.
        """
        reasons: dict[str, str] = {}
        age = live.get("sample_age_s")
        if isinstance(age, (int, float)) and age > STALE_SAMPLE_AGE_S:
            for name in WORKER_LIVE_FIELDS:
                if live.get(name) is not None:
                    live[name] = None
                    reasons[name] = REASON_SAMPLE_STALE
        merged = {**live, **assignment}
        for name in WORKER_SAMPLE_FIELDS:
            if merged.get(name) is None:
                reasons.setdefault(name, REASON_NOT_IN_SAMPLE)
        for name in WORKER_DOWNLOAD_FIELDS:
            if live.get(name) is None:
                reasons.setdefault(name, REASON_NOT_IN_SAMPLE
                                   if live.get("phase") == "downloading"
                                   else REASON_NOT_APPLICABLE)
        if live.get("validation") is None:
            reasons["validation"] = REASON_NOT_APPLICABLE
        if live.get("admission") is None:
            reasons["admission"] = REASON_NOT_REPORTED
        if assignment.get("basename") is None:
            reasons["basename"] = REASON_NOT_REPORTED
        return reasons

    def _bucket(self, status: str, attempts: int) -> str:
        if status == "complete": return "complete"
        if status in {"active", "admitted", "promoting"}: return "busy"
        if status in {"retry_wait", "failed"}: return "exhausted" if self.max_attempts and attempts >= self.max_attempts else "retry"
        if status in {"existing_unverified", "review_required", "unavailable"}: return status
        return "queued" if status == "queued" else "unknown"

    def _get_item(self, db: sqlite3.Connection, parameters: dict[str, Any]) -> dict[str, Any]:
        item_id = self._item_parameters(parameters, {"item_id", "reveal_source"})
        reveal = parameters.get("reveal_source", False)
        if not isinstance(reveal, bool):
            raise InspectionError("reveal_source must be Boolean")
        row = self._item_row(db, item_id)
        if not row:
            raise InspectionError("item was not found")
        (url, rank, logical, stored, staging, status, attempts, byte_count, digest, error,
         retry_at, target, updated_at, inventory_size, review_code) = row
        source = _source_url(url) if reveal else None
        source_truncated = False
        if source is not None:
            encoded_source = source.encode("utf-8")
            if len(encoded_source) > MAX_SOURCE_URL_BYTES:
                source = encoded_source[:MAX_SOURCE_URL_BYTES].decode("utf-8", "ignore")
                source_truncated = True
        # Runtime samples are keyed by url, not item_id (see set_active in
        # download_telemetry.py); item_id is only ever derived, never stored there.
        runtime = next((dict(value) for value in self.runtime_provider()
                        if value.get("url") == url), None)
        runtime = runtime or {}
        source_label = "Source hidden"
        if reveal and source_truncated:
            source_label = "Source truncated"
        elif reveal and source:
            source_label = "Source redacted"
        elif reveal:
            source_label = "Source unavailable"
        # Keep the original flat keys for version-1 clients.  The section keys
        # provide an explicit unavailable reason for the item-details view.
        data = {"item_id": item_id, "queue_rank": rank, "logical_path": logical,
                "storage_path": stored, "staging_path": staging, "candidate_path": target,
                "durable_state": status, "bucket": self._bucket(status, attempts),
                "attempt_count": attempts, "attempt_ceiling": self.max_attempts,
                "received_bytes": byte_count, "sha256": digest, "last_error": error,
                "retry_at": retry_at, "updated_at": updated_at,
                "inventory_size": inventory_size, "review_code": review_code,
                "source": source, "source_label": source_label,
                "basename": Path(logical).name,
                "identity": {"run_id": self.run_id, "item_id": item_id,
                             "logical_path": logical, "storage_path": stored,
                             "basename": Path(logical).name,
                             "queue_rank": rank,
                             "generation": runtime.get("generation"),
                             "attempt_id": (f"{item_id}:{runtime.get('attempt_number')}"
                                            if isinstance(runtime.get('attempt_number'), int)
                                            else None),
                             "mapping_reason": review_code,
                             "mapping_version": None},
                "state": {"durable_state": status, "bucket": self._bucket(status, attempts),
                          "phase": runtime.get("phase"),
                          "phase_reason": runtime.get("reason"),
                          "worker_id": runtime.get("worker_id"),
                          "last_transition_at": updated_at, "attempt_count": attempts,
                          "attempt_ceiling": self.max_attempts,
                          "retry_at": retry_at, "blocking_condition": None},
                "engine": {"name": runtime.get("engine_name"),
                           "version": runtime.get("engine_version"),
                           "instance_id": runtime.get("engine_instance_id"),
                           "job_id": runtime.get("engine_job_id"), "pid": runtime.get("pid"),
                           "sample_at": runtime.get("sample_at"),
                           "sample_sequence": runtime.get("sample_sequence"),
                           "sample_age_s": runtime.get("sample_age_s"),
                           "quality": (None if runtime.get("sample_age_s") is None else
                                       "exact" if runtime["sample_age_s"] <= STALE_SAMPLE_AGE_S
                                       else "unavailable")},
                "bytes": {"received": byte_count, "resume_baseline": runtime.get("resume_baseline_bytes"),
                          "transfer_total": runtime.get("total_bytes") if runtime.get("total_bytes") is not None
                                             else (byte_count if status == "complete" else None),
                          "transfer_total_source": runtime.get("total_source") if runtime.get("total_bytes") is not None
                                                    else ("completed" if status == "complete" else None),
                          "trusted_expected": None, "inventory_size": inventory_size,
                          "retained_item_bytes": byte_count,
                          "committed_completion_bytes": byte_count if status == "complete" else None},
                "validation": {"method": "SHA-256" if digest else None,
                               "processed_bytes": runtime.get("validation_processed_bytes"),
                               "result": "recorded" if digest else None,
                               "recorded_at": updated_at if digest else None,
                               "expected_sha256": None, "observed_sha256": digest,
                               "mismatch_reason": error if status == "review_required" else None,
                               "promotion_status": status,
                               "staging_cleanup_at": None},
                "unavailable": self._unavailable("not recorded by this controller version")}
        data["unavailable_reason"] = self._item_reasons(
            data, status, data["bucket"], runtime)
        return {"item": data}

    def _get_worker(self, db: sqlite3.Connection, parameters: dict[str, Any]) -> dict[str, Any]:
        if set(parameters) != {"worker_id"} or not isinstance(parameters["worker_id"], int) or isinstance(parameters["worker_id"], bool) or parameters["worker_id"] <= 0:
            raise InspectionError("worker ID is invalid")
        worker_id = parameters["worker_id"]
        worker = next((dict(row) for row in self.runtime_provider()
                       if row.get("worker_id") == worker_id), None)
        if worker is None:
            return {"worker": {"worker_id": worker_id, "assignment": None,
                                "phase": "idle", "reason": "Reason unavailable"}}
        url = worker.pop("url", None)
        item_id = hashlib.sha256(url.encode()).hexdigest() if isinstance(url, str) else None
        attempt_id = (f"{item_id}:{worker.get('attempt_number')}"
                      if item_id and isinstance(worker.get('attempt_number'), int)
                      else None)
        row = self._item_row(db, item_id) if item_id else None
        basename = Path(row[2]).name if row else None
        samples = worker.pop("progress_samples", [])
        # The last transition of a slot is the start of its current phase.
        phase_age = worker.get("phase_age_s")
        worker["last_transition_at"] = (
            (dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=phase_age))
            .isoformat(timespec="seconds")
            if isinstance(phase_age, (int, float)) and not isinstance(phase_age, bool) else None)
        now = time.monotonic()
        fresh_samples = [(at, value) for at, value in samples
                         if isinstance(at, (int, float)) and isinstance(value, int)
                         and now - at <= 30]
        smoothed = None
        if len(fresh_samples) >= 2 and fresh_samples[-1][0] - fresh_samples[0][0] >= 10:
            growth = sum(max(0, later[1] - earlier[1])
                         for earlier, later in zip(fresh_samples, fresh_samples[1:]))
            span = fresh_samples[-1][0] - fresh_samples[0][0]
            smoothed = growth / span if span else None
        received, total = worker.get("received_bytes"), worker.get("total_bytes")
        eta = None
        if (isinstance(received, int) and isinstance(total, int) and smoothed
                and total >= received):
            eta = (total - received) / smoothed
        sample_age = worker.get("sample_age_s")
        quality = ("exact" if sample_age is not None and sample_age <= STALE_SAMPLE_AGE_S
                   else "unavailable")
        assignment = {"run_id": self.run_id, "session_id": self.session_id,
                      "worker_id": worker_id, "item_id": item_id, "basename": basename,
                      "generation": worker.get("generation"),
                      "attempt_id": attempt_id,
                      "attempt_number": worker.get("attempt_number"),
                      "engine_instance_id": worker.get("engine_instance_id"),
                      "engine_job_id": worker.get("engine_job_id"), "pid": worker.get("pid")}
        live = {name: worker.get(name) for name in WORKER_SAMPLE_FIELDS
                if name not in assignment}
        live.update({"speed_bps": worker.get("speed_bps"), "smoothed_speed_bps": smoothed,
                     "eta_seconds": eta,
                     "last_progress_age_s": worker.get("last_progress_age_s"),
                     "connections": worker.get("connections"),
                     "validation": worker.get("validation"),
                     "admission": worker.get("admission")})
        reasons = self._worker_reasons(live, assignment)
        speed, smoothed, eta = live["speed_bps"], live["smoothed_speed_bps"], live["eta_seconds"]
        return {"worker": {"worker_id": worker_id, "assignment": assignment,
                            "unavailable_reason": reasons,
                            "phase": worker.get("phase"), "reason": worker.get("reason"),
                            "phase_elapsed_s": worker.get("phase_age_s"),
                            "last_transition_at": worker.get("last_transition_at"),
                            "attempt_elapsed_s": (max(0.0, now - worker["attempt_started"])
                                                  if isinstance(worker.get("attempt_started"), (int, float)) else None),
                            "last_progress_age_s": worker.get("last_progress_age_s"),
                            "received_bytes": worker.get("received_bytes"),
                            "total_bytes": worker.get("total_bytes"),
                            "total_source": worker.get("total_source"),
                            "resume_baseline_bytes": worker.get("resume_baseline_bytes"),
                            "speed_bps": speed,
                            "smoothed_speed_bps": smoothed,
                            "eta_seconds": eta,
                            "connections": worker.get("connections"),
                            "sample_sequence": worker.get("sample_sequence"),
                            "sample_age_s": sample_age, "quality": quality,
                            "estimator": "ready" if smoothed is not None else "Estimating",
                            "validation": worker.get("validation"),
                            "admission": worker.get("admission")}}

    def _list_queue(self, db: sqlite3.Connection, parameters: dict[str, Any], revision: int,
                    started: float) -> dict[str, Any]:
        allowed = {"bucket", "query", "page_size", "cursor"}
        if set(parameters) - allowed:
            raise InspectionError("queue parameters are invalid")
        bucket = parameters.get("bucket", "all")
        query = parameters.get("query", "")
        size = parameters.get("page_size", 100)
        if bucket != "all" and bucket not in BUCKETS:
            raise InspectionError("bucket is invalid")
        if not isinstance(query, str) or len(query.encode("utf-8")) > 1024:
            raise InspectionError("query is invalid")
        if not isinstance(size, int) or isinstance(size, bool) or not 1 <= size <= MAX_PAGE_SIZE:
            raise InspectionError("page_size is invalid")
        after_rank, after_item = -1, ""
        if "cursor" in parameters:
            cursor = _cursor_decode(parameters["cursor"])
            if cursor.get("run_id") != self.run_id or cursor.get("session_id") != self.session_id or cursor.get("revision") != revision or cursor.get("bucket") != bucket or cursor.get("query") != query:
                raise InspectionError("cursor expired")
            after_rank, after_item = cursor.get("rank"), cursor.get("item_id")
            if not isinstance(after_rank, int) or not isinstance(after_item, str):
                raise InspectionError("cursor is invalid")
        # Runtime samples are keyed by url, not item_id (see set_active in
        # download_telemetry.py); item_id is only ever derived, never stored there.
        runtime = {value.get("url"): value for value in self.runtime_provider()
                  if value.get("url")}
        output: list[dict[str, Any]] = []
        filled = False
        while len(output) < size:
            self._check_deadline(
                started, "durable read exceeded one second; narrow your search")
            rows = db.execute(
                "SELECT r.queue_rank,d.url,d.relative_path,d.storage_path,d.status,d.attempts,"
                "d.bytes,d.next_retry_at,d.priority FROM run_items r JOIN downloads d ON d.url=r.url "
                "WHERE r.run_id=? AND (r.queue_rank>? OR (r.queue_rank=? AND item_id(d.url)>?)) "
                "ORDER BY r.queue_rank, item_id(d.url) LIMIT ?",
                (self.run_id, after_rank, after_rank, after_item, QUEUE_SCAN_BATCH)).fetchall()
            if not rows:
                break
            for rank, url, logical, stored, status, attempts, received, retry_at, priority in rows:
                item_id = hashlib.sha256(url.encode()).hexdigest()
                after_rank, after_item = rank, item_id
                row_bucket = self._bucket(status, attempts)
                if (bucket != "all" and bucket != row_bucket) or (
                        query and query not in logical and query not in (stored or "")
                        and query not in item_id):
                    continue
                sample = runtime.get(url)
                total_bytes = sample.get("total_bytes") if sample else None
                if total_bytes is None and row_bucket == "complete":
                    total_bytes = received
                output.append({"item_id": item_id, "queue_rank": rank,
                               "basename": Path(logical).name, "bucket": row_bucket,
                               "phase": sample.get("phase") if sample else None,
                               "received_bytes": received,
                               "total_bytes": total_bytes,
                               "retry_at": retry_at, "priority": priority})
                if len(output) == size:
                    filled = True
                    break
            if filled or len(rows) < QUEUE_SCAN_BATCH:
                break
        next_cursor = None
        if filled:
            next_cursor = _cursor_encode({"run_id": self.run_id, "session_id": self.session_id,
                                           "revision": revision, "bucket": bucket, "query": query,
                                           "rank": after_rank, "item_id": after_item})
        return {"rows": output, "next_cursor": next_cursor, "matching_count": None}

    def _list_attempts(self, db: sqlite3.Connection, parameters: dict[str, Any], revision: int) -> dict[str, Any]:
        item_id = self._item_parameters(parameters, {"item_id", "cursor", "page_size"})
        size = parameters.get("page_size", 100)
        if not isinstance(size, int) or isinstance(size, bool) or not 1 <= size <= MAX_PAGE_SIZE:
            raise InspectionError("page_size is invalid")
        row = self._item_row(db, item_id)
        if not row:
            raise InspectionError("item was not found")
        # Transition rows are not attempt records.  Do not infer attempts from
        # state changes when an older controller has not recorded them.
        if "download_attempts" not in {str(value[0]) for value in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}:
            return {"attempts": [], "next_cursor": None,
                    "unavailable_reason": "attempt history was not recorded"}
        columns = self._columns(db, "download_attempts")
        required = {"run_id", "url", "attempt_number", "attempt_id"}
        if not required <= columns:
            return {"attempts": [], "next_cursor": None,
                    "unavailable_reason": "attempt history is incompatible"}
        cursor_number, cursor_id = None, None
        if "cursor" in parameters:
            cursor = _cursor_decode(parameters["cursor"])
            if (cursor.get("run_id") != self.run_id or cursor.get("session_id") != self.session_id
                    or cursor.get("revision") != revision or cursor.get("item_id") != item_id):
                raise InspectionError("cursor expired")
            cursor_number, cursor_id = cursor.get("attempt_number"), cursor.get("attempt_id")
            if not isinstance(cursor_number, int) or not isinstance(cursor_id, str):
                raise InspectionError("cursor is invalid")
        select = [name for name in ("attempt_number", "attempt_id", "generation", "started_at",
                  "ended_at", "outcome", "error_category", "error_message", "retry_at") if name in columns]
        where = "run_id=? AND url=?"
        values: list[Any] = [self.run_id, row[0]]
        if cursor_number is not None:
            where += " AND (attempt_number<? OR (attempt_number=? AND attempt_id>?))"
            values.extend([cursor_number, cursor_number, cursor_id])
        rows = db.execute("SELECT " + ",".join(select) + " FROM download_attempts WHERE " + where
                          + " ORDER BY attempt_number DESC, attempt_id ASC LIMIT ?", values + [size + 1]).fetchall()
        attempts = [dict(zip(select, value)) for value in rows[:size]]
        next_cursor = None
        if len(rows) > size and attempts:
            last = attempts[-1]
            next_cursor = _cursor_encode({"run_id": self.run_id, "session_id": self.session_id,
                                           "revision": revision, "item_id": item_id,
                                           "attempt_number": last["attempt_number"],
                                           "attempt_id": last["attempt_id"]})
        return {"attempts": attempts, "next_cursor": next_cursor}
