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
MAX_PAGE_SIZE = 200
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

    def _check_deadline(self, started: float) -> None:
        if time.monotonic() - started > 1:
            raise InspectionError("durable read exceeded one second")

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
                result = self._list_queue(db, request["parameters"], revision)
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

    def _bucket(self, status: str, attempts: int) -> str:
        if status == "complete": return "complete"
        if status in {"active", "admitted", "promoting"}: return "busy"
        if status in {"retry_wait", "failed"}: return "exhausted" if self.max_attempts and attempts >= self.max_attempts else "retry"
        if status in {"existing_unverified", "review_required"}: return status
        return "queued" if status in {"pending", "queued"} else "unknown"

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
        data = {"item_id": item_id, "queue_rank": rank, "logical_path": logical,
                "storage_path": stored, "staging_path": staging, "candidate_path": target,
                "durable_state": status, "bucket": self._bucket(status, attempts),
                "attempt_count": attempts, "attempt_ceiling": self.max_attempts,
                "received_bytes": byte_count, "sha256": digest, "last_error": error,
                "retry_at": retry_at, "updated_at": updated_at,
                "inventory_size": inventory_size, "review_code": review_code,
                "source": source, "source_label": "Source redacted" if source else "Source hidden"}
        return {"item": data}

    def _get_worker(self, db: sqlite3.Connection, parameters: dict[str, Any]) -> dict[str, Any]:
        if set(parameters) != {"worker_id"} or not isinstance(parameters["worker_id"], int) or isinstance(parameters["worker_id"], bool) or parameters["worker_id"] <= 0:
            raise InspectionError("worker ID is invalid")
        worker_id = parameters["worker_id"]
        worker = next((dict(row) for row in self.runtime_provider()
                       if row.get("worker_id") == worker_id), None)
        if worker is None:
            return {"worker": {"worker_id": worker_id, "assignment": None,
                                "reason": "No item assigned"}}
        url = worker.pop("url", None)
        if isinstance(url, str):
            worker["item_id"] = hashlib.sha256(url.encode()).hexdigest()
        return {"worker": worker}

    def _list_queue(self, db: sqlite3.Connection, parameters: dict[str, Any], revision: int) -> dict[str, Any]:
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
        rows = db.execute(
            "SELECT r.queue_rank,d.url,d.relative_path,d.storage_path,d.status,d.attempts,"
            "d.bytes,d.next_retry_at FROM run_items r JOIN downloads d ON d.url=r.url "
            "WHERE r.run_id=? AND (r.queue_rank>? OR (r.queue_rank=? AND item_id(d.url)>?)) "
            "ORDER BY r.queue_rank, item_id(d.url) LIMIT ?", (self.run_id, after_rank, after_rank,
                                                       after_item, MAX_PAGE_SIZE + 1)).fetchall()
        output = []
        for rank, url, logical, stored, status, attempts, received, retry_at in rows[:size]:
            item_id = hashlib.sha256(url.encode()).hexdigest()
            row_bucket = self._bucket(status, attempts)
            if (bucket != "all" and bucket != row_bucket) or (query and query not in logical and query not in (stored or "") and query not in item_id):
                continue
            output.append({"item_id": item_id, "queue_rank": rank,
                           "basename": Path(logical).name, "bucket": row_bucket,
                           "phase": None, "received_bytes": received, "total_bytes": None,
                           "retry_at": retry_at})
        next_cursor = None
        if len(rows) > MAX_PAGE_SIZE and output:
            last = output[-1]
            next_cursor = _cursor_encode({"run_id": self.run_id, "session_id": self.session_id,
                                           "revision": revision, "bucket": bucket, "query": query,
                                           "rank": last["queue_rank"], "item_id": last["item_id"]})
        return {"rows": output, "next_cursor": next_cursor, "matching_count": None}

    def _list_attempts(self, db: sqlite3.Connection, parameters: dict[str, Any], revision: int) -> dict[str, Any]:
        item_id = self._item_parameters(parameters, {"item_id", "cursor", "page_size"})
        size = parameters.get("page_size", 100)
        if not isinstance(size, int) or isinstance(size, bool) or not 1 <= size <= MAX_PAGE_SIZE:
            raise InspectionError("page_size is invalid")
        row = self._item_row(db, item_id)
        if not row:
            raise InspectionError("item was not found")
        transitions = db.execute("SELECT to_status,detail,recorded_at FROM download_transitions "
                                 "WHERE run_id=? AND url=? ORDER BY id DESC LIMIT ?",
                                 (self.run_id, row[0], size)).fetchall()
        return {"attempts": [{"outcome": outcome, "detail": detail, "recorded_at": recorded}
                              for outcome, detail, recorded in transitions], "next_cursor": None}
