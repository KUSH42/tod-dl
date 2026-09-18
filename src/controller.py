"""Local, controller-owned control transport for acquisition telemetry.

Only the controller creates this endpoint.  The monitor may attach explicitly,
but it never receives database or process-control authority.
"""

from __future__ import annotations

import json
import os
import secrets
import socket
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Callable


PROTOCOL_VERSION = 1
MAX_MESSAGE_BYTES = 64 * 1024
PRIORITY_MIN = -5
PRIORITY_MAX = 5
COOLDOWN_OVERRIDE_MIN_S = 0
COOLDOWN_OVERRIDE_MAX_S = 3600


class ControlError(RuntimeError):
    """A local control endpoint is unavailable or rejected a request."""


def control_directory(state: Path, run_id: str) -> Path:
    return state / "control" / run_id


def read_control_session(state: Path, run_id: str) -> dict[str, Any]:
    """Read the opt-in local capability descriptor with strict basic checks."""
    path = control_directory(state, run_id) / "session.json"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ControlError(f"cannot read controller session: {exc}") from exc
    required = ("protocol_version", "run_id", "session_id", "token", "socket_path")
    if (not isinstance(payload, dict) or payload.get("protocol_version") != PROTOCOL_VERSION
            or payload.get("run_id") != run_id
            or any(not isinstance(payload.get(name), str) or not payload[name]
                   for name in required[1:])):
        raise ControlError("controller session is malformed or incompatible")
    return payload


def control_request(state: Path, run_id: str, action: str,
                    parameters: dict[str, Any] | None = None,
                    request_id: str | None = None, timeout: float = 2.0) -> dict[str, Any]:
    """Send one authenticated local request without exposing its capability."""
    session = read_control_session(state, run_id)
    request = {
        "protocol_version": PROTOCOL_VERSION,
        "request_id": request_id or str(uuid.uuid4()),
        "run_id": run_id,
        "session_id": session["session_id"],
        "token": session["token"],
        "action": action,
        "parameters": parameters or {},
    }
    encoded = (json.dumps(request, separators=(",", ":")) + "\n").encode("utf-8")
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
            connection.settimeout(timeout)
            connection.connect(session["socket_path"])
            connection.sendall(encoded)
            response = _read_line(connection)
    except OSError as exc:
        raise ControlError(f"controller endpoint unavailable: {exc}") from exc
    try:
        payload = json.loads(response.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ControlError("controller returned malformed JSON") from exc
    if not isinstance(payload, dict) or payload.get("request_id") != request["request_id"]:
        raise ControlError("controller returned an invalid response")
    if payload.get("outcome") != "completed":
        raise ControlError(str(payload.get("reason", "controller rejected request")))
    return payload


def get_control_state(state: Path, run_id: str, timeout: float = 2.0) -> dict[str, Any]:
    """Request controller-owned state without exposing the capability in output."""
    payload = control_request(state, run_id, "get_control_state", timeout=timeout)
    state_payload = payload.get("control_state")
    if not isinstance(state_payload, dict):
        raise ControlError("controller returned no control state")
    return state_payload


def _read_line(connection: socket.socket) -> bytes:
    received = bytearray()
    while len(received) <= MAX_MESSAGE_BYTES:
        chunk = connection.recv(min(4096, MAX_MESSAGE_BYTES + 1 - len(received)))
        if not chunk:
            break
        received.extend(chunk)
        if b"\n" in chunk:
            line, _, _ = received.partition(b"\n")
            return line
    raise ControlError("control response exceeds 64 KiB or is incomplete")


class ControlServer:
    """Serve authenticated local state and explicitly confirmed commands."""

    def __init__(self, state: Path, run_id: str, session_id: str,
                 state_provider: Callable[[], dict[str, Any]],
                 action_handler: Callable[[dict[str, Any]], dict[str, Any]] | None = None) -> None:
        self.directory = control_directory(state, run_id)
        self.path = self.directory / "controller.sock"
        self.session_path = self.directory / "session.json"
        self.run_id = run_id
        self.session_id = session_id
        self.token = secrets.token_urlsafe(32)
        self.state_provider = state_provider
        self.action_handler = action_handler
        self.confirmations: dict[str, tuple[str, float, tuple[str, ...] | None]] = {}
        self.completed: dict[str, dict[str, Any]] = {}
        self.command_lock = threading.Lock()
        self.listener: socket.socket | None = None
        self.stop_requested = threading.Event()
        self.thread: threading.Thread | None = None

    def start(self) -> None:
        self.directory.mkdir(parents=True, exist_ok=True)
        os.chmod(self.directory, 0o700)
        self.path.unlink(missing_ok=True)
        session = {
            "protocol_version": PROTOCOL_VERSION,
            "run_id": self.run_id,
            "session_id": self.session_id,
            "token": self.token,
            "socket_path": str(self.path),
        }
        temporary = self.session_path.with_suffix(".tmp")
        with temporary.open("w", encoding="utf-8") as handle:
            os.fchmod(handle.fileno(), 0o600)
            json.dump(session, handle, separators=(",", ":"))
            handle.write("\n")
        os.replace(temporary, self.session_path)
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(str(self.path))
        os.chmod(self.path, 0o600)
        listener.listen(8)
        listener.settimeout(0.25)
        self.listener = listener
        self.thread = threading.Thread(target=self._serve, name="controller-control",
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
            with connection:
                connection.settimeout(2)
                response = self._handle(connection)
                try:
                    connection.sendall((json.dumps(response, separators=(",", ":")) + "\n")
                                       .encode("utf-8"))
                except OSError:
                    # the client disconnected before the reply; the action already ran
                    pass

    def _handle(self, connection: socket.socket) -> dict[str, Any]:
        request_id = ""
        try:
            if hasattr(socket, "SO_PEERCRED"):
                credential = connection.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED,
                                                   12)
                peer_uid = int.from_bytes(credential[4:8], "little")
                if peer_uid != os.getuid():
                    raise ControlError("peer UID is not authorized")
            raw = _read_line(connection)
            request = json.loads(raw.decode("utf-8"))
            if not isinstance(request, dict):
                raise ControlError("request must be an object")
            request_id = request.get("request_id")
            if not isinstance(request_id, str) or not request_id:
                raise ControlError("request ID is required")
            if request.get("protocol_version") != PROTOCOL_VERSION:
                raise ControlError("unsupported protocol version")
            if request.get("run_id") != self.run_id or request.get("session_id") != self.session_id:
                raise ControlError("run or session does not match this controller")
            if not secrets.compare_digest(str(request.get("token", "")), self.token):
                raise ControlError("capability token is invalid")
            action = request.get("action")
            if action == "get_control_state":
                return {"request_id": request_id, "outcome": "completed",
                        "control_state": self.state_provider()}
            if action == "prepare_confirmation":
                return self._prepare_confirmation(request_id, request.get("parameters"))
            if (action not in {"retry_now", "exclude_item", "resume_new_generation",
                                "set_item_priority",
                                "set_retry_cooldown", "renew_tor_circuits",
                                "pause_admission", "resume_admission",
                                "drain_and_stop", "checkpoint_stop"}
                    or self.action_handler is None):
                raise ControlError("action is unavailable")
            return self._confirmed_action(action, request_id, request.get("parameters"), request)
        except (ControlError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            return {"request_id": request_id, "outcome": "rejected", "reason": str(exc)[:512]}

    def _prepare_confirmation(self, request_id: str, parameters: Any) -> dict[str, Any]:
        if not isinstance(parameters, dict):
            raise ControlError("confirmation parameters must be an object")
        action = parameters.get("action")
        if action not in {"retry_now", "exclude_item", "resume_new_generation",
                          "set_item_priority",
                          "set_retry_cooldown", "renew_tor_circuits",
                          "pause_admission", "resume_admission",
                          "drain_and_stop", "checkpoint_stop"}:
            raise ControlError("action is unavailable")
        item_ids: tuple[str, ...] | None = None
        if action in {"retry_now", "exclude_item", "resume_new_generation",
                      "set_item_priority", "set_retry_cooldown"} and (
                action in {"exclude_item", "resume_new_generation",
                          "set_item_priority", "set_retry_cooldown"}
                or "item_ids" in parameters):
            raw_item_ids = parameters.get("item_ids")
            if (not isinstance(raw_item_ids, list) or not raw_item_ids
                    or not all(isinstance(item_id, str) and item_id for item_id in raw_item_ids)):
                raise ControlError("item_ids must be a non-empty list of strings")
            item_ids = tuple(raw_item_ids)
        priority: int | None = None
        if action == "set_item_priority":
            priority = parameters.get("priority")
            if (isinstance(priority, bool) or not isinstance(priority, int)
                    or not PRIORITY_MIN <= priority <= PRIORITY_MAX):
                raise ControlError(
                    f"priority must be an integer between {PRIORITY_MIN} and {PRIORITY_MAX}")
        cooldown_s: int | None = None
        if action == "set_retry_cooldown":
            cooldown_s = parameters.get("cooldown_s")
            if (isinstance(cooldown_s, bool) or not isinstance(cooldown_s, int)
                    or not COOLDOWN_OVERRIDE_MIN_S <= cooldown_s <= COOLDOWN_OVERRIDE_MAX_S):
                raise ControlError(
                    f"cooldown_s must be an integer between {COOLDOWN_OVERRIDE_MIN_S} "
                    f"and {COOLDOWN_OVERRIDE_MAX_S}")
        if action == "retry_now":
            scope = (f"{len(item_ids)} selected item(s) in the immutable selected run"
                      if item_ids else "retryable items in the immutable selected run")
        elif action in {"exclude_item", "resume_new_generation",
                        "set_item_priority", "set_retry_cooldown"}:
            scope = f"{len(item_ids)} selected item(s) in the immutable selected run"
        elif action in {"pause_admission", "resume_admission", "drain_and_stop",
                        "checkpoint_stop"}:
            scope = "the immutable selected run's admission and active transfers"
        else:
            scope = "future Tor streams only"
        nonce = secrets.token_urlsafe(24)
        with self.command_lock:
            self.confirmations[nonce] = (action, time.monotonic() + 60, item_ids, priority,
                                         cooldown_s)
        return {"request_id": request_id, "outcome": "completed", "confirmation": {
            "nonce": nonce, "action": action, "expires_in_s": 60, "scope": scope,
        }}

    def _confirmed_action(self, action: str, request_id: str, parameters: Any,
                          request: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(parameters, dict):
            raise ControlError(f"{action} parameters must be an object")
        with self.command_lock:
            replay = self.completed.get(request_id)
            if replay:
                return replay
            nonce = parameters.get("nonce")
            confirmation = parameters.get("confirmation")
            prepared = self.confirmations.pop(nonce, None) if isinstance(nonce, str) else None
            if (not prepared or prepared[0] != action or time.monotonic() > prepared[1]
                    or confirmation != action):
                raise ControlError(f"{action} confirmation is invalid or expired")
            item_ids = prepared[2]
            priority = prepared[3]
            cooldown_s = prepared[4]
            if item_ids is not None:
                parameters = {**parameters, "item_ids": list(item_ids)}
            if priority is not None:
                parameters = {**parameters, "priority": priority}
            if cooldown_s is not None:
                parameters = {**parameters, "cooldown_s": cooldown_s}
            request = {**request, "parameters": parameters}
            response = self.action_handler(request)
            if not isinstance(response, dict) or response.get("outcome") not in {
                    "completed", "rejected", "failed"}:
                raise ControlError(f"controller returned an invalid {action} outcome")
            response = {"request_id": request_id, **response}
            self.completed[request_id] = response
            return response
