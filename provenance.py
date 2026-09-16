"""Signed, append-only acquisition provenance records.

The module deliberately has no access to the acquisition database.  The
controller supplies only durable, non-secret acquisition facts.
"""

from __future__ import annotations

import base64
import datetime as dt
import hashlib
import json
import os
import re
import threading
import uuid
from pathlib import Path, PurePosixPath

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)


SCHEMA_VERSION = 1
HEX_DIGEST = re.compile(r"^[0-9a-f]{64}$")
EVENT_TYPES = {"run_started", "attempt_finished", "finalized",
               "candidate_created", "run_closed", "local_failure"}


class ProvenanceError(RuntimeError):
    """A provenance operation failed before its related durable state."""


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def canonical_json(value: object) -> bytes:
    """Encode the record subset using JCS-compatible UTF-8 JSON.

    Provenance values contain strings, integers, booleans, nulls, arrays, and
    objects.  Floats are rejected because acquisition records have no float
    fields and JCS number conversion is otherwise not provided by stdlib JSON.
    """
    def reject_float(item: object) -> None:
        if isinstance(item, float):
            raise ProvenanceError("provenance records must not contain floats")
        if isinstance(item, dict):
            for key, nested in item.items():
                if not isinstance(key, str):
                    raise ProvenanceError("provenance object keys must be strings")
                reject_float(nested)
        elif isinstance(item, list):
            for nested in item:
                reject_float(nested)
    reject_float(value)
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
                      allow_nan=False).encode("utf-8")


def digest(value: object) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()


def relative_text(value: str) -> str:
    path = PurePosixPath(value)
    if path.is_absolute() or not path.parts or any(part in {"", ".", ".."} for part in path.parts):
        raise ProvenanceError("provenance path is not a safe relative path")
    return path.as_posix()


def schema_document() -> dict:
    """Return the versioned Draft 2020-12 record schema."""
    base_properties = {
            "schema_version": {"const": SCHEMA_VERSION}, "event_id": {"type": "string", "format": "uuid"},
            "sequence": {"type": "integer", "minimum": 1}, "event_type": {"enum": sorted(EVENT_TYPES)},
            "run_id": {"type": "string", "minLength": 1}, "session_id": {"type": "string", "format": "uuid"},
            "recorded_at": {"type": "string", "format": "date-time"},
            "previous_digest": {"type": ["string", "null"], "pattern": "^[0-9a-f]{64}$"},
            "event_digest": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
    }
    common = ["schema_version", "event_id", "sequence", "event_type", "run_id",
              "session_id", "recorded_at", "previous_digest", "event_digest"]
    event_requirements = {
        "run_started": ["queue_input_digests", "selection_settings", "selected_item_count"],
        "attempt_finished": ["item_id", "source_url", "attempt_number", "request_started_at", "request_finished_at", "final_url", "redirect_chain", "http_status", "outcome", "response_content_length", "response_content_range", "response_content_type", "response_etag", "response_last_modified"],
        "finalized": ["item_id", "source_url", "logical_relative_path", "final_relative_path", "byte_count", "sha256", "validation_method_version", "finalized_at", "expected_size", "expected_checksum", "etag", "last_modified"],
        "candidate_created": ["item_id", "staging_relative_path", "candidate_relative_path", "sha256", "finalization_failure_reason"],
        "run_closed": ["closed_at", "close_reason", "durable_outcome_counts"],
        "local_failure": ["failure_code", "failure_detail"],
    }
    generic = {name: {} for names in event_requirements.values() for name in names}
    event_base = {"oneOf": [
        {"type": "object", "additionalProperties": False,
         "required": common + names,
         "properties": {**base_properties, **generic,
                        "event_type": {"const": event_type}}}
        for event_type, names in event_requirements.items()
    ]}
    summary = {
        "type": "object", "additionalProperties": False,
        "required": ["schema_version", "run_id", "session_id", "queue_input_digests",
                     "selection_settings", "event_count", "final_event_digest",
                     "selected_item_count", "durable_outcome_counts", "schema_sha256",
                     "signing_key_fingerprint", "signature"],
        "properties": {
            "schema_version": {"const": SCHEMA_VERSION}, "run_id": {"type": "string", "minLength": 1},
            "session_id": {"type": "string", "format": "uuid"}, "queue_input_digests": {"type": "array"},
            "selection_settings": {"type": "object"}, "event_count": {"type": "integer", "minimum": 0},
            "final_event_digest": {"type": ["string", "null"], "pattern": "^[0-9a-f]{64}$"},
            "selected_item_count": {"type": "integer", "minimum": 0}, "durable_outcome_counts": {"type": "object"},
            "schema_sha256": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
            "signing_key_fingerprint": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
            "signature": {"type": "string", "pattern": "^[A-Za-z0-9_-]{86}$"},
        },
    }
    return {"$schema": "https://json-schema.org/draft/2020-12/schema", "title": "Acquisition provenance manifest",
            "schema_version": SCHEMA_VERSION, "canonicalization": "JCS-RFC8785",
            "$defs": {"event": event_base, "summary": summary}}


def _write_atomic(path: Path, data: bytes, mode: int = 0o600) -> None:
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    except BaseException:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        raise


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def load_or_create_private_key(path: Path) -> Ed25519PrivateKey:
    if path.exists():
        return serialization.load_pem_private_key(path.read_bytes(), password=None)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    key = Ed25519PrivateKey.generate()
    private_bytes = key.private_bytes(serialization.Encoding.PEM,
                                      serialization.PrivateFormat.PKCS8,
                                      serialization.NoEncryption())
    _write_atomic(path, private_bytes)
    return key


class ProvenanceWriter:
    def __init__(self, state: Path, run_id: str, private_key_path: Path | None = None) -> None:
        self.run_id = run_id
        self.session_id = str(uuid.uuid4())
        self.directory = state / "provenance" / run_id / self.session_id
        self.directory.mkdir(parents=True, mode=0o700)
        os.chmod(self.directory, 0o700)
        _fsync_directory(self.directory)
        self.events_path = self.directory / "events.jsonl"
        self.events_path.touch(mode=0o600, exist_ok=False)
        os.chmod(self.events_path, 0o600)
        _fsync_directory(self.directory)
        self.schema_path = self.directory / "schema.json"
        _write_atomic(self.schema_path, json.dumps(schema_document(), ensure_ascii=False,
                                                    sort_keys=True, separators=(",", ":"))
                      .encode("utf-8"))
        self.schema_sha256 = hashlib.sha256(self.schema_path.read_bytes()).hexdigest()
        key_path = private_key_path or state / "provenance-signing-key.pem"
        self.private_key = load_or_create_private_key(key_path)
        public = self.private_key.public_key().public_bytes(serialization.Encoding.Raw,
                                                              serialization.PublicFormat.Raw)
        self.fingerprint = hashlib.sha256(public).hexdigest()
        self.sequence = 0
        self.previous_digest: str | None = None
        self.lock = threading.Lock()

    def event(self, event_type: str, **fields: object) -> dict:
        if event_type not in EVENT_TYPES:
            raise ProvenanceError("unknown provenance event type")
        with self.lock:
            self.sequence += 1
            record = {"schema_version": SCHEMA_VERSION, "event_id": str(uuid.uuid4()),
                      "sequence": self.sequence, "event_type": event_type,
                      "run_id": self.run_id, "session_id": self.session_id,
                      "recorded_at": utc_now(), "previous_digest": self.previous_digest}
            record.update(fields)
            record["event_digest"] = digest(record)
            data = json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8") + b"\n"
            try:
                with self.events_path.open("ab", buffering=0) as handle:
                    handle.write(data)
                    handle.flush()
                    os.fsync(handle.fileno())
            except OSError as exc:
                raise ProvenanceError(f"cannot write provenance event: {exc}") from exc
            self.previous_digest = record["event_digest"]
            return record

    def close(self, queue_input_digests: list[dict], selection_settings: dict,
              selected_item_count: int, durable_outcome_counts: dict[str, int],
              close_reason: str) -> None:
        self.event("run_closed", closed_at=utc_now(), close_reason=close_reason,
                   durable_outcome_counts=durable_outcome_counts)
        summary = {"schema_version": SCHEMA_VERSION, "run_id": self.run_id,
                   "session_id": self.session_id, "queue_input_digests": queue_input_digests,
                   "selection_settings": selection_settings, "event_count": self.sequence,
                   "final_event_digest": self.previous_digest, "selected_item_count": selected_item_count,
                   "durable_outcome_counts": durable_outcome_counts,
                   "schema_sha256": self.schema_sha256,
                   "signing_key_fingerprint": self.fingerprint}
        signature = self.private_key.sign(canonical_json(summary))
        summary["signature"] = base64.urlsafe_b64encode(signature).decode("ascii").rstrip("=")
        _write_atomic(self.directory / "summary.json", json.dumps(summary, ensure_ascii=False,
                                                                    sort_keys=True, separators=(",", ":")).encode("utf-8"))


def read_public_key(path: Path) -> Ed25519PublicKey:
    data = path.read_bytes()
    try:
        return serialization.load_pem_public_key(data)
    except ValueError:
        if len(data) == 32:
            return Ed25519PublicKey.from_public_bytes(data)
        raise
