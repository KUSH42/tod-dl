#!/usr/bin/env python3
"""Read-only verifier for an acquisition provenance record set."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import sys
from pathlib import Path

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization

from provenance import (EVENT_TYPES, ProvenanceError, HEX_DIGEST, SCHEMA_VERSION, canonical_json,
                        digest, event_schema, read_public_key, relative_text,
                        schema_document, schema_errors)

EVENT_FIELDS = {
    "run_started": {"queue_input_digests", "selection_settings", "selected_item_count"},
    "attempt_finished": {"item_id", "source_url", "attempt_number", "request_started_at",
                         "request_finished_at", "final_url", "redirect_chain", "http_status", "outcome",
                         "response_content_length", "response_content_range", "response_content_type",
                         "response_etag", "response_last_modified"},
    "finalized": {"item_id", "source_url", "logical_relative_path", "final_relative_path", "byte_count",
                  "sha256", "validation_method_version", "finalized_at", "expected_size",
                  "expected_checksum", "etag", "last_modified"},
    "candidate_created": {"item_id", "staging_relative_path", "candidate_relative_path", "sha256",
                          "finalization_failure_reason"},
    "run_closed": {"closed_at", "close_reason", "durable_outcome_counts"},
    "local_failure": {"failure_code", "failure_detail"},
}


def load_json(path: Path, errors: list[str]) -> object | None:
    def no_duplicates(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate key {key!r}")
            result[key] = value
        return result
    try:
        return json.loads(path.read_bytes().decode("utf-8"), object_pairs_hook=no_duplicates)
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        errors.append(f"{path.name}: invalid JSON: {exc}")
        return None


def check_event(event: object, run_id: str, session_id: str, sequence: int,
                previous: str | None, seen_ids: set[str], errors: list[str]) -> str | None:
    if not isinstance(event, dict):
        errors.append("events.jsonl: event is not an object")
        return None
    required = {"schema_version", "event_id", "sequence", "event_type", "run_id", "session_id",
                "recorded_at", "previous_digest", "event_digest"}
    if not required.issubset(event): errors.append("events.jsonl: event has missing required fields")
    if event.get("schema_version") != SCHEMA_VERSION: errors.append("events.jsonl: invalid schema version")
    if event.get("event_type") not in EVENT_TYPES: errors.append("events.jsonl: invalid event type")
    else:
        expected = required | EVENT_FIELDS[event["event_type"]]
        if set(event) != expected: errors.append("events.jsonl: missing or unknown event fields")
        errors.extend(f"events.jsonl: {problem}" for problem in
                      schema_errors(event, event_schema(schema_document(), event["event_type"])))
        if event["event_type"] == "finalized":
            for name in ("expected_size", "expected_checksum", "etag", "last_modified"):
                check = event.get(name)
                if (isinstance(check, dict) and check.get("available") is False
                        and (check.get("compared") is not False or check.get("value") is not None)):
                    errors.append(f"events.jsonl: {name} is unavailable but has a value or was compared")
    if event.get("run_id") != run_id or event.get("session_id") != session_id:
        errors.append("events.jsonl: event identifiers do not match record path")
    if event.get("sequence") != sequence: errors.append("events.jsonl: sequence gap or reorder")
    if event.get("event_id") in seen_ids: errors.append("events.jsonl: duplicate event ID")
    seen_ids.add(event.get("event_id"))
    if event.get("previous_digest") != previous: errors.append("events.jsonl: previous digest changed")
    actual = event.get("event_digest")
    without = dict(event); without.pop("event_digest", None)
    if not isinstance(actual, str) or not HEX_DIGEST.fullmatch(actual) or actual != digest(without):
        errors.append("events.jsonl: invalid event digest")
    return actual if isinstance(actual, str) else None


def verify(record_dir: Path, destination: Path, public_key: Path | None,
           expected_fingerprint: str | None) -> list[str]:
    errors: list[str] = []
    try:
        run_id, session_id = record_dir.parts[-2:]
    except ValueError:
        return ["record directory must end in RUN_ID/SESSION_ID"]
    schema_path = record_dir / "schema.json"
    summary_path = record_dir / "summary.json"
    events_path = record_dir / "events.jsonl"
    schema = load_json(schema_path, errors)
    if schema != schema_document(): errors.append("schema.json: schema content changed or is unsupported")
    summary = load_json(summary_path, errors)
    if not isinstance(summary, dict): return errors
    required_summary = {"schema_version", "run_id", "session_id", "queue_input_digests", "selection_settings",
                        "event_count", "final_event_digest", "selected_item_count", "durable_outcome_counts",
                        "session_finalized_count", "schema_sha256", "signing_key_fingerprint", "signature"}
    if set(summary) != required_summary: errors.append("summary.json: missing or unknown fields")
    if summary.get("run_id") != run_id or summary.get("session_id") != session_id:
        errors.append("summary.json: identifiers do not match record path")
    schema_digest = hashlib.sha256(schema_path.read_bytes()).hexdigest() if schema_path.exists() else None
    if summary.get("schema_sha256") != schema_digest: errors.append("summary.json: schema digest mismatch")
    if not public_key and not expected_fingerprint:
        errors.append("a trusted public key or expected fingerprint is required")
    key = None
    if public_key:
        try:
            key = read_public_key(public_key)
            raw = key.public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
            fingerprint = hashlib.sha256(raw).hexdigest()
            if summary.get("signing_key_fingerprint") != fingerprint:
                errors.append("summary.json: signing key fingerprint mismatch")
            if expected_fingerprint and fingerprint != expected_fingerprint:
                errors.append("trusted public key fingerprint does not match expected fingerprint")
        except (OSError, ValueError) as exc: errors.append(f"trusted public key: {exc}")
    elif expected_fingerprint:
        # A fingerprint alone cannot verify a signature; fail instead of skipping the check.
        errors.append("a public key is required to verify the summary signature")
        if summary.get("signing_key_fingerprint") != expected_fingerprint:
            errors.append("summary.json: signing key fingerprint mismatch")
    if key:
        unsigned = dict(summary); signature_text = unsigned.pop("signature", "")
        try:
            key.verify(base64.urlsafe_b64decode(signature_text + "=="), canonical_json(unsigned))
        except (InvalidSignature, ValueError) as exc: errors.append(f"summary.json: invalid signature: {exc}")
    if isinstance(summary, dict):
        errors.extend(f"summary.json: {problem}" for problem in
                      schema_errors(summary, schema_document()["$defs"]["summary"]))
    previous = None; seen_ids: set[str] = set(); final_events = []; last_event = None
    try:
        lines = events_path.read_bytes().splitlines()
    except OSError as exc:
        errors.append(f"events.jsonl: {exc}"); lines = []
    for number, line in enumerate(lines, 1):
        try:
            def no_duplicates(pairs):
                output = {}
                for key, value in pairs:
                    if key in output:
                        raise ValueError(f"duplicate key {key!r}")
                    output[key] = value
                return output
            event = json.loads(line.decode("utf-8"), object_pairs_hook=no_duplicates)
        except (UnicodeDecodeError, ValueError) as exc:
            errors.append(f"events.jsonl line {number}: invalid JSON: {exc}"); continue
        previous = check_event(event, run_id, session_id, number, previous, seen_ids, errors)
        last_event = event
        if isinstance(event, dict) and event.get("event_type") == "finalized": final_events.append(event)
    if summary.get("event_count") != len(lines): errors.append("summary.json: event count mismatch")
    if summary.get("final_event_digest") != previous: errors.append("summary.json: final event digest mismatch")
    if lines and not (isinstance(last_event, dict) and last_event.get("event_type") == "run_closed"
                      and last_event.get("durable_outcome_counts") == summary.get("durable_outcome_counts")):
        errors.append("summary.json: last event is not a matching run_closed event")
    durable_counts = summary.get("durable_outcome_counts")
    session_count = summary.get("session_finalized_count")
    if isinstance(durable_counts, dict) and isinstance(session_count, int):
        # The run total covers all sessions, so it can exceed this session's finalized events.
        if session_count != len(final_events) or durable_counts.get("complete", 0) < session_count:
            errors.append("summary.json: completed-item count does not match finalized events")
    for event in final_events:
        try:
            relative_text(event["logical_relative_path"])
            relative = relative_text(event["final_relative_path"])
            final = destination / Path(relative)
            if not final.is_file(): errors.append(f"final file is missing: {relative}"); continue
            if final.stat().st_size != event.get("byte_count"): errors.append(f"final byte count differs: {relative}")
            if hashlib.sha256(final.read_bytes()).hexdigest() != event.get("sha256"):
                errors.append(f"final SHA-256 differs: {relative}")
        except (KeyError, OSError, ValueError, ProvenanceError) as exc:
            errors.append(f"finalized event is invalid: {exc}")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("record_dir", type=Path)
    parser.add_argument("--destination", type=Path, default=Path("downloaded_files"))
    parser.add_argument("--public-key", type=Path)
    parser.add_argument("--expected-fingerprint")
    args = parser.parse_args()
    errors = verify(args.record_dir, args.destination, args.public_key, args.expected_fingerprint)
    for error in errors: print(f"FAIL: {error}")
    if errors: return 1
    print("OK: provenance record set verified")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
