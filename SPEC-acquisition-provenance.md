# Specification: acquisition provenance manifest

Status: proposed behavior, September 16, 2026. This specification adds a
portable provenance record for each acquired item. It supplements the
[reliable acquisition specification](SPEC-reliable-acquisition.md). The
SQLite database remains the controller's recovery authority.

## Purpose and boundary

The downloader must produce an exportable record that links each final file to
its source request, local validation, and finalization result. The record can
prove that a named local file has a recorded SHA-256 digest. The record cannot
prove that the source served an original or complete collection.

The controller must not modify `downloaded_files/` to add provenance data. It
must write each session record set only below
`download-state/provenance/<run-id>/<session-id>/`. The monitor must remain
read-only and must not create provenance records.

## Record set

For each controller session, the controller must create one new record
directory and write these files:

- `events.jsonl` contains append-only provenance events.
- `summary.json` contains the final session summary and the final event digest.
- `schema.json` contains the versioned JSON Schema and canonicalization method.

The controller must create each record directory with mode `0700`. It must
create each record file with mode `0600`. The controller must create and fsync
the record directory before it writes a record. When it writes `summary.json`
or `schema.json`, it must fsync a temporary file, atomically replace the
target, and fsync the record directory.

`events.jsonl` must contain one UTF-8 JSON object and one LF byte per line.
Before the controller records the related durable completion state, it must
flush and fsync the event file. A failed record write must stop new admission
and report a local failure. A completed file without a durable provenance event
is unresolved.

## Event format

Each event must contain these fields. Unknown source metadata must use JSON
`null`. Timestamps must use UTC RFC 3339 format with a `Z` offset.

| Field | Requirement |
| --- | --- |
| `schema_version` | Positive integer schema version. |
| `event_id` | Unique UUID for the event. |
| `sequence` | Positive integer that increases by one within the record set. |
| `event_type` | One of `run_started`, `attempt_finished`, `finalized`, `candidate_created`, `run_closed`, or `local_failure`. |
| `run_id`, `session_id` | Immutable selected run identifier and controller-session identifier. |
| `recorded_at` | Event creation time. |
| `previous_digest` | SHA-256 digest of the preceding event, or `null` for sequence one. |
| `event_digest` | SHA-256 digest of the canonical event object without `event_digest`. |

The controller must serialize event objects with the JSON Canonicalization
Scheme (JCS), RFC 8785. `schema.json` must be a JSON Schema Draft 2020-12
document encoded as UTF-8. It must identify `JCS-RFC8785`, define both event
and summary objects, require the fields and values in this specification, and
reject unknown fields for its schema version. `schema_sha256` is the SHA-256
digest of the exact UTF-8 bytes of `schema.json`. The controller must calculate
each event digest from UTF-8 canonical bytes. Each digest must be a lowercase,
64-character hexadecimal SHA-256 value. Consumers must reject an invalid JSON
object, duplicate JSON object keys, a missing required field, an unknown event
type, a sequence gap, a duplicate event ID, a changed previous digest, or an
invalid event digest.

For sequence one, `previous_digest` must be `null`. For every later sequence,
`previous_digest` must be a digest in the format above. Every event `run_id`
and `session_id` must equal the identifiers in its record-directory path.

## Acquisition fields

Every item event, which is `attempt_finished`, `finalized`, or
`candidate_created`, must include `item_id`. The value must be the stable item
ID from the immutable selected manifest. This field links events for one item.

`attempt_finished` must include these fields. The controller must include every
field, even when its value is `null`.

| Field | Type and requirement |
| --- | --- |
| `item_id` | Stable selected-manifest item ID. |
| `source_url` | Exact requested URL. |
| `attempt_number` | Positive integer. |
| `request_started_at`, `request_finished_at` | UTC RFC 3339 timestamps. |
| `final_url` | Final URL string, or `null` when no response URL exists. |
| `redirect_chain` | Array of zero or more URL strings. |
| `http_status` | Integer from 100 through 599, or `null`. |
| `outcome` | One of `success`, `retryable_failure`, `unavailable`, `access_denied`, `validation_failed`, or `stopped`. |
| `response_content_length`, `response_content_range`, `response_content_type`, `response_etag`, `response_last_modified` | Raw response-field string, or `null`. |

The record must never include request cookies, authorization values, Tor
control cookies, RPC secrets, or monitor capability tokens. The controller must
reject a source or redirect URL with user-info credentials or query values that
grant access. It must record an unavailable or redacted value explicitly rather
than silently omit a required field.

`finalized` must include `item_id`, `source_url`, `logical_relative_path`,
`final_relative_path`, `byte_count`, `sha256`, `validation_method_version`, and
`finalized_at`. `byte_count` must be a nonnegative integer. `sha256` must use
the digest format above. Both relative paths must use slash-separated relative
paths and must not contain `.` or `..` path components. `finalized_at` must be
a UTC RFC 3339 timestamp.

`finalized` must also include an `expected_size`, `expected_checksum`, `etag`,
and `last_modified` validation object. Each object must contain `available` and
`compared` Boolean fields and a `value` field. The value is `null` when
`available` is false. `compared` must be false when `available` is false.
When available, `expected_size.value` is a nonnegative integer.
`expected_checksum.value`, `etag.value`, and `last_modified.value` are strings.

`candidate_created` must include `item_id`, `staging_relative_path`,
`candidate_relative_path`, `sha256`, and `finalization_failure_reason`.
`sha256` is `null` when hashing did not complete. The failure reason must be
one of `validation_failed`, `destination_collision`, `unsafe_path`,
`promotion_error`, or `recovery_review`. It must not label a candidate as
evidence final. `staging_relative_path` is relative to `download-state/`.
`candidate_relative_path` is relative to
`download-state/redownload-candidates/`.

`run_started` must include `queue_input_digests`, `selection_settings`, and
`selected_item_count`, which is a nonnegative integer. `run_closed` must
include `closed_at`, `close_reason`, and `durable_outcome_counts`.
`close_reason` must be one of `finished`, `stopped`, `time_limit`,
`local_failure`, or `interrupted`. Every durable-outcome count must be a
nonnegative integer.
`local_failure` must include `failure_code` and a redacted `failure_detail`.
`failure_code` must be one of `record_write`, `database_write`, `permission`,
`storage`, or `other_local_failure`.

## Summary and verification

`summary.json` must include the run ID, session ID, queue-input digests,
selection settings, event count, final event digest, selected-item count,
durable outcome counts, the SHA-256 digest of `schema.json`, and an Ed25519
signature. The signature must cover the JCS canonical summary object without
the signature field. The summary must also identify the signing public-key
fingerprint. `schema_sha256` must use the digest format above. `signature` must
be an unpadded base64url encoding of the 64-byte Ed25519 signature.
`signing_key_fingerprint` must be the lowercase hexadecimal SHA-256 digest of
the 32-byte Ed25519 public key. It must not contain full response bodies or
copied source files.

The operator must retain the trusted Ed25519 public key or its expected
fingerprint outside the record directory. A public key copied only with the
record set is not a trust anchor.

The project must provide a read-only verifier. The verifier must require a
trusted public key or expected public-key fingerprint. It must check the schema
digest, summary signature, event sequence, digest chain, summary final-event
digest, final-file existence, final-file SHA-256 digests, and byte counts. The
verifier must validate the summary and every event against `schema.json`. It
must verify that event and summary identifiers equal their record-directory
path. The verifier must report each mismatch and return a nonzero exit status.
The verifier must never repair, rename, or delete evidence files.

## Acceptance criteria

Implementation is complete when tests show that a completed fixture creates a
valid record set and that each of these changes causes verification failure:

- A changed final file.
- A removed or reordered event line.
- A changed event field.
- A wrong summary final-event digest.
- A changed summary signature or schema file.
- A removed final event line with a replaced matching summary.
- A missing final file.
