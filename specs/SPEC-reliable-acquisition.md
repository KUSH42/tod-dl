# Specification: reliable evidence acquisition

Status: partially implemented, September 16, 2026. The downloader implements
selected safety, state, finalization, and recovery behavior. This specification
defines the remaining contract for large downloads from an intermittent source.
Complete the [tool evaluation](SPEC-acquisition-tool-evaluation.md) before you
enable source-pilot or unattended-acquisition behavior that depends on an
engine selection.

## Architecture and boundaries

Use an existing engine for HTTP transfers and resumable partial files. Keep
SQLite as the durable acquisition record and a small local adapter for scoped
job admission, policy, validation, and finalization. Prefer engine features
over custom implementations where their behavior passes the fixture tests.

The data flow is:

```text
Immutable inventory and selected manifest
  -> SQLite acquisition jobs -> bounded engine queue -> staging
  -> validation -> exclusive final-file creation -> completion record
                -> review candidate when validation or collision fails
```

Keep acquired files in `downloaded_files/` read-only. Engines must write only
to staging. Preserve raw inventories, existing extracted outputs, and all
existing final paths. Acquisition completion does not establish successful
IPED parsing, source authenticity, or the absence of malicious content.

## Manifest and durable state

Every run must reference immutable queue inputs by path and SHA-256, a versioned
selection policy, its settings, and a unique run ID. Persist the selected item
set before starting source requests. Reopening the same run must not expand
that set because an input file changed.

Each acquisition item must record these fields, with explicit null values for
unavailable metadata:

- Stable item ID, exact source URL, original path, destination mapping,
  inventory snapshot hash, queue rank, and source-generation identifier.
- Inventory display-size token, exact expected size if actually available,
  expected checksum and its origin, and available remote validators.
- State, engine job ID, engine instance ID, staging path, attempt count,
  next eligible retry time, observed bytes, and last categorized error.
- Request and response timestamps, final URL and redirect chain, HTTP status,
  available content length/range/type, ETag, and Last-Modified values.
- Local SHA-256, validation result and method version, candidate path when
  relevant, and finalization timestamps.

Use append-only attempt and transition records in addition to current state.
Record metadata once per attempt where possible; don't rewrite an ever-growing
run manifest after every worker event. Engine sessions are recovery aids, not
the sole record of completed or failed work.

Persist transitions through `queued`, `active`, `retry_wait`, `validating`,
`promoting`, and `complete`. Also support `existing_unverified`,
`review_required`, and `unavailable`. Run-level states include storage pause,
source cooldown, stopped, and finished. Persist state changes transactionally;
restart reconciliation must handle every interrupted transition.

## Selection and resource controls

Provide these behaviors through the CLI, preserving existing option names
where their meaning remains compatible:

- `--max-files N` selects at most N distinct eligible acquisition items for
  the entire run. Retries use that same set. Zero means no item-count limit;
  negative values are invalid. Existing final paths are reported and skipped
  before selecting new transfer work.
- Dry run applies the same selection rules, reports skips and validation
  problems, and makes no network requests or acquisition-state changes.
- A run time limit stops new admission and checkpoints active work on expiry.
  It must not wait indefinitely for a multi-gigabyte transfer to finish.
- Default to four active transfers, one connection per file, and one second
  between starts. Enforce the ceiling across engine instances and discovery.
- Keep admission bounded as specified in the evaluation. Use indexed database
  selection and bounded iteration; do not materialize all jobs or futures.
- Honor explicit manifest priority and stable ordering. Do not replace queue
  order with alphabetical path order. Resume eligible partials within the same
  priority before admitting fresh items; cooldown jobs don't occupy slots.
- Bound validation work and engine history. Pause admission if validation or
  storage cannot keep up. Hash one file at a time initially.

## Outage and retry behavior

Separate source availability, item-specific errors, and local failures. Store
retry deadlines across restarts and avoid retries at multiple policy layers.
Use one engine attempt per adapter attempt when the adapter owns backoff.

| Condition | Required policy |
| --- | --- |
| Connection or Tor failure, timeout, transient 5xx | Retry after 1, 2, 4, 8, 16, then 30 minutes, with up to 20% added jitter. |
| Three consecutive connectivity failures for one origin | Pause new transfers for that origin; allow one recovery probe after backoff, increasing to 30 minutes. |
| HTTP 429 or 503 with valid Retry-After | Wait at least the server's requested interval; persist the deadline. |
| HTTP 404 or 410 | Record unavailable; recheck no more than once per day or after a new inventory generation. |
| HTTP 401 or 403 | Pause affected scope for review; don't loop or attempt to bypass access controls. |
| Disk full, database write failure, permission error | Stop admission and report local failure; don't consume network retry attempts. |
| Validation mismatch or changed remote representation | Preserve a review candidate; no blind retry into the same bytes. |

An outage must not mark the remaining million items failed one by one. Probe
only an in-scope item, through Tor, without writing into an active partial.
One successful probe permits a staggered recovery; repeated failure keeps the
origin paused. Healthy active transfers need not be canceled for another
item's failure. Transient retries continue until stopped or a configured
attempt/time ceiling is reached; unavailable items must not prevent a run
from reporting an unresolved outcome and exiting.

## Resume and representation integrity

Keep each engine's partial file and control metadata together. Disable silent
restart, overwrite, and automatic renaming. Check the installed engine's
behavior when a server ignores Range or returns an inconsistent Content-Range.

Record remote validators when available. If the representation changes,
retain the old partial and use a new staging generation only through a
recorded restart decision. Never append new-version bytes to an old version.
Prefer a strong ETag with conditional resume or a trusted expected checksum.
Size and Last-Modified alone are weaker evidence. If a resumed transfer has
neither reliable version protection nor an expected checksum, retain it for
review instead of asserting that mixed versions were ruled out.

Resume and hashing must use bounded memory independent of file size. A slow
but progressing file must not hit a total-transfer timeout merely because it
is large. Record progress from engine byte counters; allocated file length is
not a reliable measure of downloaded bytes.

## Validation and finalization

Before promotion, require confirmed engine success, closed writers, completed
control metadata, a regular staging file, and a computed SHA-256. Require exact
size/checksum matches when those expectations are available. A local hash
records acquired bytes; it is not proof of correspondence to the source.

Rounded inventory sizes are triage metadata, not exact lengths. Implement a
documented rounding/unit interpretation only when supported by the listing
format. Otherwise preserve the token and mark size comparison unknown. Block
promotion for a material discrepancy or an obvious error body inconsistent
with the expected file type. A type check must accept legitimate HTML items
and unknown binary formats; don't treat every unfamiliar file as corrupt.
Support genuine zero-byte files through the same success checks.

Finalize with this crash-recoverable sequence on the same filesystem:

1. Flush the staging file and durably record its hash and promotion intent.
2. Create the final file exclusively, without following unsafe parent
   symlinks or replacing any destination. Flush the destination directory.
3. Commit the completion record. Retain staging until that commit succeeds.
4. Remove only the redundant staging link and record cleanup completion.

If the destination already exists, preserve it. Record `existing_unverified`
unless its expected identity and hash have been established. If it appears
during promotion, preserve the incoming file under a unique candidate path
created without replacement. Record both paths and the reason. Restart must
reconcile a durable promotion intent with the existing final file rather than
blindly labeling it complete or discarding its computed hash.

Preserve existing encoded-name conventions during migration. New mappings
must explicitly reject traversal, encoded separators that become traversal,
symlink escapes, and collisions between different source identities. Retain
the exact URL even if storage uses a hashed long-name fallback. Validate
redirect targets against configured source scope before following them.

## Process, storage, and operator behavior

Use one exclusive ownership mechanism covering the destination and all engine
writers, including when another state directory is supplied. On startup,
reconcile surviving engines before issuing jobs. Identify processes by more
than a stale PID. Never start a second writer against the same partial.

On SIGINT or SIGTERM, stop admission, checkpoint jobs, stop owned engines,
reconcile outcomes, and release ownership after writers exit. Unexpected
termination must be recoverable with the same input identity. An engine
failure must not discard the remaining queue.

Check free space before admission and periodically during transfers. Start
with a configurable 10 GiB free-space reserve; include known remaining sizes
of admitted work when assessing available space. Unknown sizes require
ongoing monitoring. Low space pauses admission and active writes before the
reserve is consumed where possible; actual ENOSPC must also be handled.

Status must show selected, queued, active, completed, existing-unverified,
unavailable, review, and retry counts; current throughput; last success;
cooldown deadline; and free space. Rotate runtime logs with a configured
retention policy while preserving attempt provenance. Use UTC timestamps and
avoid printing full private paths by default.

Exit 0 only when the selected run has no unresolved work and all selected
items satisfy validation. Exit 1 for unresolved work or operational failure,
2 for invalid invocation, 130 for SIGINT, and 143 for SIGTERM. A time-limited
run with remaining work exits 1 and records the reason. Persist enough status
to distinguish a planned stop from a crashed process.

## Migration and delivery order

Deliver this in small stages, without touching acquired artifacts:

1. Fix whole-run selection and dry-run consistency; add fixture coverage.
2. Complete the engine evaluation and record the selected configuration before
   enabling any transfer behavior that depends on engine-specific resume,
   validator, or shutdown semantics.
3. Add durable bounded admission, outage policy, and process recovery.
4. Add validation, promotion reconciliation, and operator status.
5. Reconcile existing SQLite rows and staging through a versioned migration.
6. Run local acceptance tests, a bounded source pilot, and a bounded production
   run before enabling continued unattended acquisition.

Before migration, stop all writers and take a consistent SQLite backup using
its backup API; copying only the database file in WAL mode is insufficient.
Retain old staging and control files. Adopt existing aria2 partials only after
compatibility checks. Preserve curl `.part` files separately when compatibility
is uncertain. Don't claim existing final files are newly verified or silently
reset failures, hashes, or attempt history.

Rollback must use a documented compatible application/schema pair and a
reconciled database. Never restore an old database over new completion history
or point an untested engine at another engine's partials. If compatibility
cannot be established, stop safely with state intact. The old RFC's curl
rollback instruction is not an operational rollback plan.

## Acceptance and next steps

All E01 through E14 scenarios in the evaluation must pass for the selected
implementation. Record results in a dated report, including unresolved source
limitations. Until that report exists, label the implementation as
`unselected` and don't claim compliance with this specification. Compare raw
inventory and existing-final hashes before and after isolated tests and the
pilot; no acquired bytes may change.

For downloader code changes, also run the repository's required Python compile,
Bash syntax, one-item dry run, and whitespace checks. There is no npm formatter
or package-manager workflow in this repository. Deliver code, fixture tests,
configuration examples, and a concise runbook for start, status, pause, resume,
storage recovery, and candidate review. Update AGENTS.md's stale curl
description when implementation lands.
