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
  The queue producer supplies the source-generation identifier as an opaque
  string in the queue line's `generation=` token. It is null when the queue
  line has no token.
- Inventory display-size token, exact expected size if actually available,
  expected checksum and its origin, and available remote validators.
- State, engine job ID, engine instance ID, staging path, staging generation,
  attempt count, next eligible retry time, observed bytes, last measured
  engine total bytes, and last categorized error. Staging generation numbers
  which representation of the item is currently staged. It is null before
  the item's first staging attempt, when it is set to 1. Each later
  representation-change restart decision (see Resume and representation
  integrity below) increments it by 1. Only these two events change it. A
  null value, not 0 or 1, is what "no staging generation recorded" means in
  the table below. It is distinct from the source-generation identifier and
  inventory snapshot hash above, which track the immutable inventory's
  identity instead.
- Request and response timestamps, final URL and redirect chain, HTTP status,
  available content length/range/type, ETag, and Last-Modified values.
- Local SHA-256, validation result and method version, candidate path when
  relevant, and finalization timestamps.

Use append-only attempt and transition records in addition to current state.
Record metadata once per attempt where possible; don't rewrite an ever-growing
run manifest after every worker event. Engine sessions are recovery aids, not
the sole record of completed or failed work.

Capture the last measured engine total bytes into the durable item record on
each attempt that reports a total. This is the same attempt counted in the
Outage and retry behavior section's per-adapter-attempt policy. An
outage-recovery probe from that section never writes into a partial. It
does not count as an attempt. It
must not set or change this field. The most recently reported value replaces
any earlier one within the same staging generation. Starting a new staging
generation for a representation change clears any value captured under the
old generation. A completed record then never reports a total measured
against a different representation. An item that reaches `complete` without
the engine ever reporting a total must keep the field null; do not back-fill
it from an inventory-size estimate. This field is distinct from the
per-attempt available content length recorded above. It is the engine's own
reported transfer total. Capture it because a response content length is
not always present or reliable. The two fields may differ or arrive
independently. This field does not detect or reconcile a later attempt
reporting a total that conflicts with an earlier one in the same generation.
That gap is acceptable only because the field is informational display data
for the console. Validation and finalization decisions must not depend on it.
The Resume and representation integrity section governs the safeguards that
apply to promotion. The table below states whether each terminal state
clears or retains the field; that table is the sole source for this
behavior. Some rows clear the field on state entry, independent of the
staging-generation reset above. The last categorized error field must record
each cause at the grain the table needs. The outage/retry table below groups
"validation mismatch" and "changed remote representation" into one policy
row, for retry purposes only. "No reliable version protection or expected
checksum" is not an outage/retry cause. It comes from the Resume and
representation integrity section instead. Defining the full categorized-error
value set is future work. This spec requires only that the four
review_required causes below stay distinguishable in that field. The
"recorded review code" checked by automatic safe-path remediation in
[SPEC-download-telemetry.md](SPEC-download-telemetry.md) is this same field,
not a separate one; `ENAMETOOLONG` (or the legacy `Errno 36`/`File name too
long` text) is the value it stores for the promotion-failure cause below.

| Terminal state or cause | Field handling |
| --- | --- |
| `complete` | Retain the value. |
| `existing_unverified` | Retain the value. |
| `review_required` — validation mismatch | Retain the value. |
| `review_required` — changed remote representation | Clear the value on entry to this state. Detecting the change enters this state directly, per the outage/retry table below. A recorded restart decision is needed only to leave this state and resume under a new staging generation, not to enter it. The cleared value described a representation the record no longer promotes. |
| `review_required` — no reliable version protection or expected checksum | Clear the value on entry to this state. Unlike the row above, the representation is not known to have changed; it is only unconfirmed. Treat that uncertainty the same as a confirmed change for this field. |
| `review_required` — promotion blocked by a filesystem name-length failure (`ENAMETOOLONG`) | Retain the value. Entering this state is defined in Validation and finalization below, not the outage/retry table. The failure is a destination-naming problem, not a representation change; the staged bytes remain valid, so nothing needs clearing. |
| `unavailable`, with a staging generation already recorded | Retain the value. A routine recheck alone never changes the staging generation. The resume transition below states what happens once the item becomes available again. |
| `unavailable`, with no staging generation recorded | The field has no value to clear, since it was never staged. A later recheck that admits the item as fresh work follows the same admission rule as any newly selected item, in Selection and resource controls below. |
| `excluded` | Retain the value. Exclusion is an operator decision, not a new finding about the item; any prior categorized error stays informational. |

No other terminal state or cause clears the field.

Persist transitions through `queued`, `active`, `retry_wait`, `validating`,
`promoting`, and `complete`. Also support `existing_unverified`,
`review_required`, `unavailable`, and `excluded`. Run-level states include
storage pause, source cooldown, stopped, and finished. Persist state changes
transactionally; restart reconciliation must handle every interrupted
transition.

`excluded` is entered only through the controller's confirmed `exclude_item`
action, defined in
[SPEC-controller-control-ui.md](SPEC-controller-control-ui.md); no outage,
retry, validation, or promotion outcome enters it automatically. It is
reachable directly from `queued`, `active`, `retry_wait`, `review_required`,
or `unavailable`, and it preserves whatever staging generation and digest
were already recorded. It is terminal: no automatic recheck, retry, or
remediation resumes an excluded item, and this release defines no controller
action that returns one to active work. Unlike `review_required`, entry
requires no detected problem; unlike the attempt-ceiling outcome, it does
not depend on retry count. Excluding an item does not alter the immutable
selected set, the recorded queue rank, or any other item's state.

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

An adapter attempt begins when the adapter starts or resumes an engine job for
an item and ends when that engine job exits back to the adapter, by
completion, by failure, or by an adapter-initiated stop. A redirect the engine
follows, a range-resume the engine performs after its own reconnect, and any
retry the engine performs under its own internal backoff happen inside one
engine job; none of them end the attempt or increment the attempt count. The
attempt count increments only when the adapter starts a new engine job after
the previous one exited back to it, under the backoff intervals in the table
below. The outage-recovery probe described later in this section runs outside
any item's engine job; it does not start, end, or count as an attempt for that
item, as already stated for the last-measured-total field above. A daily
404/410 recheck request, described later in this section, is likewise not an
engine job the adapter starts for active transfer; it does not increment the
attempt count either.

| Condition | Required policy |
| --- | --- |
| Connection or Tor failure, timeout, transient 5xx | Retry after 1, 2, 4, 8, 16, then 30 minutes, with up to 20% added jitter. |
| Three consecutive connectivity failures for one origin | Pause new transfers for that origin; allow one recovery probe after backoff, increasing to 30 minutes. |
| HTTP 429 or 503 with valid Retry-After | Wait at least the server's requested interval; persist the deadline. |
| HTTP 404 or 410 | Record unavailable. Recheck at most once per day since the last recheck. A differing source-generation identifier in a new run's manifest may trigger that day's recheck early, instead of waiting out the rest of the day. It still counts as that day's one recheck. Unless a later differing identifier triggers again, the next recheck waits a full day after it. Compare before overwriting the stored identifier with the new run's value. Only two known, differing identifiers trigger it; a null on either side does not. It applies only while the daily recheck is not yet due. A later identifier that differs from the stored one always triggers, also after an earlier early recheck. An unchanged identifier never repeats it. |
| HTTP 401 or 403 | Move the item to `review_required` with review code `access_denied`. Pause the affected scope for review; don't loop or attempt to bypass access controls. The scope is the source origin (URL scheme and authority). The controller admits no new item of that origin while any selected item of the run is `review_required` with review code `access_denied`. Transfers already active continue. The pause ends when no such item remains, and it survives a restart. |
| Disk full, database write failure, permission error | Stop admission and report local failure; don't consume network retry attempts. |
| Validation mismatch or changed remote representation | Move the item to `review_required` and preserve its staging file as a review candidate; no blind retry into the same bytes. |

A recheck request is a normal request, evaluated against the 404/410 row
first: a repeated 404 or 410 leaves the item `unavailable`, still waiting a
full day for the next attempt, the same cadence as any other recheck. A
connection or Tor failure, timeout, transient 5xx, 429/503, or 401/403
during the recheck does not end `unavailable` and does not additionally
retry within that day under that condition's own row above; the item stays
`unavailable` and still waits the same full day for its next recheck
attempt, an extension of the rule that an early-triggered recheck above
still counts as that day's one recheck. Only a response indicating the item
exists again — neither a repeated 404/410 nor one of those other conditions
— ends `unavailable` immediately. It does not by itself return the item to
`queued` or `active`. Feed that response's validators into the resume
decision in Resume and representation integrity below, exactly as for any
other resumed transfer; recheck success
does not exempt the item from that decision, and does not bypass the
`review_required`-first rule that already governs a detected representation
change. If a staging generation was already recorded: a confirmed-unchanged
representation resumes directly under the existing generation into `active`,
the same as any other confirmed-unchanged resume; a confirmed or unconfirmed
change enters `review_required` directly, the same as any other detected or
suspected change, and only a later recorded restart decision moves it to a
new staging generation and `active`. Entering `review_required` or a new
staging generation this way still applies the field-clearing rules in the
table above; the recheck path does not change what those rules clear. If no
staging generation was recorded, the item was never staged, so Resume and
representation integrity's checks do not apply; the recheck admits it as
fresh work under the same admission rule as any newly selected item, in
Selection and resource controls above, entering `queued` directly.

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
neither reliable version protection nor an expected checksum, move it to
`review_required` instead of asserting that mixed versions were ruled out.
If the newly returned strong ETag, or a trusted expected checksum, matches
the value already recorded for the item, the representation is confirmed
unchanged: resume under the existing staging generation directly, without a
restart decision and without moving through `review_required`.

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

If creating the final file fails because a destination path component exceeds
the filesystem's name-length limit (`ENAMETOOLONG`), move the item to
`review_required` and preserve the staging file as a review candidate, the
same as any other blocked promotion; record the cause in the field-handling
table above. Do not retry the same destination path; a name-length failure is
a naming problem, not a transient one.

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

Verify that `exclude_item` moves an item to `excluded` from each reachable
source state, retains its last categorized error value, leaves the selected
set, queue rank, and every other item's state unchanged, and that no outage,
retry, validation, or promotion path enters `excluded` automatically.

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
