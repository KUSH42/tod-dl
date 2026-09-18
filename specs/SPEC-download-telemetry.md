# Specification: download telemetry and progress accounting

Status: partially implemented, September 16, 2026. The controller publishes
version-1 snapshots, and the monitor reads them. This document defines the
remaining telemetry contract behind the [console UI](SPEC-console-ui.md). It
supplements the
[reliable acquisition specification](SPEC-reliable-acquisition.md); durable
acquisition rules take precedence over display calculations.

## Current implementation and integration boundary

The current `src/tod-dl.py` launches one aria2 process per URL through
`torsocks -i`. Its progress loop sums staging-file lengths every 30 seconds.
The active dictionary tracks transfers but removes entries before hashing.
SQLite records durable states and attempt outcomes, not exact live transfer
counters. Queue import does not populate the existing `inventory_size` field.
Whole-run selection already exists in `scope_run`; older planning documents
contain historical descriptions of selection that do not match this code.

Do not infer received bytes from file length, completed acquisition from engine
success alone, or runtime worker identity from historical manifest PIDs.
Add an engine-independent telemetry adapter and explicit controller phases.
The [tool evaluation](SPEC-acquisition-tool-evaluation.md) selects the engine
configuration; this contract does not select or change it.

## Sources and authority

Each reported value must identify the source capable of establishing it.

| Source | Owns |
| --- | --- |
| SQLite and committed controller transitions | Selection, queue order, attempts, retry policy, validation, promotion, completion, and run outcome. |
| Engine adapter | Received bytes, transfer total if known, speed, connections, engine job identity, and engine outcome. |
| Controller runtime | Slot assignment, phase, hashing progress, admission reason, stop deadline, and heartbeat. |
| Filesystem inspection | Free space and local file metadata; file length is not transfer progress. |
| Immutable inventory metadata | Original display-size token and any justified estimate, with snapshot/parser provenance. |

For aria2, prefer structured `tellStatus` counters such as `completedLength`,
`totalLength`, `downloadSpeed`, and `connections`. An adapter must distinguish
an unknown total from a confirmed empty file. Reference the
[aria2 RPC manual](https://aria2.github.io/manual/en/html/aria2c.html#rpc-interface).

Before enabling RPC, test loopback binding, secret authentication, permissions,
port allocation, local access under torsocks, and engine lifetime after job
completion. Polling must not keep a finished engine alive indefinitely or
weaken source routing and DNS isolation. The monitor never receives RPC
credentials or calls engine mutation methods. If exact telemetry cannot be
obtained, expose unavailable fields; parsed rounded logs are approximate only.

## Snapshot transport and compatibility

The controller publishes `download-state/telemetry/<run-id>/snapshot.json`
through a temporary file and atomic replacement in the same directory. Keep
one latest snapshot per run, restricted to the local owner. This is disposable
derived state; it must never become a recovery authority.

Publish once per second and after significant state changes, coalescing bursts
to at most two writes per second. Build snapshots from a short synchronized
copy of runtime state; perform serialization and disk I/O outside controller
locks. Engine polling has a one-second timeout and bounded concurrency. A slow
poll must not delay scheduling, shutdown, or other engine samples.

The live snapshot builder must not query SQLite. The controller must hydrate
and update its in-memory durable summary after committed state changes. The
[controller telemetry state projection specification](SPEC-telemetry-state-projection.md)
defines this required boundary, ordering, and test contract.

The envelope contains these required fields; unknown values are JSON `null`.
Byte counters are nonnegative integer bytes, rates are bytes per second,
durations are seconds, and timestamps are UTC RFC 3339 strings.

Version 1 uses `run_id` at the envelope level as the canonical run identifier.
The `run` object does not repeat it. Worker records use `worker_id` as the
stable controller-session slot identifier. A consumer that accepts a fixture
with legacy `run.id` or `slot` must normalize it before validation; a
controller must publish only the canonical names.

Version 2 adds time, trend, and filesystem fields for the console UI. A
controller must not add them to a version-1 snapshot. A version-2 consumer may
read a version-1 snapshot, but it must label each unavailable version-2 value.
The monitor must not invent a value from file timestamps, file lengths, or a
wall-clock estimate.

| Field | Contract |
| --- | --- |
| `schema_version` | Integer major version, initially 1. Unknown major versions produce an explicit compatibility error. |
| `run_id`, `session_id` | Immutable selected run and unique controller invocation; resuming a run creates a new session ID. |
| `sequence`, `published_at`, `session_elapsed_s` | Increasing sequence within a session, publication timestamp, and monotonic elapsed duration. |
| `state_revision` | Committed controller state revision represented by the snapshot. |
| `run` | Lifecycle state, stop/admission reason, creation/start times, elapsed and remaining run time, engine selection status, selected count, mutually exclusive counts, skipped-existing count, last completion time, and metrics. |
| `workers` | Bounded slot records with worker ID, item ID, generation, attempt ID/number, phase/reason, phase age, engine instance/job identity, optional PID, sample age, transfer counters, and connections. |
| `validation` | Bounded active validation records with item ID, method, phase, processed bytes, total bytes, and sample age. |
| `health` | Filesystem identities/free/reserve/headroom, Tor preflight result/time, cooldowns, retry/probe/stagger countdowns, and telemetry errors. |
| `recent_events` | At most 100 sanitized events with stable ID, UTC time, severity, item/worker identity, category, and message. |

In version 2, `run.last_payload_progress_at` is either an RFC 3339 UTC time or
`null`. Set it when any engine adapter reports a strictly positive received-byte
increase. Do not set it for packet activity, connection setup, log output,
file-length inspection, hashing, finalization, or a controller heartbeat. The
field describes observed payload progress, not host reachability. `run` also
contains `completed_at` and `stopped_at`, each either an RFC 3339 UTC time or
`null`. A final lifecycle records the applicable final time.

In version 2, `health.filesystems` is an array. Each record contains an opaque
stable `filesystem_id`, a nonempty `roles` array, `free_bytes`, `reserve_bytes`,
and `headroom_bytes`. The byte fields are signed integer bytes. The controller
sets `headroom_bytes` to `free_bytes - reserve_bytes`. `roles` uses safe role
names and must not contain paths. Publish one record per distinct filesystem.
Publish `health.filesystem_layout_error` when destination and state do not use
the same filesystem. A controller must publish `null` for a measurement it
cannot obtain and include a bounded telemetry error. It must not publish a
cached value as a current measurement.

Run metrics include `retained_bytes`, `complete_bytes`,
`session_received_bytes`, `known_total_bytes`, `known_remaining_bytes`,
`unknown_size_items`, `speed_bps`, `average_speed_bps`, `eta_seconds`,
`eta_reason`, `estimated_finish_at`, and `quality`. Quality distinguishes exact,
estimated, partial, stale, and unavailable values. Supply quality per metric
when aggregate quality alone would be ambiguous.

In version 2, every `speed_bps` metric includes a `quality` value. The values
are `exact`, `estimated`, `partial`, `stale`, or `unavailable`. A consumer uses
only `exact` and `estimated` aggregate speed values for a speed trend. It treats
the other values as gaps. The controller publishes raw current speed only. The
monitor owns its bounded display history and direction label.

Worker transfer counters include `received_bytes`, `total_bytes`,
`total_source`, `resume_baseline_bytes`, `speed_bps`, `last_progress_age_s`,
and `sample_sequence`. Inventory token, estimate, and engine total remain
separate fields when both exist. Use stable item IDs; never key UI state only
by basename, URL abbreviation, or PID.

Keep queue rows and complete attempt history out of the snapshot. The planned
[read-only inspection service](SPEC-console-inspection.md) owns their access.
Only that controller-owned service may use indexed, paginated SQLite reads
for console details, with short transactions and bounded lock waits.
The monitor must never open SQLite. The snapshot publisher must retain its
zero-query boundary. Include read revision/time in details; if a detail query
differs from the snapshot revision, label it instead of mixing values into
snapshot counts. Never enable SQLite immutable mode on a live database.

## Freshness, failure, and lifecycle

Consumers ignore older sequences within a session and reset their local speed
history on session change. Use monotonic time for intervals and countdowns;
wall-clock adjustments must not change speed or retry duration. On initial
attach use publication age, then track advancing sequences with a local
monotonic clock. Flag implausible clock differences explicitly.

Mark controller telemetry stale after five seconds without an advancing valid
snapshot and disconnected after 15 seconds. Mark an engine sample stale after
five seconds even if the controller heartbeat advances. Hold last-known values
with their timestamp and suppress affected live metrics. A recorded final
snapshot is a finished result, not a stale live controller.

On malformed or partial reads, retain the last valid snapshot and report the
error. Missing telemetry permits persisted status with live metrics unavailable.
An old controller requires a normal restart before new telemetry exists; a
monitor must not inject into or restart it. Unsupported schema is an explicit
error, not an empty successful run.

Emit lifecycle snapshots for starting, running, cooldown/storage pause,
stopping, stopped, and finished, with a reason and unresolved count. Publish
final state only after durable outcome recording. Unexpected controller loss
remains disconnected until recovery establishes its outcome.

Telemetry write failure reports a bounded warning through the existing log
channel and retries publication without a busy loop. It does not independently
mark a transfer failed. Database or storage failures still invoke the existing
acquisition safety policy. No monitor failure can suppress that policy.

## Phases and count reconciliation

Display phases refine durable states; they do not redefine completion.

| Durable condition | Display bucket or phase |
| --- | --- |
| Pending or queued | Queued. |
| Admitted or active | Busy: stagger wait, cooldown wait, connecting, or downloading, when reported. |
| Validating | Busy: validation pending, checking, or hashing. |
| Promoting | Busy: finalizing. |
| Retry wait or retryable failure | Retry, with persisted eligibility countdown. |
| Attempt ceiling reached | Exhausted; retain the underlying recorded status. |
| Complete | Complete only after the durable completion commit. |
| Existing unverified, review required, unavailable | Separate unresolved buckets. The `unavailable` bucket's durable-status trigger, and its resume transition to `active`, `review_required`, or `queued`, are defined in [SPEC-reliable-acquisition.md](SPEC-reliable-acquisition.md), not here. |
| Excluded | Separate unresolved bucket for an operator-excluded item. Its durable-status trigger is the controller's `exclude_item` action, defined in [SPEC-controller-control-ui.md](SPEC-controller-control-ui.md) and [SPEC-reliable-acquisition.md](SPEC-reliable-acquisition.md); this release defines no transition back to another bucket. |
| Unrecognized legacy state | Unknown state, visible and unresolved. |

Every selected item belongs to exactly one display bucket. Their sum must
equal the immutable selected count, including after restart and filtering.
Skipped existing files outside selection have their own count. Overall
acquisition totals are separate from selected-run totals. A transfer can show
100% received while remaining busy during validation or finalization.

Hash progress is bytes processed by the required hashing pass divided by its
known file size. Instrument that pass without adding a second evidence read.
Do not attach network speed to hashing. Until progress callbacks exist, show
an indeterminate hashing phase with elapsed time.

The initial aria2 adapter publishes `null` for live transfer counters until
the required read-only RPC evaluation has passed. It may report durable
completion and in-process hash progress, but it must not substitute staging
file length, log parsing, or a PID-derived estimate for received bytes.

## Byte accounting and estimates

Keep transfer activity, retained progress, and committed completion distinct.

- `complete_bytes` sums committed complete items once per selected item.
- `retained_bytes` sums completed items and usable received bytes in current
  selected generations. Exclude suspect candidates and unverified finals.
- `session_received_bytes` accumulates positive engine counter deltas within
  attempts after establishing each resume baseline. Never add the baseline
  again on retries. This measures observed payload progress, not network-layer
  traffic; label it partial if sampling gaps or resets prevent exact accounting.
- `known_total_bytes` sums justified totals for selected items with known sizes.
  `known_remaining_bytes` sums their nonnegative remaining transfer bytes.
  Count unresolved unknown-size items separately.
- A confirmed empty file has known total zero. Engine total zero before size
  discovery remains unknown. Never divide by zero or infer successful transfer.
- A new generation or counter decrease invalidates that attempt's speed
  history. Record the discontinuity; never produce negative speed or silently
  carry old bytes into a changed representation.
- Use engine totals for transfer progress. Preserve trusted expected sizes and
  inventory tokens separately; a disagreement is a validation concern, not a
  reason to rewrite provenance. Show approximate inventory estimates with `~`.

Retries can increase session traffic without increasing retained progress.
Retained progress can decrease after explicit invalidation; show an event
explaining it. Moving a file from staging to complete must not double-count it.
Format binary units as KiB, MiB, and GiB and expose exact bytes in details.

## Speed and ETA calculations

Publish raw counters and centralized derived metrics so all displays agree.

Sum fresh engine rates for current aggregate speed. If any active transfer
sample is stale or unavailable, mark the sum partial and suppress full-run ETA.
Keep a bounded 300-second history. File ETA uses positive progress per elapsed
second over the latest 30 seconds of the current attempt, including zero-rate
intervals; require at least ten seconds of fresh observations.

Run ETA uses net retained-byte gain over the latest 300 seconds, including
connection, retry, cooldown, and validation idle time. Require at least 60
seconds of observations. Exclude resume baselines and reset the estimate window
when representation invalidation changes retained progress discontinuously.

Calculate transfer ETA as remaining transfer bytes divided by the relevant
positive smoothed rate. Round to two significant units and label it approximate.
Estimated finish time is publication time plus ETA and must identify UTC.
This is a throughput projection, not a promise or a scheduling simulation.

Suppress full-run ETA when any unresolved size is unknown, metrics are stale,
the rate is nonpositive, the run is paused/stopped, or selected work requires
review, is unavailable, or is exhausted. Show the reason. Known-size remaining
bytes may still be shown alongside the unknown count. During estimator warmup,
show **Estimating**. Do not issue extra source requests just to discover sizes.

Show **No progress for 60s** after 60 seconds without received-byte growth in
the downloading phase. This is a display observation, not a new timeout or
retry policy. Connecting and cooldown have their own elapsed/countdown labels.
When remaining transfer bytes reach zero but validation remains, replace ETA
with **Validating remaining files**. Only the durable run outcome reports
successful completion.

## Automatic safe-path remediation

The controller may automatically promote a completed staging artifact that is
held solely because a legacy destination component exceeded the filesystem name
length. This recovery preserves the artifact, its source identity, and an
auditable mapping. It is a controller operation; the Textual monitor remains
read-only and must not invoke, approve, or retry remediation.

### Scope and eligibility

Remediation runs during a normal controller start after run selection and
promotion reconciliation, but before new transfer admission. It does not run
for `--dry-run` or `--status`, and it considers only items in the immutable
selected run. This prevents a new queue or a status inspection from changing
unrelated evidence.

An item is eligible only when all of these conditions hold:

- Its durable status is `review_required`.
- Its recorded review code — the last categorized error field defined in
  [SPEC-reliable-acquisition.md](SPEC-reliable-acquisition.md), not a separate
  field — is `ENAMETOOLONG`. Legacy rows qualify only when the recorded error
  unambiguously contains both `Errno 36` and `File name too long`.
- Its recorded staging path is an existing regular file on the approved state
  filesystem.
- Its recorded SHA-256 is present, and a new SHA-256 pass over staging matches
  it exactly.
- Its logical `relative_path` is valid and produces a destination beneath the
  configured acquisition root through the deterministic mapping below.

All other review-required conditions, including an existing final, a
destination race, a symlink or non-directory parent, a missing staging file,
an absent or mismatched digest, an unsafe logical path, an inaccessible target,
and every unknown error remain manual review items.

### Mapping and promotion

The replacement destination is derived afresh from the immutable logical path;
the controller must not reuse the failed legacy `promotion_target`. For each
component whose UTF-8 byte length exceeds
`min(240, max(1, PC_NAME_MAX - 1))`, the mapped component is
`__longname__` followed by the lowercase SHA-256 hexadecimal digest of that
component. If the complete resulting destination would exceed 3,800 encoded
bytes, the mapped relative path is `__longpath__` followed by the lowercase
SHA-256 hexadecimal digest of the complete logical path. The mapping uses no
truncation, preserving deterministic collision resistance.

The controller completes an eligible remediation in this order:

1. Recompute the mapped path and confirm that it is safe and below the active
   filesystem limits.
2. Rehash staging and compare the result with the recorded digest.
3. Create and validate destination parents with the same no-symlink checks used
   for ordinary promotion.
4. Record a durable promotion intent containing the mapped `storage_path`, the
   new target, the original logical path, the existing digest, and the reason
   `automatic safe-path remediation`.
5. Create the final with the same exclusive `link(2)` promotion operation,
   then fsync the parent directory.
6. Mark the item complete, emit an audit event, remove staging, and record the
   staging-cleanup completion only after the durable completion transition.

The controller must never overwrite a final. If the mapped destination already
exists or appears during promotion, it must retain staging, leave the item in
manual review, and record the collision. A successful remediation counts as a
normal durable completion; it does not alter the immutable run selection.

### Audit, telemetry, and recovery

The database must retain the original logical path and source URL unchanged.
It must persist the mapped `storage_path`, final target, prior review code,
remediation reason, digest, and timestamps for intent, promotion, and staging
cleanup. The manifest or durable event history must record the original review
state and the deterministic mapping version so the final location is
reproducible after future code changes.

Telemetry publishes one sanitized `info` event for a successful remediation
and one sanitized `warning` event when an eligible item cannot be remediated.
The event identifies the item and outcome without exposing credentials or an
unredacted source URL. The monitor may show these events, but it cannot mutate
their item or controller state.

If the controller stops after intent recording, normal promotion reconciliation
must verify the mapped final against the recorded digest before marking the
item complete. If it cannot prove that result, it must preserve staging and
return the item to manual review. Retrying automatic remediation is idempotent:
it must either confirm the same final or make no evidence-changing action.

### Acceptance criteria

The implementation must demonstrate the safety boundary with focused tests.

- A selected legacy `ENAMETOOLONG` item with a matching staged digest promotes
  to the documented deterministic mapped path without redownloading.
- A path component over the byte limit, a complete path over 3,800 bytes, and
  multibyte Unicode components produce stable mappings.
- A mismatched or absent digest, an invalid staging object, an unsafe parent,
  and a preexisting mapped final remain review-required and retain staging.
- An unselected review item, `--dry-run`, and `--status` do not mutate state or
  final paths.
- An interruption after intent, before link, and after link reconciles without
  overwriting, duplicate completion, or staging loss.
- Persisted audit fields and monitor events identify an automatic remediation
  without replacing the original logical path or source identity.

## Verification and delivery

Implement a pure metric reducer, a bounded telemetry publisher, an engine
adapter, and fixture-driven monitor integration. Use synthetic data and a
virtual monotonic clock for metric tests.

Required cases include resume baselines across multiple attempts, counter
resets, changed generations, zero-byte and unknown-size files, size mismatch,
stale samples, rate drops, clock jumps, cooldown, validation, promotion races,
restart with the same run ID, empty selection, and every unresolved bucket.
Assert count reconciliation and no double counting after finalization.

Exercise atomic replacement under concurrent readers, snapshot write failure,
database contention, malformed schema, bounded event history, and one million
queued items without full-list publication. Measure snapshot size and publish
latency; target at most 256 KiB and 100 ms at the 95th percentile on documented
hardware, using truncated event messages and bounded active records.

For controller code changes, run all checks required by `AGENTS.md`, including
the downloader unit tests, Python compilation, Bash syntax, one-item dry run,
and whitespace checks. UI and telemetry tests do not require a source pilot.
Engine-specific changes remain subject to the separate evaluation gate.

## Next steps

Define version-1 fixtures and implement metric accounting first. Build the UI
against those fixtures, then add telemetry publication to the selected engine
adapter. Deliver a dated validation report that distinguishes passing local
tests from untested source behavior.
