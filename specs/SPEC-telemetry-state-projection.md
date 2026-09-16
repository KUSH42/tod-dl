# Specification: controller telemetry state projection

Status: implemented, September 16, 2026. This specification removes SQLite
reads from the live telemetry publication path. The controller must publish
snapshots from a controller-owned in-memory projection. The monitor continues
to read only the published snapshot file.

This specification supplements
[download telemetry and progress accounting](SPEC-download-telemetry.md).
Durable acquisition rules take precedence if the two specifications differ.

## Problem and scope

The current publisher calls `telemetry_snapshot()` every 0.5 seconds. That
method reads all selected rows and makes aggregate SQLite queries. A run with a
large selected set therefore repeats database work that does not change the
display.

The change applies only to live snapshot production. It does not change the
SQLite manifest, final-file handling, controller controls, source requests, or
monitor transport. The monitor must not open `manifest.sqlite` for live UI
updates.

## Required architecture

The controller must create one `TelemetryStateProjection` after it finishes
startup recovery and before it starts `TelemetryPublisher`. The controller
must hydrate the projection from SQLite once for each controller session.
Hydration may use aggregate queries that each return one summary row. It must
fetch retry-index rows in batches of at most 1,000 rows. It must not load all
selected rows into one result or collection. Hydration must finish before the
first live snapshot.

After hydration, `TelemetryPublisher.publish()` and its snapshot builder must
not call SQLite, `sqlite3.Connection.execute`, `fetchone`, `fetchall`, or any
database helper. The publisher may read only a short synchronized copy of the
projection and the publisher's runtime records. Snapshot serialization and
file writes must remain outside controller and projection locks.

The projection is disposable derived state. SQLite remains the recovery and
audit authority. A controller restart must discard the old projection and
hydrate a new one from committed SQLite state.

## Projection contents and limits

The projection must contain only values needed for the version-1 snapshot.
It must not keep selected queue rows, full transition history, or stale retry
index entries.

The projection must contain these durable summaries:

- The immutable selected-item count.
- One count for each required display bucket.
- The skipped-existing count outside the selected run.
- The committed complete-byte total and the last completion timestamp.
- The earliest retry deadline, its sanitized error, and the retry-item count.
- The most recent committed run revision represented by the summary.

The projection may keep a retry index for selected items that are in the retry
bucket. The index must keep one stable item ID, deadline, and sanitized error
per retry item. It must contain no stale entries and no more entries than the
immutable selected-item count. The index must order equal deadlines by stable
item ID. This index lets the controller remove or change the earliest retry
without a database query.

The retry-error sanitizer must replace ESC with `?`, collapse whitespace, and
limit the result to 512 characters. It must use the same result as the
sanitizer for telemetry event messages. The projection must add complete bytes
and update the last completion timestamp only when an item enters `complete`.

The projection may keep a bounded index for active items. It must not retain a
full URL-to-item map only to generate summary counts. The existing publisher
records active transfer, validation, lifecycle, event, and engine-sample data.
Those records remain runtime state and are not durable recovery authority.

The projection must calculate `run.counts`, `complete_bytes`, retry health,
and `state_revision` without a database query. The controller can derive
retained bytes and other live metrics from the projection summaries and active
runtime records. A snapshot must preserve the existing schema-version 1 field
names and meanings.

## Commit and update ordering

The controller must store a durable, run-scoped revision. Each committed
snapshot-relevant transaction must increment that revision exactly once. The
transaction must store the new revision with its state change before it
commits. The controller must not use a global transition-row ID or a publisher
sequence as `state_revision`.

The controller must update the projection only after the related SQLite commit
succeeds. A failed or rolled-back database operation must leave the projection
unchanged. A snapshot can lag a committed transition until the controller
updates the projection, but it must never claim an uncommitted transition.

All controller code that changes snapshot-relevant durable state must use a
common mutation path. The path must perform these actions in order:

1. Capture the exact old and new summary values in the transaction.
2. Write the SQLite change, transition or audit record, and new run revision.
3. Commit the SQLite transaction.
4. Apply the captured change to the projection with the committed revision.
5. Request a coalesced telemetry publication.

The common path must cover state transitions, attempt counts, byte counts,
retry deadlines and errors, completion timestamps, recovery requeueing,
safe-path remediation, and accepted control actions. Direct SQL updates that
bypass the projection are prohibited after projection hydration.

If a mutation affects more than one item, the projection update must use the
same committed item set as the SQLite transaction. The controller must not
estimate affected counts from a later query. The capture must include each
item's old and new display bucket, complete-byte contribution, and retry-index
entry when those values can change.

## Startup, shutdown, and failure behavior

During startup, the controller must complete queue selection, recovery, and
durable remediation before it hydrates the projection. Queue selection occurs
before a projection exists, so hydration includes its committed result. The
initial snapshot must represent one committed state after those actions. The
controller may query SQLite during startup and final-outcome recording because
those actions are outside periodic publication.

During shutdown, the controller must record final durable outcomes, apply the
matching projection updates, publish one final snapshot, stop the publisher,
and then close SQLite. The final snapshot must identify its represented
revision.

If projection hydration fails, the controller must stop before admission. If a
projection update fails after a successful database commit, the controller
must stop new admission, report one bounded local telemetry error, and start a
controlled shutdown. The controller must require restart before later
admission. It must not rebuild the projection in the publisher thread or make
another database mutation only to report the projection failure.

If snapshot writing fails, the controller must retain acquisition behavior and
report the bounded telemetry warning. A snapshot write failure must not cause
a database read retry loop.

## Concurrency and consistency

The controller must protect projection updates and projection copies with one
dedicated lock. The publisher must hold that lock only long enough to copy the
projection. It must release the lock before metric reduction, JSON encoding,
file flushing, and atomic replacement.

The projection copy and active-runtime copy may represent adjacent controller
events. Each snapshot must remain internally valid: display counts must sum to
the immutable selected count, byte totals must be nonnegative, and the state
revision must not decrease within a session. The publisher must not wait on a
worker, network operation, or database lock.

## Tests and acceptance criteria

The downloader test suite must add focused tests for this change. Tests must
use temporary state and local fixtures. Tests must not start a source request.

- Verify that initial hydration produces the same snapshot summaries as the
  committed selected-run state.
- Verify that each durable transition updates the projected bucket counts,
  completion bytes, retry fields, and revision after commit.
- Verify that a multi-item mutation increments the run revision once and uses
  its captured preimage and postimage for every affected item.
- Verify that a forced SQLite commit failure does not change the projection.
- Verify that a projection update failure stops new admission without another
  database mutation.
- Verify that recovery requeue, retry-now, Tor-renewal audit, and safe-path
  remediation do not leave stale projection values.
- Verify that the periodic publisher performs zero SQLite calls after
  hydration. Instrument the connection or database helper to fail the test on
  any post-hydration read or write from the publisher thread.
- Verify that a 300,000-item fixture does not scan selected rows on a publish.
  The test must count database calls and prove that publishes use zero calls.
- Verify that snapshot write failure does not cause database reads or block a
  later controller transition.
- Verify that restart discards old projected state and publishes a snapshot
  hydrated from committed SQLite state.
- Verify that the monitor refreshes only `snapshot.json` and never opens
  `manifest.sqlite`.

The project must run these tests in the standard downloader test command. A
code review must reject new snapshot-relevant direct SQL updates that do not
update the projection.

## Next steps

Implement the projection before more monitor views or higher telemetry rates.
Keep the snapshot schema at version 1 unless a separate compatibility change
requires a new major version.
