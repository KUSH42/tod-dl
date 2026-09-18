# Open development work

This document lists the work that remains after the current implementation.
It separates implemented features from broader specifications that remain
incomplete. It does not authorize a source request or an unattended run.

## Current implementation

The controller has durable selected-run state, no-overwrite finalization,
recovery tests, signed provenance records, telemetry snapshots, and local
confirmed controls for retry, Tor renewal, pause admission, resume
admission, drain and stop, and checkpoint and stop. The repository test
suite runs 239 local tests. These features do not establish compliance
with every requirement in the related specifications.

A process-ownership review closed five gaps in this session and the four
before it: the exclusive ownership lock now scopes to the destination and
treats SIGTERM as a clean stop; a requeue terminates a surviving writer by
PID and start time before resetting an interrupted transfer; free-space
admission subtracts known in-flight remaining bytes from the reserve
check; disk-full stops admission and resets attempts instead of consuming
a retry; and hashing now serializes to one file at a time, so a lagging
hash holds its worker slot and backs off new admission.

## Remaining work

The acquisition tool evaluation's local phase is done. The third evaluation
report (`specs/reports/acquisition-tool-evaluation-2026-09-18c.md`) ran all
14 scenarios (E01 through E14) against the current per-URL aria2 candidate;
all 14 pass, and engineering targets (RSS, admission bound, status latency,
shutdown time) are met. `selection_eligible` is `true`. The selected
configuration is the current one: one aria2 process per URL through Tor. Do
not add a long-lived RPC worker; nothing in the report identifies a
mandatory scheduling, recovery, or resource gap that would justify one.

Since the second report, two real bugs were found and fixed:

- **`scope_run()`/`reconcile_promotions()` ordering and clobber gap
  (E05/E06).** `scope_run()` was reordered to run after
  `reconcile_promotions()` (commit `8a873d4`), then a second, distinct part
  of the same gap was fixed: `scope_run()` unconditionally re-stamped
  `existing_unverified` on any row whose final file already existed on
  disk, even when a prior run had already settled that row to a terminal
  status (`complete`, `review_required`, `excluded`, `unavailable`,
  `existing_unverified`). Fixed by checking the row's current status first
  (`SCOPE_RUN_TRACKED_STATUSES`, `src/tod-dl.py`). E05 and E06 both pass now.
- **Fixture-generator throughput (E02) and a second, distinct generator bug
  (E10).** Throughput was fixed earlier (commit `3853ab3`, module-level
  block cache, `shake_256`). This session found that `_generate_block`
  still always requested a full 1 MiB digest per block regardless of a
  fixture's actual length — E10's 1,000,000 32-byte fixtures made
  `Fixture.create` alone request roughly 1,000,000 × 1 MiB of digest output,
  so the scenario never finished constructing its fixture list. Fixed by
  bounding the digest length to what each block actually needs.

E08's HTML-200-error and post-hash-mismatch sub-cases are closed. Commits
`a7d289d`, `b387eb1`, and `b676f0e` added expected-checksum detection; all
three E08 sub-cases (short body, HTML-200-error, post-hash-mismatch) now
reach `review_required`, confirmed by re-running
`e08_checksum_mismatch_review()` directly. `acquisition-tool-evaluation-2026-09-18c.md`
predates this fix and still shows the old gap; a fresh dated report has not
been written yet.

One gap remains open, not blocking selection under the specification's
stated gate:

- The source pilot (at most five URLs, separate state, verified SOCKS
  routing evidence) has not run. Do not treat the candidate as validated for
  production use until it does.

Complete the reliable-acquisition contract after tool selection. Engine
lifecycle checks (SIGINT parity with SIGTERM), bounded large-queue admission
(met per E10/E11 and the reserve-accounting, disk-full, and hashing-
backpressure fixes), migration support (`ALTER TABLE` column migrations in
`src/tod-dl.py`), and representation-change handling for the resume path
(`probe_representation()`, `check_representation()`, the
`resume_new_generation` controller action) are done. The 404/410
daily-recheck path (`SPEC-reliable-acquisition.md:190-217`) is done as
well: `transfer()` records `unavailable` with a `recheck_at` deadline, and
the run loop sends one HEAD recheck per due item per day
(`recheck_unavailable()`), then applies the same resume decision. The
early recheck is done too. A queue line's `generation=<id>` token fills the
new `source_generation` column, and `record_source_generation()` compares it
before overwriting. A differing identifier makes the recheck due at once,
through the `early_recheck` column (0 none, 1 pending, 2 spent). A later
differing identifier triggers again, also after a spent early recheck.
`import_queues()` also refreshes `inventory_size` and `expected_sha256` of an
existing row when a later queue changes them.

Acceptance evidence is partly done: `exclude_item` is now verified from all
8 reachable source states (`tests/test_tod_dl.py`), and the runbook covers
storage recovery and candidate review (`README.md`). E01 through E14 were rerun against
the current tree and all pass
(`specs/reports/acquisition-tool-evaluation-2026-09-18d.md`). Still open:
the pilot byte-hash comparison, which requires an actual transfer run. See
`specs/reports/reliable-acquisition-acceptance-2026-09-18.md`.

Provenance and fault-recovery acceptance suites, updated 2026-09-18: every
listed tampering case, consumer rejection rule, recovery row, and scheduler
condition now has a named test. The comparison found and fixed these defects:
the verifier crashed on an unsafe path; it accepted a fingerprint without a
public key and skipped the signature check; it did not validate field values
against the schema; `close_reason`, attempt `outcome`, and candidate reasons
used values outside the specified enums; no `local_failure` event existed; and
a staging-cleanup error moved a completed item to `review_required`.

Still open before the two specs can become `implemented`:

- The controller does not compare `expected_size` with the staged byte count.
  The specification requires an exact size match when the size is available.
- HTTP 401 and 403 now move the single item to `review_required` with review
  code `access_denied`. The specification also says to pause the affected
  scope. The scope is undefined, and no code pauses other items.
- `candidate_created` never records `unsafe_path` or `promotion_error`.
- The writer rejects user-info URLs only. Queue import does not reject them,
  and no rule exists for query values that grant access.
- The recovery tests stop the controller by staged state. Only three named
  failpoints exist (`FAILPOINTS`); none covers staging cleanup or shutdown.

Complete telemetry integration. Use the read-only aria2 RPC interface for exact
live transfer counters. Add tests for stale samples, snapshot failures, and all
required metric-quality states.

Complete the monitor and control UI. Add the planned Errors / review tab.

Add inventory discovery last. Implement local snapshot parsing, reproducible
manifest generation, and safe queue export. Add bounded network refresh and
directory discovery only after the basic acquisition workflow is reliable.

Add on-demand target-directory rescan after inventory discovery. It
reconciles files already in the target directory against the inventory
manifest (path mapping plus checksum verification), from a manual command and
a debounced file-system watch, and produces a dated report plus a candidate
acquisition queue for missing and mismatched items. It depends on
inventory discovery's manifest and on the `expected_checksum` field already
added to acquisition; it does not define either.

Activity and Dashboard are not the same view. `SPEC-console-ui.md` uses
"dashboard" for the whole console screen: the worker table, disk lines, and
the `[Activity] [Queue] [Errors / review]` tab bar. "Activity" is one tab
inside that screen. `src/monitor.py` implements the dashboard screen and the
Activity and Queue tabs. The Errors / review tab from the spec does not
exist yet; that is the remaining "review view" work, not a dashboard rename.

## Specification status

The current specification status is grouped below.

- Implemented: `SPEC-tor-circuit-recovery.md`, worker details, and item
  details.
- Partially implemented: acquisition tool evaluation, acquisition fault
  recovery, provenance, console UI, controller controls, download telemetry,
  reliable acquisition, queue view, console visual style, and console
  inspection.
- Planned: inventory discovery and target-directory rescan.

## Next steps

Run the separately scheduled source pilot (at most five URLs, separate
state, verified SOCKS routing evidence) before treating the aria2 candidate
as validated for production use. Do not run the pilot without an explicit
instruction to do so.
