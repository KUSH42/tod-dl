# Open development work

This document lists the work that remains after the current implementation.
It separates implemented features from broader specifications that remain
incomplete. It does not authorize a source request or an unattended run.

## Current implementation

The controller has durable selected-run state, no-overwrite finalization,
recovery tests, signed provenance records, telemetry snapshots, and local
confirmed controls for retry, Tor renewal, pause admission, resume
admission, drain and stop, and checkpoint and stop. The repository test
suite runs 176 local tests. These features do not establish compliance
with every requirement in the related specifications.

## Remaining work

Finish the acquisition tool evaluation. The local fixture harness and an
adapter that drives the real per-URL aria2 process configuration are
available. A second evaluation report
(`specs/reports/acquisition-tool-evaluation-2026-09-18b.md`) ran 12 of 14
scenarios: E01, E03, E04, E07, E08, E09, E10, E12, E13, and E14 pass; E05 and
E06 fail on two distinct, real, confirmed controller gaps. E02 (8 GiB
interrupted transfer) did not run this session — the deterministic fixture
generator sustained only ~3.4 MB/s, too slow to finish within the harness's
600s per-attempt time limit; this is a still-unmeasured generator-throughput
question, not a controller gap (the `is_incomplete_body_failure()` resume fix
itself is confirmed working). E11 (five-item run from a larger queue) has no
harness built yet. The two confirmed gaps:

- **E05/E06 share a root-cause family in `scope_run()`/`reconcile_promotions()`
  ordering.** `scope_run()` (`src/tod-dl.py`) runs before
  `reconcile_promotions()` and unconditionally stamps `existing_unverified`
  on any queued URL whose final path already exists on disk, regardless of
  its durable status. This pre-empts `reconcile_promotions()`'s dedicated
  handling of a `promoting`-status row left by a killed supervisor: that
  branch only ever fires when the crash happens before the final file link
  exists (E06's `post_validation_intent` failpoint), never after
  (`post_final_file_creation`, `post_completion_commit`). E05's
  engine-kill sub-case hits the same bug, plus a second, related one where a
  fully-completed `--continue=true` retry can leave a stale `.aria2` control
  file behind that `Downloader.transfer()` misreads as failure despite
  `ok=True`. See the report's "Remaining gaps" section for the full trace.

Rerun E02 and build E11's harness before selecting a configuration. Fix the
`scope_run()`/`reconcile_promotions()` ordering gap (and the related stale
`.aria2`-control-file false-failure) before re-running E05/E06. Add a
long-lived RPC worker only when the results show a mandatory scheduling,
recovery, or resource gap unrelated to the above. Do not run a source pilot
until the evaluation selects a configuration.

Complete the reliable-acquisition contract after tool selection. The remaining
work includes full engine lifecycle checks, bounded large-queue admission,
representation-change handling, migration support, and acceptance evidence.

Complete the provenance and fault-recovery acceptance suites. Compare the
existing implementation with both specifications. Add each missing fixture
case before you claim complete compliance.

Complete telemetry integration. Use the read-only aria2 RPC interface for exact
live transfer counters. Add tests for stale samples, snapshot failures, and all
required metric-quality states.

Complete the monitor and control UI. Add the planned Errors / review tab.

Add inventory discovery last. Implement local snapshot parsing, reproducible
manifest generation, and safe queue export. Add bounded network refresh and
directory discovery only after the basic acquisition workflow is reliable.

Add row select for the event list in the Activity tab.

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
- Planned: inventory discovery.

## Next steps

Fix the `scope_run()`/`reconcile_promotions()` ordering gap behind E05 and
E06's failures, measure and speed up E02's fixture generator, build E11's
harness, and rerun all 14 scenarios. Record the selected engine
configuration before you expand acquisition behavior.
