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
available. The September 18, 2026 evaluation report
(`specs/reports/acquisition-tool-evaluation-2026-09-18.md`) ran 8 of 14
scenarios: E01, E03, E04, E07, E09, and E14 passed; E08 and E12 failed on
real gaps in `src/tod-dl.py`. Both gaps are fixed as of 2026-09-18, with a
regression test each: `Downloader.transfer` now recognizes aria2's short-body
"Got EOF from the server" failure, retains the partial bytes as a review
candidate, and transitions the item to `review_required` instead of
`retry_wait`; `read_queues()` now takes an `on_reject` callback, and
`import_queues` uses it to print a `[queue-rejected] <url>: <reason>` line
for an invalid URL and for a duplicate URL. The fixture-driven E08/E12
scenarios in the evaluation report have not been rerun against the fix. E02,
E05, E06, E10, and E13 did not run and still need their own infrastructure
(an 8 GiB fixture pass, a process-kill injection harness, controller
failpoints, a one-million-row queue generator with an RSS/latency sampler,
and concurrency-timing assertions). Rerun E08 and E12 against the fixture
harness, build the remaining scenario infrastructure, and rerun all 14
before selecting a configuration. Add a long-lived RPC worker only when the
results show a mandatory scheduling, recovery, or resource gap. Do not run a
source pilot until the evaluation selects a configuration.

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

Rerun E08 and E12 against the fixture harness to confirm the 2026-09-18 fix,
then build the infrastructure for E02, E05, E06, E10, and E13. Record the
selected engine configuration before you expand acquisition behavior.
