# Open development work

This document lists the work that remains after the current implementation.
It separates implemented features from broader specifications that remain
incomplete. It does not authorize a source request or an unattended run.

## Current implementation

The controller has durable selected-run state, no-overwrite finalization,
recovery tests, signed provenance records, telemetry snapshots, and local
confirmed controls for retry, Tor renewal, pause admission, resume
admission, drain and stop, and checkpoint and stop. The repository test
suite runs 167 local tests. These features do not establish compliance
with every requirement in the related specifications.

## Remaining work

Complete the acquisition tool evaluation first. The deterministic local fixture
harness is available. Run E01 through E14 against the current per-URL aria2
process configuration, then write a sanitized selection report. Add a
long-lived RPC worker only when those results show a mandatory gap. Do not run
a source pilot until the evaluation selects a configuration.

Complete the reliable-acquisition contract after tool selection. The remaining
work includes full engine lifecycle checks, bounded large-queue admission,
representation-change handling, migration support, and acceptance evidence.

Complete the provenance and fault-recovery acceptance suites. Compare the
existing implementation with both specifications. Add each missing fixture
case before you claim complete compliance.

Complete telemetry integration. Use the read-only aria2 RPC interface for exact
live transfer counters. Add tests for stale samples, snapshot failures, and all
required metric-quality states.

Complete the monitor and control UI. Add the planned dashboard and review
views. 

Add inventory discovery last. Implement local snapshot parsing, reproducible
manifest generation, and safe queue export. Add bounded network refresh and
directory discovery only after the basic acquisition workflow is reliable.

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

Start with the local acquisition-tool fixture harness. Record the selected
engine configuration before you expand acquisition behavior.
