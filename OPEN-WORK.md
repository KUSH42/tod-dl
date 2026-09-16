# Open development work

This document lists the work that remains after the current implementation.
It separates implemented features from broader specifications that remain
incomplete. It does not authorize a source request or an unattended run.

## Current implementation

The controller has durable selected-run state, no-overwrite finalization,
recovery tests, signed provenance records, telemetry snapshots, and local
confirmed controls for retry and Tor renewal. The repository test command runs
71 local tests. These features do not establish compliance with every
requirement in the related specifications.

## Remaining work

Complete the acquisition tool evaluation first. Build the deterministic local
fixture harness, run E01 through E14 for aria2, and write a sanitized selection
report. Do not run a source pilot until the evaluation selects a configuration.

Complete the reliable-acquisition contract after tool selection. The remaining
work includes full engine lifecycle checks, bounded large-queue admission,
representation-change handling, migration support, and acceptance evidence.

Complete telemetry integration. Use the read-only aria2 RPC interface for exact
live transfer counters. Add tests for stale samples, snapshot failures, and all
required metric-quality states.

Complete the monitor and control UI. Add the planned dashboard, queue, review,
and detail views. Add pause, resume, drain, and checkpoint-stop controls one at
a time with confirmation, audit, telemetry, and headless UI tests.

Complete the provenance and fault-recovery acceptance suites. Compare the
existing implementation with both specifications. Add each missing fixture
case before you claim complete compliance.

Add inventory discovery last. Implement local snapshot parsing, reproducible
manifest generation, and safe queue export. Add bounded network refresh and
directory discovery only after the basic acquisition workflow is reliable.

## Specification status

The current specification status is grouped below.

- Implemented: `SPEC-tor-circuit-recovery.md`.
- Partially implemented: acquisition fault recovery, provenance, console UI,
  controller controls, download telemetry, and reliable acquisition.
- Planned: acquisition tool evaluation and inventory discovery.

## Next steps

Start with the local acquisition-tool fixture harness. Record the selected
engine configuration before you expand acquisition behavior.
