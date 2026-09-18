# Handover: implement the four deferred run-wide controller actions

Status: implemented, September 18, 2026. `pause_admission`, `resume_admission`,
`drain_and_stop`, and `checkpoint_stop` are implemented in `src/controller.py`,
`src/tod-dl.py`, and `src/monitor.py`, per the order below. Headless-Textual
interaction tests remain unwritten, same gap as the rest of
[SPEC-controller-control-ui.md](SPEC-controller-control-ui.md)'s command set.
This document no longer hands off open work; it records the plan that was
followed.

## Scope

Implement four run-wide controller actions: `pause_admission`,
`resume_admission`, `drain_and_stop`, and `checkpoint_stop`. Each action's
design is complete and committed in
[SPEC-controller-control-ui.md](SPEC-controller-control-ui.md). This handover
adds no new design; it points to the exact sections that define correct
behavior.

- [SPEC-controller-control-ui.md](SPEC-controller-control-ui.md), "Command
  set and confirmation" section, lines 68-115: the four actions' controller
  behavior, required confirmation, and the durable audit fields (request ID,
  action, outcome, state revision, UTC timestamp) that every mutating action
  must record.
- [SPEC-controller-control-ui.md](SPEC-controller-control-ui.md), "Local
  command transport and authentication" and "Failure handling and recovery"
  sections, lines 41-54 and 152-169: the transport, replay, and
  session-restart rules that already govern `retry_now`; these four actions
  reuse the same request/response envelope and nonce lifecycle unchanged.
- [SPEC-controller-control-ui.md](SPEC-controller-control-ui.md),
  "Verification and delivery" section, lines 171-195: the contract and
  interaction tests every action needs before any live run.

Unlike the five row-scoped queue controls (retry, removal, reordering, queue
editing, export — all implemented; see `SPEC-console-queue.md`), these four
actions are run-wide: none takes an `item_ids` scope, and none is exposed
from the queue pane. They belong to the interactive `--control` session
described in "Outcome and authority boundary", lines 10-39.

## Current baseline in code

- `src/controller.py`: `ControlServer._handle` and `_prepare_confirmation`
  restrict the mutating action set to `{retry_now, exclude_item,
  set_item_priority, set_retry_cooldown, renew_tor_circuits}`. None of the
  four actions in scope here is present in either set.
- `src/tod-dl.py`: `control_state()` (line 589) advertises only
  `get_control_state`, `retry_now`, `exclude_item`, `set_item_priority`,
  `set_retry_cooldown`, and `renew_tor_circuits` in its `actions` map.
  `control_action()` (line 610) dispatches only those same five actions and
  rejects anything else as unavailable. There is no `control_pause_admission`,
  `control_resume_admission`, `control_drain_and_stop`, or
  `control_checkpoint_stop` method.
- `src/tod-dl.py` has one binary `stop_requested` event (set at line 513),
  used both to stop admission and to end the run. `pause_admission` must stop
  only new admission while leaving active transfers running, and
  `resume_admission` must reverse that without a restart, so the existing
  event cannot represent "paused." Implementing these two requires a
  separate admission-gate flag; do not repurpose `stop_requested` for pause,
  or `resume_admission` has nothing distinct to reverse.
- `drain_and_stop` must stop admission and exit only after active validation
  and promotion reach durable states — a graceful wait, not the immediate
  stop that setting `stop_requested` triggers today. `checkpoint_stop` must
  additionally request bounded checkpointing before terminating active
  engines. Neither mechanism exists yet; both need new shutdown-sequencing
  logic distinct from the current run-time-limit and free-space-reserve stop
  paths (see the `stop_requested.set()` call sites already in
  `src/tod-dl.py`'s admission loop).
- `src/monitor.py`: the `Monitor` app binds only `r`
  (`action_prepare_retry_now`) and `t` (`action_prepare_renew_tor_circuits`)
  as run-wide controls, gated by `self.control_state["actions"][name] ==
  "available"` (see `retry_now_eligible`/`tor_renewal_eligible`). No binding
  or eligibility check exists for the four actions in scope. `ActionConfirmation`
  only branches on `"retry_now"`, `"exclude_item"`, and an implicit
  Tor-renewal fallback; it has no message for `pause_admission`,
  `resume_admission`, `drain_and_stop`, or `checkpoint_stop`.

## Implementation order

Follow the existing convention in
[SPEC-controller-control-ui.md](SPEC-controller-control-ui.md) "Command set
and confirmation": add one mutating action at a time, with its confirmation,
audit, telemetry, failure, and headless-Textual tests, before exposing the
next one.

Recommended order, each step gated on the prior step's tests passing:

1. **`pause_admission`.** Lowest risk: read-only for active transfers. Add
   the new admission-gate flag in `src/tod-dl.py`, the confirmation and
   dispatch entries in `controller.py` and `control_action`, then the UI
   binding and confirmation message in `monitor.py`.
2. **`resume_admission`.** Reverses the same gate. Add its controller
   dispatch and UI binding once `pause_admission` is proven; verify it
   reopens admission only for the immutable selected run, per the command
   table.
3. **`drain_and_stop`.** Requires the new graceful-wait shutdown sequencing
   described above. Verify it does not exit until active validation and
   promotion durably complete, and that it still stops admission
   immediately.
4. **`checkpoint_stop`.** Highest risk: needs bounded checkpointing before
   terminating active engines. Implement last, after the drain sequencing
   from step 3 exists to build on.

## Definition of done, per item

For each of the four items, before moving to the next:

- Add the controller-side handling: the `actions` entry in `control_state()`,
  the dispatch branch in `control_action()`, and the new `control_*` method
  in `src/tod-dl.py`; the confirmation-scope branch and dispatch entry in
  `src/controller.py`.
- Add the UI binding, confirmation modal message, and eligibility gate in
  `src/monitor.py`, following the `retry_now_eligible`/`tor_renewal_eligible`
  pattern.
- Add contract and interaction tests using a fake controller and temporary
  state directory, per
  [SPEC-controller-control-ui.md](SPEC-controller-control-ui.md)
  "Verification and delivery" (lines 171-195).
- Run the full test suite once, fix every failure, then run it once more to
  confirm, per the project's testing convention. Do not probe with repeated
  suite reruns.

## Non-goals

Do not add an `item_ids` scope, a queue-pane binding, or any row-selection UI
to these four actions; they are run-wide by definition in the command table.
Do not implement a separate "Control" tab or view; the existing pattern of
gating run-wide bindings directly on the `Monitor` app (as `retry_now` and
`renew_tor_circuits` already do) satisfies the "Control" view requirement in
"Textual application modes and update loop," lines 140-150.
