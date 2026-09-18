# Handover: implement the five row-scoped queue controls

Status: planned. This document hands off implementation work. It does not
change spec status or authorize a source request.

## Scope

Implement five row-scoped controls in the queue view: retry, reordering,
removal, export, and queue editing. Each control's design is complete and
committed in these three specs. This handover adds no new design; it points
to the exact sections that define correct behavior.

- [SPEC-console-queue.md](SPEC-console-queue.md), "Scope and layout" section,
  lines 210-249: UI behavior, scope rules, and the filtered/paginated-rows
  rule for all five controls.
- [SPEC-controller-control-ui.md](SPEC-controller-control-ui.md), "Command
  set and confirmation" section, lines 67-113: the `set_item_priority`,
  `exclude_item`, and `set_retry_cooldown` actions, their confirmation and
  audit requirements, and per-item validation against the selected set.
- [SPEC-reliable-acquisition.md](SPEC-reliable-acquisition.md), "Acceptance
  and next steps" section, lines 350-355: the `excluded` durable state that
  `exclude_item` (removal) must produce.

## Current baseline in code

- Row-scoped retry (step 1 below) is implemented: `ControlServer` binds an
  `item_ids` scope to the confirmation nonce and re-injects it at execution
  time so a client cannot widen scope after confirming; `control_retry_now`
  in `src/tod-dl.py` filters to the requested item IDs; `QueuePane` binds
  `R` to `action_prepare_retry_selected`, distinct from the run-wide `r`
  control.
- `src/controller.py`: `ControlServer._handle` accepts only
  `get_control_state`, `prepare_confirmation`, `retry_now`, and
  `renew_tor_circuits`. `_prepare_confirmation` restricts confirmation to
  the same two mutating actions.
- `src/monitor.py`: `QueuePane`'s `BINDINGS` have no key bindings or actions
  for reordering, removal, export, or queue editing.
- No `excluded` state, `set_item_priority`, or `set_retry_cooldown` handling
  exists anywhere in `src/`.

## Implementation order

Follow the existing convention in
[SPEC-controller-control-ui.md](SPEC-controller-control-ui.md) "Next steps":
add one mutating action at a time, with its confirmation, audit, telemetry,
failure, and headless-Textual tests, before exposing the next one.

Recommended order, each step gated on the prior step's tests passing:

1. **Row-scoped retry.** Lowest risk: reuses the existing `retry_now`
   action and confirmation path. Add the `item_ids` parameter in
   `controller.py`, then the row-selection UI and a distinct key binding in
   `monitor.py`.
2. **Removal (`exclude_item`).** Add the `excluded` durable state to the
   state machine, then the controller action, then the UI binding. This
   state also affects `SPEC-download-telemetry.md`'s display-bucket table,
   already updated; verify the telemetry code path renders it.
3. **Reordering (`set_item_priority`).** Controller action, then UI. Verify
   it never renumbers rank or reorders pagination.
4. **Queue editing (`set_retry_cooldown`).** Controller action, then UI.
   Verify it stays within controller-defined bounds and does not bypass
   global or origin cooldown.
5. **Export.** No controller action; read-only. Implement last since it has
   no confirmation or audit path to reuse from the other four, and depends
   on the final row-selection UI built for the others.

## Definition of done, per item

For each of the five items, before moving to the next:

- Add the controller-side handling (skip for export).
- Add the UI binding, confirmation modal, and selection scoping.
- Add contract and interaction tests using a fake controller and temporary
  state directory, per
  [SPEC-controller-control-ui.md](SPEC-controller-control-ui.md)
  "Verification and delivery" (lines 169-188).
- Add the matching acceptance-criteria checks from
  [SPEC-console-queue.md](SPEC-console-queue.md) "Acceptance criteria"
  (lines 267-272).
- Run the full test suite once, fix every failure, then run it once more to
  confirm, per the project's testing convention. Do not probe with repeated
  suite reruns.

## Non-goals

Do not implement pause, resume, drain, or checkpoint-stop controls in this
work. Those remain separately deferred in
[SPEC-controller-control-ui.md](SPEC-controller-control-ui.md) line 71 and
are out of scope for this handover.
