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

- Row-scoped retry (step 1) is implemented: `ControlServer` binds an
  `item_ids` scope to the confirmation nonce and re-injects it at execution
  time so a client cannot widen scope after confirming; `control_retry_now`
  in `src/tod-dl.py` filters to the requested item IDs; `QueuePane` binds
  `R` to `action_prepare_retry_selected`, distinct from the run-wide `r`
  control.
- Removal (step 2) is implemented: the `excluded` durable state exists in
  `DISPLAY_BUCKETS`/`display_bucket()` in `src/tod-dl.py`;
  `control_exclude_item` validates `item_ids` against `EXCLUDABLE_STATUSES`
  (`pending`, `queued`, `active`, `admitted`, `retry_wait`, `failed`,
  `review_required`, `unavailable`), moves matching items to `excluded`, and
  reports a per-item outcome map; `item_ids` is required (non-empty), unlike
  `retry_now`'s optional scope. `QueuePane` binds `x` to
  `action_prepare_exclude_selected`. The idempotent replay path (a repeated
  `request_id`) returns the original `outcome`/`reason`/`state_revision` but
  not the original per-item map, since `control_requests` does not persist
  it; this matches `retry_now`'s existing replay fidelity.
- `src/controller.py`: `ControlServer._handle` accepts
  `get_control_state`, `prepare_confirmation`, `retry_now`, `exclude_item`,
  and `renew_tor_circuits`. `_prepare_confirmation` restricts confirmation to
  those three mutating actions and requires non-empty `item_ids` for
  `exclude_item`.
- Reordering (step 3) is implemented: the `priority` column exists on
  `downloads` (migrated for existing databases); `control_set_item_priority`
  in `src/tod-dl.py` validates `item_ids` against `PRIORITIZABLE_STATUSES`
  (same set as `EXCLUDABLE_STATUSES`) and a bounded `priority` parameter
  (`PRIORITY_MIN`/`PRIORITY_MAX`, -5 to 5 in `src/controller.py`), sets the
  hint, and reports a per-item outcome map; it never touches `queue_rank`.
  `ControlServer` binds both `item_ids` and `priority` to the confirmation
  nonce and re-injects them at execution time. `QueuePane` binds `]`/`[` to
  raise/lower the focused row's priority by one, each going through the
  standard confirmation flow; `list_queue` and the queue table now surface a
  `Priority` column.
- Queue editing (step 4) is implemented: `control_set_retry_cooldown` in
  `src/tod-dl.py` validates `item_ids` against `COOLDOWN_ELIGIBLE_STATUSES`
  (`retry_wait`, `failed`) and a bounded `cooldown_s` parameter
  (`COOLDOWN_OVERRIDE_MIN_S`/`COOLDOWN_OVERRIDE_MAX_S`, 0 to 3600 seconds in
  `src/controller.py`), and sets `next_retry_at` to `time.time() + cooldown_s`; it
  never touches the separate global/origin SOCKS cooldown gate
  (`wait_for_cooldown`), so it cannot bypass that cooldown. `ControlServer`
  binds both `item_ids` and `cooldown_s` to the confirmation nonce and
  re-injects them at execution time. `QueuePane` binds `}`/`{` to raise/lower
  the focused row's cooldown by 30 seconds, through the standard
  confirmation flow.
- `src/monitor.py`: `QueuePane`'s `BINDINGS` have no key bindings or actions
  for export.

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
