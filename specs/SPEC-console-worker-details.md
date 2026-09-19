# Specification: console worker details

Status: implemented, September 17, 2026. This view explains one worker slot and
the controller's reason for its current activity or wait.

The view supplements the [console UI](SPEC-console-ui.md) and uses the
[read-only inspection interface](SPEC-console-inspection.md). It must render
labels, values, headers, and highlights under
[SPEC-console-visual-style.md](SPEC-console-visual-style.md).

Planned changes to key bindings and field layout are in
[SPEC-console-keymap.md](SPEC-console-keymap.md) and
[SPEC-console-detail-layout.md](SPEC-console-detail-layout.md). Once
implemented, they override the placement of sample age below, and each
unavailable field carries an `unavailable_reason`.

## Entry and slot identity

**Enter** on a dashboard worker row must open worker details, including for
an idle slot. The view must bind to `(run_id, session_id, worker_id)`.
The header must show the worker number, phase, freshness, read time, and
durable revision. If the detail revision differs from the dashboard revision,
the view must label the difference. It must not merge detail values into
dashboard counts.
Refresh must follow the slot as the controller assigns new items.

The **Item details** action must open the currently displayed item by stable
item ID. If that assignment changes before activation, the UI must retain the
displayed target or cancel with an assignment-change message. It must not
silently open another item. Idle slots must disable the action.

## Required information

`get_worker` must return these fields. Runtime fields must include their
`sample_sequence`, `sample_age_s`, and quality. The service must join runtime
fields only when the item, generation, and attempt identities match. Unknown
values must remain unavailable instead of becoming zero.

- **Assignment:** run and session IDs, worker ID, current item ID and basename,
  generation, attempt ID and number, engine instance and job identity, and
  PID if known. Source URLs and private paths must remain in item details.
- **Activity:** phase, phase reason, phase elapsed time, attempt elapsed time,
  last payload-progress age, sample age, and last transition time.
- **Transfer:** received and total bytes, total source, resume baseline,
  current speed, smoothed speed, ETA, and connection count when available.
  The controller must calculate smoothed speed from positive received-byte
  growth during the latest 30 seconds of the current attempt. The calculation
  must include zero-rate intervals and require ten seconds of fresh samples.
  During warmup, show **Estimating**. The view must label ETA as approximate.
- **Admission:** next eligible start time and separate reasons for stagger,
  worker cooldown, origin cooldown, global cooldown, storage stop, or
  validation backpressure. Show only controller-reported conditions.
- **Validation:** method and progress when the slot owns validation. If
  validation leaves the slot, retain access through the item's details and
  the dashboard validation rows.

Idle slots must show **No item assigned**. If the controller reports no idle
reason, show **Reason unavailable**. The view must clear prior item counters,
PID, errors, and ETA when the assignment changes or becomes idle.

Attempt elapsed time must reset on a new attempt. Phase elapsed time must
reset on a phase transition. Payload-progress age must use received-byte
growth only. The view must not infer reachability from connections or a PID.
After 60 seconds without received-byte growth, a downloading slot must show
**No progress for 60s**. Connecting and cooldown slots must use their phase
elapsed time or countdown instead. This label must not change retry policy.
A countdown reaching zero must show **Eligible; awaiting controller** until
a new sample reports admission. It must not start or retry a transfer.

## Refresh and navigation

The view must apply the console's five-second stale and 15-second disconnected
rules. A stale engine sample must remain visible as stale even with fresh
controller telemetry. Freeze local elapsed values and countdowns when their
source becomes stale. Suppress stale live speed and ETA.
Inspection failure must retain labeled last-known values. It must not imply
that the worker stopped, the item completed, or the assignment ended.

On session replacement, keep the old slot labeled **Session ended** and stop
its refresh. Provide an explicit return to the current dashboard. A matching
worker number in a new session must not silently replace the old worker.

**Tab** and **Shift+Tab** must move focus. Arrow keys must scroll details.
**Escape** must return to the dashboard with the same slot selected when
that slot still exists. **l** must open logs for the displayed item; idle
slots must show **No item logs**. Item log views must remain bound to that
item after reassignment. **?** must explain these keys.

The view must stack fields at 80 columns and remain scrollable below
80 by 24. Refresh and resize must preserve focus and scroll position.
The view must render every basename, identifier, reason, and log value as
literal text. Markup, terminal escape sequences, and terminal hyperlinks must
not execute.
The view must provide no process signaling, worker restart, or Tor controls.

## Acceptance criteria

Headless tests must use synthetic slots and a virtual monotonic clock.

- Verify connecting, downloading, hashing, finalizing, cooldown, and idle.
- Verify assignment changes clear old fields without changing slot identity.
- Verify item activation during reassignment and return from item details.
- Verify attempt resets, phase resets, estimator warmup, and unknown counters.
- Verify the 60-second no-progress label only in the downloading phase.
- Verify separate engine staleness, frozen countdowns, revision differences,
  inspection failure, and session replacement.
- Verify off-slot validation remains accessible after slot release.
- Verify resize, keyboard navigation, literal rendering, and zero commands or
  process signals.
