# Specification: controller command channel and interactive UI

Status: partially implemented, September 18, 2026. `get_control_state`,
`retry_now` (including its row-scoped `item_ids` parameter), `exclude_item`,
`resume_new_generation`, `retry_access_denied`, `renew_tor_circuits`,
`pause_admission`, `resume_admission`, `drain_and_stop`, and `checkpoint_stop`
are implemented.
This specification extends the
read-only [acquisition console UI](SPEC-console-ui.md) with a local,
controller-owned command channel.

## Outcome and authority boundary

You can use the existing Textual monitor as an interactive operator console
without making it a second acquisition controller. The downloader remains the
only process that writes acquisition SQLite state, admits transfers, signals
workers, promotes evidence, or changes a run's lifecycle. The UI only sends
bounded requests and displays acknowledged results.

The default launch remains an observer. Interactive mode requires an explicit
`--control` option and a live controller that advertises a compatible command
endpoint. If either condition is absent, the UI displays controls as unavailable
and continues to provide read-only monitoring.

The implemented monitor command is:

```bash
python3 src/monitor.py --state download-state --run-id RUN_ID \
    --control
```

The controller creates the endpoint only while its session is live. The monitor
shows whether it attached and lists controller-advertised actions. To request
an immediate retry, press `r`, then select **Yes** or press `y` in the modal.
Select **No**, press `n`, or press Escape to cancel. The controller accepts the
request only while its 60-second, single-use confirmation nonce remains valid.

> **Warning:** The UI must never execute a shell command, edit SQLite directly,
> signal a process by PID, overwrite a final, renumber queue rank, remove an
> item from the immutable selected set's audit history, or accept an
> operator-supplied destination path.

## Local command transport and authentication

The controller creates one Unix-domain socket at
`download-state/control/<run-id>/controller.sock` for its active session. The
containing directory has mode `0700`, and the socket has mode `0600`. The socket
is removed only after the controller publishes its final lifecycle snapshot.
No TCP listener, Tor listener, or remotely reachable command endpoint exists.

On Linux, the controller verifies the connecting peer's UID with `SO_PEERCRED`.
It also requires a per-session random capability token stored in
`download-state/control/<run-id>/session.json` with mode `0600`. The monitor
reads that token only after the operator has requested `--control`; normal
snapshot monitoring never reads it. The controller rejects a wrong UID, token,
run ID, session ID, protocol version, oversized request, or expired connection.

Each request and response is one UTF-8 JSON object terminated by `\n`, with a
maximum encoded size of 64 KiB. The controller accepts one request at a time
per connection and uses its existing synchronization before changing state. A
request has `protocol_version`, `request_id`, `run_id`, `session_id`, `token`,
`action`, and optional `parameters`. A response echoes `request_id` and reports
`accepted`, `rejected`, `completed`, or `unavailable`, plus a literal-safe,
bounded reason and the controller state revision.

The UI generates a UUID `request_id` once for each operator action and reuses it
after a transport retry. The controller persists that ID with the outcome, so a
replayed request returns the original result and cannot repeat a mutation.

## Command set and confirmation

The implemented command set contains `get_control_state`,
`prepare_confirmation`, `retry_now`, `exclude_item`, `resume_new_generation`,
`retry_access_denied`, `set_item_priority`,
`set_retry_cooldown`, `renew_tor_circuits`, `pause_admission`,
`resume_admission`, `drain_and_stop`, and `checkpoint_stop`. The controller
must validate every precondition
at execution time; the UI state is advisory and can be stale.

| Action | Controller behavior | UI confirmation |
| --- | --- | --- |
| `get_control_state` | Returns command availability and immutable action metadata. | None. |
| `prepare_confirmation` | Returns one confirmation nonce and authoritative action scope. | None. |
| `pause_admission` | Stops new admissions after the current scheduler step. Active transfers continue. | Required. |
| `resume_admission` | Reopens admission only for the immutable selected run. | Required. |
| `retry_now` | Makes selected retryable items eligible now; it does not expand selection. Accepts an optional `item_ids` scope; when present, it applies only to those items instead of every selected retryable item. | Required. |
| `renew_tor_circuits` | Requests `NEWNYM` subject to the configured rate limit. | Required. |
| `drain_and_stop` | Stops admission and exits after active validation and promotion reach durable states. | Required. |
| `checkpoint_stop` | Requests bounded checkpointing, terminates active engines safely, and exits. | Required. |
| `set_item_priority` | Sets a scheduler-preference hint on selected items. It does not change queue rank, manifest order, or pagination order. | Required. |
| `exclude_item` | Moves selected items to the `excluded` durable state, defined in [SPEC-reliable-acquisition.md](SPEC-reliable-acquisition.md), stopping future admission and retry. It does not remove the item from the selected set's audit history or renumber remaining ranks. | Required. |
| `resume_new_generation` | The recorded restart decision defined in [SPEC-reliable-acquisition.md](SPEC-reliable-acquisition.md)'s Resume and representation integrity section. Moves a `review_required` item whose review code is `changed_remote_representation` or `no_reliable_version_protection` back to `queued`, increments its staging generation, and clears its review code, last error, and recorded validators so the next attempt starts a fresh representation baseline. Rejects an item in any other state or review code. | Required. |
| `retry_access_denied` | Moves a `review_required` item whose review code is `access_denied` back to `queued`, and clears its review code and last error. It keeps the item's attempts, staged bytes, validators, and staging generation. The origin pause ends when no other `access_denied` item of that origin remains. A repeated HTTP 401 or 403 returns the item to `review_required` and pauses the origin again. Rejects an item in any other state or review code. | Required. |
| `set_retry_cooldown` | Sets a per-item retry-cooldown override within controller-defined bounds. It does not bypass the existing global or origin cooldown. | Required. |

Before any mutating action, the UI asks the controller for an action-specific
confirmation nonce and displays the authoritative scope, expected effect, and
irreversibility. The operator must enter the displayed action verb. The UI then
sends the nonce and confirmation text in the final request. Nonces are single
use, bound to the session and action, and expire after 60 seconds.

For `retry_now`, the controller records a durable audit request ID, action,
outcome, state revision, and UTC timestamp. It also appends a bounded control
transition and emits a sanitized telemetry event after acceptance. It records
only selected `retry_wait` and `failed` items as immediately eligible; it
doesn't alter queue inputs, selection, final evidence, or review items.

For `set_item_priority`, `exclude_item`, `resume_new_generation`,
`retry_access_denied`, and `set_retry_cooldown`, the
controller records the same durable audit fields as `retry_now`: request ID,
action, outcome, state revision, and UTC timestamp, plus a bounded control
transition and a sanitized telemetry event. Each action requires `item_ids`
and validates every ID against the immutable selected set at execution time;
an ID outside that set, or an item whose current state makes the action
inapplicable, is rejected per item, and the response reports the per-item
outcome. None of the five changes queue rank, manifest order, the selected
set's membership, or an item's destination path.

Automatic safe-path remediation remains governed by
[the telemetry specification](SPEC-download-telemetry.md). The command channel
may expose its status, but it must not let an operator supply a replacement path
or bypass the remediation eligibility checks.

## Textual application modes and update loop

The existing `src/monitor.py` UI becomes the shared Textual application
base. It retains snapshot validation, filename safety, worker and event
rendering, event scroll behavior, and noninteractive status output. The
application uses an in-memory view model with separate read-only telemetry and
optional control-state slices.

The application defaults to 30 FPS. A Textual interval schedules an
in-memory render frame every `1 / fps` seconds using `time.monotonic()`. It
does not poll files, SQLite, the command socket, aria2, Tor, or the network on
that interval. Snapshot polling remains capped at 2 Hz, and control-state
polling remains capped at 2 Hz while control mode is active. Command responses
arrive through a bounded async task and atomically replace only the relevant
view-model slice.

Render frames update local countdowns, stalled-duration labels, marquee offsets,
progress display, and changed widgets. They don't rebuild an unchanged table or
log. If rendering is late, the application drops the missed frame rather than
queuing work. It preserves keyboard responsiveness, selection, and manual
scroll position, and it follows the activity log only when the user is already
at its bottom.

The planned UI provides **Dashboard**, **Queue**, **Activity**, **Review**,
and optional **Control** views. **Dashboard** remains the default read-only
view. The [queue specification](SPEC-console-queue.md) defines queue browsing.
The [item](SPEC-console-item-details.md) and
[worker](SPEC-console-worker-details.md) specifications define nested details.
These observer views use the separate
[inspection service](SPEC-console-inspection.md) without control credentials.
**Control** is visible only with `--control`, displays the controller session
and action availability, and opens a confirmation modal for a supported
action. A disconnected or stale controller disables action widgets
immediately. The UI never infers success from a local button press.

## Failure handling and recovery

The console treats a command response as the only acknowledgement of a request.
After a timeout, it reconnects and repeats the same `request_id` only after the
operator chooses retry or the action is explicitly idempotent. It refreshes the
snapshot and control state after every terminal response, then displays both the
controller's outcome and its durable revision.

If the socket disappears, the token is unreadable, authentication fails, or the
protocol is incompatible, control mode becomes unavailable and read-only views
continue. Closing, crashing, or force-killing the UI never cancels a request or
signals the controller. The controller finishes a request transaction before it
acts on a client disconnect.

On controller restart, the socket, session token, and confirmation nonces are
new. The UI discards all pending controls from the old session and requires a
fresh explicit `--control` attachment. It must never resend an old-session
command to a new controller session.

## Verification and delivery

Implementation requires contract and interaction testing before any live run.
The tests must use a fake controller and temporary state directory; they must
not contact a source, Tor, or aria2.

- Verify socket modes, peer UID, token rejection, protocol-version rejection,
  size bounds, malformed JSON, stale session, and token non-disclosure.
- Verify request-ID idempotency, confirmation expiry, wrong confirmation text,
  controller-side precondition checks, and audit records for every outcome.
- Verify each action cannot alter queue inputs, immutable selection, final
  evidence, or an unapproved safe-path review item.
- Verify `exclude_item` rejects an item ID outside the confirmed selected set,
  moves the item to the terminal `excluded` state, and cannot be reversed or
  resumed by any other action. Verify `set_item_priority` and
  `set_retry_cooldown` reject an out-of-bounds value, never renumber rank or
  alter another item's cooldown, and produce an audit record for every
  outcome.
- Verify a 30-FPS render loop reads snapshots and control state no more than
  twice per second, drops late frames, and keeps input latency below 150 ms at
  the 95th percentile on documented hardware.
- Verify activity scrolling, selection, and dialogs survive snapshot updates,
  controller reconnect, resize, and a 60-FPS configuration.
- Kill the UI during every command stage and prove that only the controller's
  audited, durable outcome determines acquisition state.

## Next steps

Every command in "Command set and confirmation" is implemented with its
confirmation, audit, and telemetry event, following the pattern
`retry_now` and `renew_tor_circuits` established. No mutating action remains
to add.

`tests/test_monitor_interaction.py` adds headless-Textual interaction tests
(`Pilot`/`run_test`) against a real `ControlServer` over a temporary socket,
covering: a confirm/cancel round trip for a run-wide action; dedicated
confirm/send round trips for `pause_admission`, `resume_admission`,
`drain_and_stop`, and `checkpoint_stop`; an ineligible action opening no
confirmation; `p`/`u`/`d`/`k` disabled inside `WorkerDetails`; the
control-state poll gate staying at or below two polls per second;
activity-pane scroll position surviving a snapshot update; a terminal
resize; and controller restart/reconnect recovering control state. Writing
these tests found and fixed a real bug: a queued render tick could fire
against an already-torn-down default screen during app shutdown, raising
`NoMatches` (see `populate()`'s guard in `src/monitor.py`).

Row-scoped queue actions now have Pilot-level interaction tests in
`QueueRowScopedActionInteractionTests` (`tests/test_monitor_interaction.py`),
driving a real `ControlServer` and `InspectionServer` over temporary
sockets: `retry_now`, `exclude_item`, `retry_access_denied`,
`resume_new_generation`, `set_item_priority`, and `set_retry_cooldown` each
confirm the row-scoped `item_ids` sent to the executor matches only the focused row, and one test confirms a bucket
filter that hides a second selected item does not widen that scope.

Still unverified: the 30-FPS/150ms-p95 input-latency SLO on documented
hardware (not meaningfully assertable in a CI unit test), a 60-FPS
configuration specifically, and "kill the UI during every command stage"
across the full command set (only the confirm/cancel/reconnect paths above
are covered).
