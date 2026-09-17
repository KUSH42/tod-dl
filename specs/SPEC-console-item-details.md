# Specification: console item details

Status: planned, September 17, 2026. This view explains one selected item's
identity, acquisition state, attempts, and validation results.

The view supplements the [console UI](SPEC-console-ui.md) and uses the
[read-only inspection interface](SPEC-console-inspection.md).

## Entry and identity

**Enter** on a queue or review row must open item details. The worker detail
view must provide an **Item details** action for its current item.
The view must remain bound to `(run_id, item_id)` until explicitly closed.
A worker starting another item must not change an open item view.

The header must show the basename, short item ID, durable state, phase, and
freshness. The identity section must show the full item ID and run ID.
Duplicate basenames must remain distinguishable by item ID.

## Required sections

The view must provide the following sections. Each unavailable field must
show `?` and a reason instead of a fabricated value.

- **Identity and paths:** original logical path, mapped storage path, mapping
  reason and version, immutable queue rank, generation, and attempt ID.
  Show recorded final, staging, and candidate paths with distinct labels.
  A recorded path must not imply that the monitor checked the file.
- **State:** durable state, display bucket, current phase, phase reason,
  assigned worker, last transition time, attempt count, and attempt ceiling.
  Show retry eligibility, deadline, and any controller-reported blocking
  condition separately. Eligibility must not promise immediate admission.
- **Engine:** engine name and version, instance and job identity, PID when
  known, and the timestamp of the associated runtime sample.
  A historical PID must not imply a running process.
- **Bytes:** received bytes, resume baseline, transfer total and its source,
  trusted expected size, inventory size token and estimate, retained bytes,
  and committed completion bytes. Expose exact byte values alongside binary
  units. Keep unknown size distinct from a confirmed zero-byte file.
- **Validation:** method, processed bytes, recorded result and time, expected
  and observed SHA-256 values when available, and any mismatch reason.
  Show promotion and staging-cleanup status separately from engine success.
- **Attempts and errors:** attempt number and ID, generation, start and end
  times, outcome, sanitized error category and message, and retry deadline.
  Show the newest attempts first, with at most 200 per page.

`get_item` must return these sections without embedding complete attempt
history. `list_attempts` must return history ordered by attempt number
descending, then stable attempt ID. Missing historical fields must remain
unavailable; the service must not reconstruct them from guesses.

Recorded validation results must not imply a new verification. The view must
not read evidence, recompute hashes, promote candidates, or repair paths.
Only durable `complete` state can establish completed acquisition.

## Source reveal and navigation

The source field must initially show **Source hidden**. Activating
**Reveal source** must request the source for the current item only.
The view must label redacted or truncated values. **Hide source**, closing
the view, and changing the controller session must clear the revealed value.
Source URLs must not appear in breadcrumbs, queue rows, or activity messages.

**Tab** and **Shift+Tab** must move between sections and actions. Arrow keys
must scroll the focused section or select an attempt. **Escape** must return
to the prior view and restore its selection, filter, page, and scroll position.
**l** must open the selected item's logs under the parent console log rules.
If logs are unavailable, the view must report the reason.

At 80 columns, sections must stack vertically. Long paths and digests must
wrap or scroll without losing the full identity. Below 80 by 24, details must
remain scrollable. Resizing must preserve focus and position.

## Refresh and controls

Fresh records may update fields without replacing the view or moving focus.
An attempt or generation change must clear old runtime metrics before showing
new values. Validation must remain visible after a transfer slot becomes idle.
Stale engine samples must suppress current speed and ETA under telemetry rules.

The view must show its read time and durable revision. Inspection failure
must retain labeled last-known data. It must not imply that the item vanished
or completed. A session change must clear runtime data and source reveal,
then reload the same selected item under the new session.

This view must provide no retry, path-edit, or evidence action. Existing
confirmed controls remain governed by the command channel specification.
Opening details must never prepare or submit a command.

## Acceptance criteria

Headless UI tests must use synthetic inspection records and a virtual clock.

- Verify entry from queue, review, and worker views, plus return navigation.
- Verify duplicate names, long Unicode paths, markup, and escape sequences.
- Verify source hiding, explicit reveal, redaction, and clearing on close.
- Verify retries, changed generations, missing history, and paginated history.
- Verify unknown sizes, empty files, mismatched hashes, and candidate paths.
- Verify hashing after slot release and durable completion after promotion.
- Verify stale samples, revision differences, reconnect, and session changes.
- Verify resize, focus retention, and absence of evidence reads or mutations.
