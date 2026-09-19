# Specification: console activity log

Status: planned, September 19, 2026. This document defines what one activity
event shows on the dashboard. It responds to findings H1, L5, and L6 in
[docs/console-ui-audit-2026-09-19.md](../docs/console-ui-audit-2026-09-19.md).

The document refines the Activity row of the view table in
[SPEC-console-ui.md](SPEC-console-ui.md). It does not authorize a source
request or an acquisition action.

## One event, one line

Each event must render as one line of the form:

```text
HH:MM:SSZ  LEVEL  W4  transfer admitted  report-2019-a11.msg  [a3f9c21d0e]
```

Fields, in order: UTC time with a literal `Z` suffix; severity padded to 7
columns; worker label when present; concise message; basename, middle
truncated to 40 columns; short item ID in brackets when the event is bound
to an item. Render the time and short ID dim, the worker label bold, and the
severity with the existing severity style.

The default screen must not show the logical path, the mapped storage path,
the source URL, or any directory component of an item. Remove the second
line that `activity_text()` currently appends under each event. The full
path remains available through **Enter** on the item and in item details.

## Concise messages

An event message must come from the controller's event vocabulary, not from
raw engine output. When the controller records an engine diagnostic, the
activity line must show a controller category (for example
**connect failed: timeout**) and the engine text must move to the item's
logs. The controller-side mapping is owned by
[SPEC-download-telemetry.md](SPEC-download-telemetry.md); this document
requires only that the monitor render `event.category` when present and
fall back to a literal, single-line, 120-column-truncated `event.message`
when it is not.

Deduplicate repeated countdown messages as
[SPEC-console-ui.md](SPEC-console-ui.md) already requires: show the latest
occurrence with a `×N` suffix.

## Acceptance criteria

- Verify with a synthetic event that contains a path with a personal name
  that no path component appears in the rendered activity text.
- Verify the time carries the `Z` suffix and matches the event's UTC value.
- Verify a raw multi-line engine message renders as one line, literal,
  truncated to 120 columns.
- Verify a duplicate basename shows its short item ID.
- Verify Rich markup and terminal escapes in message, basename, and worker
  label render literally.
