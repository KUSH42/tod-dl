# Specification: console worker table

Status: planned, September 19, 2026. This document defines each cell of the
dashboard worker table. It responds to findings H4, M1, and M2 in
[docs/console-ui-audit-2026-09-19.md](../docs/console-ui-audit-2026-09-19.md).

The document refines the worker-table rules in
[SPEC-console-ui.md](SPEC-console-ui.md). It does not authorize a source
request or an acquisition action.

## Stalled rows

When the phase cell reads **stalled**, the speed cell must read `—` and the
ETA cell must read `—`. A stall belongs to one transfer: the row is stalled
when that worker's own transfer has had no payload progress for 60 seconds.
Progress on another worker neither sets nor clears the stall. A speed or ETA
next to a stall is a contradiction. The same rule applies to a row whose
engine sample is stale.

The monitor must apply this rule in the row renderer. It must not depend on
the controller zeroing the sample, because a zero speed is a known value
and the spec reserves zero for a known zero.

## Unknown sizes

The **Received / total** cell must render `?` for an unknown total and a
byte value for a known total. The controller's telemetry must publish
`total_bytes` as null when the engine has not reported a length. It must
not publish `0`. [SPEC-download-telemetry.md](SPEC-download-telemetry.md)
owns the field; this document records the requirement because the monitor
cannot distinguish `0` from unknown after publication.

A non-null `total_bytes` of `0` is a confirmed zero. `0 B / 0 B` is valid only
in that case, and the monitor must render it as `0 B / 0 B (empty file)`.

## Basename cell

Truncate a long basename in the middle: keep the first `⌈(w-1)/2⌉`
display columns and the last `⌊(w-1)/2⌋` display columns around one `…`, where
`w` is the column width in display columns. Count a wide character as 2
columns and never split it. Keep the extension inside the tail. This replaces the
head-plus-extension rule in `truncate_filename()`.

The marquee for the focused row must scroll the basename alone, with no
separator glyph. When the scroll window reaches the end of the name, it
must pause for 2 seconds and restart from the beginning. The `   ·   `
separator in `marquee_filename()` must go.

Two rows with the same basename must each show a short item ID after the
name, as [SPEC-console-queue.md](SPEC-console-queue.md) requires for its rows.
The ID renders in brackets, for example `[a3f9c21d0e]`. It takes 12 columns
plus one separating space. Truncate the name to the remaining width.

## Acceptance criteria

- Verify a row with `last_progress_age_s >= 60`, `speed_bps = 161`, and
  `eta_seconds = 1135` renders `stalled`, `—`, `—`.
- Verify `total_bytes = None` renders `?` and `total_bytes = 0` renders
  `0 B / 0 B (empty file)`.
- Verify one worker with `last_progress_age_s >= 60` renders `stalled` while
  another worker with recent progress keeps its speed and ETA.
- Verify a 60-character basename in a 36-column cell keeps the extension
  and shows one `…` near the middle.
- Verify no marquee frame contains `·`.
- Verify duplicate basenames show distinct short item IDs.
