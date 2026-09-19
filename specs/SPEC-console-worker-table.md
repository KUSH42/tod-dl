# Specification: console worker table

Status: planned, September 19, 2026. This document defines each cell of the
dashboard worker table. It responds to findings H4, M1, and M2 in
[docs/console-ui-audit-2026-09-19.md](../docs/console-ui-audit-2026-09-19.md).

The document refines the worker-table rules in
[SPEC-console-ui.md](SPEC-console-ui.md). It does not authorize a source
request or an acquisition action.

## Stalled rows

When the phase cell reads **stalled**, the speed cell must read `—` and the
ETA cell must read `—`. A stall is defined by no payload progress for 60
seconds; a speed or ETA next to it is a contradiction. The same rule applies
to a row whose engine sample is stale.

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

`0 B / 0 B` is valid only for an item whose expected size is a confirmed
zero. The monitor must render that case as `0 B / 0 B (empty file)`.

## Basename cell

Truncate a long basename in the middle: keep the first `⌈(w-1)/2⌉`
characters and the last `⌊(w-1)/2⌋` characters around one `…`, where `w` is
the column width. Keep the extension inside the tail. This replaces the
head-plus-extension rule in `truncate_filename()`.

The marquee for the focused row must scroll the basename alone, with no
separator glyph. When the scroll window reaches the end of the name, it
must pause for 2 seconds and restart from the beginning. The `   ·   `
separator in `marquee_filename()` must go.

Two rows with the same basename must each show a short item ID after the
name, as [SPEC-console-ui.md](SPEC-console-ui.md) requires; the ID counts
toward the column width.

## Acceptance criteria

- Verify a row with `last_progress_age_s >= 60`, `speed_bps = 161`, and
  `eta_seconds = 1135` renders `stalled`, `—`, `—`.
- Verify `total_bytes = None` renders `?` and `total_bytes = 0` with a
  confirmed zero expected size renders `0 B / 0 B (empty file)`.
- Verify a 60-character basename in a 36-column cell keeps the extension
  and shows one `…` near the middle.
- Verify no marquee frame contains `·`.
- Verify duplicate basenames show distinct short item IDs.
