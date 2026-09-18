# Specification: console visual style

Status: planned, September 18, 2026. This document collects the label, value,
emphasis, and highlight rules for the acquisition console's Textual UI into
one shared reference. It defines display rules only. It does not authorize a
source request or an acquisition action.

## Outcome and dependencies

The console currently has two independent sources of visual-style rules:
[SPEC-console-ui.md](SPEC-console-ui.md) (main screen) and
[SPEC-console-queue.md](SPEC-console-queue.md) (queue tab), which explicitly
states it reuses a main-screen rule where one matches and defines its own
rule directly where none does. This document names those rules once so that
[item details](SPEC-console-item-details.md) and
[worker details](SPEC-console-worker-details.md), and any later view, cite
one shared rule set instead of restating it or drifting from it. This
document does not replace SPEC-console-ui.md or SPEC-console-queue.md as the
record of what each of those two views renders; it defines the general rule
each rule instance follows.

## Label and value rules

- Render a static field label dim (**Session**, **Last complete**, **ago**,
  **free**, **reserve**, **headroom**, the status-bucket labels, and the
  queue's **Run**, **Selected**, **Loaded rows**, **Filters**, **Revision**,
  and **Read**). Render its value in the default style, including every
  duration and timestamp value.
- When a field has one fixed string with no separate label/value split (for
  example the queue's **Matching count unavailable**), render the whole
  string dim.
- Render every retry status message dim (for example **Eligible; awaiting
  controller**, **Eligible; cooldown active**). A retry countdown value
  itself stays default style; freezing it while stale changes its updates,
  not its color.

## Emphasis rules

- Render column and section headers bold to mark structure, not state (the
  main screen's **Disk** line and the queue's **Rank**, **Basename**, **Item
  ID**, **Bucket**, **Retry deadline**, **Phase**, and **Received / total**
  column headers). Apply a header's bold rule only to columns actually shown
  at the current layout width.
- Render a row-list result notice bold in the default text color when it
  must stand out without a new color (the queue's **Results changed**).
- Render the **live** freshness label green without bold text. This is the
  console's only defined use of a color to carry meaning by itself; every
  other status distinction must pair a color with a text label, so it stays
  legible in monochrome.

## Highlight and focus rules

- Render the active tab and the focused row with the same mechanism: a
  reverse-video highlight. Render an inactive tab or an unfocused row in
  plain default style. This highlight is a background change, not a
  color-only cue, so both remain legible in monochrome.
- Render a focused row-list action (the queue's **Refresh results**,
  **Clear filters**, **First page**) in default style, underlined while
  focused.

## Precedence

A view spec must cite the matching rule above instead of restating it. When
no rule above covers an element the view needs to style, the view spec may
define one directly; it must say plainly that no existing rule applies,
matching the pattern already used in
[SPEC-console-queue.md](SPEC-console-queue.md#visual-style). A view spec must
not restate a covered rule with different values; a new visual need is a
reason to extend this document, not to fork it.

## Acceptance criteria

Tests must render each view in monochrome, without relying on terminal color
output.

- Verify that the active tab and the focused row stay identifiable by their
  reverse-video highlight alone.
- Verify that dim labels and retry-status text stay visually distinguishable
  from default-style values without color.
- Verify that bold headers and notices apply only where a rule above or a
  view's own directly defined rule says they apply.
- Verify that the **live** label is the only element whose meaning depends on
  color alone, and that every other status distinction also shows a text
  label.
