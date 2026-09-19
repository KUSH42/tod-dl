# Specification: console visual style

Status: partially implemented, September 19, 2026. This document collects the label, value,
emphasis, and highlight rules for the acquisition console's Textual UI into
one shared reference. It defines display rules only. It does not authorize a
source request or an acquisition action.

Implementation status, from
[docs/console-visual-style-audit-2026-09-19.md](../docs/console-visual-style-audit-2026-09-19.md):

- Unmet: the monochrome acceptance tests below. No test in
  `tests/test_monitor.py` or `tests/test_monitor_interaction.py` asserts
  monochrome rendering.
- Implemented September 19, 2026: a retry status splits into dim label
  spans and default-style deadline and countdown values. Section headers are
  bold. Event timestamps are dim, in local time without a zone marker.
  Event messages are dim, and event short item IDs are white.
- Not yet in code: the styles for the storage-risk error, the storage-stop
  condition, and the modal and footer rules below.

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

- Render a static field label dim (**Session**, **Last complete**, **Last
  payload progress**, **ago**, **free**, **reserve**, **headroom**, the
  status-bucket labels **complete**, **busy**, **retry**, **review**, and
  **queued**, the byte-total labels **retained** and **remaining**, and the
  queue's **Run**, **Selected**, **Loaded rows**, **Filters**, **Revision**,
  and **Read**). Render its value in the default style, including every
  duration and timestamp value.
- When a field has one fixed string with no separate label/value split (for
  example the queue's **Matching count unavailable**), render the whole
  string dim.
- Render a field's label and value as separate text spans. A view must not
  infer the label from the value string.
- Render every absolute time that the console formats in the operator's
  local time, with no zone marker: `HH:MM:SS` for a time of day, and
  `YYYY-MM-DD HH:MM:SS` where the date matters, such as a retry deadline.
  Add a zone marker only when a reader could take the time for another zone.
  Use UTC with a `Z` suffix for a time that actors in different time zones
  share, such as an export or a provenance record. A recorded RFC 3339 value
  that the console shows verbatim keeps its own `Z` or offset. Format local
  times in one place, so the activity log and the queue stay comparable.
- Render an unknown value `?` and its reason dim, so a missing value recedes
  and a present value stands out.
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
- Do not set a fixed foreground color for structural text; use bold or dim
  only. Do not combine bold and dim on one span.
- Render **Disk** in the same color as **Files** on the main screen, so the
  two section headers read as one group. **Files** renders in the default
  text color; **Disk**'s bold rule above still applies on top of that color.
- Render a row-list result notice bold in the default text color when it
  must stand out without a new color (the queue's **Results changed**).
- Render the **live** freshness label green without bold text. This is the
  console's only color that carries meaning by itself. Every other color in
  the Status colors section pairs with a text label, so it stays legible in
  monochrome.

## Status colors

Each row pairs a color with a required text label. Section headers are not in
this table; they are bold only.

| Element | Style | Required text label |
| --- | --- | --- |
| `TOD-DL` title | `bold cyan` | the title text |
| Run ID | `bold white` | the run ID text |
| Lifecycle RUNNING, FINISHED | `bold green` | the lifecycle word |
| Lifecycle STOPPED | `bold yellow` | the lifecycle word |
| Lifecycle unknown | `bold red` | the lifecycle word |
| Freshness stale | `yellow` | **Telemetry stale** |
| Freshness disconnected | `red` | **Disconnected** |
| Freshness recorded | `dim` | the recorded time |
| Severity INFO | `cyan` | `INFO` |
| Severity WARNING | `bold yellow` | `WARNING` |
| Severity ERROR | `bold red` | `ERROR` |
| Event message | `dim` | the message text |
| Event message **complete** | `green` | the word **complete** |
| Event short item ID | `white` | the bracketed ID, for example `[a3f9c21d0e]` |
| **Disk status unavailable** | `dim` | the label itself |
| Storage-risk error (negative headroom) | `bold red` | `storage risk` |
| Storage-stop condition (zero headroom) | `bold yellow` | `storage stop` |

## Footer and modal text

Footer and modal text follow the label and value rules above. A confirmed
command key label renders bold. A read-only key label renders in the default
style.

## Highlight and focus rules

- Render the active tab and the focused row with the same mechanism: a
  reverse-video highlight. Render an inactive tab or an unfocused row in
  plain default style. This highlight is a background change, not a
  color-only cue, so both remain legible in monochrome.
- Render a focused row-list action (the queue's **Refresh results**,
  **Clear filters**, **First page**) in default style, underlined while
  focused.

## Precedence

A view spec written or revised after this document exists must cite the
matching rule above instead of restating it. When no rule above covers an
element the view needs to style, the view spec may define one directly; it
must say plainly that no existing rule applies, matching the pattern already
used in [SPEC-console-queue.md](SPEC-console-queue.md#visual-style). A view
spec must not restate a covered rule with different values; a new visual need
is a reason to extend this document, not to fork it.

[SPEC-console-queue.md](SPEC-console-queue.md#visual-style) predates this
document and still states its own bold-header, tab-highlight, dim-label, and
row-action rules directly rather than citing this document, as noted under
Outcome and dependencies above. That restatement is a known exception, not a
violation of this rule; a future edit to that section must migrate it to a
citation instead of adding to the restatement.

[SPEC-console-ui.md](SPEC-console-ui.md) is a second known exception. It
restates the dim-label, **live**, **Disk**, and retry-status rules in its
header section and predates this document. Apply the same migration rule to
it.

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
- Verify that each row of the Status colors table renders with its listed
  style and label.
- Verify that no span combines bold and dim, and that no structural text sets
  a fixed foreground color.
- Verify that a label and its value render as separate spans, and that an
  unknown value and its reason render dim.
- Verify that every absolute time the console formats renders in local time
  with no zone marker, and that an unknown event time renders `--:--:--`,
  the same width as `HH:MM:SS`.
