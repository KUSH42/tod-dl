# Specification: console detail layout

Status: implemented, September 19, 2026. This document defines the shared layout
for the worker details and item details screens. It responds to findings H3,
M3, M4, M5, M6, L1, L2, L3, and L4 in
[docs/console-ui-audit-2026-09-19.md](../docs/console-ui-audit-2026-09-19.md).

The document supplements
[SPEC-console-worker-details.md](SPEC-console-worker-details.md) and
[SPEC-console-item-details.md](SPEC-console-item-details.md). Those two
documents keep authority over which fields each view shows. This document
defines how a field renders. Styling follows
[SPEC-console-visual-style.md](SPEC-console-visual-style.md). It does not
authorize a source request or an acquisition action.

## Header

The first line must identify the subject, not the read metadata. Worker
details:

```text
Worker 1  downloading  live       Read 17:21:12  Rev 10350
```

Item details:

```text
photo 3.PNG  [f53957ba3f]  retry  downloading  live       Read 17:21:12  Rev 10350
```

The read time renders in local time with no zone marker, as
[SPEC-console-visual-style.md](SPEC-console-visual-style.md) requires. In the item header, `retry` is the item's display bucket, as defined in
[SPEC-console-queue.md](SPEC-console-queue.md). Render the subject bold, the
phase and freshness as text labels, and the
read metadata right-aligned and dim. When the detail revision differs from
the dashboard revision, add a second line **Dashboard revision 10344;
details differ**. Both views compare against the dashboard revision that the
screen held when the operator opened it. Do not repeat the worker number, phase, or basename in a
body section.

## Two-column field grid

Each section must render as a grid of one field per row: a dim label column
of fixed width 24 that includes a two-column gap, then a default-style
value column. Do not place two
fields on one line. Section headers stay bold with one blank line before
each; no other blank lines appear in the body. A conditional line such as
**No progress for 60s** must be omitted, not rendered empty, when its
condition is false.

At 80 columns or fewer, the label column shrinks to 18 and values wrap. Below
80 columns, the grid stacks label above value. The layout tier follows the
terminal width. Wrapping reserves a two-column scrollbar gutter. A label
longer than its column
must wrap inside the label column at a word boundary. It must not be
truncated. **Last payload progress** (21 characters) wraps at 80 columns.

Worker details section order: Assignment, Activity, Transfer, Admission,
Validation. Worker details must have no **Source** section and no reveal
action; source stays in item details.

## Unknown values

An unavailable value must render `?` followed by one reason from this fixed
set. The console service that builds the `get_worker` and `get_item`
responses must choose the reason and return it as `unavailable_reason` beside
the unavailable field. The view must not choose or infer a reason:

| Reason | Meaning |
| --- | --- |
| `not in sample` | the runtime sample has no such field |
| `sample stale` | a value exists but its sample is stale; live value suppressed |
| `not applicable` | the field has no meaning in this phase or state |
| `controller did not report` | the controller supports the field but sent nothing |
| `unsupported by engine` | the engine cannot report this field |

The `?` and its reason render dim, as
[SPEC-console-visual-style.md](SPEC-console-visual-style.md) requires. A view
must never append a unit or suffix to `?`. `Last payload progress`
must render `12s ago` for a value and `? (not in sample)` for none, never
`? ago`. The **Admission** section must render one row per condition the
controller reports; when it reports none, render one row **Next eligible
start** with `? (controller did not report)`. The controller reports
`admission` as a map of condition name to display text, one entry per active
condition, for a slot in its cooldown phase only. The controller reports
**Global cooldown**, **Stagger**, and **Next eligible start** (the longest
remaining wait). It has no state for worker cooldown, origin cooldown,
storage stop, or validation backpressure, so it does not report them.

## Value formats

- Durations and ETAs render through the existing formatters. An ETA must
  render `~19m 4s`, never a raw number. A test must assert this.
- Byte fields render `560.0 KiB (573,440 bytes)`, as they do now.
- Every age or elapsed value carries a unit: `Sample age  0s`, not `0`.
- A basename renders percent-decoded, matching the dashboard table, with
  literal-text neutralization applied after decoding. Show the raw encoded
  form only in the **Identity and paths** section of item details.

## Secondary telemetry

`Sample sequence`, `Sample age`, and `Quality` belong to a **Telemetry
sample** subsection at the end of Transfer, rendered dim in full. They are
diagnostic, not operational. This placement overrides the Activity placement
of sample age in [SPEC-console-worker-details.md](SPEC-console-worker-details.md).
The field stays required there. Item details has no Transfer section, so it
renders the same subsection at the end of its **Bytes** section.

## Acceptance criteria

Headless tests with synthetic records:

- Verify the header line of each view contains subject, phase, freshness,
  read time, and revision, in that order.
- Verify each grid row of the render model holds exactly one label cell and
  one value cell. Do not count `: ` in the rendered text, because a value
  such as `connect failed: timeout` contains it.
- Verify a 21-character label wraps at 80 columns and loses no character.
- Verify no empty line appears except before a section header.
- Verify each unknown value renders `?` plus one reason from the table, and
  that `? ago` never appears.
- Verify `eta_seconds = 1144.03` renders `~19m 4s`.
- Verify worker details has no **Source** line and no `s` binding. Verify
  `r` there is bound only to the no-op notice in
  [SPEC-console-keymap.md](SPEC-console-keymap.md).
- Verify a percent-encoded basename renders decoded in the header and
  encoded only in Identity and paths.
- Verify the 80-column and stacked layouts keep every value reachable by
  scrolling.
