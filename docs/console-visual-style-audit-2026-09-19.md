# Audit: SPEC-console-visual-style.md

Date: September 19, 2026. Subject: `specs/SPEC-console-visual-style.md`
(status "partially implemented, September 18, 2026") checked against
`src/monitor.py` and `tests/test_monitor*.py`.

Verdict: the spec is internally consistent but incomplete as the "one shared
reference" it claims to be. It documents 9 style rules; the code applies
about 20. Three rules the code applies contradict the spec. No monochrome
test exists. Ten findings follow.

## HIGH

### H1. The spec omits most colors the console uses

The spec says the **live** label is "the console's only defined use of a
color." The code defines 11 more colors (`src/monitor.py:31-46`, `1780`,
`1782`, `437`):

| Element | Code style | In spec |
| --- | --- | --- |
| `TOD-DL` title | `bold cyan` | no |
| run ID | `bold white` | no |
| lifecycle RUNNING / FINISHED | `bold green` | no |
| lifecycle STOPPED | `bold yellow` | no |
| lifecycle unknown | `bold red` | no |
| freshness stale / disconnected | `yellow` / `red` | no |
| freshness recorded | `dim` | no |
| severity INFO / WARNING / ERROR | `cyan` / `bold yellow` / `bold red` | no |
| "complete" event message | `green` | no |
| section headers | `bold white` | bold only |

Each of these pairs a color with a text label, so the monochrome principle
holds. The gap is documentation: a view author cannot cite a rule that does
not exist, and the Precedence section forbids defining one locally when the
shared document "should" cover it. Fix: add a **Status colors** section
that lists each element above with its color and its required text label.

### H2. No monochrome test exists

The Acceptance criteria require rendering "each view in monochrome." Grep of
`tests/test_monitor.py` and `tests/test_monitor_interaction.py` finds no
`monochrome`, `no_color`, `reverse`, or `underline` assertion. Status
"partially implemented" is accurate; the spec must say which criteria are
unmet so the next session knows the gap. Fix: add an "Implementation
status" list under Status.

## MEDIUM

### M1. Labels are detected by a regex, not by structure

`detail_text_visual()` dims any run of up to 40 non-colon characters that
ends in `:` at line start or after two spaces (`src/monitor.py:753`). This
is a heuristic over free text. A value that contains `  ` followed by
text and `:` is mis-dimmed; a label longer than 40 characters is not
dimmed. The spec's rule "render a static field label dim" is correct, but
the spec never says the label must be a distinct render unit. Fix: add
"A view must render label and value as separate text spans; it must not
infer the label from the value string." This also supports
`SPEC-console-detail-layout.md`.

### M2. Retry status with a countdown loses its dim style entirely

The spec says: "Render every retry status message dim. A retry countdown
value itself stays default style." `retry_status_visual()` renders the
whole line default when the text contains `remaining)`
(`src/monitor.py:764`). The message part is not dim when a countdown is
present. Deviation from the spec.

### M3. Section headers use `bold white`, spec says bold

`bold white` (`src/monitor.py:748`) fixes a foreground color. On a light
terminal theme white headers are invisible. The spec's bold-only rule is
right; the code deviates. Fix code, and add to spec: "Do not set a fixed
foreground color for structural text; use bold or dim only."

### M4. No rule for unknown values

Code dims any value that starts with `?` (`src/monitor.py:740-742`). The
spec has no rule for `?` values or their reason text. Fix: add "Render an
unknown value `?` and its reason dim, so a missing value recedes and a
present value stands out."

## LOW

### L1. `bold dim` on event timestamps

`src/monitor.py:1874` uses `bold dim`. The two attributes fight; terminals
render this inconsistently. The spec should forbid combining bold and dim.

### L2. No rule for the freshness banners and storage errors

`SPEC-console-ui.md` requires **Telemetry stale**, **Disconnected**,
**Disk status unavailable**, a "storage-risk error" for negative headroom,
and a "storage-stop condition" for zero headroom. The visual-style spec
defines no style for any of them. Fix: one rule each, color plus label.

### L3. No rule for modals, footer, or help

Confirmation modals (`ActionConfirmation`), the footer, and the planned help
screen have no style rule. The keymap spec now needs a footer rule. Fix:
add "Footer and modal text follow the label/value rules; a confirmed
command key label renders bold."

### L4. Precedence section names one exception but the code has two more

The Precedence section grants `SPEC-console-queue.md` a restatement
exception. `SPEC-console-ui.md` also restates the dim-label and **Disk**
rules (lines 170-176), and it predates this document too. Name it as a
second known exception or migrate it.

## What is correct

- The reverse-video rule for focused row and highlighted attempt matches
  the code (`src/monitor.py:750`).
- **Results changed** bold in default color matches (`src/monitor.py:1322`).
- **live** green without bold matches (`src/monitor.py:42`).
- **Disk** bold, dim **free/reserve/headroom** matches
  (`src/monitor.py:1854-1862`).

## Recommended edits to the spec

1. Add a **Status colors** section (H1).
2. Add an **Implementation status** list under Status (H2).
3. Add rules: separate label/value spans (M1); no fixed foreground on
   structural text (M3); unknown values dim (M4); no `bold dim` (L1);
   freshness banners and storage errors (L2); footer and modals (L3).
4. Name `SPEC-console-ui.md` as a second known exception (L4).

## Recommended code fixes

- `retry_status_visual()`: split message and countdown into two spans (M2).
- Section headers: `bold`, not `bold white` (M3).
- Event timestamp: `dim`, not `bold dim` (L1).
