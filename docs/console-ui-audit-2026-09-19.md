# Console UI design audit

Date: September 19, 2026. Scope: the Textual monitor in `src/monitor.py`,
audited against `SPEC-console-ui.md`, `SPEC-console-queue.md`,
`SPEC-console-item-details.md`, `SPEC-console-worker-details.md`, and
`SPEC-console-visual-style.md`.

Evidence: two captures. `worker.png` shows the worker details screen (read
time 2026-09-17T17:21Z). `monitor-controller-demo.gif` shows the dashboard
(file date 2026-09-17 00:17). Both predate the Queue tab and 2 of the fixes
noted below. Each finding states whether the current code still has the
defect.

Verdict: the console is functionally rich and spec-driven, but the two
detail screens and the activity log are hard to scan, expose data the specs
forbid on the default screen, and use one key (`r`) for two different
actions. Fourteen findings follow, ranked by severity. Four fix specs
address them (see "Fix specs" at the end).

## HIGH

### H1. Activity log shows full private paths and personal names

The dashboard event log prints the full logical path of every admitted and
completed item, including directory names that contain personal names.
`SPEC-console-ui.md` says: "Do not show full source URLs or private directory
paths on the default screen" and the Activity view must show a "short item ID
or worker, and concise message." Code: `activity_text()` appends
`event_item_path(event)` under every event (`src/monitor.py:1882-1884`).
Status: present in current code. Fix spec: `SPEC-console-activity-log.md`.

### H2. `r` means "Retry now" on the dashboard and "Reveal source" in details

Dashboard `BINDINGS` bind `r` to `prepare_retry_now`
(`src/monitor.py:1746`). Worker details and item details bind `r` to
`reveal_source` (`src/monitor.py:882`, `1053`). An operator who learns `r`
on one screen gets a different action on the other. The retry action is
confirmed, so the risk is a wasted confirmation, not an unintended command;
the reveal action is not confirmed, so the reverse case reveals a source URL
by mistake. Status: present. Fix spec: `SPEC-console-keymap.md`.

### H3. Worker details exposes a source-reveal action

`SPEC-console-worker-details.md` says: "Source URLs and private paths must
remain in item details." The worker view renders a **Source** section and a
**Reveal source** binding (`src/monitor.py:696-697`, `1053`). Status:
present. Fix spec: `SPEC-console-detail-layout.md`.

### H4. Stalled rows show a nonzero speed and an ETA

Dashboard rows 2 and 3 read `stalled 1m 59 ... 161 B/s ~18m 55s`. A stall
means no payload progress for 60 seconds. A speed and ETA next to that label
contradict it and violate "Unknown values use `?` or an explanation." Cause:
`worker_phase_label()` decides "stalled" from `last_progress_age_s`, but the
row's speed and ETA columns render the raw sample values regardless
(`src/monitor.py:2022-2025`). Status: present. Fix spec:
`SPEC-console-worker-table.md`.

## MEDIUM

### M1. `0 B / 0 B` shown for an unknown total

Row 1 shows `0 B / 0 B` while downloading. The spec reserves zero for a
known zero. `format_bytes(0)` returns `0 B`; the value arrives as `0` from
telemetry, so the monitor cannot tell unknown from zero. Status: present;
requires a telemetry-side rule (`total_bytes` null when unknown). Fix spec:
`SPEC-console-worker-table.md`.

### M2. Filename truncation cuts the head, and the marquee leaks a `·`

Row 3 reads `tail of a long name.msg  ·` (head cut off). The spec requires middle
truncation. `truncate_filename()` keeps the head and the extension;
`marquee_filename()` scrolls with a `   ·   ` separator that appears as a
stray glyph at window edges (`src/monitor.py:221-239`). Status: present.
Fix spec: `SPEC-console-worker-table.md`.

### M3. Worker details header lacks worker number and phase

The spec header must show "the worker number, phase, freshness, read time,
and durable revision." The rendered header shows read time, revision, and
freshness only; worker number and phase appear 5 and 11 lines lower
(`src/monitor.py:668-669`). Status: present. Fix spec:
`SPEC-console-detail-layout.md`.

### M4. `Last payload progress: ? ago`

The line hardcodes ` ago` after the value, so an unknown value renders as
`? ago` with no reason (`src/monitor.py:684`). The spec requires `?` plus an
explanation. Status: present. Fix spec: `SPEC-console-detail-layout.md`.

### M5. Unknown fields show a reason that is not a reason

Eight fields on the captured worker screen read `? (not recorded)`:
Engine instance, Job, Reason, Phase elapsed, Resume baseline, Controller
conditions. "not recorded" tells the operator nothing about whether the
controller lacks the value, the sample is stale, or the field is
unsupported. The **Admission** section, which the spec says must show "next
eligible start time and separate reasons," is a single `not reported` line.
Status: present; part service gap, part display. Fix spec:
`SPEC-console-detail-layout.md`.

### M6. Detail screens pack 2-3 fields per line with no column alignment

Both detail screens render `Label: value  Label: value  Label: value` as
free text. Values do not align, so the eye cannot scan a column. Most of the
80x24 area is empty below line 30 while the top is dense. Status: present.
Fix spec: `SPEC-console-detail-layout.md`.

### M7. Footers omit navigation keys and help

The dashboard footer shows control actions only (`q`, `r`, `t`, ...). The
spec footer lists `↑↓ Select  Enter Details  / Search  l Logs  ? Help`. No
screen binds `?`. Status: present. Fix spec: `SPEC-console-keymap.md`.

## LOW

### L1. ETA rendered as a raw float

The capture shows `ETA (approximate): 1144.0317319769808`. Commit
`6bd7dff` (2026-09-17) rendered `eta_seconds` through `detail_value()`
without formatting. Current code formats it through `format_countdown()`
(`src/monitor.py:690`). Status: fixed in code; no test asserts the format.
Fix spec: `SPEC-console-detail-layout.md` (adds the test).

### L2. Extra blank line between Activity and Transfer

The "No progress for 60s" line is an empty string when the condition is
false, which renders as a blank line (`src/monitor.py:685`). Status:
present. Fix spec: `SPEC-console-detail-layout.md`.

### L3. Basename shown percent-encoded in details, decoded in the table

Worker details shows `photo%203.PNG`; the dashboard table shows decoded names
such as `photo 3.PNG`. One item has two spellings across screens.
Status: present. Fix spec: `SPEC-console-detail-layout.md`.

### L4. `Sample age: 0` has no unit; sample plumbing is shown by default

`Sample sequence`, `Sample age`, and `Quality` are telemetry internals. They
belong in a collapsed or secondary group, with units. Status: present.
Fix spec: `SPEC-console-detail-layout.md`.

### L5. Activity timestamps are ambiguous

Log lines read `00:02:14` while the session timer reads `00:44:46`. The
spec requires "UTC event time." The format gives no zone marker, so an
operator cannot tell UTC clock time from session-relative time. Status:
present. Fix spec: `SPEC-console-activity-log.md`.

### L6. Raw engine diagnostics in the activity log

`W4 -> [SocketCore.cc:507] errorCode=1 Failed to connect to the host
127.42.42.0, cause: Connection timed out` is a raw aria2 line. The spec asks
for a "concise message." Status: present. Fix spec:
`SPEC-console-activity-log.md`.

## What is working

- Dim label / default value styling is applied on the dashboard summary and
  on both detail screens.
- Disk line sits between the worker table and the log, as the spec orders.
- Literal-text rendering: no markup or escape execution observed.
- Unknown-size ETA suppression on the summary row reads correctly
  (`ETA — (227645 item sizes unknown)`).
- Stalled and downloading labels are text, not color only.

## Not verified

The captures predate the Queue tab and the row-scoped controls. This audit
did not exercise the Queue tab, the 80-column layout, or monochrome
rendering. `SPEC-console-visual-style.md` still lists its monochrome tests
as unimplemented.

## Fix specs

| Spec | Findings |
| --- | --- |
| `specs/SPEC-console-activity-log.md` | H1, L5, L6 |
| `specs/SPEC-console-keymap.md` | H2, M7 |
| `specs/SPEC-console-detail-layout.md` | H3, M3, M4, M5, M6, L1, L2, L3, L4 |
| `specs/SPEC-console-worker-table.md` | H4, M1, M2 |

Recommended order: keymap (small, removes a footgun), activity log (privacy),
worker table, then detail layout.
