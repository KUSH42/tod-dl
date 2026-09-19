# Specification: console keymap

Status: planned, September 19, 2026. This document assigns one meaning to
each key across every console screen and defines the footer contents. It
responds to findings H2 and M7 in
[docs/console-ui-audit-2026-09-19.md](../docs/console-ui-audit-2026-09-19.md).

The document supplements [SPEC-console-ui.md](SPEC-console-ui.md). It does
not authorize a source request or an acquisition action. Existing command
confirmation rules in
[SPEC-controller-control-ui.md](SPEC-controller-control-ui.md) are unchanged.

## One key, one meaning

A key must have the same meaning on every screen where it is bound. A screen
may leave a key unbound; it must not rebind the key to a different action.

| Key | Meaning | Screens |
| --- | --- | --- |
| `Ctrl+C` | Close the monitor only | all, outside text entry |
| `q` | Close the monitor only | dashboard, queue, outside text entry |
| `Escape` | Back to the prior view, or cancel text entry | all |
| `Enter` | Open details for the focused row | dashboard, queue |
| `Tab`, `Shift+Tab` | Move focus to the next or previous focus region of the current view | all |
| `↑` `↓` | Move selection or scroll | all |
| `/` | Focus search | queue |
| `l` | Logs for the selected or displayed item | all item-bound views |
| `i` | Item details for the displayed item | worker details |
| `n` | Next attempts | item details |
| `?` | Context help for the current screen | all |
| `s` | Reveal or hide source (toggle) | item details only |
| `r` | Retry now (confirmed, run-scoped) | dashboard |
| `R` | Retry now (confirmed, row-scoped) | queue |
| `t` | Renew Tor circuits (confirmed) | dashboard |
| `p`, `u`, `d`, `k` | Pause, resume, drain, checkpoint stop (confirmed) | dashboard |
| `x`, `]`, `[`, `}`, `{`, `e`, `A`, `N` | Existing queue row controls, unchanged | queue |
| `c`, `f`, `g`, `PageUp`, `PageDown` | Clear filters, refresh results, first page, previous page, next page (read-only) | queue |

The confirmation modal binds `y` and `n` to answer the prompt. It is not a
screen for this table.

In a text-entry field, every printable key, including `q`, `r`, `l`, `s`, and
`?`, must enter text. On the dashboard, which has no prior view, `Escape`
does nothing outside text entry.

## Focus regions

A view is either a set of panes or a filtered list. `Tab` and `Shift+Tab`
move focus between the focus regions of the current view only. They never
switch to another view.

| View | Focus regions |
| --- | --- |
| Dashboard | Worker table, activity log |
| Queue | Filters, search field, rows, page actions |
| Worker details, item details | Sections and actions |

This rule replaces the `Tab` reading in
[SPEC-console-ui.md](SPEC-console-ui.md) that named pane navigation for every
view. [SPEC-console-queue.md](SPEC-console-queue.md) already states the queue
regions.

`r` moves off the detail screens. Source reveal moves to `s`, item details
only, per [SPEC-console-worker-details.md](SPEC-console-worker-details.md)
("Source URLs and private paths must remain in item details").
Detail screens must keep `q`, `r`, `t`, `p`, `u`, `d`, `k` bound to a
no-op that shows **Not available here; press Escape to return** in the
footer for 3 seconds. This keeps a control key from falling through to a
parent screen.

## Footer

The footer must show, in this order: navigation keys, then screen-local
actions, then command keys. `Esc`, `↑↓`, `Enter`, `Tab`, and `/` are
navigation keys. `l`, `i`, `n`, `s`, and `?` are screen-local actions. `r`,
`R`, `t`, `x`, and `q` are command keys, and `q` comes last. Show at most 8
entries at 120 columns and at most 5 at 80 columns; `?` must always be one of them. At 80 columns, keep the
first 5 entries of the 120-column footer. If `?` is not among them, replace
the fifth entry with `?`. The palette entry that Textual adds by default must
be hidden.

Dashboard footer at 120 columns:

```text
↑↓ Select  Enter Details  Tab Pane  l Logs  ? Help  r Retry now  t Renew Tor  q Close
```

Worker details footer:

```text
Esc Back  ↑↓ Scroll  i Item details  l Logs  ? Help
```

Item details footer:

```text
Esc Back  ↑↓ Attempt  n Next attempts  l Logs  s Source  ? Help
```

At 80 columns, the item details footer drops `s Source`. The help modal
still lists `s`.

Queue footer at 120 columns:

```text
Tab Focus  ↑↓ Select  Enter Details  / Search  l Logs  ? Help  R Retry row  x Exclude row
```

At 80 columns, the queue footer drops `l Logs`.

## Help screen

`?` must open a modal that lists every binding of the current screen with
its meaning, drawn from the same binding table the footer uses. It must
name confirmed command keys as **confirmed** and read-only keys as
**read-only**. `Escape` closes it. The modal issues no request.

## Acceptance criteria

- Verify with a headless test that no key maps to two different action
  names across the dashboard, queue, worker details, and item details
  binding tables. The test must fail if a future screen rebinds a key.
- Verify `r` on worker details and item details does nothing except show
  the footer notice, and submits no command and no `get_item` request.
- Verify `s` toggles source on item details only and is unbound on worker
  details.
- Verify `?` opens help on every screen and the help lists each bound key.
- Verify the footer shows `?` at 80 and 120 columns.
- Verify each footer above matches its 120-column and 80-column text and
  never exceeds 8 and 5 entries.
- Verify `Tab` moves focus only among the regions in the focus-region table
  and never switches view.
- Verify `q` closes the monitor on the dashboard and queue and is a no-op
  with the notice on both detail screens.
