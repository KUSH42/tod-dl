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
| `q`, `Ctrl+C` | Close the monitor only | all, outside text entry |
| `Escape` | Back to the prior view, or cancel text entry | all |
| `Enter` | Open details for the focused row | dashboard, queue |
| `Tab`, `Shift+Tab` | Move focus between panes or sections | all |
| `↑` `↓` | Move selection or scroll | all |
| `/` | Focus search | queue |
| `l` | Logs for the selected or displayed item | all item-bound views |
| `i` | Item details for the displayed item | worker details |
| `?` | Context help for the current screen | all |
| `s` | Reveal or hide source (toggle) | item details only |
| `r` | Retry now (confirmed, run-scoped) | dashboard |
| `R` | Retry now (confirmed, row-scoped) | queue |
| `t` | Renew Tor circuits (confirmed) | dashboard |
| `p`, `u`, `d`, `k` | Pause, resume, drain, checkpoint stop (confirmed) | dashboard |
| `x`, `]`, `[`, `}`, `{`, `e`, `A`, `N`, `g` | Existing queue row controls, unchanged | queue |

`r` moves off the detail screens. Source reveal moves to `s`, item details
only, per [SPEC-console-worker-details.md](SPEC-console-worker-details.md)
("Source URLs and private paths must remain in item details").
Detail screens must keep `q`, `r`, `t`, `p`, `u`, `d`, `k` bound to a
no-op that shows **Not available here; press Escape to return** in the
footer for 3 seconds. This keeps a control key from falling through to a
parent screen.

## Footer

The footer must show, in this order: navigation keys, then screen-local
actions, then command keys. Show at most 8 entries at 120 columns and at
most 5 at 80 columns; `?` must always be one of them. The palette entry
that Textual adds by default must be hidden.

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
