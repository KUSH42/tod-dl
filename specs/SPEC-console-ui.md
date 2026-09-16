# Specification: acquisition console UI

Status: partially implemented, September 16, 2026. The checkout contains a
read-only monitor and confirmed retry and Tor-renewal controls. This document
also defines planned dashboard behavior. It does not authorize source
requests.

## Outcome and dependencies

You can see what every worker is doing, how much work remains, why progress
has stopped, and which items need attention. The dashboard follows the
[reliable acquisition contract](SPEC-reliable-acquisition.md) and consumes the
[telemetry contract](SPEC-download-telemetry.md).

Use Textual as an optional monitor dependency, initially pinned to `8.2.8` in
`requirements-monitor.txt`, the stable release verified on September 15, 2026.
Keep transfer and state modules usable with the standard library and existing
external binaries.
Use Textual's DataTable, ProgressBar, Sparkline, and bounded log widgets.
Reference: [Textual release](https://pypi.org/project/textual/8.2.8/) and
[widget gallery](https://textual.textualize.io/widget_gallery/).

The [engine evaluation](SPEC-acquisition-tool-evaluation.md) owns the engine
decision. This UI does not require per-URL processes or long-lived RPC workers.
Synthetic telemetry can drive UI development before that decision is made. The
first release supports the synthetic fixture and demo path, plus read-only
version-1 controller snapshots. It never reads the live SQLite database or
changes controller state.

The [controller command channel specification](SPEC-controller-control-ui.md)
defines the later interactive mode. That mode reuses this application's layout,
snapshot reader, literal-safe rendering, and navigation; it does not add a
second controller or grant the UI direct database access.

The version-1 synthetic fixture is a JSON object with `schema_version` set to
`1`, top-level `run_id` and `session_id`, plus `run`, `workers`, `validation`,
`health`, and `recent_events`. `run` includes `lifecycle`, `session_started_at`,
`created_at`,
`selected_count`, `counts`, and `metrics`; `counts` names every selected-item
bucket and must sum to `selected_count`. `metrics` includes the nullable fields
displayed in the main-screen summary. Worker records include `worker_id`, `item_id`,
`basename`, `phase`, `received_bytes`, `total_bytes`, `speed_bps`, and
`eta_seconds`. Fixture data is untrusted display input: all strings are
rendered literally and all numeric values are validated as nonnegative.

## Launch and lifecycle

Provide a separate monitor process. Use these supported commands:

```bash
python3 monitor.py --state download-state --run-id RUN_ID
python3 monitor.py --state download-state
python3 monitor.py --demo
```

With no run ID, open the live controller session when exactly one valid,
nonfinal snapshot exists; otherwise present a run picker ordered by session
start time. Show completed runs as recorded results. A missing, malformed, or
incompatible snapshot is not a candidate live session.
The demo uses synthetic names and data and performs no network requests.

Closing the monitor, losing its terminal, or a UI exception must not signal
the controller or its workers. Multiple monitors may observe one run. The
monitor does not acquire the acquisition ownership lock or change state.
Keep the existing `--status` and plain downloader output available.

If the terminal is not interactive, print a concise status derived from the
selected fixture or last valid snapshot and exit. If neither is available,
print a clear unavailable-status message and exit nonzero. This fallback never
opens a write-capable database connection.
If Textual is missing, explain the optional dependency and show the existing
status command. Installation must be explicit, with a pinned dependency file
and documented virtual-environment setup delivered during implementation.

## Install and run the first release

The phase-one monitor runs versioned synthetic telemetry or a read-only
controller snapshot. It never opens the acquisition database or modifies
acquisition state, so you can use the demo without starting a controller.

Create a dedicated virtual environment, install the pinned optional dependency,
and launch the demo with these commands:

```bash
python3 -m venv .venv-monitor
. .venv-monitor/bin/activate
python3 -m pip install -r requirements-monitor.txt
python3 monitor.py --demo
```

Use `python3 monitor.py --fixture PATH` to validate and display a
different version-1 synthetic fixture. In a noninteractive environment, either
command prints a concise literal-text status and exits. `--state` and
`--run-id` read only the controller's published snapshot; they never open the
acquisition database or send a command to the controller.

## Main screen

The default screen prioritizes the selected run. All numbers in this wireframe
are synthetic; the layout illustrates a wide terminal.

```text
TOR-DL  RUN_ID  RUNNING  Elapsed 02:14:38  Updated 1s ago
Files  126/500 complete | 4 busy | 8 retry | 2 review | 360 queued
Data   18.4/~72.0 GiB retained | ~53.6 GiB remaining
Speed  700 KiB/s | 5m average 740 KiB/s | trend: ▁▂▅▆▄▃▅▇▅▃
ETA    —  2 items require review

#  File            Phase          Received/total   Speed       ETA
1  archive.zip     Downloading    1.2/3.8 GiB       410 KiB/s   ~1h 51m
2  mailbox.pst     Downloading    680/920 MiB       290 KiB/s   ~14m
3  bundle.zip      Hashing        73%              —           —
4  document.pdf    Connecting     18s elapsed      —           —

Disk 84.2 GiB free | 10 GiB reserve | 74.2 GiB headroom
Tor preflight passed at 12:16 UTC | Last complete 42s ago

[Activity] [Queue] [Errors / review]
14:31:08 UTC  Worker 2 resumed an existing partial
14:30:52 UTC  Worker 3 started SHA-256 calculation
14:30:26 UTC  Timeout; retry in 120s

↑↓ Select  Enter Details  / Search  l Logs  ? Help  q Close monitor
```

The summary must show run ID, lifecycle state, controller session start,
session elapsed time, run creation time, and remaining configured run time.
Show mutually exclusive item counts that sum to selected items. The summary
lists a nonzero count for every bucket, including complete, busy, retry,
queued, exhausted, unavailable, existing-unverified, review-required, and
unknown state. Busy includes connection setup, transfer, validation, and
finalization; expose its breakdown.
Show existing files skipped before selection separately from selected items.

Show retained progress, committed completion bytes, session transfer bytes,
known total and remaining bytes, unknown-size count, current speed, five-minute
average speed, transfer ETA, and estimated transfer finish time when available.
Use the definitions and suppression rules in the telemetry contract.

Worker numbers identify stable slots within a controller session. Keep rows in
slot order during refresh. Show basename by default, with middle truncation
when needed. Distinguish duplicate basenames with a short item ID. Do not show
full source URLs or private directory paths on the default screen.

Show separate validation rows if validation runs outside transfer slots. A
transfer slot becoming free must not make unfinished validation disappear.
Keep idle slots visible and explain deliberate staggering or cooldown.

## Detail, queue, and activity views

You can inspect additional information without crowding the worker table.

| View | Required information |
| --- | --- |
| Item details | Original and mapped paths, source URL on explicit reveal, item and generation IDs, queue rank, durable state, phase, attempt count and ceiling, engine identity, PID if known, staging/candidate paths, sizes and their origins, hash and validation result. |
| Worker details | Current item, attempt elapsed time, last progress age, current and smoothed speed, connection count if available, phase reason, and next eligible start. |
| Queue | Paginated selected items in manifest order; filter by state and search by literal path or item ID; display remaining work and retry deadlines. |
| Errors / review | Categorized error, last occurrence, attempt history, retry eligibility, validation failure, unavailable item, and candidate location. |
| Activity | UTC event time, severity, short item ID or worker, and concise message; deduplicate repeated countdown messages. |
| Run details | Input hashes, selection settings, engine version and evaluation status, run outcome, stop reason, and selected versus overall acquisition totals. |

Bind `Tab` to pane navigation, arrows to selection, `Enter` to details, `/` to
search, `Escape` to dismiss, `l` to selected-item logs, `?` to help, and `q` to
close the monitor. `Ctrl+C` also closes only the monitor. Search and sorting
change the view, never the acquisition order. Follow live logs until you
scroll away; offer an explicit return to live position.

Bound the visible log to 1,000 lines and load older records in pages on demand.
Read logs incrementally, including after truncation or rotation. Render
filenames, errors, and logs as literal text; neutralize terminal escape and
control sequences. Do not interpret them as Rich markup or shell commands.

## Health and uncertainty

The health panel explains whether work can proceed using observed state.

- Show free bytes, configured reserve, and headroom for each relevant
  filesystem. Deduplicate mounts by filesystem identity. Surface incompatible
  staging/finalization placement as a controller-reported error.
- Show Tor preflight result and its timestamp. Do not imply continuous Tor
  health or separate circuits solely from a successful preflight or PIDs.
- Show global or origin cooldown, next recovery probe, worker stagger, local
  storage failure, validation backpressure, and last successful completion.
- Show retry-exhausted, unavailable, existing-unverified, and review-required
  items explicitly. They never count as verified completion.
- After five seconds without a fresh snapshot, show **Telemetry stale** and
  suppress live speed and ETA. After 15 seconds, show **Disconnected**.
  Preserve last-known values with their timestamp; do not report worker death
  unless the controller or process reconciliation establishes it.
- If one engine sample is stale while the controller is live, mark that worker
  stale and label aggregate throughput incomplete. Never substitute zero.

Use text labels as well as color. Downloading, waiting, validating, stopped,
and failed must remain distinguishable in monochrome. Unknown values use `?`
or an explanation; zero is reserved for a known zero.

## Layout and performance

The dashboard must remain usable over SSH and in small terminals.

At 120 columns, display the full worker table. At 80 columns, retain file,
phase, progress, speed, and ETA, moving secondary fields into details. At
80 by 24, scroll the lower pane while retaining summary and worker rows where
space permits. Below that size, provide a compact summary and scrollable
details rather than failing. Resizing must preserve selection and filters.

Read a published snapshot at most twice per second from one-second telemetry.
The Textual application renders from its in-memory view model at 30 frames per
second by default. A future `--fps` option accepts whole values from 10 through
60 and defaults to `30`; it controls rendering only, not controller, network,
database, snapshot, or command polling.

At every render tick, sample monotonic time, advance local-only countdowns and
marquees, and repaint only widgets whose rendered value changed. A snapshot or
control-state result replaces the view model atomically between frames. Never
read a file, database, socket, or queue page in the render callback. Poll those
sources asynchronously at their stated bounded rates and coalesce a newer
result over an older pending result.

If a frame misses its deadline, discard it and render the newest state at the
next tick. Do not queue catch-up frames. Input, confirmation dialogs, scrolling,
and quit processing take priority over optional animation. Preserve selection,
scroll position, filters, and follow-live state across repaint. The activity
pane follows new events only while it is already at its live end.

Update changed cells rather than rebuilding the worker table, and never create
a widget for every queued item. Fetch at most 200 queue items per page.
Large-run counts must come from bounded-cost aggregate data rather than a full
scan on every repaint.

Acceptance targets are less than 150 ms input-to-render latency at the 95th
percentile and less than 128 MiB monitor RSS with one million synthetic queue
items. Record hardware, terminal size, dataset, and measurement method; these
are targets, not measured results.

## Delivery and acceptance

Deliver the monitor in stages with synthetic fixtures and recorded results.

1. Build the demo, layouts, navigation, state labels, and deterministic metric
   rendering against versioned telemetry fixtures.
2. Add read-only snapshot and paginated database adapters, then connect the
   controller telemetry after its engine integration passes the required gate.
3. Verify resumed and unknown-size transfers, retries, hashing, finalization,
   storage stops, and every unresolved outcome without live acquisition.
4. Verify stale controller and engine samples, reconnect, session replacement,
   missing state, incompatible schema, malformed snapshot, and log rotation.
5. Verify Unicode and duplicate names, literal markup and escape sequences,
   empty runs, 80-column layout, resizing, keyboard navigation, and large
   queues.
6. Kill or close the monitor during a synthetic transfer and prove the worker
   continues. Compare fixture hashes and durable state before and after
   monitoring; the monitor must cause no acquisition mutations.

Use Textual's headless test facilities for interaction tests and manual SSH
checks for terminal behavior. Document optional dependency installation and
launching alongside the existing controller in separate terminals or tmux.

## Deferred controls and next steps

The monitor supports observer mode and confirmed retry and Tor-renewal actions.
The [controller command channel specification](SPEC-controller-control-ui.md)
defines the interactive extension. Pause admission, resume, drain, and
checkpoint stop require controller acknowledgement, audit records, and defined
shutdown behavior. Do not implement these actions as direct database edits or
PID signaling from the UI. Start each new control with synthetic telemetry and
the demo screen.
