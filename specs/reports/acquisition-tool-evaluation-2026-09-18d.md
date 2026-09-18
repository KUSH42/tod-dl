# Acquisition tool evaluation report (4)

Status: complete local run. All 14 scenarios (E01 through E14) ran against
the current `master` tree (commit `80d83af`) and passed. `selection_eligible`
is `true`. The candidate is unchanged: one aria2 process per URL through Tor
(`torsocks -i`). This report does not authorize a source pilot.

This run follows the SIGINT, disk-full, hashing-backpressure,
reserve-accounting, representation-change, 404/410-recheck, and
E08-checksum changes. The third report
(`acquisition-tool-evaluation-2026-09-18c.md`) predates all of them.

## Run conditions

- Command: `python3 src/run_acquisition_evaluation.py /tmp/tod-dl-e-20260918d`
- Raw report: `/tmp/tod-dl-e-20260918d/evaluation-report.json` (not tracked)
- Fixture manifest SHA-256:
  `ab88415b4c1c49ed9e4d694f96001754c7d49ee3b6adef6952d850bf8203c826`
- aria2 1.37.0, Python 3.13.11, Linux 6.8.0-139-generic, x86_64.
- No `tod-dl.py` or `aria2c` process ran during the run. Only two
  `monitor.py` processes ran. The output directory path is short, because the
  control socket path limit is about 107 bytes.
- The run used a loopback fixture server only. It sent no source request.

## Results

| Scenario | Outcome |
| --- | --- |
| E01 through E14 | pass (14 of 14) |

E07 reads the existing final file before and after the run and compares the
bytes. The bytes were identical, and the row recorded `existing_unverified`.

E08 ran all three sub-cases (short body, HTML-200-error, post-hash mismatch).
The earlier E08 gap is closed.

## Compared with the second same-day rerun

The rerun recorded in the project notes as `20260918b` passed 11 of 14
scenarios. E05, E06, and E08 failed there while production downloaders shared
the Tor daemon. They passed in this run, which had no concurrent downloader.
This supports the earlier finding that those failures came from the
environment. It does not prove that E05, E06, and E08 never fail under load.

## Remaining gaps

1. The source pilot has not run (at most five URLs, separate state, verified
   SOCKS routing evidence). Do not run it without an explicit operator
   instruction.
2. The before/after hash comparison of the pilot's inventory has not run,
   because it needs the pilot.
