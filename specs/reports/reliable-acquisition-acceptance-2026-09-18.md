# Reliable-acquisition acceptance evidence — 2026-09-18

Scope: SPEC-reliable-acquisition.md's "Acceptance and next steps" section
(lines 350-369), run against the current `master` tree after the SIGINT,
disk-full, hashing-backpressure, reserve-accounting, and representation-
change changes landed this session and earlier the same day.

## `exclude_item` verification

Added `test_control_exclude_item_from_every_reachable_state_retains_error`
(tests/test_tod_dl.py), which excludes an item from each of the 8 states in
`EXCLUDABLE_STATUSES` (`pending`, `queued`, `active`, `admitted`,
`retry_wait`, `failed`, `review_required`, `unavailable`) and checks, per
state: the item lands in `excluded`; its `last_error` is unchanged; a second
item's status, priority, and queue rank are all unchanged. Prior coverage
exercised only the `retry_wait` case.

Added `test_only_control_exclude_item_ever_sets_excluded_status`, which
greps `src/tod-dl.py` for `status='excluded'` assignments and asserts both
occurrences are inside `control_exclude_item`. This confirms no outage,
retry, validation, or promotion path can set `excluded`, per the spec's
explicit requirement.

Both tests pass. Full suite: 188 tests (186 prior + 2 new), all pass.

## Byte-integrity check

Not run this session: no test-fixture or pilot transfer was executed as
part of this acceptance pass (only unit/interaction tests), so there is no
before/after hash comparison to record. The requirement applies to isolated
tests and the source pilot, neither of which ran here.

## Required checks (AGENTS.md "Development commands")

```
python3 -m unittest -v tests/test_tod_dl.py tests/test_monitor.py tests/test_monitor_interaction.py
  -> 186 tests, OK
python3 -m py_compile src/tod-dl.py src/download_telemetry.py src/monitor.py \
    src/controller.py src/provenance.py src/verify_provenance.py
  -> OK
bash -n ./run.sh
  -> OK
git diff --check
  -> OK (no whitespace errors)
```

## Runbook

README.md already covered start, status, pause, and resume. Added two
sections that were missing: "Recover from low storage" and "Review a
candidate" (documents `exclude_item`, bound to `x` in the monitor's Queue
tab, and `resume_new_generation`, which is controller-API-only — no
monitor keybinding exists for it yet).

## AGENTS.md's "stale curl description"

AGENTS.md contains no `curl` reference in the current tree; this item in
the spec is already satisfied (or was overtaken by an earlier edit).

## Not covered by this pass

- E01 through E14 were not rerun. The most recent full run
  (`specs/reports/acquisition-tool-evaluation-2026-09-18c.md`) predates the
  SIGINT, disk-full, hashing-backpressure, reserve-accounting, and
  representation-change changes. That report's own "Remaining gaps" section
  already flags two open items: E08's HTML-200-error and post-hash-mismatch
  sub-cases, and the source pilot. Rerunning the full harness is out of
  scope for this pass and should happen before the implementation is
  treated as fully accepted.
- The 404/410 daily-recheck path (SPEC-reliable-acquisition.md:190-217) is
  still greenfield, per
  `project_representation_change_handling_implemented.md`.

## Addendum: follow-up run (same day, commit `80d83af`)

- E01 through E14 all pass on the current tree. See
  `acquisition-tool-evaluation-2026-09-18d.md`. The earlier "not rerun" item
  is closed.
- The 404/410 daily-recheck path is implemented, with an early recheck on a
  changed source generation. The "still greenfield" item is closed.
- Full suite: 239 tests, all pass. Compile, `bash -n ./run.sh`,
  `git diff --check`, and a one-item dry run all pass. The one-item dry run
  reported one missing file and wrote no file.
- The `pending` status no longer exists (merged into `queued`). The
  `EXCLUDABLE_STATUSES` list above therefore names 7 states now. The
  exclusion test covers every state in the set.
- Correction: the "Required checks" block shows 186 tests, and the text above
  it says 188. Both counts are stale. Use 239.
- Still open: the source pilot and its before/after byte-hash comparison.
  E07 checks only the fixture case. The pilot needs an explicit operator
  instruction.

## Addendum: source pilot (same day)

- The source pilot for queue A ran: 5 of 5 items complete, HTTP 200, through
  `torsocks -i`. See `source-pilot-2026-09-18.md`.
- The pilot byte comparison is done. The raw inventory and snapshot hashes are
  unchanged, and the destination was new. The queue had no `sha256=` token,
  so no source checksum was compared.
- Still open: source Range behavior, queues B and C, and a bounded production
  run.
