# Acquisition tool evaluation report (2)

Status: partial. E01, E03, E04, E07, E08, E09, E10, E12, E13, and E14 ran
against the current candidate and passed. E05 and E06 ran and failed on real
controller gaps. E02 and E11 did not run. Per
[the evaluation specification](../SPEC-acquisition-tool-evaluation.md),
`selection_eligible` is `false` until all 14 scenarios pass; this report does
not select a configuration and authorizes no source pilot.

This is the second evaluation report. The first
(`acquisition-tool-evaluation-2026-09-18.md`) ran 8 of 14 scenarios and found
two real gaps (E08, E12), both since fixed
(`specs/reports/acquisition-tool-evaluation-2026-09-18.md`, commits
`a6ee1d9` and the `read_queues()` rejection-reporting fix). This report:

- Reruns E08 and E12 against those fixes: both now pass.
- Runs E05, E06, E10, and E13 for the first time, using the infrastructure
  built for them (`specs/SPEC-acquisition-evaluation-infrastructure.md`,
  implemented 2026-09-18 but not wired into `src/run_acquisition_evaluation.py`'s
  `main()` until this session). E10 and E13 pass; E05 and E06 fail on real
  controller gaps (below). E13 needed two harness corrections first (below).
- Confirms the `is_incomplete_body_failure()` fix (`src/tod-dl.py`,
  this session): a first "Got EOF from the server" failure now retries once
  through `retry_wait` instead of going straight to `review_required`, so a
  resumable mid-transfer cut is not foreclosed. E08's short-body sub-case
  still correctly reaches `review_required`, now after a second, no-growth
  attempt.
- Does not run E02 (8 GiB interrupted transfer): the deterministic fixture
  generator sustained only ~3.4 MB/s in a partial run this session, which
  would take 30+ minutes to finish an 8 GiB pass and risks exceeding each
  attempt's 600s time limit. This is the generator throughput that
  `SPEC-acquisition-evaluation-infrastructure.md` Section 1 flagged as an
  unmeasured open question. Separately, that partial run confirmed the
  `is_incomplete_body_failure()` fix works end-to-end for E02's own
  requirement: the staging file grew past the 256 MiB interrupt point
  (i.e., it resumed) before the run was stopped for time.
- Does not run E11 (five-item run from a larger queue): no harness exists
  for it yet; it was never part of the five pieces
  `SPEC-acquisition-evaluation-infrastructure.md` built.

## Scenario results

| ID | Outcome | Requirement | Notes |
|----|---------|-------------|-------|
| E01 | **pass** | Exact bytes and expected hashes. | Small and empty fixtures completed with matching hashes. |
| E02 | not run | 8 GiB interrupted transfer, correct resume/hash. | Generator throughput unmeasured/too slow this session (~3.4 MB/s); the `is_incomplete_body_failure()` fix itself is confirmed working (resumed past the 256 MiB interrupt point) in a partial run. See "Remaining gaps," item 3. |
| E03 | **pass** | Preserve old partial; never combine versions. | Server ignored Range and resent from 0; the old partial was preserved and not promoted (`retry_wait`, matching aria2's own range-mismatch error). |
| E04 | **pass** | Apply outage policy; resume on recovery. | 503 backed off, then completed once the fixture recovered. |
| E05 | **fail** | Restart without losing jobs, concurrent duplicate writers, or premature promotion. | A real gap: killing the engine mid-transfer, then restarting, does not reliably reconcile to `complete` even when the underlying transfer genuinely finished. See "Remaining gaps," item 1. |
| E06 | **fail** | Reconcile idempotently; preserve the final file and recover its recorded hash. | A real gap: only the `post_validation_intent` failpoint (crash before the final file link exists) reconciles correctly. `post_final_file_creation` and `post_completion_commit` (crash after the final file exists on disk) do not. See "Remaining gaps," item 2. |
| E07 | **pass** | No existing bytes change; collision recorded. | Existing final preserved; recorded `existing_unverified`. |
| E08 | **pass** | Block automatic promotion; retain review candidate. | Short body correctly reaches `review_required` after a second, no-growth attempt (see `is_incomplete_body_failure()` fix, above). Only the short-body sub-case ran this session, not the HTML-200-error or post-hash-mismatch sub-cases. |
| E09 | **pass** | Stop admission; report local storage failure. | `--reserve-bytes` set above actual free space stopped admission before any transfer request. |
| E10 | **pass** | Meet resource/responsiveness targets; do not request every URL. | Peak RSS 219 MB (target < 512 MB), max status latency 0.00s (target < 2s), shutdown 0.0s (target < 30s), 54 of 1,000,000 rows requested. |
| E11 | not run | Five-item run selected from a larger queue; only those transfer, including after retries/restart. | No harness built yet. |
| E12 | **pass** | Stable mapping; explicit rejection/collision reports; enforced source scope. | Duplicate URL and unsafe traversal path both rejected before queueing with an explicit `[queue-rejected]` report; Unicode name transferred correctly. (The harness's own E12 check was stale from before the rejection-report fix landed and always returned "fail" regardless of outcome; fixed this session to actually check for the report.) |
| E13 | **pass** | Refill idle slots promptly; the large file does not block unrelated work. | All small files and the due retry completed; retry refilled promptly; small files completed before the large file. Needed two harness fixes first (below). |
| E14 | **pass** | Refuse source traffic when Tor is absent; no direct fallback. | Real Tor control-port preflight failed immediately on a deliberately unreachable control address; zero fixture requests. |

## Remaining gaps

1. **E05: kill-then-restart does not reliably reconcile to `complete`.**
   Killing the aria2 engine mid-transfer, then restarting, twice produced a
   non-`complete` terminal status even though the transfer had genuinely
   finished (staging bytes matched the fixture's full size both times). The
   failure mode was not the same across runs, which points at a race rather
   than one deterministic bug:
   - One run ended in `existing_unverified` — the same root cause as gap 2
     below (`scope_run()`'s "target already exists" fast path pre-empting
     `reconcile_promotions()`).
   - Another run ended in `retry_wait` with `last_error` reading
     `(OK):download completed.` — aria2 itself reported success
     (`ok=True`), but `Downloader.transfer()` (`src/tod-dl.py`) still routed
     it to the failure branch. The likely cause: when `--continue=true`
     finds the target already fully downloaded, aria2 can skip its normal
     cleanup and leave the `.aria2` sidecar control file in place;
     `transfer()`'s failure test (`not ok or not staging.exists() or
     control.exists()`) treats a lingering control file as failure even
     when `ok` is `True` and the bytes are complete. This is a real,
     reproducible controller gap, not a harness artifact — confirmed with a
     real transfer engine, not the local fake used in unit tests.
2. **E06: `reconcile_promotions()`'s "target already exists" branch is
   unreachable from `Downloader.run()`.** `scope_run()` (`src/tod-dl.py`)
   runs before `reconcile_promotions()` and, for any queued URL whose final
   path already exists on disk, unconditionally transitions it to
   `existing_unverified` and excludes it from `run_items` — regardless of
   its current durable status. This pre-empts `reconcile_promotions()`,
   which specifically exists to idempotently reconcile a `promoting`-status
   row left by a killed supervisor once the final file is already present
   (`Downloader.reconcile_promotions()`, its `target_matches` branch, built
   in the prior session for "failpoint (a)"). That branch is exercised only
   when the crash happens *before* the final file exists
   (`post_validation_intent`); it is provably unreachable for a crash
   *after* the final file link is created (`post_final_file_creation`,
   `post_completion_commit`), because by the time `reconcile_promotions()`
   runs, `scope_run()` has already overwritten the row's status. Traced with
   a standalone script reproducing the crash and restart outside the
   harness, confirming the target file, its recorded digest, and the
   `download_transitions` sequence (`promoting` → `existing_unverified`,
   skipping `reconcile_promotions()` entirely). This is a real, reproducible
   controller gap, not a harness artifact.
3. **E02's fixture generator throughput is still unmeasured and appears too
   slow for a bounded run.** ~3.4 MB/s over the first 1.25 GiB observed this
   session implies 30+ minutes for the full 8 GiB pass, which risks
   exceeding the harness's 600s per-attempt time limit. `is_incomplete_body_failure()`'s
   fix itself is confirmed correct (resumed past the interrupt point); the
   remaining blocker is purely generator performance, which
   `SPEC-acquisition-evaluation-infrastructure.md` Section 1 flagged as an
   open question and which nobody has measured or optimized yet.
4. **E11 has no harness.** It is in the evaluation specification's scenario
   table but was never one of the five pieces the evaluation infrastructure
   specification built. A harness needs to be written from scratch.

Per the specification, do not add a long-lived RPC worker on this basis:
none of E05, E06, or E02's gap is a scheduling, recovery, or resource
failure of the kind that would justify one. No source pilot follows from
this report.

## Harness corrections made this session

Distinct from the controller gaps above, these are fixes to
`src/run_acquisition_evaluation.py` itself, needed to get an honest read on
the scenarios:

- E05, E06, E10, and E13's scenario functions computed a real result but
  always returned a hardcoded `"not run"` outcome, discarding the
  computation. They are now wired into `main()`'s scenario list and return
  real pass/fail based on the requirement.
- E12's scenario function always returned `"fail"` unconditionally,
  predating the `read_queues()` rejection-report fix; it now checks
  `stdout` for the `[queue-rejected]` lines the fix produces.
- E08's scenario function ran only one attempt, which cannot observe the
  `is_incomplete_body_failure()` fix's second-attempt, no-growth check; it
  now runs two attempts, forcing the second's retry due immediately.
- E13's large-file fixture was scripted with an 8-second response delay
  against `aria2_evaluation_adapter.py`'s fixed `--timeout=5`, so the large
  file could never complete; reduced to 3 seconds. Its refill-timing check
  also compared a microsecond-precision timestamp against a whole-second
  `recorded_at` column, producing spurious small negative "refill" times;
  now tolerates up to 1 second of that rounding.
