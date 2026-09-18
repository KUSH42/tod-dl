# Acquisition tool evaluation report (3)

Status: complete local run. All 14 scenarios (E01 through E14) ran against
the current candidate and passed. Per
[the evaluation specification](../SPEC-acquisition-tool-evaluation.md),
`selection_eligible` is `true`. This report selects the current
configuration: one aria2 process per URL through Tor
(`torsocks -i`, `--enable-rpc` per process). It does not add a long-lived RPC
worker and does not authorize a source pilot; that remains a separately
scheduled next step.

This is the third evaluation report. The second
(`acquisition-tool-evaluation-2026-09-18b.md`) ran 12 of 14 scenarios: E05
and E06 failed on a confirmed `scope_run()`/`reconcile_promotions()`
ordering gap; E02 did not run because the deterministic fixture generator
was too slow (~3.4 MB/s) to finish an 8 GiB pass inside the harness's 600s
per-attempt limit; E11 had no harness. Since that report:

- `scope_run()` was reordered to run after `reconcile_promotions()`
  (commit `8a873d4`), then a second, distinct part of the same gap was found
  and fixed this session: `scope_run()` unconditionally re-stamped
  `existing_unverified` on any row whose final file already existed on
  disk, even when a prior run had already settled that row to a terminal
  status (`complete`, `review_required`, `excluded`, `unavailable`,
  `existing_unverified` itself). A second `run()` over an already-`complete`
  row silently demoted it back to `existing_unverified`. Fixed by checking
  the row's current status first and skipping the transition when it is
  already one of those terminal statuses (`SCOPE_RUN_TRACKED_STATUSES`,
  `src/tod-dl.py`). E05 and E06 both pass now.
- The fixture generator's throughput was fixed (commit `3853ab3`): a
  module-level cache keyed on `(seed, block_index)` replaced a
  per-instance cache that could never hit, using `shake_256` in place of a
  64-byte `blake2b`-per-block digest. Measured at 8 GiB in ~16s (>500 MB/s),
  comfortably inside the 600s limit.
- E11's harness was built (commit `6b980e9`).
- This session found and fixed a second, distinct generator bug the E10
  scenario exposed: `_generate_block` always requested a full 1 MiB
  `shake_256` digest per block regardless of how much of that block a
  fixture actually needed. E10 builds 1,000,000 32-byte fixtures, so
  `Fixture.create` alone was requesting roughly 1,000,000 × 1 MiB of digest
  output before the harness could even reach the disk-writing phase — the
  scenario never finished construction. Fixed by bounding the digest length
  to `min(GENERATOR_BLOCK_SIZE, fixture_length - block_index * GENERATOR_BLOCK_SIZE)`.
  Fixture-list construction for 1,000,000 rows now takes ~2.6s. Confirmed
  with the existing `DeterministicFixtureTests` (range-joining across
  non-block-aligned offsets) and the full 189-test suite; no regression.
- This session also found and fixed a harness-only false negative in E02's
  own pass check: it asserted the *second* logged HTTP request for the
  fixture carried a nonzero-offset Range header. In practice, aria2's
  `--continue=true` resume issues a harmless small preliminary request
  (64 KiB, no Range header) before its real resumed request, so the second
  logged event is not always the meaningful one. The underlying download
  resumed correctly in every observed run (`status=complete`, byte count
  and `sha256` both matching the fixture manifest); only the harness's
  check was wrong. Fixed to scan every request after the first for a
  genuine nonzero-Range resume and to separately flag an actual full
  restart from byte 0 (a later request with no/zero-start Range that
  transmits more than 1 MiB).

## Scenario results

| ID | Outcome | Requirement | Notes |
|----|---------|-------------|-------|
| E01 | **pass** | Exact bytes and expected hashes. | Small and empty fixtures completed with matching hashes. |
| E02 | **pass** | 8 GiB interrupted transfer, correct resume/hash. | Truncated at 256 MiB, retried after backoff, resumed with `Range: bytes=268435456-8589934591`, completed at 8,589,934,592 bytes with a matching `sha256`. No full restart from byte 0. |
| E03 | **pass** | Preserve old partial; never combine versions. | Server ignored Range and resent from 0; old partial preserved and not promoted (`retry_wait`). |
| E04 | **pass** | Apply outage policy; resume on recovery. | 503 backed off, then completed once the fixture recovered. |
| E05 | **pass** | Restart without losing jobs, concurrent duplicate writers, or premature promotion. | Engine, controller, and both-killed sub-cases all recovered to a `complete` status with a matching digest on restart. Previously failed on the `scope_run()` ordering/clobber gap, now fixed. |
| E06 | **pass** | Reconcile idempotently; preserve the final file and recover its recorded hash. | All three failpoints (`post_validation_intent`, `post_final_file_creation`, `post_completion_commit`) reconciled to `complete` with a matching digest on restart. Previously failed on the same gap as E05. |
| E07 | **pass** | No existing bytes change; collision recorded. | Existing final preserved; recorded `existing_unverified`. |
| E08 | **pass** | Block automatic promotion; retain review candidate. | Short body reached `review_required` after a second, no-growth attempt. Only the short-body sub-case ran this session, not the HTML-200-error or post-hash-mismatch sub-cases (see "Remaining gaps"). |
| E09 | **pass** | Stop admission; report local storage failure. | `--reserve-bytes` set above actual free space stopped admission before any transfer request (adapter-side controlled quota, per the spec's disk-full option). |
| E10 | **pass** | Meet resource/responsiveness targets; do not request every URL. | Peak RSS 208 MB (target < 512 MB), max status latency 0.00s (target < 2s), shutdown 0.0s (target < 30s), 19 of 1,000,000 rows requested. Required a fixture-generator fix this session (see above) before it could complete at all. |
| E11 | **pass** | Five-item run selected from a larger queue; only those transfer, including after retries/restart. | Exactly the five selected items ever transferred or were ever requested from the fixture server; item 5 (index 5) was never selected or requested; `run_items` stayed stable across a restart. |
| E12 | **pass** | Stable mapping; explicit rejection/collision reports; enforced source scope. | Duplicate URL and unsafe traversal path both rejected before queueing with an explicit `[queue-rejected]` report; Unicode name transferred correctly. |
| E13 | **pass** | Refill idle slots promptly; the large file does not block unrelated work. | All small files and the due retry completed; small files completed before the large file; retry refilled within the rounding-tolerant target. |
| E14 | **pass** | Refuse source traffic when Tor is absent; no direct fallback. | Real Tor control-port preflight failed immediately (`Connection refused`) against a deliberately unreachable control address; zero fixture requests. |

## Engineering targets (from E10)

- Peak combined RSS: 208 MB, target < 512 MB for 1,000,000 queued rows. Met.
- Admission bound: 19 of 1,000,000 rows requested, bound <= 100. Met (spec
  requires not requesting every URL; this run's admission ceiling of 4
  concurrent workers plus retries accounts for the 19).
- Status latency: 0.00s observed, target < 2s. Met.
- Shutdown: 0.0s observed, target < 30s. Met.

## Remaining gaps

1. **E08's HTML-200-error and post-hash-mismatch sub-cases have not run.**
   Only the short-body sub-case has an implemented scenario this session;
   the spec's E08 row lists three distinct failure modes ("Short body, HTML
   error with HTTP 200, and checksum mismatch"). The short-body sub-case
   passing does not by itself confirm the other two are handled.
2. **The source pilot has not run.** The specification requires it after
   local success, using an explicit queue of at most five URLs, a time
   limit, and separate state, with verified SOCKS routing settings and
   sanitized connection evidence recorded. Local success here does not
   substitute for it.

Neither gap blocks selection under the specification's stated gate (E01
through E14 passing and engineering targets met), but both should be closed
before treating the candidate as fully validated for production use.

## Selected configuration

One aria2 process per URL through Tor (`torsocks -i`, one process per file,
one connection per file) meets all 14 scenario requirements and the
engineering targets with the adapter already in place
(`src/aria2_evaluation_adapter.py`). Per the specification's decision order,
do not add a long-lived RPC worker or evaluate lftp: nothing in this report
identifies a mandatory scheduling, recovery, or resource failure that would
justify either. `aria2 1.37.0`, adapter revision
`src/aria2_evaluation_adapter.py@initial`.

## Commands and configuration

```
python3 src/tod-dl.py --queue <per-scenario> --torsocks /usr/bin/torsocks \
    --aria2c /usr/bin/aria2c
```

Fixture manifest hash for this run's E01 fixtures:
`ab88415b4c1c49ed9e4d694f96001754c7d49ee3b6adef6952d850bf8203c826`. Full
per-scenario detail, sanitized event logs, and the machine-readable report
are in the harness output directory
(`src/run_acquisition_evaluation.py <output-dir>`); this directory is not
tracked in source control.

Operating system: Linux 6.8.0-139-generic, x86_64, Python 3.13.11.
