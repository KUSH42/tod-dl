# Specification: acquisition evaluation infrastructure

Status: implemented, September 18, 2026. All five pieces are built: the
streaming 8 GiB fixture pass for E02 (`Fixture`, `FixtureServer`, and
`e02_large_file_interrupted_resume`, `src/run_acquisition_evaluation.py`);
the kill-injection harness for E05 (`start_downloader`,
`src/aria2_evaluation_adapter.py`; `process_tree_pids`, `find_engine_pid`,
`wait_for_bytes_written`, `src/acquisition_evaluation.py`;
`e05_kill_injection`); the three named controller failpoints for E06
(`hit_failpoint`, `FAILPOINTS`, `src/tod-dl.py`, gated by `TOD_DL_FAILPOINT`)
plus the `reconcile_promotions()` extension for failpoint (a), already built
before this specification (`Downloader.reconcile_promotions()` and
`is_promotable_final_path()`, `src/tod-dl.py`); the one-million-row queue
generator and resource sampler for E10 (`generate_synthetic_queue_rows`,
`ResourceSampler`, `src/acquisition_evaluation.py`;
`e10_million_row_admission_and_resources`); and the concurrency-timing
assertions for E13 (`e13_concurrency_timing`), reading admission and
completion timestamps from `download_transitions` via `read_transitions`
(`src/aria2_evaluation_adapter.py`). Per this specification's scope, none of
E02, E05, E06, E10, or E13 was run as part of this work; a later evaluation
report must run them, per
[the acquisition tool evaluation specification](SPEC-acquisition-tool-evaluation.md).
E02 is expected to fail as built, per Section 1's resolved note: a
mid-transfer connection cut is routed to `review_required`, foreclosing
resume, and that controller-side gap stays out of this specification's scope.

## Outcome and scope

Define the test infrastructure that
[the acquisition tool evaluation specification](SPEC-acquisition-tool-evaluation.md)
requires to run E02, E05, E06, E10, and E13. This specification does not
change any required result in that specification's scenario table. It does
not evaluate a candidate and does not select a configuration. Build each
piece as an addition to the existing harness (`src/acquisition_evaluation.py`)
and the existing adapter and runner (`src/aria2_evaluation_adapter.py`,
`src/run_acquisition_evaluation.py`), not as a separate harness.

Implement the five pieces in the order listed below. Each piece stands alone:
a later piece must not depend on an earlier piece beyond shared harness code
that already exists.

## 1. Streaming 8 GiB fixture pass (for E02)

Requirement: "Generate fixture bytes from a documented seekable deterministic
generator, so the server can serve an 8 GiB representation without
materializing it first. The harness must calculate the expected digest with
the same generator in a separate streaming pass." (parent specification,
"Evaluation fixtures.")

- Add an 8 GiB fixture entry to the manifest generator, built from the
  existing deterministic generator (already used for smaller fixtures) run to
  a length of `8 * 1024**3` bytes. Confirm the existing generator is seekable
  at arbitrary byte offsets before reusing it; if it is not, that gap belongs
  in this section, not a new generator.
- Compute the expected SHA-256 in a second, independent streaming pass over
  the same generator and seed, never by hashing the served bytes. Record
  generator name, seed, length, and digest in the fixture manifest.
- Serve the fixture without materializing it: the fixture server's request
  handler must produce each response chunk from the generator at the
  requested byte offset, not from a file written to disk first.
- Interrupt the transfer after exactly 256 MiB, per E02's requirement, using
  the existing scripted-response mechanism (connection termination at a byte
  count). Assert on resume: a nonzero `Range` request, a final digest that
  matches the manifest, and no full restart from byte 0.
- Run this fixture once per viable candidate, per the parent specification's
  "Run the full large-file fixture once per viable candidate" instruction.
  Do not add it to the repeated failure-boundary test set.

Open question: confirm the current generator's per-chunk cost is low enough
that an 8 GiB streamed response completes in a bounded time on the evaluation
machine. Measure this during implementation; do not assume it.

Resolved: `terminate_after_bytes` (the scripted-response mechanism named
above) closes the connection before the response reaches its advertised
`Content-Length`, which aria2 reports as `GOT EOF FROM THE SERVER` — the same
error text `is_incomplete_body_failure()` (`src/tod-dl.py`) matches to route
a nonzero-size staging file to `review_required`, not `retry_wait`, since the
E08/E12 fix in commit `a6ee1d9`. Commit `a6ee1d9`'s own docstring for
`is_incomplete_body_failure()` confirms aria2 gives no distinguishable
signal: it reports `errorCode=1` for this case, the same generic code it
uses for unrelated failures, which is why that fix matches on message text
rather than the code. aria2 has no way to tell a genuinely short body (the
server will always send fewer bytes than `Content-Length`, which no retry
fixes) apart from a connection cut mid-transfer that a `--continue=true`
retry could resume. A transfer interrupted at 256 MiB therefore takes the
`review_required` route under current controller code, which forecloses
resume before a second `run_aria2()` attempt can ever try it, and fails
E02's "resume, no full restart" requirement. This is a controller-side gap:
`is_incomplete_body_failure()` cannot become more precise using only aria2's
output, so the fix (if any) must live in what the controller does with a
nonzero-size, `GOT EOF`-classified staging file, not in a more precise
error match. Closing that gap is outside this specification's five pieces,
per the scope line above ("This specification does not change any required
result"); E02 cannot pass until a decision is made and implemented
separately. Do not implement any change to `is_incomplete_body_failure()` or
its caller as part of this specification.

## 2. Process-kill injection harness (for E05)

Requirement: "Kill the engine, controller, and both during transfer. Restart
without losing jobs, concurrent duplicate writers, or premature promotion."
(parent specification, scenario table.)

- Add a kill-injection mode driven from `src/run_acquisition_evaluation.py`.
  The existing invocation, `run_downloader()` in
  `src/aria2_evaluation_adapter.py`, uses `subprocess.run(...)`, which blocks
  until the controller exits and exposes no PID or live handle while it
  runs. Add a separate, `Popen`-based invocation function alongside
  `run_downloader()` in `src/aria2_evaluation_adapter.py` — the module whose
  stated purpose is driving the controller subprocess — that starts the same
  controller command line and returns a live handle instead of a completed
  result. Do not reuse `run_downloader()` unmodified for this mode.
- For the engine sub-case, identify the aria2 process to signal by walking
  the process tree rooted at the controller's PID, the same approach Section
  4 uses for RSS sampling: the controller spawns `torsocks -i aria2c`
  (`Downloader.run_aria2()`, `src/tod-dl.py`), and the actual `aria2c`
  process may be a direct child or a grandchild depending on whether
  `torsocks` exec-replaces itself on the evaluation machine. Confirm which
  during implementation before selecting a PID to kill; signaling the wrong
  process would leave the real writer running and invalidate the sub-case's
  "concurrent duplicate writer" assertion.
- Trigger the kill by byte-transferred count, measured by polling the size of
  the staging file the controller is writing to on disk (the destination and
  state directories are already known to the harness) rather than by
  wall-clock delay, so the kill point is reproducible across runs. The
  fixture server's event log cannot serve as the trigger: it records one
  event per request, written only in the request's `finally` block after the
  request completes or its connection is torn down (`FixtureServer._record`,
  `src/acquisition_evaluation.py`), so it has no entry to observe during an
  in-progress transfer.
- After the kill, restart the controller against the same isolated
  destination and state directories used before the kill, and let it run to
  completion or its normal terminal state.
- Assert, from durable state and the final file: exactly one final file per
  item, and no promotion of a partial transfer that was in flight at kill
  time. The reliable acquisition specification does not enumerate valid
  partial-and-final coexistence states; the controller's own finalization
  path (`Downloader.transfer()`, `src/tod-dl.py`) defines the only
  legitimate coexistence window instead: the final file is created by
  `os.link(staging, target)` while `status="promoting"`, and staging is
  unlinked only after the following `status="complete"` transition commits.
  Assert that any staging-and-final coexistence observed after a kill and
  restart matches this window (a `promoting` or `complete` row for that URL
  at the moment of coexistence) and not a state outside it.
- Run three sub-cases: engine killed, controller killed, both killed
  (simultaneously, within the same event-log tick). Record which sub-case
  produced which durable state.

Open question: decide whether "both during transfer" means simultaneous
`SIGKILL` to both processes or a kill to one shortly followed by a kill to
the other before either can react. Pick simultaneous first, since it is the
harder case; add the staggered case only if the evaluation report calls it
out as a distinct gap.

## 3. Controller failpoints (for E06)

Requirement: "Test-only controller failpoints must stop immediately after
durable validation intent, exclusive final-file creation, and
completion-record commit. E06 then has unambiguous process boundaries."
(parent specification, "Evaluation fixtures.")

- Add three named, test-only failpoints to the controller's finalization
  path (`Downloader.transfer()`, `src/tod-dl.py`), each stopping the process
  immediately after one step: (a) the durable record of validation intent —
  the `status="promoting"` transition, which is written only after hashing
  completes and already carries the computed digest, immediately before the
  final file is created; (b) exclusive final-file creation (after
  `os.link(staging, target)` succeeds, before the completion record
  commits); and (c) the completion-record commit itself — the first
  `status="complete"` transition, which records `bytes` and `sha256` (after
  that database write, before the staging file is unlinked and the second,
  cleanup `status="complete"` transition that follows it).
- Gate each failpoint behind an environment variable or CLI flag that only
  the evaluation runner sets, never a flag reachable in normal operation.
  Name the three failpoints distinctly (for example,
  `TOD_DL_FAILPOINT=post_validation_intent`,
  `post_final_file_creation`, `post_completion_commit`) so a single run
  exercises exactly one boundary.
- After each failpoint fires and the process exits, restart the controller
  and assert idempotent reconciliation, scoped per failpoint since two
  different functions in `src/tod-dl.py` handle the three landing states,
  not one: failpoints (a) and (b) leave the row at `status='promoting'` and
  are picked up by `reconcile_promotions()`; failpoint (c) leaves the row
  already at `status='complete'` with `cleanup_completed_at` still unset
  (the first `complete` transition, line 1905, lands before the kill point)
  and is picked up by the separate `cleanup_completed_staging()` function,
  which finishes the staging unlink and the second `complete` transition.
  For failpoint (b), assert the final file is preserved, its recorded hash
  matches, and no duplicate completion record or duplicate final file
  results, per `reconcile_promotions()`'s existing handling. For failpoint
  (c), assert the same outcome, per `cleanup_completed_staging()`'s existing
  handling. For failpoint (a), where the durable `promoting` record exists
  but the final file was never created, extend `reconcile_promotions()` (see
  the resolved open question below) to recreate `os.link(staging, target)`
  and complete automatically; assert that outcome, not `review_required`.
- Read the existing controller finalization code (`src/tod-dl.py`) before
  placing the failpoints; place them at the exact three steps the parent
  specification names, not at steps that only approximate them.

Resolved: `reconcile_promotions()` has no code path that recognizes
"digest already durably recorded in a `promoting` row, staging intact,
target simply not yet created" — failpoint (a)'s exact landing state — and
instead falls into the same `review_required` branch used for a genuinely
corrupted or mismatched promotion. The fall-through happens at the
`target_matches` check: `is_safe_final_path(target)` calls
`target.lstat()`, catches the resulting `FileNotFoundError` internally, and
returns `False`; `target_matches` then short-circuits to `False` before
`sha256sum(target)` is ever called, so no exception propagates to
`reconcile_promotions()`'s own `except OSError` handler.

Extend `reconcile_promotions()` rather than accept `review_required`.
`SPEC-reliable-acquisition.md` already requires that "restart must reconcile
a durable promotion intent with the existing final file rather than blindly
labeling it complete or discarding its computed hash" — the digest for
failpoint (a) is already durably recorded and was computed before the kill,
so routing it to manual review discards a trustworthy result instead of
reconciling it. At the `target_matches` check, when `target` is absent
(`is_safe_final_path` returns `False` because `target.lstat()` raises
`FileNotFoundError`) but the `promoting` row carries a digest and `staging`
still exists as a regular file, recreate `os.link(staging, target)` and
complete the row the same way the normal `transfer()` path does, instead of
falling through to `review_required`. Retain `review_required` for every
other `target_matches` failure (target present but hash mismatch, staging
missing, unsafe path). This decision means the piece touches
`Downloader.reconcile_promotions()` itself, not only test infrastructure;
the Deliverables section below reflects that.

## 4. One-million-row queue generator and resource sampler (for E10)

Requirement: "One million synthetic queue rows... Meet the resource and
responsiveness targets... do not request every URL," together with the
engineering targets: combined peak RSS below 512 MiB, at most 64 admitted
unfinished jobs with at most 4 actively transferring, five-second idle-slot
refill, two-second status latency, 30-second graceful shutdown. (parent
specification, scenario table and "Initial acceptance targets.")

- Add a generator that produces one million syntactic queue rows (URLs and
  expected metadata) without writing one million fixture bodies; the fixture
  server may synthesize response bodies for these rows on demand, using the
  same deterministic generator as smaller fixtures, sized small enough that
  a full transfer of every row is not the point of this scenario.
- Add a sampler that records controller and every child engine process RSS,
  summed, at a fixed interval (document the interval; the parent
  specification requires it to be documented, not a specific value) for the
  duration of the run, and reports the maximum sum. Walk the full process
  tree rooted at the controller, not only its direct children: the
  controller spawns `torsocks -i aria2c` per engine worker
  (`Downloader.run_aria2()`, `src/tod-dl.py`), and the transferring `aria2c`
  process may be a grandchild rather than a direct child, depending on
  whether `torsocks` exec-replaces itself on the evaluation machine. Confirm
  which it is during implementation instead of assuming direct-children-only
  is sufficient.
- Assert, from the fixture server's request log, that the number of distinct
  URLs requested stays within the configured admission bound across the run,
  not one million — this is the scenario's core assertion, not an
  optimization detail. Candidate 1 (one aria2 process per URL, evaluated
  first per the parent specification's "Candidates and decision order") has
  no separate admission stage: `Downloader.run()` (`src/tod-dl.py`) admits a
  row only immediately before submitting it to the worker pool, so
  "admitted" and "actively transferring" are the same count, bounded by the
  single `--workers` value. The parent specification's "at most 64 admitted,
  unfinished jobs" and "at most 4 actively transferring" targets are
  therefore not two independently checkable bounds under candidate 1; assert
  only that requested-URL count stays within `--workers`, and record the
  64-job ceiling as not applicable to this candidate rather than asserting
  it as a separate, currently nonexistent bound. If a later candidate (for
  example, the bounded-RPC-feed candidate) introduces a real admission
  queue distinct from active transfers, this bullet must be revised to
  assert both bounds independently.
- Measure status-request latency by issuing a local status request at
  intervals during the run and recording response time; assert the two-second
  target.
- Measure shutdown time by sending the graceful-shutdown signal near the end
  of the run and recording time to process exit; assert the 30-second target,
  including the child-termination escalation path if the controller does not
  exit gracefully within it.

Open question: decide where the one-million-row queue file lives during the
run (generated fresh per run vs. cached). Generate it fresh in the isolated
run directory each time, consistent with every other scenario's isolation
requirement, unless generation time itself becomes a measured confound.

## 5. Concurrency-timing assertions (for E13)

Requirement: "Slow large file alongside small files and a due retry. Refill
idle slots promptly; the large file does not block unrelated work." Engineering
target: "Refill an eligible idle slot within five seconds, excluding
deliberate staggering, cooldown, storage pause, and validation backpressure."
(parent specification, scenario table and "Initial acceptance targets.")

- Add a scripted fixture mix: one slow large file (throttled via the existing
  scripted-response delay mechanism, not a separate throttling
  implementation), several small files that complete quickly, and one item
  already in `retry_wait` with a due retry timestamp at run start.
- Record the timestamp of every completion and every new admission from the
  controller's durable state: the `download_transitions` table already
  records a `recorded_at` timestamp on every status change, including the
  `active` transition (admission) and the `complete` transition
  (completion). The fixture server's event log cannot supply these
  timestamps; its recorded events carry no timestamp field
  (`FixtureServer._record`, `src/acquisition_evaluation.py`).
- Assert that each idle-slot refill following a completion or a due retry
  occurs within five seconds of that completion or due timestamp, excluding
  any interval attributable to deliberate staggering, cooldown, storage
  pause, or validation backpressure that the run's configuration explicitly
  set — record which exclusion, if any, applied to each measured interval.
- Assert that the slow large file's transfer does not delay the small files'
  admission or completion: compare the small files' completion timestamps
  against a run of the same small files with no concurrent large file, or
  against the five-second refill target directly, whichever the
  implementation finds simpler to make objectively checkable.

## Deliverables

Extend `src/acquisition_evaluation.py`, `src/aria2_evaluation_adapter.py`,
`src/run_acquisition_evaluation.py`, and the controller finalization path in
`src/tod-dl.py` to add the five pieces above. In `src/tod-dl.py`, this means
the three failpoints (Section 3) and the `reconcile_promotions()` extension
for failpoint (a) resolved in that section's open question; it does not mean
a fix for the E02 `is_incomplete_body_failure()` gap (Section 1), which
stays out of scope for this specification. Add
corresponding entries to `tests/test_acquisition_evaluation.py` for harness
behavior that unit tests can cover (generator determinism, sampler interval
correctness, failpoint gating) separately from the end-to-end scenario runs
that only `src/run_acquisition_evaluation.py` can produce. Do not run E02,
E05, E06, E10, or E13 as part of this specification; running them and
recording results belongs to a later evaluation report, per
[the acquisition tool evaluation specification](SPEC-acquisition-tool-evaluation.md).

## Next steps

Wait for an explicit instruction to implement before writing code against
this specification. After implementation, update this specification's status
to `partially implemented` or `implemented` and record which of the five
pieces landed, matching the neighboring specifications' status-header style.
