# Open development work

This document lists the work that remains after the current implementation.
It separates implemented features from broader specifications that remain
incomplete. It does not authorize a source request or an unattended run.

## Current implementation

The controller has durable selected-run state, no-overwrite finalization,
recovery tests, signed provenance records, telemetry snapshots, and local
confirmed controls for retry, Tor renewal, pause admission, resume
admission, drain and stop, and checkpoint and stop. The repository test
suite runs 322 local tests. These features do not establish compliance
with every requirement in the related specifications.

A process-ownership review closed five gaps in this session and the four
before it: the exclusive ownership lock now scopes to the destination and
treats SIGTERM as a clean stop; a requeue terminates a surviving writer by
PID and start time before resetting an interrupted transfer; free-space
admission subtracts known in-flight remaining bytes from the reserve
check; disk-full stops admission and resets attempts instead of consuming
a retry; and hashing now serializes to one file at a time, so a lagging
hash holds its worker slot and backs off new admission.

`relative_path()` and `legacy_relative_path()` now reject an absolute
result, a `..` segment, and a NUL byte. Before this fix, a URL such as
`/%2Fetc/passwd` gave `/etc/passwd`, and `destination / path` then ignored
the destination. No caller checked for this case. The check lives in
`is_unsafe_relative()`. The database and `ensure_safe_parent` do not
re-validate a stored `relative_path`; a database written before this fix
can still hold an absolute row. No such row is known.

## Remaining work

The acquisition tool evaluation's local phase is done. The third evaluation
report (`specs/reports/acquisition-tool-evaluation-2026-09-18c.md`) ran all
14 scenarios (E01 through E14) against the current per-URL aria2 candidate;
all 14 pass, and engineering targets (RSS, admission bound, status latency,
shutdown time) are met. `selection_eligible` is `true`. The selected
configuration is the current one: one aria2 process per URL through Tor. Do
not add a long-lived RPC worker; nothing in the report identifies a
mandatory scheduling, recovery, or resource gap that would justify one.

Since the second report, two real bugs were found and fixed:

- **`scope_run()`/`reconcile_promotions()` ordering and clobber gap
  (E05/E06).** `scope_run()` was reordered to run after
  `reconcile_promotions()` (commit `8a873d4`), then a second, distinct part
  of the same gap was fixed: `scope_run()` unconditionally re-stamped
  `existing_unverified` on any row whose final file already existed on
  disk, even when a prior run had already settled that row to a terminal
  status (`complete`, `review_required`, `excluded`, `unavailable`,
  `existing_unverified`). Fixed by checking the row's current status first
  (`SCOPE_RUN_TRACKED_STATUSES`, `src/tod-dl.py`). E05 and E06 both pass now.
- **Fixture-generator throughput (E02) and a second, distinct generator bug
  (E10).** Throughput was fixed earlier (commit `3853ab3`, module-level
  block cache, `shake_256`). This session found that `_generate_block`
  still always requested a full 1 MiB digest per block regardless of a
  fixture's actual length — E10's 1,000,000 32-byte fixtures made
  `Fixture.create` alone request roughly 1,000,000 × 1 MiB of digest output,
  so the scenario never finished constructing its fixture list. Fixed by
  bounding the digest length to what each block actually needs.

E08's HTML-200-error and post-hash-mismatch sub-cases are closed. Commits
`a7d289d`, `b387eb1`, and `b676f0e` added expected-checksum detection; all
three E08 sub-cases (short body, HTML-200-error, post-hash-mismatch) now
reach `review_required`, confirmed by re-running
`e08_checksum_mismatch_review()` directly. `acquisition-tool-evaluation-2026-09-18c.md`
predates this fix and still shows the old gap; a fresh dated report has not
been written yet.

The source pilot ran on 2026-09-18 with queue A (5 URLs, 512 KiB to 1 MiB,
separate state). All 5 items completed with HTTP 200 through `torsocks -i`
and the recorded `127.0.0.1:9050 IsolateSOCKSAuth` preflight. The provenance
record set verifies, and the raw inventory and snapshot hashes did not change
(`specs/reports/source-pilot-2026-09-18.md`). A first run of the same queue
returned HTTP 404 for all 5 files, because the base URL in `~/base-url.txt` lacked
the `/data/` segment. A resume test then interrupted one 2.7 MB transfer at
1,048,576 bytes and resumed it. The source answered HTTP 206 with
`Content-Range: bytes 1081344-2745378/2745379`, and the file completed. A
follow-up resumed a PDF and a MOV the same way (HTTP 206). Fresh full downloads
of all three files are byte-identical to the resumed files, and the ETag is the
same across attempts. The follow-up also removed the 60-second backoff after a
planned stop. One gap remains open, and it does not block selection:

- Queues B and C, and a bounded production run, have not run. Do not treat
  the candidate as validated for unattended production use until they do.

Complete the reliable-acquisition contract after tool selection. Engine
lifecycle checks (SIGINT parity with SIGTERM), bounded large-queue admission
(met per E10/E11 and the reserve-accounting, disk-full, and hashing-
backpressure fixes), migration support (`ALTER TABLE` column migrations in
`src/tod-dl.py`), and representation-change handling for the resume path
(`probe_representation()`, `check_representation()`, the
`resume_new_generation` controller action) are done. The 404/410
daily-recheck path (`SPEC-reliable-acquisition.md:190-217`) is done as
well: `transfer()` records `unavailable` with a `recheck_at` deadline, and
the run loop sends one HEAD recheck per due item per day
(`recheck_unavailable()`), then applies the same resume decision. The
early recheck is done too. A queue line's `generation=<id>` token fills the
new `source_generation` column, and `record_source_generation()` compares it
before overwriting. A differing identifier makes the recheck due at once,
through the `early_recheck` column (0 none, 1 pending, 2 spent). A later
differing identifier triggers again, also after a spent early recheck.
`import_queues()` also refreshes `inventory_size` and `expected_sha256` of an
existing row when a later queue changes them.

Acceptance evidence is partly done: `exclude_item` is now verified from all
8 reachable source states (`tests/test_tod_dl.py`), and the runbook covers
storage recovery and candidate review (`README.md`). E01 through E14 were rerun against
the current tree and all pass
(`specs/reports/acquisition-tool-evaluation-2026-09-18d.md`). Still open:
the byte-hash comparison for isolated tests. The pilot comparison is done:
the raw inventory and snapshot hashes are unchanged, and the destination held
no earlier final. The pilot queue had no `sha256=` token, so no source
checksum was compared. See `specs/reports/source-pilot-2026-09-18.md` and
`specs/reports/reliable-acquisition-acceptance-2026-09-18.md`.

Provenance and fault-recovery acceptance suites, updated 2026-09-18: every
listed tampering case, consumer rejection rule, recovery row, and scheduler
condition now has a named test. The comparison found and fixed these defects:
the verifier crashed on an unsafe path; it accepted a fingerprint without a
public key and skipped the signature check; it did not validate field values
against the schema; `close_reason`, attempt `outcome`, and candidate reasons
used values outside the specified enums; no `local_failure` event existed; and
a staging-cleanup error moved a completed item to `review_required`.

The `retry_access_denied` controller action returns an `access_denied` review
item to `queued` and ends the origin pause. The monitor's Queue tab binds it to
`A` for a `review_required` row. `resume_new_generation` is bound to `N` for a
`review_required` row.

Still open before the two specs can become `implemented`:

- `candidate_created` never records `unsafe_path` or `promotion_error`.
- The writer rejects user-info URLs only. Queue import does not reject them,
  and no rule exists for query values that grant access.
- The recovery tests stop the controller by staged state. Only three named
  failpoints exist (`FAILPOINTS`); none covers staging cleanup or shutdown.

Complete telemetry integration. Use the read-only aria2 RPC interface for exact
live transfer counters. Add tests for stale samples, snapshot failures, and all
required metric-quality states.

Complete the monitor and control UI. Add the planned Errors / review tab.

Inventory discovery is partly done. `src/inventory.py` implements local
snapshot parsing, reproducible manifest generation, safe queue export, and
snapshot diffing with a dated report (`SPEC-inventory-snapshot-manifest.md`,
implemented). A real 1,012,909-line listing parsed with 0 issues, and two
manifest runs gave identical bytes.

Still open in inventory discovery:

- Network refresh and directory discovery are not built. Add them only after
  the basic acquisition workflow is reliable. The source pilot for queue A is
  done; the production gap above remains.
  Scheduled refresh waits for both.
- The `diff` command does not produce a candidate queue or candidate manifest
  from the `added` and `metadata_changed` paths. The parent specification
  requires new items to become candidate manifest items.
- Neither the manifest nor the queue export can carry an expected checksum.
  The queue reader accepts a `sha256=` token, but no source of checksums
  exists in the inventory tools yet. The rescan specification needs one for
  checksum verification.
- The parser splits on `\n`. A file name that contains a newline is split
  into fragments that the parser can misread as valid entries. The parser
  cannot detect this case from the text alone.
- A manifest flags only the later item of a Unicode-normalization collision.
  The earlier item has no flag. The diff flags both.
- Memory use grows with the number of unique paths. The `diff` command used
  316 MB for two lists of about 450,000 files. The 1 GiB snapshot limit does
  not bound this. Measure a larger listing before you raise the limit.
- The reject rules in a policy that an operator derived from an earlier
  filter are inferred from what that filter removed. An operator must review
  them. One earlier deferred list matched no listing, so it has no policy.
  Find its source before you derive one.
- `queue --generation` is opt-in. A new `generation=` value starts early
  rechecks in the downloader. Decide whether a default value (for example a
  snapshot hash prefix) is wanted.
- Nobody has reviewed `SPEC-inventory-snapshot-manifest.md` in a fresh
  spec-review pass. It has only the author's checks and its tests.
- The test `test_row_scoped_resume_new_generation_sends_only_the_focused_review_item`
  failed once in one full run and passed alone and in the other runs. The
  cause is not found. It is probably a timing flake in the headless UI test.

Add on-demand target-directory rescan after inventory discovery. It
reconciles files already in the target directory against the inventory
manifest (path mapping plus checksum verification), from a manual command and
a debounced file-system watch, and produces a dated report plus a candidate
acquisition queue for missing and mismatched items. It depends on
inventory discovery's manifest and on the `expected_checksum` field already
added to acquisition; it does not define either.

Activity and Dashboard are not the same view. `SPEC-console-ui.md` uses
"dashboard" for the whole console screen: the worker table, disk lines, and
the `[Activity] [Queue] [Errors / review]` tab bar. "Activity" is one tab
inside that screen. `src/monitor.py` implements the dashboard screen and the
Activity and Queue tabs. The Errors / review tab from the spec does not
exist yet; that is the remaining "review view" work, not a dashboard rename.

## Specification status

The current specification status is grouped below.

- Implemented: `SPEC-tor-circuit-recovery.md`, worker details, item
  details, and `SPEC-inventory-snapshot-manifest.md`.
- Partially implemented: acquisition tool evaluation, acquisition fault
  recovery, provenance, console UI, controller controls, download telemetry,
  reliable acquisition, queue view, console visual style, console
  inspection, and inventory discovery.
- Planned: target-directory rescan. Its input manifest now exists.

## Next steps

Run queues B and C and a bounded production run before you treat the aria2
candidate as validated for unattended production use. Do not start any of
these runs without an explicit instruction. Check every base URL against a
known working URL first: a wrong base URL returns HTTP 404 for every file.
