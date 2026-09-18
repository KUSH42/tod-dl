# Specification: acquisition tool evaluation

Status: partially implemented, September 18, 2026. The local fixture harness
implements deterministic content, immutable manifests, scripted HTTP behavior,
event logs, and structured reports. E01 through E14 ran against the current
per-URL aria2 candidate; all 14 pass and the engineering targets are met
(`specs/reports/acquisition-tool-evaluation-2026-09-18c.md`). The separately
scheduled source pilot ran on 2026-09-18 for one queue of 5 URLs, and all 5
transferred (`specs/reports/source-pilot-2026-09-18.md`). Resume tests on
three files (JPEG, PDF, MOV) recorded HTTP 206 with a `Content-Range` header,
and each resumed file equals a fresh full download, so the source honors Range
requests. Queues B and C and a bounded production run are open,
so the specification is not complete. This specification defines how you select an existing transfer tool
before expanding custom code. It does not authorize a production download.

## Outcome and scope

Select the simplest existing tool configuration that passes the
[reliable acquisition requirements](SPEC-reliable-acquisition.md). Keep custom
code for manifest conversion, evidence validation, and durable bookkeeping.
Use the [discovery specification](SPEC-inventory-discovery.md) for crawling.

The current downloader launches one aria2 process per URL through Tor. It
protects final files and persists `--max-files` selection for the whole run.
It still has incomplete shutdown and validation behavior. Use an explicit,
immutable fixture queue and isolated state for every evaluation.

## Evaluation protocol

The evaluation subject is TOD-DL with one configured transfer engine. E01
through E14 test the controller, adapter, durable state, finalization, and
engine as one acquisition configuration. The tests do not certify aria2 alone.

Run every candidate through the same controller-facing adapter contract. The
adapter accepts an immutable fixture manifest, an isolated destination and
state directory, an active-transfer ceiling, and a test-only fault schedule.
It returns structured attempt, engine, and finalization events. The harness,
not an engine log parser alone, decides each scenario result. This keeps the
comparison fair while leaving HTTP transfer and resume behavior to the engine.

Each result must name the candidate revision, command line, installed binary
version, adapter revision, operating-system details, and the exact fixture
manifest hash. Record a target as `pass`, `fail`, `blocked`, or `not run`.
`Blocked` and `not run` never satisfy the selection gate. A failure must cite
the violated requirement and retain the sanitized event evidence needed to
reproduce it.

## Candidates and decision order

Evaluate these configurations in order. Stop expanding the comparison once a
candidate meets the requirements with a small, understandable adapter.

1. Evaluate the current configuration: one aria2 process per URL through Tor.
   Do not add a long-lived RPC worker unless the evaluation identifies a
   mandatory scheduling, recovery, or resource failure.
2. If the initial configuration fails a mandatory requirement, evaluate
   long-lived aria2 workers with a bounded RPC feed. SQLite owns the complete
   queue; aria2 owns only the admitted jobs. Avoid developing a general RPC
   framework.
3. Evaluate lftp only if aria2 fails a mandatory requirement, or if lftp can
   demonstrably combine directory discovery and transfer with less code.
   Confirm that it understands the source's actual listing format.

Compare against the current downloader using the same synthetic fixtures.
JDownloader and archival crawlers are outside this first evaluation: the
immediate need is unattended acquisition from existing URL inventories.

The [aria2 manual](https://aria2.github.io/manual/en/html/aria2c.html) documents
input files, saved sessions, and RPC. It also states that `--save-session`
disables `--deferred-input`. Do not assume that those options jointly provide
bounded memory and durable queues. Test the installed version and record its
effective configuration. The
[lftp feature list](https://lftp.yar.ru/features.html) documents mirroring,
parallel downloads, and automatic reconnect. Those features alone do not
establish compatibility with this source or Tor setup.

## Tor and process model

Preserve Tor routing and authentication-based stream isolation while testing
changes to worker lifetime. A single `torsocks -i` process does not provide
different credentials for each URL it downloads. The initial long-lived
candidate uses up to four independently isolated processes, each with one
active file and one connection per file.

Verify the exact SOCKS listener used by those processes, not merely any
listener returned by the ControlPort. Record effective settings and sanitized
connection evidence. Distinct credentials separate isolation groups; process
IDs alone do not prove routing, circuit allocation, or independent capacity.
Use the [Tor SOCKS specification](https://spec.torproject.org/socks-extensions.html)
when you design these checks.

If a future evaluation selects RPC, bind it to loopback, require a local
secret, restrict its file permissions, and test local control access through
the chosen torsocks configuration. Source connections and DNS must still go
through Tor. Do not substitute aria2's HTTP proxy option for a SOCKS
implementation.

The local fixture is exempt from source-routing checks only when its resolved
address is loopback and the scenario explicitly declares `local_fixture`.
All non-loopback candidate requests, including a source pilot and recovery
probe, must use the verified SOCKS listener. E14 tests the admission guard
before any source request; it must prove that no fixture request was received
when Tor is absent or the configured listener is wrong.

## Evaluation fixtures

Implement a deterministic local HTTP fixture server using synthetic content.
Record request ranges, status codes, transmitted bytes, and simultaneous
connections. Each scenario must run in a fresh temporary directory with known
expected hashes. Never use acquired correspondence or documents as fixtures.

Generate fixture bytes from a documented seekable deterministic generator, so
the server can serve an 8 GiB representation without materializing it first.
The harness must calculate the expected digest with the same generator in a
separate streaming pass. Record the generator algorithm, seed, length, and
digest in the immutable fixture manifest. A successful 8 GiB scenario is run
once per viable candidate; smaller deterministic representations may exercise
the same boundaries repeatedly.

The fixture server must support scripted per-request responses, including
Range handling, validators, delays, short bodies, redirects, and connection
termination. It must write a machine-readable event log. Test-only controller
failpoints must stop immediately after durable validation intent, exclusive
final-file creation, and completion-record commit. E06 then has unambiguous
process boundaries. Disk-full tests may use a controlled filesystem quota or
a test write shim; the report must identify which method was used.

| ID | Scenario | Required result |
| --- | --- | --- |
| E01 | Small files and a legitimate empty file | Exact bytes and expected hashes; empty file accepted only after confirmed success. |
| E02 | An 8 GiB generated file interrupted after 256 MiB | Resume with a nonzero range; correct final hash; no silent full restart. |
| E03 | Server ignores Range, returns 416, or changes its validator/content | Preserve the old partial; block promotion or create a separate generation; never combine versions. |
| E04 | Refused connections, stalled reads, 429, and 503 | Apply the specified outage policy and resume when service recovers. |
| E05 | Kill the engine, controller, and both during transfer | Restart without losing jobs, concurrent duplicate writers, or premature promotion. |
| E06 | Kill between validation, link creation, and database commit | Reconcile idempotently; preserve the final file and recover its recorded hash. |
| E07 | Existing final, destination race, and symlink parent | No existing bytes change; collision is recorded; unsafe path is rejected. |
| E08 | Short body, HTML error with HTTP 200, and checksum mismatch | Block automatic promotion and retain a review candidate with its reason. |
| E09 | Disk exhaustion and unwritable state | Stop admission, preserve partials, and report a local storage failure. |
| E10 | One million synthetic queue rows | Meet the resource and responsiveness targets below; do not request every URL. |
| E11 | Five-item run selected from a larger queue | Only those five distinct items may transfer, including after retries and restart. |
| E12 | Encoded names, Unicode, long paths, duplicate URLs, and redirects | Stable mapping, explicit rejection/collision reports, and enforced source scope. |
| E13 | Slow large file alongside small files and a due retry | Refill idle slots promptly; the large file does not block unrelated work. |
| E14 | Tor absent or wrong SOCKS listener | Refuse source traffic; no direct-network fallback or external DNS lookup. |

Use a virtual clock for deterministic retry-policy tests and a short real
outage for engine integration. An accelerated clock result is not evidence of
a multi-day soak. Run the full large-file fixture once per viable candidate;
use smaller bodies for repeated failure-boundary tests.

Initial acceptance targets are engineering targets, not measured capabilities:

- Keep combined controller and engine peak RSS below 512 MiB for one million
  queued items, excluding the fixture server and OS filesystem cache.
- Keep at most 64 admitted, unfinished jobs across all engines, with at most
  four actively transferring files. Completed engine history must be bounded.
- Refill an eligible idle slot within five seconds, excluding deliberate
  staggering, cooldown, storage pause, and validation backpressure.
- Return local status within two seconds while transfers run.
- Begin graceful shutdown immediately and exit within 30 seconds, escalating
  to child termination if needed while retaining resumable state.

Measure RSS from the controller and every child engine process at a documented
sampling interval, and report the maximum sum. Measure status latency from a
local status request to its complete response. For E10, the fixture may
generate one million manifest rows without starting one million transfers; a
passing result must demonstrate that only the configured admission bound was
requested. For E13, record the timestamp of each completion and admission so
the five-second refill target is objectively checkable.

Record CPU, RAM, disk, software versions, fixture parameters, import duration,
peak RSS, request counts, extra bytes after interruption, shutdown time, and
every failure. A target revision must be explicit in the decision record;
do not silently call a missed target a pass.

## Deliverables and selection gate

Produce reusable fixture scripts and a dated, sanitized evaluation report.
The report must include commands, configurations, per-scenario results,
remaining gaps, estimated adapter complexity, and a selected configuration.
Store generated bodies and runtime logs outside tracked source files.

Select a candidate only when E01 through E14 pass, all engineering targets are
met, and its adapter remains within the bounded responsibilities in the
reliable-acquisition specification. If no candidate passes, report the exact
missing capability before adding custom behavior. Do not replace the transfer
engine with handwritten HTTP or resume code.

After local success, a separately scheduled source pilot must use an explicit
queue of at most five URLs, a time limit, and separate state. Record verified
SOCKS routing settings, sanitized connection evidence, and source Range
behavior. Do not claim that the pilot proves a Tor circuit route. If the source
is unavailable, report the pilot as blocked; local test success remains valid
but is not source success.

## Next steps

Run pilots for queues B and C, each with at most five URLs, and then a
bounded production run. Add a long-lived RPC worker only when the
evaluation identifies a mandatory gap. Use the selected result to implement the acquisition specification, then
add discovery without making crawling a prerequisite for downloading known
URLs.
