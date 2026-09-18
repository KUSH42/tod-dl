# Acquisition tool evaluation report — September 18, 2026

Status: partial. E01, E03, E04, E07, E08, E09, E12, and E14 ran against the
current candidate. E02, E05, E06, E10, and E13 did not run this session. Per
[the evaluation specification](../SPEC-acquisition-tool-evaluation.md),
`selection_eligible` is `false` until all 14 scenarios pass; this report does
not select a configuration and does not authorize a source pilot.

## Candidate

One aria2 process per URL, wrapped in `torsocks -i`, launched by the
unmodified `src/tod-dl.py` CLI (2,261 lines at commit `f67180d` plus this
session's uncommitted adapter files). No long-lived RPC worker was added; the
evaluation did not identify a mandatory gap that would justify one.

- aria2: 1.37.0 (`/usr/bin/aria2c`)
- torsocks: `/usr/bin/torsocks`
- Tor: 0.4.9.11, system daemon, `SocksPort 127.0.0.1:9050 IsolateSOCKSAuth`,
  `ControlPort 9051`, `CookieAuthentication 1`
- OS: Linux 6.8.0-139-generic, x86_64, Python 3.13.11
- Command shape: `python3 src/tod-dl.py --queue <fixture-queue> --destination
  <isolated> --state <isolated> --workers 1 --max-files 0 --reserve-bytes
  <per-scenario> --tor-control-address <per-scenario> --tor-control-cookie
  /run/tor/control.authcookie --connect-timeout 5 --timeout 5`
- Adapter: `src/aria2_evaluation_adapter.py` (new), driving the real CLI as a
  subprocess. It does not reimplement transfer, resume, or finalization
  logic. Estimated complexity: small — roughly 130 lines, mostly queue/state
  plumbing and a durable-row reader for the existing `downloads` table.
- Fixture harness: `src/acquisition_evaluation.py` (existing, extended with a
  configurable `path_prefix` on `FixtureServer` so generated fixture URLs
  satisfy `relative_path()`'s required `<source>/data/<tail>` shape, and a
  `urllib.parse.unquote()` fix so percent-encoded and raw-Unicode request
  paths route to the correct fixture — a real bug this evaluation found and
  fixed in the harness itself, not in `tod-dl.py`).
- Runner: `src/run_acquisition_evaluation.py` (new). Reproduce with:
  `python3 src/run_acquisition_evaluation.py <empty-output-dir>`. Wall time
  is roughly three minutes, dominated by two real ~65-second outage-recovery
  backoffs (E03, E04).
- Fixture manifest hash for this run:
  `0dda39154879967161d9a7e28aeb6a007788a11a94f2035dd2db5aad2ecf0697`.

Every scenario used a fresh isolated `--destination`/`--state` pair under
`/tmp/tod-dl-eval-run/<scenario>/`, never the operator's real
`download-state/` trees (two real endpoint runs were active on this machine
throughout the evaluation and were not touched).

## Tor and process model finding

`tod-dl.py` unconditionally wraps every aria2 invocation in `torsocks -i`.
Verified empirically in this session: default torsocks configuration refuses
to proxy connections to loopback destinations at all —

```
$ torsocks -i curl http://127.0.0.1:<port>/
torsocks[...]: [connect] Connection to a local address are denied since it
might be a TCP DNS query to a local DNS server. Rejecting it for safety
reasons.
```

This is torsocks's own anti-leak protection (`AllowOutboundLocalhost`
defaults to `0`), separate from and in addition to Tor's own default
`ClientRejectInternalAddresses`. It means the real, unmodified downloader
CLI cannot reach a loopback fixture server under its production torsocks
configuration — by design.

For the `local_fixture`-declared scenarios (all of E01–E13 attempted here),
the adapter points `TORSOCKS_CONF_FILE` at a test-only config
(`AllowOutboundLocalhost 1`) that makes torsocks connect to the loopback
fixture directly, bypassing Tor entirely for that one destination. This
matches the exemption in the specification's "Tor and process model"
section. **This bypass proves nothing about Tor routing and was not used
for E14.** E14 instead exercised the downloader's real, unmodified Tor
control-port preflight (`verify_tor_isolation()` in `tod-dl.py`), which runs
before any queue import or transfer and is a genuine SOCKS-listener
admission guard, not a fixture-routing workaround.

Gap for a later session: none of E01–E13 as run here prove that the
selected aria2 processes route *production* traffic through the verified
Tor SOCKS listener with isolated credentials — only E14's admission guard
was tested against real Tor. A source pilot (out of scope until selection)
is the specification's designated place for that check; this evaluation was
correctly local-only.

## Per-scenario results

| ID | Result | Requirement | Evidence |
| --- | --- | --- | --- |
| E01 | **pass** | Exact bytes and expected hashes; legitimate empty file accepted. | A 4,096-byte fixture and a genuine 0-byte fixture both reached `complete` with stored bytes and recorded SHA-256 matching the generator. |
| E03 | **pass** | Preserve old partial; never combine versions. | First attempt: server closes the connection after 100,000 of 300,000 bytes (simulated interruption). Second attempt (after the real ~60s retry backoff): server ignores the Range request and returns a full 200. aria2 itself detected the inconsistency (`errorCode=8 Invalid range header. Request: 98304-299999/300000, Response: 0-299999/300000`) and refused to combine the responses; the item stayed `retry_wait` with its original partial (100,000 bytes) intact, never promoted. |
| E04 | **pass** | Apply outage policy; resume when service recovers. | First run: fixture returns 503; item enters `retry_wait` (`errorCode=29 The response status is not successful. status=503`). After the real backoff, a second run with the 503 script removed completed normally (`status=complete`, correct SHA-256). |
| E07 | **pass** | No existing bytes change; collision recorded. | Pre-created a final file with known content before running. After the run, the file's bytes were byte-for-byte unchanged and the row recorded `existing_unverified`. |
| E08 | **fail** | Block automatic promotion; retain a review candidate with its reason. | Only the short-body sub-case was exercised (HTML-200-error and post-hash-mismatch sub-cases were not). A truncated body (100 of 4,096 bytes) correctly did **not** promote, but aria2's own `errorCode=1 Got EOF from the server` is classified as a retryable engine failure (`retry_wait`), not `review_required`. No review candidate file is retained under this configuration for this sub-case. This is a genuine gap in the current adapter/controller, not a harness artifact. |
| E09 | **pass** | Stop admission; report local storage failure. | `--reserve-bytes` set above actual free space (the controlled-quota method the specification allows; no filesystem quota or write shim was used). The fixture server received zero requests; the downloader exited 1 having stopped admission before any transfer began. |
| E12 | **fail** | Stable mapping; explicit rejection/collision reports; enforced source scope. | A duplicate URL was silently de-duplicated and an encoded-traversal URL was silently dropped by `read_queues()` (`relative_path()` raises `ValueError`, caught and skipped with no report); a Unicode-named fixture transferred and hashed correctly. The mapping and scope enforcement work, but "explicit rejection... report" is not met: the rejection is silent, not reported. |
| E14 | **pass** | Refuse source traffic when Tor is absent; no direct fallback. | Ran with a deliberately unreachable `--tor-control-address` (nothing listens on port 9). The downloader's real Tor control-port preflight failed immediately (`ERROR: Tor isolation preflight failed: [Errno 111] Connection refused`, exit 1) and the fixture server recorded zero requests — no source or fixture contact happened before the guard ran. |
| E02 | not run | 8 GiB interrupted transfer, correct resume/hash. | Needs the streaming-generator 8 GiB pass and its own isolated run given the runtime cost; not attempted this session. |
| E05 | not run | Kill engine/controller/both during transfer; restart cleanly. | Needs a process-kill injection harness around `Downloader.transfer`/`run_aria2`; not built this session. |
| E06 | not run | Kill between validation, link creation, and DB commit; reconcile idempotently. | Needs the controller failpoints the specification asks the harness to add after durable validation intent, exclusive final-file creation, and completion-record commit; not implemented. |
| E10 | not run | One million synthetic queue rows; resource/responsiveness targets; bounded admission. | Needs a one-million-row queue generator and an RSS/latency sampler at a fixed interval; not built this session. |
| E13 | not run | Slow large file alongside small files and a due retry; five-second idle-slot refill. | Needs concurrent slow/small fixtures and completion/admission timestamp assertions; not attempted this session. |

## Remaining gaps

1. **E08 review-candidate classification.** A short/EOF transfer is treated
   as a retryable engine failure rather than `review_required` with a
   preserved candidate. Confirm this is deliberate for the "incomplete
   engine transfer" case (distinct from "validation ran and the hash
   mismatched," which this session did not separately test) before treating
   it as a gap to fix, since the spec's outage/retry table does cover
   connectivity-style failures under ordinary backoff.
2. **E12 silent rejection.** `read_queues()` in `tod-dl.py` drops an invalid
   queue line with no report. The specification requires an explicit
   rejection report. This is a real, fixable gap, not a harness artifact.
3. **E02, E05, E06, E10, E13 are unimplemented**, per the "not run" table
   above — each needs infrastructure this session did not build (large-file
   fixture pass, kill-injection harness, controller failpoints, million-row
   generator plus RSS sampler, and concurrency-timing assertions).
4. **No engineering-target measurement was taken** (peak RSS, idle-slot
   refill latency, status-request latency, shutdown time) because none of
   the scenarios run this session exercise concurrency, scale, or shutdown
   timing in the way the targets require.

## Selection

Not selected. `selection_eligible` is `false`: 8 of 14 scenarios ran, and 2
of those 8 (E08, E12) failed. Per the specification, do not add a long-lived
RPC worker on this basis — neither E08 nor E12's gap is a scheduling,
recovery, or resource failure of the kind that would justify one; both are
adapter/harness-level reporting gaps in the current per-URL aria2
configuration. No source pilot follows from this report.

## Raw evidence

The full structured report (`evaluation-report.json`, per-scenario
`fixture-manifest.json` and `fixture-events.json` files) was written to
`/tmp/tod-dl-eval-run/` during this session and is not committed to source
control, per the specification's "store generated bodies and runtime logs
outside tracked source files" instruction. Re-run
`python3 src/run_acquisition_evaluation.py <new-empty-dir>` to regenerate it.
