# Source pilot, queue A (512 KiB to 1 MiB) — 2026-09-18

Scope: the source pilot required by `SPEC-acquisition-tool-evaluation.md`
(after "After local success") and `SPEC-reliable-acquisition.md` (delivery
step 6). The operator instructed this run explicitly. It used the 5 URLs in
`~/pilot/pilot-queue-a-512K-1M.txt`, the current per-URL aria2 process
configuration, and separate state. Queues B and C did not run.

Result: 5 of 5 items reached `complete`. The second run is the valid pilot.
The first run failed because the queue had a wrong base URL.

## Run 1: wrong base URL (invalid pilot)

- The queue had no `/data/` segment, because `~/base-url.txt` held a wrong base URL.
- Run `pilot-a-20260918`, state `~/pilot/st-a`, destination
  `~/pilot/dl-a`.
- The onion host answered all 5 requests with HTTP 404. aria2 reported
  `errorCode=3 Resource not found`. All 5 items ended as `unavailable`.
- No file was written to the destination.
- This run shows that the daily-recheck path records a real 404 as
  `unavailable`. It does not count as the pilot. The state directory stays as
  evidence.
- Fix: the operator supplied a working URL that has `/data/` after the
  collection ID. The queue and manifest from `superseded-with-data/` replaced
  the no-data set. `superseded-no-data/` keeps the wrong set.

## Run 2: corrected queue (the pilot)

Command:

```bash
python3 src/tod-dl.py \
    --queue ~/pilot/pilot-queue-a-512K-1M.txt \
    --destination ~/pilot/dl-a2 --state ~/pilot/st-a2 \
    --workers 2 --max-files 5 --max-attempts 3 --time-limit 900 \
    --run-id pilot-a2-20260918
```

A dry run before the transfer listed 5 missing paths, 0 existing, and wrote no
file. The run started 2026-09-18T20:36:52Z. The last attempt ended at
20:38:20Z, about 88 seconds later.

| Item (case file names are omitted) | Queue size token | Bytes received | HTTP | Attempts |
| --- | --- | --- | --- | --- |
| 1 (PNG image) | 579K | 592,628 | 200 | 1 |
| 2 (PDF) | 777K | 795,552 | 200 | 1 |
| 3 (PDF) | 888K | 908,302 | 200 | 1 |
| 4 (PDF) | 708K | 724,951 | 200 | 1 |
| 5 (PDF) | 839K | 858,588 | 200 | 1 |

Every byte count rounds up to its size token, so each file agrees with the
inventory listing. The final files have SHA-256 digests recorded in the
provenance `finalized` events. The run summary was `complete=5`.

## Evidence required by the specification

Verified SOCKS routing settings:
- The preflight recorded `127.0.0.1:9050 IsolateSOCKSAuth` in the run manifest
  (`tor_isolation_preflight`).
- Every transfer ran as `torsocks -i aria2c`. The host is a `.onion` name.
  Such a name resolves only through Tor. The 5 successful responses and the
  earlier 404 responses came from that host.
- The evidence does not prove which Tor circuit carried a request. The
  specification prohibits that claim.

Source Range behavior: **not observed.** All 5 transfers were fresh and full.
Each response was HTTP 200 with no `Content-Range`. No transfer resumed, so
no response showed HTTP 206. The provenance events record an ETag and a
Last-Modified value for every response, so a later resume can probe the
representation. A resume test needs a deliberate interruption. That test was
outside this pilot's 5-URL bound.

Provenance: `verify_provenance.py`, with the public key derived from
`st-a2/provenance-signing-key.pem`, reported `OK: provenance record set
verified`. The record set holds 12 events: 1 `run_started`, 5
`attempt_finished`, 5 `finalized`, and 1 `run_closed`.

Byte integrity:
- The destination was new and empty, so no existing final could change.
- SHA-256 of `~/source-listing` and of every file under
  `~/pilot/store/snapshots/` is identical before and after the pilot.
- The manifest and queue hashes differ between the two checks. The cause is
  the deliberate swap to the with-data set described above, not a transfer.
- The queue carried no `sha256=` token, so no downloaded digest was compared
  with a source checksum. Only the size tokens and the HTTP length agree.

## Required checks

This pass changed no code. `git diff --check` reported no whitespace error in
the edited documents.

## Not covered

- Queues B (1M to 2M) and C (2M to 3M) did not run.
- No resume, no Tor circuit change, and no outage occurred during the pilot.
  The pilot does not test those paths against the source.
- A bounded production run has not run.
- The wrong base URL entered through a hand-made `~/base-url.txt`. The tooling
  cannot detect a base URL that is well formed but wrong. Only a source
  response reveals it.
