# Specification: acquisition fault-recovery tests

Status: proposed behavior, September 16, 2026. This specification defines
fault tests for controller recovery. It supplements the
[reliable acquisition specification](SPEC-reliable-acquisition.md). Tests must
protect acquired data and must not make source requests.

## Test boundary

Fault tests must use a temporary destination, temporary state directory, and a
local fake transfer engine. Tests must not read, write, rename, or hash files
below `downloaded_files/`, `archive/`, `pst_extracted/`, `msg_extracted/`, or
`output/`.

The fake engine must simulate partial files, successful completion, terminal
failure, a stalled transfer, and a changed remote representation. The harness
must expose controlled failure points in SQLite updates, file flushes,
exclusive final-file creation, staging cleanup, and controller shutdown.

## Required recovery cases

Each test must stop the controller at one named failure point, start a new
controller with the same run ID and unchanged queue input, then check durable
state and filesystem results.

The standard downloader test file must name recovery tests by the failure point.
Each test must build a new temporary root. The test must use fixture bytes and a
local fake transfer engine. The test must not start `aria2c`, `torsocks`, or a
source request.

| Failure point | Required result after restart |
| --- | --- |
| Before attempt state commits | The item remains eligible or is safely requeued. |
| During an active partial transfer | The partial and its control metadata remain available for resume. |
| After engine success but before hashing completes | The item is not final. The controller rechecks staging. |
| After hash and promotion intent commit | The controller reconciles the intended final path and digest. |
| After exclusive final creation but before completion commit | The controller hashes the existing final and records completion only on a match. |
| After completion commit but before staging cleanup | The final remains complete and cleanup is safe to repeat. |
| During candidate creation | The existing final remains unchanged and the incoming bytes remain reviewable. |
| During SQLite commit or WAL checkpoint | The controller reports a local failure and does not admit new work until recovery. |

## Transfer and scheduler cases

The harness must test transfer failures without network access. It must verify
that a retry does not admit a queue item outside the immutable selection.

The harness must test these scheduler conditions:

- `retry_now` wakes admission while another transfer remains active.
- An eligible retry fills an idle worker slot within one admission poll.
- A time limit stops admission only at expiry.
- A storage-reserve failure stops admission without changing final files.
- A zero-progress transfer reaches the configured stall policy without a busy
  loop.

## Assertions and artifacts

Every fault test must assert final-file paths, file bytes, SHA-256 digests,
SQLite state, transition order, and staging or candidate preservation. The
test must check that no final path is overwritten. The test must check that a
restart does not expand the selected item set.

The test harness must retain its temporary artifacts only when a test fails.
When it retains artifacts, it must print their path and must not copy them into
the repository.

## Acceptance criteria

The project must run all fault tests in the standard downloader test command.
The tests must pass before a downloader change is handed off. A failed fault
test blocks unattended acquisition until an operator records the failure and
the project resolves it.
