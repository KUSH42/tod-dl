# RFC: aria2-backed acquisition

Status: historical design, superseded for future work on September 15, 2026,
by the [acquisition tool evaluation](SPEC-acquisition-tool-evaluation.md),
[reliable acquisition specification](SPEC-reliable-acquisition.md), and
[inventory discovery specification](SPEC-inventory-discovery.md).
The text below records the earlier design, not verified implementation
behavior. Its curl rollback instructions are obsolete. Follow the migration
and recovery requirements in the new specifications.

This RFC replaces the custom curl transfer loop with `aria2c 1.37.0` while
keeping a small local supervisor for provenance, path safety, and finalization.
It preserves the existing acquisition tree and never treats a partial transfer
as evidence.

## Decision

The acquisition supervisor will schedule at most four separate
`torsocks -i aria2c` processes. Each process receives exactly one URL and uses
one HTTP connection. A per-URL process gives the supervisor an unambiguous exit
status, and `torsocks -i` gives each process unique SOCKS authentication.
The supervisor staggers process starts by one second by default to avoid a
burst of new Tor streams. Startup must verify that Tor has `IsolateSOCKSAuth`
enabled before acquisition begins, so those streams use separate circuits.

The supervisor remains responsible for the following:

- Selecting only the queue named for the run.
- Importing the inventory's displayed size for later triage comparison.
- Recording URL, original relative path, local storage path, inventory
  display-size token, observed byte count, status, attempt, SHA-256, and error
  in SQLite.
- Promoting a completed staging file atomically, without replacing a final
  file.
- Applying retry and SOCKS-outage cooldown policies.

## Storage and provenance

aria2 will write only below `download-state/incoming/`. Each URL receives a
SHA-256-derived staging name, preventing Linux path-component length failures.
The supervisor writes a mapping from the original URL and relative path to that
staging name and to the final local path. If an original path component exceeds
240 UTF-8 bytes, the final component is the deterministic
`__longname__<sha256>` form. If the final absolute path would exceed 3,800
bytes, the file is stored as `downloaded_files/__longpath__<sha256>`. The
SQLite mapping retains every original name and path.

After aria2 exits successfully, the supervisor confirms that no aria2 control
file remains before hashing the staging file and atomically linking it into
`downloaded_files/` with exclusive target creation. This prevents a destination
race from replacing an existing final file. A legitimate zero-byte file may be
promoted only after this success check. The inventory uses rounded display
sizes, so the supervisor records its size token and flags material discrepancies
for review; it does not treat that token as an exact byte count. The supervisor
must first confirm that the destination does not exist. If it exists, it moves
the staging file to `download-state/redownload-candidates/` and records a
candidate event.
Raw inventories, queue inputs, existing final files, and extracted material
remain unchanged.

## aria2 invocation

Each aria2 process receives one URL and a SHA-256-derived staging filename, not
a path under `downloaded_files/`. aria2's adjacent `.aria2` control file stays
with that staging file. Worker commands use these settings:

```text
torsocks -i aria2c --dir STAGING_DIRECTORY --out STAGING_FILENAME \
  --continue=true \
  --allow-overwrite=false --auto-file-renaming=false \
  --max-concurrent-downloads=1 --split=1 --max-connection-per-server=1 \
  --file-allocation=none --max-tries=1 --connect-timeout=90 --timeout=300 URL
```

`--max-tries=1` prevents aria2 from immediately repeating a failed request. The
supervisor schedules retries after 1, 2, 4, 8, 16, and then 30 minutes until
each selected URL is complete or already exists. A
SOCKS5, name-resolution, or connection failure pauses all workers for 60
seconds before another attempt. aria2 logs and control files stay in
`download-state/` and are not evidence.

## Tor isolation preflight

The supervisor must refuse to start unless the dedicated Tor SOCKS listener is
configured with `IsolateSOCKSAuth`. The deployment exposes an authenticated Tor
ControlPort, and the launcher queries its effective `SocksPort` configuration
before it starts workers. This is a precondition, not a best-effort check. A
successful preflight and the four distinct `torsocks -i` process IDs are written
to the run manifest.

## Run lifecycle

The supervisor creates `download-state/aria2/<run-id>/` for logs and a run
manifest. It also takes an exclusive acquisition lock so that two supervisors
cannot select or promote the same URL concurrently. On startup, it imports only
the specified queue, reconciles interrupted staging files and aria2 control
files, and marks no file complete until it is hashed and promoted.

During a run, the supervisor emits periodic aggregate progress from staging-file
sizes and records aria2 exit results. On interruption, it terminates workers,
keeps all staging files, control files, and logs, and marks affected records
resumable. A later run resumes the same staging targets. It never invokes legacy
`tor-dl-subdir*.sh` wrappers because their `-force` behavior can overwrite data.

## Acceptance criteria

The migration is accepted only after a local fixture server and a five-URL Tor
pilot demonstrate every condition below.

- A completed staging file has an aria2 success exit, no control file, a
  recorded SHA-256, and atomic promotion.
- A zero-byte fixture is promoted only after an aria2 success exit with no
  control file.
- A fixture whose connection closes early is not promoted and remains
  resumable.
- A material difference from the rounded inventory-size token is flagged for
  review without changing the final file.
- An interrupted file resumes without changing its staging path or final file.
- An existing final file is skipped, never renamed or overwritten.
- A simulated destination race creates a redownload candidate instead of
  replacing evidence.
- A path component longer than 255 bytes completes via its deterministic local
  mapping, with the original path retained in SQLite.
- A fixture whose full original destination would exceed 3,800 bytes uses the
  deterministic long-path fallback, with the original path retained in SQLite.
- Four workers use four `torsocks -i` processes, each with one active HTTP
  connection, after the Tor isolation preflight succeeds.
- A SOCKS5 or connection failure causes the 60-second global cooldown and the
  documented per-file retry schedule.
- Source inventories and queue files have identical SHA-256 values before and
  after the pilot.

## Rollout and rollback

You first run the fixture checks, then the five-URL pilot, then a bounded
production queue. Keep `download_priority.py` as the rollback path until the
pilot and one bounded production run meet every acceptance criterion. If any
criterion fails, stop the aria2 workers, retain their `download-state/aria2/`
directory, and resume with the committed curl downloader; do not delete or
alter staging files.

## Non-goals

This change does not make an unavailable onion service reachable, validate
server-supplied content against a remote checksum, parse evidence, or alter
the filtering and deferral rules. It does not commit downloaded files, SQLite
state, logs, control files, or generated run manifests.
