# TOD-DL repository guidelines

TOD-DL (Tor Onion Dump Downloader) is a forensic workspace for material
acquired from a public data leak. Preserve provenance, keep transformations
reproducible, and avoid changing acquired artifacts.

## Project structure and module organization

The root contains raw inventory snapshots (`ALL_FILES*.txt`), generated
priority and deferred listings (`*_filtered.txt`, `*_deferred.txt`), URL
queues (`urls_*_priority.txt`), and downloader scripts. `download-state/`
holds logs, SQLite state, staging files, and re-download candidates.

`downloaded_files/` is the canonical acquisition tree. Derived content lives
in `archive/`, `pst_extracted/`, `msg_extracted/`, and `output/`.
`iped-working/` is the active IPED workspace. Do not rename, reorganize, or
edit acquired data unless a task explicitly requires it.

## Development and operational commands

There is no package manager or build target. Use the manifest-driven downloader
for new acquisition:

```bash
./run_priority.sh --dry-run --max-files 3
./run_priority.sh --workers 4 --max-files 5
./run_priority.sh --run-id PILOT_20260915 --max-files 5
./run_priority.sh --status
python3 diff.py OLD_LISTING.txt NEW_LISTING.txt new_paths.txt
```

`download_priority.py` launches one `aria2c` process per URL through
`torsocks`, resumes aria2 staging files, records state transitions in SQLite,
and hashes a completed staging file before finalization. It never replaces a
final file. An existing final becomes `existing_unverified`, and a collision or
unsafe destination path becomes a review-required item. The controller
enforces a 10 GiB free-space reserve by default; use `--reserve-bytes` only
when an operator has assessed the storage risk. `--time-limit` stops admission
and checkpoints active work after the stated number of seconds.

`--max-files` selects at most that many missing items for the entire run;
retries don't admit later queue items. Use `--run-id` to resume the same
immutable selection. The run ID is bound to the queue file hashes and selection
setting, so don't reuse it with modified queues. `--status` reads persisted
state without starting transfers. Do not use `tor-dl-subdir*.sh` for continued
acquisition: `-force` can overwrite files and those wrappers don't retain
resumable state.

## Telemetry monitoring and retry recovery

The monitor is a separate, read-only process. It reads only the controller's
published telemetry snapshot and never opens the acquisition database, changes
controller state, or signals a worker. Install its optional Textual dependency
in a dedicated environment, then launch the demo or observe an active run.

```bash
python3 -m venv .venv-monitor
. .venv-monitor/bin/activate
python3 -m pip install -r requirements-monitor.txt
python3 monitor_priority.py --demo
python3 monitor_priority.py --state download-state --run-id PILOT_20260915
python3 monitor_priority.py --state download-state --run-id PILOT_20260915 \
    --control
```

The Textual monitor renders at 30 FPS by default, while snapshot reads remain
capped at twice per second. Use `--fps` only to choose a render rate from 10
through 60; it does not change controller, network, database, snapshot, or
control polling. `--control` is opt-in and attaches only to a live controller's
same-user Unix socket. It currently displays the authenticated control state;
press `r`, then select **Yes** or press `y` in the confirmation modal to make
only selected, retryable items eligible immediately. Select **No**, press `n`,
or press Escape to cancel. The controller records this action and does not
alter the queue, immutable selection, or final evidence. Other mutating
controls are unavailable. The monitor cannot write the database, signal a
worker, or expose the session capability token. Restart the downloader before
using it so the active session can create its endpoint.

The current aria2 adapter reports live transfer counters as unavailable until
its read-only RPC evaluation passes. Durable state and hashing progress remain
available. If the controller reports no eligible URL and a retry wait, it has
preserved the selected-run boundary. After diagnosing and correcting the
underlying problem, stop the controller and resume the same run ID with
`--retry-now`; keep the queue inputs and `--max-files` setting unchanged.

## Tor circuit renewal

The controller checks Tor isolation before it starts acquisition. It requires a
Tor ControlPort with `IsolateSOCKSAuth` and a readable control-cookie file. A
connectivity failure can request fresh circuits for future streams. The
controller does not change an active transfer or prove that Tor selected a new
route.

The monitor can request the same action. Start the monitor with `--control`,
press `t`, and confirm the action. The controller records every request in
`tor_renewals`, including rejected and failed requests. The default renewal
interval is 60 seconds. Set `--tor-newnym-interval 0` to disable renewal. Do
not copy the Tor control cookie, the monitor capability token, or an aria2 RPC
secret into logs, provenance records, or commits.

## Forensic workflow and verification

Treat final files in `downloaded_files/` as read-only evidence. Never overwrite
an existing final file; store a suspect replacement under
`download-state/redownload-candidates/`. Preserve raw `ALL_FILES_BER_*.txt`
snapshots and regenerate derived queues from the active baseline.

The controller writes signed provenance records below
`download-state/provenance/<run-id>/<session-id>/`. Each record directory
contains `events.jsonl`, `schema.json`, and `summary.json`. The records link a
final file to its source URL, final path, byte count, and SHA-256 digest. The
records do not prove that a source served the original collection.

Keep the trusted Ed25519 public key or expected fingerprint outside the record
directory. Verify a record set without modifying evidence:

```bash
python3 verify_provenance.py \
    download-state/provenance/RUN_ID/SESSION_ID \
    --destination downloaded_files --public-key TRUSTED_PUBLIC_KEY.pem
```

The verifier exits with a nonzero status when a signature, event chain, final
file, byte count, or SHA-256 digest does not match. Do not use the verifier to
repair, rename, or delete evidence files.

IPED parsing is not assumed complete. After an import, review IPED's processing
report and errors, compare IPED item counts and file types with the relevant
`ALL_FILES*.txt` inventory, and spot-check PST, MSG, archive, and document
extracts. Record unsupported, encrypted, corrupt, and parsing-failed items in
a dated note alongside the relevant output.

## Coding style and validation

Use four spaces for Python indentation, `snake_case` names, and standard
library modules. Write Bash with quoted expansions. Before handing off
downloader changes, run `python3 -m unittest -v test_download_priority.py`,
`python3 -m py_compile download_priority.py`, `bash -n run_priority.sh`,
`./run_priority.sh --dry-run --max-files 1`, and `git diff --check`. Do not
run a bulk download solely for a code change.

## Commits, pull requests, and data handling

Git history contains only `initial commit`, so use concise imperative subjects,
such as `Add PST header validation`. Keep commits focused. Do not commit
passwords, personal information, private correspondence, indexes, or newly
downloaded artifacts without explicit authorization. In pull requests, state
the purpose, commands run, affected manifests or outputs, and any
data-handling or storage impact. Review `git status` before every commit.
