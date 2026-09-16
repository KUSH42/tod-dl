# TOD-DL source repository guidelines

TOD-DL is a source repository for the Tor Onion Dump Downloader. Keep source
changes reproducible, testable, and safe for forensic acquisition work.

## Repository scope

The repository contains source code, tests, fixtures, dependency files, and
design specifications. It must not contain acquired files, queue files,
download state, derived content, secrets, private correspondence, or indexes.

The project root contains the downloader, monitor, control server, provenance
writer, and verifier. `fixtures/` contains non-sensitive test inputs. The
`SPEC-*.md` files define expected behavior. `OPEN-WORK.md` lists incomplete
development work and does not change a specification requirement.

Use a separate case directory for these runtime paths:

- `downloaded_files/`
- `download-state/`
- queue files such as `urls_1_priority.txt`
- extracted, archived, or IPED output

## Main modules

`download_priority.py` controls resumable acquisition. It invokes one `aria2c`
process per selected URL through `torsocks`. It records durable state in SQLite
and never overwrites a final file.

`monitor_priority.py` reads a published telemetry snapshot. It does not open
the acquisition database or write evidence. `controller_control.py` provides
the same-user control endpoint for confirmed retry and Tor renewal requests.

`provenance.py` writes signed provenance records. `verify_provenance.py` reads
and verifies those records without changing final files.

## Development commands

Run these commands before you hand off downloader changes:

```bash
python3 -m unittest -v test_download_priority.py test_monitor_priority.py
python3 -m py_compile download_priority.py download_telemetry.py \
    monitor_priority.py controller_control.py provenance.py verify_provenance.py
bash -n run_priority.sh
git diff --check
```

Run a dry run only with operator-provided case queues and temporary paths. Do
not start a bulk download solely to validate a code change.

## Acquisition safety

Use `--max-files` to define an immutable selected set. Reuse the same run ID,
queue inputs, and `--max-files` value when you resume a run. A retry must not
add a later queue item to the selected set.

The destination and state directories must use the same filesystem. The
controller keeps a 10 GiB free-space reserve by default. Do not lower the
reserve unless an operator has assessed the storage risk.

Treat each final file as read-only evidence. Do not replace an existing final.
Store a suspect replacement under `download-state/redownload-candidates/`.

## Tor and control safety

Tor must provide `IsolateSOCKSAuth` on the configured SocksPort. The controller
must verify Tor isolation before it admits a transfer.

The monitor can request `retry_now` and `renew_tor_circuits` only after user
confirmation. A Tor renewal affects future streams. It must not change active
transfers or claim that Tor selected a new route. Do not log or commit Tor
control cookies, monitor capability tokens, or aria2 RPC secrets.

## Provenance safety

The controller writes provenance records below
`download-state/provenance/<run-id>/<session-id>/`. A completed final requires
a durable provenance event. Keep the trusted Ed25519 public key or expected
fingerprint outside the record directory.

The verifier must remain read-only. It must report signature, event-chain,
path, byte-count, and SHA-256 mismatches without repairing evidence files.

## Code style

Use four spaces for Python indentation, `snake_case` names, and standard
library modules when they meet the requirement. Quote Bash expansions. Keep
state transitions and filesystem operations explicit and durable.

Add a focused test for every recovery, finalization, control, or scheduler
change. Fault tests must use temporary paths and local fixture bytes. They must
not start `aria2c`, `torsocks`, or a source request.

## Documentation and commits

Update the relevant `SPEC-*.md` file and `README.md` when a public behavior
change needs user instructions. Keep Markdown lines at 80 characters or fewer.

Use the Conventional Commits format: `type(scope): summary`. Write the summary
in past tense. For example: `fix(ci): fixed issue where duplicate GitHub runs
would error out due to duplicate artifact upload`.

Review `git status` before you commit. Keep commits focused. Do not commit
virtual environments, evidence, queues, state, secrets, or personal data.
