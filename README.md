# TOD-DL

TOD-DL is a resumable downloader for a defined URL queue. It protects existing
final files, records durable state in SQLite, and creates signed provenance
records for completed files.

> **Warning:** Use TOD-DL only for material that you are authorized to acquire
> and retain. Treat completed files as forensic evidence.

## Repository contents

The repository contains downloader source code and test fixtures. It does not
contain acquired evidence, queue files, download state, or derived content.

- `tod-dl.py` runs the bounded downloader.
- `run_priority.sh` runs the downloader with three local priority queues.
- `monitor.py` displays the read-only telemetry snapshot.
- `verify_provenance.py` verifies a signed provenance record set.
- `test_tod_dl.py` and `test_monitor.py` contain tests.
- `acquisition_evaluation.py` provides local acquisition-engine fixtures.

## Requirements

You need Python 3, `aria2c`, `torsocks`, and a local Tor service. The Tor
ControlPort must use `IsolateSOCKSAuth`. The downloader reads the Tor control
cookie from `/run/tor/control.authcookie` by default.

Create the downloader environment with these commands:

```bash
python3 -m venv .venv
. .venv/bin/activate
python3 -m pip install -r requirements-downloader.txt
```

Create a separate monitor environment when you use the Textual monitor:

```bash
python3 -m venv .venv-monitor
. .venv-monitor/bin/activate
python3 -m pip install -r requirements-monitor.txt
```

## Prepare a queue

Each queue file contains one URL per line. A URL must identify a file below a
host-specific `data/` path. Blank lines, comment lines, duplicate URLs, unsafe
paths, URLs with credentials, and URLs with query values are ignored or
rejected.

Store queue files outside this source repository when they contain case data.
The `run_priority.sh` wrapper expects these untracked files beside the script:

- `urls_1_priority.txt`
- `urls_2_priority.txt`
- `urls_3_priority.txt`

Copy the three example files before you use the wrapper. Replace every
`example.invalid` URL with an authorized source URL. The example URLs are safe
placeholders and do not identify a source.

```bash
cp urls_1_priority.txt.example urls_1_priority.txt
cp urls_2_priority.txt.example urls_2_priority.txt
cp urls_3_priority.txt.example urls_3_priority.txt
```

## Run an acquisition

Start with a dry run. The dry run reads queues and reports the selected final
paths. It does not start Tor, `aria2c`, or a source request.

```bash
python3 tod-dl.py \
    --queue /case/urls_priority.txt \
    --destination /case/downloaded_files \
    --state /case/download-state \
    --dry-run --max-files 3
```

After an operator reviews the paths and storage, start a bounded run:

```bash
python3 tod-dl.py \
    --queue /case/urls_priority.txt \
    --destination /case/downloaded_files \
    --state /case/download-state \
    --workers 4 --max-files 5
```

The destination and state directories must use the same filesystem. The
controller keeps a 10 GiB free-space reserve by default. It never replaces an
existing final. A collision creates a review item and preserves incoming bytes
under `download-state/redownload-candidates/`.

## Resume a run

Use the same run ID, queue files, and `--max-files` value to resume a stopped
run. The downloader binds the run ID to queue file hashes and the selection
value. A retry cannot add a later queue item to the selected set.

```bash
python3 tod-dl.py \
    --queue /case/urls_priority.txt \
    --destination /case/downloaded_files \
    --state /case/download-state \
    --run-id RUN_ID --max-files 5 --retry-now
```

Use `--status` to read persisted state without starting transfers. Do not use
the legacy `tor-dl-subdir*.sh` wrappers for continued acquisition.

## Monitor and renew Tor circuits

The monitor reads a published telemetry snapshot. It does not open the
acquisition database or write final evidence.

```bash
python3 monitor.py --state /case/download-state \
    --run-id RUN_ID --control
```

With `--control`, press `r` to make selected retryable items eligible now.
Press `t` to request fresh Tor circuits for future streams. Both actions need
confirmation. Tor renewal does not change active transfers or prove a new
route. The controller records each renewal request and applies the configured
rate limit, which defaults to 60 seconds.

## Verify provenance

The controller writes signed record sets below
`download-state/provenance/<run-id>/<session-id>/`. Keep the trusted Ed25519
public key or its fingerprint outside the record directory.

Run the verifier to check the signature, event chain, final paths, byte counts,
and SHA-256 digests. The verifier does not modify evidence.

```bash
python3 verify_provenance.py \
    /case/download-state/provenance/RUN_ID/SESSION_ID \
    --destination /case/downloaded_files \
    --public-key /secure/TRUSTED_PUBLIC_KEY.pem
```

## Test the source

Run the complete test set before you change downloader behavior:

```bash
python3 -m unittest -v test_tod_dl.py test_monitor.py
python3 -m unittest -v test_acquisition_evaluation.py
python3 -m py_compile tod-dl.py acquisition_evaluation.py
bash -n run_priority.sh
git diff --check
```

## Test the acquisition fixture harness

Run the local self-test before you create a transfer-engine adapter. The
self-test starts a loopback HTTP fixture server. It generates only synthetic
bytes. It does not start `aria2c`, `torsocks`, Tor, or a source request.

```bash
python3 acquisition_evaluation.py \
    --self-test --output /tmp/tod-dl-evaluation
```

The output directory must be empty. The self-test writes an immutable fixture
manifest, a machine-readable fixture event log, and a structured report. It
does not select an engine. An engine becomes selected only after E01 through
E14 pass and meet all evaluation targets.

## Development status

Read [open development work](specs/OPEN-WORK.md) for the current implementation
state and the planned work that remains.

The acquisition-engine evaluation is local-only. It needs no onion target and
does not permit a source pilot. Read the
[acquisition-tool evaluation specification]
(specs/SPEC-acquisition-tool-evaluation.md) before you implement or run the
evaluation.

## Portfolio project

TOD-DL demonstrates durable acquisition design for forensic work. It includes
SQLite state recovery, no-overwrite finalization, SHA-256 validation, signed
provenance, Tor isolation checks, a read-only monitor, local control actions,
and deterministic acquisition-engine fixtures.

Use the project as a portfolio example of safety-focused systems work. Do not
describe TOD-DL as production-ready or fully specification-complete. The open
work record identifies the remaining engineering and evaluation work.

## License

The [TOD-DL Non-Commercial License](LICENSE) permits personal, educational,
and portfolio-review use. Commercial use and redistribution need prior written
permission from the copyright holder.

## Next steps

Create case-specific queues outside the repository. Run a dry run and review
the selected paths before you start an acquisition.
