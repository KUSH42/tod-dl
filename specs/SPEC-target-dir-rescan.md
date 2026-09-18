# Specification: on-demand target-directory rescan

Status: planned, September 18, 2026. This specification depends on the
path-encoding and checksum rules in the
[inventory discovery specification](SPEC-inventory-discovery.md) and on the
`expected_checksum` field added to acquisition in
[SPEC-reliable-acquisition](SPEC-reliable-acquisition.md). Implement it after
inventory discovery produces deterministic URL-to-path mappings.

## Outcome and initial scope

Reconcile files already present in the target directory against the current
URL inventory, without re-downloading anything during the scan. Detect three
conditions: a local file with no matching URL, a URL with no matching local
file, and a local file whose checksum does not match the recorded expected
checksum. Produce a dated report and a candidate acquisition queue for the
missing and mismatched items. Do not delete, move, or overwrite any file
during a rescan.

A rescan runs on demand from an explicit command. A file-system watch may
also queue a rescan automatically when it detects a change in the target
directory; the watch triggers the same rescan, it does not add a separate
reconciliation path.

## Manual trigger

Add a `rescan` command that takes the target directory and a baseline
manifest (from inventory discovery) as arguments. The command must:

- Read the baseline manifest by path and verify its SHA-256 before use. Refuse
  to run against a manifest that fails this check.
- Walk the target directory and build the set of local files by relative
  path. Symbolic links and files outside the target directory root must not
  be followed.
- Map each local file to a URL using the deterministic encoding rules from
  inventory discovery. A local file whose path does not decode to a known
  URL under those rules is unmatched, not an error.
- Exit with a non-zero status only on a setup failure (missing manifest,
  unreadable directory). A rescan that finds mismatches is a successful run;
  report the mismatches, do not fail the command for finding them.

## File-system watch

The watch observes create, write, and delete events under the target
directory root and debounces them into a single rescan request. Configure an
exact debounce window in seconds; document the default value in the runbook.
The watch must not read file contents on every event; it enqueues a rescan
and lets the rescan do the checksum work.

The watch process is separate from any running acquisition. It must not
pause, cancel, or otherwise affect an in-progress download. If the watch and
a manual command both request a rescan for the same target directory at the
same time, run one rescan and let the second request join it; do not run two
concurrent rescans over the same directory.

## Matching and checksum verification

For each local file matched to a known URL:

- If the manifest records an `expected_checksum` for that URL, compute the
  local file's SHA-256 and compare it. A mismatch is reported as
  `checksum_mismatch`, with both the expected and computed values.
- If the manifest records no `expected_checksum` for that URL, report the
  file as `unverified_present`; a rescan must not invent a checksum
  expectation that acquisition itself does not have.
- Reading a file to compute its checksum must not fail the whole rescan; a
  single unreadable file becomes a `read_error` entry with the OS error
  message, and the rescan continues over the remaining files.

For each URL in the manifest with no matching local file, report
`missing`. For each local file with no matching URL, report `unmatched`; a
rescan must not guess that this is empty space and must not delete it.

Do not treat equal file size alone as a checksum match. If the manifest has
no `expected_checksum` and no other trusted validator for a URL, the best a
rescan can report is `unverified_present`; label it as such, not as
confirmed correct.

## Rescan report

Write a dated report file under the existing reports directory with:

- The manifest path and SHA-256 used for the rescan.
- Total counts for `checksum_mismatch`, `unverified_present`, `read_error`,
  `missing`, and `unmatched`.
- One record per item, with its URL (when known), local path (when known),
  expected checksum (when known), computed checksum (when computed), and
  category.

The report is immutable once written; a repeated rescan against the same
manifest and directory state must produce a byte-for-byte identical report,
except for the recorded start time.

## Candidate queue generation

Generate a candidate acquisition queue from the `missing` and
`checksum_mismatch` items only. Use the existing queue-generation rules from
inventory discovery for priority and format. `unmatched`, `unverified_present`,
and `read_error` items never enter the candidate queue automatically.

The candidate queue is a new file; a rescan must not update an existing
queue in place, and must not start or extend a running acquisition. Starting
acquisition against the candidate queue is a separate, explicit action.

## Acceptance and next steps

Use a synthetic target directory and manifest to demonstrate these
conditions:

- A file with a checksum mismatch produces exactly one `checksum_mismatch`
  record with correct expected and computed values, and appears in the
  candidate queue.
- A URL with no local file produces exactly one `missing` record and appears
  in the candidate queue.
- A local file with no matching URL produces exactly one `unmatched` record
  and is left untouched on disk.
- A matched file with no recorded `expected_checksum` produces
  `unverified_present` and does not appear in the candidate queue.
- An unreadable file produces `read_error` and does not stop the rescan from
  completing the remaining files.
- Two rescan requests arriving together (manual command and watch-triggered)
  over the same directory produce one rescan, not two.
- Repeating a rescan against unchanged input produces an identical report.

No live acquisition is part of writing or reviewing this specification.
