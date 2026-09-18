# Specification: inventory snapshot, manifest, and queue export

Status: implemented, September 18, 2026. This specification defines the first
release of [inventory discovery](SPEC-inventory-discovery.md): local snapshot
parsing, reproducible manifest generation, and safe queue export. It defines
the concrete formats that the parent specification leaves open. It also
defines the snapshot diff and its dated report. It does not add network
refresh or directory crawling. Those stay in the parent specification.

The implementation is `src/inventory.py`. It does not open the acquisition
database, contact a network, or write outside the paths that the operator
names.

## Inputs and outputs

The tool reads one local listing file (the snapshot input). It writes only
new files and new directories. It never modifies, moves, or deletes an
existing file. Each command refuses to run if its output path exists.

| Command | Input | Output |
| --- | --- | --- |
| `snapshot` | listing file | snapshot directory in the store |
| `activate` | accepted snapshot | one line in `activations.jsonl` |
| `manifest` | snapshot, policy, base URL | manifest directory |
| `queue` | manifest | queue file plus provenance sidecar |
| `diff` | two accepted snapshots | diff directory with a dated report |

## Listing format

The listing is an `ls -R`-style text file, encoded in UTF-8, with `\n` line
ends. It consists of sections. Each section has one header line, one optional
blank separator line, and zero or more entry lines. A blank line after the
entries ends the section.

- A header line ends with `:` and names a directory relative to the
  inventory root. The root is `.:`. A leading `./` is removed once.
- An entry line has the form `<kind> <size-token> <name>`. The kind is `d`
  (directory) or `-` (file). The size token matches
  `[0-9]+(\.[0-9]+)?[KMGTPE]?`. One space separates the fields. The name is
  every remaining byte of the line, including leading, trailing, and repeated
  spaces, and a trailing `:`.

The parser classifies a line by its position, not by its last character. The
first nonblank line of the file, and the first nonblank line after a blank
line (a blank line directly after a header is only its separator), must be a
header. Every other nonblank line must be an entry line. The
older `diff.py` checks for a trailing `:` first, so a file named `x:` becomes
a header there. The parser must not repeat that defect.

Entries named `.` and `..` and all `d` entries are counted as directory
entries. They do not become manifest items. Only `-` entries become items.

### Parse accounting and limits

Every input line has exactly one class: `blank`, `header`, `directory`,
`file`, `unparsed`, or `decode_error`. The parse report counts each class.
The counts must add up to the line total.

- The parser decodes each line on its own with strict UTF-8. A decoding
  failure records a `decode_error` issue with the line number and the first 64
  bytes in hex. The parser never uses `errors='ignore'` or `errors='replace'`.
- A line longer than 8192 bytes is an `unparsed` issue with code
  `line_too_long`. The parser reads it in bounded chunks and keeps no more
  than 8192 bytes of it.
- A snapshot larger than 1 GiB is refused before parsing.
- The report lists at most 1000 issues. It always gives the full issue count.
  Each issue has a line number, a code, and a detail limited to 120
  characters.
- A carriage return is part of the name. The path check rejects it as a
  control character.

Issue codes: `decode_error`, `line_too_long`, `bad_entry`, `bad_size_token`,
`empty_name`, `not_a_header`, `duplicate_header`.

## Snapshot store

`snapshot --input FILE --store DIR` copies the exact bytes to
`DIR/snapshots/<UTC date>-<sha256 first 16>/raw.txt` (read-only). It writes
`snapshot.json` (source path, size, SHA-256, tool version, parser version,
import time, status) and `parse-report.json` beside it.

- The store creates the directory with an exclusive `mkdir`. An existing
  snapshot with the same date and hash is an error, not a replacement.
- A snapshot has status `accepted` if it has zero issues of the classes
  `decode_error`, `line_too_long`, `bad_entry`, `bad_size_token`,
  `empty_name`, and `not_a_header`, or at most `--max-issues N` of them.
  It has status `rejected` otherwise, and it needs at least one `file` entry.
  `duplicate_header` alone does not reject a snapshot.
- A rejected snapshot is stored under `DIR/rejected/` with the same files, so
  the operator can inspect it. It can never become a baseline.
- An HTTP 200 error page fails this check. Its first nonblank line is not a
  header, so the snapshot is rejected.

`activate --store DIR --snapshot <sha256 or unique prefix>` appends one JSON
line to `DIR/activations.jsonl` with the snapshot hash, the previous active
hash, and the UTC time. It refuses a rejected or unknown snapshot. Activation
does not alter any snapshot or manifest. Later commands take an explicit
snapshot path; no command reads the active baseline implicitly.

## Identity and path mapping

The base URL names the source collection. It has the form
`http(s)://<host>/<collection>` or `http(s)://<host>/<collection>/data`, with
no user information, no query, and no fragment. The tool rejects any other
base URL. The `/data` segment is optional. It is part of the destination only
if the operator includes it.

The source URL is `<base URL>/<encoded path>`. The encoder splits the
relative path on `/`. For each segment, it encodes the UTF-8 bytes and keeps
only `A-Z a-z 0-9 - . _ ~`. Every other byte becomes `%` plus two uppercase
hex digits. A literal `%` therefore becomes `%25`, and a name that already
contains `%20` cannot merge with a name that contains a space. The
destination path is `<collection>/<path>`, or `<collection>/data/<path>` if the
base URL ends in `/data`. It equals the value that
`relative_path()` in `src/tod-dl.py` computes for the URL. A test must check
this agreement.

The path check rejects an item with reason `unsafe_path` if any segment is
empty, `.` or `..`, or contains a control character (U+0000 to U+001F, U+007F).
It rejects a root-level `ALL_FILES` with reason `inventory_listing`. This
matches `relative_path()`.

Two items with the same path are a duplicate. The first item keeps its place.
Each later item is rejected with reason `duplicate_path` and the line number
of the first. Two different paths that are equal after Unicode NFC
normalization both stay in the manifest. The later item gets the flag
`nfc_collision_with_line:<line of the first>`, and the header totals count
them. The tool never renames or merges them.

## Policy file

The policy is a JSON object. The tool rejects unknown keys and any invalid
value with a message that names the key.

```json
{
  "policy_version": "1",
  "rules": [
    {"id": "skip-tmp", "match": {"extensions": [".tmp"]},
     "disposition": "rejected", "reason": "temporary file"},
    {"id": "mail", "match": {"path_prefix": "Documents/Mail"},
     "disposition": "priority", "reason": "mail tree"}
  ],
  "default": {"disposition": "deferred", "reason": "no rule matched"}
}
```

- `match` holds one or more of `path_prefix` (matches whole path segments),
  `extensions` (lowercase, each starting with `.`, compared with the
  lowercased name), and `names` (exact, case-insensitive base names). All
  keys in one `match` must match. An empty `match` is invalid.
- `disposition` is `priority`, `deferred`, or `rejected`. `reason` is required
  and must not be empty. Rule IDs are unique. A policy has at most 256 rules.
- The first matching rule wins. A rule's rank is its position. The default
  rule has the last rank.

## Manifest

`manifest --snapshot DIR --policy FILE --base-url URL --output DIR` reads the
accepted snapshot, re-verifies the SHA-256 of `raw.txt` against
`snapshot.json`, and writes to a temporary directory. It then renames that
directory to `--output`, which must not exist.

`manifest.jsonl` is JSON Lines. Every line uses sorted keys, compact
separators, and ASCII escapes, so the bytes are the same on every run. The
first line is the header with the fields `manifest_format`, `snapshot_sha256`,
`parser_version`, `policy_version`, `policy_sha256`, `base_url`, and `totals`.
Each later line is an item with the fields `line`, `path`, `size_token`,
`source_url`, `destination`, `disposition`, `rule`, `reason`, `rank`, and
`flags`. An item that a built-in check rejects (`builtin:unsafe_path`,
`builtin:inventory_listing`, `builtin:duplicate_path`) has `source_url` and
`destination` set to null. Its `rule` field names the built-in check.

Items appear in this order: priority, deferred, rejected. Within a
disposition, items sort by rule rank, then by source line number. The tool
writes items to per-rank temporary files, so memory use does not grow with the
item count.

The totals reconcile with the parse report: `files` equals `priority` plus
`deferred` plus `rejected`. The tool exits with an error if they differ.

The sidecar `manifest.meta.json` holds every volatile value: generation
time, tool version, Python version, and the snapshot path. `manifest.sha256`
holds the SHA-256 of `manifest.jsonl`. Repeating the command with the same
snapshot, policy, and base URL must produce the same `manifest.jsonl` and the
same SHA-256.

The manifest carries the size token as text only. It has no exact size and no
checksum. An equal size token does not prove equal bytes.

## Queue export

`queue --manifest DIR --output FILE [--disposition priority|deferred]
[--generation ID]` writes the existing plain queue format.

- The tool verifies `manifest.jsonl` against `manifest.sha256` first. A
  mismatch is an error.
- The output file must not exist. The tool writes a temporary file in the same
  directory and links it into place, so it can never replace a queue that a
  run might read. The file is read-only.
- The first lines are comments with the manifest SHA-256 and the disposition.
  Each later line is `<source URL> size=<token>`. If the operator gives
  `--generation`, each line also carries `generation=<ID>`. The tool does not
  add a generation token on its own, because a new value can start early
  rechecks in the downloader.
- Before writing, the tool checks each URL: no whitespace, no user information,
  no query, and a path that matches `<collection>/<path>`. The path can start with `data/`. It exports
  each URL once.
- `FILE.provenance.json` records the manifest SHA-256, the queue SHA-256, the
  disposition, the item count, and the generation time.

The tool does not update any queue in place and does not touch a persisted
acquisition run.

## Snapshot diff

`diff --old DIR --new DIR --output DIR` compares two accepted snapshots. It
re-verifies both raw files and refuses a rejected snapshot. The output
directory must not exist. The tool writes it in a temporary directory and
renames it.

Every unique path falls into exactly one category:

- `added`: the path is only in the new snapshot.
- `removed`: the path is only in the old snapshot. A removed path does not
  authorize local deletion.
- `metadata_changed`: the path is in both and the size tokens differ.
- `unchanged`: the path is in both and the size tokens are equal. This means
  unchanged inventory metadata only. Rounded tokens do not prove equal bytes.
- `ambiguous`: the path is listed more than once in either snapshot, cannot
  map safely, or one snapshot has it only in a Unicode-normalization
  variant of a path that only the other snapshot has. Each item carries its
  reasons: `duplicate_path_in_old`, `duplicate_path_in_new`, `unsafe_path`,
  `nfc_equivalent_add_remove`.

The root `ALL_FILES` entry is not a change. The tool counts it apart, because
the listing file changes with every refresh.

`diff.jsonl` has a header line (both snapshot hashes, parser version, totals)
and one line per path that is not `unchanged`, sorted by path. It is
byte-for-byte reproducible. The totals must reconcile: for each snapshot,
file lines equal unique paths plus duplicate lines plus listing entries, and
the category counts add up to the union of both path sets. The tool stops with
an error if they differ. `diff.sha256` holds the SHA-256, and
`diff.meta.json` holds the generation time. `report.md` is the dated report.
It gives both snapshot hashes and parse summaries, the category table, a
per-directory table (first 25 top-level directories), and the reading notes
above.

## Policy validation against an earlier filter

A policy can be checked against filtered and deferred listings that an
operator made earlier. Build the manifest from the original listing. Then
compare the `priority` and `deferred` path sets with those listings. Keep
case-specific policies in the case directory, not in this repository.

On 2026-09-18 this method reproduced two earlier lists exactly: 3 and 59
deferred trees gave the same deferred sets as the old files (2,544 and
147,382 files). The old filter also removed files by extension
(`.cab .com .dll .exe .iso .lnk .msi .msp .ocx .rpm .scr .sys .url .wc`) and
by name (`desktop.ini`, `thumbs.db`) outside the deferred trees. The priority
sets equal the old filtered sets, except that the root `ALL_FILES` entry is
now rejected. A third list could not be reconciled: its deferred file matched
no listing.

The extension and name rules are inferred from what the old filter removed.
They are not the old filter's own definition. Review them before you rely on
them.

## Acceptance

Tests use synthetic fixtures only. They must show these conditions:

- Every input line has one class, and the counts add up to the line total.
- A name with a trailing `:`, leading or repeated spaces, Unicode, and a
  literal `%` keeps its exact bytes and maps to a distinct URL.
- Invalid UTF-8, an over-long line, and an HTTP error page are reported, and
  they reject the snapshot.
- Unsafe paths, duplicates, and NFC collisions are reported and never lost.
- Repeated manifest generation gives identical bytes. Only the sidecar
  changes.
- Every exported queue line passes `read_queues()` from `src/tod-dl.py` with
  zero rejections, and the path agrees with `relative_path()`.
- No command changes the input listing, an existing snapshot, or an existing
  queue. A tampered manifest blocks queue export.
- Added, removed, changed-size, duplicate, unsafe, and normalization-variant
  paths land in the right diff category. The diff totals reconcile, and two
  runs give the same bytes.

## Runbook

1. Run `snapshot` on the listing. Read `parse-report.json`. If the status is
   `rejected`, fix the source listing. Do not raise `--max-issues` until you
   have read every issue.
2. Run `activate` to record the baseline.
3. Write a policy file. Run `manifest` and read the totals and `rejected`
   reasons.
4. Run `queue`. Start the acquisition separately with a bounded
   `--max-files`.
5. After you import a newer listing, run `diff` on the old and new
   snapshots. Read `report.md`, and review every `ambiguous` item.

## Remaining work

Network refresh and directory crawling are not implemented. They stay in the
parent specification, and they wait for a reliable acquisition workflow and
the source pilot.
