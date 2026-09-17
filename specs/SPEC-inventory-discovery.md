# Specification: reproducible inventory discovery

Status: planned, September 15, 2026. This specification separates
finding source files from acquiring their bytes. Implement it after the basic
[acquisition workflow](SPEC-reliable-acquisition.md) is reliable; existing URL
queues remain usable without a crawler.

## Outcome and initial scope

Turn preserved source inventories into reproducible, versioned manifests.
Refresh listings without mutating the active baseline or silently extending a
running acquisition. Use existing HTTP and parsing tools where practical;
don't build a general browser automation platform.

For the first release, support the configured plain-text inventory format and
an explicit local snapshot input. Add network refresh of known inventory URLs
next. Add directory-page crawling only if the inventory is absent, incomplete,
or proven stale. JavaScript rendering and authenticated navigation are outside
the initial scope.

## Snapshot acquisition

Fetch only configured inventory URLs through the verified Tor route. Apply
the acquisition connection ceiling and outage policy to discovery too. While
acquisition saturates the ceiling, defer discovery instead of adding requests.

Store each response under a unique dated snapshot directory, with exact raw
body bytes, source URL, UTC request/response times, status, redirect chain,
available headers, local SHA-256, and tool version. Keep failed responses
separate from accepted inventories. An HTTP 200 error page must not become the
active baseline. Do not replace an existing snapshot.

A snapshot is accepted only after format validation and a parse report.
Partial or malformed input must not silently replace a previously complete
baseline. Record baseline activation explicitly by snapshot hash. A refresh
must produce a new candidate manifest, leaving current runs unchanged.

## Parsing and identity

Preserve original path text, size tokens, and source line numbers. Report
decoding failures and unparsed nonblank records rather than dropping bytes
with `errors='ignore'`. Keep a versioned parser and fixtures for directory
headers, spaces, Unicode, duplicates, empty files, and malformed lines.

Separate source identity from local storage naming. Store exact source URLs
and deterministic encoded URLs; document the encoding rules so a literal
percent sign and an already encoded name cannot silently merge. Reject unsafe
paths and report mapping collisions. Don't rename legacy acquired paths while
improving normalization.

Diff snapshots into added, removed, metadata-changed, unchanged, and ambiguous
items. A removed item does not authorize local deletion. A size token change
at an existing path must appear in the report. Equal rounded sizes cannot
prove unchanged bytes; label the result as unchanged inventory metadata.
The current `diff.py` compares paths only and does not meet this contract.

## Queue generation

Generate queues from an explicitly chosen baseline and versioned priority and
deferral rules. Keep current filtering decisions until a separate policy
change is requested. Every accepted item must be assigned to priority,
deferred, or rejected-with-reason; totals must reconcile with parsed input.

Each manifest must contain its snapshot hash, parser version, policy version,
ordered items, and selection reasons. Keep volatile generation timestamps in
a sidecar so the manifest itself is byte-for-byte reproducible. Repeating the
same inputs and versions must produce the same manifest and SHA-256.

Export the existing plain URL queue format for compatibility, plus structured
records for inventory metadata and provenance. Do not update existing queues
in place while they might be in use. Deduplicate deterministically within a
source generation and preserve priority ordering across exports.

## Optional directory discovery

If directory crawling is required, first test an existing crawler or lftp
against a saved synthetic listing fixture. Keep crawl state separate from
download state. Configure exact origins and path prefixes, maximum depth,
page count, response bytes, and time; report which bound stopped discovery.

Persist the visited frontier and rejected links. Resolve relative links,
remove fragments, detect loops, and enforce scope across redirects. Don't
follow arbitrary external hosts, invoke downloaded scripts, or traverse parent
directories outside the configured root. Record fetch errors and incomplete
branches so a partial crawl cannot masquerade as a complete inventory.

Use a separate URL identity policy for discovery queries; don't remove query
parameters from acquisition URLs without knowing their meaning. New links
become candidate manifest items, not automatic extensions of a running job.

## Acceptance and next steps

Use synthetic snapshots and listing pages to demonstrate these conditions:

- Repeated generation produces identical manifests and compatibility queues.
- Added, removed, changed-size, duplicate, malformed, and ambiguous items are
  counted correctly, with every input record accounted for.
- Existing snapshots, queue inputs, and acquired-file hashes remain unchanged.
- An interrupted or error-page refresh cannot activate a baseline.
- Unicode and percent-encoding fixtures preserve source identity and report
  collisions without silently losing names.
- Cycles, out-of-scope redirects, pagination, and configured bounds produce
  a finite crawl with an explicit completeness report, if crawling is added.
- Regenerating a queue does not alter a persisted acquisition run's item set.

Deliver the parser, manifest exporter, fixtures, and a dated parse/diff report
before adding scheduled refresh. The runbook must show how to choose a
baseline, inspect rejected records, generate a queue, and start a separately
bounded acquisition. No live scraping or bulk acquisition is part of writing
or reviewing this specification.
