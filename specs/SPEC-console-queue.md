# Specification: console queue view

Status: partially implemented, September 17, 2026. This view shows the
immutable selected set, remaining work, and retry deadlines without changing
acquisition order.

The view supplements the [console UI](SPEC-console-ui.md) and uses the
[read-only inspection interface](SPEC-console-inspection.md).

## Scope and layout

The **Queue** tab must be available in observer and control modes.
It must contain only selected run items, including completed items.
Skipped-existing files outside selection must appear only as a separate
summary count. Filters must never expand the selected set.

Each row must show immutable queue rank, basename and short item ID, display
bucket, phase when available, received and total bytes, and retry deadline.
Total bytes must come only from an active engine sample, the same source used
for snapshot totals. Show it as unknown for any item without an active engine
sample, even when an inventory-size estimate exists for that item.
Default order must be ascending manifest queue rank, then stable item ID.
Ranks must retain controller values; filtering must not renumber items.
Duplicate basenames must remain distinguishable.

The header must show run ID, selected count, loaded row count, active filters,
read time, and revision. Loaded row count is the current page's row count,
not a running total across pages. `matching_count` is out of scope for this
release:
the service must always return it as null, and the UI must always show
**Matching count unavailable**. Page length must not serve as an estimate of
the full result count.

The summary must use published run totals for retained bytes, known remaining
bytes, and unknown-size items. Label these values **Whole selected run**.
Filtered rows must not change those totals. Every non-complete display bucket
must remain visible as unfinished or unresolved work, including
`review_required`, `unavailable`, `exhausted`, `existing_unverified`, and
`unknown`.

At 80 columns, retain rank, basename, short item ID, display bucket, and
retry deadline. Move phase and received/total bytes into item details. Below
80 by 24, provide a scrollable queue with its filter and freshness labels
visible. Refresh and resize must preserve selection by item ID.

## Filters and literal search

The state filter must support **All** (wire value `bucket=all`) and each
telemetry display bucket: `queued`, `busy`, `retry`, `exhausted`, `complete`,
`existing_unverified`, `review_required`, `unavailable`, and `unknown`.
The service must use the same state-to-bucket mapping as snapshot counts.
[SPEC-download-telemetry.md](SPEC-download-telemetry.md) owns the durable
status values that map to `unavailable`; this view must not define them.

**/** must focus a search field. **Enter** must apply the search; **Escape**
must cancel unsubmitted changes. Search must match a case-sensitive literal
substring of the original logical path, mapped storage path, or full item ID.
An empty query must clear search. Search must not interpret regular
expressions, shell syntax, SQL wildcards, or Rich markup.

State and search filters must combine with logical AND. The UI must retain
the submitted query and show a **Clear filters** action. Source URLs and
absolute private paths must not appear in rows or search-result previews.
Typing `q`, `r`, or `t` in search must enter text, not trigger an action.

This release must provide manifest order only. Column headers must not imply
sorting support. A later sorting extension must define stable pagination and
must not change the controller's acquisition order.

## Pagination and consistency

`list_queue` must accept `bucket`, `query`, `page_size`, and an optional
`cursor`. Defaults must be `bucket=all`, empty query, 100 rows, and the first
page.
The maximum page size must be 200. Responses must include `rows`,
`next_cursor`, nullable `matching_count`, and the shared revision envelope.
Rows must contain the fields required above and canonical item identities.

The service must use keyset pagination: continue after the last queue rank
and item ID. It must not use increasing SQL offsets or load the whole queue.
Queries must use an index for run selection and rank traversal.

The service must apply the bucket and query filters while it scans the
manifest in keyset order, continuing until it fills `page_size` matching
rows or exhausts the manifest. "Full" is defined by row count alone: a page
holding `page_size` matching rows gets a non-null `next_cursor`, even when no
further matching row exists. The service must not look ahead to detect that
case. A page holding fewer than `page_size` rows is short because the
manifest is exhausted, and it gets a null `next_cursor`. A request against a
non-null cursor that finds no further matching rows must return an empty
page with a null `next_cursor`.

A bucket or query filter makes this scan a literal substring search, so the
scan must obey the inspection deadline. If the deadline is reached before the
scan fills the page or exhausts the manifest, the service must return
`unavailable` with a narrower-search suggestion, per
[SPEC-console-inspection.md](SPEC-console-inspection.md); it must not return
the partial scan as a page.

**PageDown** must request the next page. **PageUp** must request the previous
visited page. Disable unavailable directions. The monitor must retain at most
three pages and 100 cursor entries. If earlier navigation expires, offer
**First page**. Filter changes must clear cursors and start at the first page.
An exhaustion-confirmation request does not push a new entry onto the visited
page history and must not change which page **PageUp** returns to.

Refresh must not silently remove a focused row or jump to another page.
When the durable revision changes, retain the displayed page as recorded
data, show **Results changed**, and offer **Refresh results**. Refresh must
restart pagination. Restore the selected item if present on the new page;
otherwise select the first row and announce the selection change.
The UI must not combine pages from different revisions or sessions.

An empty selected set must show **No selected items**, checked before any
`list_queue` call and independent of filters; this takes precedence over
**No matching items** whenever both conditions would otherwise hold.
A successful empty result on the first page (no prior cursor), with a
non-empty selected set, must show **No matching items**. This applies
whether or not a bucket or query filter is active.

An empty page returned against a non-null cursor is the exhaustion
confirmation described above, not a fresh empty result: the UI must keep
showing the currently displayed rows and their header fields (loaded row
count, read time, revision), must not show **No matching items**, and must
disable **PageDown**. Missing service, rejected requests, and timeouts must
show unavailable status instead of either message.

## Interaction and retry information

**Tab** and **Shift+Tab** must move between filters, rows, and page actions.
Arrow keys must select rows. **Enter** on a row must open
[item details](SPEC-console-item-details.md). **Escape** from details must
restore the queue's filter, page, selection, and scroll position.
**l** must open logs for the selected item under the console log rules.
**?** must show context-specific help. Outside text entry, **q** and
**Ctrl+C** must close only the monitor.

Retry rows must show the recorded UTC deadline and a local countdown when
freshness permits. At zero, show **Eligible; awaiting controller**.
Freeze countdowns when stale. This release has no controller mechanism to
re-admit an exhausted or review-required item, so the view must always show
those buckets as not eligible, never a countdown.
Global or origin cooldown can delay an eligible item; show
**Eligible; cooldown active** instead of **Eligible; awaiting controller**.

Selection and filtering must never submit a retry command. The existing `r`
control must retain the controller-confirmed scope defined in
[SPEC-controller-control-ui.md](SPEC-controller-control-ui.md). That spec
does not yet document a `t` binding; until it does, this view must not
change `t`'s existing run-scoped behavior. A filtered queue must not imply
that a run-scoped action applies only to visible rows.
Row-scoped retry, reordering, removal, export, and queue editing are deferred.

## Acceptance criteria

Tests must use temporary manifests and synthetic pages, without source access.

- Verify manifest order, stable ranks, duplicate names, and every bucket that
  [SPEC-download-telemetry.md](SPEC-download-telemetry.md) can produce. The
  `unavailable` bucket has no defined durable-status trigger there yet; this
  view cannot test it until that spec defines one.
- Verify literal search, combined filters, search cancellation, and clearing.
- Verify page boundaries with 0, 1, 100, 200, and 201 selected items.
- Verify no duplicate or missing rows across pages at one stable revision.
- Verify cursor expiry, revision changes, session changes, and late responses.
- Verify empty results differ from service and query failures.
- Verify details return, selection retention, keyboard focus, and resizing.
- Verify retry countdowns do not imply admission or trigger commands.
- Verify filters do not change selection, totals, or command scope.
- Run a scale test against one million synthetic items. The service must not
  load the full queue into memory, and the test must not instantiate the
  Textual widget tree. Measure the parent console's memory and
  input-latency targets, and record query deadlines, page bounds, hardware,
  and terminal size.
