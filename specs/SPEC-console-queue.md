# Specification: console queue view

Status: planned, September 17, 2026. This view shows the immutable selected
set, remaining work, and retry deadlines without changing acquisition order.

The view supplements the [console UI](SPEC-console-ui.md) and uses the
[read-only inspection interface](SPEC-console-inspection.md).

## Scope and layout

The **Queue** tab must be available in observer and control modes.
It must contain only selected run items, including completed items.
Skipped-existing files outside selection must appear only as a separate
summary count. Filters must never expand the selected set.

Each row must show immutable queue rank, basename and short item ID, display
bucket, phase when available, received and total bytes, and retry deadline.
Default order must be ascending manifest queue rank, then stable item ID.
Ranks must retain controller values; filtering must not renumber items.
Duplicate basenames must remain distinguishable.

The header must show run ID, selected count, loaded row count, active filters,
read time, and revision. Show an exact matching count only when the service
supplies one. Otherwise show **Matching count unavailable**. Page length must
not serve as an estimate of the full result count.

The summary must use published run totals for retained bytes, known remaining
bytes, and unknown-size items. Label these values **Whole selected run**.
Filtered rows must not change those totals. Every non-complete display bucket
must remain visible as unfinished or unresolved work, including review,
unavailable, exhausted, existing-unverified, and unknown states.

At 80 columns, retain rank, item identity, state, and retry information.
Move secondary fields into item details. Below 80 by 24, provide a scrollable
queue with its filter and freshness labels visible. Refresh and resize must
preserve selection by item ID.

## Filters and literal search

The state filter must support **All** and each telemetry display bucket:
`queued`, `busy`, `retry`, `exhausted`, `complete`, `existing_unverified`,
`review_required`, `unavailable`, and `unknown`.
The service must use the same state-to-bucket mapping as snapshot counts.

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
`cursor`. Defaults must be **All**, empty query, 100 rows, and the first page.
The maximum page size must be 200. Responses must include `rows`,
`next_cursor`, nullable `matching_count`, and the shared revision envelope.
Rows must contain the fields required above and canonical item identities.

The service must use keyset pagination: continue after the last queue rank
and item ID. It must not use increasing SQL offsets or load the whole queue.
Queries must use an index for run selection and rank traversal. Literal
substring searches must obey the inspection deadline even when many rows
require examination. A timeout must show a narrower-search suggestion.

**PageDown** must request the next page. **PageUp** must request the previous
visited page. Disable unavailable directions. The monitor must retain at most
three pages and 100 cursor entries. If earlier navigation expires, offer
**First page**. Filter changes must clear cursors and start at the first page.

Refresh must not silently remove a focused row or jump to another page.
When the durable revision changes, retain the displayed page as recorded
data, show **Results changed**, and offer **Refresh results**. Refresh must
restart pagination. Restore the selected item if present on the new page;
otherwise select the first row and announce the selection change.
The UI must not combine pages from different revisions or sessions.

An empty selected set must show **No selected items**. A successful empty
filtered result must show **No matching items**. Missing service, rejected
requests, and timeouts must show unavailable status instead of either message.

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
Freeze countdowns when stale. Exhausted or review-required items must not
appear eligible unless the controller explicitly reports eligibility.
Global or origin cooldown can delay an eligible item; show that distinction.

Selection and filtering must never submit a retry command. Existing `r` and
`t` controls must retain their controller-confirmed scope. A filtered queue
must not imply that a run-scoped action applies only to visible rows.
Row-scoped retry, reordering, removal, export, and queue editing are deferred.

## Acceptance criteria

Tests must use temporary manifests and synthetic pages, without source access.

- Verify manifest order, stable ranks, duplicate names, and every bucket.
- Verify literal search, combined filters, search cancellation, and clearing.
- Verify page boundaries with 0, 1, 100, 200, and 201 selected items.
- Verify no duplicate or missing rows across pages at one stable revision.
- Verify cursor expiry, revision changes, session changes, and late responses.
- Verify empty results differ from service and query failures.
- Verify details return, selection retention, keyboard focus, and resizing.
- Verify retry countdowns do not imply admission or trigger commands.
- Verify filters do not change selection, totals, or command scope.
- Verify one million synthetic items without full-list loading or widgets.
  Measure the parent console's memory and input-latency targets, and record
  query deadlines, page bounds, hardware, and terminal size.
