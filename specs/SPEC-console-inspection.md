# Specification: read-only console inspection

Status: planned, September 17, 2026. This specification defines the shared
data interface for the planned console views. It does not describe an
implemented endpoint.

The [console UI](SPEC-console-ui.md) owns navigation and display rules.
[Item details](SPEC-console-item-details.md),
[worker details](SPEC-console-worker-details.md), and the
[queue view](SPEC-console-queue.md) define their required records.

## Authority and transport

The controller must provide a read-only inspection service. Observer mode
must use this service without `--control` or a control capability token.
The service must not accept acquisition commands or invoke source requests.
The monitor must not open SQLite, queue inputs, staging files, or final files.

The controller must create a Unix-domain socket at
`download-state/telemetry/<run-id>/inspection.sock`. The directory must have
mode `0700`; the socket must have mode `0600`. The controller must create the
socket only after it starts a controller session. It must publish the socket
path in a local, owner-readable inspection-session descriptor. The descriptor
must contain only the protocol version, run ID, session ID, and socket path.
It must have mode `0600` and must not contain a capability token or secret.

On Linux, the service must verify the peer user ID with `SO_PEERCRED`. It must
reject a different user, run, session, or protocol version. It must not expose
a TCP listener. The peer check protects the local-user boundary. **Reveal
source** is a display choice and must not serve as a security boundary between
processes owned by the same user.

The inspection service must remain separate from the
[command channel](SPEC-controller-control-ui.md). Inspection must not require
confirmation, write acquisition state, or create control audit records.
The controller must remove the socket and descriptor after publishing its
final snapshot. An absent endpoint must leave snapshot monitoring available.

The controller may query SQLite for requested durable records. Queries must
use a controller-owned read-only connection and a short read transaction.
The connection must not use SQLite immutable mode on a live database.
Queries must run outside the scheduler, render loop, and snapshot publisher.
The [telemetry projection](SPEC-telemetry-state-projection.md) must remain
bounded and must not cache the complete selected queue.

## Protocol and records

Each connection must carry one request and one response. Each message must
contain one UTF-8 JSON object followed by a newline. Protocol version 1 must
support `get_item`, `get_worker`, `list_queue`, and `list_attempts`.
The service must use `SOCK_STREAM`, set a one-second receive and send timeout,
and close the connection after its response. It must reject an incomplete line,
more than one message, invalid UTF-8, and non-object JSON. A client must not
send bytes after the request newline. The service must close the connection
when it receives such bytes. The service must enforce the request byte bound
before JSON parsing.

Requests must contain `protocol_version`, `request_id`, `run_id`, `session_id`,
`operation`, and `parameters`. Parameters must accept identifiers and filters,
never database paths, SQL, or operator-supplied filesystem paths.
`request_id` must be a nonempty UUID string. `operation` must be one supported
string. `parameters` must be an object. The service must reject missing fields,
wrong field types, duplicate JSON keys, and unknown parameter names.
`item_id` must be a 64-character lowercase hexadecimal SHA-256 value.
`worker_id` must be a positive integer. `reveal_source` must be a Boolean.
Page sizes, cursors, buckets, and queries must meet the restrictions in the
[queue specification](SPEC-console-queue.md). A cursor must be an opaque ASCII
string of at most 1,024 bytes.

Successful responses must echo `request_id` and contain `status`,
`read_at`, `state_revision`, and `data`. `status` must be `ok` on success.
Errors must use `not_found`, `unavailable`, `busy`, `invalid_request`,
`incompatible`, `session_changed`, or `cursor_expired`, with a bounded reason.
For a request with a valid `request_id`, an error must echo that value. For a
malformed or missing request ID, an error must set `request_id` to `null`.
An error response must contain only `request_id`, `status`, and `reason`.
An error must not be represented as an empty successful result. `read_at` must
be an RFC 3339 UTC timestamp. `state_revision` must be a nonnegative integer.

`state_revision` must identify the committed durable read. Runtime fields
must carry their own `sample_sequence`, `sample_age_s`, and quality.
The service must join runtime data only when item, generation, and attempt
identities match. Missing identity must produce unavailable runtime fields.
The UI must label a detail revision that differs from the dashboard revision.
It must not merge detail values into dashboard counts.

Records must use canonical `item_id`, `worker_id`, `generation`, and
`attempt_id` meanings from the [telemetry contract](SPEC-download-telemetry.md).
An item key is `(run_id, item_id)`. A worker key is
`(run_id, session_id, worker_id)`. Names and PIDs must not serve as keys.
Unknown values must be `null`, with an unavailable reason where needed.
Sizes, rates, timestamps, and quality must use telemetry units and rules.
Published identifiers must be opaque values. They must not embed a source URL,
private path, engine job value, or PID. The controller must convert a legacy
runtime identifier that embeds a source URL into its stable opaque item ID
before it creates an inspection record.

`get_item` must accept `item_id` and `reveal_source`, which defaults to false.
`get_worker` must accept `worker_id`. `list_attempts` must accept `item_id`,
an optional cursor, and a page size. `list_queue` parameters are defined in
the queue specification. Unknown operations and parameters must be rejected.

## Bounds and refresh

Requests must be at most 16 KiB. Responses must be at most 256 KiB.
List responses must contain at most 200 records and an opaque continuation
cursor. The service must reduce the page length to meet the byte bound.
It must preserve full identifiers. Oversized display text must have an
explicit truncation flag. An oversized single record must return unavailable.

Each query must stop within one second of request acceptance, measured with a
monotonic clock. This duration includes a database lock wait of at most 100 ms.
The service must permit at most four concurrent queries per controller and
return `busy` for excess work. A search timeout must return `unavailable`; it
must not return incomplete results as a complete page.

The monitor must keep at most one inspection request in flight. It must send
at most two inspection requests per second across all views. It must coalesce
refreshes and discard responses for an old view, filter, item, or session.
Only visible records may refresh automatically. Failed requests must not
cause a tight retry loop. The render callback must perform no inspection I/O.

Cursors must bind the run, session, operation, filters, order, and read
revision. A changed durable revision must invalidate a cursor. The UI must
show **Results changed** and offer a restart from the first page. The service
must not retain a database transaction between page requests.

## Privacy and unavailable data

All strings must use literal-safe rendering. Escape sequences, markup, and
terminal hyperlinks must not execute. Inspection must not read or return Tor
cookies, control tokens, RPC secrets, or environment variables.

Source URLs must be absent unless `get_item` explicitly requests a reveal.
The service must remove the complete user-info and query components from a
revealed URL. It must label the resulting value **Source redacted**. The
service must not reveal a URL when its parsed scheme or host is invalid.
The monitor must not open a browser, invoke a shell, or copy data to a
clipboard automatically. Closing details must clear the source reveal.

Version-1 and version-2 snapshots must remain usable without this service.
Missing records must show **Details unavailable**, with the reason.
After controller exit, cached records must show their recorded time and
revision. Uncached pages must remain unavailable. The monitor must not start
a controller or open SQLite to obtain them. Offline inspection beyond cached
records is deferred. The monitor may cache only successful responses for views
that the user opened. It must clear cached revealed URLs when the view closes,
the session changes, or the monitor exits.

## Acceptance criteria

Tests must use a fake service, temporary state, and synthetic records.
They must not start Tor, aria2, or a source request.
The demo must provide an in-memory inspection adapter with deterministic
queue, detail, and attempt records. Demo navigation must require no socket,
database, or case data. Snapshot-only fixtures must retain unavailable-field
behavior when inspection records are absent.

- Verify protocol errors, peer rejection, byte bounds, query deadlines,
  connection timeouts, concurrency bounds, cursor expiry, and session
  replacement.
- Verify that obsolete responses cannot replace the current selection.
- Verify that inspection leaves acquisition state and evidence unchanged.
- Verify that the monitor never opens SQLite or reads control credentials.
- Verify that the snapshot publisher performs zero SQLite calls.
- Verify missing endpoints, old snapshots, final snapshots, and cache labels.
- Verify that source URLs remain absent when `reveal_source` is false.
- Verify that all secrets remain absent from every response.
- Verify that a revealed URL removes user-info and query components.
- Verify literal rendering and explicit truncation of oversized text.
