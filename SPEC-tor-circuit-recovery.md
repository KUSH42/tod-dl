# Specification: Tor circuit recovery control

Status: implemented behavior, September 16, 2026. This specification defines
controlled Tor circuit renewal for acquisition retries. It supplements the
[controller command specification](SPEC-controller-control-ui.md) and the
[reliable acquisition specification](SPEC-reliable-acquisition.md).

## Purpose and boundary

The controller can request `SIGNAL NEWNYM` after a connectivity failure. The
request asks Tor to use fresh circuits for future streams. It does not prove a
new route, identify a circuit, or change an active stream.

Circuit renewal must not modify queue inputs, immutable selection, final files,
staging files, retry counts, retry deadlines, or attempt data. It must not
cancel a healthy transfer. A circuit-renewal request is not a substitute for a
retry request.

## Authority and access

The downloader controller is the only process that can send `SIGNAL NEWNYM`.
The default monitor remains read-only. An operator can request renewal only
through the existing authenticated local command channel with `--control` and
the action-specific confirmation flow.

The command channel must advertise `renew_tor_circuits` only when all these
conditions are true:

- Tor control authentication passed during controller startup.
- The configured renewal interval is greater than zero.
- The controller is running and owns the active session.

The controller must reject requests that fail these conditions. The controller
must reject a request during the renewal interval. The response must state the
remaining wait in whole seconds.

## Command contract

`renew_tor_circuits` must use the same peer UID check, session capability,
request ID, confirmation nonce, expiry, and replay protection as `retry_now`.
The controller persists a `pending` renewal record before it sends `SIGNAL
NEWNYM`. It records the terminal result after Tor responds or the command
fails. The persisted successful time enforces the shared rate limit after a
controller restart.

The controller must record these values in the durable control audit record:

- Request ID, run ID, session ID, action, and UTC request time.
- Renewal interval and the previous successful renewal time.
- Outcome: `completed`, `rejected`, or `failed`.
- A bounded failure reason when Tor rejects the request or the control socket
  fails.
- The next eligible renewal time after a successful request.

A successful Tor control response means that Tor accepted the signal. The UI
must display `Tor accepted a request for new future streams`. The UI must not
display `new circuit established` or equivalent language.

## Automatic recovery

The controller may request one renewal after a categorized connectivity
failure. The automatic path must use the same rate limit and audit event as the
operator path. The controller must not request renewal for disk, database,
permission, checksum, path-safety, or finalization failures.

After a successful automatic request, the controller must keep the configured
source cooldown. It must retry only eligible selected items. It must retain
the original attempt record and create a new attempt record for every retry.

## Tor isolation and safety

The acquisition deployment must use a Tor `SocksPort` with
`IsolateSOCKSAuth`. The controller must verify that setting before admission.
The project recommends a dedicated Tor instance for acquisition. A shared Tor
control port can affect future streams from other local programs.

The controller must not record the Tor control cookie, SOCKS credentials, or
circuit identifiers in SQLite, telemetry, provenance records, or logs. It may
record the configured control endpoint only when project policy permits it.

## Acceptance criteria

Implementation is complete when tests show that the controller:

- Rejects an unauthenticated, expired, replayed, or rate-limited request.
- Records accepted, rejected, and failed requests without secrets.
- Does not change an item status or retry deadline during renewal.
- Uses one shared rate limit for automatic and operator requests.
- Shows the limited success statement after a Tor `250` response.
