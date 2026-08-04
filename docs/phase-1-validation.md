# Phase 1 Local Validation

## Status

- Validation date: 2026-08-04
- Host: macOS 26.5.1 arm64
- Rust: `rustc 1.94.0`, `cargo 1.94.0`
- Fixture runtime: Python 3.9.6
- Result: local Phase 1 fixture acceptance passed
- Production readiness: not claimed

This report records evidence for the process and protocol PoC only. It does not replace the
real-account, target-platform, packaging, resource, or long-duration gates in the implementation
plan.

## Implemented Boundary

- Private Unix socket in a current-user-owned `0700` directory; socket mode is `0600`.
- Exact 256-bit bootstrap secret file, current-user ownership checks, immediate deletion, and
  zeroization of decoded secret buffers.
- Five-second HMAC-SHA256 challenge-response handshake and API `1.0` negotiation.
- One authenticated host connection, 1 MiB host frame limit, bounded recent request IDs, and a
  default-deny method dispatcher.
- One supervised `signal-cli ... jsonRpc` child with stdin/stdout protocol isolation and discarded
  stderr content.
- 8 MiB upstream line limit, 128-command queue, 128 pending upstream requests, and 1,024-event
  broadcast queue.
- Graceful child shutdown by closing stdin, followed by a three-second forced-stop deadline.
- Host connections act as runtime leases: disconnect, protocol failure, or connector shutdown stops
  the supervised child before another host session is accepted.
- Normalized receive metadata that does not forward message bodies, phone numbers, account values,
  or raw envelopes.
- Conservative send semantics: a timeout, malformed response, write failure, or child exit after
  dispatch yields an unknown mutating outcome and is never retried by the connector.

## Automated Evidence

The local suite contains 22 passing tests:

| Area | Evidence |
| --- | --- |
| CLI | version output and fail-closed missing command |
| authentication | valid/tampered proof, exact secret length, deletion, and malformed secret |
| IPC | private socket mode, cleanup identity, and insecure-directory rejection |
| host protocol | successful handshake, generic auth failure, status, and request-ID replay rejection |
| engine success | JSON-RPC response matching and receive normalization |
| engine failure | malformed/oversized lines, timeout, duplicate response ID, child crash, and pending-request backpressure |
| send safety | timeout and process crash both produce unknown mutating outcome |
| lifecycle | restart after crash, graceful shutdown, disconnect cleanup, reconnect, and full binary flow |

Commands passed:

```bash
cargo fmt --check
cargo test --all-targets
cargo clippy --all-targets --all-features -- -D warnings
cargo build --release
jq empty schemas/connector-api-v1.schema.json
```

The stripped local arm64 release executable is approximately 1.1 MiB. This is artifact size, not
runtime RSS and not a cross-platform measurement.

## Review Findings Fixed

- Mutating requests initially returned a generic exit on child failure; they now return an unknown
  outcome after any possibly dispatched failure.
- Timed-out request cancellation could initially be dropped under command-queue pressure; cleanup is
  now queued reliably without delaying the caller.
- Child stdin was initially retained during graceful stop, causing every stop to reach the forced
  deadline; the handle is now released and the end-to-end fixture exits normally.
- Idle server shutdown initially waited only inside an active connection; `Ctrl-C` now interrupts
  both accept and connection states.
- Socket cleanup now checks the original device/inode before removing its endpoint.
- Secret-file and signal-data directories now require current-user ownership on Unix.

## Not Yet Verified

- A real unmodified `signal-cli v0.14.7` artifact on JRE 25.
- Account linking, a real Signal account, upstream network behavior, or message delivery.
- Windows named-pipe implementation and ACL verification.
- macOS x64 and Windows x64 builds; only macOS arm64 is installed locally.
- Linux behavior, packaging signatures, manifests, rollback, and update delivery.
- SQLite persistence, account mapping, conversations, text send API, idempotency, and history.
- Attachments, stickers, stories, calls, group administration, or bulk automation.
- Actual JRE/connector/KT RSS, CPU, disk growth, multi-account cost, 24-hour stability, and crash-loop
  policy.
- Dependency vulnerability and license-policy tooling; `cargo-audit` and `cargo-deny` are not
  installed in this workspace.

No remote repository, push, release, deployment, or external Signal action was performed.
