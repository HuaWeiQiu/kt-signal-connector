# Phase 2 Local Validation

## Status

- Validation date: 2026-08-05
- Host: macOS arm64
- Result: local Phase 2 fixture acceptance passed
- Real-account acceptance: not claimed; not authorized in this loop

This report records evidence for account linking, opaque account mapping, SQLite
persistence, text receive/send, and client-request idempotency against the fake
`signal-cli` fixture. It does not claim production packaging, Windows transport,
or live Signal service behavior.

## Implemented Boundary

- `link.start` / `link.finish` / `link.cancel` with one active session, expiry, and
  zeroization of the device-link URI.
- Host receives `linkSessionId` + `qrPayload`; raw URI is not accepted from the host
  on finish.
- `accounts.list` maps signal-cli account numbers to opaque account IDs and masked
  addresses only.
- Incoming `dataMessage` receive events are persisted before host delivery.
- `conversations.list` / `messages.list` read only connector SQLite state.
- `messages.sendText` requires opaque account/conversation IDs, enforces
  `clientRequestId` idempotency, and never auto-retries unknown mutating outcomes.
- Host events: `account.changed`, `conversation.changed`, `message.upserted`,
  `message.statusChanged`.
- Connector state directory is owner-only and separate from the signal-cli data dir.

## Automated Evidence

Commands passed:

```bash
cargo fmt --check
cargo test --all-targets
cargo clippy --all-targets --all-features -- -D warnings
cargo build --release
jq empty schemas/connector-api-v1.schema.json
```

Notable integration coverage:

- authenticated runtime lifecycle (Phase 1 regression)
- Phase 2 host flow: link start conflict, finish, account list, receive ingest,
  conversation/message list, text send, and idempotent resend

## Not Yet Verified

- Real Signal account linking or message delivery
- Windows named-pipe transport
- Packaging, signatures, manifests, LKG rollback
- Media attachments
- KT Desktop ConnectorSupervisor integration (Phase 4)

No push, release, deployment, or external Signal action was performed.
