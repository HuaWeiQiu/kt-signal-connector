# KT Signal Connector Implementation Plan

## 1. Status

- Decision date: 2026-08-04
- Current status: Connector Phases 1–3 are implemented locally; the separate KT Desktop Phase 4
  integration is locally merged at `5e18793c`, while production Phase 3 exit gates remain open
- Connector source baseline: `main` @ `6656f70`
- Target engine baseline: unmodified `signal-cli v0.14.7`
- Target Java baseline: JRE 25
- Initial platforms: Windows 10/11 x64, macOS x64, macOS arm64
- Remote publication: not authorized yet; all work remains local until testing and review are complete

This document is the standalone implementation plan for `kt-signal-connector`. KT Desktop owns the
product UI and business features; this repository owns the isolated local transport, process,
storage, and normalized message boundary.

## 2. Delivery Decision

The production process model is:

```text
closed-source KT Desktop
        |
        | authenticated, versioned local IPC
        v
open-source kt-signal-connector (Rust)
        |
        | stdin/stdout JSON-RPC 2.0
        v
unmodified signal-cli + JRE 25
        |
        v
Signal service
```

Per local KT profile the fixed process count is:

```text
1 x kt-signal-connector
G x signal-cli engines (JVM or native), one per proxy group (G >= 1, hard ceiling 8)
N x Signal accounts
M x conversations
```

Accounts and conversations do not create processes, and accounts do not create engines either: an
account joins one launcher-defined proxy group at link time and shares that group's engine (ADR
0001, see 4.4). The default single-group deployment (G = 1) is exactly the original model. One
ordinary text conversation is expected to add only its bounded UI/message window in KT; it does not
consume a separate engine or 1 GB of memory.

## 3. Ownership Boundaries

### 3.1 KT Desktop

KT Desktop owns:

- Signal account, conversation, message, and composer UI.
- AI reply, target-language policy, translation, remarks, and quick replies.
- Renderer-to-main-process allowlisted IPC.
- Connector artifact download, signature verification, activation, and last-known-good rollback.
- Tenant capability and product rollout switches.

KT Desktop must not own:

- signal-cli process protocol parsing.
- Signal account keys or protocol databases.
- raw signal-cli envelopes.
- connector source or AGPL dependencies linked into its executable.

### 3.2 Connector

The connector owns:

- authenticated local IPC and API version negotiation.
- signal-cli child process lifecycle and JSON-RPC request matching.
- link flow state and QR secret lifetime.
- raw-envelope validation and normalization.
- stable message IDs, deduplication, SQLite indexes, cursors, and bounded caches.
- attachment metadata and any approved file-handle boundary.
- runtime health, RSS/CPU monitoring, redacted diagnostics, and engine restart policy.

The connector must not:

- render UI or accept arbitrary renderer connections.
- execute unrestricted KT feature code.
- expose arbitrary signal-cli JSON-RPC methods.
- expose raw envelopes, data-directory paths, account keys, or arbitrary file paths.
- link or copy signal-cli/libsignal source.

### 3.3 signal-cli

signal-cli exclusively owns:

- Signal protocol and cryptography.
- linked-device account state and keys.
- upstream websocket communication.
- Signal-specific send and receive behavior.

The first release uses an unmodified, pinned upstream artifact. A required upstream patch is a new
architecture and license review, not a routine implementation detail.

## 4. API Boundary

### 4.1 Transport

- Windows: a random per-start named pipe restricted to the current user SID.
- macOS/Linux: a socket in a profile-private directory with mode `0600`. The listener binds a
  staging sibling, hardens it, then publishes it with a rename, because the host connects as
  soon as the endpoint path appears and must never reach a socket that is still world-connectable.
- No public listener and no fixed localhost HTTP/TCP port.
- The renderer never receives the endpoint or the session secret.
- A host that closes its connection abruptly ends the session exactly like a clean EOF: the
  process still exits zero. Only an unparsable or oversized frame is a protocol failure.

Electron Main creates a 256-bit random bootstrap secret. On macOS/Linux it writes an owner-only
temporary file and the connector reads and deletes that file before accepting a client. On Windows
Main writes the exact secret to the child process's inherited anonymous stdin pipe and closes it;
the Windows connector refuses file bootstrap because POSIX mode bits do not prove a private DACL.
The first frame performs a challenge-response handshake and negotiates API capabilities. The secret
and endpoint are never logged, and their mutable buffers are zeroized after bootstrap/handshake.

The bootstrap payload is two lines (Phase 3 key contract, 2026-08-26): line 1 is the bootstrap
secret, exactly 64 lowercase hexadecimal characters, unchanged; line 2 is the 32-byte store key,
also exactly 64 lowercase hexadecimal characters, generated once by the desktop and persisted via
Electron `safeStorage`. Read-and-delete (file) and zeroization (memory) cover both lines. The store
key never crosses the socket, never enters the wire protocol or the schema, and is never logged;
the connector holds it in `Zeroizing` memory only. `KT_SIGNAL_STORE_KEY` is a dev/test override and
never set in production. A single-line legacy payload is still parsed, but without a store key the
connector fails closed: an existing plaintext store is never opened without a key, and with no
store at all startup is refused until the desktop generates and delivers the key (first run).

The server sends a random
32-byte hexadecimal `serverNonce`; KT answers with a random 32-byte hexadecimal `clientNonce` and:

```text
proof = hex(HMAC-SHA256(secret,
  "kt-signal-connector-v1\0" + serverNonce + "\0" + clientNonce + "\0" + apiVersion))
```

Nonces and proofs must be canonical lowercase hex. Authentication has a five-second deadline and a
connection is closed after any malformed or failed handshake. Phase 1 supports exactly API `1.0`.

### 4.2 Framing

The initial host protocol uses newline-delimited JSON with explicit maximum frame size. Each request
contains:

```json
{
  "apiVersion": "1.0",
  "requestId": "opaque-id",
  "method": "runtime.status",
  "params": {}
}
```

A response contains exactly one of `result` or `error`. Events have no request ID and use a separate
`event` field. Unknown fields are ignored only when the negotiated minor version permits them;
unknown methods are rejected.

### 4.3 Initial allowlist

Phase 2 host methods:

- `handshake`
- `runtime.status`
- `runtime.start`
- `runtime.stop`
- `accounts.list`
- `accounts.deleteLocalData`
- `link.start`
- `link.finish`
- `link.cancel`
- `conversations.list`
- `messages.list`
- `messages.getText`
- `messages.sendText`
- `contacts.sync`
- `contacts.list`
- normalized runtime/account/conversation/message events

No generic `call`, `exec`, `jsonRpc`, file-read, URL-open, or raw-envelope endpoint is allowed.
The machine-readable Phase 2 envelope contract is
[`schemas/connector-api-v1.schema.json`](../schemas/connector-api-v1.schema.json). Methods outside this
allowlist return `METHOD_NOT_ALLOWED`. Media and bulk automation remain unavailable.

### 4.4 Proxy groups (Phase 4 contract, 2026-08-26; ADR 0001)

Proxy groups evolve API `1.0` in place; the apiVersion handshake binding is unchanged. Every change
is additive for a caller that never names a group: such a caller gets exactly the pre-Phase-4
behavior, with all of its accounts in the reserved `default` group.

- Groups are launcher input, never IPC input: repeatable `serve --proxy-group <id>=<host:port>` or
  the `KT_SIGNAL_PROXY_GROUPS` comma-separated equivalent, governed by the signed runtime manifest
  like every other process input (see 5). Group ids match `^[a-z0-9]([a-z0-9-]{0,30}[a-z0-9])?$`;
  `default` is reserved and always exists. The legacy global `--socks-proxy` /
  `KT_SIGNAL_SOCKS_PROXY` configures the `default` group's proxy and must not be redefined in the
  group list. When the flag and the environment variable are given together they merge into one
  launcher-ordered list (flag entries first, then environment entries); a group id that appears in
  both sources — or twice anywhere — aborts startup: ambiguity is refused fail-closed and never
  resolved by precedence, and neither source may name `default` itself. More than 8 groups, a
  malformed id, or a malformed proxy value aborts startup with a clear error.
- `link.start` params gain an optional `proxyGroup` (opaque id). Absent means `default`. An unknown
  id fails with `PROXY_GROUP_NOT_FOUND` (`retryable=false`). The result gains a `proxyGroup` field
  echoing the selected group. The binding is fixed when `link.finish` succeeds and is immutable for
  the life of the account; moving an account means `accounts.deleteLocalData` plus a fresh link.
- Link flows are per group: `LINK_IN_PROGRESS` applies within one group only, the `link.finish`
  wait lane is per group, and a `link.cancel` that must restart an engine restarts only that
  group's engine (see 6.1).
- `accounts.list` result items, the `link.finish` result, and `account.changed` event data gain a
  `proxyGroup` field (always present; accounts linked before Phase 4 read as `default`).
- `runtime.status`, `runtime.start`, and `runtime.stop` results gain `proxyGroups`: an array in
  launcher config order with one entry per group,
  `{groupId, state, pid?, rssBytes?, resourcePressure, accountCount}`, where `state` is
  `running|stopped|exited|faulted` and `accountCount` counts the accounts bound to the group. The
  top-level aggregate is preserved for existing callers with a pinned rule: `state` is `running`
  if any group is running, else `faulted` if any is faulted, else `exited` if any is exited, else
  `stopped`; `rssBytes` is the sum of live group samples; `resourcePressure` is true when any
  group is in pressure; the top-level `pid` is present only when exactly one group exists. For a
  single-group deployment this aggregate is field-for-field identical to the pre-Phase-4 result.
  `runtime.start` fails with `RUNTIME_START_FAILED` only when no group engine started.
- New event `proxyGroup.stateChanged` with data `{groupId, state, pid?, rssBytes?,
  resourcePressure}` on every group engine transition. `runtime.stateChanged` keeps carrying the
  top-level aggregate with its shape unchanged. `runtime.resourcePressure` event data gains a
  required `groupId` (`default` in single-group deployments). Unknown event names and unknown
  fields are ignored by existing hosts; this follows the precedent of `message.statusChanged`
  gaining a producer while older hosts only routed it.
- Account-addressed methods (`messages.*`, `conversations.list`, `contacts.*`,
  `accounts.deleteLocalData`) route by `accountId` to the account's group engine; their request
  and response shapes are unchanged.
- Dormant groups: an account whose stored `proxyGroup` is not part of this launch plan stays
  intact but unreachable. Every account-addressed method naming it fails with
  `CAPABILITY_UNAVAILABLE` (`retryable=false`; the message carries the group id, never an
  endpoint), and the account does not appear in `accounts.list`, which unions configured groups
  only. The connector deliberately never auto-starts a group outside the launch plan: a group's
  proxy exists only as launcher input (above), so inventing one at runtime could route the
  account through the wrong or missing egress and break anti-association (ADR 0001 R1).
  Restoring the group to the launch plan makes its accounts reachable again unchanged; the
  store-level delete-replay closure in 6.2 is the one deliberate exception that survives a
  vanished group.
- Deliberately absent from the wire: group creation/reconfiguration/deletion, group reassignment,
  per-group `runtime.start`/`runtime.stop`, and proxy endpoints. The desktop allocated the local
  proxy ports and already knows the group-to-proxy mapping; the wire carries opaque group ids only,
  and error messages never contain a proxy host:port.
- Version skew: a group-aware host detects support by the presence of `proxyGroups` in the
  `runtime.status` result before ever sending `proxyGroup`; against a pre-Phase-4 connector the
  unknown param is rejected with `INVALID_REQUEST` (`deny_unknown_fields`) and fails closed, never
  silently landing the account in the wrong egress.

### 4.5 messages.remoteDelete (contract revision 1.6, 2026-09-02)

API `1.0` evolves in place again (§4.4 precedent); the apiVersion handshake binding is unchanged.
The method is additive: a host that never calls it sees no change, and a host must detect support
through the handshake `capabilities` array — calling it against an older connector answers
`METHOD_NOT_ALLOWED`. Design and rationale: `docs/remote-delete-l2-plan.md`.

- `messages.remoteDelete` remotely deletes ("Delete for everyone") one message the linked account
  itself sent. Params: `accountId`, `conversationId`, `messageId` (all required, opaque ids) and an
  optional `operationId` that the connector validates by shape but never persists — there is no
  operation ledger and no store migration. Addressing is by `conversationId` only: the target must
  already exist in the local history.
- Eligibility: only a local outgoing row in the terminal state `sent` qualifies; its `sentAt` was
  overwritten with the send response's upstream Signal timestamp and becomes the delete's protocol
  identity. Pending/failed/unknown rows carry only a local clock value, and incoming rows are not
  the account's own messages — all of them answer `MESSAGE_NOT_FOUND`, exactly like quote
  resolution. The conversation's kind selects the upstream addressing: `recipient: [peerKey]` for
  direct chats, `groupId: peerKey` for groups (signal-cli jsonRpc `remoteDelete` params
  `account`/`targetTimestamp`/`recipient`/`groupId`, verified against the pinned 0.14.7
  distribution).
- Result: `{"status": "deleted"}` when signal-cli confirmed the delete, or
  `{"status": "unknown"}` when the outcome of the mutating upstream call is indeterminate. The
  `unknown` semantics equal every `*_OUTCOME_UNKNOWN` error: the connector never retries
  automatically; a host may explicitly re-send with a fresh requestId and the same `operationId`,
  and the real outcome is whatever the peer's rendering shows (best effort, 24h upstream window).
- Error mapping reuses existing codes only (zero new codes): `ACCOUNT_NOT_FOUND`,
  `CONVERSATION_NOT_FOUND`, `MESSAGE_NOT_FOUND` (missing or ineligible row), `RUNTIME_NOT_RUNNING`,
  `INVALID_REQUEST`, `UPSTREAM_EXITED`, and `UPSTREAM_ERROR` for an explicit upstream rejection
  (expired window, already deleted, not deletable) — its `retryable=true` is the global mapping,
  and hosts must not auto-retry deletes on it.
- Known behavior boundaries: the local `messages` row is intentionally left unchanged on a
  successful delete (presentation/bookkeeping belongs to the desktop, which receives
  `status: "deleted"`); deletion events from peers or the account's other devices are not yet
  converged locally; nothing is emitted to `message.statusChanged` by this method.
- Dispatch inherits the mutating infrastructure unchanged: the write lane with the per-account
  mutex (same-account `sendText`/`remoteDelete`/`contacts.sync`/`deleteLocalData` serialize), the
  delete drain barrier, and the per-account request budget. Metrics classify it as `send`.

### 4.6 messages.sendReaction (contract revision 1.7, 2026-09-03)

API `1.0` evolves in place again (§4.5 precedent); the apiVersion handshake binding is unchanged.
The method is additive: a host that never calls it sees no change, and a host must detect support
through the handshake `capabilities` array — calling it against an older connector answers
`METHOD_NOT_ALLOWED`. It follows the `messages.remoteDelete` (§4.5) implementation shape end to
end; design rationale and the upstream-verification notes live in `docs/remote-delete-l2-plan.md`
§3.2.

- `messages.sendReaction` sends or removes a reaction on one message that already exists in the
  local history. Params: `accountId`, `conversationId`, `messageId`, `emoji` (all required; the
  first three opaque ids) plus an optional `remove` (boolean, default `false`) and an optional
  `operationId` that the connector validates by shape but never persists — there is no operation
  ledger and no store migration. Addressing is by `conversationId` only: the target must already
  exist in the local history.
- `emoji` must be a single unicode grapheme cluster of 1–32 UTF-8 bytes — the pinned signal-cli
  requirement (SendReactionCommand `--emoji` help text, verified via `javap` against the pinned
  0.14.7 distribution). Grapheme clustering keeps multi-codepoint emoji (ZWJ sequences,
  skin-tone modifiers, flags) valid while rejecting multi-emoji strings; the service enforces the
  rule deterministically.
- Eligibility and target identity: an own outgoing row qualifies in the terminal state `sent`
  (its `sentAt` was overwritten with the send response's upstream Signal timestamp); an incoming
  row carries the envelope timestamp, so it is addressable too. The upstream `targetAuthor`
  follows the row direction — the linked account's own number for outgoing rows, the conversation
  peer for incoming direct rows (the quote-resolution precedent). Pending/failed/unknown rows
  carry no protocol identity and answer `MESSAGE_NOT_FOUND`; a group incoming row persists only a
  local sender hash, never the member's number, so its author is not resolvable and the request
  fails with a deterministic `INVALID_REQUEST` instead of a mis-addressed upstream reaction. The
  conversation's kind selects the addressing: `recipient: [peerKey]` for direct chats,
  `groupId: peerKey` for groups (signal-cli jsonRpc `sendReaction` params
  `account`/`emoji`/`remove`/`targetAuthor`/`targetTimestamp`/`recipient`/`groupId`, verified
  against the pinned 0.14.7 distribution).
- Result: `{"status": "sent"}` when signal-cli confirmed the reaction, or `{"status": "unknown"}`
  when the outcome of the mutating upstream call is indeterminate. The `unknown` semantics equal
  every `*_OUTCOME_UNKNOWN` error: the connector never retries automatically; a host may
  explicitly re-send with a fresh requestId and the same `operationId`. `remove=true` removes a
  previously sent reaction (upstream best effort); the emoji must still identify the reaction to
  remove.
- Error mapping reuses existing codes only (zero new codes): `ACCOUNT_NOT_FOUND`,
  `CONVERSATION_NOT_FOUND`, `MESSAGE_NOT_FOUND` (missing or ineligible row), `RUNTIME_NOT_RUNNING`,
  `INVALID_REQUEST` (shape, emoji, or unresolvable group-incoming author), `UPSTREAM_EXITED`, and
  `UPSTREAM_ERROR` for an explicit upstream rejection (unknown target, unregistered recipient,
  reaction not found) — its `retryable=true` is the global mapping, and hosts must not auto-retry
  reactions on it.
- Known behavior boundaries: the local `messages` row is intentionally left unchanged on a
  successful reaction; incoming reaction envelopes from peers are not yet converged into a
  rendered-reaction state locally; nothing is emitted to `message.statusChanged` by this method.
- Dispatch inherits the mutating infrastructure unchanged: the write lane with the per-account
  mutex (same-account `sendText`/`remoteDelete`/`sendReaction`/`contacts.sync`/`deleteLocalData`
  serialize), the delete drain barrier, and the per-account request budget. Metrics classify it
  as `send`.

## 5. signal-cli Boundary

The connector starts multi-account JSON-RPC mode without `-a`:

```text
signal-cli --data-dir <profile-private-data-dir> jsonRpc
```

Rules:

- executable path, data directory, JRE arguments, and environment come from a signed runtime
  manifest, never from an IPC request.
- stdin writes one JSON-RPC object per line.
- stdout is incrementally parsed with a hard line-size limit.
- response IDs are matched to a bounded pending map.
- `method=receive` is normalized as an event.
- stderr is redacted diagnostic input, never protocol input.
- read-only requests may be retried under policy; mutating requests are never retried automatically
  after an unknown outcome.
- a mutating request whose result is lost must not be reported as a retryable timeout. `finishLink`
  in particular answers `LINK_OUTCOME_UNKNOWN` with `retryable=false`, because the phone may already
  have accepted the device and a retry would claim a second device slot. A definite
  `UPSTREAM_TIMEOUT` stays retryable: the call provably did not take effect.
- child shutdown is graceful first, then forced after a fixed deadline.
- signal-cli does not read OS proxy settings. An optional SOCKS proxy reaches the JVM as
  `-DsocksProxyHost/-DsocksProxyPort` appended to the pinned `JAVA_OPTS`; the launcher passes it via
  the `KT_SIGNAL_SOCKS_PROXY=host:port` environment variable (preferred, so it stays off the process
  command line) or `serve --socks-proxy host:port`. An invalid value aborts startup with a clear
  error. The default is a direct connection, and watchdog restarts reuse the same proxy config.
  Phase 4 (ADR 0001) generalizes this per proxy group: each group's engine receives its own
  group's proxy through exactly this mechanism (`KT_SIGNAL_PROXY_GROUPS` / repeated
  `serve --proxy-group`), the legacy global option configures the `default` group, and each
  group's watchdog restarts reuse that group's config unchanged.
- Phase 5 adds a GraalVM native-image mode, selected explicitly with `serve --signal-cli-native`
  or `KT_SIGNAL_CLI_NATIVE=1` (explicit flag, not file-type probing: packaging controls what it
  ships, and a wrong heuristic guess would silently drop the JVM heap budget). The CLI arguments
  are identical to the JVM launcher shape. Native mode skips `JAVA_HOME` validation/forwarding and
  `JAVA_OPTS` injection; a configured SOCKS proxy is prepended to argv as
  `-DsocksProxyHost/-DsocksProxyPort`, which the GraalVM native-image launcher applies as runtime
  system properties (verified against signal-cli 0.14.7 native: SOCKS5 CONNECT with remote DNS,
  staging provisioning round-trip through a local relay). The trade-off is that the proxy host:port
  appears on the child command line; the JVM mode keeps it in the environment. Every other
  supervision guarantee (absolute-path non-symlink executable check, stdio JSON-RPC, receive-mode,
  kill-on-drop, watchdog, RSS sampling) is identical across modes.

During the local Phase 1 PoC, the trusted launcher supplies absolute executable and data-directory
paths as process arguments; neither is accepted over host IPC. Phase 3 replaces this bootstrap with
signed runtime-manifest verification before packaging acceptance.

## 6. Linking and Messaging

### 6.1 Linking

`link.start` calls signal-cli `startLink`. The raw device-link URI remains only in connector memory.
KT receives a short-lived session ID and the QR payload needed for display. `link.finish` resolves the
session ID internally and calls `finishLink`; KT cannot supply or alter the raw URI.

One proxy group permits one active link flow; different groups may hold independent link flows in
parallel (Phase 4, ADR 0001 — a single-group profile keeps the original global behavior). Within a
group, `link.finish` uses its own single-capacity wait lane so a
five-minute phone-approval wait cannot block runtime control, `link.start`, or `link.cancel`.
Duplicate finish calls are rejected instead of accumulating pending requests.

Expired sessions keep their attribution: a `link.finish` (or `link.cancel`) for a session that has
already expired is still routed to its owning group's wait lane and answered there with the
definite `LINK_EXPIRED` (`retryable=false`, matching every other definite finish failure) instead
of the unknown-session `LINK_NOT_FOUND`. Answering expiry consumes the session, so any later call
for the same id reports `LINK_NOT_FOUND`; only a session the connector no longer knows at all is
`LINK_NOT_FOUND` on the first attempt. If the owning group's runtime is stopped, the ordinary
`RUNTIME_NOT_RUNNING` answer precedes dispatch, as for any finish.

Cancellation is linearized before it returns: a cancelled or superseded finish result cannot create
an account row or emit `account.changed`. The pinned signal-cli JSON-RPC API has no operation that
cancels an already-dispatched `finishLink`; when `link.cancel` finds one in flight, the connector
restarts that link session's group engine after clearing the link session. This does not delete or
unlink existing accounts, but it can briefly pause every Signal account bound to that group (in a
single-group profile, every account in the profile).
KT must therefore reuse an unexpired QR and perform this restart only for an explicit replacement,
not an automatic render retry.

Cancellation, timeout, client disconnect, or connector shutdown zeroizes and removes the state. QR
data is never stored or logged.

### 6.2 Local account exit

`accounts.deleteLocalData` accepts an `accountId`; current KT versions also send a stable generated
`operationId`. The field remains optional in API 1.0 so a Connector upgrade does not break an older
Desktop, but new Desktop code must persist and reuse it. The operation removes only that account's
signal-cli data and Connector rows; it must not clear or replace the profile's active link session.
Connector row deletion is one SQLite transaction and is idempotent when the account row is already
absent.

For requests carrying `operationId`, the Connector persists only the random operation ID, opaque
account ID, state, and timestamps. It does not retain the Signal address after account deletion.
At most 256 completed operations are retained; one unfinished operation is allowed per account. On a
later explicit user retry, an unfinished operation first performs two read-only `listAccounts`
checks. If both confirm that the account is absent, the Connector atomically completes its local
cleanup without issuing a second delete; otherwise that user action may dispatch the delete again.

Delete completion is a store-level fact, and a replay must not depend on the owning proxy group
being reachable (Phase 4, ADR 0001). Routing consults the account row only to find the owning
engine; when the row is already gone — an idempotent replay, or a v1-compatible delete of an
absent account — the shared operation ledger completes the operation entirely inside the store,
served through any supervisor (all groups share one store) without contacting any engine, even
when the account's group is not part of this launch plan. Conversely, a first attempt interrupted
between upstream deletion and local commit leaves the row in place: the replay routes normally to
the owning group, where the reconcile-first check above completes locally once the upstream no
longer lists the account; re-dispatching the upstream delete there is safe because it is
idempotent. Replay therefore never requires the owning group to be alive and never resurrects
local rows on its own.

The Connector does not retry `deleteLocalAccountData` automatically. If signal-cli returns a
definitive failure, local rows remain unchanged. If the process, transport, or timeout makes the
result unknowable, the response is `ACCOUNT_DELETE_OUTCOME_UNKNOWN` with `retryable=false` and local
rows also remain. KT keeps the binding and the same `operationId`; only a new explicit user action may
repeat the idempotent exit operation. This avoids both silent local data loss and background repeats
of a destructive operation.

### 6.3 Receive

```text
signal-cli receive notification
  -> bounded JSON parser
  -> schema validation + bounded projection
  -> critical receive queue (bounded by count and bytes)
  -> normalize + stable message ID
  -> SQLite transaction + dedupe
  -> best-effort UI event queue
  -> KT event
```

The critical receive queue is independent from the lossy runtime/UI broadcast queue. Persistence of
the message, conversation summary, and account unread summary is one transaction and always happens
before event delivery, so a lagging or disconnected KT event consumer reloads facts from the store.
If SQLite is unavailable, the Connector retains the current receive and applies bounded
backpressure instead of acknowledging it internally or silently dropping it. It emits only an
opaque `runtime.storageChanged` state (`unavailable` or `recovered`); Desktop pauses new operations
while unavailable. Recovery retries persistence with bounded backoff and never replays a send,
restarts signal-cli, deletes history, or exposes a path or message body.

Incoming text is bounded twice after signal-cli parsing: Connector persistence accepts at most
128 KiB, matching Signal iOS's legacy-compatible receive ceiling, while list/event projections carry
at most a 4 KiB UTF-8 preview. Larger upstream bodies retain a stable message row, bounded preview,
original byte count, and an explicit non-retrievable marker; the excess body is discarded before
SQLite and Host event serialization. A user may fetch one complete persisted body through
`messages.getText`; bulk/background fetch is not exposed.

### 6.4 Send

Every send has a KT `clientRequestId`. The connector inserts a pending record before calling
signal-cli. A repeated request returns the same local record. Timeout or child death after dispatch
produces `unknown`; it never automatically sends again.

`clientRequestId` is persisted and returned as an optional field on that outgoing message so KT can
reconcile an optimistic row and an unknown outcome exactly. It is not forwarded to Signal and is
never used across accounts. Renderer and Connector must not reconcile by equal text: repeated text
is valid user data. Signal receive dedupe uses the account, conversation, direction, sender, and the
Signal-provided message timestamp; before inserting a new versioned stable ID, the store also checks
that identity tuple so rows written by the legacy text-dependent ID remain idempotent.

Receive normalization preserves the exact Signal text body, including leading/trailing whitespace.
Only an empty string is body-less. A body-less envelope becomes a system row only for an explicit
supported Signal control field; it is never guessed from the absence of attachments. `system`
direction and status round-trip through SQLite unchanged.

Text must be non-empty and at most 65,536 bytes after UTF-8 encoding. This follows the 64 KiB
long-message body cap used by the official Signal clients, while signal-cli handles the native
inline-versus-long-text attachment representation. The JSON schema character limit is only an early
shape guard; the Rust service byte check is authoritative for CJK, emoji, and malformed input.
Oversized text is rejected before creating the pending row or dispatching signal-cli and is never
silently truncated, split, or automatically retried.

Reference implementations (pinned links; no source is copied into this repository):

- [Signal Desktop `longAttachment.std.ts`](https://github.com/signalapp/Signal-Desktop/blob/de8fe1e7084fbab9c4e9c667c2d0ec0f208d1adc/ts/util/longAttachment.std.ts)
- [Signal Android `MessageUtil.kt`](https://github.com/signalapp/Signal-Android/blob/9b2c2ed66d854b7abb8ed1a29e976a516ab2ce67/app/src/main/java/org/thoughtcrime/securesms/util/MessageUtil.kt)
  and [`ByteLimitInputFilter.kt`](https://github.com/signalapp/Signal-Android/blob/9b2c2ed66d854b7abb8ed1a29e976a516ab2ce67/core/util/src/main/java/org/signal/core/util/ByteLimitInputFilter.kt)
- [Signal iOS `OWSMediaUtils.swift`](https://github.com/signalapp/Signal-iOS/blob/58cc49ec14da01e7afa89d6e603ba1ca79bcf9b4/SignalServiceKit/Messages/Attachments/OWSMediaUtils.swift)

AI-generated and human messages use the same send method. Language policy and tenant capability are
enforced by KT before the connector call, while connector ownership and idempotency checks remain
mandatory.

### 6.5 Contacts cache and peer-addressed send

After link, signal-cli has already synchronized contacts and groups from the primary device.
`contacts.sync` reads them via read-only `listContacts` (registered contacts only, no
`allRecipients` walk) and `listGroups` (membership-filtered, `isMember=true`) on the account's
group engine queue and upserts them into a per-account `contacts` cache table keyed by
`(account_id, kind, peer_key)`. A successful sync less than 60 seconds old returns the cached
counts without touching the engine. `link.finish` runs one best-effort sync inline; its failure is
logged and never fails the link flow. `contacts.list` serves the cache only (optional substring
filter, cursor pagination) and never calls upstream, so the Desktop new-chat picker cannot starve
the JVM queue. Contact rows and the sync marker are deleted with the account.

`messages.sendText` accepts either `conversationId` or a peer target (`kind` + `peerKey`, optional
`peerTitle`); exactly one form is required. A peer send resolves the conversation by
`(account_id, kind, peer_key)` and, when absent, creates it together with the first outgoing
message. The conversation therefore becomes an active conversation only once the first message is
actually sent — no empty conversation skeletons are produced. `kind` accepts `contact`/`direct`
(both a direct chat) or `group`; a missing `peerTitle` falls back to the masked peer address,
never the raw number.

### 6.6 History limitation

The connector only promises history it has persisted. `sendSyncRequest` can synchronize contacts and
groups but is not treated as complete phone/Desktop message-history backfill. Product UI must not
promise pre-link history until a separately verified upstream capability exists.

### 6.7 Media limitation

Phase 1 runs signal-cli with attachments, stories, and stickers ignored. Current signal-cli downloads
non-ignored incoming attachments before it emits the receive notification, and `getAttachment`
returns an already-downloaded file as full Base64. That is not an acceptable large-media boundary.

Media remains disabled until a dedicated PoC proves bounded disk-pressure behavior and streams a
canonical, already-downloaded file through a short-lived handle without Base64 or arbitrary path
exposure. Failure to prove the limit leaves the media capability disabled.

## 7. Data and Resource Limits

### 7.1 Data ownership

| Data | Owner | Cleanup |
| --- | --- | --- |
| Signal keys and protocol account DB | signal-cli private data directory | explicit destructive action only |
| normalized messages, conversations, contacts, cursors | connector SQLCipher store | retention pass, see 7.1.1 |
| plaintext migration backup | connector store directory | 7-day retention, see 7.1.2 |
| current 100-200 message window | KT UI | release on navigation/unmount |
| media cache | connector | TTL/LRU after media approval |
| link URI | connector memory | timeout/cancel immediately |

The signal-cli data directory is never a cache and must be explicitly excluded from cleanup code.

### 7.1.1 Message retention

Stored history is bounded so a long-lived install cannot grow without limit. One retention pass runs
per process start, in the background, and deletes messages that are both outside a safety window and
selected by one of two rules: older than 90 days, or beyond the newest 2000 rows of their
conversation. It never repeats on a timer; whatever is left is expired on the next start.

- Age is measured on `messages.stored_at`, the local time this connector first stored the row, never
 on `sent_at`. A peer with a wrong clock must not be able to decide when local history disappears.
 Rows written before schema 6 have no `stored_at` and are dated by `received_at` instead; the upgrade
 deliberately does not rewrite the table, because that would delay the startup handshake.
- Nothing inside a 7-day floor is pruned, and neither is any send whose outcome is still `pending` or
 `unknown`. Receive dedupe and send idempotency both answer from stored rows.
- Conversations, contacts and accounts survive an empty history, so titles, pins and the link itself
 are never lost to retention. Summaries of conversations that lost rows are recomputed in the same
 transaction, and an unread badge is clamped to the incoming rows still stored.
- Each transaction deletes a bounded batch and releases the store lock between batches, so a large
 first pass cannot stall inbound receives.
- Deleted pages do not break a caller mid-scroll: message cursors carry their own `(sent_at, id)`
 sort key, so a page still resolves after the row it pointed at is gone.

### 7.1.2 Store encryption at rest

The connector store is SQLCipher (rusqlite `bundled-sqlcipher`), keyed with the 32-byte store key
from the bootstrap payload; the blast radius of the encryption boundary is `Store::open`. A store
file starting with the plaintext `SQLite format 3` header is migrated on first start: the plaintext
WAL is checkpointed, one consistent plaintext backup (`connector.sqlite3.plaintext-backup`) is
written for rollback, the data is exported online into a fresh encrypted file via
`sqlcipher_export`, and the encrypted file is atomically swapped in. Migration failure fails closed
— the connector never silently serves plaintext — and no plaintext copy lands anywhere but the
backup. The backup is pruned after 7 days through the same once-per-start retention pass as
history (7.1.1). A keyed open that the store rejects fails closed as well.


### 7.2 Memory budget before measurement

| Component | Idle RSS | Active text RSS | Media transient RSS |
| --- | ---: | ---: | ---: |
| signal-cli JVM/native | 140-280 MB | 200-400 MB | 300-600 MB |
| Rust connector | 8-25 MB | 15-40 MB | 20-60 MB |
| KT Signal gateway/UI/cache increment | 20-70 MB | 35-110 MB | 60-150 MB |
| total Signal increment | 170-350 MB | 250-550 MB | 400-800 MB |

These are capacity budgets, not measurements or promises. Phase 3 records actual process RSS on each
target platform and replaces the estimates.

Additional idle account budget: 20-80 MB while sharing the same JVM. Each additional proxy group
(Phase 4, ADR 0001) adds one full engine at 140-280 MB idle RSS; the group count is capped at 8 by
the connector and at a product default of 4 by desktop policy, so the worst-case idle Signal
increment is bounded by configuration rather than by account growth. An ordinary open text
conversation should normally remain below 1-5 MB incremental UI state, with an acceptance hard limit
of 20 MB relative to the Signal idle baseline.

The Connector enforces the initial JVM budget at `-Xms16m -Xmx384m` and removes Java's global
option-injection variables before spawning signal-cli. JVM heap is not total RSS. Sustained idle
Signal increment above 350 MB or monotonic growth triggers profiling rather than a documentation
increase. RSS pressure still degrades admission and never kills a live JVM automatically.

### 7.3 Bounded structures

- host frame: default 1 MiB, configurable only downward in production policy.
- signal-cli line: default 8 MiB for text PoC; media does not use this path.
- host connections: exactly one successfully authenticated KT Main connection per Connector process.
  Failed handshakes do not consume the process; closing the authenticated session shuts down the
  engine and Connector, and reconnection starts a fresh process with a fresh one-use secret.
- pending host requests: 128 global, 32 per account, and 8 MiB aggregate request bytes per
  authenticated connection.
- authenticated host dispatch: control 1, link wait 1, persisted reads 4, sends 2; sends remain
  ordered per account and responses are correlated by `requestId`, not arrival order. The link-wait
  lane admits at most one `link.finish` per proxy group and is independent from link cancellation
  and lifecycle control.
- pending signal-cli requests: 128 per group engine (Phase 4: each proxy group's engine keeps its
  own bounded queue; host-side limits stay global on the single authenticated connection).
- runtime/UI broadcast queue: 1,024 non-critical events with pressure reporting; lag is recoverable
  from SQLite.
- critical receive queue: 256 items and 2 MiB of normalized projected data. It backpressures the
  signal-cli stdout reader at either limit and never routes receives through broadcast delivery.
- current message page: default 100, maximum 200.
- message text projection: 4 KiB per list/event row; persisted inbound body: 128 KiB maximum.
- one signal-cli RSS sampler per group engine, every 30 seconds. Pressure requires three consecutive
  samples at or above 512 MiB; recovery requires two consecutive samples at or below 420 MiB.
  Sampling emits only PID/RSS/state and exits immediately with the engine. RSS pressure never kills
  or restarts the JVM automatically. Pressure events identify their group by `groupId` (see 4.4).
  macOS/Linux sample through a short-lived `ps`; Windows uses the
  native process working-set API and never starts PowerShell for monitoring.
- conversation cursors are opaque keyset cursors over `(last_message_at nullness,
  last_message_at, id)`; message cursors are opaque keyset cursors over `(sent_at, id)` bound to one
  account and conversation, so a page still resolves after retention removed the row it pointed at.
  Unknown, malformed or cross-account/cross-conversation cursors fail closed instead of silently
  returning the first page. A bare message id is still accepted, for cursors a running host obtained
  before the connector was upgraded.
- reconnect attempts: exponential backoff with a circuit breaker.

Desktop may restart a terminal engine only after its bounded request scheduler drains, with no
request replay and a three-attempt circuit breaker. If an active mutation does not drain, recovery
fails closed for explicit user action; resource pressure alone is not a restart trigger. Terminal
events carry the exited PID so a delayed event cannot fault a newer engine, and an explicit Desktop
stop always wins a concurrent recovery completion. A clean internal `stopped` to `running` transition
is treated as a managed restart and must not trigger a second engine restart.

Core persistence must not be dropped under event pressure. Non-critical enrichment is disabled
first. SQLite write failure enters an explicit storage-degraded state; one failed receive remains at
the head of the bounded queue until a transaction succeeds, after which a recovered state is emitted.

## 8. Security and Privacy

- Owner-only endpoint, secret file, account data, SQLite, and diagnostic directories.
- No secret in command-line arguments, environment logs, telemetry, or crash reports.
- Runtime schema and length validation at every IPC and JSON-RPC boundary.
- Canonical-path checks for every approved attachment file handle.
- No message body, contact, phone number, QR payload, token, key path, or raw envelope in logs.
- Metrics use opaque account IDs, method categories, sizes, durations, result classes, and trace IDs.
- Connector API is default deny; a manifest capability does not authorize an operation by itself.
- Deleting local account data is separate from unlinking and requires explicit confirmation in KT.

## 9. Licensing and Distribution

- This connector is AGPL-3.0-only and remains an independent executable.
- signal-cli remains an independent, unmodified GPLv3 executable.
- libsignal remains under Signal's AGPLv3 license inside the signal-cli runtime boundary.
- KT Desktop does not copy or link connector, signal-cli, or libsignal code.
- The host protocol is public, versioned, general-purpose, and does not exchange internal language
  objects or shared memory.

Every binary release must include or provide equivalent access to the exact corresponding source,
build scripts, license texts, notices, SBOM, source commits, hashes, and patch list for all covered
components. Pointing only to a moving upstream branch is insufficient. Phase 1 patch lists must be
empty for signal-cli and libsignal.

This code license does not grant Signal service, API, trademark, or partnership authorization. The
product must describe the integration as unofficial and separately review Signal service terms,
automated messaging behavior, account suspension, and compatibility risk.

## 10. Phases and Exit Criteria

### Phase 0: repository and delivery contract

Deliver:

- Rust project, AGPL license, NOTICE, README, agent rules, and this plan.
- local build, format, test, clippy, and release build.
- local commit only.

Exit: a clean review finds no contract contradiction, secret, unrelated file, or build failure.

### Phase 1: process and protocol PoC

Deliver:

- authenticated private IPC on the current platform with platform abstractions.
- bounded host framing and API handshake.
- signal-cli child process supervisor and bounded JSON-RPC transport.
- runtime status/start/stop and normalized receive events.
- fake signal-cli integration fixture covering success, malformed output, timeout, crash, restart,
  shutdown, duplicate ID, and unknown send outcome.

Exit: fmt, unit/integration tests, clippy, release build, clean review, and local commit.

The macOS implementation and tests use a private Unix socket. The Windows implementation now uses a
random per-start named pipe with a current-user-only DACL, rejects remote clients, protects the
profile directories, bootstraps through inherited stdin, and watches the Electron parent so an
orphan connector shuts down its JVM. The Windows-only Rust modules compile against
`x86_64-pc-windows-msvc` in an isolated target check; native build and runtime acceptance on Windows
hardware are still required before packaging. Cross-compilation alone is not an acceptance claim.

### Phase 2: account linking and text channel

Deliver:

- start/finish/cancel link state with expiry and zeroization.
- account list and account-ID mapping.
- normalized direct/group text events and text send.
- SQLite schema, migrations, stable IDs, dedupe, pagination, unread, and client-request idempotency.

Exit: fake integration coverage plus a separately authorized real-account test. No real-account result
may be inferred from a fixture.

### Phase 3: packaging and resource baseline

Local hardening status (2026-08-09): the signed manifest, exact file verification, corresponding
source/compliance gates, immutable stage/atomic pointers, production verification CLI and fixed JRE
plumbing are implemented and automatically tested. Release keys, signed target bundles, native
Windows packaging/runtime, and the long-duration resource gates below remain open; this phase is
therefore not marked production-complete.

Deliver:

- signed connector, signal-cli, and JRE manifests for Windows x64 and macOS x64/arm64.
- exact source archive, LICENSE/NOTICE/SBOM, hashes, and reproducible build record.
- LKG stage/activate/rollback.
- idle, active text, restart, 24-hour, and multi-account resource reports.

Exit: target-platform resource gates and license delivery review pass.

### Phase 4: KT Desktop integration

Local integration status (2026-08-09): integrated in the separate `kt-desktop` worktree on
`codex/signal-test-main-latest`. Main and Connector use the same per-start endpoint, Main performs a
bounded connect retry instead of testing named-pipe existence, and eager challenge frames cannot be
missed during listener setup. This does not satisfy the remaining native Windows,
signed-distribution, 24/72-hour, or real multi-account production gates.

Deliver in KT Desktop:

- ConnectorSupervisor and connector client.
- native Signal account/conversation/message/composer UI.
- AI language policy, translation, remarks, and quick replies through the shared send path.
- tenant gray rollout and diagnostics.

Exit: desktop contract, integration, actual UI, and target-platform acceptance pass.

### Phase 5: optional media

Deliver only when proven:

- bounded incoming download and disk-pressure behavior.
- streaming file handles without Base64 or arbitrary paths.
- cache cleanup, cancellation, low-disk, and large-file tests.

Exit: resource/security review. Otherwise media remains disabled.

### Phase 6: optional Feature Host

This phase requires evidence of independent release, third-party extension, or fault-isolation need.
It reuses the connector and signal-cli processes and adds a bounded Feature Host pool; it never starts
one worker per conversation or one JVM per account.

## 11. Review Gates

Every phase follows:

```text
implement
  -> focused tests
  -> integration/build checks
  -> security/concurrency/resource review
  -> minimal fixes
  -> retest
  -> rereview
  -> local commit
```

Review priority:

1. wrong-account/wrong-recipient actions, key exposure, arbitrary execution, path traversal, data loss.
2. duplicate sends, unknown-send retry, races, deadlocks, orphan processes, unbounded memory/disk.
3. contract drift, malformed upstream data, restart behavior, diagnostics, missing tests.
4. maintainability and clarity that prevent future boundary mistakes.

No push, remote repository creation, release, deployment, or external message is part of these phases
without separate authorization.
