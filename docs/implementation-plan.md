# KT Signal Connector Implementation Plan

## 1. Status

- Decision date: 2026-08-04
- Current status: Connector Phases 1–3 are implemented locally; the separate KT Desktop Phase 4
  integration is locally merged at `5e18793c`, while production Phase 3 exit gates remain open
- Contract revision: 1.40 (2026-10-05)
- Connector source baseline: `main` @ `6656f70`
- Target engine baseline: unmodified `signal-cli v0.14.8` (upgraded from 0.14.7 on 2026-09-23
  per `docs/signal-cli-upgrade.md`: smoke 4/4 on JRE 25; 0.14.8 adds voice-note metadata and
  GroupsV2 end-group exposure, no JSON-RPC field removals)
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
- Eligibility: only a local addressable outgoing row (`sent`/`delivered`/`read`) qualifies; its `sentAt` was
  overwritten with the send response's upstream Signal timestamp and becomes the delete's protocol
  identity (delivery/read receipts preserve it). Pending/failed/unknown rows carry only a local clock value, and incoming rows are not
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
- Eligibility and target identity: an own outgoing row qualifies in an addressable state
  (`sent`/`delivered`/`read`; its `sentAt` was overwritten with the send response's upstream Signal
  timestamp and preserved across receipts); an incoming
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
- Known behavior boundaries: the local `messages` row itself stays unchanged on a successful
  reaction, but the own reaction is now recorded in `message_events` under the contract 1.27
  actor model (the linked account as actor, no display name) once signal-cli confirms, and a
  `conversation.changed` event is pushed so host-side pills survive the optimistic window
  without a poll; `remove=true` records the removal the same way. Incoming reaction envelopes
  from peers converge through the same store path on receive; nothing is emitted to
  `message.statusChanged` by this method.
- Dispatch inherits the mutating infrastructure unchanged: the write lane with the per-account
  mutex (same-account `sendText`/`remoteDelete`/`sendReaction`/`contacts.sync`/`deleteLocalData`
  serialize), the delete drain barrier, and the per-account request budget. Metrics classify it
  as `send`.

### 4.7 messages.attachments.get (contract revision 1.8, 2026-09-03; **retired with 1.20, 2026-09-24**)

> **Retired (contract 1.20).** The chunked media triple (ADR 0002: `messages.attachments.open`
> / `readChunk` / `closeHandle`) replaced the one-shot base64 reader as the only inbound
> attachment channel; the method, its params `$def`, its lane-table entry, and the fixture
> `getAttachment` handler were removed in the same batch. The history below is kept for
> context; `validate_attachment_payload` and the 100 MiB budget constants survive because the
> send path and the chunked channel still enforce them.

API `1.0` evolves in place again (§4.5/§4.6 precedent); the apiVersion handshake binding is
unchanged. The method is additive and advertised through the handshake `capabilities` array —
calling it against an older connector answers `METHOD_NOT_ALLOWED`.

- `messages.attachments.get` returns one attachment that is **already downloaded** into the
  pinned signal-cli data directory as base64. Params: `accountId`, `conversationId`,
  `messageId`, `attachmentId`, `sizeBytes` (all required). `attachmentId` is the upstream
  attachment id from the received message's attachment metadata (the pinned signal-cli jsonRpc
  receive field `id`); `sizeBytes` is the size the caller expects from that metadata.
- PoC downgrade, recorded per the task contract: the streaming design was evaluated and
  **rejected for this revision** — it would thread a second stream through `host.rs` frame
  accounting, pending budgets, and engine shutdown semantics, well past the allowed intrusion.
  The response is `{"attachmentId", "data"}` — `data` is the base64 payload mirroring the
  upstream `getAttachment` jsonRpc response (`JsonAttachmentData` of the pinned 0.14.7
  distribution, verified in source), and `attachmentId` echoes the request so a caller can
  correlate parallel fetches. The connector adds nothing it does not have: no receive-time
  attachment metadata is persisted locally this revision, so there is no `contentType`/
  `filename` to serve. The local `messages` row is never modified and no event is emitted.
- Why the limit is 5 MiB and not the 10 MiB task ceiling: the task explicitly allowed the
  downgrade only up to 10 MiB, but the pinned engine reads upstream stdout lines with an 8 MiB
  limit (`DEFAULT_UPSTREAM_LINE_LIMIT`, `src/engine.rs`) and a longer line faults the shared
  engine with the existing oversized-output semantics, killing every account in the proxy group.
  10 MiB of raw bytes is ~13.7 MiB of base64 — incompatible. The connector therefore enforces
  5 MiB raw (5,242,880 bytes), which encodes to at most 6,990,508 base64 characters, a
  deliberate margin under the line limit. `sizeBytes` is the caller's declared budget, verified
  against the actual decoded size before anything is returned to the host; a mismatch answers
  `INVALID_REQUEST`. The engine line limit remains the hard backstop: the connector persists no
  attachment metadata locally (receive normalization keeps none), so a caller that declares a
  small size for a genuinely larger attachment still trips the upstream fault — a known PoC
  boundary, and one more reason full media enablement stays future work under §6.7.
- Downloading is out of scope and stays out: the connector keeps starting signal-cli with
  `--ignore-attachments` (§6.7), so `getAttachment` is a pure local read of an
  already-downloaded file (`AttachmentStore.retrieveAttachment`, verified in source — no
  upstream fetch, no network call). An attachment signal-cli has not downloaded answers
  `UPSTREAM_ERROR` (the upstream `FileNotFoundException` → `UserErrorException` jsonRpc error),
  and the row in the local history carries no attachment metadata this revision (receive
  normalization keeps none), so the caller learns attachment ids out of band. This is the
  explicit PoC boundary: it proves the bounded token-for-bytes path end to end; enabling
  downloads and streaming remain future work under §6.7.
- Error mapping reuses existing codes only (zero new codes): `ACCOUNT_NOT_FOUND`,
  `CONVERSATION_NOT_FOUND`, `MESSAGE_NOT_FOUND` (missing conversation/message row), `INVALID_REQUEST`
  (shape, oversized `sizeBytes`, or a size mismatch), `RUNTIME_NOT_RUNNING`, `UPSTREAM_EXITED`,
  `UPSTREAM_TIMEOUT`, and `UPSTREAM_ERROR` for the explicit upstream rejection (attachment not
  downloaded, or the upstream call failed) — its `retryable=true` is the global mapping.
- Dispatch: the upstream call is read-only against signal-cli, but the response writes a large
  frame, so the method joins the **read lane** (`conversations.list`/`messages.list`/`getText`
  semantics, `READ_CONCURRENCY`), with the per-account pending budget unchanged. Metrics
  classify it as `read`.

### 4.8 groups.get (contract revision 1.9, 2026-09-03)

Read-only projection of the local contacts cache for one group. Params:
`{accountId, groupKey}` — `groupKey` is the upstream group id (`opaqueId` shape, 1..128 bytes).
The result is `{peerKey, title, memberCount?, syncedAt}`: `title` is the synced group name,
`memberCount` is present when the sync batch captured a member count (`extra` JSON
`memberCount`), and `syncedAt` is the cache row's sync timestamp.

- Data source and staleness: `groups.get` is served from the rows written by `contacts.sync`, The sync path short-circuits
  batches younger than 60 seconds and best-effort sync runs on `link.finish`, so a fresh link
  can read without an explicit sync; beyond that the row is exactly as stale as the last sync.
  The response carries `syncedAt` so the host can judge staleness itself — the connector adds
  no second cache layer.
- Errors (one new code, registered in the schema error enum): `ACCOUNT_NOT_FOUND` for an
  unknown account; `INVALID_REQUEST` for a malformed `groupKey`; `GROUP_NOT_FOUND` when no
  `kind='group'` contacts row exists for `(accountId, groupKey)` — this covers a group the
  account has left (the sync filters `isMember=false`, so departed groups are never cached),
  a peer key that names a contact instead of a group, and a never-synced group. The method
  never calls the upstream: an unsynced cache is a `GROUP_NOT_FOUND`, not a `listGroups` call.
- Dispatch: no upstream call and no event emission; served under the **read lane** together
  with `contacts.list` (no upstream work, but the same bounded concurrency). Metrics classify
  it as `contacts`.
- Local alias is out of scope here: contact titles in the cache are upstream display names;
  the host-side rename method is `contacts.setLocalAlias`, which does not touch this cache
  either (it renames upstream, via `updateContact`).

### 4.9 contacts.setLocalAlias (contract revision 1.10, 2026-09-03)

Rename one contact as the linked account sees it. Params: `{accountId, peerKey, alias,
operationId?}` — `alias` is 1..128 UTF-8 bytes, `operationId` is an optional host correlation
id (validated, never persisted; same semantics as §4.5/§4.6). The upstream call is the
mutating `updateContact` jsonRpc command with parameters `{"account", "recipient", "name"}`:
`recipient` is the peer key as a **single string** (verified against the pinned 0.14.7
`UpdateContactCommand`, which reads it with `ns.getString("recipient")` — passing an array
would throw a ClassCastException and surface as UPSTREAM_ERROR; this corrects the earlier
feasibility note that wrote `recipient: [peerKey]`), and `name` is the new alias.

- Targeting: `peerKey` must resolve to a peer the account already knows — an existing
  `contacts` row with `kind='contact'`, or an existing `direct` conversation peer. Anything
  else (unknown number, a group key, an unsynced contact) answers `INVALID_REQUEST` before
  any upstream call; no new error code is registered for this method.
- Outcome semantics (sendReaction precedent): upstream `Ok` answers `{status: "updated"}`;
  an indeterminate mutating outcome (`EngineError::UnknownOutcome` — engine exit or timeout
  with the call in flight) answers `{status: "unknown"}`. `unknown` is never retried
  automatically by the connector; the host may re-send with a fresh requestId.
- Dispatch: mutating, so the method joins the **send lane** with the per-account mutex and
  the delete barrier (`sendText`/`remoteDelete`/`sendReaction` semantics); same-account
  mutations serialize. Metrics classify it as `send`. No local row is modified — the alias
  lives in the upstream account data and comes back through the next `contacts.sync`, so
  the response status is the only authoritative answer. No event is emitted (the local
  history carries no contact row to transition).

### 4.10 presence.setTypingMessage (contract revision 1.11, 2026-09-03)

Send or stop the typing indicator to one conversation. Params: `{accountId, conversationId,
stop?, operationId?}` — `stop` defaults to `false` (start typing); `operationId` is an
optional host correlation id (validated, never persisted; same semantics as §4.5–§4.9).
Targeting is by `conversationId` only: the conversation row must already exist in the
local history — direct and group conversations are both addressable — and anything else
(missing row, foreign account) answers `CONVERSATION_NOT_FOUND` before any upstream call.

- Upstream call: the mutating `sendTyping` jsonRpc command. Verified against the pinned
  0.14.7 distribution bytecode: `SendTypingCommand.getName()` is `sendTyping`; its dests
  are `recipient` (`nargs="*"`, consumed with `ns.getList("recipient")` — `getList` wraps
  a scalar in a one-element list, so the array form the connector sends follows the
  `sendReaction` precedent), `group-id` (mapped to the camelCase `groupId` JSON key by
  `JsonRpcNamespace`'s dash→camel fallback), and `stop` (a boolean dest read with
  `ns.getBoolean`). `TypingAction` carries START/STOP; the upstream indicator expires
  automatically after roughly 15 seconds, so `stop` is an explicit early clear, not a
  requirement for the indicator to end. The connector always sends the `stop` key
  explicitly (`{account, stop, recipient: [peerKey]}` or `{account, stop, groupId}`) —
  with the key absent `getBoolean` answers null and the connector does not depend on how
  the upstream treats a missing boolean.
- Outcome semantics (sendReaction precedent): upstream `Ok` answers `{status: "sent"}`;
  an indeterminate mutating outcome (`EngineError::UnknownOutcome`) answers
  `{status: "unknown"}`. `unknown` is never retried automatically by the connector; the
  host may re-send with a fresh requestId.
- Dispatch: mutating, so the method joins the **send lane** with the per-account mutex
  and the delete barrier (`sendText`/`remoteDelete`/`sendReaction`/`setLocalAlias`
  semantics); same-account mutations serialize. Metrics classify it as `send`.
- Nothing is written locally and no event is emitted: a typing indicator is ephemeral
  upstream protocol state, not a history row. The inbound direction is out of scope this
  revision — incoming typing events remain a declared `TODO` in the event contract.

### 4.11 messages.sendReceipts (contract revision 1.12, 2026-09-05) — reserved, not wired

Registered intent: `messages.sendReceipts` sends **delivery receipts** for previously
received messages, shape `{accountId, conversationId, timestamps, type, operationId?}`
with `type` deliberately restricted to `"DELIVERY"`. Read receipts are out of scope for
this method by contract: READ receipts leak exactly-when-read and the connector never
sends them. This revision registers the contract only — the method is **not** added to
the schema method enum, so the connector answers `METHOD_NOT_ALLOWED` today — because
the pinned signal-cli 0.14.7 distribution exposes no upstream channel to send a
delivery receipt. Verified against the pinned distribution bytecode:

- The only receipt command is `SendReceiptCommand`, whose jsonRpc name is `sendReceipt`
  (singular). Its `--type` dest carries exactly two choices (`read`, `viewed`), and its
  `handleCommand` dispatches only `Manager.sendReadReceipt`/`Manager.sendViewedReceipt`,
  falling through to `UserErrorException` for anything else. The full jsonRpc command
  table contains no `sendReceipts` (plural) and no delivery-sending command.
- The libsignal layer does define `SignalServiceReceiptMessage$Type.DELIVERY`, but no
  CLI code path reaches it from jsonRpc: `Manager` exposes no `sendDeliveryReceipt`.
- A host-triggered delivery receipt is redundant protocol state to begin with: the
  pinned `IncomingMessageHandler.handleMessage` already constructs
  `SendReceiptAction(recipientId, Type.DELIVERY, timestamp)` for incoming messages, so
  the engine answers deliveries automatically on receipt.

Consequences for the host:

- Delivery receipts need no connector API: they are guaranteed by the engine's inbound
  path, not by an explicit host call.
- The reservation exists so a future upstream that wires `sendDeliveryReceipt` can add
  the method without reshaping the contract. Registering a method that could only fail
  upstream (`sendReceipt` with an unsupported type, answering `UPSTREAM_ERROR` on every
  call) was rejected: a permanently-failing lane is worse than the explicit
  not-implemented `METHOD_NOT_ALLOWED` answer.

### 4.12 messages.attachments.send (contract revision 1.13, 2026-09-22)

API `1.0` evolves in place again (§4.5–§4.7 precedent); the apiVersion handshake binding is
unchanged. The method is additive and advertised through the handshake `capabilities` array —
calling it against an older connector answers `METHOD_NOT_ALLOWED`. It follows the
`messages.sendText` (§4.2) implementation shape end to end: pending row → upstream mutating
call → `complete_send_success` / `complete_send_unknown` settlement.

- `messages.attachments.send` sends one attachment (optionally with a text caption) to one
  conversation. Params: `accountId`, `conversationId` **or** `kind`+`peerKey`(+`peerTitle`)
  addressing (exactly one form, `sendText` semantics), `clientRequestId` (idempotency),
  `dataBase64`, `sizeBytes` (declared raw budget), optional `filename` (1–128 bytes;
  caller-supplied display name, rejected outright on path separators or control
  characters — never sanitized), optional `contentType`
  (1–128 chars, `type/subtype` shape), optional `text` caption (existing `validate_text`
  bounds). The base64 payload is **decoded by the connector and re-encoded as an RFC 2397
  data URI** — upstream jsonRpc `send` accepts `attachments` entries as file paths or data
  URIs, and `AttachmentHelper` (pinned 0.14.7 distribution, verified in bytecode and source)
  decodes data URIs itself, uploads via the CDN path, and manages its own temp file lifetime.
  The connector therefore never persists attachment bytes and never passes caller-controlled
  file paths upstream (signal-cli itself rejects data-directory paths — same boundary).
- Why not file paths: the host→connector boundary passes bytes, not paths (§8 discipline);
  a connector-managed temp file would create a third cleanup owner and a crash window, and
  upstream already implements the data-URI decode with its own bounded temp handling.
- Why the limit stays 5 MiB raw (5,242,880 bytes): symmetric with `messages.attachments.get`
  (§4.7). The inbound host frame limit rises with this revision — `DEFAULT_HOST_FRAME_LIMIT`
  1 MiB → 16 MiB (`lib.rs`) — because 5 MiB raw encodes to at most 6,990,508 base64 chars
  (~6.99 MB) plus the JSON envelope; 16 MiB matches the desktop-side
  `DEFAULT_MAX_CONNECTOR_FRAME_BYTES` so the whole chain narrows at the same gate. The 8 MiB
  upstream **output** line limit (`DEFAULT_UPSTREAM_LINE_LIMIT`) is not in the send path: the
  `send` response carries only ids/timestamps, and the data URI travels connector→upstream on
  an inbound line for which signal-cli sets no length cap (`BufferedReader.readLine`,
  verified in v0.14.7 source). The frame limit raise is deliberate and reviewed: one host
  connection may hold one ≤16 MiB line buffer, bounded by the existing per-connection model;
  the desktop client is the only local peer (private Unix socket / named pipe, peer
  handshake-authenticated), so the exposure is the same trust domain as before.
- Result: the sent `MessageRecord` (same projection as `sendText`) on upstream confirmation,
  or `SEND_OUTCOME_UNKNOWN` when the mutating outcome is indeterminate — identical settlement
  and no-auto-retry discipline as every other mutating send (§4.2). The pending row's `text`
  is the caption (may be empty). The pending and sent rows carry the metadata-only attachment
  descriptor (the inbound wire shape, contract 1.15) so the desktop renders the outgoing
  attachment from its first tick, and the conversation preview falls back to the filename
  when the caption is empty — never a blank preview for an attachment send.
- Error mapping reuses existing codes only (zero new codes): `ACCOUNT_NOT_FOUND`,
  `CONVERSATION_NOT_FOUND`, `INVALID_REQUEST` (shape, base64 decode failure, size mismatch,
  bounds violations), `RUNTIME_NOT_RUNNING`, `UPSTREAM_EXITED`, `UPSTREAM_TIMEOUT`,
  `UPSTREAM_ERROR` for an explicit upstream rejection (`AttachmentInvalidException` family) —
  its `retryable=true` is the global mapping, no auto-retry.
- Dispatch inherits the mutating infrastructure unchanged: the write lane with the per-account
  mutex (same-account sends serialize), and the per-account request budget. Metrics classify
  it as `send`.

### 4.13 Inbound control plane: quote / reaction / remote delete / typing / edit / attachment metadata (contract revision 1.15, 2026-09-23)

API `1.0` evolves in place (§4.5–§4.12 precedent). Receive normalization stops dropping the
five per-message control surfaces the official clients and signal-cli both carry, and attaches
bounded inbound attachment metadata to message rows. Everything stays metadata-only: bytes are
still never downloaded or persisted (§6.7 media boundary unchanged).

Inbound shapes (pinned signal-cli `MessageEnvelope` json surface, cross-checked against the
official v0.14.8 JSON schemas):

- **quote** (`dataMessage.quote` / `syncMessage.sentMessage.quote`): `{id, author, text}` —
  the quoted message's upstream timestamp, author number, and bounded text preview. Stored on
  the message row; the host resolves display through its own history.
- **reaction** (`dataMessage.reaction` / `syncMessage.sentMessage.reaction`):
  `{emoji, targetAuthor, targetSentTimestamp, isRemove}` — recorded as a per-conversation
  event (`message_events` table), not a message row, mirroring the protocol shape. The host
  receives a `conversation.changed` event and re-reads events with the conversation.
- **remoteDelete** (`dataMessage.remoteDelete` / `syncMessage.sentMessage.remoteDelete`):
  `{timestamp}` — the target upstream timestamp authored by the envelope sender. The matching
  outgoing row (our own multi-device delete) or incoming row (peer delete) in the same
  conversation is marked remote-deleted locally (no upstream call); the host receives
  `message.statusChanged` with the new status.
- **typing** (`typingMessage`): `{action, groupId}` — START/STOP is ephemeral state only,
  never persisted. Emitted to the host as a new `conversation.typing` host event; a fixed
  in-memory rate limiter (one notification per account+peer per second, bounded map, oldest
  entry evicted) keeps a typing storm from flooding the event lane. The host owns display
  timeout.
- **edit** (`editMessage` / `syncMessage.sentMessage.editMessage`):
  `{targetSentTimestamp, dataMessage}` — the new body replaces the target row's body when the
  row exists in the same conversation and the editor matches the row's sender identity; the
  row's `edited_at` marks it edited. Missing/mismatched targets are dropped without a local
  row; the upserted event still notifies the host.
- **attachment metadata** (`dataMessage.attachments[]` / `syncMessage.sentMessage.attachments[]`):
  `{id, contentType, filename, size, width, height, isVoiceNote}` — bounded to 32 entries per
  message, each `id` ≤ 128 chars, `filename` ≤ 128 bytes, `contentType` ≤ 64 chars. Persisted
  as a JSON column on the message row. Bytes are still never downloaded (`--ignore-attachments`
  unchanged): `messages.attachments.get` keeps answering `UPSTREAM_ERROR` for these ids until
  a dedicated bounded download PoC exists (§6.7). `messages.attachments.send` (§4.12) also
  records its own single descriptor on the sent row at confirm time, so the sender's own
  bubble shows the attachment across restarts.

Consequences for the host: the `MessageRecord` projection gains optional `quote`, `attachments`,
`remoteDeleted`, and `editedAt` fields (absent on rows without them), and the host event surface
gains `conversation.typing` (ephemeral) alongside the existing `message.upserted` /
`message.statusChanged` / `conversation.changed` events.

### 4.14 messages.edit (contract revision 1.15, 2026-09-23)

`messages.edit` edits one previously sent message upstream: params `accountId`,
`conversationId`, `messageId`, `text`, `clientRequestId`. It resolves the target exactly like
`messages.remoteDelete` (addressable outgoing row `sent`/`delivered`/`read` = upstream protocol identity),
then dispatches signal-cli `send` with `editTimestamp: <upstream timestamp>` plus the same
target addressing (`recipient`/`groupId`) — the signal-cli edit entry point (no dedicated
`sendEditMessage` exists in the jsonRpc surface). Official clients allow a 24 h edit window;
the connector adds no local window — a late edit answers the upstream rejection
(`UPSTREAM_ERROR`), keeping the connector's rule set minimal.

Settlement mirrors `messages.sendText`: on confirmed success the row's body is replaced and
`edited_at` set (status unchanged) and the method answers the updated `MessageRecord`; an
indeterminate outcome answers `SEND_OUTCOME_UNKNOWN` with the local row unchanged. No
auto-retry.

### 4.15 Attachment budget to official scale + reaction aggregate projection (contract revision 1.16, 2026-09-23)

Two alignment changes with the official clients' behavior, both host-visible without new
methods or events.

**Attachment size budget.** `messages.attachments.send` lifts the deliberate PoC ceiling from
5 MiB to the official 100 MiB (104857600 bytes, the same budget Signal Desktop enforces). The
four framing constants move together so one attachment never overruns its container:

| Constant | Value | Role |
| --- | --- | --- |
| `MAX_ATTACHMENT_BYTES` (service) | 104857600 | declared + validated attachment payload |
| `DEFAULT_HOST_FRAME_LIMIT` (lib) | 167772160 (160 MiB) | one inbound host frame |
| `DEFAULT_UPSTREAM_LINE_LIMIT` (lib) | 167772160 (160 MiB) | one signal-cli JSON-RPC line |
| `MAX_PENDING_HOST_BYTES` (host) | 184549376 (176 MiB) | in-flight frame accumulation pool |

The base64 expansion of a 100 MiB attachment (~134 MiB) plus its JSON wrapper fits the 160 MiB
frame/line limits; the host pool holds one full frame plus overhead. `build_attachment_data_uri`
now sizes its buffer from the actual base64 length instead of preallocating the maximum. The
schema's `sizeBytes` maximums and the `attachments.get`/`attachments.send` descriptions state
the same budget. Upstream acceptance of very large payloads still depends on the pinned
signal-cli reader (§6.7 boundary unchanged: bytes are never persisted connector-side).

**Reaction pills in the message projection.** `MessageRecord` gains `reactions`:
`[{emoji, count, mine}]`, aggregated from `message_events` rows (`removed=0`, grouped by emoji,
`mine` marks the linked account's own reaction, oldest first). The field is always serialized —
an empty array, never absent — so a host that merges upserted records into cached state
replaces the whole list and never keeps a stale pill. Read paths (`messages.list`,
`messages.get`) and every `message.upserted` event path (send settlement, edits in both
directions) attach the aggregates; reaction add/remove itself notifies through
`conversation.changed` only (§4.13 shape), and the host refreshes the open conversation from
that event.

The schema also gains the `conversation.typing` event entry (§4.13 shipped the event but
omitted it from the schema enum; data `{accountId, conversationId, action: START|STOP}`).

### 4.16 Retire messages.attachments.get (contract revision 1.20, 2026-09-24)

The desktop contract (kt-desktop `contracts/signal-host-adapter.md` 1.20) removes
`getAttachmentSessionMessage` / `messages.attachments.get`: the ADR 0002 chunked triple
(§4.15-era media PoC, contract revision 1.17) has been the default inbound-attachment channel
since 1.17 and the one-shot base64 path only added a second code path to validate (whole
payload through one host frame, no chunk budget). Removal is symmetric across both repos:

- host: dispatch branch, `MessagesGetAttachmentParams`, service `prepare_get_attachment`,
  supervisor `get_attachment` (the only upstream `getAttachment` call), registry router,
  lane-table entry, metrics label, schema method enum + `messagesAttachmentsGetParams` `$def`
  + if/then binding, and the fixture's `getAttachment` handler.
- kept on purpose: `validate_attachment_payload` / `MAX_ATTACHMENT_BASE64_CHARS` (send path),
  `sanitize_attachment_id` (open path), and all budget constants — the chunked channel and
  the send path still enforce them.

Failure-shape coverage moves with the replacement: NOT_FOUND-before-upstream addressing and
sanitize/path-traversal rejection are asserted on `messages.attachments.open`
(`open_media_handle`, integration + unit tests), and the upstream base64 passthrough test was
deleted with the method.

### 4.17 Device-unlink detection and account recovery (contract revision 1.21, 2026-09-24)

The desktop contract (kt-desktop `contracts/signal-host-adapter.md` 1.21) adds a joint
recovery path for the case where the phone unlinking a linked device leaves stale local
state on both sides. Previously the engine kept pinging a dead link forever, the supervisor
kept restarting it (ping failures looked like any other engine failure), and the desktop
surfaced a permanent `ACCOUNT_BOUND` wall with no way forward from the UI.

Connector behavior, all inside the existing process/watchdog boundary (no new IPC method):

- upstream error classification: engine responses carrying
  `AuthorizationFailedException` / `Authorization failed` map to
  `EngineError::Unauthorized` instead of a generic upstream error.
- attribution: because `send`-family failures arrive without a caller context, the engine
  attaches the owning account (from the request params' `account` field) to the failure and
  emits a new internal `AccountUnauthorized { account }` event; requests with no account
  attribution stay unclassified.
- durable state: `store.mark_account_device_unlinked` moves the account to a new
  `device_unlinked` static state (guarded against clobbering an in-flight `unlinking`);
  the service turns the store result into host-side `account.changed` events so the
  desktop sees the transition.
- watchdog: an `Unauthorized` ping result resets the failure counter and does NOT restart
  the engine — restarting cannot fix a phone-side unlink and only burns the episode
  budget. The account-unlink watch loop (spawned in `Supervisor::new`, aborted on drop)
  consumes `AccountUnauthorized` events; the registry router deliberately ignores them
  (no-op arm) so harnesses without a registry still exercise the real path.
- wire surface: error enum gains `ACCOUNT_UNLINKED` (`retryable: false`). The upstream
  classification, the store state, and the wire code are covered by unit tests, the
  `fake-signal-cli` `.fixture-auth-failed-user-status` marker, and a
  `supervisor_watchdog` integration test asserting the account flips state without a
  restart.

Desktop-side obligations (same revision): the `device_unlinked` state enters the wire
state enum; linking over a dead owner binding auto-cleans (unbind + local-data purge only
when the collision is a different account id) and proceeds; deleting a session purges its
connector session via the host adapter.

### 4.18 Conversation summary last-message kind (contract revision 1.23, 2026-09-28)

The desktop contract (kt-desktop `contracts/signal-host-adapter.md` 1.23) adds an optional
`lastMessageKind` to conversation summaries so the desktop list renders the official
attachment-noun preview ("📷 Photo" / "🎥 Video" / "🎤 Voice message" / "📎 File") without
reverse-engineering it out of the preview string.

- derivation is read-time only, per summary row: the newest message
  (`ORDER BY sent_at DESC, id DESC LIMIT 1`, covered by the existing
  `messages_conversation_sent_at` index) decides. An attachment-only ending — the row has
  attachments and no non-empty body text — yields the first attachment's kind:
  `image/*` → `image`, `video/*` → `video`, `audio/*` or the voice-note flag → `audio`,
  anything else → `file`. A text/caption ending, a system row, or an empty conversation
  yields nothing: the field is omitted and the desktop falls back to `lastMessagePreview`.
  The caption wins over the noun — exactly the official list-preview behavior
  (official source: a body text takes precedence over the attachment type noun).
- wire shape: `lastMessageKind?: 'image' | 'video' | 'audio' | 'file'` on
  `ConversationSummary`, `skip_serializing_if = "Option::is_none"` like
  `lastMessagePreview`. Nothing new is persisted; the contract 1.15
  preview/filename fallback stays authoritative for the text itself.
- cost: one extra indexed LIMIT-1 query per summary row per page read (local SQLite,
  desktop-scale page sizes). No write-path change, no cache, no new IPC method.

### 4.19 Inbound peer receipts (contract revision 1.24, 2026-10-01)

`envelope.receiptMessage` — the delivery/read confirmation a peer sends back for our
outgoing rows — stops being skipped and routes through the inbound control plane (§4.13
precedent). Shape (pinned signal-cli `JsonReceiptMessage`): `{timestamp: [...], when,
isDelivery, isRead, isViewed}`.

- tier selection: `isViewed` > `isRead` > isDelivery-default. The desktop status ladder is
  `pending → sent → delivered → read`, so `viewed` maps onto `read` on the wire; there is
  no fourth tier to surface.
- confirmed timestamps are bounded (256, de-duplicated, truncation on overflow) and resolve
  to outgoing rows in the envelope sender's direct conversation by upstream `sent_at`.
- upgrade is monotonic and per row: `pending|sent → delivered → read`; a lower tier never
  downgrades a higher one, and incoming/terminal/system rows are never touched. Only real
  transitions emit `message.statusChanged` (one per moved row) — replays are silent.
- conversation scoping: receipts carry no `groupId`, so only direct conversations resolve
  (envelope sender as peer). Group-sent rows stay untouched for now — a declared boundary,
  not a silent gap.
- nothing new is persisted: receipt state lives in the message rows' `status` column, so
  history reads and restarts see the last tier for free. Offline-period receipts (peer
  confirmed while the connector was down) are lost with the envelope and surface on the
  next real receipt — the same eventual consistency the official clients accept.
- no new IPC method, no schema-event addition: the host learns through the existing
  `message.statusChanged` event and its existing status projection.

### 4.20 Inbound rich bodies: previews / mentions / textStyles / viewOnce (contract revision 1.25, 2026-10-01)

The four rich-body fields a peer's `dataMessage` (and the multi-device `syncMessage.sentMessage`
mirror) carries stop being dropped and flow to the host on the message row. Shapes are the pinned
signal-cli 0.14.8 JSON models, verified by decompiling `JsonPreview` / `JsonMention` /
`JsonTextStyle` from the distribution jar:

- `previews: [{url, title?, description?, image?}]` — bounded to 4 cards; url capped at 2048
  chars, title/description at 512 UTF-8 bytes. `image` reuses the §4.13 metadata-only attachment
  descriptor (id/contentType/filename/size/width/height); the connector never fetches image bytes.
- `mentions: [{number|uuid, name?, start, length}]` — bounded to 64 ranges; author resolves
  number-first, name capped at 128 bytes. Ranges index into the received body text.
- `textStyles: [{style, start, length}]` — bounded to 64 ranges; only `BOLD` `ITALIC`
  `STRIKETHROUGH` `MONOSPACE` `SPOILER` pass validation, `NONE` and unknown values drop
  individually.
- `viewOnce: true` — persisted only when true; plain messages carry nothing.

Bounds follow §4.13: entries past a cap drop, oversized strings truncate, malformed entries drop
individually — a hostile payload can never fail the message itself. An all-default result
collapses to absent, so rows without rich data are byte-identical to pre-1.25 rows.

Persistence: one additive nullable `messages.rich_json` TEXT column (schema 8 → 9) holding the
packed `NormalizedRich` JSON, read back flattened onto the wire MessageRecord as
`previews` / `mentions` / `textStyles` / `viewOnce` (absent keys stay absent on the wire). Rich
fields never change after receive — an edit replaces the body but keeps the original ranges
(the official client re-renders edited bodies the same way until a new body arrives with fresh
ranges). Outgoing rows our client sends stay plain text; the desktop does not compose rich bodies.

No new IPC method or event: the host learns through the existing `message.upserted` payload and
history reads. The desktop renders previews as link cards, mentions as highlighted ranges, the
four styles, and a view-once badge; its boundaries are declared in the desktop contract 1.25.

### 4.21 Outbound link previews on messages.sendText (contract revision 1.26, 2026-10-01)

`messages.sendText` gains an optional `previews` array; only the first entry is used because the
pinned signal-cli builds `List.of(one)` preview per send (verified by decompiling `SendCommand` /
`ManagerImpl` from the distribution jar). The entry shape is `{url, title, description?,
imageDataUri?}`, and every constraint is enforced deterministically before the pending row exists:

- `url`: trimmed, non-empty, ≤ 2048 bytes, absolute http(s), and **must appear in the message
  text** — signal-cli's own requirement ("the same url must also appear in the message body")
  surfaced here as a local `INVALID_REQUEST` instead of a late upstream send error.
- `title`: non-empty, ≤ 1024 bytes. `description`: optional, ≤ 4096 bytes.
- `imageDataUri`: optional, `data:image/*` RFC 2397 data URI only, ≤ 1.5M chars (~1.1 MiB
  binary). Bytes stay in memory and pass straight to upstream — the same pattern as
  `messages.attachments.send` (upstream `AttachmentHelper.uploadAttachment` is data-URI aware,
  verified from bytecode). **No caller-controlled file path ever reaches upstream.**

Validation passes project 1:1 onto the upstream JSON-RPC keys `previewUrl` / `previewTitle` /
`previewDescription` / `previewImage` (`JsonRpcNamespace` maps dash-separated option names to
camelCase — verified from bytecode). The connector never composes or synthesizes preview content.

### 4.22 Conversation-summary author/reaction metadata + per-actor reaction detail (contract revision 1.27, 2026-10-02)

Two read-side capability additions that close the desktop list/interactions gaps the v8.31 parity
audit filed against the wire ("wire 缺作者/反应元数据"): the conversation list cannot render the
official group-author prefix, reaction emoji prefix, or send-state direction on the last message,
and the ReactionViewer ("who reacted") has no per-author data — the 1.16 pill projection is
aggregated only. No new IPC method or event; both surfaces evolve in place.

**Author-name capture (shared foundation).** The receive envelope's `sourceName` — the peer's
profile/contact label as the linked account sees it, already bounded to 64 chars by the engine —
is now persisted at receive time on two additive nullable columns (schema 9 → 10):
`messages.sender_name` for incoming message rows and `message_events.actor_name` for inbound
reaction events. Outgoing rows and own multi-device echoes store nothing (the author is the
account itself). Pre-1.27 rows are never backfilled: the name a past envelope carried was never
stored and must not be invented. `MessageRecord` projects the column as `senderName`
(skip-when-absent), so the desktop can already name group senders on the canvas.

**Conversation summary fields.** `ConversationSummary` gains three optional fields derived at
read time from the newest row (`ORDER BY sent_at DESC, id DESC LIMIT 1`, the existing
`messages_conversation_sent_at` index; one extra indexed LIMIT-1 reaction query per row, bounded
to 8 distinct emoji):

- `lastMessageDirection?: 'outgoing' | 'incoming'` — absent when the conversation has no rows or
  ends on a system row, the same endings the official preview shows without send state or author.
  `outgoing` marks the linked account as the author: the client renders its own localized self
  label ("You"), the connector never ships locale-dependent strings.
- `lastMessageAuthorName?: string` — the newest incoming author's captured display name; absent
  for outgoing (self), system endings, and rows without a captured name.
- `lastMessageReactions?: string[]` — distinct active reaction emoji on the newest row, oldest
  reaction first (the same order the message pills use); absent when none. A reaction on an older
  row never leaks into the summary, and removal drops the emoji.

All three use `skip_serializing_if`, so rows without the data are byte-identical to pre-1.27
summaries and a host that merges summaries replaces the fields wholesale.

**Per-actor reaction detail.** The storage was already per-actor (`message_events` keys
`(conversation, target_timestamp, actor_id)` with `emoji`/`removed`/`updated_at`, one row per
actor per target); only the wire aggregated it. `MessageReactionSummary` gains `actors` (always
serialized, like `reactions` itself, so host-side pill merges stay total and a removal clears the
actor it removed):

```text
reactions: [{
  emoji: string,          // unchanged (1.16)
  count: number,          // unchanged (1.16); == actors.length
  mine: boolean,          // unchanged (1.16)
  actors: [{              // new in 1.27, newest reaction first (official ReactionViewer order)
    self: boolean,        // true = the linked account's own reaction
    name?: string,        // peer display name captured from the reaction envelope's sourceName
    reactedAt: number     // local wall-clock ms the reaction was last recorded
  }]
}]
```

Removal semantics: a removed reaction (`isRemove` upsert) is excluded from `count` and `actors`
entirely — removal is the actor's disappearance, exactly like the aggregated count it already
followed; re-reacting flips the row back with a fresh `reactedAt`. Reads are bounded
deterministically: at most 1024 newest active reaction rows per conversation read and 64 actors
per emoji, so a pathological store cannot inflate a response. `actorId` (the connector-internal
sender hash) deliberately stays off the wire — it is not resolvable by the host and carries no
rendering value beyond `self` + `name`.

### 4.23 Conversation-summary last-message send state (contract revision 1.28, 2026-10-02)

The last of the three list-preview gaps the v8.31 parity audit filed against the wire
(audit-B `b-row-status`, P1): the official list renders the send-state icon on the last message
— only when that message is outgoing (`sending` 4s spinner / `sent` check / `delivered`
double-check / `read`·`viewed` solid double-check / `error` red exclamation) — and the summary
carried no field to drive it. Pure read-side addition, the 1.27 pattern: no new IPC method, no
event, no store-schema change (the `status` column has been persisted at receive/send time since
the 1.15 era; nothing is backfilled because nothing needs to be).

`ConversationSummary` gains one optional field derived in the same single newest-row read as the
1.23/1.27 projections (`conversation_last_message_meta`, the existing
`messages_conversation_sent_at` index, zero extra queries):

- `lastMessageStatus?: 'pending' | 'sent' | 'delivered' | 'read' | 'failed'` — present only when
  the newest row is `outgoing`, and then exactly that row's `status` value: the same vocabulary
  `MessageRecord.status` projects, so the list icon and the bubble icon can never disagree. The
  desktop maps the tiers onto its existing official projection (`pending` → sending spinner,
  `sent` → check, `delivered`/`read` → double-checks, `failed` → red exclamation); the connector
  ships no icon or locale-dependent string.

Boundaries, all explicit: incoming and system endings and empty conversations expose nothing
(the official icon exists only on outgoing endings); an outgoing row that ended
`remote-deleted` or `unknown` exposes no status either — the official icon set has no
representation for a deleted last message, so the row degrades to the same no-icon rendering an
incoming ending gets while `lastMessageDirection` still marks it self-authored. There is no
`paused` tier (the connector has no pausable send pipeline) and no `error` tier — send failures
already land as `failed` rows via the existing failure path, which the summary reflects for
free. Receipt upgrades (§4.19) move the summary field for free on the next read: it is a
projection of the row, never a cached copy.

The field uses `skip_serializing_if`, so endings without a send state are byte-identical to
pre-1.28 summaries. Feasibility note recorded for the desktop parity backlog (verified against
the pinned signal-cli command registry, all 61 JSON-RPC methods enumerated from source): mute,
archive, forward, and full-text search have no upstream JSON-RPC support — conversation-level
mute does not exist upstream at all, `listContacts[].isArchived` is a read-only legacy-contact
projection with no write API, and there is no `forward` or `search` method; any desktop-side
mute/archive/forward/search can therefore only be connector-local state or a local
approximation (never cross-device synced), which is a separate decision, not part of this
revision.

### 4.24 Unread @mentions: row marker + conversation badge (contract revision 1.29, 2026-10-02)

The official list renders an @-mention badge next to a conversation's unread counter when the
unread messages @mention the account (Signal-Desktop `unreadMentionCount`), and highlights the
mentioned range as "@you" on the bubble. Neither was representable on the wire: the desktop
cannot decide self-ness itself because the account address it sees (`maskedAddress`) is masked,
and mention ranges carry the *mentioned peer's* identity, not the reader's. Two additive
read/write surfaces close the gap; the store schema moves 10 → 11.

**Row marker.** `MessageRecord` gains `mentionsSelf?: boolean` (skip-when-false, so plain rows
are byte-identical to pre-1.29 rows): at receive, the ingest compares every normalized mention
author (§4.20 `mentions[].author`, resolved number-first by the engine) against the linked
account's own number and persists the verdict on the row in the same insert that stores the
rich body. The comparison is **number-based by necessity**: the pinned upstream jsonRpc surface
exposes the account only as `listAccounts: [{number}]` (`JsonAccount(number)` in the pinned
source) — the account UUID is not queryable — so a mention author carrying only a UUID cannot
be attributed to self. That is this revision's recorded boundary; it under-counts only when
upstream fails to resolve a self-mention to the number, not a false positive. The verdict is
receive-time and never recomputed or backfilled: pre-1.29 rows carry no flag, edits never move
it (the counter counts receipt-time mentions, like the official receipt-time increment), and
the client renders "@you" from `mentionsSelf` + the existing §4.20 ranges without needing the
real address.

**Conversation badge.** `conversations` gains `unread_mentions` (schema 11), a write-side
counter moving in lockstep with `unread_count`: the same receive transaction bumps both (an
incoming row that mentions self and contributes unread bumps both or neither — a crash can
never split the pair), and the same open-chat clear (`conversations.markRead`) zeroes both.
`ConversationSummary` gains `unreadMentions?: number` (skip-when-zero, pre-1.29 hosts read
byte-identical summaries). There is deliberately no account-level mention aggregate — the
official badge is per-conversation too — and no summary read cost: the counter is maintained
at write time, mirroring how `unread_count` already works.

Boundaries, all explicit: only incoming rows can mention self (outgoing/system rows never set
the flag and never touch the counter); a remote-deleted row stays unread, so both counters
keep counting it — the same boundary `unread_count` already has (the official client
decrements on delete; this connector's delete path does not adjust unread, and the @ badge
follows its sibling rather than inventing its own rule); retention pruning removes rows
without touching either counter. The desktop renders the badge from `unreadMentions` alone and
never diffs message lists to maintain it, so receipt upgrades, edits, and reactions cannot
drift it.

### 4.25 messages.search: account-wide local full-text search (contract revision 1.30, 2026-10-02)

The official client searches messages as-you-type (`Signal-Desktop` `MessageSearch`), but it does
so entirely over its **local** database — upstream Signal offers no server-side search endpoint,
and the pinned `signal-cli` jsonRpc surface has none either (§4.23). The connector's store is the
only searchable corpus, so `messages.search` is a bounded read over it: `params` are `accountId`,
`query` (required), `cursor?`, `limit` (1..=200, same clamps as `messages.list`). The result is
`Page<MessageRecord>` — identical row shape to `messages.list`, with reaction aggregates attached
— so the client renders a hit exactly like a thread row.

Semantics, all explicit: the query is trimmed; an empty or whitespace-only query answers an empty
page (the client treats it as "no query" rather than an error); a query over 128 characters is
rejected `INVALID_REQUEST` (the wire schema caps the raw string at 4096 so an oversized paste gets
the semantic error, not a schema violation). Matching is a `LIKE '%term%'` over stored bodies with
`ESCAPE '\'` — `%`, `_`, and `\` in the query are literal. Ordering is `sent_at DESC, id DESC`
across **every conversation in the account**; pagination rides a dedicated `s1:accountId:sentAt:id`
cursor carrying its own sort key (a page still resolves after retention removes a row it pointed
at) and bound to the account alone, since results span conversations. Rows whose body was never
stored (attachment-only or unretrievable) simply never match — `LIKE` over `NULL` is false.

Recorded boundaries: SQLite `LIKE` folds case only for **ASCII** — non-ASCII terms (CJK among
them) match byte-exactly, so a CJK query is case-sensitive by construction (which is also what
the official desktop ships on SQLite, minus its FTS5 index). There is no FTS5 index, no
tokenization, and no relevance ranking this revision: substring over `body` only. Search never
touches upstream, never mutates state, and rides the READ lane (`read` metrics class, not
account-scoped-mutating). `PHASE2_CAPABILITIES` grows by one entry, advertised at handshake.

### 4.26 messages.retryText: same-row resend of a failed send (contract revision 1.31, 2026-10-03)

The official client offers "resend" on a message that definitively failed to send, and the resent
message **replaces** the failed one — the history never grows a second row for the same logical
message. The connector's `clientRequestId` idempotency made the naive desktop-side retry wrong: a
retry that minted a new id inserted a second pending row while the old `failed` row stayed in the
store, so every refresh rendered both (a failed bubble plus its resent twin). `messages.retryText`
makes the retry a first-class connector operation instead: `params` are `accountId`,
`conversationId`, `clientRequestId` (the idempotency key of the ORIGINAL send — persisted rows
always carry it, so both an in-session optimistic row and a reloaded store row address the same
target). The service resolves the row via `message_by_client_request`, guards it, rearms it
in place, and re-dispatches the upstream send rebuilt from the persisted record; the pending id
never changes, so settlement (success / `SEND_OUTCOME_UNKNOWN` / definite failure) completes the
SAME row and the history keeps exactly one row per logical message.

Guards, all explicit: the row must be an outgoing **text** row with a stored `clientRequestId` in
the terminal state `failed` — a missing row answers `MESSAGE_NOT_FOUND`, `pending` answers
`RETRY_IN_FLIGHT` (double-click or a concurrent rearm; the store-level
`WHERE status='failed'` rearm makes the transition atomic), `unknown` answers `RETRY_NOT_ALLOWED`
(its wire outcome was never settled, so a blind resend could double-deliver — the standing rule
"an unknown send outcome must not be retried automatically" extends to user-initiated retries),
incoming/system rows and rows without a clientRequestId answer `INVALID_REQUEST`, and an empty
body answers `INVALID_REQUEST` (attachment retry needs the attachments path and is out of scope
this revision). Rebuilt upstream params carry the persisted body and quote (`quoteTimestamp` /
`quoteAuthor` re-resolved at retry time); staged link previews are not persisted with the row and
therefore do not ride a retry — the wire message goes out plain-text when its original preview
staging is gone, matching the composer-less nature of a retry.

Recorded boundaries: the rearm is one store transaction (`failed -> pending` only), so a status
push that already settled the row between guard and rearm cannot be overwritten; the send lane
(`Lane::Send`, mutating, metrics class `send`) serializes the retry with ordinary sends of the
same account; the row stays `pending` while the upstream call is in flight, and a definite
upstream error flips it straight back to `failed` (the desktop's retry affordance reappears).
There is no automatic retry anywhere in the path — the connector retries nothing by itself; only
an explicit `messages.retryText` may re-dispatch a failed send, and only a definite failure may
be so re-dispatched.

### 4.27 Peer-receipt timestamps + prior-body edit history (contract revision 1.32, 2026-10-03)

Two host-visible data additions, both read-only consequences of events the engine already
delivers — the connector previously projected them away.

**(a) Delivery/read receipt timestamps.** The desktop's message-info page (official
MessageDetail) shows *when* a peer receipt arrived; the wire `message.statusChanged` event
carried only the new `status`, so the desktop could show the ladder but never a timeline.
`ControlReceive::Receipt` now carries `when` — the receipt envelope's own `timestamp` (when the
peer sent the receipt), falling back to connector wall-clock when absent. The monotonic
`upgrade_outgoing_receipts` write stamps `delivered_at` / `read_at` (additive nullable columns,
store schema 12) on the rows the receipt actually moves: first stamp wins (`COALESCE` — a
replayed or later receipt never re-stamps), and no history row is backfilled (receipts only
ever arrive live). The `message.statusChanged` event gains optional `deliveredAt` / `readAt`
(copy of the just-written stamps), and the `MessageRecord` row projection carries the same
optional fields so a reloaded window renders the timeline without re-deriving it. Boundary:
the local "view thread marks it read" badge write is store-local only and stamps nothing —
no peer receipt, no timestamp.

**(b) Prior-body edit history.** The official client keeps every prior body of an edited
message (EditHistoryMessagesModal, newest first). The connector edits overwrote the body in
place, so prior text was unrecoverable. Every accepted edit — inbound peer edit, host-initiated
`messages.edit`, multi-device mirror edit — now snapshots the PRIOR body into a `message_edits`
table (schema 12) inside the same transaction as the overwrite: `{messageId, body, bodyBytes,
editedAt}` where `editedAt` is the replacement time. Bounds: one entry ≤ 4096 bytes
(UTF-8-safe truncation, same budget as the host text preview), at most 20 entries per message
(oldest pruned), `editedAt` ascending = chronological order; the desktop renders newest-first
like the official unshift. The `MessageRecord` row projection gains optional `edits`
(absent on rows never edited), so list reads, search hits and every `message.upserted` carry
the full history without a new method. The table cascades with its message row (foreign key ON
DELETE CASCADE, `foreign_keys=ON` already enforced) and retention passes remove it with the row
— the store never grows edit orphans.

### 4.28 Message pin/unpin + group admin delete (contract revision 1.33, 2026-10-04)

Three additive mutating methods plus three inbound projections, all carrying official protocol
fields the engine's stack already speaks — `DataMessage.pinMessage = 27`, `unpinMessage = 28`,
`adminDelete = 29` (Signal-Desktop v8.31.0-alpha.1 `protos/SignalService.proto`, verified;
behavior aligned with official `SendMessage.preload.ts` / `processDataMessage.preload.ts`).
Official semantics: one pinned message per conversation, `pinDurationSeconds != null` means a
timed pin, absent/null means forever; `unpinMessage` carries only the target identity;
`adminDelete` is group-only (a group admin removing someone else's message; direct chats have no
admin concept).

- `messages.sendPinMessage`: params `accountId`, `conversationId`, `messageId` (required; the
  reaction addressing model §4.6 verbatim), optional `pinDurationSeconds` (u32; absent = forever,
  out-of-range fails closed with `INVALID_REQUEST`), optional `operationId` (shape-validated,
  never persisted). Engine call: `sendPinMessage` with `targetAuthor` per row direction (own
  number for outgoing rows, peer for incoming direct rows, unresolvable group-incoming rows keep
  the §4.6 `INVALID_REQUEST` answer) — the engine resolves it to the 16-byte
  `targetAuthorAciBinary` the wire requires. Groups map to the engine `groupId` form.
- `messages.sendUnpinMessage`: same addressing, no duration. Engine call: `sendUnpinMessage`.
- `messages.sendAdminDelete`: same addressing, group conversations only (`INVALID_REQUEST`
  otherwise). Engine call: `sendAdminDelete`. Target eligibility against admin state is not
  pre-checked — the server is the authority and rejects non-admin attempts; the connector
  surfaces the upstream rejection (official behavior: send and show the failure).
- Result follows the reaction precedent exactly: `{"status": "sent"}` / `{"status": "unknown"}`,
  no automatic retries, same write-lane mutex and delete-drain barrier. Error mapping reuses
  existing codes only. Handshake `capabilities` gains `pin-messages` and `admin-delete`
  (contract-gated discovery, §4.5 precedent).
- Inbound projections: `receive` envelopes now carry optional `pinMessage`
  (`{targetAuthor, targetSentTimestamp, pinDurationSeconds?}` — absent `pinDurationSeconds` =
  forever), `unpinMessage` (`{targetAuthor, targetSentTimestamp}`), and `adminDelete`
  (`{targetAuthor, targetSentTimestamp}`) keys on `dataMessage`, with the reaction targetAuthor
  resolution (E.164 on contacts hit, ACI string otherwise). The connector persists the derived
  per-conversation pinned state (at most one entry; a newer pin replaces the older one; expiry
  from `pinDurationSeconds` is connector-clock computed) and surfaces it on the conversation
  summary as optional `pinnedMessage: {messageId, targetAuthor, targetSentTimestamp,
  pinnedAt, expiresAt?}` so a reloaded window renders the pin bar without replaying events;
  `unpinMessage` and `adminDelete` clear/update that state and persist as row-level markers
  (`adminDeleted` flag renders the official "deleted by admin" tombstone; the row body stays
  for the audit window, retention prunes normally). Store schema 13 (additive columns + one
  table), no data migration.
- Known boundaries: pins are conversation-scoped (the official model) — the backup-archive
  `ChatItem.PinDetails` surface is engine-side import metadata (§6.6) and does not feed this
  state; phone clients older than the official pin rollout ignore the unknown field (proto
  forward-compatibility), so peers without support simply never show the pin.

### 4.29 Outbound read/viewed receipts + send rich-face + group receive parity (contract revision 1.34, 2026-10-04)

The outbound receipt face, the send-side mention face, and the group-receive parity note. All
upstream JSON-RPC extensions below are provided by `kt-signal-engine` (the ADR-0005 replacement
line, behavior pinned to official Signal-Desktop `sendReceipts.preload.ts` /
`SendMessage.preload.ts`); against stock `signal-cli 0.14.8` the new methods fail upstream and
surface through the existing error mapping — a declared swap boundary, not a silent gap.

- `messages.markRead` / `messages.markViewed`: send-lane, account-scoped mutating (reaction
  precedent §4.6 write-lane mutex). Params: `accountId`, `conversationId`, optional `messageIds`
  (bounded 512, incoming rows only — outgoing and system rows are skipped; absent = every
  incoming row of the conversation, ascending `sent_at`, bounded 512). The connector does not
  persist our own read state (the desktop owns its unread state), so absent mode may re-receipt
  rows the desktop already marked — duplicates are protocol-idempotent for the peer. Rows whose
  author cannot be resolved from local data (group rows carrying only the sender hash, §4.28)
  are skipped from the fan-out, not an error. The connector groups the selected rows by author
  and calls the engine `sendReadReceipt` / `sendViewedReceipt` once per author (single-author
  recipient + that author's timestamps; the engine chunks 100 per envelope, official CHUNK_SIZE).
  Result follows the reaction precedent: `{"status": "sent"}` / `{"status": "unknown"}` — a
  receipt send failure never fails the request, and unknown outcomes are never retried
  (AGENTS.md). Zero eligible rows is a trivial `{"status": "sent"}` no-op. Handshake
  `capabilities` gains `send-receipts`.
- Auto delivery receipts: for every incoming envelope the connector best-effort calls the engine
  `sendDeliveryReceipt` (author, [sent_at]) after the row lands — official receiving-client
  behavior, no user setting, no retry, fully silent (failures change nothing).
- `messages.sendText` gains optional `mentions: [{number, start, length}]` (bounded 64, the §4.20
  receive caps mirrored; `number` resolves contacts-first, ACI passthrough; offsets are UTF-16
  code units, the official BodyRange semantics — the caller indexes its own body). Engine side
  converts to `BodyRange.MentionAci`. Bounds violations drop the entry; a fully malformed array
  fails closed `INVALID_REQUEST`.
- Upstream rich-face parity notes (no connector change): `quoteTimestamp`/`quoteAuthor`
  (contract 1.13) and `previewUrl`/`previewTitle`/`previewDescription` (§4.21) now take real
  effect on the engine wire face — quote `text` is enriched engine-side from the engine's own
  store (official/`signal-cli` behavior; when the quoted body is unknown the quote rides without
  text, the official degrade path), and `previewImage` (same data-URI shape as `attachments`)
  uploads with the attachment batch and lands as the official `Preview.image` (engine
  2026-10-04; before that face imageless previews rode protocol-valid).
- Group receive parity: the engine now projects the signal-cli-shaped `groupInfo {groupId}` on
  inbound group `dataMessage` / `sentMessage` (master-key base64, the exact shape
  `data_message_group_id` already consumes), so group receives land through the unchanged
  connector pipeline — the earlier "engine skips groups" note is void. Group conversation
  skeletons remain progressive-fill (first group message); direct-chat skeletons are covered by
  the engine's contacts sync on link/restart (its receive pipeline runs one
  `request_contacts` round per account, warn-only, 60s one-shot — no connector change).

### 4.30 Outbound voice notes on messages.attachments.send (contract revision 1.35, 2026-10-04)

`messages.attachments.send` (§4.12) gains an optional `voiceNote: boolean` (default false). When
true the connector's `build_attachment_data_uri` appends the `;voice=true` data-URI parameter and
the engine maps it to the official `AttachmentPointer.flags = VOICE_MESSAGE` — the exact wire
shape the official desktop recorder emits (`audioRecorder.preload.ts`: attachment with
`flags=VOICE_MESSAGE`, contentType `audio/mpeg`, no separate message body; receiving clients'
`isVoiceMessage` judges by the flag first). Validation: `voiceNote` requires `contentType` to be
`audio/*` (the official recorder only produces audio; anything else is `INVALID_REQUEST` before
the pending row exists). The sent row's attachment descriptor records `isVoiceNote: true`, so
the sender's own bubble renders the voice UI across restarts.

Receive side is already contract-complete: the inbound attachment descriptor has carried
`isVoiceNote` since contract 1.15 and the engine projects it from the pointer flags — zero
receive-plane change. Waveform rendering is a desktop concern (the official client derives it
from the decoded audio; no protocol surface). No new method, no schema change, no capability
entry — `attachments.send` callers discover the face from the updated schema description.

### 4.31 Stickers: messages.sendSticker + receive projection (contract revision 1.35, 2026-10-04)

Official send face (`sendStickerMessage`): `DataMessage.sticker {packId, packKey, stickerId,
emoji, data: AttachmentPointer}` with `body` undefined, no regular attachments, mutually
exclusive with text/attachments/previews/mentions, quote allowed to co-ride. The pinned
signal-cli JSON-RPC has no sticker method (sticker faces are CLI-command-only), so this is an
engine-extension method mirrored 1:1 by the connector.

**`messages.sendSticker`** (new host method, Send lane, mutating, §4.12 settlement discipline:
pending row → upstream → `complete_send_success` / `complete_send_unknown`, no auto-retry,
advertised through handshake `capabilities` as `send-sticker`): params `accountId`,
`conversationId` **or** `kind`+`peerKey` addressing (exactly one form), `clientRequestId`,
`packId` (hex, even-length, 1–64 chars — official pack ids serialize hex), `packKey` (standard
base64, 1–128 chars — official `Bytes.fromBase64` convention), `stickerId` (u32), `emoji?`
(≤ 32 chars), `image` `{dataBase64, sizeBytes, contentType, width?, height?}` — the same budget
and validation family as §4.12 (≤ 100 MiB declared+validated, path-separator/control-char
rejection n/a here since no filename) with one sticker-specific rule: `contentType` must be
`image/*` (the official client sniffs image MIME and falls back to `image/webp`; it refuses
`video/*` and `text/*`). The connector composes the engine `send` call with the `sticker`
object (packId/packKey passthrough after shape validation, image re-encoded data URI,
width/height passthrough — official `AttachmentPointer` display metadata) and an empty body.
The sent row's descriptor records the sticker image attachment plus a `sticker` metadata field
(packId/packKey/stickerId/emoji) so the sender's bubble renders as a sticker across restarts.

**Receive projection**: the engine projects inbound `DataMessage.sticker` as
`sticker {packId(hex), packKey(base64), stickerId, emoji?, data}` (official serialization
conventions). The connector normalizes it onto the row: the sticker metadata persists as an
additive JSON column (`messages.sticker_json`, schema 13 → 14) and `MessageRecord` gains an
optional `sticker` field (`skip_serializing_if` absent — pre-1.35 rows stay byte-identical).
The `data` pointer rides the existing metadata-only attachment descriptor list (contract 1.15
shape, bounded 32) so byte fetch goes through the existing media channel — the connector never
downloads sticker bytes (§6.7 boundary unchanged). Reactions/replies/remote-delete on sticker
rows behave exactly like attachment rows (the sticker is body-less metadata, not a special row
kind). A sticker on a message that also carries `pinMessage`/`unpinMessage`/`adminDelete`
control surfaces keeps working — the control plane is orthogonal (§4.28).

Bounds failures are deterministic `INVALID_REQUEST` before the pending row exists; unknown
mutating outcomes answer `SEND_OUTCOME_UNKNOWN` and are never retried.

### 4.32 Sticker pack browsing: getStickerPackManifest + getStickerImage (contract revision 1.36, 2026-10-04)

The browse face of contract 1.36: two additive Read-lane host methods through which the desktop
fetches a sticker pack's manifest and one sticker's decrypted bytes on demand. Neither method
takes an `accountId` — pack browsing is anonymous CDN traffic in the official client, entirely
outside any linked account's session — and neither reads or writes connector state: no row, no
cache, no event, no persistence. Both are engine-extension methods (the pinned signal-cli
JSON-RPC has no sticker-pack face) forwarded 1:1 to the engine, which performs all network and
crypto work.

Official behavior reference (file-level): `ts/textsecure/WebAPI.preload.ts` (anonymous CDN GET
of `manifest.proto`), `ts/Crypto.node.ts` (HKDF `"Sticker Pack"` → AES-256-CBC + HMAC-SHA256
verify/decrypt), `ts/types/Stickers.preload.ts` (manifest decode, `isPackIdValid`, and the 200
sticker display window). The engine owns the whole pipeline: anonymous CDN GET, digest
verification, decryption, proto decode. The connector never touches key material beyond passing
the pack key through as a parameter — the key exists on the wire only as the base64 the host
supplied, is never persisted, never logged, and never leaves the request scope (the §6.7
boundary's crypto wording, kept verbatim: bytes and keys both stay engine-side).

- `getStickerPackManifest`: params `packId` (exactly 32 lowercase-or-uppercase hex characters),
  `packKey` (standard base64 that decodes to exactly 32 bytes). Result `{title, author, cover,
  stickers, stickerCount}` where `cover` is `{id, emoji?, contentType?} | null` and `stickers`
  is `[{id, emoji?, contentType?}]`. Bounds: manifest entries are capped at 1024 — the official
  decoder truncates over-long proto lists, the connector mirrors that truncation semantics
  rather than rejecting (official behavior); `title`/`author` are ≤ 256 characters (KT bounds —
  the official decoder applies no length validation; the connector keeps the early
  `INVALID_REQUEST` fail-closed guard instead of passing absurd strings through). The 32-hex
  packId is deliberately stricter than the 1.35 sendSticker receive projection (even-length hex
  ≤ 64): the browse face aligns with the official `isPackIdValid`, which accepts exactly 32 hex
  characters for pack ids minted by the official sticker creators.
- `getStickerImage`: params `packId` (same 32-hex rule), `packKey` (same 32-byte rule),
  `stickerId` (u32). Result `{dataBase64, contentType, size}` where `dataBase64` is the
  decrypted standard-base64 image (≤ 400 KB encoded — the official sticker CDN caps uploads at
  300 KB, and 4 × ceil(307200 / 3) = 409600 characters covers the base64 expansion with margin),
  `contentType` is the manifest/sniffed image media type, `size` the decoded byte count. The
  decoded bytes exist only inside the engine for the lifetime of the response; the connector
  persists nothing and never decodes or re-encodes beyond the pass-through.

Lane classification: both methods are **Read lane, non-mutating** — CDN fetches mutate no
upstream state, so no account mutex, no delete barrier, no pending-row settlement, and a
timeout answers the ordinary retryable `UPSTREAM_TIMEOUT` (unlike mutating calls, a read that
did not answer provably took no effect). Error mapping: engine error codes surface through the
structured connector codes — `STICKER_PACK_KEY_INVALID` / `STICKER_PACK_FETCH_FAILED` /
`STICKER_PACK_MALFORMED` / `STICKER_IMAGE_TOO_LARGE` map to the connector's
`STICKER_PACK_KEY_INVALID` / `STICKER_PACK_FETCH_FAILED` / `STICKER_PACK_MALFORMED` /
`STICKER_IMAGE_TOO_LARGE` (`retryable=false` for key-invalid and too-large,
`retryable=true` for fetch-failed and malformed — a malformed upstream proto may be fixed by a
refetch; an invalid key never succeeds on retry), engine-down answers the existing
`RUNTIME_NOT_RUNNING`, and an old engine without the method answers the existing
`CAPABILITY_UNAVAILABLE` (same "engine does not support this face" mapping the §4.29
engine-extension family uses; desktops gate on the handshake `capabilities` tag
`sticker-pack-browse`, contract-gated discovery per §4.5 precedent).

### 4.33 Conversation pin sync: getPinnedConversations + setConversationPinned (contract revision 1.36, 2026-10-04)

The conversation-level pin face of contract 1.36: two host methods that read and write the
official conversation-pinning state. Source of truth is the Signal Storage Service
`AccountRecord.pinnedConversations` (official mechanism, `ts/services/storage.preload.ts`):
the upstream stores an ordered list of conversation references, array order = pin order (no
timestamps — pinnedAt ordering is a desktop render concern derived from list position), and the
storage service write path is the same one the official desktop uses when the user pins or
unpins a chat.

- `getPinnedConversations`: params `accountId`. Result `{pinned:
  [{conversationId, kind}]}` with `kind` the three-value enum `contact` | `group` |
  `legacyGroup` (GroupsV1), order exactly the cloud order. Read lane, non-mutating.
- `setConversationPinned`: params `accountId`, `conversationId`, `kind` (same enum), `pinned`
  (boolean, required). Result: the same `{pinned: [...]}` projection read back after the
  upstream write, so the host sees the post-write cloud state — including the server-applied
  ordering — instead of a local guess. Send lane, account-scoped mutating (the reaction-class
  write-lane mutex and delete drain barrier verbatim; an unknown mutating outcome answers
  `SEND_OUTCOME_UNKNOWN` and is never retried, per AGENTS.md).

Connector responsibilities are strictly bounded: forward, bounds-check, and resolve the
conversation identity and account context. `conversationId` resolves through the existing
ladder (§4.4 wire order: account, then conversation — `CONVERSATION_NOT_FOUND` before any
upstream call) so the connector maps the host's opaque id to the upstream conversation
reference the engine needs, mirroring the conversationId resolution every other method uses;
`kind` is validated against the three-value enum; `accountId` resolves the owning proxy group
and engine through the normal routing. The connector does not interpret pin semantics and adds
no second ordering.

Bounds (early `INVALID_REQUEST`, deterministic, before any upstream call): `conversationId` ≤
256 characters (a conversation reference bound, wider than the local opaqueId family because
the host may address a conversation it has not yet materialized locally), `kind` enum as above,
`pinned` required boolean, and the returned `pinned` list is capped at 128 entries (KT bound —
the official clients apply no explicit cap; the connector truncates beyond 128 to keep one
host frame bounded and records the truncation in the §5 boundary notes). Engine errors pass
through structurally: `CONVERSATION_NOT_RESOLVED` maps to the connector's
`CONVERSATION_NOT_RESOLVED` (`retryable=false` — the cloud reference does not resolve against
the account's storage state), storage-service failures surface under the engine's
`STORAGE_*` codes verbatim (`retryable=true`; the connector neither swallows nor invents
storage codes — the engine owns their final set), and engine-down / method-absent follow the
§4.32 mapping (`RUNTIME_NOT_RUNNING` / `CAPABILITY_UNAVAILABLE`). Handshake `capabilities`
gains `conversation-pin-sync`.

Orthogonality and live-push boundaries: message-level pins (contract 1.33 `pinMessage` /
`unpinMessage`, §4.28 — at most one pinned *message* per conversation, projected onto the
conversation summary) and conversation-level pins (this face — ordered list of pinned
*conversations* in the account record) are two unrelated protocol concepts that merely share
the word; neither feeds the other, and the §4.28 `pinnedMessage` summary field is untouched by
this face. Live push is out of scope for this face: the connector does not subscribe to storage-service
pin writes, and the engine projects no inbound pin event; the host pulls with
`getPinnedConversations` at startup/refresh (poll-on-open replaces push). The connector
persists no pinned list — the cloud record plus the desktop's own cache is the whole story; no
new on-disk face, no store schema change.

### 4.34 Sticker pack sync: stickerPacks.getSyncs + stickerPacks.setSync (contract revision 1.37, 2026-10-04)

The account-scoped sticker-pack install face of contract 1.37: two host methods that read and
write the official per-account sticker-pack sync records. Source of truth is the Signal Storage
Service `StickerPackRecord` set (official mechanism): an installed pack is the record
`packId + packKey + position`, an uninstall is the tombstone `packId + deletedAtTimestamp`
(the engine stamps its own clock), and a re-install/update inserts the new key while the old
key's record is deleted — the connector sees only the projected record list and adds no
semantics of its own. Unlike the §4.32 browse face these records are account state: they live
in the account's storage session and ride the account's engine.

- `stickerPacks.getSyncs`: params `accountId`. Result `{packs: [{packId, packKey|null,
  position|null, deletedAtTimestampMs|null}]}` — installed packs carry key and position,
  uninstalled packs the tombstone timestamp. Read lane, non-mutating.
- `stickerPacks.setSync`: params `accountId`, `packId`, `packKey` (optional), `installed`
  (boolean, required), `position` (optional u32). Result: the same `{packs: [...]}` projection
  read back after the upstream write, so the host sees the post-write cloud state instead of a
  local echo. Send lane, account-scoped mutating (the reaction-class write-lane mutex and
  delete drain barrier verbatim; an unknown mutating outcome answers `SEND_OUTCOME_UNKNOWN`
  and is never retried, per AGENTS.md).

Engine wire (frozen shapes, kt-signal-engine): `getStickerPackSyncs {account}` and
`setStickerPackSync {account, packId, packKey|null, installed, position|null}`. Engine
semantics: `installed=true` must carry a valid pack key (exactly 32 bytes); `installed=false`
ignores `packKey`/`position` entirely and writes the tombstone with the engine's clock — the
connector forwards null for both in that case, so the wire shape is deterministic regardless of
what the caller supplied.

Connector responsibilities are strictly bounded: forward, bounds-check, and resolve the account
context (the account maps to its owning proxy group and engine through the normal routing; the
upstream key is the resolved `signal_account`, never the host's opaque id). The connector does
not interpret install semantics, keeps no pack list, and persists nothing — the cloud record
plus the desktop's own cache is the whole story; no new on-disk face, no store schema change.

Bounds (early `INVALID_REQUEST`, deterministic, before any upstream call): `packId` exactly 32
hex characters (the §4.32 browse-face shape, `isPackIdValid`-aligned), `packKey` when present
standard base64 decoding to exactly 32 bytes (44 padded characters), `installed` required
boolean, `position` within the u32 range (out-of-range values fail at params deserialization),
and the returned `packs` list is capped at 256 entries (KT bound — the official clients apply
no explicit cap; the connector truncates beyond 256 to keep one host frame bounded and records
the truncation in the §5 boundary notes). Entries missing the identity field (`packId`) drop;
the optional fields project leniently beside it. Engine errors pass through structurally: the
`STORAGE_*` family (`STORAGE_UNAVAILABLE` / `STORAGE_READ_FAILED` / `STORAGE_WRITE_FAILED`)
maps verbatim with `retryable=true` — the allowlist and schema already declare these codes, the
§4.4 classification tests stay total over the same set, and no new codes are introduced.
Engine-down and method-absent follow the §4.32 mapping (`RUNTIME_NOT_RUNNING` /
`CAPABILITY_UNAVAILABLE`). Handshake `capabilities` gains `sticker-pack-sync`.

### 4.35 Sticker reply: sendSticker quote coexistence (contract revision 1.37, 2026-10-04)

`messages.sendSticker` gains the optional `quoteMessageId` parameter: a sticker message that
quotes one earlier message in the same conversation — the official `StagedStickerReply` shape
(one upstream `send` carrying the sticker object and the quote identity together). The receive
side needs no new face: an incoming sticker with a quote already lands as the ordinary sticker
projection beside the ordinary quote projection (both engine normalizer fields have been
independent since 1.15/1.31), so this section is send-side only.

Resolution reuses the `dispatch_send` ladder verbatim (§4.4 wire order, no new errors): the
quote targets a row in the same conversation; an unknown target answers `MESSAGE_NOT_FOUND`;
a target whose Signal author is not established (unaddressable outgoing state, group incoming
row, system row) answers the deterministic `INVALID_REQUEST`. The upstream keys are
`quoteTimestamp` (the quoted row's Signal timestamp) and `quoteAuthor` at the top level of the
`send` params beside the `sticker` object — the same wire keys the text face uses, because the
quote belongs to the send, not to the sticker. Absent `quoteMessageId` is exactly the 1.35
behavior (an unquoted sticker).

Ordering discipline (a quote is part of send validation): resolution runs after the
idempotency short-circuit and addressing resolution and before the pending row is inserted — a
rejected quote leaves nothing behind, so the same `clientRequestId` stays a fresh
(re-validated) request, while a replay of an already-inserted row keeps returning the existing
row without re-resolving. The pending row records `quoteMessageId`; lane, account-scoped
mutating classification, and §4.12 settlement (upstream-timestamp completion,
`SEND_OUTCOME_UNKNOWN` with no automatic retry, failure marks the same row) are the 1.35
`sendSticker` discipline verbatim. Bounds: `quoteMessageId` is the `opaqueId` family, the same
schema bound the text face carries.

### 4.36 Outbound view-once: attachments.send `viewOnce` + messages.sendViewOnceOpen (contract revision 1.38, 2026-10-04)

The send-side view-once face: a view-once media message our client composes, plus the bare
open-notification passthrough the official `sendViewOnceOpenSync` mirrors. Upstream extensions
are provided by `kt-signal-engine` (behavior pinned to official Signal-Desktop view-once send
semantics); against stock `signal-cli 0.14.8` the new keys and method fail upstream and surface
through the existing error mapping — the declared swap boundary of §4.29.

- `messages.attachments.send` (§4.12) gains an optional `viewOnce: boolean` (default false).
  Constraints, all deterministic `INVALID_REQUEST` before the pending row exists: `viewOnce`
  requires `contentType` to be `image/*` or `video/*` (the official view-once faces are photos
  and videos only), requires `caption` to be empty, forbids `quoteMessageId`, and forbids
  `voiceNote` (audio is never view-once; the single-attachment structure of §4.12 already makes
  the combination unambiguous). A validated send projects a pending row whose rich record carries
  `viewOnce: true`, so the sender's own bubble renders the view-once UI across restarts, and the
  upstream `send` params carry `"viewOnce": true` at the top level beside the attachment data URI
  (the key is absent when the flag is false — the wire shape stays byte-identical to the 1.13
  send for every existing caller). Lane, account-scoped mutating classification, and §4.12
  settlement are unchanged.
- `messages.sendViewOnceOpen`: params `accountId`, `senderAci` (`opaqueId` family), `timestamp`
  (u64 ms). Send lane, account-scoped mutating (the reaction-class write-lane mutex and delete
  drain barrier verbatim). The connector resolves the account (the upstream key is the resolved
  `signal_account`, never the host's opaque id) and forwards `{account, senderAci, timestamp}`
  to the engine `sendViewOnceOpen`. Result follows the receipt precedent (§4.29):
  `{"status": "sent"}` / `{"status": "unknown"}` — any upstream failure (engine down, method
  absent, unknown outcome) degrades to `"unknown"` and is never retried (AGENTS.md). Bounds
  (early `INVALID_REQUEST`, before any upstream call): `senderAci` non-empty, bounded by the
  `opaqueId` length family; `timestamp` is u64 (out-of-range fails at params deserialization).
  `ACCOUNT_NOT_FOUND` keeps its existing meaning when the account does not resolve.

Handshake `capabilities` gains `view-once`. This face is a bare passthrough: the connector adds
no queue, no dedup, and no persistence of its own. The desktop-facing orchestration that wraps
it lives in §4.37.

### 4.37 View-once burn: viewOnceOpen consumption + erasure + messages.markViewOnceOpened (contract revision 1.38, 2026-10-04)

The receive-side view-once face: consuming a peer's open notification, erasing the viewed bytes,
and the desktop-facing trigger that orchestrates both legs of the official view-once flow. The
engine (kt-signal-engine) already projects the official `syncMessage.viewOnceOpen
{senderAci, timestamp}` — the sender's client notifies every linked device when the recipient
viewed the message — and re-delivers the same sync envelope once per resync cycle, so exactly-once
is the connector's job.

- Receive projection: the engine's sync branch parses `viewOnceOpen`; a missing `senderAci`
  drops the envelope; an absent `timestamp` normalizes to 0 (the "unknown" marker). The
  connector receives it as a new control receive kind `viewOnceOpen` (direction "incoming",
  no session routing) and intercepts it in the control pipeline after account resolution but
  before the `(kind, peer_key)` conversation routing — the sync envelope's own source is our
  account, so generic routing would misroute it into a self-conversation.
- Target ladder: `timestamp == 0` warns and drops. The candidate peer-key list is the literal
  `senderAci` plus every stored identity that maps to it (a new bounded `peer_identities` table
  captures the ACI→peer-key mapping from ingest from 1.38 forward — one row per ACI,
  primary-keyed `(account_id, aci)`, written only when an incoming envelope's `sourceUuid`
  differs from the stored peer key; rows before 1.38 rely on the ladder's fallbacks). The
  candidate query joins conversations, caps at 16 rows (incoming-conversation rows first), and
  takes the first row whose rich record carries `viewOnce: true`. No match warns and drops —
  warn-only, no state, no error: the official clients stage open syncs until the message
  arrives, this connector deliberately does not (no ViewOnceOpenSyncs staging; a sync that
  arrives out of order is dropped and the sender's resync cycle re-delivers it).
- Idempotency and the event: the first transition marks the row opened, clears the body bytes
  (the row's projection downgrades to a viewed placeholder — the bytes are no longer available),
  deletes the media file and preview under the connector-owned attachment directories, closes
  any open media handle for the message, and emits exactly one new host event
  `message.viewOnceOpened {accountId, conversationId, messageId, openedAt}`. Duplicate sync
  envelopes (the engine's pinned-rev resync delivers the same envelope twice) find no
  transition and emit nothing. The event is a local-viewed notice, deliberately not folded into
  `message.statusChanged` (that carries peer receipts, not our own view state).
- `messages.markViewOnceOpened`: params `accountId`, `conversationId`, `messageId`. The
  connector resolves the target through the standard ladder (missing row `MESSAGE_NOT_FOUND`).
  A row that is not a view-once message (`rich.viewOnce` absent — valid or invalid-flagged rows
  both qualify) answers deterministic `INVALID_REQUEST`. The burn runs first: on a fresh
  transition it erases the row bytes and media exactly as above and emits
  `message.viewOnceOpened`; on a replay (already opened) it is a trivial `{"status": "sent"}`
  no-op with no event and no upstream fan-out. After a successful burn the connector
  best-effort sends the official notification pair, in the official order, under the §4.29
  receipt discipline (any failure degrades `{"status": "unknown"}`, never retried; the burn and
  the event are unaffected): for an incoming row with a resolvable author, engine
  `sendViewedReceipt {recipient, timestamps: [sentAt]}`; then engine `sendViewOnceOpen
  {senderAci, timestamp}` where `senderAci` is the author for incoming rows and our own account
  for outgoing rows (the engine resolves the registered number to the own ACI). A group row
  whose author cannot be resolved skips the receipt leg (the §4.28 boundary) but still burns.
- Orchestration ownership (the §4.4 division, recorded): `messages.markViewOnceOpened` is the
  desktop's single trigger point; the connector internally orchestrates the VIEWED receipt and
  the ViewOnceOpen notification in the official order. The desktop does not need to call
  `messages.markViewed` or `messages.sendViewOnceOpen` itself — a stray call is harmless
  (protocol-idempotent) but sits outside the desktop flow; the bare faces of §4.29/§4.36 remain
  available.
- Media reads: `attachments.open` on a view-once row whose `viewOnceOpenedAt` is set answers
  `UPSTREAM_ERROR` with the exact wording of the not-downloaded path, so an opened view-once
  attachment is indistinguishable from one the engine never fetched — no new error surface
  leaks the state transition. `viewOnceInvalid: true` rows (the metadata-only §4.20 marker) are
  included in byte erasure the same way.

No store schema-version bump: the `peer_identities` table is additive (`CREATE TABLE IF NOT
EXISTS` runs on every open), and the row-level erasure reuses the existing rich/body columns.
Bounds stay structural: candidate cap 16, peer-key list bounded by the contacts table, media
deletion reuses the §4.12-best-effort discipline (missing files are success).

### 4.38 Link-time history import: backup5 archive consumption + history.importStatus (contract revision 1.39, 2026-10-04)

The connector side of kt-signal-engine ADR 0005 S3: consuming the NDJSON export the engine's S2
stage streams after a `backup5` link, importing it into the store under the same identity and
bounded-batch discipline as live traffic, and exposing an honest status face for the desktop's
S4 import UI. The engine face is file-based by design (engine 0eedc8c: "the consumer decides by
file presence") — no engine JSON-RPC method or event changes, so the connector consumes a
documented data-directory artifact instead of an RPC surface.

- Engine face (consumption contract, complementing the layout already recorded in §5): after a
  `backup5` link the engine best-effort downloads and decrypts the transfer archive and streams
  `history-import.ndjson` into the account's store directory (`<engine data-dir>/accounts/<dir>/`,
  with `<dir>` recorded per number in the engine's `engine-state.json` registry, compat §1).
  Export failure leaves the file absent; an engine crash mid-export can leave a partial file
  (no completion marker exists on the v1 face). The connector parses the one-JSON-object-per-line
  records — `recipient {id, kind: contact|group|self|other, name, aci?, e164?, masterKey?}`,
  `chat {id, recipientId}`, `message {chatId, authorId, dateSent, direction, text?, attachments?,
  quote?, remoteDeleted?, pinnedAtTimestamp?, ...}` — tolerantly: malformed lines, unknown types,
  and over-long lines are counted and skipped, never fatal. The recipient-level service identity
  fields (`aci`, `e164`, `masterKey`) are required for conversation attribution and are NOT yet
  projected by the engine's S2 export (kt-signal-engine gap, registered for the engine-side S3
  task): until the engine lands them, every import run completes with all chats skip-counted
  (`skippedChats`), which the status face reports honestly. The connector consumes the fields
  when present, so the engine addition needs no further connector change.
- Decision — independent status method, not `link.finish` fields: the engine's archive download
  starts in the background when `finishLink` returns (long-poll plus CDN transfer, minutes) and
  is best-effort, so `link.finish` cannot carry the import outcome without lying or blocking;
  `link.finish`'s result is the stable account summary with its own non-retryable outcome
  discipline (`LINK_OUTCOME_UNKNOWN`), while the import is an idempotent, retryable background
  task with a different lifecycle. The official desktop has the same shape: linking completes
  first, the history import runs behind the main UI. The status method gives the S4 UI the
  pending → running → completed/failed progression it needs; there is deliberately no new event
  in 1.39 — the desktop polls `history.importStatus` while the state is `running` (the UI
  requirement is one number ticking, not low-latency delivery).
- `history.importStatus` params `{accountId}`; result `{accountId, state, ...}` with state
  `unavailable` (`reason: "engine-mode"` under signal-cli JVM/native, which has no backup5
  capability at all; `reason: "no-archive"` when the link-time wait window has expired with no
  archive produced — the engine's best-effort failure modes are indistinguishable from "the
  phone had nothing to transfer" and are reported as such, never guessed), `pending` (linked,
  inside the wait window, archive not on disk yet), `running` / `completed` / `failed` (from the
  persisted import row, which carries `importedMessages`, `skippedLines`, `skippedChats`,
  `skippedMessages`, `attempts`, `errorClass`, `startedAt`, `updatedAt`). Unknown account:
  `ACCOUNT_NOT_FOUND`. The method is read-lane, read metrics class; no new error codes.
- Store: one `history_imports` row per account (schema version 15, FK cascade with the account),
  plus the bounded import writer. Message rows are written with exactly the live-receive
  identity — the message id is the same `signal-message-v2` stable hash over
  `(account, conversation, direction, sent_at, sender)`, `sent_at` being the Signal timestamp
  the archive carries (`dateSent`) — so `INSERT OR IGNORE` deduplicates against rows that live
  traffic already wrote (or will write): re-running an import, or a later live envelope for the
  same message, lands on the same identity and inserts nothing. Chats attach through
  `ensure_conversation`, so a contacts-sync skeleton (§6.5) is filled in place — no second
  conversation row; the title keeps the §6.5 upgrade policy.
- Bounded discipline (ADR 0002): the archive is read as a line stream with a hard per-line cap;
  pass 1 stages recipient/chat identity maps into bounded SQLite temp tables (dropped at the
  end), pass 2 streams messages in fixed-size batches (256 rows per transaction, store lock
  released between batches on the blocking pool). Nothing proportional to the archive is held in
  memory. Imported rows do not bump unread badges and emit no per-message host events (a
  100k-message import would otherwise flood the host lane); the desktop refreshes
  conversations/messages after the status reaches a terminal state. Imported text follows the
  receive size ladder (oversized bodies keep only the truncated preview, marked not-retrievable —
  there is no engine copy to fetch); attachment descriptors are metadata-only mirrors of §4.13
  (their ids are synthetic: opening one answers the normal not-found failure — no bytes were
  ever downloaded, §6.7). Quotes resolve their target row when it is already imported or live
  (click-to-scroll works) and always carry the bounded snapshot. `remoteDeleted` items import as
  `remote-deleted` tombstone rows; pinned items import into the conversation-pins table;
  direction-less items, long-text-only bodies (metadata-only overflow), and messages whose
  author/chat cannot be resolved to a stored peer identity are skip-counted. Imported rows carry
  fresh `stored_at`, so the §7.1.1 age rule treats them like any other local copy; one bounded
  retention pass runs after a completed import so the store converges to the per-conversation
  cap within the same session.
- Orchestration: at most one import runs per group at a time. After `link.finish` succeeds in
  kt-engine mode, a bounded watcher polls for the archive (every few seconds, at most ten
  minutes — the engine's own transfer long-poll plus transfer budget) and starts the import when
  the file appears; once per supervisor start, a sweep imports any account whose archive exists
  but has no completed row (connector restarted between link and import, or a previous attempt
  failed). A failed run is retried on a later start up to three attempts, then stays `failed`
  with its error class until the account is deleted or re-linked (only-first-link semantics,
  ADR 0005 §风险与边界). The connector never deletes or rewrites the engine's archive file; the
  completed row in the connector store is the consumption marker.

### 4.39 Group member roster: groups.get `members` (contract revision 1.40, 2026-10-05)

The official conversation-info page and the composer @-mention picker both render the group's
member roster. The §4.8 projection carries only `memberCount` — the sync captures the upstream
member list solely to count it (supervisor `sync_contacts_with`, contract 1.9) — so the desktop
has no identity, display-name, self, or admin information for any member. This revision extends
`groups.get` instead of adding a second method: the roster belongs to the same cached row, the
same read-lane, and the same `syncedAt` staleness story; a separate `groups.getMembers` would
duplicate the targeting, the error family, and the cache-read classification for no new
capability.

- Decision — capture at sync, resolve at read: `contacts.sync` keeps the upstream member array
  it already walks, this time inside the group row's `extra` JSON beside `memberCount`
  (`{"memberCount": N, "members": [{"id", "uuid"?, "admin"?}, ...]}` — no store schema-version
  bump; `extra` is the additive channel §4.8 already uses). `groups.get` stays upstream-free
  (§4.8) and projects the array, resolving each member's display name at read time from the same
  account's cached `kind='contact'` rows — one lookup per response, no name copy in the roster
  row, so a name the next sync improves is reflected without a roster rewrite. The result gains
  an optional `members` array; rows synced before this revision carry no `members` key, and the
  response stays byte-identical for them.
- Member shape (the official member row's minimal set): `id` — the member address with the
  connector-wide first-wins convention (`number` before `uuid`, the same ladder the contacts
  sync and the §4.20 mention author use, so roster ids join against contact peer keys, message
  senders, and mention authors without a second identity mapping); `uuid` — present only when
  the upstream supplied it separately (the pinned signal-cli 0.14.8 member record does;
  absent otherwise); `name` — the contacts-cache title, absent when the member is not in the
  cache (a group-only or not-yet-synced peer) — the pinned upstream member records carry no name
  field of their own, so there is no engine raw name to fall back to and none is invented;
  `self` — true when `id` equals the linked account's number (number-based, the §4.24 recorded
  boundary: the own ACI is not queryable upstream, an ACI-keyed roster entry cannot be marked);
  `admin` — present only when the upstream marks the member: pinned signal-cli ships both a
  per-member `isAdmin` flag and a separate group-level `admins` address set (verified in the
  0.14.8 distribution jar, `ListGroupsCommand$JsonGroupMember` / `$JsonGroup`), so the connector
  records `admin: true` when either names the member. `false`/absent collapses to an absent key,
  like `mentionsSelf` (§4.29).
- Engine-mode projection difference (recorded, not worked around): `kt-signal-engine`'s
  `listGroups` returns `members: [{number}]` — one address per member (number first, the ACI
  string where the contacts map has no number), no separate uuid, no admin face. In that mode
  roster entries carry `id` only (plus a read-time `name` when the cache knows the number), and
  `admin`/`uuid` are honestly absent. Neither engine's member record exposes the official
  member label/emoji badges; they are not projected.
- Bounds: the roster cap is the official GroupsV2 group-size ceiling, 1001 members
  (`MAX_GROUP_MEMBERS`); entries past the cap drop, entries with no address drop individually,
  and every address is capped at 128 chars — a hostile upstream payload can never fail the sync
  or the read (the §4.13 discipline). `memberCount` keeps its existing meaning (the upstream
  array length), even in the pathological case where it exceeds the projected roster. The
  roster lives in the row the sync already writes, so its store cost is bounded by the groups
  the account is actually in at upstream roster scale.

### 4.40 Mention author ACI projection (contract revision 1.40, 2026-10-05)

The premise check this revision records first: the mission's "mention ranges are not persisted"
no longer holds — §4.20 (contract 1.25) already persists the normalized mention ranges on the
row (`messages.rich_json`, schema 9) and projects them on the wire row (`mentions:
[{author, name?, start, length}]`, flattened), and both engines deliver them inbound: the pinned
signal-cli 0.14.8 jsonRpc receive carries `JsonDataMessage.mentions` (`JsonMention{name, number,
uuid, start, length}`, verified in the distribution jar), and kt-signal-engine projects the
protocol `body_ranges` MentionAci entries as `{"number": <aci>, start, length}` (≤64, the P2
receive-projection face, engine `3ac097a`). The §4.29 `mentionsSelf` marker and the §4.24 badge
ride those same ranges. What is genuinely missing is the *canonical ACI*: official
Signal-Desktop body ranges key the mentioned identity by ACI (`mentionId`), while the
connector's `author` value is engine-dependent — signal-cli mode resolves it number-first
(an ACI only when the number is unknown), engine mode puts the ACI into the same `author`
string with no way for a host to tell which identity family it is reading.

- `mentions[].authorAci?: string` (wire row field, contract 1.40): present only when the ACI is
  honestly known — the envelope's `uuid` field (signal-cli mode), or the `number`-keyed value
  when it is UUID-shaped (engine mode, where that value *is* the ACI by construction). `author`
  keeps its exact pre-1.40 value and the number-first resolution, so every existing row and
  consumer is byte-identical; `authorAci` is purely additive. A number that merely looks short
  or malformed never promotes — only the strict UUID grammar (`8-4-4-4-12` hex, case-insensitive)
  counts.
- Persistence rides the existing `NormalizedRich` JSON in `messages.rich_json`: new rows carry
  `authorAci` inside their mention entries, pre-1.40 rows deserialize without it and the key
  stays absent on the wire — no store schema-version bump (the §4.37 additive precedent), no
  new cap (the §4.20 per-row 64-mention cap and 128-char id cap already bound the payload),
  no new method or event.
- `mentionsSelf` semantics are deliberately unchanged: the §4.24 verdict stays number-based
  receive-time, never recomputed, because the linked account's own ACI is not queryable on
  either engine's jsonRpc surface (`listAccounts` returns numbers only). An engine-mode
  self-mention (author = the account's ACI) still cannot be attributed to self — the recorded
  under-count boundary stands; `authorAci` does not silently re-decide it.
- Send chain untouched: outbound `messages.sendText` mentions (§4.34 ladder) already pass
  through and this revision adds nothing to them.
- Desktop consumption: an @-chip resolves against the §4.39 roster by `authorAci` when present,
  falling back to `author` (which the roster's `id`/`uuid` pair joins in both engine modes);
  the "@you" highlight keeps reading `mentionsSelf` and needs neither field.

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
- cloud-backed list projections are capped connector-side so one host frame stays bounded: the
  §4.33 pinned-conversation list at 128 entries and the §4.34 sticker-pack sync list at 256
  entries — cloud order preserved, overflow truncated, entries missing the identity field
  dropped; the cloud record remains the source of truth and the connector persists neither list.
- view-once erasure (§4.37) covers only the channels this connector can see: the row body bytes
  in its store, the media files under the attachment directories it owns, and its own handle
  table. The engine's protocol store and any CDN blob are outside connector reach — the same
  receiving-side scope as the official view-once erase. No ViewOnceOpenSyncs staging: an open
  sync that arrives before its message is warn-only dropped, and the sender's resync cycle
  re-delivers it. `peer_identities` is bounded (one row per ACI, contacts scale); `senderUuid`
  capture applies from 1.38 forward and earlier rows fall back through the §4.37 ladder.
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
- Experimental `kt-engine` mode (2026-10-03): launches KT's own AGPL-3.0 sidecar
  `kt-signal-engine` (separate repository; the libsignal-based replacement for the signal-cli JVM)
  in place of signal-cli, selected explicitly with `serve --signal-cli-engine kt-engine` or
  `KT_SIGNAL_CLI_ENGINE=kt-engine` (explicit selection, never probed: packaging controls what it
  ships). In this mode `--signal-cli` points at the engine executable. The stdio JSON-RPC surface,
  the argv tail (`--data-dir <dir> jsonRpc --receive-mode on-start [--ignore-attachments]
  --ignore-stories --ignore-stickers`), and every supervision guarantee are identical to the
  signal-cli modes, so the watchdog, unlink detection (§4.17: the engine reproduces the
  AuthorizationFailed classification surface), and shutdown semantics apply unchanged. Two
  transport divergences, both deliberate: the JVM/native `-DsocksProxy*` entries are never
  emitted in this mode because the engine's argv parser is fail-closed on unknown arguments —
  proxy parity instead holds at the environment level, with the engine consuming the same
  `KT_SIGNAL_SOCKS_PROXY=host:port` variable this launcher injects and mapping it onto reqwest's
  `ALL_PROXY`, which the libsignal-service HTTP and WebSocket transport (a single reqwest client)
  honors natively, so the per-group desktop proxy probe keeps working unchanged; and
  `--media-ingest` is downgraded instead of refused (2026-10-03): the engine never downloads
  attachments, so arming ingest in this mode silently keeps the pre-1.17 shape —
  `--ignore-attachments` stays on the argv, the media governor is not armed, and every media
  method answers CAPABILITY_UNAVAILABLE — with a launch-time warning, so the dev desktop
  (which arms ingest unconditionally) still boots instead of failing closed. `JAVA_HOME` is
  ignored with a warning, and the child environment is inherited
  except for the store-key override, exactly like the other modes. The engine writes its own
  `engine-state.json` / `engine-send-log.jsonl` / `accounts/lk-<hex>` layout under the group data
  directory, disjoint from signal-cli's files and the connector's `.kt-signal-connector.lock`.
  Scope: local smoke and development only — runtime-manifest packaging, licensing metadata, and
  Desktop integration for the engine bundle are future work.

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

Per-engine account ceiling (optimization-plan §6.4 M3.3, decision D3): one proxy group's engine
serves at most 8 accounts. `link.start` on a full group is refused before the engine mints a QR,
and `link.finish` is refused before dispatching `finishLink`, so a phone is never asked to approve
a link the connector would then reject at commit; both answers are `ACCOUNT_LIMIT_REACHED`
(`retryable=false`) and name the group. The store-level guard re-checks at commit and treats
re-linking a number already bound to the full group as an update, not an addition. A pre-ceiling
data directory that already holds more accounts keeps serving them: engine re-sync imports
reality and never truncates.

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
logged and never fails the link flow. Every group supervisor engine start (boot `runtime.start` or
a watchdog recovery restart) also runs one best-effort background re-sync pass for the group's
linked accounts after a settle delay: an account linked before the skeleton revision — or whose
inline link-time sync failed — still materializes its contact/group conversation skeletons on the
next engine start instead of surfacing conversations only message by message. The pass is
one-shot per process start (history-retention discipline), warn-only, and the 60-second cache
folds repeat starts into read-only no-ops. `contacts.list` serves the cache only (optional substring
filter, cursor pagination) and never calls upstream, so the Desktop new-chat picker cannot starve
the JVM queue. Contact rows and the sync marker are deleted with the account.

`messages.sendText` accepts either `conversationId` or a peer target (`kind` + `peerKey`, optional
`peerTitle`); exactly one form is required. A peer send resolves the conversation by
`(account_id, kind, peer_key)` and, when absent, creates it together with the first outgoing
message. `kind` accepts `contact`/`direct` (both a direct chat) or `group`; a missing `peerTitle`
falls back to the masked peer address, never the raw number.

Contract revision 1.14 (2026-09-23) replaces the earlier "no empty conversation skeletons"
resolution: a successful `contacts.sync` now also ensures one conversation row per synced
direct contact and per synced member group, in the same transaction as the contact upserts.
This mirrors the official Desktop post-sync shape — the linked account sees its existing chats
as empty skeletons (sorted after conversations with history by `last_message_at IS NULL`), the
title comes from the sync cache and upgrades through the normal `title_should_upgrade` path
when the first real message lands. Message history is still never backfilled (§6.6); a skeleton
only becomes an active conversation through a real message. The pre-1.14 behavior —
conversations created only by the first sent or received message — remains true for entries the
upstream has not synced (unknown numbers that message the account create their conversation on
arrival as before). Since contract revision 1.39 the other half of the skeleton story is the
link-time history import (§4.38, §6.6): a skeleton conversation is never duplicated by the
import — the archive's chats resolve to the same `(account, kind, peer_key)` identity and fill
the existing row in place through `ensure_conversation`, upgrading the title through the normal
policy when the archive knows a better name.

### 6.6 History limitation and link-time import (backup5)

The connector only promises history it has persisted. `sendSyncRequest` can synchronize contacts and
groups but is not treated as complete phone/Desktop message-history backfill. Product UI must not
promise pre-link history beyond what the import below actually lands. Since contract
revision 1.14 the synced chats do surface as empty conversation skeletons (§6.5); their message
lists stay empty until real messages arrive or the link-time import fills them.

Verified against official sources (2026-10-03, Signal-Desktop v8.31.0-alpha.1): the mechanism is
the `backup5` link capability (`ts/textsecure/Provisioner.preload.ts` — advertised in the link QR
URL only when the desktop has never registered) plus a `ProvisionEnvelope.ephemeralBackupKey`
carried by the phone at scan time; the newly linked desktop then downloads a full backup archive
from the Signal backup service and imports it before entering the app
(`ts/services/backups/index.preload.ts`). Two product constraints apply officially as well: the
phone must be online and cooperate at link time, and an installation that ever registered cannot
re-import (isLinkAndSyncEnabled gates on registration). Upstream tracking: AsamK/signal-cli
issue/PR #2134 (open, 2026-09-29, "qr history daemon") proposes `importHistory` on
`startLink`/`finishLink` plus paged `export-history` with attachment metadata only — not merged,
not released, and has no maintainer engagement. Product decision (2026-10-03): full link-time
history is a hard requirement, so this is no longer a standing boundary but an active project:
kt-signal-engine ADR 0005 (2026-10-03) commits to a self-built backup5 receiver (capability
advertisement, ephemeralBackupKey capture, archive download/decrypt, receive-compatible frame
export); libsignal v0.99.0 (already in the engine's dependency tree) ships the `message-backup`
and backup-key primitives. The S1 kill-gate spike PASSED on 2026-10-03 (ADR 0005 §取证记录):
the phone honors a third-party `backup5` QR capability and the provisioning envelope carries
`ephemeralBackupKey`, `GET /v1/devices/transfer_archive` plus the CDN attachment download
succeeded byte-level (6,576-byte archive), and no presage patch is needed. S2 landed in the
engine on 2026-10-04 (kt-signal-engine 0eedc8c, ADR 0005 §5): the link flow bypasses presage's
`Manager::link_secondary_device` (which drops `ephemeral_backup_key`) and calls
`provisioning::link_device` directly with `capabilities=backup5` on the QR URL, persists the
registration through the official store format, then best-effort long-polls the transfer
archive, decrypts it (official `libsignal-message-backup` v0.99.0 primitives) and streams a
frame-by-frame NDJSON export to the account store directory (`history-import.ndjson`).

**Contract revision 1.39 (2026-10-04) consumes that face (§4.38)**: the connector imports the
archive into its store after link — bounded batched writes, live-traffic identity dedupe
(same `signal-message-v2` stable id, `sent_at` = the archive's Signal timestamp, so history and
live envelopes of the same message collapse to one row), existing §6.5 skeletons filled in place
through `ensure_conversation`, no unread-badge or host-event flood, retention converged by the
normal §7.1.1 pass. The import is best-effort and honest end to end: the desktop reads
`history.importStatus` (§4.38) — `unavailable` under signal-cli modes (pinned signal-cli 0.14.x
has no such capability; the kt-engine mode is the only import-capable face) or when no archive
was produced, `pending` during the link-time wait, `running`/`completed`/`failed` from the
persisted row with real counters. Attachments stay metadata-only (§6.7 philosophy — the archive
carries no bytes and the connector never downloads any). The archive itself is never mirrored in
full: imported rows live under the same §7.1.1 retention bounds as every other stored message
(age measured on local `stored_at`, per-conversation cap converged right after import), so the
product promise is "the phone's recent history, to the connector's normal retention bounds", not
a full-archive shadow. Engine-side dependency registered for the engine's S3 task: the S2 export
does not yet project recipient service identity (`aci`/`e164`/`masterKey`), so against the
current engine build a run completes with every chat skip-counted; the connector consumes the
fields the moment the engine adds them (no further connector change).

### 6.7 Media limitation

Phase 1 runs signal-cli with attachments, stories, and stickers ignored. Current signal-cli downloads
non-ignored incoming attachments before it emits the receive notification, and `getAttachment`
returns an already-downloaded file as full Base64. That is not an acceptable large-media boundary.

Media remains disabled until a dedicated PoC proves bounded disk-pressure behavior and streams a
canonical, already-downloaded file through a short-lived handle without Base64 or arbitrary path
exposure. Failure to prove the limit leaves the media capability disabled.

Contract revision 1.15 (2026-09-23) narrows this boundary without lifting it: incoming envelopes
now contribute bounded attachment **metadata only** (§4.13) — descriptors are stored on the
message row while `--ignore-attachments` keeps byte download, disk pressure, and
`messages.attachments.get` answering `UPSTREAM_ERROR` exactly as before. A media capability
change still requires the dedicated bounded-download PoC this section demands.
(2026-09-24, contract 1.20: the one-shot `messages.attachments.get` reader itself was removed
after the PoC landed — §4.16; the boundary statement now lives entirely in the chunked
channel's gating.)

That PoC is now designed: `docs/adr/0002-media-ingest-poc.md` (2026-09-23) defines the
opt-in `--media-ingest` spawn flag, a quota+TTL media governor, and chunked handle
delivery (`messages.attachments.open` / `readChunk` / `closeHandle`, contract revision
1.17). Until that PoC passes its acceptance gates, this boundary stands.

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
- per-engine accounts: at most 8 per proxy group (decision D3, optimization-plan §6.4 M3.3),
  enforced at both link entries with `ACCOUNT_LIMIT_REACHED`; pre-ceiling data directories keep
  serving existing accounts and re-sync never truncates.
- runtime/UI broadcast queue: 1,024 non-critical events with pressure reporting; lag is recoverable
  from SQLite.
- critical receive queue: 256 items and 2 MiB of normalized projected data. It backpressures the
  signal-cli stdout reader at either limit and never routes receives through broadcast delivery.
- current message page: client-supplied `limit` is required in schema (1–200; the store clamps to the same range) and has no server-side default.
- message text projection: 4 KiB per list/event row; persisted inbound body: 128 KiB maximum.
- one signal-cli RSS sampler per group engine, every 30 seconds. Pressure requires three consecutive
  samples at or above 512 MiB; recovery requires two consecutive samples at or below 420 MiB.
  Sampling emits only PID/RSS/state and exits immediately with the engine. RSS pressure never kills
  or restarts the JVM automatically. Pressure events identify their group by `groupId` (see 4.4).
  The 60-second metrics snapshot additionally logs each engine's latest RSS sample (group id and
  bytes, one line per sampled engine) next to the queue gauges (`receiveQueueDepth`,
  `hostPendingRequests`) and counters (`receiveDroppedTotal`, `watchdogRestartsTotal`), so a soak
  run's drop/restart evidence is in the connector log without any external metrics service.
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
