# D3 audit: engine last-moments dump redaction (read-only, cross-repository)

Status: **audit only — the fix must land in `kt-signal-engine`.** This
connector worktree is the wrong repository for the change; nothing here
modifies the engine. Recorded for the engine owner (the audit's W8
`stability2` line or a dedicated batch).

Anchor: audit §9.13.6 D3 — "last-moments dump 脱敏: `logfile.rs` WIP 会抓在飞
状态；必须确保不落正文/密钥/号码/路径".

## What the dump contains

`kt-signal-engine/src/logfile.rs` installs one `tracing` subscriber
(`RollingFileSubscriber`). Its `event()` builds a line from the event's
`message` plus every remaining field as `key=Debug(value)`
(`src/logfile.rs:252-292`), pushes it into a 1024-line ring
(`LAST_MOMENTS_RING`), and mirrors it to `engine-stderr.log`. The WIP
`dump_last_moments` writes that ring verbatim to
`<data-dir>/logs/engine-last-moments-<ts>.log` and also embeds the last
`ka_diag` line (`src/logfile.rs:118-164`). There is **no redaction layer**:
whatever a warn+ event formats is persisted and then copied into the
sidecar. The sidecar is 0o600, but 0o600 is access control, not redaction —
the file is explicitly a support artifact.

## Findings

### 1. High — full `Envelope` debug is logged at warn

`kt-signal-engine/vendor/libsignal-service/src/cipher.rs:464`

```rust
tracing::warn!(?envelope, "Envelope with plaintext content. ...");
```

`Envelope` is the proto type in
`vendor/libsignal-service/protobuf/SignalService.proto:75-114`. Its derived
`Debug` includes:

- `sourceServiceId` / `destinationServiceId` (ACI/PNI identifiers),
- `sourceDeviceId`, `clientTimestamp`, `serverTimestamp`, `serverGuid`,
- `updatedPni`,
- `content` (ciphertext bytes, potentially large),
- `report_spam_token` (a bearer-like token).

The `PlaintextContent` arm is hit on a decryption retry / encryption-error
envelope, i.e. exactly the degraded windows the ring is meant to preserve.
This single call puts identifiers, a spam-report token, and message
ciphertext into both `engine-stderr.log` and every last-moments dump. It
also blows the 64 KiB dump buffer with content bytes.

Required fix: never debug the envelope. Log fixed structural facts only,
e.g. type word, `source_device_id`, `content_len`, `has_server_guid` —
no source/destination ids, no token, no bytes.

### 2. Medium — phone-number parse error echoes the input

`kt-signal-engine/vendor/libsignal-service/src/models.rs:59`

```rust
tracing::warn!(e=%e, "unparseable phone number");
```

The `E164` parse error's `Display` is not contractually content-free; a
validation error commonly repeats the rejected input. The input is a phone
number, which the engine's own log discipline forbids. Required fix: drop
`e=%e` (fixed literal only) or log a fixed class word.

### 3. Medium — RPC error text is echoed verbatim

`kt-signal-engine/src/methods.rs:192`

```rust
tracing::warn!(method = %request.method, code, message, "rpc error face");
```

`message` is the response error's `message`, i.e. engine-authored text. Today
most engine errors are fixed literals (`internal_error` has no detail), so
this is lower risk than (1), but it is an unbounded string that could gain a
target id or detail from a future structured error. Required fix: bound it
(e.g. ≤64 chars) and/or pass it through the same field allowlist as the
subscriber.

### 4. Structural — the subscriber has no field allowlist

`src/logfile.rs:255-290` trusts every `warn!`/`error!` call site, and the
last-moments ring copies the result. Any future warn that logs a path, a
number, a key, or a body is immediately persisted and dumped. This is the
class-level defect; fixing only (1) and (2) leaves the next call site open.

Required fix: a redaction gate in `LineVisitor`/`event()` — either an
allowlist of field names for engine-owned events plus a fixed vendor
passthrough policy, or a value scrubber that drops known sensitive keys
(`envelope`, `content`, `sourceServiceId`, `destinationServiceId`,
`report_spam_token`, `updatedPni`, `serverGuid`, `path`, `key`, `token`) and
redacts E.164/UUID-looking values. The gate must apply before the line
enters both the ring and the file, so the two carriers cannot diverge.

## Required tests (engine repository)

1. `last_moments_dump_never_carries_identifiers_or_tokens`: emit a
   `warn!(?envelope, ...)`-shaped event carrying a marker service id, a
   marker spam token, and a marker body; call `dump_last_moments`; read the
   sidecar and assert none of the markers appears. This is the regression
   the connector audit cannot host (no engine logger in this crate).
2. `last_moments_dump_drops_phone_parse_input`: emit the unparseable-number
   warn with a marker number; assert the dump has the fixed class, not the
   number.
3. `last_moments_dump_is_bounded_and_contains_no_paths`: assert the dump
   never contains the data directory, an account directory, or a file path;
   assert the ring stays at ≤1024 lines and the dump at a bounded size.

## Why this is not in the connector batch

The connector has no `logfile.rs`, no last-moments ring, and no engine
subscriber; its own logs already follow the content-free discipline (fixed
method classes, masked addresses, no bodies/paths). A connector-side test
cannot exercise the engine's `tracing` subscriber without importing engine
sources, which AGENTS.md forbids (`Do not copy, link, or modify ... engine
sources in this repository`). The change belongs in the engine worktree and
must be verified by the engine's own gates.
