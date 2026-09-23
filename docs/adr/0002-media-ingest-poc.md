# ADR 0002: Bounded inbound media ingest (PoC)

- Status: accepted as design; implementation in progress
- Date: 2026-09-23
- Amends: implementation-plan §6.7 (Media limitation) — the boundary stays, the
  unlock path is defined here
- Depends on: ADR 0001 (namespace/data-dir disjointness), §7.1 data ownership

## Context

The connector starts signal-cli with `--ignore-attachments`: inbound media bytes are
never downloaded, so received images/files/voice notes surface as metadata-only rows
(contract 1.15) and every byte fetch answers `UPSTREAM_ERROR`. This red line exists
because unmodified signal-cli downloads non-ignored attachments **eagerly, before the
receive notification**: disk pressure would be attacker-controlled (any peer can fill
the disk), receive latency would be hostage to media transfer over slow proxies, and
the only delivery shape signal-cli offers (`getAttachment`) is a full-file base64 read
(`readAllBytes()` in `GetAttachmentCommand`) that would blow the RSS and frame budgets.

Upstream signal-cli v0.14.8 (verified against the release notes and source, 2026-09-23)
still has no on-demand/lazy download mode, so the official-style "download when
rendered" behavior cannot be reached without forking — which violates the pinned
upstream red line (AGENTS.md; implementation-plan §3.3, §9).

## Decision

Lift `--ignore-attachments` **behind an explicit launcher opt-in** and satisfy §6.7's
two demands — bounded disk pressure, streamed delivery without base64 whole-file or
path exposure — with connector-side machinery:

### 1. Opt-in spawn flag

The launcher enables media ingest per engine with `--media-ingest` (connector argv,
never IPC): without it the engine spawns exactly as today (`--ignore-attachments`) and
every media method answers `capability_unavailable`. Desktop turns it on only for dev
runtimes first; signed bundles adopt it after the capacity gates pass.

### 2. Media governor (bounded disk pressure)

A governor pass runs over each account's `attachments/` directory
(`<signal-data-dir>/<account>/attachments/`, per signal-cli `PathConfig` — the exact
per-account layout is verified at implementation time from a live data dir):

- **TTL**: files older than 7 days are deleted (a received attachment stays fetchable
  for a week, mirroring Signal's own server-side retention expectations).
- **Quota + LRU**: when the directory exceeds 2 GiB, oldest-`mtime` files are deleted
  until back under quota. Deleted ids simply make later fetches answer
  `UPSTREAM_ERROR` — signal-cli's `AttachmentStore` is a stateless file store, so
  deletion has no ledger to corrupt.
- **Trigger discipline**: once per process start (same shape as the §7.1.1 retention
  pass, which never repeats on a timer) plus once per 200 inbound messages, whichever
  first. Bounded batch: at most 256 deletions per pass. Filesystem work never holds
  the store lock.
- **Eager-download overshoot is accepted and documented**: signal-cli downloads before
  the connector can veto, so the quota bounds *retention*, not the instantaneous
  write. The blast radius of one oversized download is capped by the server's own
  100 MiB per-attachment limit, and ADR 0001's group isolation bounds whose receive
  loop can be delayed by it — one account's media download never blocks another
  account's engine.

### 3. Chunked handle delivery (bounded memory, no path exposure)

Three new host methods (contract revision 1.17) replace one-shot base64 for media:

- `messages.attachments.open {accountId, conversationId, messageId, attachmentId}`
  → `{mediaHandle, sizeBytes, chunkBytes}`. The handle is a random 128-bit hex token,
  bound to the resolved (account, message, attachment) tuple, expires after 300 s,
  and at most 8 handles are live per account. Addressing rows are re-resolved so a
  bogus conversation/message answers before any file is touched.
- `messages.attachments.readChunk {mediaHandle, offset}` → `{dataBase64, sizeBytes,
  eof}`. Strictly sequential offsets (no random-access scanning), `chunkBytes` raw
  (256 KiB; ~350 KB base64 characters, far under the host frame limit). The
  connector streams from the file — resident memory is one chunk, never the file.
- `messages.attachments.closeHandle {mediaHandle}` — explicit early release; the TTL
  is the backstop.

`sanitizeId` parity is a hard security requirement: signal-cli sanitizes ids with
`id.replaceAll("[^A-Za-z0-9_.-]", "_")` before joining paths, so the connector must
apply the identical transform (and reject `..` outright) before any path join. A
renderer-supplied id can otherwise path-traverse.

Deviation from §6.7's wording, stated honestly: chunk payloads are still base64 —
per-chunk (≈1.33× on 256 KiB), bounded, frame-safe. The "without Base64" demand is
satisfied in spirit (no whole-file base64, no unbounded memory); a binary frame
extension can replace it later without contract changes.

### 4. Desktop delivery

Electron Main drives open → readChunk loop → closeHandle and forwards chunks to the
renderer over IPC events (one chunk per event); the renderer accumulates them into a
Blob, preserving the existing in-memory-only, never-to-disk discipline. The renderer
fetch API keeps its Promise shape; chunking is invisible above Main.

### 5. What this does not do

- No CDN direct-fetch (violates the pinned JSON-RPC red line).
- No transcoding, thumbnails, or voice-note playback UI (separate work).
- No change to receive semantics: the receive notification still lands after
  signal-cli's download; text messages behind a large download in the same group are
  delayed (bounded by the 100 MiB server cap, isolated to the group by ADR 0001).

## Acceptance gates (from §6.7, made concrete)

1. **Disk**: seed > quota of synthetic media; after one pass the directory holds
   ≤ quota + one file.
2. **Memory**: stream a 100 MiB file chunk-by-chunk; connector RSS growth < 50 MiB.
3. **Security**: path-traversal ids (`../`, absolute, `\`) fail closed; handles are
   single-origin, expire, and cannot outlive their account session.
4. **Frame safety**: chunk base64 length < host frame limit with margin.
5. **Opt-out**: without `--media-ingest`, the three methods answer
   `capability_unavailable` and spawn argv is byte-identical to today's.

## Rejected alternatives

- **Keep eager download without governor**: unbounded disk, attacker-controlled.
- **CDN direct-fetch in the connector**: reimplements the Signal media protocol and
  ignores the pinned-signal-cli boundary; a fork-sized decision, not a PoC.
- **Full base64 through `getAttachment` with a bigger frame limit**: unbounded
  connector and renderer memory; frame limits exist precisely to prevent this.
