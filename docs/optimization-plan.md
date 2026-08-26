# Signal Integration Optimization Plan

Date: 2026-08-25. Last updated: 2026-08-26. Scope: `kt-signal-connector` (this repo) +
KT Desktop Signal integration (worktree `.worktrees/signal-test-main-latest`,
branch `codex/signal-test-main-latest` @ d4de1d24).

This plan follows the 2026-08-25 architecture review of both sides plus an open-source
ecosystem survey (signal-cli v0.14.7, presage, libsignal-client v0.101.0, mautrix-signal,
signald, Signal-Desktop).

## Status snapshot (2026-08-26)

| Phase | Status | Evidence |
| --- | --- | --- |
| 0 — Safety net | ✅ DONE, committed | schema↔code consistency gate; CDP smoke harness `yarn smoke:signal-cdp` |
| 1 — Internal fixes | ✅ DONE, committed | 163 tests green ×3; liveness deadlock regression test; tracing + redacted metrics |
| 2 — Contract cleanup | ✅ DONE, committed | quote upstream delivery; `message.statusChanged` producer+consumer; IPC envelope; 31 error copies |
| 3 — Encryption at rest | ✅ DONE, committed | real-profile online migration verified; key reuse + wrong-key fail-closed verified on machine |
| 4 — Proxy groups | 🟡 DESIGN FINALIZED, desktop partially implemented, connector not started | ADR 0001 + schema v1 contract + AGENTS.md rule revision in working tree; desktop has proxyGroups.ts + supervisor/eventRouter/hostAdapter/sessionHost/sessionRegistry/types changes (typecheck-clean); connector src/ untouched |
| 5 — Native image | ✅ DONE, committed | 112.8 MiB binary; RSS 265→136 MiB (−48%); cold start 6.25→4.99 s; smoke 11/11 on real account |

Both repos are committed locally (connector 3 commits, desktop 4 commits) and not pushed.
Phase 4 implementation was interrupted by a subagent gateway outage (502/503) on 2026-08-26;
resumption resumes the two agents in place, no rework. Desktop side is typecheck-clean
(3 TS errors in `sessionHost.ts` fixed before handover). See `docs/handover.md`.

## 0. Conclusions that frame the plan

- Keep the signal-cli wrapping architecture. Alternatives were evaluated and rejected:
  presage has product-level feature gaps (no group create #298, no receipts #141,
  no disappearing messages #168, no recaptcha #176, git-only dependency);
  libsignal-client is crypto primitives, not a client; signald is unmaintained;
  mautrix-signal is Go+cgo and not an extractable library.
- The real gaps are: one liveness deadlock, plaintext-at-rest across the whole chain,
  contract drift between schema and code, no observability despite plan §8 promising it,
  and no per-account network isolation (KT's multi-account anti-association scenario).
- No plan can guarantee zero regression in a 4-layer system
  (Vue UI → Electron Main → Rust connector → signal-cli JVM). What we guarantee instead:
  every step is verifiable, reversible, and attributable to one layer.

## 1. Principles

1. Contract freeze: `schemas/connector-api-v1.schema.json` is the single source of truth.
   Any IPC protocol change ships in one batch on both sides, gated by the apiVersion
   binding already present in the HMAC handshake.
2. Internal refactors must not change the protocol: request/response shapes, event
   shapes, socket protocol, and the auth flow stay byte-identical in Phase 1.
3. Every phase is independently reversible: connector rollbacks use the LKG pointer;
   desktop rollbacks use the branch.
4. Regression tests are written before the code they protect.

## 2. Phases

### Phase 0 — Safety net (1-2 days, zero risk, additive only)

- Connector: schema↔code consistency test — machine-compare methods, events, and error
  codes in `schemas/connector-api-v1.schema.json` against the code, so contract drift
  fails CI. (Known drift to be reconciled in Phase 1/2, not here.)
- Connector: extend the fake signal-cli fixture into a happy-path smoke suite:
  link, send, receive, contacts sync, watchdog restart. This is the baseline for all
  later phases.
- Desktop: CDP smoke (port 9223 harness) covering: open conversation, send text,
  receive event render.

Gate: all new tests green on unmodified code. This doubles as a health check of the
current feature set.

### Phase 1 — Connector internal fixes ✅ DONE (committed)

- Fix the liveness deadlock: bounded wait on receive `enqueue` (timeout degrades to a
  storage-degraded event instead of blocking the actor); total timeout on
  `EngineHandle::shutdown` followed by hard kill; timeout on host teardown.
  (engine.rs:237-240, 528-540, 724-725; supervisor.rs:1238-1267; host.rs:541)
- `sync_contacts` → single-transaction batch upsert (supervisor.rs:1167-1178).
- Mutual exclusion between account delete and in-flight send (drain barrier reusing
  the existing per-account send locks) (supervisor.rs:795-868).
- Introduce `tracing`, preserve error causes (`#[source]`), land the redacted metrics
  promised by plan §8.
- Register the already-emitted `runtime.resourcePressure` event in the schema
  (recording existing behavior, not a protocol change).
- Fix `INTERNAL_ERROR retryable=true` mislabel for missing pending rows
  (service.rs:54-56, 573-574).

Gate: Phase 0 baseline green + new "queue full + dead store + shutdown" regression test
+ fmt / clippy / tests / release build per AGENTS.md.
Rollback: LKG pointer back to previous version directory.

### Phase 2 — Contract cleanup ✅ DONE (committed; both sides shipped together)

- Resolve `quoteMessageId`: implement upstream delivery (local row → quoteTimestamp /
  quoteAuthor → signal-cli send params). Preferred over removal because the UI already
  renders quotes. (service.rs:413-414, 546-554; store.rs:532)
- Resolve the `MessageStatusChanged` drift by completing it, not deleting it (decision
  2026-08-26, supersedes the earlier "remove dead API" line): the desktop already routes
  the event, so the connector gains a producer on real status transitions and the
  renderer gains a consumer. Deleting on both sides would have codified poll-only
  delivery status. `MessageRecord.attachments` (always empty) is still removed.
- Desktop: structured IPC errors — replace JSON-in-`Error.message` with an
  `{ok, error}` envelope; classify deterministic local rejections (scheduler queue
  full / not accepting) as `retryable=true` so the user gets a retry entry.
  (registerIpc.ts:41-47; signalSendState.ts:41-44, 84-95; errors.ts:26-46)
- Desktop: consume `message-status-changed` in the renderer (routed today, never
  consumed; SignalWorkspace.vue:3864-3932).

Gate: joint end-to-end smoke (link → send → receive → quote → status receipt) +
schema consistency test; apiVersion mismatch must hard-fail the handshake with a clear
error. Rollback: both sides roll back as one batch; apiVersion binding keeps old↔old
working.

### Phase 3 — Encryption at rest ✅ DONE (committed; ships alone)

- Connector: SQLite → SQLCipher (rusqlite `bundled-sqlcipher`; blast radius contained
  in `Store::open`).
- Key delivery contract (decision 2026-08-26): the desktop owns the 32-byte store key —
  generated once, persisted via Electron `safeStorage`, and delivered **at spawn time
  over the existing bootstrap-secret private channel** (0600 file on Unix / inherited
  stdin pipe on Windows), alongside the handshake secret. The key never crosses the
  socket and never enters the wire protocol, so `connector-api-v1.schema.json` is
  untouched by this phase. The connector holds it in `Zeroizing` memory (existing
  pattern). Missing key + existing plaintext DB = fail closed; missing key + no DB =
  first-run key creation by the desktop side.
- Migration: detect plaintext DB on first start → online `sqlcipher_export` → keep
  the plaintext backup for 7 days (reuse the retention batch framework) → fail closed
  on migration error, never silently stay plaintext.
- signal-cli data directory: no format change; document that it holds account keys and
  keep 0700 discipline.
- Desktop: move outbound-origin drafts and KT remarks out of plaintext localStorage
  into safeStorage-encrypted files.

Gate: migration test (plaintext → encrypted, row-count parity) + rollback test +
Phase 1 baseline. Rollback: DB backup + LKG.

### Phase 4 — Per-account proxy → proxy groups (approved 2026-08-26; design finalized, implementation pending)

Spike concluded 2026-08-26 against pinned signal-cli v0.14.7 (JVM and native modes):

- The proxy is a JVM process-global property (`-DsocksProxyHost/-DsocksProxyPort`); multi-account
  JSON-RPC mode shares one JVM and has no per-account proxy entry point. Per-account proxy
  therefore has exactly one engineered path: one engine per proxy group.
- Cost: 140-280 MB idle RSS per engine, +20-80 MB per additional account inside the same engine,
  so the group count is a bounded product decision (hard ceiling 8, product default 4).
- Group membership must be chosen at link time and is immutable afterwards: QR/captcha/CDSI first
  contact already goes through the group's proxy. Re-grouping = delete local data + re-link.
- The signal-cli data directory is the physical account boundary, so each group gets its own
  data dir; the `default` group keeps today's `--signal-data-dir` path unchanged.

Decision (ADR 0001, `docs/adr/0001-jvm-per-proxy-group.md`): engine-group model — one connector
process, one supervised engine (watchdog/RSS/restart breaker replicated) per launcher-defined
proxy group; groups are spawn-time config, never IPC-driven; the desktop reuses its existing
local SOCKS port allocator (WhatsApp/TG `socks5.ts`) to provision group proxies. The AGENTS.md
single-JVM rule was revised to this per-group wording in the same change.

Contract: additive in-place evolution of `schemas/connector-api-v1.schema.json` at apiVersion
`1.0` (no v2 — every change is optional/additive, and the version machinery has no multi-version
runtime support to exploit). `link.start` gains optional `proxyGroup`; accounts/list/link/account
surfaces gain group attribution; `runtime.*` results gain a `proxyGroups` array with a pinned
top-level aggregate rule; new event `proxyGroup.stateChanged`; `runtime.resourcePressure` gains
`groupId`; one new error code `PROXY_GROUP_NOT_FOUND`. Full field-level contract:
implementation-plan §4.4. A caller that never names a group is byte-compatible with today.
`tests/schema_consistency.rs` intentionally goes red on the two new names until the
implementation lands — that diff is the implementation checklist.

Implementation ships on two tracks per §1 contract freeze: connector (group config validation,
per-group supervision, accountId→group routing, store `proxy_group` column) and desktop (group
assignment UX, spawn config, port allocation) together; a single-group rollout needs no desktop
change at all.

Gate: schema consistency green again + multi-group fake-fixture integration (two groups, link
both, kill one engine, the other group unaffected) + RSS report at the 8-group ceiling +
real-link smoke on a dev profile. Rollback: single-group config equals today; rolling the binary
back leaves non-`default` groups dormant but intact (ADR 0001 Rollback).

### Phase 5 — GraalVM native image ✅ DONE (committed; orthogonal to main line)

Build signal-cli as a native image (community metadata exists, still updated at
v0.14.2). Success removes JRE packaging/signing/cold-start/300-500MB RSS. Failure
leaves the status quo untouched. Zero risk to the main line.

**Actual outcome (verified on machine, 2026-08-26):** GraalVM CE 25.2.4 (JDK 25.0.4,
macOS aarch64, tarball SHA256 pinned) builds signal-cli v0.14.7 to a single
112.8 MiB Mach-O binary via `./gradlew nativeCompile` after trimming three stale
Linux/amd64 resource globs from the upstream `reachability-metadata.json`
(trim script is in `packaging/scripts/trim-native-reachability-metadata.py`).
CI pipeline is in `.github/workflows/build-signal-cli-native.yml` (macos runner,
`workflow_dispatch` + tag trigger, smoke 4/4, artifact upload); a full cold run
took 3m50s end-to-end and is reproducible from the script alone.

Connector gained a `SignalCliMode::Native` path (`--signal-cli-native` / `KT_SIGNAL_CLI_NATIVE=1`):
native mode skips JAVA_HOME validation and JVM opts, keeps stdio JSON-RPC,
watchdog, RSS sampling and `kill_on_drop`. SOCKS proxy still works in native mode —
verified end-to-end with a real SOCKS5 relay (DNS resolves at the proxy side, no
silent direct fallback); the only new limitation is that the proxy host:port now
appears in the child's argv (not credentials; no environment-variable channel exists
for native images).

Measured on the same machine, same account, same profile:

| Metric | JVM | Native |
| --- | --- | --- |
| RSS (ready) | 265 MiB | 136 MiB (−48%) |
| Cold start (startRuntime → ready) | 6.25 s | 4.99 s |
| Smoke (real send) | 11/11 | 11/11 |

Known gap: long-run soak memory curve not measured (only ~1 min window);
re-run the SOCKS relay test after every signal-cli/native upgrade
(`/tmp/kt-native-proxy-probe/socks5_relay.py`).

## 3. Execution rules

- Order: 0 → 1 → 2 → 3 strictly serial; 4 and 5 independent (both confirmed
  parallelizable: Phase 4 design + Phase 5 CI/engine ran in two tracks with no
  file overlap; Phase 4 connector + desktop tracks are also independent).
- Definition of done per phase: baseline tests green + phase-specific tests green +
  real-link smoke on a dev profile (scan-code link a test account, one send/receive
  round).
- Rollout: dev profile → internal test accounts → production. LKG supports
  per-profile staging natively.
- Never merge-ship Phase 2 and Phase 3: if both land together, failures cannot be
  attributed to a layer.
- Phase 2 and Phase 3 are not parallelized even under schedule pressure.
- Joint verification (not just single-side gates) is what has caught real bugs:
  bootstrap payload trailing-newline mismatch, `START_FAILED` code mapping gap,
  and `message.statusChanged` consumer gap were all invisible to unit tests.

## 4. Verification loop (run after every phase and every major change)

Per phase, in order; stop at the first failure:

1. Build: `cargo build --release` (connector) / desktop production build.
2. Static checks: `cargo fmt --check`, `cargo clippy -- -D warnings` (connector);
   `tsc --noEmit` + lint (desktop).
3. Tests: `cargo test` (connector, incl. schema consistency + new regressions);
   desktop smoke suite.
4. Security scan: no secrets in diff; no new plaintext-sensitive logging; connector
   log discipline per AGENTS.md (no bodies, numbers, contacts, QR payloads, key paths).
5. Diff review: `git diff --stat` + read every changed file for unintended changes,
   missing error handling, and edge cases.

Report format per phase: Build / Types / Lint / Tests / Security / Diff →
READY or NOT READY, with an explicit issue list.
