# Signal Integration Optimization Plan

Date: 2026-08-25. Last updated: 2026-09-06. Scope: `kt-signal-connector` (this repo) +
KT Desktop Signal integration (worktree `.worktrees/signal-test-main-latest`,
branch `codex/signal-test-main-latest` @ a7e203be; this repo @ 77f94b6).

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

> **2026-09-06 更新**：Phase 4 已于后续批次双侧落地（connector `proxy_group` 实现遍布
> src/，desktop 见 handoff §6.27 及其后的四阶段 L2 交付），上表快照保留作历史记录。
> **当前进行中的工作见 §5 —— 2026-09-06 全链路重构与优化提案（批次 A/B/C，已批全量执行）。**

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

---

## 5. 2026-09-06 全链路重构与优化提案（批次 A/B/C 终稿 · 已批全量执行）

来源：四路并行调研合并——① connector 冗余与纪律审查 ② desktop 冗余盘点 ③ 外部开源/文献调研
（Signal-Desktop、TanStack、SQLCipher 等）④ desktop 架构与算法审查，结论均复核到 file:line。
desktop 侧注册指针：`.worktrees/signal-test-main-latest/docs/plan/signal-handoff-next-owner.md` §6.28。
基线：desktop `a7e203be` / connector `77f94b6`，两仓工作区干净，全部本地未 push。

### 5.0 已核实的发布风险（先于一切批次）

worktree 打包 runtime `signal-runtime/macos-arm64/bin/kt-signal-connector` 为 08-10 构建（3.6 MB）。
strings 验证：`presence.setTypingMessage` / `messages.sendReaction` / `messages.remoteDelete` /
`messages.attachments.get` 全部缺失——四阶段 L2 方法一个都不在包里（`messages.sendReceipts`
则本就不实现，见契约 1.12）。dev client 走本地新构建，实机验收不受影响；但正式发布链路若直接取
仓内 runtime，用户机器将静默降级。**处置：全部批次完成后用 connector 终态重出二进制入库
（B5 门禁防复发）。**

### 5.1 批次 A — 低风险，先行（约 4–5 人日）

Desktop（A1/A2 同文件，仓内串行 A1→A2→A3→A4）：

| # | 事项 | 依据 | 量级/风险 |
| --- | --- | --- | --- |
| A1 | 删除送达回执死链路整链（~500 行/14 处/5 文件）。三方证据闭环：渲染端 flag 默认关；connector 无 `messages.sendReceipts`（源码与契约 1.12 均确认上游无 delivery 发送通道）；打包二进制亦无。步骤：①断渲染端进料（`noteDeliveryReceiptCandidates` + watch 钩子 + scheduler 实例/dispose/rebuild）②删 `signalDeliveryReceipts.ts` + spec + typing wiring spec 中 receipts 部分（**typing 部分保留**）③删 `signalHost.sendSessionReceipts` ④删 Main 侧 registerIpc handler / `sessionHost.sendSessionReceipts` / `hostAdapter.sendDeliveryReceipts` + allowlist / `classifySignalRequest` 分支 ⑤desktop 契约 `signal-host-adapter.md` §5.4/1.8 登记撤销（保留 revision 历史）⑥清 `SessionFeatureId.SIGNAL_DELIVERY_RECEIPTS` | `signalDeliveryReceipts.ts` 全文件；SignalWorkspace.vue:4702,4127-4140,3107-3109,4685,4985；signalHost.ts:174-179；sessionHost.ts:851-889；hostAdapter.ts:406-419；requestScheduler.ts:249 | 0.5–1d / 极低（链路本不通，TS 保证删序） |
| A2 | 消息批量 merge 优化：`applyMessageEventBatch` 整批一次 merge（现状批内逐条全量 merge：B×(克隆 200+排序 200)）；`mergeBoundedSignalMessages` 未变化行保旧引用（现状每行新对象→200 行全 re-diff）。必须逐条保持 dedupe（clientRequestId 折叠）与 preserveIds 溢出语义 | SignalWorkspace.vue:3075-3089；signalPageWindow.ts:33,48-50,61-88 | 0.5–1d / 低（signalPageWindow.spec 兜底，补批量用例） |
| A3 | 修用户可见破图：blob LRU 逐出只 revoke URL 不清 `attachmentUrls`，回翻 >12 张图渲染破图；加 `onEvict` 回调同步删 ref | signalAttachment.ts:126-131；SignalWorkspace.vue:1439-1442 | ~10 行 / 极低（补 spec 用例） |
| A4 | 杂项清理：删 `signalStorageRetainedSessions`（3 行）；删死 i18n key（grep 复核后删，调研计数 6 个）；SIGNAL_TYPING 是活代码等 UI 开关——只修不实注释、不删（去留待产品）；契约 §5.3 漂移修正；`account_delete_pending` 契约登记；20 个在用 key 补登记 en.ts | — | 0.5d / 极低 |

Connector（A5–A9，合计 <300 行）：

| # | 事项 | 依据 | 量级 |
| --- | --- | --- | --- |
| A5 | attachmentId 长度统一 128（schema 128 vs 代码 256） | service.rs:42 | ~5 行 |
| A6 | `update_message_status` 双取锁 TOCTOU → 单事务（unchecked_transaction） | store.rs:1372-1398 | ~10 行 |
| A7 | stdin `write_all` 内联阻塞互堵窗口：写拆独立 task 或加超时 | engine.rs:819 | ~30 行 |
| A8 | `schema_consistency.rs` 补数值边界 diff（长度/上限类约束入一致性门禁，防 A5 类漂移复发） | tests/schema_consistency.rs | 30–50 行 |
| A9 | `prune_history` 全表窗口扫描优化（90 天/2000 条界内减少全表扫） | store.rs:1784-1803 | 小 |

### 5.2 批次 B — 中型结构化（约 4–6 人日）

| # | 仓 | 事项 | 依据 | 量级 |
| --- | --- | --- | --- | --- |
| B1 | connector | 方法→车道单一事实源：4 处重复分类表收敛为 1 处，新增方法触点 6→3 | host.rs:189-198,236-243,294-301；metrics.rs:27-44；lib.rs:31-52 | ~60 行 |
| B2 | 双侧 | `contacts.sync` 车道归位：Main 侧 classify 无分支落 control（并发 1），慢同步挤 `listAccounts` 致账号状态抖动 → 归 read；两侧 limits 镜像注释（Main requestScheduler 与 host.rs CONTROL/READ/SEND_CONCURRENCY 改动需同动） | requestScheduler.ts classify；host.rs:194,242 | ~5 行+注释 |
| B3 | connector | `prepare_*` 七函数收敛 `resolve_target` | service.rs prepare_* 族 | ~90 行净减 |
| B4 | desktop | 样板收敛打包：registerIpc 表驱动（200→50 行）；wire 类型下沉 `shared/`（~120 行双写消除）；UNKNOWN_OUTCOME_PATTERN 下沉 shared；reaction/remoteDelete 乐观骨架共享（省 60–90 行）。注意：发送链路不是同类重复，不并入 | registerIpc.ts；类型双写处；SignalWorkspace.vue reaction/remoteDelete 骨架 | ~2d |
| B5 | desktop | release 门禁：runtime 二进制方法清单 × 契约 revision 配对校验（strings/符号比对，防 §5.0 漂移复发）+ connector 终态重出二进制入库 | §5.0 | 0.5–1d |

### 5.3 批次 C — 大重构，最后（约 6–10 人日）

- **C1** `SignalWorkspace.vue`（6706 行）拆 composable，四刀依次、每刀一 commit、行为纯搬移：
  ①`useSignalLinkFlow`（~900 行，耦合最小先切：:1115-1120,1267-1274,1549-1640,1741-1848,3333-3720,4618-4649）
  ②`useSignalAttachments`（:1373-1449，核心已在 signalAttachment.ts）
  ③`useSignalMessageActions`（:1352-1371,2500-2754，菜单/reaction/引用/删除确认）
  ④`useSignalOptimisticSend`（:2438-2500,2958-3317,4197-4325,4420-4576，最后拆、回归面最大）。
  每刀先补 characterization 测试再搬移；`viewAlive + sessionUid/accountId/conversationId` 四重
  防串话守卫随行。
- **C2** Pinia store 化 + store 层窗口化：外部调研证实 Signal-Desktop 官方 Timeline 不用虚拟列表库，
  靠 store 窗口化（messageIds+lookup、discardMessages 裁剪、IntersectionObserver 驱动已读/翻页/贴底、
  四锚滚动定位）。我们已有 DynamicScroller + 200 封顶已达标；C2 真正收益 = 状态机脱离单体获得真单测能力。
- **C3** 融进 C1/C2 的官方设计（不单独立项）：reaction 去重键
  `(targetAuthorAci,targetTimestamp,fromId,emoji)`；quote 按作者 ACI+sentAt 双键；send 返回
  timestamp 作乐观 ack 对齐；backoff（上游已停更）换 backon + full jitter。

### 5.4 明确不做（四路调研一致结论，防过度工程）

`Map<id,index>` 替代 findIndex（n≤200 非瓶颈）；恢复退避 × Rust watchdog 协议级去重（跨仓协议
变更破坏半升级兼容，现状 Backpressure→收敛已有界自愈）；虚拟滚动换库/增强；requestScheduler 重写
（FIFO+超时+queue_full 语义正确，双侧同构限流是手工镜像——B2 补注释即可）；connector phf
dispatch、宏化 prepare_*、jsonschema 热路径；redux 样板 / jsonrpsee 直配 stdio / sqlx+SQLCipher /
XState 管消息数据面；SIGNAL_TYPING 删除（活代码等 UI 开关，去留待产品）。

### 5.5 执行纪律与状态回填

- 两仓可并行、同仓内严格串行（A1/A2 同文件；B1/B2 注释同文件）。
- 不 push（延续现状）；中文 commit；大提交为主提交、属其一部分的小改动并入（既定口径）。
- 门禁全绿才 commit：desktop `yarn typecheck:signal` + `yarn vitest run`（proxy groups 两个
  spawn 用例为基线 flaky，已在 a7e203be 放宽预算——若再红先重跑甄别）；connector `cargo fmt` +
  `cargo clippy -- -D warnings` + `cargo test` + `cargo build --release`。
- dev client（Vite 3355 / CDP 9336）常驻在跑：不得杀其进程；vitest 与 Vite 互不干扰。

| 批次 | 状态 | commit 回填 |
| --- | --- | --- |
| A-desktop | 待执行 | |
| A-connector | 待执行 | |
| B-connector | 待执行 | |
| B-desktop（B2/B4） | 待执行 | |
| B5 + 二进制重出 | 待执行 | |
| C（四刀 + store 化） | 待执行 | |
