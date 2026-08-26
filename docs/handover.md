# Signal Integration — Handover

Date: 2026-08-26. Owner: KT AI engineering. Status: **Phases 0–3 and 5 complete and
committed locally; Phase 4 design finalized, implementation interrupted by a
subagent gateway outage.**

## 1. What is done

### kt-signal-connector (Rust, `main`, 3 local commits, not pushed)

| Phase | What landed | Proof |
| --- | --- | --- |
| 0 | `tests/schema_consistency.rs` (methods/events/error-codes machine diff with explicit allowlist) + happy-path smoke baseline; desktop CDP smoke harness `yarn smoke:signal-cdp` | gates green on unmodified code |
| 1 | Liveness deadlock fix (bounded receive `enqueue`, `shutdown` total-timeout + hard kill, host teardown timeout); `sync_contacts` single-transaction batch upsert; account-delete / in-flight-send drain barrier; `INTERNAL_ERROR retryable=true` mislabel; `tracing` + redacted metrics (`src/metrics.rs`) | 163 tests green ×3; `runtime.resourcePressure` registered in schema |
| 2 | `quoteMessageId` upstream delivery (local row → `quoteTimestamp`/`quoteAuthor`); `message.statusChanged` producer + renderer consumer; `MessageRecord.attachments` removed from wire; IPC `{ok, error}` envelope; 31 error-code → Chinese copy map | joint smoke 11/11, statusChanged event +3756 ms ahead of DOM convergence |
| 3 | SQLite → SQLCipher (`Store::open`); bootstrap channel extended to two lines (`hexSecret\nhexKey`); `KT_SIGNAL_STORE_KEY` dev override; plaintext → encrypted online migration with 7-day backup; fail-closed on missing key | real-profile migration verified (row counts identical, backup present, key reused on restart, wrong-key fail-closed) |
| 5 | `SignalCliMode::Native`; GraalVM native-image CI pipeline (`.github/workflows/build-signal-cli-native.yml` + `packaging/scripts/*`) | 112.8 MiB binary; RSS 265→136 MiB (−48%); cold start 6.25→4.99 s; smoke 11/11 on real account |

Wire-protocol changes (Phase 2) are breaking and were shipped as one batch on both
sides; the `apiVersion` handshake hard-fails mismatches. Phase 3 and 5 made no wire
changes (key goes over the spawn-time private channel; native mode is a process
shape, not a protocol shape).

### KT Desktop (`codex/signal-test-main-latest`, 4 local commits, not pushed)

Phase 0/2/3: CDP smoke harness, IPC envelope, error-code copy map,
`message-status-changed` consumer, store-key/safeStorage delivery, renderer
sensitive-data migration to encrypted values, contract doc `1.3`.

**Hunk-level care was required:** the worktree also contains *another person's*
uncommitted WIP (`signalFriendRemark*`, `FriendEditModal`, `AppType.ts`, `Home.vue`,
`signalQuickReplyComponent.spec.ts`, ~14 WIP hunks inside `SignalWorkspace.vue`).
Those were excluded from every commit and are byte-identical to before; each commit
was verified by stashing the WIP and typechecking/testing the staged snapshot.

## 2. What is in the working tree now (uncommitted)

### Phase 4 — JVM-per-proxy-group (approved 2026-08-26)

- **Design finalized, not committed:** `docs/adr/0001-jvm-per-proxy-group.md`
  (R1–R10), `AGENTS.md` single-JVM rule revised, `schemas/connector-api-v1.schema.json`
  additive v1 evolution, `docs/implementation-plan.md` §4.4, `docs/optimization-plan.md`
  Phase 4 section.
- **Contract (field-level, implementation-ready):** optional `proxyGroup` on
  `link.start`; `proxyGroup` attribution on `accounts.list` / `account.changed` /
  `link.start` result / `link.finish` result; `proxyGroups[]` on `runtime.*` results
  with a pinned top-level aggregate rule; new event `proxyGroup.stateChanged`;
  `groupId` added to `runtime.resourcePressure`; new error code
  `PROXY_GROUP_NOT_FOUND` (retryable=false). Hard ceiling 8 groups, product default 4.
  Single-group config is byte-compatible with today.
- **`tests/schema_consistency.rs` is intentionally red on exactly two names**
  (`PROXY_GROUP_NOT_FOUND`, `proxyGroup.stateChanged`) — that diff is the
  implementation checklist.
- **Desktop side partially implemented** (files already on disk, uncommitted):
  `connectorSupervisor.ts`, `eventRouter.ts`, `hostAdapter.ts`, `proxyProbe.ts`,
  `sessionHost.ts`, `sessionRegistry.ts`, `types.ts`, `contracts/signal-host-adapter.md`,
  `proxyGroups.ts` (new, 408 lines — group store, port allocator, safeStorage persistence).
  **Typecheck-clean as of 2026-08-26** (3 TS errors in `sessionHost.ts` fixed before
  handover: `beginSessionLinkInternal` missing 4th `linkProxy` param; two
  `this.activeLink` assignments missing required `proxyGroup` field).
- **Connector side not started** (no `src/` changes for Phase 4).

## 3. How to resume Phase 4

The two implementation agents (`agent-20` connector, `agent-21` desktop) were killed by
a gateway outage, not by a decision. Resume with:

```bash
# from the connector repo
# AgentSwarm: resume_agent_ids { "agent-20": "continue", "agent-21": "continue" }
```

Both agents retain their context (they read ADR 0001 and §4.4 before dying); the
desktop agent's partial changes are on disk and will be picked up. No rework.

If the gateway IDs are no longer valid, re-dispatch from the design docs in
`docs/adr/0001-jvm-per-proxy-group.md` and `docs/implementation-plan.md` §4.4 —
they are the implementation contract.

## 4. How to verify (per phase, in order)

Connector:
```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test            # then ×3, no flakes
cargo build --release
git diff --check
```

Desktop:
```bash
yarn typecheck:signal   # baseline has 2 reds from someone else's WIP; must not grow
yarn test:unit          # baseline has 1 red from WIP; must not grow
yarn smoke:signal-cdp   # dry run; KT_SIGNAL_SMOKE_SEND=1 for the real send round
```

Joint verification is the gate that has caught real bugs invisible to unit tests
(payload trailing-newline mismatch, `START_FAILED` code mapping gap,
`message.statusChanged` consumer gap). Run it after every phase that touches both
repos.

## 5. Known open items

- **Receive inbound rendering** never verified end-to-end: the only linked test
  account's peer does not reply, and there is no Note to Self to self-test.
- **`Enter` to send is flaky under CDP** (button path is 100% reliable); suspected
  real-machine feel issue — P0 to verify by hand.
- **link QR / quote UI / `unknown`/`failed` status values** not exercised on machine.
- **Phase 4 long-run soak**: multi-group RSS curve at the 8-group ceiling not measured.
- **Native long-run soak**: only ~1 min window observed.
- **Desktop worktree has 2 typecheck reds + 1 unit-test red from another person's
  WIP** (`signalFriendRemark.ts`, `signalFriendRemark.spec.ts`, `xRemarkFailureOwnerGuard.spec.ts`).
  These are baseline and not ours. Our Phase 4 work is now typecheck-clean
  (fixed 3 TS errors in `sessionHost.ts` on 2026-08-26 before handover).
- **`scripts/signal-real-e2e-cdp.mjs`** and the `.tmp/` probes are test scaffolding;
  `.command` files are hand-edited per run.

## 6. Pending decisions (owner)

1. **Phase 4 rollout scope** — default 4 groups; what happens when a customer
   exceeds it (reject new group with a Chinese message, or raise the ceiling)?
2. **Push authorization** — both repos are committed locally only.
3. **Native soak test** — schedule a real-account daemon soak before switching
   the production desktop to native signal-cli.

## 7. Key files

- Plan: `docs/optimization-plan.md`, `docs/handover.md`
- Architecture decision: `docs/adr/0001-jvm-per-proxy-group.md`
- Contract: `schemas/connector-api-v1.schema.json`, `docs/implementation-plan.md` §4.4
- Desktop contract: `contracts/signal-host-adapter.md` (1.4 once Phase 4 lands)
- CI: `.github/workflows/build-signal-cli-native.yml`, `packaging/scripts/build-signal-cli-native.sh`
- Desktop smoke: `scripts/signal-smoke-cdp.mjs`

## 8. Exact git state at handover (2026-08-26)

### connector — `main`, not pushed

Committed (top 3 are this session's; cf9383/b2ff296 were already local before):
```
d59dce4 feat: 落地优化方案 Phase 0-3 并支持 signal-cli native 模式
dc355a4 ci: 新增 signal-cli GraalVM native 构建管线
cf9c696 docs: 新增 Signal 集成优化方案与分阶段执行计划
cfb9383 fix(host): treat a vanished host as a session end, not a broken protocol
```
Uncommitted (Phase 4 design + docs):
```
 M AGENTS.md
 M docs/implementation-plan.md
 M docs/optimization-plan.md
 M schemas/connector-api-v1.schema.json
?? docs/adr/0001-jvm-per-proxy-group.md
?? docs/handover.md
```

### desktop — `codex/signal-test-main-latest`, not pushed

Committed (top 4 are this session's; d4de1d24 was already local before):
```
4cd0b6a0 docs(signal): 契约升 1.3 记录信封、错误码与落盘加密
74415a30 test(signal): 新增 CDP 冒烟基线脚本
ca2d2465 feat(signal): 本地存储落盘加密与 store key 托管
4d77f6c1 feat(signal): IPC 命令统一信封返回并按错误码映射文案
d4de1d24 docs(signal): record the shutdown race as found and fixed
```
Uncommitted — **our Phase 4 work**:
```
 M contracts/signal-host-adapter.md
 M electron/main/signal/connectorSupervisor.ts
 M electron/main/signal/eventRouter.ts
 M electron/main/signal/hostAdapter.ts
 M electron/main/signal/proxyProbe.ts
 M electron/main/signal/sessionHost.ts   ← 3 TS errors fixed 2026-08-26
 M electron/main/signal/sessionRegistry.ts
 M electron/main/signal/types.ts
 M src/view/signal/SignalWorkspace.vue
?? electron/main/signal/proxyGroups.ts
```
**Someone else's WIP — must stay untouched and uncommitted:**
`src/view/signal/signalFriendRemark.ts`, `tests/unit/signalFriendRemark.spec.ts`,
`src/apis/friendApis.ts`, `src/libs/AppType.ts`, `src/view/home/Home.vue`,
`src/view/home/components/FriendEditModal.vue`,
`tests/unit/signalQuickReplyComponent.spec.ts`, plus ~14 WIP hunks inside
`SignalWorkspace.vue` (remarkRows card, openRemark series, sg-remark CSS).
They cause the 2 `typecheck:signal` reds and 1 `test:unit` red that are baseline
and not ours.