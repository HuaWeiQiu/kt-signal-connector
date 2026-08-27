# Signal Integration — Handover

Date: 2026-08-26 (updated 2026-08-27 00:30 +08:00). Owner: KT AI engineering.
Status: **Phase 4 (JVM-per-proxy-group) resumed, implemented and contract-pinned
locally; nothing pushed. 8-engine soak running (ends ~2026-08-27 20:44 +08:00);
P0 real-device acceptance: all machine-checkable items pass (§3.3).**

---

## 0. Current state at a glance (read this first)

| Item | State | Evidence |
| --- | --- | --- |
| Phase 4 design | committed | connector `1749ed3` (ADR 0001, §4.4, schema additive) |
| Phase 4 implementation | committed | connector `7996828` (one engine per proxy group) |
| A · contract pinning | committed | connector `c742fc9`; desktop `387400c5` (contract 1.5) |
| B · soak tier-1 (8 engines, no real accounts) | **re-run in progress** since 2026-08-27 20:52 +08:00 (run 1: INCOMPLETE at ~4.7/24 h, host teardown, window healthy) | `/tmp/kt-soak-8g/`; see §3.2 |
| B · soak tier-2 (real accounts) | **blocked** — needs a 2nd real account + phone | §6 |
| C · P0 real-device acceptance | **machine-checkable items all pass** (2026-08-27 probe round: items 1-4 pass, 5 chain pass + ASR needs-human, 6 blocked on product decision); smoke 11 pass / 0 fail / 1 skip | §3.3 |
| D · push both repos | authorized by owner, **waits for C to pass** | §3.4 |

Working trees: connector `main` is **clean** at `c742fc9`. Desktop
`codex/signal-test-main-latest` HEAD is `387400c5`; the only uncommitted files
are **someone else's WIP** (see §5) — do not commit them.

## 1. What was done before Phase 4 (unchanged record)

### kt-signal-connector (Rust, `main`)

| Phase | What landed | Proof |
| --- | --- | --- |
| 0 | `tests/schema_consistency.rs` (methods/events/error-codes machine diff with explicit allowlist) + happy-path smoke baseline; desktop CDP smoke harness `yarn smoke:signal-cdp` | gates green on unmodified code |
| 1 | Liveness deadlock fix (bounded receive `enqueue`, `shutdown` total-timeout + hard kill, host teardown timeout); `sync_contacts` single-transaction batch upsert; account-delete / in-flight-send drain barrier; `INTERNAL_ERROR retryable=true` mislabel; `tracing` + redacted metrics (`src/metrics.rs`) | 163 tests green ×3; `runtime.resourcePressure` registered in schema |
| 2 | `quoteMessageId` upstream delivery (local row → `quoteTimestamp`/`quoteAuthor`); `message.statusChanged` producer + renderer consumer; `MessageRecord.attachments` removed from wire; IPC `{ok, error}` envelope; 31 error-code → Chinese copy map | joint smoke 11/11, statusChanged event +3756 ms ahead of DOM convergence |
| 3 | SQLite → SQLCipher (`Store::open`); bootstrap channel extended to two lines (`hexSecret\nhexKey`); `KT_SIGNAL_STORE_KEY` dev override; plaintext → encrypted online migration with 7-day backup; fail-closed on missing key | real-profile migration verified (row counts identical, backup present, key reused on restart, wrong-key fail-closed) |
| 5 | `SignalCliMode::Native`; GraalVM native-image CI pipeline (`.github/workflows/build-signal-cli-native.yml` + `packaging/scripts/*`) | 112.8 MiB binary; RSS 265→136 MiB (−48%); cold start 6.25→4.99 s; smoke 11/11 on real account |

Wire-protocol changes (Phase 2) are breaking and were shipped as one batch on
both sides; the `apiVersion` handshake hard-fails mismatches. Phase 3/4/5 made
no breaking wire changes (Phase 4 is additive v1 evolution — see schema).

## 2. Phase 4 landed (2026-08-26, resumed after the gateway outage)

The two implementation agents killed by the gateway outage were re-dispatched
from the design docs; Phase 4 is now complete on both repos:

- **Connector** `1749ed3` design (`docs/adr/0001-jvm-per-proxy-group.md`,
  `docs/implementation-plan.md` §4.4, additive schema) → `7996828`
  implementation (one signal-cli engine per proxy group, 8-group ceiling,
  `proxyGroup.stateChanged` event, `PROXY_GROUP_NOT_FOUND`) → `c742fc9`
  contract-pinning tests (cross-source config, dormant groups, expired finish,
  delete replay).
- **Desktop** `c03dc355` host contract 1.4 → `ef160a8b` link locked to proxy
  group + per-group runtime display → `387400c5` error-code formalization +
  dormant-group copy (contract **1.5**).
- `tests/schema_consistency.rs` is **green again** — the two intentional reds
  (`PROXY_GROUP_NOT_FOUND`, `proxyGroup.stateChanged`) closed when the
  implementation landed.

## 3. 遗留项修复方案 (A/B/C/D) — execution state

### 3.1 A · 契约钉死 — done

- connector `c742fc9` pins the proxy-group contract gaps with tests:
  cross-source proxy config rejection, dormant group visibility, expired
  `link.finish`, delete replay.
- desktop `387400c5` formalizes the new error codes and dormant-group UI copy;
  host contract bumped to **1.5** (`contracts/signal-host-adapter.md`).

### 3.2 B · 24h soak — tier-1 **INCOMPLETE (host teardown, observed window healthy)**, tier-2 blocked

**Result of the 2026-08-27 20:37 check:** the soak did **not** reach 24 h. All
processes (driver, connector pid 86084, engines 86092–86099) are gone; the last
sample is **2026-08-27 01:24 +08:00**, i.e. **~4.7 h of the planned 24 h**.
Nothing in the data indicates a product failure — the interruption matches a
host/session teardown (this also killed the originating AI session and its
cron; a successor session ran this check by hand, cron id
`01M0ZFRPFTRKPKG754Y7SDYB9E`). The 24 h gate is therefore **not met** and the
soak must be re-run to count.

Observed window (20:44 → 01:24) was healthy on every criterion:

- Every `driver.log` status line: 8/8 engines `state=running`,
  `resourcePressure=False`, aggregate RSS steady ≈ 1.32 GiB. Two sampling gaps
  (22:49→23:16, 00:12→00:32) are consistent with brief host sleep, not engine
  trouble.
- Per-engine RSS over 240 samples each, first→last / max MiB: default
  161→172/173, g1 170→170/170, g2 170→182/182, g3 144→152/152, g4
  179→179/179, g5 176→175/176, g6 156→167/168, g7 151→150/151 — flat or
  plateau, **no monotonic climb**, no pressure event.

**Re-run started 2026-08-27 20:52 +08:00** — connector pid 14497, engines
14501–14508, handshake ok, 8/8 `running` at launch. This time the driver and
connector are guarded by `caffeinate -dims -w <pid>` (pids 15259/15260) so an
idle-sleeping host cannot silently end the run; run-1 artifacts are archived
under `/tmp/kt-soak-8g/run1/`. One-shot check cron `01M11MKQ8METJ1W79QNF32RRS4`
fires 2026-08-28 21:07 +08:00 (session-local — if this session is gone, check
by hand with the commands below).

<details><summary>Original run record (2026-08-26)</summary>

Tier-1 (no real Signal accounts; 8 signal-cli engines at the group ceiling,
lifecycle + RSS only):

- Started **2026-08-26 20:44 +08:00**, planned 24 h (ends ~2026-08-27 20:44).
- Artifacts: `/tmp/kt-soak-8g/` — `soak_driver.py` (driver), `driver.log`
  (5-min status lines), `rss.csv` (per-process RSS samples), `launch.sh`.
- At +1.3 h: 8/8 engines `running`, aggregate RSS ≈ 1.32 GiB,
  `resourcePressure=False` throughout.
- A one-shot cron check exists **in the originating AI session only**
  (id `01M0Z05NR5CSNJA6T42H5SF103`, fires 2026-08-27 20:35 +08:00). Cron tasks
  die with the session — **if you are a new session/human, run the check by
  hand**:

```bash
tail -20 /tmp/kt-soak-8g/driver.log   # every status line must show state 'running'
python3 - <<'EOF'
import csv
rows = list(csv.DictReader(open('/tmp/kt-soak-8g/rss.csv')))
engines = {}
for r in rows:
    if r['role'] == 'engine':
        engines.setdefault(r['group_id'], []).append(int(r['rss_kb']))
for g, v in sorted(engines.items()):
    print(g, 'samples:', len(v), 'first/last/max MiB:',
          v[0]//1024, v[-1]//1024, max(v)//1024)
EOF
```

Pass criteria: all 8 engines `running` in every status line; per-engine RSS
without unbounded growth (flat or plateau, no monotonic climb); no
`resourcePressure=True`.

</details>

Tier-2 (real accounts, real traffic) is **blocked**: needs a second real
Signal account and a phone to approve linking. Do not fake this with the
single existing test account — it has no peer that replies (see §5).

### 3.3 C · P0 real-device acceptance — machine-checkable items all pass (2026-08-27)

**Probe round 2026-08-27** (dev client from `/tmp/kt-p0-test`, Vite 3355 / CDP 9336,
page `http://localhost:3355`; driven via CDP `Runtime.evaluate` DOM events +
`Input.dispatchMouseEvent`; the stop-channel `window.confirm` was accepted via
`Page.handleJavaScriptDialog`; probe scripts `p0-probe1/2/3/4/5.mjs` in the
worktree root):

1. **Text quick-reply dblclick — PASS.** QuickReply panel 「文本」entry dblclick →
   new outgoing bubble (`data-total-items` 18→19), draft cleared, no warning
   toast; real send to 「菲菲 林」. No `send2webview` on the Signal path.
2. **Media quick-reply rejection — PASS (QuickReply path).** Video entry dblclick
   → toast「Signal 暂不支持发送图片、语音或文件快捷回复」, total unchanged,
   draft marker byte-identical. SmartReply panel had **no media entries** (AI
   replies text-only in this profile) — the string-type media path has no
   on-machine data source and remains covered by the `signalQuickReply`
   whitelist unit tests only.
3. **Stopped-channel rejection — PASS.** Stop via menu → confirm dialog →
   「通道已停止」banner + disabled composer; dblclick text entry →
   toast「当前 Signal 会话无法发送，请确认已选择聊天且通道可用」, total and
   draft unchanged; start channel → 「已连接 · +86***50」, draft intact.
4. **AI / toUpScreen — PASS.** SmartReply AI round-trip (typed question → AI
   bubble) then send-arrow on own bubble → text landed in composer only,
   `data-total-items` unchanged, nothing sent.
5. **Voice refill — chain PASS, capture+ASR needs-human.** `.sg-voice` click
   (trusted coords) → `#/record` window opened and cancelled cleanly, main
   composer untouched. Record window shows「无配额-请联系管理员」— the ASR
   backend has no quota, so mic capture → recognition → refill is
   **needs-human + needs quota**. Cross-conversation draft isolation is
   untestable on this profile: only one conversation exists.
6. **KT remark — still blocked** (WIP replaced the L1 UI; product decision
   pending, see below).

Environment note: the dev client is left **running** with the channel **ready**
(`bx y · +86***50`, conversation 「菲菲 林」, draft cleared, total 19 = +1 real
probe send from item 1).

<details><summary>Previous state (2026-08-26): analysis phase</summary>

Environment (keep alive until C finishes):

- Clean test worktree `/tmp/kt-p0-test` (detached at desktop `ef160a8b`;
  `node_modules` is a symlink into the main worktree). Dev client runs from
  there: Vite **3355**, CDP **9336**, page URL `http://localhost:3355`
  (not 127.0.0.1 — the CDP target list keys on `localhost`).
- Test profile `/Users/tanye/.kt-desktop-signal-daemon-test`.
  **It also contains real WhatsApp sessions.** Before double-clicking any
  quick reply, assert the selected session is the Signal one, or the message
  goes into a real WhatsApp conversation. This is the biggest risk of the
  probe work.
- Probe scripts: `/tmp/kt-p0-test/p0-l1-probe.mjs`, `p0-probe2.mjs`
  (run them from inside the worktree so the `ws` package resolves; CDP
  `Runtime.evaluate` mode is proven working).

Done so far:

- Real-send smoke (earlier): **11 pass / 0 fail / 1 skip**. The skip is
  inbound rendering — the peer never replies and the compose list has no
  Note to Self, so P1-18 remains without a data source.
- Dispatch-path analysis (static, verified against current source):
  - `Home.vue:2625-2650` `dbClickReply` handler: Signal native sessions branch
    at `2634` (`isNativeSignalSession`) into `dispatchNativeSignalQuickReply
    (data, "send")` → `signalQuickReply.ts` `extractSignalQuickReplyText`
    (whitelist: TEXT/PROMPT/ASISSTANT only; `files` non-empty or media type →
    warning, no send) → `SignalWorkspace.sendPreparedText`. **No
    `send2webview` on the Signal path.** The debug double-send at
    `2646-2649` only exists on the non-Signal branch.
  - `Home.vue:2664-2684` `toUpScreen`: same branch, mode `"draft"` →
    `SignalWorkspace.insertDraft` (draft only, never sends).
  - Quick-reply re-entry lock: `QUICK_REPLY_LOCK_MS = 500`
    (`Home.vue:309`), key `sessionUid::data.id ?? data.text` — space repeated
    probe triggers >500 ms apart or vary the id.
  - `SmartReply.vue:797-810` `handleSend` emits `{type:
    message.type === "text" ? 1 : message.type, text}` — media replies arrive
    with **string** types (`'image'` etc.), which is exactly why the
    extraction is a whitelist.
  - Quick-reply panel renders via `Home.vue:62` `ReplySlide` →
    `ReplySlide.vue:21/39` `QuickReply`/`SmartReply` inside `VerticalIconTabs`
    with `:default-collapsed="true"` — the panel starts collapsed, which is
    why visibility-filtered DOM probes find nothing.
  - `eventBus` is a private `mitt` instance (`src/libs/eventBus.ts`), **not
    on `window`** — CDP cannot emit `dbClickReply` directly; drive it through
    real UI events (expand the right tab rail, dblclick an entry) or through
    the Vue component tree.

Remaining for C (do in this order):

1. Text quick-reply dblclick on the Signal session → outbound bubble appears,
   no `send2webview("dbClickReply")` (assert via behavior: bubble + no webview
   channel errors; there is no webview for native Signal anyway).
2. Media quick-reply dblclick → warning toast "Signal 暂不支持发送图片、语音
   或文件快捷回复", outbound count unchanged, draft untouched. Repeat through
   the **SmartReply** panel specifically (string types were the historical
   fail-open).
3. Dblclick while channel stopped / account logged out → "无法发送" warning
   and the existing draft is **not** overwritten.
4. AI / toUpScreen path → text lands in composer only, nothing sent.
5. Voice mic refill → lands in the current conversation's composer; after
   switching conversations it must not write into the previous one. If CDP
   cannot drive the mic, mark **needs-human** honestly.
6. KT remark — **scope changed, see below.**

**KT remark scope change (important):** the uncommitted WIP in this worktree
**deleted** the L1 title-bar remark input (`.sg-kt-remark-input`) and replaced
it with a `.sg-remark-entry` button → `openRemark()` → `open-remark` event →
`Home.vue:256` `onOpenSignalRemark` → `FriendEditModal` (signalFriendRemark
store, `util.saveContacts`, and a `if (!isSignalSession)` guard before any
`send2webview("setRemark")` at `FriendEditModal.vue:652/659`). The L1 store
`signalKtRemark.ts` is still imported but only for `hydrate`/`clear` — it has
**no write UI left**. Consequences:

- The P0 item "标题栏改 KT 备注" cannot be verified as originally designed
  while the WIP sits in the tree. Either stash the WIP to test the L1 UI, or
  accept that the remark feature is now the WIP author's deliverable.
- Do not "fix" this by editing the WIP files — they are byte-frozen (§5).
- Product decision needed: L1 local remark vs WIP friend-remark — keep both,
  merge, or drop one. Recorded as an open item in §6.

</details>

### 3.4 D · push — authorized, runs after C passes

Order matters (AGPL: source availability precedes any binary distribution):

1. Desktop: `git push` the integration branch to origin (needs the owner's
   target remote/branch confirmation if not obvious from `git remote -v`).
2. Connector: push `main` to the public repo `HuaWeiQiu/kt-signal-connector`
   and tag the release. Connector is AGPL-3.0-only; Desktop consumes it via
   versioned socket/JSON IPC only.
3. After push, update both handover docs with the remote refs.

## 4. How to verify (per repo)

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

Joint verification is the gate that has caught real bugs invisible to unit
tests (payload trailing-newline mismatch, `START_FAILED` code mapping gap,
`message.statusChanged` consumer gap). Run it after every phase that touches
both repos.

## 5. Known open items

- **Someone else's WIP in the desktop worktree — frozen, never commit, never
  edit:** `src/view/signal/signalFriendRemark.ts`,
  `tests/unit/signalFriendRemark.spec.ts`, `src/apis/friendApis.ts`,
  `src/libs/AppType.ts`, `src/view/home/Home.vue`,
  `src/view/home/components/FriendEditModal.vue`,
  `tests/unit/signalQuickReplyComponent.spec.ts`, plus the remark hunks inside
  `SignalWorkspace.vue` (remark-entry button, remarkRows card, openRemark
  series, sg-remark CSS). They cause the 2 `typecheck:signal` reds and 1
  `test:unit` red that are baseline and not ours. **They also replaced the L1
  KT-remark title-bar UI — see §3.3.**
- **Receive inbound rendering** never verified end-to-end: the only linked
  test account's peer does not reply, and there is no Note to Self to
  self-test (P1-18).
- **`Enter` to send is flaky under CDP** (button path is 100% reliable);
  suspected real-machine feel issue — P0 to verify by hand.
- **link QR / quote UI / `unknown`/`failed` status values** not exercised on
  machine.
- **Phase 4 long-run soak**: tier-1 running (§3.2); multi-group RSS curve at
  the 8-group ceiling will be answered by it; tier-2 blocked.
- **Native long-run soak**: only ~1 min window observed.
- **`scripts/signal-real-e2e-cdp.mjs`** and the `.tmp/` probes are test
  scaffolding; `.command` files are hand-edited per run. `/tmp/kt-p0-test`
  probes (§3.3) are likewise scaffolding — delete with the worktree when C
  finishes.

## 6. Pending decisions (owner)

1. **Phase 4 rollout scope** — default 4 groups; what happens when a customer
   exceeds it (reject new group with a Chinese message, or raise the ceiling)?
2. **Dormant-group visibility & deletion semantics** — two suggested behavior
   changes came out of the contract-pinning review (whether dormant groups
   should be listed, and whether deleting a group with dormant accounts should
   be allowed); needs a product call before any code change.
3. **L1 KT remark vs WIP friend remark** — see §3.3; keep/merge/drop.
4. **Tier-2 soak** — needs a second real account + phone.
5. **Push targets** — D is authorized; confirm exact remote/branch names at
   execution time.

## 7. Key files

- Plan: `docs/optimization-plan.md`, `docs/handover.md` (this file)
- Architecture decision: `docs/adr/0001-jvm-per-proxy-group.md`
- Contract: `schemas/connector-api-v1.schema.json`, `docs/implementation-plan.md` §4.4
- Desktop contract: `contracts/signal-host-adapter.md` (**1.5** as of `387400c5`)
- CI: `.github/workflows/build-signal-cli-native.yml`, `packaging/scripts/build-signal-cli-native.sh`
- Desktop smoke: `scripts/signal-smoke-cdp.mjs`
- Desktop handover (long-form): `docs/plan/signal-handoff-next-owner.md` in
  the desktop worktree — its §6.27 mirrors this file's §3.

## 8. Exact git state (2026-08-26 22:10 +08:00)

### connector — `main`, working tree clean, not pushed

```
c742fc9 test: 钉死代理组契约缺口（跨源配置、dormant 组、过期 finish、delete 重放）
7996828 feat: 落地 Phase 4 代理组模型（每组一个 signal-cli 引擎）
1749ed3 docs: 定稿 Phase 4 代理组契约与设计（ADR 0001、§4.4、schema 增量）
d59dce4 feat: 落地优化方案 Phase 0-3 并支持 signal-cli native 模式
dc355a4 ci: 新增 signal-cli GraalVM native 构建管线
cf9c696 docs: 新增 Signal 集成优化方案与分阶段执行计划
cfb9383 fix(host): treat a vanished host as a session end, not a broken protocol
```

### desktop — `codex/signal-test-main-latest`, not pushed

```
387400c5 feat(signal): 代理组错误码正式化与 dormant 组文案（契约 1.5）
ef160a8b feat(signal): 链接锁定代理组并按组展示运行状态（Phase 4）
c03dc355 docs(signal): host 契约升 1.4，增补代理组条款（§5.8）
4cd0b6a0 docs(signal): 契约升 1.3 记录信封、错误码与落盘加密
74415a30 test(signal): 新增 CDP 冒烟基线脚本
ca2d2465 feat(signal): 本地存储落盘加密与 store key 托管
4d77f6c1 feat(signal): IPC 命令统一信封返回并按错误码映射文案
d4de1d24 docs(signal): record the shutdown race as found and fixed
```

Uncommitted: **only** the frozen WIP listed in §5 (6 modified + 2 untracked).
