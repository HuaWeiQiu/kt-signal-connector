# messages.remoteDelete 协议层变更方案（L2）

- 起草日期：2026-09-02
- 仓库基线：`main` @ `f8fc99a`（本方案只新增本文档，不改任何代码/schema；落盘后不 commit）
- 上游基线：unmodified `signal-cli v0.14.7`（docs/implementation-plan.md §1）
- 性质：实施前设计文档。按 KT 既定纪律执行：**先方案文档 → 后 host 合同 → 再 schema/实现**。

---

## 0. 现状核实（先读后写，全部为仓库实况）

撰写本方案前已核实以下事实，后文引用均以此为准：

| 事实 | 出处 |
|---|---|
| wire 层 `apiVersion` 常量为 `"1.0"` | `src/lib.rs:21`；`schemas/connector-api-v1.schema.json:11-13`（`apiVersion` 为 const `"1.0"`） |
| envelope 校验拒绝一切非 `"1.0"` 的 apiVersion | `src/protocol.rs:21-28`（返回 `UNSUPPORTED_VERSION`） |
| Phase 1 定调 "supports exactly API 1.0"；Phase 4 确立 "evolve API 1.0 in place; the apiVersion handshake binding is unchanged" 的原地加法演进先例 | `docs/implementation-plan.md` §4.1、§4.4 |
| 能力清单 `PHASE2_CAPABILITIES` 共 14 个方法（即任务所称"现契约 14 个方法"），无任何删除类方法 | `src/lib.rs:31-46` |
| schema 方法枚举为 15 项 = 14 能力 + `handshake` | `schemas/connector-api-v1.schema.json:38-54` |
| handshake 结果携带 `capabilities` 数组（host 特性探测入口） | `src/host.rs:488-492` |
| 未知结局纪律：unknown outcome 绝不自动重试；未知结局一律 `*_OUTCOME_UNKNOWN` 家族错误码 + 本地状态标记 | `AGENTS.md:34`、`docs/signal-cli-upgrade.md` §6、`src/supervisor.rs` `send_text`/`delete_local_account`、`src/engine.rs:665-678` |
| **全库未找到任何 "1.5" 契约版本标记**（grep `docs/ src/ schemas/`） | — |

### 0.1 关于"契约版本 1.5 → 1.6"的前提差异（必须说明）

任务背景称"connector 契约版本 1.5（schemas/connector-api-v1.schema.json 当前版本）"。核实结果：
该 schema 文件与代码中的 wire 版本均为 `"1.0"`，仓库内不存在 1.5 编号体系（可能 1.5/1.6 编号
存在于 desktop 侧的 host 合同文档，本仓库无法核实）。

**决策**：遵循本仓库自己的演进先例（implementation-plan §4.4：apiVersion 握手绑定不变、契约原地
加法演进）——

- wire `apiVersion` 保持 `"1.0"`（`src/protocol.rs:22` 的等值校验不动，不破坏现有 host）；
- "契约 1.6" 以 **contract revision 1.6** 落地：schema 增加方法、`PHASE2_CAPABILITIES` 增加能力、
  `docs/implementation-plan.md` 新增 "§4.5 messages.remoteDelete (contract revision 1.6)" 小节；
- host 侧通过 handshake `capabilities` 数组探测 `messages.remoteDelete`（`src/host.rs:488-492`），
  与 `link.start` 的 `proxyGroup` 特性探测先例同型（schema 中的探测指引见
  `schemas/connector-api-v1.schema.json:220`）。

若 desktop 侧 host 合同确有连续小版本编号，与本仓库 revision 编号对齐即可，不影响 wire 行为。

---

## 1. 目标与非目标

### 1.1 目标

1. 为 host 提供与官方 "Delete for everyone" 一一对应的显式删除方法 `messages.remoteDelete`：
   仅针对**本账号自己发出、且上游确认过协议身份**的消息（Signal 官方窗口为 24 小时内），
   **best effort**；引用该消息的其它消息不会被删除（上游语义，契约原文照录）。
2. 未知结局显式化：变异上游调用的一切不确定路径映射为响应 `{"status":"unknown"}`，完整复用
   仓库现行 UNKNOWN_OUTCOME 语义（绝不自动重试；host 只能显式决策重试或对账）。
3. 零存储迁移、零新增错误码、零新增事件：完全复用既有 lane、每账号预算、删除屏障与可观测性
   设施，把 blast radius 压在一个方法内（一次只加一个 connector 方法）。

### 1.2 非目标

1. **admin delete**（群主/管理员删除他人消息）：上游无对应公开能力，本契约不承载。
2. **本地删除的 UI 细节**：由 desktop 侧文档承载。1.6 中 connector 对本地 `messages` 行
   **不做任何删改**（理由与后果见 §3.6）；desktop 收到 `status:"deleted"` 后如何呈现/记账是
   host 合同与 desktop 文档的事。
3. **故事/群管理**：stories 已被 `--ignore-stories` 排除在引擎之外（`src/engine.rs:188`）；
   群成员/群信息管理不在本方法范围。
4. **入站 remoteDelete 同步信封的本地收敛**（对端或本账号其它设备删除消息时，signal-cli 以
   无正文 dataMessage 推送删除事件）：当前 `normalize_receive` 将无正文 dataMessage 归为
   `skip` 丢弃（`src/engine.rs:1205-1229`），1.6 不改此行为；跨设备/对端删除的本地呈现留待
   后续契约版本单独立项（一次只加一个方法的纪律）。
5. **删除操作持久化账本**：不为 `operationId` 建账本表（取舍见 §2.3）。

---

## 2. 契约变更（contract revision 1.6）

### 2.1 新增方法与 params

`request.method` 枚举新增 `messages.remoteDelete`（排在 `messages.sendText` 之后，
`schemas/connector-api-v1.schema.json:38-54`）；`request.allOf` 新增对应 if/then 分支；
新增 `$defs.messagesRemoteDeleteParams`：

```json
{
  "type": "object",
  "additionalProperties": false,
  "required": ["accountId", "conversationId", "messageId"],
  "properties": {
    "accountId":      { "$ref": "#/$defs/opaqueId" },
    "conversationId": { "$ref": "#/$defs/opaqueId" },
    "messageId":      { "$ref": "#/$defs/opaqueId" },
    "operationId":    { "$ref": "#/$defs/opaqueId" }
  }
}
```

- 形状与 `messageGetTextParams` 同构（`schemas/connector-api-v1.schema.json:321-330`），
  `operationId` 可选项借鉴 `accountDeleteLocalDataParams`（`schemas/connector-api-v1.schema.json:224-232`）。
- params 描述文字须写明：仅 `status:"sent"` 的本人 outgoing 消息可删；上游语义为
  Delete for everyone（24h 窗口、best effort、引用不删除）。
- 寻址只接受 `conversationId`（不像 `messages.sendText` 那样支持 kind+peerKey 直达）：
  删除的目标必须已存在于本地历史，凭空寻址没有意义，也让 messageId→协议身份的解析路径唯一。

### 2.2 响应与 UNKNOWN_OUTCOME 语义

成功结果（非错误）携带 `status` 字段：

- `{"status":"deleted"}`：signal-cli 对 `remoteDelete` 返回成功，删除已获上游确认。
- `{"status":"unknown"}`：上游调用结局不可判定。语义与现行 `*_OUTCOME_UNKNOWN` 完全一致：
  **retryable=false 等价物——connector 绝不自动重试**（`AGENTS.md:34`），host 收到后要么放弃、
  要么用**新 requestId + 同 operationId** 显式重发；真实结果以对端呈现为准（best effort）。

engine 层保证：`CallClass::Mutating` 下，超时、stdin 写失败、引擎中途退出、result/error 歧义
一律收敛为 `EngineError::UnknownOutcome`（`src/engine.rs:665-678`、`819-827`、`990-998`、
terminal 洩漏路径 `914-919`），supervisor 将该变体映射为 `{"status":"unknown"}`——与
`send_text` 的 unknown 路径同型（`src/supervisor.rs:1107-1121`），只是出口从错误码改为
响应字段（响应形态为任务指定，见 §2.3 取舍）。

schema 中 `response.result` 本就是自由形状（`schemas/connector-api-v1.schema.json:338`），
result 形状不进 schema 枚举，写入 `docs/implementation-plan.md` §4.5——与全部现有方法的
result 处理方式一致。

### 2.3 错误码取舍：不新增任何错误码

任务允许在 `REMOTE_DELETE_NOT_FOUND` 与复用现有之间取舍，结论：**全部复用现有，新增数为零**。

| 场景 | 错误码 | 说明 |
|---|---|---|
| 本地无此账号 | `ACCOUNT_NOT_FOUND` | 现有映射（`src/service.rs:49-51`） |
| 本地无此会话 | `CONVERSATION_NOT_FOUND` | 现有映射（`src/service.rs:52-56`） |
| 本地无此消息行，**或**行不满足资格守卫（非本人 outgoing / 非 `sent` 终态） | `MESSAGE_NOT_FOUND` | 取舍见下 |
| runtime 未运行 | `RUNTIME_NOT_RUNNING` | `src/service.rs:71-75` |
| 请求形状/字段非法 | `INVALID_REQUEST` | 反序列化 `deny_unknown_fields` 失败路径 |
| 上游明确拒绝（超 24h 窗口、消息已被删、非可删目标等） | `UPSTREAM_ERROR` | 沿用现有映射（`src/service.rs:90-92`）；其 `retryable=true` 是全局既有映射，不在本方法内特判，方案如实记录 |
| 请求未及写入（engine 已停） | `UPSTREAM_EXITED` | 与 sendText 同一边缘路径（`src/engine.rs:814-817`），请求未达上游、结局确定 |

**为什么复用 `MESSAGE_NOT_FOUND` 而不新增 `REMOTE_DELETE_NOT_FOUND`**：唯一可区分的"找不到"
就是本地行查找；pending/failed/unknown 状态的 outgoing 行**没有上游协议身份**（其 `sent_at`
是本地时钟，见 §3.2），对本方法而言与"行不存在"不可区分——专用错误码是 1:1 重复，只会扩大
错误枚举并迫使 `tests/schema_consistency.rs:174-189` 增加无信息量的对齐项。

**为什么 unknown 走响应字段而非新错误码**：响应形态（`status: deleted|unknown`）为任务指定，
且 `message.statusChanged` 事件里 `"unknown"` 作为消息状态值已是既有 wire 概念
（`schemas/connector-api-v1.schema.json:427-445`），语义同源。代价是 host 层指标会把该响应
计为 `"ok"`（`src/metrics.rs:39-45` 只认 `*OUTCOME_UNKNOWN` 错误码），已知观测性缺口的补偿
见 §3.5。

**`operationId` 在 1.6 的语义**：host 生成的关联/幂等 ID，connector 仅做 opaqueId 形状校验，
**不落库**。理由：与账号删除不同，remoteDelete 上游重放是 best-effort 幂等的（对已删消息再删
最坏是一次良性 `UPSTREAM_ERROR`），不是"二次发送/二次绑设备"那种危险重复，为此引入账本表
（需要 store schema 迁移，`SCHEMA_VERSION` 7→8 + 新表，`src/store.rs:15`、`2171-2229`）不成比例。
备选方案（有界账本 + 重放应答缓存，参照 `account_delete_operations` 表与
`MAX_COMPLETED_ACCOUNT_DELETE_OPERATIONS=256`，`src/store.rs:16`）明确记为 1.7 候选项，
触发条件：soak 观察到上游对重复删除返回硬错误（见 §6）。

### 2.4 版本落点汇总

- `schemas/connector-api-v1.schema.json`：method 枚举 + allOf 分支 + `$defs.messagesRemoteDeleteParams`（含描述）。
- `src/lib.rs:31-46`：`PHASE2_CAPABILITIES` 追加 `"messages.remoteDelete"`（handshake 可探测）。
- `docs/implementation-plan.md`：新增 §4.5，标题带 `(contract revision 1.6, 2026-09-02)`，
  沿用 §4.4 的 dated-contract 记录法。
- wire `apiVersion` 不变（§0.1 决策）。

---

## 3. connector 实现要点（Rust）

### 3.1 调用链（对齐现有结构）

```
host.rs dispatch 新 arm（"messages.remoteDelete"，紧邻 messages.sendText arm，src/host.rs:1014 后）
  → serde 反序列化 MessagesRemoteDeleteParams（deny_unknown_fields，同 src/service.rs:966-973 风格）
  → ProxyGroupRuntime::remote_delete → slot_for_account 路由（src/registry.rs:476-492）
  → RuntimeSupervisor::remote_delete
  → ConnectorService::prepare_remote_delete（纯本地校验 + 组装上游 params）
  → engine.call("remoteDelete", params, CallClass::Mutating)（同型于 src/supervisor.rs:1090）
  → Ok ⇒ {"status":"deleted"}；UnknownOutcome ⇒ {"status":"unknown"}；其余 ⇒ into_api()
```

与 `send_text`（`src/supervisor.rs:1038-1135`）同构：先本地 prepare（持 service 锁），后上游
调用（不持锁）。**不新增本地 pending 行、不新增事件**（remoteDelete 不改变本地行状态，见 §3.6）。

### 3.2 messageId → (target-timestamp, recipient) 映射

先读 `src/store.rs` 确认的消息行结构（`messages` DDL，`src/store.rs:65-82`；读取函数
`message_by_id`，`src/store.rs:1169-1185`，返回 `MessageRecord`，`src/store.rs:284-306`）：

| 上游 remoteDelete 参数 | 本地来源 | 依据 |
|---|---|---|
| `account` | `accounts.signal_account`（`AccountRow`，`src/store.rs:336-340`；`account_by_id`，`src/store.rs:587-602`） | 与 send 相同的账号寻址（`src/service.rs:619`） |
| `targetTimestamp` | `messages.sent_at`，**仅当 `status=='sent'`** | 本人已发消息的 `sent_at` 在发送完成时被上游时间戳覆盖：`complete_outgoing_send`（`src/store.rs:1456-1461`）写入 send 响应的 `result.timestamp`（`src/supervisor.rs:1092-1095`）。pending/failed/unknown 行的 `sent_at` 是本地 `now_ms()`（`src/service.rs:596`），不是 Signal 协议身份——**先例**：引用解析只信 `status=="sent"` 的 outgoing 行（`resolve_quote`，`src/service.rs:659-666, 677`） |
| `recipient`（direct） | `conversations.peer_key`（kind=='direct'），组装为 `recipient:[peer_key]` | 与 send 完全同型（`src/service.rs:625`） |
| `groupId`（group） | `conversations.peer_key`（kind=='group'） | 与 send 完全同型（`src/service.rs:622-623`）；`ConversationRow` 见 `src/store.rs:342-348`、`conversation_by_id` `src/store.rs:927-948` |

**资格守卫**（prepare 阶段确定性执行，不打上游）：`direction=='outgoing' && status=='sent'`，
否则返回 `MESSAGE_NOT_FOUND`。incoming 行虽然 `sent_at` 也是信封时间戳，但"删别人的消息"属于
admin delete（非目标 §1.2.1），上游亦会拒绝。

note-to-self：本仓库未见专门的 note-to-self 会话形态；若 peer_key 等于本机号的 direct 会话
存在，按 recipient 寻址并在实施时按 §6 验证项确认上游是否需要专用形态（CLI 面为
`--note-to-self`）。

### 3.3 schema 迁移结论：**不需要**

请求映射所需字段全部现成：`messages.sent_at/direction/status`（`src/store.rs:65-82`）+
`conversations.kind/peer_key`（`src/store.rs:51-64`）。`SCHEMA_VERSION` 保持 7
（`src/store.rs:15`），`migrate_schema`（`src/store.rs:2171-2229`）不动。唯一会被引入的
迁移（operationId 账本表）已按 §2.3 明确排除在 1.6 之外。

### 3.4 lane 归类：写 lane，同账号串行

`HostDispatchLimits::acquire`（`src/host.rs:181-295`）的变异分支判定
（`src/host.rs:187-190`）当前为：

```rust
matches!(method, "messages.sendText" | "contacts.sync" | "accounts.deleteLocalData")
```

`messages.remoteDelete` **必须加入此 match**，从而一次性获得三重既有保障：

1. **同账号串行**：进入每账号互斥锁 `send_accounts`（`src/host.rs:97, 214-224`）——同账号的
   remoteDelete 与 sendText/contacts.sync/deleteLocalData 互不交错；
2. **删除屏障**：账号删除进行中时新变异请求被 `ACCOUNT_NOT_FOUND` 拒绝、`accounts.deleteLocalData`
   会等待在途 remoteDelete 完整结束（含响应写回）才级联删本地行（`src/host.rs:100-105, 192-212`，
   线性化论证见 `src/host.rs:163-180` 与测试 `src/host.rs:1391-1399`）——"上游已删但本地行已消失"
   的竞态被既有机制天然排除；
3. **写 lane 容量**：lane 选择落在 `self.send`（`SEND_CONCURRENCY=2`，`src/host.rs:54, 228-232`）；
   若不加变异分支，该方法会按 `src/host.rs:282-289` 落入 control lane（并发 1），错误。

per-host 请求预算自动生效：`request_account_id` 读 `params.accountId`
（`src/host.rs:773-779`），全局/每账号/字节三重预算与 requestId 防重放照常
（`src/host.rs:44-47, 528-560`）。metrics 分类：`method_class("messages.remoteDelete") => "send"`
（`src/metrics.rs:27-37`，不动 `METHOD_CLASSES` 数组）。

### 3.5 错误路径与可观测性

- 全部错误映射见 §2.3 表格；**路径中不存在任何自动重试**（engine 无重试逻辑；supervisor 的
  unknown 分支只回响应，不重发；`AGENTS.md:34`）。
- unknown 响应在 host 层日志记为 `"ok"` 是已知缺口（§2.3）；补偿：engine 层每次调用已按
  `call_class=mutating, error_class=unknown_outcome` 打 WARN（`src/engine.rs:679-696`），
  排障入口不受影响。host 层不改（避免为一类响应引入特判日志逻辑）。
- 隐私边界不变：新路径不新增任何日志字段（不落 messageId/conversationId/号码；
  `AGENTS.md:33`、日志分类纪律 `src/host.rs:800-838`）。

### 3.6 已知行为边界（如实写入 §4.5 契约文档）

1. 本地 `messages` 行在成功删除后**保持原样**（有正文、无 deleted 标记）：desktop 凭
   `{"status":"deleted"}` 自行决定呈现，跨会话记账由 desktop 承担（非目标 §1.2.2）。
2. 对端/其它设备的删除事件不会回推本地状态（入站 remoteDelete 信封当前为 skip，
   `src/engine.rs:1205-1229`，非目标 §1.2.4）。
3. `UPSTREAM_ERROR` 的 `retryable=true` 是全局既有映射，host 不应据此自动重试删除
   （契约文档中明示）。

### 3.7 上游参数名复核（实施前置项）

jsonRpc daemon 形态的 `remoteDelete` 参数名（预期 `account` / `recipient[]` / `groupId` /
`targetTimestamp`，对照 CLI 面 `-t/--target-timestamp` 必填、`recipient`/`-g`/`-u`/`--note-to-self`）
**必须**在实现前对照 pinned 0.14.7 发行版做一次性探活确认——本机 `packaging/out/local-bundle`
当前是占位构建（`bin/signal-cli` 为占位文本，manifest 记 0.14.7），无法离线核实。先例：
quote 参数即经此流程确认并注释在代码里（`src/service.rs:627-629`）。探活用
`packaging/scripts/smoke-signal-cli.sh` 所验过的同一二进制。

---

## 4. 测试计划

### 4.1 schema_consistency（机器强制）

`tests/schema_consistency.rs` 已强制四向一致：schema 方法枚举 ↔ `PHASE2_CAPABILITIES`+handshake
（140-172 行）↔ host.rs dispatch 字面量（166-171 行）；错误码 ↔ `ApiError::new` 字面量
（174-189 行，本方案零新增故不动）；事件 ↔ `HostEvent::new` 字面量（191-224 行，零新增）。
schema、capability、dispatch **必须同落一个本地提交**，否则测试红（这也是 §5 发布顺序中
"schema 与 Rust 同提交"的机械原因）。

### 4.2 单元测试（新增）

1. **映射与时间戳解析**（service 层，对照 `src/service.rs:1589, 1814` 的 params 断言风格）：
   - 本人 outgoing `sent` 行（经 `complete_outgoing_send` 写入上游 ts=777，先例
     `src/service.rs:1832` 的引用测试）→ prepare 产出 `targetTimestamp==777`、
     `account==signal_account`、direct ⇒ `recipient==[peer_key]`、group ⇒ `groupId==peer_key`；
   - pending/failed/unknown 行、incoming 行、不存在的行 ⇒ 一律 `MESSAGE_NOT_FOUND` 且**未**
     触发上游调用（prepare 是纯本地函数，直接断言错误即可）；
   - 账号/会话不存在 ⇒ `ACCOUNT_NOT_FOUND` / `CONVERSATION_NOT_FOUND`。
2. **unknown 注入**（supervisor/host 层）：
   - fake-signal-cli 夹具新增 `remoteDelete` handler：记录入参到 send-log 式文件（先例
     `tests/fixtures/fake-signal-cli.py:202-208`）+ 两种注入——慢响应（复用
     `[slow-host-test]` 0.35s 延迟技巧，`tests/fixtures/fake-signal-cli.py:243-249`，触发
     Mutating 超时→UnknownOutcome）与一次性 crash（复用 `DELETE_MODE` 标记文件模式，
     `tests/fixtures/fake-signal-cli.py:188-201`）；
   - 断言：响应为 `{"status":"unknown"}`；send-log 中上游调用**恰一次**（无自动重试）；
     本地消息行内容/状态**未变**；无任何 `message.statusChanged` 事件发出。
3. **lane/屏障**（host 层，对照 `src/host.rs:1391-1399` 测试风格）：
   - remoteDelete 与 sendText 同账号互斥（第二个 acquire 挂起直到首个释放）；
   - `deleting_accounts` 标记期间 remoteDelete 被 `ACCOUNT_NOT_FOUND` 拒绝
     （`src/host.rs:199-213` 路径）。
4. **metrics**：`method_class("messages.remoteDelete")=="send"`（`src/metrics.rs:190-196` 测试
   同表追加）。

### 4.3 集成测试

`tests/server_integration.rs`（现有 harness + fake-signal-cli）新增端到端用例：
happy path 全链路响应 `{"status":"deleted"}`、入参断言（含 group 与 direct 两种寻址）、
未知方法参数 `INVALID_REQUEST`、`deny_unknown_fields` 生效。

### 4.4 soak 前冒烟（人工清单，进入 §4.5 契约文档）

1. 双测试账号互发 → 24h 窗口内对已发消息 remoteDelete → 接收端确认呈现"已删除"；
2. 对超窗消息 remoteDelete → 得 `UPSTREAM_ERROR`，确认连接器不重试；
3. `SIGSTOP` 引擎进程后发起 remoteDelete → 15s 超时后得 `{"status":"unknown"}`，恢复进程后
   确认无自动重发、用同 operationId + 新 requestId 显式重试的行为符合 §2.2；
4. 跑通既有 24h soak 流程（docs/handover.md 记录的两轮 soak 惯例），观察 resource/queue
   指标无回归。

---

## 5. 发布顺序

任务指定顺序：**schema → Rust 实现 → desktop host 适配（另行提交）→ 文档**。结合本仓库
机械约束（`tests/schema_consistency.rs` 强制 schema+capability+dispatch 同提交；
`AGENTS.md:39` 要求先改契约文档/ schema 再改契约）落地为：

1. **方案文档**：本文档（已落 `docs/remote-delete-l2-plan.md`）。
2. **host 合同**：desktop 侧 host 合同文档更新（另一仓库、另行提交），引用本文 §2 的
   params/result/错误码表与 capabilities 探测要求；desktop 侧"本地删除 UI 细节"文档同步起草。
3. **schema + 实现（同一本地提交）**：`schemas/connector-api-v1.schema.json` →
   `src/lib.rs`（capabilities）→ `src/protocol.rs` 无改动 → `src/service.rs` /
   `src/supervisor.rs` / `src/registry.rs` / `src/host.rs` / `src/metrics.rs` /
   `tests/fixtures/fake-signal-cli.py` → `docs/implementation-plan.md` §4.5。提交前
   `cargo fmt && cargo test --all-targets && cargo clippy --all-targets -- -D warnings`
   （`AGENTS.md:42`）。不 push、不 deploy。
4. **desktop host 适配**：另行提交；以 handshake `capabilities` 含 `messages.remoteDelete`
   为门控（`src/host.rs:488-492`），旧 connector 上调用将得到 `METHOD_NOT_ALLOWED`
   （`src/host.rs:1100-1104`）。
5. **文档收尾**：`docs/handover.md` / 新 validation 记录补冒烟与 soak 结果。

兼容矩阵：旧 host + 新 connector 零影响（加法演进，旧 host 永不调用新方法）；
新 host + 旧 connector 须先探测 capabilities（§2.1 与 schema 描述中明示）。

---

## 6. 开放问题 / 待验证清单

1. **0.14.7 daemon `remoteDelete` 的 jsonRpc 参数名与响应形状**（§3.7，实施硬前置）。
2. note-to-self 会话（peer_key==本机号）的上游寻址形态（§3.2 末）。
3. 上游对"已删除消息再次 remoteDelete"的返回行为——决定 1.7 是否引入 operationId 有界账本
   （§2.3 备选方案的触发条件）。
4. desktop 对 `status:"unknown"` 的重试 UX 与本地记账（desktop 侧文档承载，本仓库不决策）。
5. 任务前提中"契约版本 1.5"的出处（疑在 desktop 侧 host 合同体系内），确认后与本方案的
   revision 1.6 编号对齐（§0.1）。
