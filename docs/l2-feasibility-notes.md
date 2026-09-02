# L2 候选可行性核查：contacts.setLocalAlias / peers.lookupRegistered

- 核查日期：2026-09-03
- 仓库基线：`main` @ `731b0b7`（工作树中另有并行任务的 sendReaction/unicode-segmentation 未提交改动，本文档为新增文件，不与其重叠）
- 上游基线：unmodified `signal-cli v0.14.7`（docs/implementation-plan.md §1）
- 证据来源：上游 v0.14.7 源码 tarball（GitHub AsamK/signal-cli tag v0.14.7，
  sha256 `08b56db45109e351c8f41bd73e05bcb1e29bae9c51783d51b8c3c4996ac83a7b`）逐文件核对；
  本机 `packaging/out/local-bundle/bin/signal-cli` 为占位文件（内容 `signal-cli-placeholder`），
  无法离线探活，与 docs/remote-delete-l2-plan.md §3.7 记录一致。

## 结论速览

| 候选方法 | 上游能力 | 结论 |
|---|---|---|
| `contacts.setLocalAlias` | jsonRpc `updateContact`（`-n/--name`） | **可做（任务前提已过期）**：0.8.2 对 linked device 的禁令已在 0.13.23 解除，0.14.7 代码核实无 gate |
| `peers.lookupRegistered` | jsonRpc `getUserStatus` | **可做但需产品确认**：能力存在，走 CDSI 有硬限流；`listContacts -u` 不存在 |
| `groups.get`（任务附加项） | 本地投影 | **可做**：纯 store 投影，零上游调用 |

## 1. contacts.setLocalAlias — 结论：可做（任务前提已过期）

### 1.1 任务前提 vs 实际

任务前提："updateContact 的 -n/--name 仅主设备可用（CHANGELOG 0.8.2：linked device 被禁用）"。

CHANGELOG 0.8.2 确有该禁令原文：

> updateContact, block and unblock are now disabled for linked devices

但该禁令在 **0.13.23（2026-01-24）已解除**：

> ### Changed
> - Allow updating contact names from linked devices

pinned 0.14.7 包含此变更，且源码核实无任何 gate 残留：

- `src/main/java/org/asamk/signal/commands/UpdateContactCommand.java`：全文件无 linked-device
  检查；对比同目录仍带 "This command doesn't work on linked devices." 的命令
  （FinishChangeNumberCommand / UpdateConfigurationCommand / UnblockCommand / SetPinCommand /
  RemovePinCommand / AddDeviceCommand / StartChangeNumberCommand 等）形成反证。
- `UpdateContactCommand.handleCommand` → `m.setContactName(recipient, givenName, familyName,
  nickGivenName, nickFamilyName, note)` →
  `lib/.../manager/helper/ContactHelper.java#setContactName`：仅写 `account.getContactStore()`
  本地 contact store，无上游网络调用。
- man page（man/signal-cli.1.adoc "=== updateContact"）：明确 "This change is only local but can
  be synchronized to other devices by using `sendContacts`"。即变更本就是本地 store 语义，
  "主设备限定"历史上是 CLI 面的自我限制，而非协议约束。

### 1.2 持久性与呈现语义（已核实）

- 信号侧持久性：主设备的联系人同步信封（sync message contacts）导入路径
  `lib/.../manager/helper/SyncHelper.java#handleSyncDeviceContacts` 对 name 字段是
  "仅在本地无名字时填充"（`contact == null || (givenName == null && familyName == null)` 才接受
  远端 name），**不覆盖** linked device 已写的别名；别名不会被主设备周期性冲掉。
- connector 侧呈现链路：connector 从 `listContacts` JSON 组合显示名
  （`src/supervisor.rs#compose_contact_display_name`），当前优先级为
  profile(givenName/familyName) → contact `name` → nickName → username。
  `Contact.getName()`（上游 `lib/.../manager/api/Contact.java`）返回的是 contact store 的
  given/family 组合——即 setLocalAlias 写入的值。因此设置别名后，`contacts.sync` 重跑会把
  contact `name`（= 别名）同步进 `contacts.title`，但 `compose_contact_display_name` 在存在
  profile 名时会继续优先展示 profile 名，别名对 UI 呈现的收益有限。

### 1.3 实现边界（供后续排期，本批次不实现）

- 一次只加一个 connector 方法的纪律（AGENTS.md 演进先例）：sendReaction 正在并行落地
  （contract revision 1.7），setLocalAlias 应排在其后顺延（1.8），不得同批。
- 链路复用 remoteDelete 先例（docs/remote-delete-l2-plan.md §3）：写 lane 变异方法
  （host.rs acquire 的 mutating match 追加，命中同账号互斥 + 删除屏障 + send lane），
  `prepare_set_local_alias` 纯本地校验（账号/联系人存在性、别名长度上限，
  schema `messagesRemoteDeleteParams` 同型 params：`accountId` + `peerKey` + `alias` ≤128 字节），
  上游 `updateContact` params（0.14.7 JsonRpcNamespace 的 dash→camelCase 映射）：
  `{"account", "recipient": [peerKey], "name": alias}`。
- 结果语义与 remoteDelete 同型：`{"status":"updated"}` / `{"status":"unknown"}`
  （JsonRpcLocalCommand 走 daemon 时无返回体，`Ok(_)` ⇒ updated）。
- 错误码零新增：ACCOUNT_NOT_FOUND / INVALID_REQUEST / RUNTIME_NOT_RUNNING / UPSTREAM_EXITED /
  UPSTREAM_ERROR（上游 UnregisteredRecipientException → UserErrorException → jsonRpc error）。
- 本地呈现影响（§1.2）：若产品期望"别名优先于 profile 名展示"，需要同步调整
  `compose_contact_display_name` 的优先级或在 `contacts.title` 写入时做标记——这是产品决策，
  单独立项时确认。

## 2. peers.lookupRegistered — 结论：可做但需产品确认

### 2.1 上游能力核查

- 任务假设的 `listContacts -u`（按号码/用户名查询注册状态）：**不存在**。上游
  `ListContactsCommand.attachToSubparser` 的 flag 只有 `recipient`（本地已知收件人）、
  `-a/--all-recipients`、`--blocked`、`--name`、`--detailed`、`--internal`；`-u` 是全局账号
  选择参数，不是注册查询。
- 真正的能力是 jsonRpc 命令 **`getUserStatus`**
  （`src/main/java/org/asamk/signal/commands/GetUserStatusCommand.java`，"Check if the specified
  phone number/s have been registered"）：入参 `recipient[]`（E.164，上游内部做 canonicalize）
  与 `--username`（用户名/username link，走 `m.getUsernameStatus`）；JSON 输出
  `[{recipient, number?, username?, uuid?, isRegistered}]`（`uuid != null` 即已注册）。
- 限流证据：`m.getUserStatus` 走 CDSI（Contact Discovery Service）——
  `ManagerImpl#getUserStatus` 捕获 `CdsiResourceExhaustedException` 并抛
  `RateLimitException`（携带服务端 `retryAfterSeconds`）；CLI 层映射为
  `RateLimitErrorException`（"Rate limit reached. Next attempt may be tried at ..."）。
  CDSI 查询消耗账号配额，且上游把"查不到注册"的号码写入本地 recipient store
  （`markUndiscoverablePossiblyUnregistered`）——批量误用会同时污染本地 store 与账号信誉。

### 2.2 connector 现状（先例相关）

- watchdog 已在生产链路用 `getUserStatus` 做存活 ping（`src/supervisor.rs`，
  `CallClass::ReadOnly`，对自身号码每 ~60s 一次，`call_with_timeout` 15s）——ReadOnly lane
  分类被证实可行；但那是单号码、固定节律、面向本账号的调用，不是任意号码的批量查询。
- fake-signal-cli 夹具已实现 `getUserStatus` handler（marker 注入失败/计数失败），测试
  基建现成。

### 2.3 建议契约形状（供产品确认，本批次不实现）

- `peers.lookupRegistered`：params `accountId` + `numbers[]`（1..=20，E.164）；
  响应 `{"results":[{"number","registered":bool}],"status":"ok"}`。
- **限流方案（必须落进实现）**：每账号令牌桶，默认每账号每小时 1 次上游调用（单次 ≤20 号码），
  超额直接 `RATE_LIMITED`（需新增错误码）或复用 `UPSTREAM_ERROR`+本地预判拒绝；
  服务端 retryAfter 到达时映射 `UPSTREAM_ERROR`（retryable=true，但 host 仍不得自动重试）。
  watchdog 的自身 ping 与本方法共用配额的取舍需在实现时定（建议 ping 独立，避免 watchdog
  被 UI 查询挤死——watchdog 是死 man's switch）。
- **需产品确认的三件事**：① 该能力对终端用户可见的产品场景与频率预期；② 配额上限数值
  （建议保守 1 次/账号/小时起步，观察上游 429 行为再放开）；③ CDSI 查询会把号码交给
  Signal 服务端做发现（隐私面等同"给该号码发消息前的好友探测"），KT 侧是否接受该数据流。

## 3. groups.get（任务附加项，只读 lane）— 结论：可做

- 上游 `listGroups --detailed` 的信息（id/name/description/members/pendingMembers/admins/
  isMember/isBlocked/messageExpirationTimet/groupInviteLink 等）在 connector 的
  `contacts.sync` 链路已有子集落地：`listGroups`（无 --detailed）结果中 isMember=true 的群
  以 `contacts` 行（kind='group'，title=群名，extra={"memberCount":N}）进 store
  （`src/supervisor.rs#sync_contacts`）。
- connector 契约面没有 `groups.get`。作为只读方法实现：params `accountId` + `groupKey`，
  响应从 `contacts` store 行（kind='group'）投影（peerKey/title/memberCount/syncedAt），
  零上游调用、纯 store 读——与 `contacts.list`（src/service.rs#list_contacts）同 lane 同风险级。
- 边界（实现时写进契约文档）：仅反映最近一次 `contacts.sync` 的缓存快照（含 60s 短路窗），
  不做上游实时查询；未同步或已退群的 key 返回 GROUP_NOT_FOUND（新增错误码，或复用
  INVALID_REQUEST 语义的取舍在实现时定）。
