# signal-cli 升级 SOP

本仓库把 signal-cli 作为**固定版本、外部预装**的运行时依赖：connector 永不自动下载、
永不自动更新 signal-cli。版本基线见 `implementation-plan.md`（当前 v0.14.7，
unmodified），bundle manifest 逐文件记录 SHA-256。升级 signal-cli 是一个有门禁的
手动流程，本文档是唯一入口。

## 0. 何时触发升级

- 订阅上游 release：<https://github.com/AsamK/signal-cli/releases>
- 只在以下情况升级：
  - 安全修复（libsignal 协议漏洞、依赖 CVE）；
  - 服务端协议迁移导致旧版本功能退化（历史上 Signal 服务端会给月到年级的
    双栈缓冲期，见 §5，不需要追每个版本）；
  - 我们需要的 JSON-RPC 新能力。
- 不追上游每个 minor 版本。每追一次都要走完整套流程，成本不为零。

## 1. 升级前评估（读 release notes，10 分钟）

重点看三类条目：

1. **JSON-RPC / daemon 变更**：`daemon` 参数、`receive`/`send`/`listAccounts`
   等方法的请求或响应字段增删改。这直接影响 `crates/kt-signal-connector-core`
   的协议解析。
2. **存储格式变更**：账号数据目录格式迁移（一旦迁移，回退旧版本 signal-cli
   可能读不了新数据——rollback 成本变高，要在 §4 灰度时格外小心）。
3. **JRE 要求变更**：0.14.7 要求 JRE 21+。若新版抬高要求，bundled runtime
   和 system 解析策略都要同步升，否则 smoke 第 2 步（`--version`）就会挂。

## 2. 本地验证（新版本二进制）

```bash
# 对下载的新二进制跑 smoke（4 道检查：形状/版本/daemon 参数/真实 JSON-RPC 探活）
packaging/scripts/smoke-signal-cli.sh \
  --bin /path/to/new/signal-cli \
  --expect-version 0.15.0

# 夹具契约测试仍必须全绿。若上游改了 receive/send 字段，
# 先同步 tests/fixtures/fake-signal-cli.py，再让测试通过。
cargo test --all-targets
cargo clippy --all-targets -- -D warnings
```

smoke 脚本任何一步 FAIL 都不要继续：版本不符、JRE 不兼容、daemon 模式被改、
JSON-RPC 握手失败，都会在打包前暴露。

## 3. 打包与签名

1. 用新二进制 + 匹配的 JRE 打新 bundle，manifest 中 `signal-cli-version`
   升到新版本号，逐文件 SHA-256 重新计算。
2. 按发布流程签名（开发环境用 dev key；生产签名不在本仓库范围内）。
3. **不要覆盖安装到正在运行的 profile 目录**；先 stage 到独立目录。

## 4. 灰度与回退

1. **stage**：新 bundle 放入 stage 目录，不激活。
2. **灰度激活**：先在一个测试 profile / 测试账号上 activate，启动通道，
   观察：ready 状态、收发消息、receive 解析无 unknown 字段告警。
3. **异常回退**：activate 指回上一个 LKG（last-known-good）bundle。
   注意 §1 第 2 条：若新版已迁移过账号数据格式，旧 signal-cli 可能无法
   直接读回，灰度期间务必用测试账号而非主力账号。
4. 灰度稳定后才全量 activate。

顺序永远是：**验证 → 打包签名 → stage → 灰度 activate → 全量**。
不要反着来，不要跳步，不要让任何环节自动触发 signal-cli 替换。

## 5. 上游协议演进规律（判断升级窗口的依据）

Signal 服务端协议演进没有公开时间表，但历史上节奏稳定：

1. `libsignal-client` 先发布新能力（如 2023 PQXDH、2024 手机号隐私迁移）；
2. 官方客户端灰度启用；
3. signal-cli 通常在数天到数周内跟进发布；
4. 服务端长期保持双栈兼容，旧协议下线以**月到年**计，很少突袭失效。

结论：看到 signal-cli 跟进发布后有充足缓冲期完成本 SOP，不需要在
libsignal 发布当天行动；但也不要滞后超过一个大版本周期，避免存储格式
跨多版迁移放大回退风险。

## 6. 失效兜底（已内置，不随升级改变）

无论协议如何演进，以下行为是 connector 的固定承诺：

- 发送结果不确定时进入 `UNKNOWN_OUTCOME`，**绝不自动重发**（防止服务端
  实际已收到时造成重复消息）；
- 发送失败保留本地草稿，用户可手动重试；
- 通道异常只上报状态，不在未验证新 bundle 前替换 signal-cli。
