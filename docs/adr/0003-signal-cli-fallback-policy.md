# ADR 0003：signal-cli 兜底模式定策——仅开发与行为对照，不承载生产账号

状态：已定策（2026-10-10）。决策人：owner（经 AI 审计汇报确认）。

## 背景

- 引擎格局已定：生产/真机主链路 = 自研 kt-signal-engine（本机自举
  `~/.kt-desktop-signal-dev/env.json` 实证 `signalCliEngine: "kt-engine"`）；
  signal-cli（JVM/native 模式）自引擎接管后不在运行路径上。
- Signal 上游按客户端 capability（如 SPQR）做强制过滤，且无预告：2026-03-06
  起卡新注册、2026-03-30~04-02 存量批量注销 ≤v0.14.0（signal-cli#1993/#2005）；
  release note 不提前告知。上游 README 三个月弃用政策（"official clients
  expire after three months"）对长期钉定的 signal-cli 意味着周期性出窗：
  v0.14.7（2026-08-01 发布）约 2026-11-01 出窗、v0.14.8（2026-09-10）约
  2026-12-10 出窗。
- 引擎不受上述影响：vendor 链声明 `spqr: true`（presage confirmation.rs /
  registered.rs、libsignal-service account.rs / linking.rs 四处），且引擎
  vendor 升级由引擎仓节奏自主掌控。

## 决策

1. **connector 的 `SignalCliMode::Jvm` / `SignalCliMode::Native` 定位降级为
   「开发与行为对照」**：不承载生产账号、不承诺对 pinned 版本（0.14.7/0.14.8）
   的滚动升级。
2. **pinned 版本维持现状**：`package manifest --signal-cli-version` 默认
   0.14.7 仅为清单元数据默认值；`packaging/scripts/dev-setup.sh` 的 0.14.8
   仅为开发环境自举。两者均不构成运行时拉取或生产依赖。
3. **重新生产化 JVM/native 模式需本 ADR 变更**，触发条件（任一）：
   - kt-engine 长期不可用，需要信号面回退路径；
   - 产品明确要求某类账号跑 signal-cli 模式。
   届时升级策略 = 跟随上游最新 + 月度滚动（0.14.x 线内无 breaking、account
   数据目录通用、换 tarball 即可，JRE≥25 已满足），并补capability 达标验证。
4. **「兜底」定性修正（2026-10-10 补充，owner 质询成立）**：signal-cli 回退是
   **冷兜底，非热兜底**——引擎账号存储 = presage-store-sqlite（Rust/SQLite），
   signal-cli = 自有 Java 格式（`accounts.json` + 号码目录），互不兼容；切换 =
   手机重新扫码链接 + 本地历史不可携（backup5 链只在引擎侧）。且双模式的
   connector 契约面已系统性分叉（getUsername JVM 缺/stories reaction 引擎缺/
   chatFolders kt-engine only/textStyle 曾缺等），每批功能需双轨测试——这是
   持续税，不是保险。故：**JVM/native 运行时模式自即日起冻结维护**——不再为
   其适配任何新面，仅保留既有缝的「诚实拒绝」测试锁；engine-gap 的权威答案
   一律是「等引擎批」而非「切 JVM」。真正的引擎故障回退 = 引擎二进制版本
   回滚，不是切 signal-cli。**移除 JVM/native 运行时模式代码**（dispatch arms/
   双轨测试/打包链）另立清理批，待 owner 拍板后排期。
5. **对照结论失真时的权威序**：官方 Signal-Desktop 源码行为 > Signal-Server
   实际行为 > signal-cli 行为。signal-cli 对照结论与本仓「钉定版无此面」类
   记录在每次引擎 vendor 升级窗口逐项重审（见桌面 matrix 权威注 2026-10-10
   三次订正段）。signal-cli 的「参考实现」角色由只读源码 checkout
   （`.references/signal-cli`）承担，与运行时模式无关、不依赖其存活。

## 后果

- signal-cli 版本线风险从「生产断供风险」降级为「开发对照环境可能失真」；
  失真不影响线上用户。
- 原「signal-cli 升级窗口」立项撤销，不作为独立项目（2026-10-10 审计定案）。
- 上游强制信号的感知职责转移至引擎上游哨兵（engine 仓
  `scripts/upstream-sentinel.sh` + 引擎 `engine.protocolCapabilities` 自证面，
  2026-10-10 立项实施）。
