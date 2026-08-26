# ADR 0001: One signal-cli engine per proxy group

- Status: accepted as design; implementation pending (optimization-plan Phase 4)
- Date: 2026-08-26
- Supersedes: the single-engine reading of AGENTS.md "one signal-cli JVM serves all accounts"
  (revised in the same change)

## Context

KT Desktop runs multiple Signal accounts in one local profile, and KT's anti-association
scenario requires that different account clusters reach the Signal service through different
network egress (one local SOCKS5 proxy per cluster, allocated by the desktop's existing local
port allocator used for WhatsApp/TG). The connector today runs exactly one signal-cli engine
with one global SOCKS proxy (`KT_SIGNAL_SOCKS_PROXY` / `serve --socks-proxy`).

Phase 4 spike conclusions (verified 2026-08-26 against pinned signal-cli v0.14.7, JVM and
GraalVM native modes):

1. The SOCKS proxy reaches signal-cli as JVM process-global system properties
   (`-DsocksProxyHost/-DsocksProxyPort` via `JAVA_OPTS`, or as leading `-D` argv in native
   mode). signal-cli's multi-account JSON-RPC mode shares one JVM for all accounts and exposes
   no per-account network or proxy entry point.
2. Measured cost: one idle engine holds 140-280 MB RSS; each additional account inside the
   same engine adds 20-80 MB. The number of engines must therefore be a bounded product
   decision, not something accounts can grow implicitly.
3. All upstream contact of a link flow — QR/captcha/CDSI first connection included — runs
   inside signal-cli's process, so an account that must be associated with a proxy has to be
   behind that proxy from its very first link handshake. Retrofitting a proxy onto an already
   linked account does not undo the association signals already emitted.
4. The account data of a signal-cli instance is physically bounded by its `--data-dir`: one
   engine serves exactly the accounts stored in its data directory.

## Rejected alternatives

- **Per-account proxy inside one shared JVM.** Impossible without upstream changes: the proxy
  is a process-global JVM property and the multi-account JSON-RPC API has no per-account
  network hook (spike fact 1).
- **Fork or patch signal-cli to add a per-account proxy.** Violates the pinned-upstream red
  line (AGENTS.md; implementation-plan §3.3 and §9: an upstream patch is a new architecture
  and license review, not an implementation detail). Also rejected for the whole integration
  in optimization-plan §0.
- **OS-level per-process routing** (pf/iptables rules, per-interface binding, namespace
  tricks). Not viable on a managed desktop fleet: requires elevated privileges on end-user
  macOS/Windows machines, breaks the current per-user no-admin install model, and is
  unportable across the three target platforms.
- **One engine per account.** The unbounded form of this decision: N accounts would mean N
  engines at 140-280 MB idle each (spike fact 2). Rejected by the long-standing
  "never one JVM per account" rule; the group model below is its bounded refinement.
- **Do nothing (one global proxy).** Leaves the anti-association requirement unmet; all
  accounts share one egress.

## Decision

Adopt an **engine-group model**: a *proxy group* is a named, bounded set of accounts that
share one signal-cli engine process (JVM or native) and therefore one SOCKS proxy and one
network egress. One connector process per local profile supervises all groups.

### Normative rules

R1. **Groups are launcher input, never IPC input.** The set of groups and their proxies is
    defined at connector spawn by the trusted launcher (repeatable `serve --proxy-group
    <id>=<host:port>`, the `KT_SIGNAL_PROXY_GROUPS` comma-separated equivalent, or both at
    once — the entries then form one launcher-ordered list with flag entries first and
    environment entries in order), governed by the signed runtime manifest like every other
    process input (implementation-plan §5). The same group id appearing twice — across the
    two sources or within either one — aborts startup: an ambiguous egress assignment is a
    configuration error and is never resolved by precedence. The wire protocol can only
    *select* an existing group at link time and *observe* group state; it cannot create,
    reconfigure, or delete groups. Group ids match `^[a-z0-9]([a-z0-9-]{0,30}[a-z0-9])?$` so
    they are filesystem-safe; `default` is reserved.

R2. **The default group is the compatibility anchor.** With no group configuration the
    connector runs exactly one group, `default`, with a direct connection — byte-identical to
    today's process model. The legacy global `--socks-proxy` / `KT_SIGNAL_SOCKS_PROXY`
    configures the `default` group's proxy; `KT_SIGNAL_PROXY_GROUPS` adds further groups and
    must not redefine `default` (startup error otherwise).

R3. **Membership is fixed at link time and immutable.** `link.start` optionally names the
    group; the account is bound when `link.finish` succeeds, and the binding never changes for
    the life of the link. Moving an account means `accounts.deleteLocalData` plus a fresh link
    into the new group (spike fact 3 makes any softer migration meaningless). Link sessions,
    the `LINK_IN_PROGRESS` mutual exclusion, and the `link.finish` wait lane are all per
    group, so links into different groups may proceed in parallel.

R4. **One data directory per group.** Following spike fact 4, each group's engine runs with
    its own `--data-dir`; an account physically lives in its group's directory. The `default`
    group keeps the existing `--signal-data-dir` path unchanged; additional groups use
    `<signal-data-dir>/proxy-groups/<groupId>/` with owner-only permissions, never exposed
    over IPC. The connector store stays a single SQLCipher database; account rows persist
    their `proxy_group` (pre-Phase-4 rows read as `default`).

R5. **Supervision is replicated per group, not shared.** Each group engine gets the full
    existing supervision stack unchanged: bounded JSON-RPC transport and pending map, watchdog
    with its own restart circuit breaker, RSS sampler every 30 s with the existing
    512/420 MiB pressure thresholds, graceful-then-forced shutdown. Per-group upstream queues
    keep today's per-engine limits (128 pending signal-cli requests each); host-side limits
    stay global on the single authenticated IPC connection. A `link.cancel` that must restart
    an engine restarts only that group's engine — an improvement over today, where it pauses
    every account in the profile.

R6. **Group count is capped.** The connector refuses to start with more than **8** groups
    (hard ceiling); the product default cap is **4**, enforced by desktop tenant policy.
    Rationale from spike fact 2: 8 groups cost 1.1-2.2 GB idle RSS worst case, which is a
    ceiling for exceptional deployments; 4 groups (0.6-1.1 GB idle) is the expected product
    envelope. Accounts never create groups.

R7. **Lifecycle control stays global.** `runtime.start`/`runtime.stop` act on all groups.
    `runtime.start` fails with `RUNTIME_START_FAILED` only when *no* group engine started
    (equivalent to today's single-engine failure); partial success is reported per group.
    `runtime.status` keeps its top-level aggregate for existing callers and adds a
    `proxyGroups` array; the aggregation rule is pinned in implementation-plan §4.4.

R8. **Failure-domain isolation.** A group engine crash, watchdog restart, or resource-pressure
    state affects only that group's accounts; other groups keep serving. The single connector
    store and the single host session remain shared failure domains by design (they are
    cheap, supervised, and restartable as a whole), exactly as today.

R9. **Wire delta is additive API 1.0.** The protocol change is an in-place evolution of
    `schemas/connector-api-v1.schema.json` at apiVersion `1.0` — no v2. The full field-level
    contract is implementation-plan §4.4. New error code: `PROXY_GROUP_NOT_FOUND`. New event:
    `proxyGroup.stateChanged`.

R10. **Proxy endpoints never cross the wire or logs.** The desktop allocated the local proxy
    ports and already knows the mapping; the wire carries only opaque group ids, and error
    messages may carry a group id but never a proxy host:port.

## Consequences

- Anti-association granularity is the group: accounts inside one group share an egress IP.
  Strict one-account-one-egress deployments use one account per group and are bounded by the
  group ceiling; the product must size clusters accordingly.
- Memory budget becomes a group-count policy (R6), enforced at startup rather than discovered
  in production.
- New-desktop → old-connector skew fails closed: the unknown `proxyGroup` param is rejected
  (`INVALID_REQUEST`, `deny_unknown_fields`), never silently ignored into the wrong egress.
- Old-desktop → new-connector skew is a no-op: frames are unchanged and everything lands in
  `default`. Feature detection is the presence of `proxyGroups` in `runtime.status`.
- Phase 5's native mode composes unchanged: one native process per group, same supervision,
  same proxy mechanism (already-argv `-D` properties).

## Rollback

- Rolling back to a single-group configuration is exactly today's behavior; no migration.
- Rolling back the *binary* below Phase 4 with multi-group data on disk: the old connector
  serves only the `default` group's data directory; other groups' accounts are dormant but
  intact on disk and become reachable again on roll-forward. No data-format incompatibility is
  introduced (the store column is additive with a `default` fallback).
- The wire change needs no rollback: old and new frames interoperate per the skew rules above,
  and the apiVersion `1.0` handshake binding is untouched.

## References

- `docs/optimization-plan.md` Phase 4 (spike summary and gate)
- `docs/implementation-plan.md` §4.4 (wire contract), §5 (launcher boundary), §7.2-7.3
  (budgets and limits)
- `schemas/connector-api-v1.schema.json` (machine-readable contract)
- `src/engine.rs` (`SocksProxy`, JVM/native proxy injection), `src/main.rs` (`--socks-proxy`)
