# ADR 0005: Connection and concurrency model — one account is one client

- Status: proposed (2026-10-20)
- Related: ADR 0001 (engine per proxy group), ADR 0002 (multi-connector),
  ADR 0004 (bounded media ingest), connector contract 1.194
- Official anchor: Signal-Desktop `ts/textsecure/MessageSender.preload.ts`
  (`singleProtoJobQueue`), `ts/jobs/JobManager.std.ts`,
  `ts/jobs/AttachmentDownloadManager.preload.ts`, and the vendored
  `libsignal-service-rs` websocket slot/handover discipline

## Context

The KT Signal surface spans three repositories and, in production, more than
one account per desktop. The current topology is:

| Layer | Topology | Connection | Concurrency |
|---|---|---|---|
| Desktop | one connector supervisor per proxy group (`connectorPool.ts`) | one Unix socket per connector | `requestScheduler.ts` lanes (control 1 / linkWait 1 / read 4 / send 2) **per connector** |
| Connector | one process per proxy group, one engine child | `serve()` accepts **serially** (one host client at a time) | global pending 128 / per-account 32 / 176 MiB; lanes **global** control 1 / read 4 / send 2; per-account mutex + delete barrier |
| Engine | one process per proxy group, N accounts | one `Manager` per account (single identified websocket) + one receive pipeline per account | sends serialized per account (`AsyncMutex<Manager>`); **all receive pipelines and request dispatch share one `LocalSet` OS thread** |

Official Signal Desktop is one account per app instance: one identified
websocket, one `singleProtoJobQueue`, one `JobManager`. The protocol is
stateful and ordered — a device identity must not run two concurrent
sessions, and mutating calls must not interleave.

The current model has four coupling points:

1. **Engine head-of-line blocking.** `main.rs:438-447` runs every account's
   receive pipeline and every JSON-RPC dispatch on one `LocalSet` thread
   (`service/mod.rs:2083` `spawn_local`). One account's blocking work stalls
   all accounts.
2. **Global lane quotas.** `connector/src/host.rs:89-99` defines control 1 /
   read 4 / send 2 **per connector**, not per account. With multiple
   accounts, unrelated accounts serialize behind each other.
3. **Serial host accept.** `host.rs:467` serves one client connection at a
   time; a second connect (reconnect race, another window) waits in the
   backlog with no explicit busy answer.
4. **Blast radius = engine process.** A fault in one account's path can take
   down every account in the proxy group (the 2026-10-19 4.4 MB incident
   surfaced as `SEND_OUTCOME_UNKNOWN` for the whole group).

## Decision

**One account is one client.** Concurrency is modelled as an actor per
account, never as a shared connection/thread pool.

### Topology

- Keep one connector process and one engine process **per proxy group**
  (ADR 0001). Do not multiply processes by account by default.
- Within the engine, each account owns one **AccountSession actor**:
  - a dedicated OS thread running a `current_thread` Tokio runtime +
    `LocalSet` (presage receive futures are `!Send`),
  - one bounded mailbox (mpsc) for account-addressed work,
  - exactly one `Manager` / identified websocket, one receive pipeline,
  - mutating calls serialized FIFO inside the actor.
- JSON-RPC dispatch routes by account to the actor's mailbox and awaits a
  oneshot reply. Blocking work that does not need `Manager` (media decode,
  disk) runs on `spawn_blocking`, not on the actor thread.
- Connector and desktop lane quotas become **per-account with a global
  backstop** (e.g. per-account control 1 / read 4 / send 2; connector-wide
  ceiling bounds total permits). Keep per-account pending 32 and global 128
  as the admission backstop.

### Invariants (test-locked)

1. **Single session per account.** One identified websocket per account;
   never pool multiple connections for one account. Sends per account are
   serialized; ordering is preserved.
2. **No cross-account blocking point.** No lock, semaphore, queue slot, or
   thread is held across accounts. A busy account cannot starve another.
3. **Bounded everything.** Every queue, mailbox, frame buffer, and cache has
   a hard bound; overflow answers a retryable error, never waits forever.
4. **Failure containment.** A panic or fault in one account's actor must not
   terminate the process or other accounts. Actor/pipeline restart uses the
   existing Fibonacci backoff with jitter.
5. **One writer per connection.** The connector's writer task owns the
   socket; all frames go through its bounded queue.
6. **Official retry semantics.** Download/send retries follow
   `JobManager` (`maxAttempts 5`, 30s/5m/50m/6h, concurrency 6, urgent
   `forceStart`), and `attachment.backfillError` is set only by a remote
   `TERMINAL_ERROR` status — never by a request-face refusal or a timeout.

### Blast radius

Default remains engine-per-proxy-group. An optional **isolation tier**
(one engine per account) may be offered for high-value accounts; it costs
roughly one process RSS (~40 MB) and one connector socket per account. The
default tier is not changed by this ADR.

## Alternatives rejected

- **Shared thread pool for account work.** Reintroduces head-of-line
  blocking and lets one blocking task occupy a worker; also invites
  parallel sends on one session, which breaks ordering.
- **Connection pool per account.** Violates the Signal single-connection
  session model; libsignal's identified slot hands over and terminates the
  previous connection, so a pool would thrash sessions.
- **Process per account by default.** Maximal isolation, but N× RSS and N×
  connector sockets; deferred to the optional isolation tier.

## Consequences and rollout

1. Write this ADR and lock the invariants with tests before refactoring.
2. Add a soak test: N accounts, one account injected with slow/blocking
   work; assert other accounts' p95 latency is unaffected.
3. Engine actor-per-account (largest change), then connector/desktop
   per-account quotas, then observability: per-account queue depth,
   in-flight, p95, reconnects, mailbox occupancy.
4. Until the refactor lands, the existing global quotas remain; document
   the known coupling in the implementation plan.

## References

- Signal-Desktop: `MessageSender.preload.ts`, `MessageReceiver.preload.ts`,
  `JobManager.std.ts`, `AttachmentDownloadManager.preload.ts`,
  `WebAPI.preload.ts`
- Vendored `libsignal-service-rs`: `websocket/mod.rs` (identified/
  unidentified slots, handover), `push_service/mod.rs`
- Connector ADR 0001–0004
- Tokio: `LocalSet`, `spawn_blocking`, task panic isolation, `Semaphore`,
  bounded `mpsc`
- Optional sizing references: Little's law; Google SRE load shedding;
  Netflix adaptive concurrency limits
