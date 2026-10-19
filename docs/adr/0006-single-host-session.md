# ADR 0006: One host session per connector — explicit busy, never implicit queueing

- Status: accepted (2026-10-20)
- Related: ADR 0005 (connection/concurrency model, item 3 "serial host accept"),
  ADR 0002 (multi-connector instances), connector contract 1.197 (§4.197)
- Official anchor: none — the host/connector socket is a KT boundary, so this
  face is `[KT 适配]`. The model it completes is official: Signal Desktop is
  one account per app instance with one identified websocket and one job queue
  (`ts/textsecure/MessageSender.preload.ts` `singleProtoJobQueue`,
  `ts/jobs/JobManager.std.ts`); a second device session is never multiplexed
  onto the same protocol identity.

## Context

`host::serve` accepted one connection at a time and awaited its whole session
before calling `accept()` again (`src/host.rs` pre-1.197). A second host that
connected during a live session did not fail and did not get an answer: it sat
in the kernel listen backlog with no deadline, and when the live session ended
the process either shut down (after authentication) or the queued connection
was silently dropped. That is an implicit queue with no owner, no bound, and
no signal the caller can act on. The desktop reconnect path could not tell
"busy" from "hung connector".

ADR 0005 fixes the topology (one connector process per proxy group, one host
socket), but it left this admission detail open.

## Decision

**The first accepted connection owns the connector session. Every later
connection is answered with one explicit `runtime.busy` frame and closed.**

- The accept loop keeps polling `listener.accept()` while the live session is
  being driven (`tokio::select!` over the session future and `accept()`).
- A late connection receives one NDJSON event and no challenge:

  ```json
  {"apiVersion":"1.0","event":"runtime.busy",
   "data":{"code":"HOST_BUSY","message":"another host session already owns this connector","retryable":true}}
  ```

- No `runtime.challenge` is sent to the late connection. A challenge would
  invite it to authenticate into a session it cannot own; the busy frame is a
  terminal answer for that connection.
- The busy answer is bounded (`HOST_BUSY_ANSWER_TIMEOUT`, 1 s) and runs on a
  detached task under a four-slot ceiling
  (`HOST_BUSY_ANSWER_CONCURRENCY`), so a peer that stops reading cannot stall
  the accept loop or the live session's polling; a connect flood beyond the
  ceiling has its connection dropped immediately rather than growing a task.
  After the frame (or the timeout) the connection is dropped.
- The frame carries fixed content only: a code, a static message, and
  `retryable`. No session id, account id, pid, path, or timing detail.
- The live session is untouched: the rejected connection takes no dispatch
  lane, no pending-budget slot, and no account lock.
- Process lifetime is unchanged: an unauthenticated/malformed first
  connection still does not consume the process; the first authenticated
  session still ends the process. The caller's retry therefore requires a
  fresh spawn and bootstrap secret, which is exactly what `retryable: true`
  documents.

## Consequences

- A reconnect race now fails fast and observably instead of hanging in the
  backlog. The desktop supervisor treats `runtime.busy` as "spawn/attach later",
  not as a protocol error.
- There is no accept-time queue, so there is no new admission state to bound,
  persist, or leak.
- A hostile local peer can make the connector spend one bounded frame write
  per connection attempt, but cannot make it serve two sessions, allocate a
  second dispatch budget, or keep the accept loop from the live session for
  longer than the answer timeout.
- Multi-window support, if ever required, is a new decision (a second
  connector process under ADR 0002), not a change to this rule.

## Alternatives rejected

- **Keep queueing in the backlog.** Unbounded in time, invisible to the
  caller, and the queued connection is dropped anyway when the live session
  ends the process.
- **Answer with the normal `AUTHENTICATION_FAILED` response.** Requires a
  request id and implies the caller reached the protocol; the connection did
  not.
- **Send `runtime.challenge` and refuse the handshake.** Wastes a handshake
  and misleads the client into believing it can authenticate.
- **Serve two host sessions concurrently.** Both would dispatch into the same
  account state and per-account mutexes; the delete drain barrier and the
  single writer assumption in ADR 0005 assume one host session.
