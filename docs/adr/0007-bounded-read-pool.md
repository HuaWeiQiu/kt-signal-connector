# ADR 0007: Bounded read pool over the single writer connection

- Status: accepted (2026-10-20)
- Related: ADR 0005 (one account is one client), ADR 0001 (one store per
  profile), connector contract 1.197 (§4.197)
- Official anchor: none for the pool itself — Signal Desktop's SQLite
  concurrency is a process-internal choice and its `DataWriter` serializes
  writes the same way. The read/write split is `[KT 适配]`.

## Context

`Store` held one `Mutex<Connection>` (`src/store.rs`, `Store::lock_conn`) and
every read and write went through it. SQLite in WAL mode already allows
concurrent readers with one writer, but the connector could not use that:
a long page read (20k-row history, a `LIKE` search) blocked an unrelated
read, and a writer commit blocked every reader. With multiple accounts and
per-account lanes (ADR 0005), the single connection remained a cross-account
serialization point even after the lane work.

## Decision

Add a **fixed, bounded pool of read-only connections** beside the single
writer:

- `READ_POOL_CONNECTIONS = 2`. Each connection has a 2 MiB page cache
  (`PRAGMA cache_size = -2048`) and a 250 ms busy timeout, so the pool's
  memory and WAL-reader footprint are pinned.
- Every pool connection is opened with the same SQLCipher key and
  `PRAGMA query_only = ON`. A write attempted through a pool connection fails
  at the SQL layer; it can never race the writer.
- The pool is used **only by pure page reads**: `list_conversations`, the
  `list_messages` page query, and the `search_messages` page query. Every
  mutating method, every read-modify-write method, and every test helper
  still uses the writer connection.
- The writer connection remains the single writer and the serializer for all
  mutating methods. No write is moved, batched, or parallelized.
- The pool is round-robin (`AtomicUsize`), so two concurrent reads use
  different connections and neither waits for the other.
- The store still opens readers only after the writer has completed schema
  migration and the plaintext→ciphertext swap, so readers always see the
  current schema; a rejected key fails at open, not on the first host read.

## Invariants

1. **One writer.** Only `lock_conn`-mediated methods write. A pool connection
   is `query_only`.
2. **Bounded.** The pool size is a constant; no connection is created per
   request, account, or conversation.
3. **No cross-account state.** Pool connections carry no account, lane, or
   session state; they are interchangeable.
4. **WAL semantics unchanged.** Readers see committed snapshots; a page read
   that spans multiple SQL statements may observe a commit between them, as
   it could before. Methods that need one snapshot wrap their work in a
   single statement or keep using the writer connection.
5. **Checkpoint honesty.** `PRAGMA wal_checkpoint(TRUNCATE)` after an account
   delete may answer busy while a reader holds a page; the receipt records
   `deferred` and `wal_autocheckpoint` keeps the WAL bounded.

## Consequences

- A slow read no longer stalls an unrelated read, and a writer commit no
  longer stalls a page read.
- Memory cost is bounded at ~4 MiB of page cache plus two WAL reader slots.
- A method that accidentally writes through a pool connection fails loudly in
  tests instead of silently contending with the writer.
- Full migration of every read method is deliberately not part of this ADR:
  methods that read then write, or that need a single-snapshot multi-query
  read, stay on the writer connection until each one is audited.

## Alternatives rejected

- **A general-purpose pool for all methods.** A read-modify-write method
  routed to a reader would either fail (`query_only`) or, worse, be moved to
  a second writer connection. One writer is the invariant.
- **`deadpool`/`r2d2` with dynamic sizing.** Adds a dependency and an
  unbounded growth policy for a workload whose ceiling is known (two
  readers).
- **Per-account connections.** Multiplies memory by account count and
  re-introduces the cross-account resource coupling ADR 0005 removes.
