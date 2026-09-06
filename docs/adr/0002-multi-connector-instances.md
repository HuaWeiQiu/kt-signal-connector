# ADR 0002: Multiple connector instances on one host

- Status: accepted and implemented (optimization-plan §6.2, stage M1)
- Date: 2026-09-06
- Amends: the "one connector process per local profile" reading of AGENTS.md
  (revised in the same change) and the process-model sentence in
  `src/registry.rs`'s header. ADR 0001 is untouched: everything inside one
  process still follows its engine-per-proxy-group rules.

## Context

P2-12 (optimization-plan §6) removes the shared connector-process failure
domain: one connector crash currently takes down every account in a local
profile, and the desktop supervisor pool (stage M2) will run one connector
per proxy group. A read-only survey of this repo confirmed what M1 builds
on: the three launcher namespaces (`--endpoint`, `--state-dir`,
`--signal-data-dir`, all passed in `src/main.rs`) carry no hardcoded
singleton, no PID file, and no global mutex, so multiple instances can
already coexist. Endpoint reuse already fails closed at bind
(`src/ipc.rs`, first-bind-wins `AlreadyExists`). Three gaps remained:

1. An endpoint path longer than the platform `sockaddr_un::sun_path` limit
   was only reported by `bind(2)` as a bare `EINVAL` — synchronous,
   non-retryable, and naming nothing. This is a shipped failure
   (optimization-plan §5.5 root cause: a long `userDataDir` pushed the
   socket path past macOS's ~104-byte limit and the first connect failed
   fast).
2. Nothing prevented two connector processes from opening the same
   signal-cli data directory — the equivalent of running two signal-cli
   instances against one account database.
3. AGENTS.md and ADR 0001 both still said "one connector process per local
   profile", contradicting the direction the platform is taking.

## Decision

Multiple connector instances on one host are **legal exactly when their
launcher-granted namespaces are pairwise disjoint**:

N1. **Endpoints are disjoint.** Two instances never bind the same endpoint
    path. Enforced by the OS: `bind` publishes via rename under a staging
    name and an existing socket path fails closed with `AlreadyExists`
    (first bind wins; the loser exits 1 before opening the store).

N2. **Signal data directories are disjoint, per data directory, not per
    root.** The unit of exclusivity is each directory a signal-cli engine
    will run on: the `default` group's `--signal-data-dir` root and every
    `proxy-groups/<groupId>/` subdirectory (ADR 0001 R4). Two instances may
    split the groups of one root (instance A takes `default`, instance B
    takes `proxy-groups/team-b/`) because those are separate signal-cli
    databases; they may not share any single directory.

N3. **Each instance owns a separate state-dir store.** The SQLCipher store
    has no cross-process lock and no instance dimension, so a shared
    state-dir is out of bounds entirely. This condition is a launcher
    obligation (the M2 desktop supervisor parameterizes per-connector
    state dirs), deliberately *not* an enforced guard: the desktop already
    owns directory layout, and duplicating engine-side directory validation
    here would add a second source of truth.

### Guard G1: endpoint length fail-fast (`src/ipc.rs`)

- Unix: `bind(2)` copies the path into `sockaddr_un::sun_path` — 104 bytes
  on macOS, 108 on Linux, NUL included — leaving 103/107 usable. Both the
  canonical endpoint path and the longer per-process staging name
  (`.<name>.<pid>.staging`) it is first bound under are checked against
  that limit before any bind, and an overrun aborts startup with
  `InvalidInput` plus a message naming the byte counts and the limit
  (never the path itself, per the AGENTS.md log discipline). The staging
  check matters: an endpoint that fits only without its staging suffix
  would still hit the historical `EINVAL`.
- Windows: the named-pipe name is documented (CreateNamedPipe `lpName`) as
  limited to 256 characters; the existing check counts the whole normalized
  `\\.\pipe\...` string and rejects >256 fail-fast. No further limit
  exists to enforce — the single-path-segment shape rule is the only other
  documented constraint.

### Guard G2: data-dir occupancy lock (`src/datalock.rs`)

- Mechanism: an advisory **exclusive, non-blocking kernel lock on
  `<data-dir>/.kt-signal-connector.lock` — `flock(2)` on Unix (rustix),
  `LockFileEx` on Windows (windows-sys)**. Chosen over an `O_EXCL` +
  PID-liveness lockfile because the kernel already owns the truth: the lock
  is bound to the open file description, not to a PID recorded in a file,
  so there is no PID-reuse race, no takeover heuristics, and no stale-lock
  brick-after-crash. Both primitives are released automatically when the
  holding process exits for *any* reason, SIGKILL included — verified by a
  test that kills a holder and takes over.
- Granularity and lifetime: one lock per planned group data directory,
  acquired in `serve` startup for the whole launch plan at once
  (`lock_plan_data_dirs`), all-or-nothing — a conflict on any group
  releases every lock already taken, so a failed start never pins a
  directory it does not serve. Acquisition happens before the bootstrap
  payload is read and before the endpoint is published; the guards are held
  for process lifetime, so `runtime.stop` (which parks engines but keeps
  serving) does not release occupancy.
- Path-identity robustness: locking the file rather than comparing path
  strings means symlinked, relative, or otherwise differently-spelled
  paths to the same directory still collide on the same inode.
- Residue: the lock file is deliberately left on disk after release.
  Deleting it would reintroduce the unlink/relink takeover race the kernel
  lock makes unnecessary; an empty leftover file in a 0700 data directory
  is inert.
- Diagnostics: the rejection names the proxy group id, never the directory
  path ("signal data directory of proxy group 'x' is already in use by
  another connector process").

## Rejected alternatives

- **O_EXCL lockfile with PID liveness probing.** More code, and wrong by
  construction: a reused PID makes a dead holder look alive (bricked
  restart), a dead-looking PID makes a live holder look dead (unsafe
  takeover). The kernel lock is strictly simpler and has neither failure.
- **Locking the state-dir store instead / SQLite-based exclusion.** SQLite
  permits multi-process access by design, so it cannot express "one owner";
  and N3 already rules the shared store out at the launcher level.
- **Locking the data-dir root only (one lock per root).** Would forbid the
  legal split where sibling instances take different group subdirectories
  of one root — exactly the connector-per-proxy-group topology M2 deploys.
- **Enforcing N3 with a lock too.** Rejected for the N3 reasons: launcher
  obligation, single source of truth for directory layout.

## Consequences

- The M2 desktop supervisor pool can spawn N connectors with per-connector
  endpoint/state-dir/data-dir trees and rely on fail-closed startup for any
  accidental overlap; a mis-routed spawn dies at once with a named group
  instead of corrupting a signal-cli database.
- Multi-instance rollouts keep the ADR 0001 R4 layout: the `default` group
  of each instance uses that instance's `--signal-data-dir` root, extra
  groups live under `proxy-groups/<groupId>/`.
- Single-instance deployments see one new dotfile (`.kt-signal-connector.lock`)
  per data directory and otherwise unchanged behavior; an old binary that
  never locks coexists harmlessly with the file on disk (it just does not
  respect the lock, which is the documented skew: both binaries must be
  M1+ for multi-instance use).
- The lock guards occupancy, not simultaneous *first* creation: two
  instances racing to create a fresh data directory serialize on the lock
  file open; the loser exits before touching anything else.

## Rollback

Removing both guards restores the pre-M1 binary exactly: the lock file is
inert residue, and endpoint-length overruns return to bare `EINVAL`
fail-fast behavior. No data format, protocol, or layout changes are
involved.

## References

- `docs/optimization-plan.md` §6 (P2-12 proposal, stage M1), §5.5 (the
  `sun_path`/EINVAL incident this guard reads on)
- ADR 0001 (engine-per-proxy-group; R4 data-directory layout)
- `src/ipc.rs` (G1), `src/datalock.rs` (G2), `src/main.rs` (wiring),
  `tests/multi_instance.rs` (conflict tests incl. the SIGKILL takeover)
