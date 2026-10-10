# `runtime.doctor` — the read-only diagnostic face

Contract revision 1.96, §4.79 of `docs/implementation-plan.md` is the normative
specification; this document is the field-level quick reference for consumers
(the desktop support surface). The method answers the whole diagnostic payload
in one Control-lane call.

## Call

- Method: `runtime.doctor`, Control lane, non-mutating, params `{}` exactly —
  any other params answer `INVALID_REQUEST` (retryable=false). No new error
  codes, no new capability token: the method rides the handshake's method-list
  advertisement like every other `runtime.*` face.
- Available for the single kt-engine runtime; the engine faces degrade
  honestly where the engine does not carry a probe.
- Advertised through the standard method enumeration; a caller that does not
  know the method simply never calls it.

## Read-only discipline

The doctor never acts. Concretely:

- it never starts or stops an engine (`runtime.start`/`runtime.stop` remain
  the only lifecycle writers);
- it never removes, truncates, or breaks a lock — the occupancy probe takes
  the kernel lock and drops it immediately (`LOCK_EX|LOCK_NB`, then unlock);
- it writes nothing: the data-directory probe is `access(2)` + `statvfs(2)`,
  the lock-file read is capped at 32 bytes, and the engine is asked only
  through its existing read face with a 3-second budget (a wedged engine
  cannot hold a diagnostic anywhere near the ordinary 15 s request timeout).

The report is a snapshot: by the time the caller reads a field the process
may have moved on. Nothing in the answer is a lease or a reservation.

## Answer shape

```jsonc
{
  "connector": { "version", "apiVersion", "pid" },
  "groups": [ {
    "groupId",
    "engine":   { "state", "pid"?, "rssBytes"?, "resourcePressure",
                  "reachable", "unreachableReason"?,
                  "version"?:            { "present", "face"?|, "reason"? },
                  "protocolCapabilities"?: { "present", "face"?|, "reason"? } },
    "dataDir":  { "path", "exists", "writable", "freeBytes"?, "totalBytes"? },
    "lock":     { "present", "held", "pid"?, "holderAlive"?, "classification" }
  } ],
  "network":      { "watchdogRestartsTotal", "receiveDroppedTotal" },
  "recentErrors": { "window", "recordedTotal",
                    "families": [ { "source", "family", "retryable",
                                    "count", "lastAtUnixMs" } ] }
}
```

`groups` lists every launch-plan group in launcher order. Keys marked `?` are
absent (not null) when the value does not apply.

## Engine face

`engine` mirrors the supervisor's status view (`state` the live lifecycle
state, `pid`/`rssBytes` present only
for a live process). `reachable` is the doctor's own probe result: whether the
engine answered a read call during this diagnostic.

When `reachable` is false, `unreachableReason` carries a static, content-free
classification — exactly one of:

| `state`     | `unreachableReason`                                                                  |
| ----------- | ------------------------------------------------------------------------------------ |
| `stopped`   | `engine is stopped: not started since connector boot, or stopped by runtime.stop`    |
| `exited`    | `engine exited: the process ended on its own; start it with runtime.start`           |
| `faulted`   | `engine faulted: the process crashed or failed to start; the watchdog may restart it` |

The one transitional case has its own string, independent of `state`: the
engine was `running` but its live handle vanished mid-probe —
`the engine is transitioning: the live handle vanished mid-probe`.

Each method face (`version`, `protocolCapabilities`) is mutually exclusive by
construction:

- `present: true` — the engine answered; `face` is the engine's own JSON answer
  passed through **verbatim**. The connector never re-derives, re-keys, or
  normalizes it (the kt-engine self-certification face keeps its upstream
  spelling, including `profiles_v2`).
- `present: false` — the engine did not answer; `reason` is a static string:
  the `unreachableReason` above when the engine is not running, or
  - `the engine stopped answering mid-probe` (timeout / not-running /
    exited / backpressure during the probe),
  - `the engine answered an error for the version method …`,
  - `the engine answered an error for the self-certification method …`
    (a stock signal-cli engine does not carry the kt-engine face; the full
    string carries the error class).

Absent engine answers are the designed degradation — a diagnostic never fails
as a whole because one engine face could not answer.

## Lock face

The `.kt-signal-connector.lock` occupancy, in the contract's classification:

| `classification` | `present` | `held` | meaning |
| ---------------- | --------- | ------ | ------- |
| `none`     | false | false | no lock file |
| `held`     | true  | true  | a kernel lock is held — by construction the holder is alive (the kernel releases it on any exit); `holderAlive: true` is a kernel guarantee |
| `stale`    | true  | false | the recorded pid is dead — a crashed holder's remnant |
| `released` | true  | false | the recorded pid is alive or unknown — the designed post-release remnant; the lock file is deliberately never deleted, so this is the normal state after a clean connector exit |

- `pid` is the decimal pid the connector wrote into the lock file at acquire
  time; absent for files written by pre-face connectors or corrupted content.
- `holderAlive` is `true` whenever `held`; otherwise it is the recorded pid's
  liveness (`kill(pid, 0)`), absent when no pid is known.
- The probe is observational only: open read-only, `flock(LOCK_EX|LOCK_NB)`,
  unlock, close. A `held` verdict means someone else holds it; the doctor
  never waits for, breaks, or removes a lock.

## Data-directory face

`path` is the launcher-configured signal data directory (the authenticated
launcher already knows it; it never enters logs). `exists` is a stat;
`writable` is access-based (`access(2)` `W_OK`) — the directory's own
writability without writing anything. `freeBytes`/`totalBytes` come from
`statvfs(2)` and are absent when the directory does not exist. On Windows the
writability check falls back to the read-only attribute — best effort there
until a Windows test host exists; every other field behaves identically.

## Recent-error ring

`recentErrors` aggregates the process's classified failures:

- `window` is the ring's fixed capacity (64) — the newest 64 recorded events
  are the aggregation basis.
- `recordedTotal` counts every event ever recorded this process; when it
  exceeds `window`, the families below cover only the newest slice.
- `families` merges two sources: `source: "host"` (a host dispatch answered a
  structured error; `family` is the wire error code, `retryable` the code's
  retryability) and `source: "protocol"` (an engine protocol warning;
  `family` is the warning kind). Sorted by `count`, descending;
  `lastAtUnixMs` is the newest event's unix-millisecond timestamp.
- The ring is in-memory only: it resets with the process and holds no message
  content — codes, kinds, counts, timestamps.

## Network face

`watchdogRestartsTotal` and `receiveDroppedTotal` are the two existing
counters (`metrics::record_watchdog_restart`,
`metrics::record_receive_drop`) read out as-is — the doctor adds no new
network instrumentation.
