# Connector soak harness (24h multi-account stability gate)

Rebuild of the lost run-2 soak tooling as in-repo code (optimization-plan
§6.4 M3.1–M3.5, P2-13). The run-2 methodology (docs/handover.md §3.2) is
preserved — one driver polling `runtime.status` every 5 minutes, one
per-process RSS sample per minute, artifacts `driver.log` / `rss.csv` /
`connector.log` — and the two run-2 gaps are fixed: the judge now pins
**continuously active** time (sleep holes fail or explicitly downgrade), and
the fake signal-cli accepts controlled-rate receive injection so a baseline
exists without real accounts (decision D4).

Layout:

| path | role |
| --- | --- |
| `launch.sh` | one-shot launcher: release build → caffeinate-wrapped driver → report → verdict |
| `soak_driver.py` | spawns the connector + engines, keeps one authenticated host session for the whole run, writes the artifacts |
| `judge.py` | independent verdict from artifacts only; exit 0 PASS / 1 FAIL / 2 DEGRADED |
| `rss_report.py` | rss.csv aggregation (per-series first/last/min/max/p95/drift/slope, aggregate, sleep-hole list); `--json` for machine use |
| `runs/` | run artifacts (gitignored) |

## Quick start

```bash
# full default: 24h, 8 proxy groups (default + g1..g7), fake engines,
# load ladder 1 -> 5 -> 20 msg/min/account x 2 accounts
packaging/soak/launch.sh

# short smoke soak (verifies the harness end to end)
packaging/soak/launch.sh --duration-seconds 900 --status-interval 60

# idle soak, no injection (the run-2 shape)
packaging/soak/launch.sh --load-steps ""
```

Manual driver + judge (e.g. for cron; cron tasks die with their session —
schedule a fresh invocation, not a session-local one):

```bash
cargo build --release
python3 packaging/soak/soak_driver.py --run-dir packaging/soak/runs/manual-1 \
    --duration-seconds 86400
python3 packaging/soak/judge.py packaging/soak/runs/manual-1
python3 packaging/soak/rss_report.py packaging/soak/runs/manual-1 --json baseline.json
```

Judge flags: `--require-duration N` (default: the run's meta duration),
`--drift-mib 48`, `--slope-mib-per-hour 4`, `--max-drops 0`,
`--allow-degraded` (sleep holes downgrade to exit 2 instead of FAIL).

## Sleep discipline (read before citing a run)

Run 2 passed 24 h of wall-clock while the host slept ~13.3 h (16 gaps,
`caffeinate -dims` does not stop lid-close sleep). This harness:

- `launch.sh` wraps the run in **`caffeinate -s`** (system-sleep prevention;
  effective on AC — keep the machine on AC and the lid open), and
- the judge independently detects sampling holes (any gap between sample
  rounds beyond max(3 × median spacing, 600 s)) and fails the run in strict
  mode. A hole-free run has nothing to downgrade: PASS means continuously
  awake for the required duration.

## Pass criteria (what judge.py enforces)

1. **Duration**: wall span ≥ required AND active span (wall − hole time) ≥
   required. Holes present → strict FAIL (`--allow-degraded` → DEGRADED).
2. **Engine state**: every rss.csv engine row and every driver.log status
   line `state=running`, `resourcePressure=false`, for the whole run.
3. **No restart**: exactly one engine pid per group across all samples, and
   the connector's own 60s metrics snapshot reports
   `watchdog_restarts_total=0` (a missing snapshot is itself a FAIL).
4. **RSS flatness** (per engine, MiB): FAIL when the mean of the last 5% of
   samples exceeds the mean of the first 5% by > 48 MiB, or when BOTH the
   least-squares slope > 4 MiB/h AND the fitted growth over the window
   > 48 MiB (a slow leak must clear both bars; a step-then-plateau like
   run-2's +11 MiB does not). Thresholds are flags — tune them against the
   baseline this harness produces, then tighten.
5. **Bounded drops (under load)**: when injection ran, the final
   `receive_dropped_total` must be ≤ `--max-drops` (default 0). A no-load
   run reports the counter without applying the bound.

## Load injection (fake engines only)

`soak_driver.py` steers the fake signal-cli through a `.fixture-load` marker
file in each group's data directory: `{"ratePerMinute": R, "accounts": N}`.
The fake emits receive notifications at R messages per minute per account
over N simulated receiving accounts (round-robin, strictly increasing
timestamps so receive dedupe does not collapse them, per-engine account
ranges so engines sharing one store never flap an account between groups).
Each emitted message appends to `.fixture-load-log.jsonl` in the data dir,
and the driver counts host events (`message.changed` …) as fan-out evidence.
The traffic exercises the full receive path: engine stdout → bounded receive
queue → persistence → store → event fan-out.

Keep `--load-accounts ≤ 8`: receive-driven account upserts bypass the link
ceiling (`MAX_ACCOUNTS_PER_ENGINE` gates linking only, by design), and the
soak models a legal engine.

Real engines: pass `--signal-cli <path>` (plus `--java-home`, `--native` as
needed) and no load steps — injection is fake-only. The real-account ladder
is a separate supply decision (D4) and does not block baselines.

## Artifacts

```
runs/<id>/
  driver.log      status lines + lifecycle + load steps (run-2 format)
  rss.csv         ts,role,group_id,pid,rss_kb,state,pressure (run-2 format)
  connector.log   connector stderr incl. 60s metrics snapshots
  load.log        load ladder steps
  meta.json       run parameters, connector sha256, event counts
  baseline.json   rss_report.py --json output (if requested)
```

Keep finished runs out of git (runs/ is gitignored); archive notable ones
with `cp -a` like `soak-archives/run2-20260828/`.

## Why no GitHub CI (M3.5)

Hosted runners cap at 6 h — a 24 h gate there is dead code. Capacity numeric
assertions ride `cargo test` instead (schema-consistency pairs + behavior
tests: account ceiling, group ceiling, RSS policy constants); the ≥24h soak
is this directory, run manually or from a local timer via `launch.sh`.
