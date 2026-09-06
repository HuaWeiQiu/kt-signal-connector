#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only

"""Independent soak verdict for one run directory (optimization-plan §6.4
M3.1/M3.5). Reads only the artifacts (driver.log, rss.csv, connector.log,
load.log, meta.json) — it never talks to a live connector — and exits:

    0  PASS      every criterion green, sampling continuous
    1  FAIL      a health or duration criterion is broken, or sampling holes
                  (host sleep) were found in strict mode (the default, the
                  run-2 lesson: 24 h wall-clock with 10.7 h awake must not
                  count as a 24 h soak)
    2  DEGRADED  every health criterion green, but sampling holes exist and
                  --allow-degraded was passed (the run counts as an
                  explicitly downgraded, shorter-than-declared soak)

Criteria (numbers in parentheses are the defaults, all overridable):

  duration      wall span >= --require-duration AND active span (wall span
                minus hole time) >= --require-duration
  continuity    no sample-round gap beyond the rss_report hole threshold
                (max(3 x median spacing, 600 s)); holes are the sleep
                evidence — strict FAIL, or DEGRADED with --allow-degraded
  engine state  every rss.csv engine row state=running; every driver.log
                status line state 'running'
  pressure      resourcePressure false in every row and status line
  no restart    one engine pid per group across the whole run; the
                connector's own metrics must report watchdog_restarts_total=0
                (a missing metrics snapshot is itself a FAIL: no evidence)
  rss climb     per engine: mean(first window) vs mean(last window) (each the
                first/last 5% of samples, min 3) drift > --drift-mib (48)
                FAILs; a least-squares climb also FAILs when both
                slope > --slope-mib-per-hour (4) and fitted growth over the
                window > --drift-mib (a slow leak must clear both bars)
  bounded drops when load ran (load.log exists): receive_dropped_total from
                the last metrics snapshot must be <= --max-drops (0); a run
                without load reports the counter without applying a bound

Usage:
    python3 packaging/soak/judge.py <run-dir> [--allow-degraded]
        [--require-duration 86400] [--drift-mib 48] [--slope-mib-per-hour 4]
        [--max-drops 0]
"""

import argparse
import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import rss_report  # noqa: E402  (shared aggregation, same directory)

STATUS_LINE = re.compile(r"^\[(?P<ts>[^]]+)\] status: agg=(?P<agg>.+)$")
METRICS_SNAPSHOT = re.compile(r"connector metrics snapshot")
DROP_COUNTER = re.compile(r"receive_dropped_total=(\d+)")
RESTART_COUNTER = re.compile(r"watchdog_restarts_total=(\d+)")


class Verdict:
    def __init__(self):
        self.failures = []
        self.degraded = []
        self.checks = []

    def check(self, name, ok, detail):
        self.checks.append((name, ok, detail))
        return ok

    def fail(self, name, detail):
        self.checks.append((name, False, detail))
        self.failures.append(f"{name}: {detail}")

    def degrade(self, name, detail):
        self.checks.append((name, False, detail))
        self.degraded.append(f"{name}: {detail}")

    def exit_code(self, allow_degraded):
        if self.failures:
            return 1
        if self.degraded and allow_degraded:
            return 2
        if self.degraded:
            # Holes are strict failures unless explicitly downgraded.
            for entry in self.degraded:
                self.failures.append(entry)
            return 1
        return 0


def window_mean(values, tail):
    window = max(3, int(round(len(values) * 0.05)))
    window = min(window, max(1, len(values) // 2))
    if tail:
        picked = values[-window:]
    else:
        picked = values[:window]
    return sum(picked) / len(picked)


def last_metrics_snapshot(connector_log):
    """Final receive_dropped_total / watchdog_restarts_total pair, or None."""
    drops = restarts = None
    saw_snapshot = False
    for line in connector_log:
        if METRICS_SNAPSHOT.search(line):
            saw_snapshot = True
            drop_match = DROP_COUNTER.search(line)
            restart_match = RESTART_COUNTER.search(line)
            if drop_match:
                drops = int(drop_match.group(1))
            if restart_match:
                restarts = int(restart_match.group(1))
    return (drops, restarts) if saw_snapshot else None


def judge(run_dir, args):
    verdict = Verdict()
    run_dir = Path(run_dir)

    meta_path = run_dir / "meta.json"
    if not meta_path.is_file():
        verdict.fail("artifacts", "no meta.json — not a driver run directory")
        return verdict
    meta = json.loads(meta_path.read_text())
    require_duration = (
        args.require_duration or meta.get("duration_seconds") or 86400
    )

    samples = rss_report.read_samples(run_dir / "rss.csv")
    if not samples:
        verdict.fail("artifacts", "rss.csv is missing or empty")
        return verdict
    report = rss_report.aggregate_report(run_dir)

    # --- duration & continuity (the run-2 lesson) -----------------------
    span = report["window"]["seconds"]
    holes = report["holes"]
    active = span - holes["total_hole_seconds"]
    verdict.check(
        "duration",
        span >= require_duration,
        f"wall {span:.0f}s / required {require_duration}s",
    )
    if span < require_duration:
        verdict.fail("duration", f"wall span {span:.0f}s < required {require_duration}s")
    if holes["holes"]:
        detail = (
            f"{len(holes['holes'])} sampling gap(s) beyond "
            f"{holes.get('threshold_seconds', '?')}s, ~{holes['total_hole_seconds']:.0f}s "
            f"suspended, longest {holes['longest_hole_seconds']:.0f}s "
            f"(host sleep evidence; active {active:.0f}s of {span:.0f}s)"
        )
        verdict.degrade("continuity", detail)
        if active < require_duration:
            verdict.fail(
                "duration",
                f"active span {active:.0f}s < required {require_duration}s "
                "after subtracting sleep holes",
            )

    # --- engine state / pressure / restarts -----------------------------
    bad_states = {}
    for sample in samples:
        if sample["role"] != "engine":
            continue
        if sample["state"] and sample["state"] != "running":
            bad_states.setdefault(sample["state"], set()).add(sample["group_id"])
        if str(sample["pressure"]).lower() == "true":
            verdict.fail(
                "pressure",
                f"group {sample['group_id']} reported resourcePressure=true",
            )
    if bad_states:
        verdict.fail(
            "engine state",
            "non-running samples: "
            + ", ".join(
                f"{state} in {sorted(groups)}" for state, groups in bad_states.items()
            ),
        )
    else:
        verdict.check("engine state", True, "every engine sample state=running")

    engine_rows = [s for s in samples if s["role"] == "engine"]
    pids = {}
    for sample in engine_rows:
        pids.setdefault(sample["group_id"], set()).add(str(sample["pid"]))
    restarted = {
        group: sorted(p) for group, p in pids.items() if len(p) > 1
    }
    if restarted:
        verdict.fail("no restart", f"pid changes per group: {restarted}")
    else:
        verdict.check(
            "no restart",
            True,
            f"single pid for each of {len(pids)} engine group(s)",
        )

    driver_log = (run_dir / "driver.log").read_text().splitlines()
    status_lines = [line for line in driver_log if "status: agg=" in line]
    bad_status = [
        line
        for line in status_lines
        if "'state': 'running'" not in line or "resourcePressure': False" not in line
    ]
    if bad_status:
        verdict.fail("status lines", f"{len(bad_status)} bad status line(s), first: {bad_status[0]}")
    else:
        verdict.check("status lines", True, f"{len(status_lines)} status lines all running/no pressure")
    if not any("soak duration complete" in line for line in driver_log):
        verdict.fail("completion", "driver.log has no 'soak duration complete' line")

    metrics = last_metrics_snapshot((run_dir / "connector.log").read_text().splitlines())
    if metrics is None:
        verdict.fail("metrics", "no 'connector metrics snapshot' line in connector.log")
    else:
        drops, restarts = metrics
        if restarts not in (None, 0):
            verdict.fail("no restart", f"watchdog_restarts_total={restarts}")
        else:
            verdict.check("no restart", True, "watchdog_restarts_total=0")

    # --- RSS monotonic climb --------------------------------------------
    grouped = _grouped(run_dir)
    for (role, group), rows in sorted(grouped.items()):
        if role != "engine":
            continue
        values = [row for row in rows if row["rss_mib"] is not None]
        if len(values) < 6:
            verdict.check(
                "rss climb",
                True,
                f"{group}: only {len(values)} RSS samples, climb test skipped",
            )
            continue
        mib = [row["rss_mib"] for row in values]
        drift = window_mean(mib, tail=True) - window_mean(mib, tail=False)
        stats = rss_report.series_stats(rows)
        slope = stats["slope_mib_per_hour"]
        fitted = stats["fitted_growth_mib"]
        climbed = drift > args.drift_mib or (
            slope > args.slope_mib_per_hour and fitted > args.drift_mib
        )
        if climbed:
            verdict.fail(
                "rss climb",
                f"{group}: window drift {drift:+.1f} MiB, slope {slope:+.2f} MiB/h, "
                f"fitted growth {fitted:+.1f} MiB over {stats['window_hours']}h",
            )
        else:
            verdict.check(
                "rss climb",
                True,
                f"{group}: drift {drift:+.1f} MiB, slope {slope:+.2f} MiB/h, "
                f"fitted {fitted:+.1f} MiB",
            )

    # --- bounded drops under load ---------------------------------------
    load_ran = (run_dir / "load.log").is_file()
    if metrics is not None:
        drops = metrics[0]
        if drops is None:
            verdict.fail("metrics", "metrics snapshot has no receive_dropped_total")
        elif load_ran:
            if drops <= args.max_drops:
                verdict.check("bounded drops", True, f"receive_dropped_total={drops} under load")
            else:
                verdict.fail(
                    "bounded drops",
                    f"receive_dropped_total={drops} > {args.max_drops} under load",
                )
        else:
            verdict.check(
                "bounded drops",
                True,
                f"no load ran; receive_dropped_total={drops} (bound not applied)",
            )
    return verdict


def _grouped(run_dir):
    """role/group -> rows, reusing rss_report's reader."""
    from collections import defaultdict

    grouped = defaultdict(list)
    for sample in rss_report.read_samples(run_dir / "rss.csv"):
        grouped[(sample["role"], sample["group_id"])].append(sample)
    return grouped


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_dir")
    parser.add_argument("--allow-degraded", action="store_true")
    parser.add_argument("--require-duration", type=int, default=0)
    parser.add_argument("--drift-mib", type=float, default=48.0)
    parser.add_argument("--slope-mib-per-hour", type=float, default=4.0)
    parser.add_argument("--max-drops", type=int, default=0)
    args = parser.parse_args()

    verdict = judge(args.run_dir, args)
    print(f"soak verdict for {args.run_dir}")
    for name, ok, detail in verdict.checks:
        print(f"  {'PASS' if ok else 'FAIL'}  {name:<14} {detail}")
    code = verdict.exit_code(args.allow_degraded)
    if code == 0:
        print("VERDICT: PASS")
    elif code == 2:
        print("VERDICT: DEGRADED (health green; sampling holes, explicitly downgraded)")
    else:
        print("VERDICT: FAIL")
        for failure in verdict.failures:
            print(f"  - {failure}")
    return code


if __name__ == "__main__":
    sys.exit(main())
