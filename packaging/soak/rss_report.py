#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only

"""Aggregate a soak run's rss.csv into a baseline summary report.

Reads the per-process RSS samples written by packaging/soak/soak_driver.py
(the same csv shape as the archived run-2 artifacts,
`ts,role,group_id,pid,rss_kb,state,pressure`) and prints one line per series
(connector + one engine per proxy group) plus the aggregate sum and the
sample-hole summary. `--json PATH` additionally writes the same numbers as
machine-readable JSON; packaging/soak/judge.py reuses these functions for its
monotonic-climb and continuity criteria.

Usage:
    python3 packaging/soak/rss_report.py <run-dir> [--json out.json]
"""

import argparse
import csv
import json
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

# A sample round is "missing" beyond this multiple of the median round
# spacing; holes beyond it are treated as host-sleep candidates by the judge.
HOLE_SPACING_MULTIPLE = 3.0
# ... but never below this floor: status cadence itself is 5 minutes in the
# historical runs, and short scheduler jitter must not count as a hole.
HOLE_FLOOR_SECONDS = 600.0


def parse_timestamp(value):
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S")


def read_samples(csv_path):
    """Rows of rss.csv as dicts with parsed epoch seconds and rss MiB."""
    samples = []
    with open(csv_path, newline="") as handle:
        for row in csv.DictReader(handle):
            try:
                ts = parse_timestamp(row["ts"]).timestamp()
            except (ValueError, KeyError):
                continue
            rss_kb = (row.get("rss_kb") or "").strip()
            samples.append(
                {
                    "ts": ts,
                    "role": row.get("role", ""),
                    "group_id": row.get("group_id", "-"),
                    "pid": row.get("pid", ""),
                    "rss_mib": int(rss_kb) / 1024.0 if rss_kb else None,
                    "state": row.get("state", ""),
                    "pressure": row.get("pressure", ""),
                }
            )
    samples.sort(key=lambda sample: sample["ts"])
    return samples


def series_stats(samples):
    """(first/last/min/max/mean MiB, drift MiB, slope MiB/h) for one series.

    Rows without an RSS sample (rss_mib None) are skipped; callers keep them
    for pid/state/pressure evidence instead.
    """
    samples = [sample for sample in samples if sample["rss_mib"] is not None]
    if not samples:
        return {"samples": 0}
    values = [sample["rss_mib"] for sample in samples]
    first = samples[0]["ts"]
    hours = max((samples[-1]["ts"] - first) / 3600.0, 1e-9)
    mean = sum(values) / len(values)
    # Least-squares slope of MiB against time; the fitted total growth over
    # the window is slope * hours, robust against single-sample spikes.
    if len(values) > 1:
        t_mean = sum(sample["ts"] - first for sample in samples) / len(samples)
        denominator = sum((sample["ts"] - first - t_mean) ** 2 for sample in samples)
        slope_per_hour = (
            sum(
                (sample["ts"] - first - t_mean) * (sample["rss_mib"] - mean)
                for sample in samples
            )
            / denominator
            / 3600.0
            if denominator > 0
            else 0.0
        )
    else:
        slope_per_hour = 0.0
    ordered = sorted(values)
    p95 = ordered[min(len(ordered) - 1, int(round(0.95 * (len(ordered) - 1))))]
    return {
        "samples": len(values),
        "first_mib": round(values[0], 1),
        "last_mib": round(values[-1], 1),
        "min_mib": round(min(values), 1),
        "max_mib": round(max(values), 1),
        "mean_mib": round(mean, 1),
        "p95_mib": round(p95, 1),
        "drift_mib": round(values[-1] - values[0], 1),
        "slope_mib_per_hour": round(slope_per_hour, 3),
        "fitted_growth_mib": round(slope_per_hour * hours, 1),
        "window_hours": round(hours, 2),
    }


def hole_summary(samples):
    """Gaps between sample rounds that look like a suspended host.

    A round is the set of rows sharing a timestamp; the gap between
    consecutive rounds must stay near the sampling cadence. Anything beyond
    max(HOLE_SPACING_MULTIPLE x median spacing, HOLE_FLOOR_SECONDS) is
    reported as a hole with its duration — the evidence the judge uses to
    fail or downgrade a run that slept (the run-2 lesson: wall-clock 24 h
    with only 10.7 h effectively awake).
    """
    rounds = sorted({sample["ts"] for sample in samples})
    if len(rounds) < 3:
        return {"holes": [], "total_hole_seconds": 0.0, "longest_hole_seconds": 0.0}
    spacings = [b - a for a, b in zip(rounds, rounds[1:])]
    spacings.sort()
    median = spacings[len(spacings) // 2] or 1.0
    threshold = max(HOLE_SPACING_MULTIPLE * median, HOLE_FLOOR_SECONDS)
    holes = [
        {"from": rounds[index], "to": rounds[index + 1], "seconds": round(gap, 1)}
        for index, gap in enumerate(spacings)
        if gap > threshold
    ]
    return {
        "threshold_seconds": round(threshold, 1),
        "holes": holes,
        "total_hole_seconds": round(sum(hole["seconds"] for hole in holes), 1),
        "longest_hole_seconds": round(max((hole["seconds"] for hole in holes), default=0.0), 1),
    }


def aggregate_report(run_dir):
    """Full report dict for one run directory (rss.csv required)."""
    csv_path = Path(run_dir) / "rss.csv"
    if not csv_path.is_file():
        raise SystemExit(f"no rss.csv under {run_dir}")
    samples = read_samples(csv_path)
    if not samples:
        raise SystemExit(f"rss.csv under {run_dir} has no usable rows")

    grouped = defaultdict(list)
    for sample in samples:
        grouped[(sample["role"], sample["group_id"])].append(sample)

    series = {}
    for (role, group_id), rows in sorted(grouped.items()):
        series[f"{role}:{group_id}"] = series_stats(rows)

    # Aggregate = sum of engine RSS per round (the connector's own share is
    # reported separately), matching the aggregate the runtime reports.
    # Rounds where an engine has no sample yet sum the engines that do.
    per_round = defaultdict(float)
    for sample in samples:
        if sample["role"] == "engine" and sample["rss_mib"] is not None:
            per_round[sample["ts"]] += sample["rss_mib"]
    aggregate_rows = [
        {"ts": ts, "rss_mib": total} for ts, total in sorted(per_round.items())
    ]
    series["aggregate:engines"] = (
        series_stats(aggregate_rows) if aggregate_rows else {"samples": 0}
    )

    return {
        "run_dir": str(run_dir),
        "window": {
            "start": samples[0]["ts"],
            "end": samples[-1]["ts"],
            "seconds": round(samples[-1]["ts"] - samples[0]["ts"], 1),
            "rounds": len({sample["ts"] for sample in samples}),
        },
        "series": series,
        "holes": hole_summary(samples),
    }


def format_report(report):
    lines = [
        f"RSS baseline report: {report['run_dir']}",
        (
            f"window: {datetime.fromtimestamp(report['window']['start']).isoformat()}"
            f" .. {datetime.fromtimestamp(report['window']['end']).isoformat()}"
            f" ({report['window']['seconds']:.0f} s,"
            f" {report['window']['rounds']} sample rounds)"
        ),
        "",
        (
            f"{'series':<24}{'samples':>8}{'first':>8}{'last':>8}{'min':>8}{'max':>8}"
            f"{'p95':>8}{'drift':>8}{'slope/h':>9}{'fit':>8}"
        ),
    ]
    for name, stats in report["series"].items():
        if stats.get("samples", 0) == 0:
            continue
        lines.append(
            f"{name:<24}{stats['samples']:>8}{stats['first_mib']:>8.1f}"
            f"{stats['last_mib']:>8.1f}{stats['min_mib']:>8.1f}{stats['max_mib']:>8.1f}"
            f"{stats['p95_mib']:>8.1f}{stats['drift_mib']:>8.1f}"
            f"{stats['slope_mib_per_hour']:>9.2f}{stats['fitted_growth_mib']:>8.1f}"
        )
    lines.append("(all figures MiB; drift = last-first, fit = slope x window)")
    holes = report["holes"]
    if holes["holes"]:
        lines.append(
            f"holes: {len(holes['holes'])} gap(s) > {holes.get('threshold_seconds', '?')} s,"
            f" suspended ~{holes['total_hole_seconds']:.0f} s total,"
            f" longest {holes['longest_hole_seconds']:.0f} s"
        )
    else:
        lines.append("holes: none beyond threshold (continuous sampling)")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_dir", help="soak run directory containing rss.csv")
    parser.add_argument("--json", metavar="PATH", help="also write the report as JSON")
    args = parser.parse_args()

    report = aggregate_report(args.run_dir)
    print(format_report(report))
    if args.json:
        Path(args.json).write_text(json.dumps(report, indent=2) + "\n")
        print(f"wrote {args.json}", file=sys.stderr)


if __name__ == "__main__":
    main()
