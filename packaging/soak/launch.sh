#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
#
# One-shot reproducible 24h soak launch (optimization-plan §6.4 M3.1/M3.5).
#
# Usage:
#   packaging/soak/launch.sh [extra soak_driver.py args...]
#
# Examples:
#   packaging/soak/launch.sh                          # 24h, 8 fake engines, 1/5/20 msg/min ladder x2 accounts
#   packaging/soak/launch.sh --duration-seconds 7200  # 2h smoke soak
#   packaging/soak/launch.sh --load-steps ""          # idle soak (no injection)
#
# Sleep discipline (the run-2 lesson, docs/handover.md §3.2): caffeinate -dims
# did NOT stop lid-close sleep — 16 gaps, ~13.3h suspended, only 10.7h
# effective. This launcher wraps the whole run in `caffeinate -s` (system
# sleep prevention, effective on AC power). That is defense, not proof: keep
# the lid open and the machine on AC, and let judge.py detect any hole that
# still happens (strict FAIL unless --allow-degraded).

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
SOAK="$ROOT/packaging/soak"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_DIR="$SOAK/runs/$RUN_ID"

cargo build --release --manifest-path "$ROOT/Cargo.toml"
mkdir -p "$(dirname "$RUN_DIR")"

if command -v caffeinate >/dev/null 2>&1; then
  set -- caffeinate -s python3 "$SOAK/soak_driver.py" --run-dir "$RUN_DIR" "$@"
else
  set -- python3 "$SOAK/soak_driver.py" --run-dir "$RUN_DIR" "$@"
fi

echo "== soak run $RUN_ID -> $RUN_DIR"
DRIVER_STATUS=0
"$@" || DRIVER_STATUS=$?

echo "== rss baseline report"
python3 "$SOAK/rss_report.py" "$RUN_DIR" || true

echo "== verdict"
JUDGE_STATUS=0
python3 "$SOAK/judge.py" "$RUN_DIR" || JUDGE_STATUS=$?
echo "== artifacts: $RUN_DIR (driver.log, rss.csv, connector.log, load.log, meta.json)"
echo "== driver exit $DRIVER_STATUS; judge exit $JUDGE_STATUS (0=PASS 1=FAIL 2=DEGRADED)"
exit "$JUDGE_STATUS"
