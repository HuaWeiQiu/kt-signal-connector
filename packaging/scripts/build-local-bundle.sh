#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
OUT="${1:-"$ROOT/packaging/out/local-bundle"}"

# The output directory is owned by this script: rebuild it from scratch so a
# stale JVM-era bundle (jre/, bin/signal-cli) can never leak into a manifest.
case "$OUT" in "$ROOT"|"$ROOT"/|"$ROOT"/packaging|"$ROOT"/packaging/)
    echo "refusing to use the repo root or packaging/ as the bundle output" >&2
    exit 2
    ;;
esac
rm -rf "$OUT"
mkdir -p "$OUT/bin" "$OUT/licenses"
cd "$ROOT"

cargo build --release
cp -f "$ROOT/target/release/kt-signal-connector" "$OUT/bin/kt-signal-connector"
cp -f "$ROOT/LICENSE" "$OUT/licenses/kt-signal-connector.AGPL-3.0-only.txt"
cp -f "$ROOT/NOTICE" "$OUT/licenses/NOTICE.txt"

# The kt engine face embeds libsignal (AGPL-3.0-only) and every manifest must
# carry the license entry; this local stub is a pointer, not the license text.
cat >"$OUT/licenses/libsignal.txt" <<'EOF'
libsignal
https://github.com/signalapp/libsignal
SPDX-License-Identifier: AGPL-3.0-only

This bundle embeds libsignal through the kt engine face. The license text and
the corresponding source ship with the engine distribution; see NOTICE.
EOF

# Minimal CycloneDX-like SBOM placeholder generated from cargo metadata.
python3 - <<PY
import json, subprocess, pathlib
root = pathlib.Path("$ROOT")
out = pathlib.Path("$OUT")
meta = json.loads(subprocess.check_output(["cargo", "metadata", "--format-version", "1", "--no-deps"], cwd=root))
packages = []
for pkg in meta.get("packages", []):
    packages.append({
        "type": "library",
        "name": pkg["name"],
        "version": pkg["version"],
        "bom-ref": f'{pkg["name"]}@{pkg["version"]}',
    })
sbom = {
    "bomFormat": "CycloneDX",
    "specVersion": "1.5",
    "version": 1,
    "metadata": {
        "component": {
            "type": "application",
            "name": "kt-signal-connector",
            "version": meta["packages"][0]["version"] if meta.get("packages") else "0.0.0",
        }
    },
    "components": packages,
}
(out / "sbom.cdx.json").write_text(json.dumps(sbom, indent=2) + "\n")
PY

cat >"$OUT/build-record.json" <<EOF
{
  "bundleKind": "local-dev",
  "platform": "$(uname -s)-$(uname -m)",
  "connectorCrateVersion": "$(cargo pkgid | sed 's/.*#//')",
  "rustc": "$(rustc --version)",
  "cargo": "$(cargo --version)",
  "note": "Unsigned local bundle. Not a production release artifact."
}
EOF

"$OUT/bin/kt-signal-connector" package manifest \
  --bundle-dir "$OUT" \
  --bundle-id "local-$(date -u +%Y%m%d%H%M%S)" \
  --connector-path bin/kt-signal-connector \
  --license "kt-signal-connector:AGPL-3.0-only:licenses/kt-signal-connector.AGPL-3.0-only.txt" \
  --license "libsignal:AGPL-3.0-only:licenses/libsignal.txt" \
  --output "$OUT/manifest.json"

"$OUT/bin/kt-signal-connector" package verify \
  --bundle-dir "$OUT" \
  --manifest "$OUT/manifest.json"

echo "local bundle ready: $OUT"
