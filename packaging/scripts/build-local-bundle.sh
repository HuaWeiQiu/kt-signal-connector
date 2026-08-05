#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
OUT="${1:-"$ROOT/packaging/out/local-bundle"}"
SIGNAL_CLI_SRC="${SIGNAL_CLI_SRC:-}"
JRE_MARKER="${JRE_MARKER:-}"

mkdir -p "$OUT/bin" "$OUT/jre" "$OUT/licenses"
cd "$ROOT"

cargo build --release
cp -f "$ROOT/target/release/kt-signal-connector" "$OUT/bin/kt-signal-connector"
cp -f "$ROOT/LICENSE" "$OUT/licenses/kt-signal-connector.AGPL-3.0-only.txt"
cp -f "$ROOT/NOTICE" "$OUT/licenses/NOTICE.txt"

if [[ -n "$SIGNAL_CLI_SRC" && -f "$SIGNAL_CLI_SRC" ]]; then
  cp -f "$SIGNAL_CLI_SRC" "$OUT/bin/signal-cli"
else
  # Placeholder artifact so local manifest/LKG flows can be exercised without claiming a
  # production signal-cli distribution was produced by this script.
  printf 'signal-cli-placeholder\n' >"$OUT/bin/signal-cli"
fi

if [[ -n "$JRE_MARKER" && -f "$JRE_MARKER" ]]; then
  cp -f "$JRE_MARKER" "$OUT/jre/release"
else
  printf 'JAVA_VERSION="placeholder"\n' >"$OUT/jre/release"
fi

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
  --signal-cli-path bin/signal-cli \
  --jre-path jre/release \
  --output "$OUT/manifest.json"

"$OUT/bin/kt-signal-connector" package verify \
  --bundle-dir "$OUT" \
  --manifest "$OUT/manifest.json"

echo "local bundle ready: $OUT"
