#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Build signal-cli as a GraalVM native image (optimization plan Phase 5 spike).
#
# Inputs are pinned and SHA256-verified, so CI and local runs produce the same
# binary from the same bytes:
#   - signal-cli v0.14.7 source tarball (GitHub archive of the tag)
#   - GraalVM Community 25.2.4 macos-aarch64 tarball. NOTE: the upstream release
#     asset is mis-named "25i2-25.0.4" but its content is 25.2.4+7.1 — the SHA256
#     below is the one published next to the asset and matches the local spike.
#
# The build applies the reachability-metadata trim (see
# trim-native-reachability-metadata.py) which cut the binary 340 MB -> 112.8 MiB
# in the 2026-08-25 local spike.
#
# Nothing is written outside the workdir except the --out directory. Default
# workdir is a mktemp scratch that is deleted on exit; pass --workdir to reuse
# one (faster local reruns: tarballs and the Gradle cache are kept).
#
# Usage:
#   packaging/scripts/build-signal-cli-native.sh [--out DIR] [--workdir DIR]
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

# ---- Pinned inputs (single source of truth; also emitted as build-info.env) ----
SIGNAL_CLI_VERSION="0.14.8"
SIGNAL_CLI_TAG="v${SIGNAL_CLI_VERSION}"
SIGNAL_CLI_URL="https://github.com/AsamK/signal-cli/archive/refs/tags/${SIGNAL_CLI_TAG}.tar.gz"
SIGNAL_CLI_SHA256="08b56db45109e351c8f41bd73e05bcb1e29bae9c51783d51b8c3c4996ac83a7b"
GRAALVM_VERSION="25.2.4"
GRAALVM_URL="https://github.com/graalvm/graalvm-ce-builds/releases/download/graal-${GRAALVM_VERSION}/graalvm-community-jdk-25i2-25.0.4_macos-aarch64_bin.tar.gz"
GRAALVM_SHA256="507330fff8907de51b8621c95a21a491fa9b0d8c240de184594f40f83add3bfa"
GRAALVM_DIR_NAME="graalvm-community-25.2.4+7.1" # top-level directory inside the tarball

OUT=""
WORKDIR=""
while [ $# -gt 0 ]; do
  case "$1" in
    --out) OUT="$2"; shift 2 ;;
    --workdir) WORKDIR="$2"; shift 2 ;;
    -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "FAIL: unknown argument $1" >&2; exit 2 ;;
  esac
done

CREATED_WORKDIR=""
if [ -z "$WORKDIR" ]; then
  WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/signal-cli-native-build.XXXXXX")"
  CREATED_WORKDIR=1
fi
mkdir -p "$WORKDIR"
WORKDIR="$(cd "$WORKDIR" && pwd)"
if [ -n "$CREATED_WORKDIR" ]; then
  trap 'rm -rf "$WORKDIR"' EXIT
fi

if [ -z "$OUT" ]; then
  OUT="$WORKDIR/out"
fi
mkdir -p "$OUT"
OUT="$(cd "$OUT" && pwd)"

download() { # <url> <dest> <sha256>
  if [ ! -f "$2" ]; then
    echo "downloading: $1"
    curl -fL --retry 3 --connect-timeout 30 -o "$2" "$1"
  else
    echo "reusing cached: $2"
  fi
  local actual
  actual="$(shasum -a 256 "$2" | awk '{print $1}')"
  if [ "$actual" != "$3" ]; then
    echo "FAIL: SHA256 mismatch for $2" >&2
    echo "  expected: $3" >&2
    echo "  actual:   $actual" >&2
    exit 1
  fi
  echo "sha256 ok: $3"
}

echo "== workdir: $WORKDIR"
echo "== out:     $OUT"

# 1. Fetch and verify pinned inputs.
download "$GRAALVM_URL" "$WORKDIR/graalvm.tar.gz" "$GRAALVM_SHA256"
download "$SIGNAL_CLI_URL" "$WORKDIR/signal-cli-src.tar.gz" "$SIGNAL_CLI_SHA256"

# 2. Extract.
if [ ! -d "$WORKDIR/$GRAALVM_DIR_NAME" ]; then
  tar -xzf "$WORKDIR/graalvm.tar.gz" -C "$WORKDIR"
fi
if [ ! -d "$WORKDIR/signal-cli-${SIGNAL_CLI_VERSION}" ]; then
  tar -xzf "$WORKDIR/signal-cli-src.tar.gz" -C "$WORKDIR"
fi
GRAALVM_HOME="$WORKDIR/$GRAALVM_DIR_NAME/Contents/Home"
[ -x "$GRAALVM_HOME/bin/native-image" ] || {
  echo "FAIL: native-image not found under $GRAALVM_HOME" >&2
  exit 1
}
SRC="$WORKDIR/signal-cli-${SIGNAL_CLI_VERSION}"

# 3. Trim non-target-arch native resource globs (340 MB -> 112.8 MiB in spike).
python3 "$ROOT/packaging/scripts/trim-native-reachability-metadata.py" "$SRC"

# 4. nativeCompile with the pinned GraalVM. GRADLE_USER_HOME stays inside the
#    workdir so a run never touches ~/.gradle.
cd "$SRC"
GRAALVM_HOME="$GRAALVM_HOME" \
JAVA_HOME="$GRAALVM_HOME" \
PATH="$GRAALVM_HOME/bin:$PATH" \
GRADLE_USER_HOME="$WORKDIR/gradle-home" \
  ./gradlew --no-daemon --console=plain nativeCompile

BIN="$SRC/build/native/nativeCompile/signal-cli"
[ -x "$BIN" ] || { echo "FAIL: expected binary missing: $BIN" >&2; exit 1; }

# 5. Stage the artifact and its provenance.
cp -f "$BIN" "$OUT/signal-cli"
(
  cd "$OUT"
  shasum -a 256 signal-cli > SHA256SUMS.txt
)
cat >"$OUT/build-info.env" <<EOF
SIGNAL_CLI_VERSION=$SIGNAL_CLI_VERSION
SIGNAL_CLI_TAG=$SIGNAL_CLI_TAG
GRAALVM_VERSION=$GRAALVM_VERSION
GRAALVM_SHA256=$GRAALVM_SHA256
SIGNAL_CLI_SRC_SHA256=$SIGNAL_CLI_SHA256
EOF

echo "== built: $OUT/signal-cli ($(du -h "$OUT/signal-cli" | awk '{print $1}'))"
echo "== sha256: $(awk '{print $1}' "$OUT/SHA256SUMS.txt")"
echo "BUILD OK"
