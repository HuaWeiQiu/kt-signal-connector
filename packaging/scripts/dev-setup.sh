#!/usr/bin/env bash
# dev-setup.sh — one-command development runtime for the KT Signal integration.
#
#   1. cargo build --release (connector binary)
#   2. downloads the pinned signal-cli release (SHA-256 verified) into a
#      user-level dev dir — never touches production runtime paths
#   3. locates a JRE >= 21 (required by signal-cli 0.14.x)
#   4. runs packaging/scripts/smoke-signal-cli.sh against the result
#   5. writes env.sh with the KT_SIGNAL_* vars the desktop client's
#      development resolver reads
#
# This script never auto-updates signal-cli: version and hash below are the
# pin. To upgrade, follow docs/signal-cli-upgrade.md.
set -euo pipefail

SIGNAL_CLI_VERSION="0.14.7"
# SHA-256 of signal-cli-0.14.7.tar.gz from the official AsamK/signal-cli
# release. Upstream publishes no checksum asset; this pin was taken from the
# tarball fetched from the release URL below.
SIGNAL_CLI_SHA256="0e1eefdf4a2109edf7c899c9d1667167c54ac12c3ec824f27db7c1dac4fa7506"
SIGNAL_CLI_URL="https://github.com/AsamK/signal-cli/releases/download/v${SIGNAL_CLI_VERSION}/signal-cli-${SIGNAL_CLI_VERSION}.tar.gz"
MIN_JAVA_MAJOR=21

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DEV_ROOT="${KT_SIGNAL_DEV_ROOT:-$HOME/.kt-desktop-signal-dev}"

log() { printf '[dev-setup] %s\n' "$*" >&2; }
die() { printf '[dev-setup] ERROR: %s\n' "$*" >&2; exit 1; }

java_major() {
  "$1" -version 2>&1 | awk -F'"' '/version/ {
    split($2, a, "."); print (a[1] == "1" ? a[2] : a[1]); exit
  }'
}

# Echo a JAVA_HOME whose root contains bin/java and the release file (the
# desktop resolver requires <javaHome>/release), or nothing.
normalize_java_home() {
  local dir="$1"
  if [[ -f "$dir/release" && -x "$dir/bin/java" ]]; then
    echo "$dir"
  elif [[ -f "$dir/libexec/openjdk.jdk/Contents/Home/release" ]]; then
    echo "$dir/libexec/openjdk.jdk/Contents/Home"
  fi
}

find_java_home() {
  local candidate major best="" best_major=0

  if [[ -n "${JAVA_HOME:-}" ]]; then
    candidate="$(normalize_java_home "$JAVA_HOME" || true)"
    if [[ -n "$candidate" ]] && [[ "$(java_major "$candidate/bin/java")" -ge "$MIN_JAVA_MAJOR" ]]; then
      echo "$candidate"; return 0
    fi
  fi

  if [[ "$(uname)" == "Darwin" && -x /usr/libexec/java_home ]]; then
    candidate="$(/usr/libexec/java_home -v "${MIN_JAVA_MAJOR}+" 2>/dev/null || true)"
    if [[ -n "$candidate" && -f "$candidate/release" ]]; then
      echo "$candidate"; return 0
    fi
  fi

  # Homebrew openjdk is keg-only and invisible to /usr/libexec/java_home.
  local dir
  for dir in /opt/homebrew/opt/openjdk* /usr/local/opt/openjdk*; do
    candidate="$(normalize_java_home "$dir" || true)"
    [[ -n "$candidate" ]] || continue
    major="$(java_major "$candidate/bin/java")"
    if [[ -n "$major" && "$major" -ge "$MIN_JAVA_MAJOR" && "$major" -gt "$best_major" ]]; then
      best="$candidate"; best_major="$major"
    fi
  done
  if [[ -n "$best" ]]; then
    echo "$best"; return 0
  fi

  if command -v java >/dev/null 2>&1; then
    local jbin
    jbin="$(readlink -f "$(command -v java)" 2>/dev/null || command -v java)"
    candidate="$(normalize_java_home "$(cd "$(dirname "$jbin")/.." && pwd)" || true)"
    if [[ -n "$candidate" ]] && [[ "$(java_major "$candidate/bin/java")" -ge "$MIN_JAVA_MAJOR" ]]; then
      echo "$candidate"; return 0
    fi
  fi

  return 1
}

ensure_signal_cli() {
  local cli_home="$DEV_ROOT/signal-cli-$SIGNAL_CLI_VERSION"
  local cli_bin="$cli_home/bin/signal-cli"
  if [[ -x "$cli_bin" ]]; then
    log "signal-cli $SIGNAL_CLI_VERSION already present, skipping download"
    echo "$cli_bin"; return 0
  fi
  mkdir -p "$DEV_ROOT"
  local tarball="$DEV_ROOT/signal-cli-$SIGNAL_CLI_VERSION.tar.gz"
  if [[ ! -f "$tarball" ]]; then
    log "downloading $SIGNAL_CLI_URL"
    curl -fL --retry 3 -o "$tarball.part" "$SIGNAL_CLI_URL"
    mv "$tarball.part" "$tarball"
  fi
  log "verifying SHA-256"
  if command -v shasum >/dev/null 2>&1; then
    echo "$SIGNAL_CLI_SHA256  $tarball" | shasum -a 256 -c - >/dev/null
  else
    echo "$SIGNAL_CLI_SHA256  $tarball" | sha256sum -c - >/dev/null
  fi
  log "extracting"
  tar -xzf "$tarball" -C "$DEV_ROOT"
  [[ -x "$cli_bin" ]] || die "signal-cli binary missing after extraction: $cli_bin"
  echo "$cli_bin"
}

main() {
  log "building connector (cargo build --release)"
  cargo build --release --manifest-path "$REPO_ROOT/Cargo.toml"
  local connector_bin="$REPO_ROOT/target/release/kt-signal-connector"
  [[ -x "$connector_bin" ]] || die "connector binary missing after build: $connector_bin"

  local cli_bin
  cli_bin="$(ensure_signal_cli)"

  local java_home
  if ! java_home="$(find_java_home)"; then
    die "no JRE >= $MIN_JAVA_MAJOR found. Install one, e.g.:
  macOS:  brew install openjdk@21
  Debian: sudo apt install openjdk-21-jre-headless
then re-run this script (or set JAVA_HOME first)."
  fi

  log "running smoke check"
  JAVA_HOME="$java_home" \
    "$REPO_ROOT/packaging/scripts/smoke-signal-cli.sh" \
    --bin "$cli_bin" --expect-version "$SIGNAL_CLI_VERSION"

  local env_file="$DEV_ROOT/env.sh"
  cat > "$env_file" <<EOF
# Generated by kt-signal-connector packaging/scripts/dev-setup.sh
# Development-mode runtime for the KT desktop Signal integration.
export KT_SIGNAL_CONNECTOR_BIN="$connector_bin"
export KT_SIGNAL_CLI_BIN="$cli_bin"
export KT_SIGNAL_JAVA_HOME="$java_home"
EOF

  cat <<EOF

[dev-setup] ready.
  connector : $connector_bin
  signal-cli: $cli_bin ($SIGNAL_CLI_VERSION)
  JAVA_HOME : $java_home

Next:
  source "$env_file"
  # then start the KT desktop client from the same shell — its development
  # resolver picks up the three KT_SIGNAL_* variables.
EOF
}

main "$@"
