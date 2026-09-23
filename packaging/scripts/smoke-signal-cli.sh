#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Smoke-check a signal-cli binary before it is accepted into a runtime bundle.
# Catches the upgrade killers early: wrong version, broken JRE/classpath,
# missing daemon JSON-RPC socket support. Usage:
#   packaging/scripts/smoke-signal-cli.sh --bin /abs/path/signal-cli --expect-version 0.14.8
set -euo pipefail

BIN=""
EXPECT_VERSION=""
DATA_DIR=""
DAEMON_PID=""

while [ $# -gt 0 ]; do
  case "$1" in
    --bin) BIN="$2"; shift 2 ;;
    --expect-version) EXPECT_VERSION="$2"; shift 2 ;;
    --data-dir) DATA_DIR="$2"; shift 2 ;;
    *) echo "FAIL: unknown argument $1" >&2; exit 2 ;;
  esac
done

fail() { echo "FAIL: $1" >&2; exit 1; }
pass() { echo "PASS: $1"; }

[ -n "$BIN" ] || fail "--bin is required"
[ -n "$EXPECT_VERSION" ] || fail "--expect-version is required"

# 1. Binary shape: absolute, regular file, executable, not a symlink.
case "$BIN" in /*) ;; *) fail "--bin must be an absolute path" ;; esac
[ -e "$BIN" ] || fail "binary does not exist: $BIN"
[ ! -L "$BIN" ] || fail "binary must not be a symlink: $BIN"
[ -f "$BIN" ] || fail "binary is not a regular file: $BIN"
[ -x "$BIN" ] || fail "binary is not executable: $BIN"
pass "binary shape"

# 2. Version pin: this also proves the bundled JRE can run the classpath.
ACTUAL_VERSION="$("$BIN" --version 2>/dev/null | awk '/^signal-cli / {print $2; exit}')"
[ -n "$ACTUAL_VERSION" ] || fail "could not parse 'signal-cli --version' output"
[ "$ACTUAL_VERSION" = "$EXPECT_VERSION" ] \
  || fail "version mismatch: expected $EXPECT_VERSION, got $ACTUAL_VERSION"
pass "version $ACTUAL_VERSION (JRE/classpath healthy)"

# 3. Daemon mode must expose a JSON-RPC UNIX socket.
"$BIN" daemon --help 2>/dev/null | grep -q -- "--socket" \
  || fail "daemon mode does not advertise --socket"
pass "daemon --socket advertised"

# 4. Live JSON-RPC probe: start a daemon on a private socket and require a
#    well-formed JSON-RPC 2.0 response to listAccounts (result or error).
DATA_DIR="${DATA_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/signal-cli-smoke.XXXXXX")}"
SOCK="$DATA_DIR/smoke.sock"
cleanup() {
  [ -n "$DAEMON_PID" ] && kill "$DAEMON_PID" 2>/dev/null || true
  [ -n "$DAEMON_PID" ] && wait "$DAEMON_PID" 2>/dev/null || true
  rm -rf "$DATA_DIR"
}
trap cleanup EXIT

"$BIN" -c "$DATA_DIR" daemon --socket "$SOCK" --no-receive-stdout \
  >"$DATA_DIR/daemon.log" 2>&1 &
DAEMON_PID=$!

READY=0
for _ in $(seq 1 50); do
  [ -S "$SOCK" ] && { READY=1; break; }
  kill -0 "$DAEMON_PID" 2>/dev/null || fail "daemon exited early; see $DATA_DIR/daemon.log"
  sleep 0.2
done
[ "$READY" = 1 ] || fail "daemon socket never appeared; see daemon log"

PROBE_RESPONSE="$(SOCK="$SOCK" python3 - <<'PY'
import json, os, socket, time
# The socket file can exist before the listener accepts connections; retry
# briefly instead of failing the whole smoke on that race.
deadline = time.monotonic() + 10
last_err = None
while time.monotonic() < deadline:
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.settimeout(10)
    try:
        s.connect(os.environ["SOCK"])
        break
    except (ConnectionRefusedError, FileNotFoundError) as exc:
        last_err = exc
        s.close()
        time.sleep(0.2)
else:
    raise SystemExit(f"daemon socket never accepted connections: {last_err}")
s.sendall(b'{"jsonrpc":"2.0","method":"listAccounts","id":1,"params":{}}\n')
data = b""
while b"\n" not in data:
    chunk = s.recv(65536)
    if not chunk:
        break
    data += chunk
s.close()
line = data.split(b"\n", 1)[0].decode("utf-8", "replace")
msg = json.loads(line)
assert msg.get("jsonrpc") == "2.0" and msg.get("id") == 1, line
assert "result" in msg or "error" in msg, line
print("ok")
PY
)" || fail "JSON-RPC probe did not return a well-formed response"
[ "$PROBE_RESPONSE" = "ok" ] || fail "JSON-RPC probe failed: $PROBE_RESPONSE"
pass "live JSON-RPC listAccounts probe"

echo "SMOKE OK: $BIN ($ACTUAL_VERSION)"
