#!/usr/bin/env bash
# §4.102 (contract 1.121) real-process regression: the launcher's
# --media-ingest decision is the whole decision. Armed -> handshake
# advertises attachment-download and the media face is live; unarmed ->
# no tag and messages.attachments.open answers CAPABILITY_UNAVAILABLE.
set -euo pipefail
cd "$(dirname "$0")/.."

ROOT=$(mktemp -d /tmp/kt-media-flip-regression.XXXXXX)
CONNECTOR_PID=""
cleanup() {
  [ -n "$CONNECTOR_PID" ] && kill "$CONNECTOR_PID" 2>/dev/null || true
  rm -rf "$ROOT"
}
trap cleanup EXIT

run_probe() {
  local armed="$1" endpoint="$ROOT/$2.sock"
  local secret_file="$ROOT/bootstrap-$2.secret"
  mkdir -p "$ROOT/signal-data-$2" "$ROOT/state-$2"
  chmod 700 "$ROOT/signal-data-$2" "$ROOT/state-$2"
  SECRET=$(openssl rand -hex 32)
  STOREKEY=$(openssl rand -hex 32)
  printf '%s\n%s' "$SECRET" "$STOREKEY" > "$secret_file"
  chmod 600 "$secret_file"
  local args=(serve
    --endpoint "$endpoint"
    --bootstrap-secret-file "$secret_file"
    --signal-cli tests/fixtures/fake-signal-cli.py
    --java-home "$ROOT/jre"
    --signal-data-dir "$ROOT/signal-data-$2"
    --state-dir "$ROOT/state-$2")
  [ "$armed" = "armed" ] && args+=(--media-ingest)
  target/release/kt-signal-connector "${args[@]}" &
  CONNECTOR_PID=$!
  for _ in $(seq 1 100); do [ -S "$endpoint" ] && break; sleep 0.1; done
  sleep 0.3
  SECRET="$SECRET" ENDPOINT="$endpoint" ARMED="$armed" python3 <<'PY'
import hashlib, hmac, json, os, socket, sys
secret = bytes.fromhex(os.environ["SECRET"])
endpoint = os.environ["ENDPOINT"]
armed = os.environ["ARMED"]
s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
s.settimeout(5)
s.connect(endpoint)
rf = s.makefile("r", encoding="utf-8")

challenge = json.loads(rf.readline())
assert challenge.get("event") == "runtime.challenge", challenge
server_nonce = challenge["data"]["serverNonce"]
client_nonce = os.urandom(32).hex()
context = b"kt-signal-connector-v1\0"
proof = hmac.new(secret, context + server_nonce.encode() + b"\0"
                 + client_nonce.encode() + b"\0" + b"1.0", hashlib.sha256).hexdigest()
s.sendall((json.dumps({"apiVersion": "1.0", "requestId": "hs",
                       "method": "handshake",
                       "params": {"clientNonce": client_nonce, "proof": proof}}) + "\n").encode())
while True:
    frame = json.loads(rf.readline())
    if frame.get("requestId") == "hs":
        break
caps = frame["result"]["capabilities"]

def request(rid, method, params):
    s.sendall((json.dumps({"apiVersion": "1.0", "requestId": rid,
                           "method": method, "params": params}) + "\n").encode())
    while True:
        response = json.loads(rf.readline())
        if response.get("requestId") == rid:
            return response

open_res = request("om", "messages.attachments.readChunk",
                   {"mediaHandle": "regression-handle", "offset": 0})
unavailable = open_res.get("error", {}).get("code") == "CAPABILITY_UNAVAILABLE"
tag = "attachment-download" in caps
if armed == "armed":
    ok = tag and not unavailable
    print(f"armed:   tag={tag} media-live={not unavailable} -> {'PASS' if ok else 'FAIL'}")
else:
    ok = (not tag) and unavailable
    print(f"unarmed: tag={tag} capability-unavailable={unavailable} -> {'PASS' if ok else 'FAIL'}")
sys.exit(0 if ok else 1)
PY
  kill $CONNECTOR_PID 2>/dev/null || true
  wait $CONNECTOR_PID 2>/dev/null || true
  CONNECTOR_PID=""
}

run_probe armed connector-armed
run_probe unarmed connector-unarmed
echo "§4.102 flip regression: PASS (armed advertises and serves, unarmed stays honest)"
