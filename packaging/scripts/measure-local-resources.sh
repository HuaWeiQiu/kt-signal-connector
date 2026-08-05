#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
REPORT_DIR="${1:-"$ROOT/packaging/out/resource-reports"}"
BIN="$ROOT/target/release/kt-signal-connector"
FIXTURE="$ROOT/tests/fixtures/fake-signal-cli.py"
mkdir -p "$REPORT_DIR"

cargo build --release --manifest-path "$ROOT/Cargo.toml"

# Sample the measurement path itself with a short-lived sleeper. Connector RSS is captured in
# the active-text fixture report below.
"$BIN" package measure-idle \
  --executable /bin/sleep \
  --arg 2 \
  --settle-ms 300 \
  --output "$REPORT_DIR/idle-sample-path.json"

export KT_SIGNAL_ROOT="$ROOT"
export KT_SIGNAL_BIN="$BIN"
export KT_SIGNAL_FIXTURE="$FIXTURE"
export KT_SIGNAL_REPORT_DIR="$REPORT_DIR"

python3 <<'PY'
import asyncio, hashlib, hmac, json, os, secrets, subprocess, tempfile, time
from pathlib import Path

root = Path(os.environ["KT_SIGNAL_ROOT"])
bin_path = Path(os.environ["KT_SIGNAL_BIN"])
fixture = Path(os.environ["KT_SIGNAL_FIXTURE"])
report_dir = Path(os.environ["KT_SIGNAL_REPORT_DIR"])
tmpdir = Path(tempfile.mkdtemp(prefix="kt-signal-resource-"))
os.chmod(tmpdir, 0o700)
endpoint = tmpdir / "connector.sock"
secret = secrets.token_bytes(32)
secret_file = tmpdir / "bootstrap.secret"
secret_file.write_text(secret.hex())
os.chmod(secret_file, 0o600)
state = tmpdir / "state"
signal_data = tmpdir / "signal-data"
proc = subprocess.Popen(
    [
        str(bin_path),
        "serve",
        "--endpoint",
        str(endpoint),
        "--bootstrap-secret-file",
        str(secret_file),
        "--signal-cli",
        str(fixture),
        "--signal-data-dir",
        str(signal_data),
        "--state-dir",
        str(state),
    ],
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
)
time.sleep(0.2)

async def main():
    for _ in range(50):
        if endpoint.exists():
            break
        await asyncio.sleep(0.05)
    reader, writer = await asyncio.open_unix_connection(str(endpoint))
    challenge = json.loads(await reader.readline())
    server_nonce = challenge["data"]["serverNonce"]
    client_nonce = secrets.token_hex(32)
    msg = (
        b"kt-signal-connector-v1\0"
        + server_nonce.encode()
        + b"\0"
        + client_nonce.encode()
        + b"\0"
        + b"1.0"
    )
    proof = hmac.new(secret, msg, hashlib.sha256).hexdigest()
    writer.write(
        (
            json.dumps(
                {
                    "apiVersion": "1.0",
                    "requestId": "hs",
                    "method": "handshake",
                    "params": {"clientNonce": client_nonce, "proof": proof},
                }
            )
            + "\n"
        ).encode()
    )
    await writer.drain()
    await reader.readline()

    async def call(rid, method, params=None):
        writer.write(
            (
                json.dumps(
                    {
                        "apiVersion": "1.0",
                        "requestId": rid,
                        "method": method,
                        "params": params or {},
                    }
                )
                + "\n"
            ).encode()
        )
        await writer.drain()
        while True:
            line = await reader.readline()
            obj = json.loads(line)
            if obj.get("requestId") == rid:
                return obj

    await call("start", "runtime.start")
    link = await call("link", "link.start", {"deviceName": "resource"})
    await call(
        "finish",
        "link.finish",
        {"linkSessionId": link["result"]["linkSessionId"]},
    )
    await asyncio.sleep(0.1)
    try:
        out = subprocess.check_output(
            ["ps", "-o", "rss=", "-p", str(proc.pid)], text=True
        ).strip()
        rss = int(out) * 1024
    except Exception:
        rss = 0
    report = {
        "scenario": "active-text-fixture",
        "platform": "macos-arm64",
        "durationMs": 0,
        "samples": [{"label": "connector", "pid": proc.pid, "rssBytes": rss}],
        "notes": [
            "fixture-backed active text path",
            "not multi-account production load",
            "not a 24-hour stability gate",
        ],
    }
    report_dir.mkdir(parents=True, exist_ok=True)
    (report_dir / "active-text-fixture.json").write_text(
        json.dumps(report, indent=2) + "\n"
    )
    writer.close()
    try:
        await writer.wait_closed()
    except Exception:
        pass

asyncio.run(main())
proc.terminate()
try:
    proc.wait(timeout=2)
except Exception:
    proc.kill()
print(f"wrote reports under {report_dir}")
PY
