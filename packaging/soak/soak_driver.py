#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only

"""24-hour soak driver for the KT Signal connector (optimization-plan §6.4
M3.1, rebuilt from the run-2 methodology in docs/handover.md §3.2).

One driver run:

- creates an isolated run directory (endpoint, state dir, signal data dirs
  for `default` plus `g1..gN-1` proxy groups) and a two-line bootstrap
  payload (handshake secret + fixed store key),
- spawns one connector (release binary) whose engines are the fake
  signal-cli (real signal-cli works too: pass --signal-cli/--java-home),
- opens exactly ONE authenticated host connection and keeps it for the whole
  run (closing it ends the connector process, by contract),
- every --status-interval seconds records one `runtime.status` aggregate
  line into driver.log; every --rss-interval seconds records one row per
  process into rss.csv (engine rows from runtime.status, connector row from
  `ps`) — the exact artifact shapes the run-2 judge read,
- optionally drives a fake-load ladder: writes the `.fixture-load` marker
  into every group data directory per --load-steps (messages per minute per
  account) x --load-accounts (simulated receiving accounts per engine),
  rotating steps evenly across the run and logging each step to load.log,
- counts host events (message.changed and friends) as receive-fanout
  evidence, writes meta.json, and ends with the `soak duration complete`
  line.

Verdicts are NOT produced here: run packaging/soak/judge.py on the run
directory afterwards. See packaging/soak/README.md.

Usage (see launch.sh for the caffeinate-wrapped form):
    python3 packaging/soak/soak_driver.py --duration-seconds 86400 \
        [--groups 8] [--load-steps 1,5,20] [--load-accounts 2] \
        [--run-dir packaging/soak/runs/<id>]
"""

import argparse
import asyncio
import hashlib
import hmac
import json
import os
import secrets
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

API_VERSION = "1.0"
PROOF_CONTEXT = b"kt-signal-connector-v1\0"
GROUP_PREFIX = "g"

driver = None  # set in main; typed via the Driver class


def log_line(message):
    stamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    if hasattr(driver, "log_file"):
        driver.log_file.write(f"[{stamp}] {message}\n")
        driver.log_file.flush()
    print(f"[{stamp}] {message}", flush=True)


class Driver:
    def __init__(self, args):
        self.args = args
        self.run_dir = Path(args.run_dir).resolve()
        self.endpoint = self.run_dir / "connector.sock"
        self.state_dir = self.run_dir / "state"
        self.data_root = self.run_dir / "signal-data"
        self.group_ids = ["default"] + [
            f"{GROUP_PREFIX}{index}" for index in range(1, args.groups)
        ]
        self.log_path = self.run_dir / "driver.log"
        self.connector = None
        self.reader = None
        self.writer = None
        self.request_seq = 0
        self.event_counts = {}
        self.load_step_marks = []

    # --- lifecycle ------------------------------------------------------

    def prepare(self):
        self.run_dir.mkdir(parents=True, exist_ok=True)
        os.chmod(self.run_dir, 0o700)
        self.state_dir.mkdir(exist_ok=True)
        self.data_root.mkdir(parents=True, exist_ok=True)
        for group_id in self.group_ids[1:]:
            (self.data_root / "proxy-groups" / group_id).mkdir(parents=True, exist_ok=True)
        self.log_file = self.log_path.open("w")
        self.rss_file = (self.run_dir / "rss.csv").open("w", newline="")
        self.rss_file.write("ts,role,group_id,pid,rss_kb,state,pressure\n")
        self.rss_file.flush()

        # Bootstrap payload: line 1 handshake secret (fresh per run), line 2
        # the fixed store key so a re-run against the same run directory can
        # still open the earlier store. Exactly 64 hex + \n + 64 hex, no
        # trailing byte — the loader rejects anything else.
        self.secret = secrets.token_bytes(32)
        payload = self.run_dir / "bootstrap.secret"
        payload.write_text(f"{self.secret.hex()}\n{self.args.store_key}")
        os.chmod(payload, 0o600)

    def spawn_connector(self):
        command = [
            str(self.args.connector),
            "serve",
            "--endpoint",
            str(self.endpoint),
            "--bootstrap-secret-file",
            str(self.run_dir / "bootstrap.secret"),
            "--signal-cli",
            str(self.args.signal_cli),
            "--signal-data-dir",
            str(self.data_root),
            "--state-dir",
            str(self.state_dir),
        ]
        for group_id in self.group_ids[1:]:
            # Allocation placeholders: the fake engine never dials them. A
            # real-engine run must pass proxies that actually route.
            command += ["--proxy-group", f"{group_id}=127.0.0.1:19080"]
        if self.args.java_home:
            command += ["--java-home", str(self.args.java_home)]
        if self.args.native:
            command += ["--signal-cli-native"]
        connector_log = (self.run_dir / "connector.log").open("wb")
        self.connector = subprocess.Popen(
            command,
            stdout=subprocess.DEVNULL,
            stderr=connector_log,
        )
        log_line(f"connector spawned: pid={self.connector.pid}")
        return connector_log

    # --- host protocol --------------------------------------------------

    async def connect(self):
        deadline = time.monotonic() + 30
        while not self.endpoint.exists():
            if self.connector.poll() is not None:
                raise RuntimeError("connector exited before opening the endpoint")
            if time.monotonic() > deadline:
                raise RuntimeError("endpoint never appeared")
            await asyncio.sleep(0.1)
        self.reader, self.writer = await asyncio.open_unix_connection(str(self.endpoint))

        challenge = json.loads(await self.reader.readline())
        server_nonce = challenge["data"]["serverNonce"]
        client_nonce = secrets.token_hex(32)
        message = (
            PROOF_CONTEXT
            + server_nonce.encode()
            + b"\0"
            + client_nonce.encode()
            + b"\0"
            + API_VERSION.encode()
        )
        proof = hmac.new(self.secret, message, hashlib.sha256).hexdigest()
        await self._send(
            {
                "apiVersion": API_VERSION,
                "requestId": "handshake",
                "method": "handshake",
                "params": {"clientNonce": client_nonce, "proof": proof},
            }
        )
        response = await self._next_line()
        if response.get("requestId") != "handshake" or "result" not in response:
            raise RuntimeError(f"handshake failed: {response}")
        log_line("handshake ok")

    async def _send(self, frame):
        self.writer.write(json.dumps(frame).encode() + b"\n")
        await self.writer.drain()

    async def _next_line(self):
        line = await self.reader.readline()
        if not line:
            raise RuntimeError("connector closed the session")
        value = json.loads(line)
        event = value.get("event")
        if event:
            self.event_counts[event] = self.event_counts.get(event, 0) + 1
        return value

    async def call(self, method, params=None):
        self.request_seq += 1
        request_id = f"driver-{self.request_seq}"
        await self._send(
            {
                "apiVersion": API_VERSION,
                "requestId": request_id,
                "method": method,
                "params": params or {},
            }
        )
        while True:
            response = await self._next_line()
            if response.get("requestId") == request_id:
                return response

    # --- sampling -------------------------------------------------------

    async def wait_all_running(self):
        deadline = time.monotonic() + 120
        while True:
            status = await self.call("runtime.status")
            groups = status["result"].get("proxyGroups", [])
            running = [g for g in groups if g.get("state") == "running"]
            if len(running) == len(self.group_ids):
                named = ", ".join(
                    f"{g['groupId']}=pid{g.get('pid')}" for g in groups
                )
                log_line(f"all {len(groups)} groups running: {named}")
                return
            if time.monotonic() > deadline:
                raise RuntimeError(f"engines never reached running: {status}")
            await asyncio.sleep(2)

    def connector_rss_kb(self):
        try:
            out = subprocess.check_output(
                ["ps", "-o", "rss=", "-p", str(self.connector.pid)], text=True
            ).strip()
            return int(out)
        except (subprocess.CalledProcessError, ValueError):
            return None

    def record_rss_round(self, status_result):
        stamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        connector_rss = self.connector_rss_kb()
        if connector_rss is not None:
            self.rss_file.write(
                f"{stamp},connector,-,{self.connector.pid},{connector_rss},-,-\n"
            )
        for group in status_result.get("proxyGroups", []):
            pid = group.get("pid")
            if pid is None:
                continue
            rss = group.get("rssBytes")
            rss_kb = "" if rss is None else str(rss // 1024)
            self.rss_file.write(
                f"{stamp},engine,{group['groupId']},{pid},{rss_kb},"
                f"{group.get('state', '')},{group.get('resourcePressure', '')}\n"
            )
        self.rss_file.flush()

    def record_status(self, status_result):
        aggregate = {
            key: status_result.get(key)
            for key in ("state", "rssBytes", "resourcePressure")
        }
        log_line(f"status: agg={aggregate}")

    # --- load ladder ----------------------------------------------------

    def write_load_markers(self, rate):
        spec = None if rate is None else {
            "ratePerMinute": rate,
            "accounts": self.args.load_accounts,
        }
        for directory in [self.data_root] + [
            self.data_root / "proxy-groups" / group_id
            for group_id in self.group_ids[1:]
        ]:
            marker = directory / ".fixture-load"
            if spec is None:
                marker.unlink(missing_ok=True)
            else:
                temporary = directory / ".fixture-load.tmp"
                temporary.write_text(json.dumps(spec))
                temporary.replace(marker)
        now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        rate_text = "off" if rate is None else (
            f"{rate} msg/min/account x {self.args.load_accounts} accounts"
        )
        log_line(f"load: {rate_text}")
        with (self.run_dir / "load.log").open("a") as handle:
            handle.write(f"[{now}] step {rate_text}\n")

    async def run_load_ladder(self, started_at):
        steps = self.args.load_steps
        if not steps:
            return
        duration = self.args.duration_seconds
        step_seconds = max(1, duration // len(steps))
        while time.monotonic() - started_at < duration:
            for rate in steps:
                self.write_load_markers(rate)
                self.load_step_marks.append(rate)
                remaining = min(
                    step_seconds, duration - (time.monotonic() - started_at)
                )
                if remaining > 0:
                    await asyncio.sleep(remaining)
                if time.monotonic() - started_at >= duration:
                    return

    # --- main -----------------------------------------------------------

    async def run(self):
        self.prepare()
        self.spawn_connector()
        await self.connect()
        await self.call("runtime.start")
        await self.wait_all_running()

        started_at = time.monotonic()
        # First round immediately (the run-2 shape: a status line and an RSS
        # round right at launch), then once per interval.
        status = await self.call("runtime.status")
        self.record_status(status["result"])
        self.record_rss_round(status["result"])
        next_status = next_rss = time.monotonic()
        ladder = asyncio.create_task(self.run_load_ladder(started_at))
        try:
            while time.monotonic() - started_at < self.args.duration_seconds:
                now = time.monotonic()
                due_status = now >= next_status + self.args.status_interval
                due_rss = now >= next_rss + self.args.rss_interval
                if due_status or due_rss:
                    status = await self.call("runtime.status")
                    if due_status:
                        self.record_status(status["result"])
                        next_status = now
                    if due_rss:
                        self.record_rss_round(status["result"])
                        next_rss = now
                    continue
                # Drain events while waiting for the next sampling deadline;
                # a quiet socket here would let the OS buffer events forever.
                # Cancelling readline mid-line is safe: the partial line stays
                # in the stream buffer and the retry continues it.
                wait = max(
                    0.1,
                    min(
                        next_status + self.args.status_interval - now,
                        next_rss + self.args.rss_interval - now,
                        5.0,
                    ),
                )
                try:
                    await asyncio.wait_for(self._next_line(), wait)
                except asyncio.TimeoutError:
                    pass
            # Closing round: the judged window must span the full duration,
            # not duration minus one sampling interval.
            status = await self.call("runtime.status")
            self.record_status(status["result"])
            self.record_rss_round(status["result"])
        finally:
            ladder.cancel()
            self.write_load_markers(None)
            self.finish()

    def finish(self):
        log_line(f"events: {json.dumps(self.event_counts, sort_keys=True)}")
        log_line("soak duration complete")
        meta = {
            "finished_at": datetime.now().isoformat(),
            "event_counts": self.event_counts,
            "load_steps": self.load_step_marks,
            "connector_pid": self.connector.pid if self.connector else None,
        }
        meta_path = self.run_dir / "meta.json"
        existing = (
            json.loads(meta_path.read_text()) if meta_path.exists() else {}
        )
        existing.update(meta)
        meta_path.write_text(json.dumps(existing, indent=2) + "\n")
        try:
            self.writer.close()
        except (AttributeError, RuntimeError):
            pass
        try:
            self.connector.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.connector.kill()
            self.connector.wait()


def parse_args(argv):
    root = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--duration-seconds", type=int, default=24 * 3600)
    parser.add_argument("--status-interval", type=int, default=300)
    parser.add_argument("--rss-interval", type=int, default=60)
    parser.add_argument("--groups", type=int, default=8)
    parser.add_argument("--load-steps", default="", help="comma-separated msg/min per account, e.g. 1,5,20")
    parser.add_argument("--load-accounts", type=int, default=2)
    parser.add_argument("--connector", default=str(root / "target/release/kt-signal-connector"))
    parser.add_argument("--signal-cli", default=str(root / "tests/fixtures/fake-signal-cli.py"))
    parser.add_argument("--java-home", default=None)
    parser.add_argument("--native", action="store_true")
    parser.add_argument(
        "--store-key",
        default="5a" * 32,
        help="64-hex store key (fixed default keeps re-runs on one run-dir compatible)",
    )
    args = parser.parse_args(argv)
    args.load_steps = [
        int(step) for step in args.load_steps.split(",") if step.strip()
    ]
    if not 1 <= args.groups <= 8:
        parser.error("--groups must be 1..8 (the connector group ceiling)")
    if not 1 <= args.load_accounts <= 8:
        parser.error("--load-accounts must be 1..8 (the per-engine account ceiling)")
    if any(rate <= 0 for rate in args.load_steps):
        parser.error("load steps must be positive msg/min values")
    if len(args.store_key) != 64 or any(
        character not in "0123456789abcdef" for character in args.store_key
    ):
        parser.error("--store-key must be exactly 64 lowercase hex characters")
    return args


def main(argv=None):
    global driver
    args = parse_args(argv)
    driver = Driver(args)
    meta = {
        "started_at": datetime.now().isoformat(),
        "duration_seconds": args.duration_seconds,
        "status_interval": args.status_interval,
        "rss_interval": args.rss_interval,
        "groups": args.groups,
        "load_steps": args.load_steps,
        "load_accounts": args.load_accounts,
        "connector": str(args.connector),
        "connector_sha256": hashlib.sha256(
            Path(args.connector).read_bytes()
        ).hexdigest()
        if Path(args.connector).exists()
        else None,
        "signal_cli": str(args.signal_cli),
    }
    (Path(args.run_dir) / "meta.json").parent.mkdir(parents=True, exist_ok=True)
    (Path(args.run_dir) / "meta.json").write_text(json.dumps(meta, indent=2) + "\n")
    try:
        asyncio.run(driver.run())
    except KeyboardInterrupt:
        log_line("interrupted by signal")
        driver.write_load_markers(None)
        return 130
    except RuntimeError as error:
        log_line(f"driver aborting: {error}")
        driver.write_load_markers(None)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
