#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only

import json
import os
from pathlib import Path
import sys
import threading
import time
import zlib


expected_java_opts = os.environ.get("KT_FAKE_EXPECT_JAVA_OPTS")
if expected_java_opts is not None:
    # Phase 4: group engines carry their SOCKS proxy as extra `-D` properties
    # appended to the documented heap budget, so the expectation is a prefix
    # match; the memory flags themselves must still lead the value.
    if not os.environ.get("JAVA_OPTS", "").startswith(expected_java_opts):
        sys.exit(91)
    if any(os.environ.get(name) for name in ("JAVA_TOOL_OPTIONS", "_JAVA_OPTIONS", "JDK_JAVA_OPTIONS")):
        sys.exit(92)

expected_java_home = os.environ.get("KT_FAKE_EXPECT_JAVA_HOME")
if expected_java_home is not None and os.environ.get("JAVA_HOME") != expected_java_home:
    sys.exit(93)

# Native-mode guard: a GraalVM binary has no JVM, so the engine must not set
# JAVA_OPTS or forward JAVA_HOME to the child at all.
if os.environ.get("KT_FAKE_EXPECT_NO_JAVA") is not None:
    if os.environ.get("JAVA_OPTS") is not None or os.environ.get("JAVA_HOME") is not None:
        sys.exit(94)


LINKED_ACCOUNT = "+15555550100"
ACTIVE_LINK_URI = "sgnl://link?uuid=fixture&pub_key=fixture"
WRITE_LOCK = threading.Lock()


def argument_value(name):
    try:
        return sys.argv[sys.argv.index(name) + 1]
    except (ValueError, IndexError):
        return None


SIGNAL_DATA_DIR = Path(argument_value("--data-dir") or "/tmp/kt-signal-fixture")
SIGNAL_DATA_DIR.mkdir(parents=True, exist_ok=True)
# Phase 4: every group engine shares the connector's environment, so group
# identity can only come from the data directory each engine is launched with.
# A non-default group answers with a distinct number, otherwise two engines
# would both report (and the store would UNIQUE-collide on) +15555550100.
LINKED_ACCOUNT = (
    "+15555550101"
    if str(SIGNAL_DATA_DIR).endswith(str(Path("proxy-groups") / "team-a"))
    else "+15555550100"
)
DELETED_MARKER = SIGNAL_DATA_DIR / ".fixture-account-deleted"
STDERR_MARKER = SIGNAL_DATA_DIR / ".fixture-stderr-websocket-error"
FAIL_USER_STATUS_MARKER = SIGNAL_DATA_DIR / ".fixture-fail-user-status"
FAIL_USER_STATUS_COUNT_MARKER = SIGNAL_DATA_DIR / ".fixture-fail-user-status-count"
AUTH_FAILED_MARKER = SIGNAL_DATA_DIR / ".fixture-auth-failed-user-status"
SEND_LOG = SIGNAL_DATA_DIR / ".fixture-send-log.jsonl"
DELETE_MODE = os.environ.get("KT_FAKE_DELETE_MODE", "")
ACCOUNT_LINKED = not DELETED_MARKER.exists()

# Multi-account mode (optimization-plan §6.4 M3.3 tests and the soak driver's
# account ladder): each finishLink hands out a fresh number from its own
# range (+1556555xxxx, disjoint from the fixed fixture numbers) and persists
# it, so a group can link up to its account ceiling and engine restarts keep
# reporting every number handed out so far.
MULTI_ACCOUNT = os.environ.get("KT_FAKE_MULTI_ACCOUNT") == "1"
ACCOUNT_NUMBERS_LOG = SIGNAL_DATA_DIR / ".fixture-linked-numbers"


def linked_multi_numbers():
    if not ACCOUNT_NUMBERS_LOG.exists():
        return []
    return [line.strip() for line in ACCOUNT_NUMBERS_LOG.read_text().splitlines() if line.strip()]


def next_multi_number():
    with WRITE_LOCK:
        numbers = linked_multi_numbers()
        number = f"+1556555{len(numbers) + 1:04d}"
        with ACCOUNT_NUMBERS_LOG.open("a") as log:
            log.write(number + "\n")
    return number


# Receiving account for receive notifications: the newest multi-account
# number when one exists (so post-link receives land on a linked account and
# the store count matches the link count), otherwise the fixed fixture number.
def receiving_account():
    numbers = linked_multi_numbers()
    return numbers[-1] if MULTI_ACCOUNT and numbers else LINKED_ACCOUNT


# --- Controlled-rate receive injection (optimization-plan §6.4 M3.2) -------
#
# The soak driver cannot reach the engines directly (the connector owns the
# only JSON-RPC channels), so it steers load through a marker file in this
# engine's data directory: `.fixture-load` holding
# {"ratePerMinute": R, "accounts": N}. R is messages per minute per account,
# N the number of simulated receiving accounts (keep N <= 8: receive-driven
# upserts bypass the link ceiling, and the soak models a legal engine).
# Every emission appends one line to `.fixture-load-log.jsonl` so the
# baseline report can count exactly what was injected.
LOAD_MARKER = SIGNAL_DATA_DIR / ".fixture-load"
LOAD_LOG = SIGNAL_DATA_DIR / ".fixture-load-log.jsonl"
LOAD_LOCK = threading.Lock()
LOAD_STATE = {"spec": None, "stop": None}


def read_load_spec():
    try:
        spec = json.loads(LOAD_MARKER.read_text())
        rate = int(spec.get("ratePerMinute", 0))
        accounts = int(spec.get("accounts", 1))
    except (OSError, ValueError, AttributeError):
        return None
    if 0 < rate <= 6000 and 1 <= accounts <= 8:
        return {"rate": rate, "accounts": accounts}
    return None


def engine_load_accounts(count):
    # Per-engine deterministic range (+1557xxxxxxxx, disjoint from the fixed
    # fixture numbers and the multi-account link range) seeded from the data
    # directory: two engines sharing one store never flap one account row
    # between groups.
    seed = zlib.crc32(str(SIGNAL_DATA_DIR).encode()) % 10_000_000
    return [f"+1557{seed:07d}{index:02d}" for index in range(count)]


def run_load_injector(spec, stop):
    accounts = engine_load_accounts(spec["accounts"])
    spacing = 60.0 / (spec["rate"] * len(accounts))
    seq = 0
    last_ts = 0
    while not stop.is_set():
        seq += 1
        account = accounts[(seq - 1) % len(accounts)]
        # Strictly increasing timestamps: the connector dedupes receives on
        # (account, conversation, direction, sent_at, sender), so a fixed
        # timestamp would collapse the whole ladder into one message.
        now_ms = int(time.time() * 1000)
        last_ts = max(now_ms, last_ts + 1)
        emit_json(
            {
                "jsonrpc": "2.0",
                "method": "receive",
                "params": {
                    "account": account,
                    "envelope": {
                        "source": "+15555550102",
                        "timestamp": last_ts,
                        "dataMessage": {"message": f"soak load {seq}"},
                    },
                },
            }
        )
        with LOAD_LOG.open("a") as log:
            log.write(
                json.dumps(
                    {"seq": seq, "account": account, "timestamp": last_ts},
                    separators=(",", ":"),
                )
                + "\n"
            )
        stop.wait(spacing)


def watch_load_marker():
    while True:
        spec = read_load_spec()
        with LOAD_LOCK:
            if spec != LOAD_STATE["spec"]:
                if LOAD_STATE["stop"] is not None:
                    LOAD_STATE["stop"].set()
                stop = None
                if spec is not None:
                    stop = threading.Event()
                    threading.Thread(
                        target=run_load_injector, args=(spec, stop), daemon=True
                    ).start()
                LOAD_STATE["spec"] = spec
                LOAD_STATE["stop"] = stop
        time.sleep(0.5)


threading.Thread(target=watch_load_marker, daemon=True).start()


def emit_json(value):
    with WRITE_LOCK:
        print(json.dumps(value, separators=(",", ":")), flush=True)


def emit_stderr_websocket_error():
    # Mimics signal-cli logging a dead receive WebSocket to stderr.
    with WRITE_LOCK:
        print(
            "ERROR WebSocketConnection - WebSocket connection closed unexpectedly",
            file=sys.stderr,
            flush=True,
        )


# Watchdog fixture: with the marker present, report a dead receive WebSocket on
# stderr repeatedly (every 0.5s, delayed so the watchdog has subscribed). The
# loop re-checks the marker so a test can stop further emissions by deleting it;
# each restarted engine process re-reads it at startup.
def emit_stderr_while_marked():
    while STDERR_MARKER.exists():
        time.sleep(0.5)
        if STDERR_MARKER.exists():
            emit_stderr_websocket_error()


if STDERR_MARKER.exists():
    threading.Thread(target=emit_stderr_while_marked, daemon=True).start()


def receive_notification():
    return {
        "jsonrpc": "2.0",
        "method": "receive",
        "params": {
            "account": receiving_account(),
            "envelope": {
                "source": "+15555550101",
                "timestamp": 42,
                "dataMessage": {"message": "private text"},
            },
        },
    }


def emit_receive():
    emit_json(receive_notification())


def emit_json_after(value, delay_seconds):
    time.sleep(delay_seconds)
    emit_json(value)


for line in sys.stdin:
    request = json.loads(line)
    request_id = request.get("id")
    method = request.get("method")
    params = request.get("params") or {}
    emit_receive_after = False

    if method in ("hang", "sendHang"):
        continue
    if method in ("crash", "crashSend"):
        os._exit(17)
    if method == "malformed":
        print("not-json", flush=True)
        continue
    if method == "oversized":
        print("x" * 512, flush=True)
        continue
    if method == "stallStdin":
        # Regression fixture for the stdin writer task (A7): answer this one
        # request, then stop reading stdin for good while staying alive, like
        # a wedged signal-cli whose pipe stays full.
        emit_json({"jsonrpc": "2.0", "id": request_id, "result": {"method": method}})
        time.sleep(3600)
        continue

    if method == "armDelayedReceive":
        # Regression fixture for the stdin writer task (A7): emit a receive
        # notification after a delay without reading anything else from stdin,
        # so a test can prove the actor keeps pumping stdout while the stdin
        # writer is parked on a full pipe.
        threading.Thread(
            target=emit_json_after,
            args=(receive_notification(), max(params.get("delayMs", 0), 0) / 1000),
            daemon=True,
        ).start()
        result = {"method": method}
    elif method == "startLink":
        result = {"deviceLinkUri": ACTIVE_LINK_URI}
    elif method == "finishLink":
        if params.get("deviceLinkUri") != ACTIVE_LINK_URI:
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -1, "message": "Unknown device link uri."},
            }
            emit_json(response)
            continue
        if params.get("deviceName") == "[slow-link-test]":
            continue
        if params.get("deviceName") == "[crash-link-test]":
            # Die with the mutating call in flight: the device may or may not
            # have been linked upstream, which is the indeterminate case.
            os._exit(21)
        ACCOUNT_LINKED = True
        DELETED_MARKER.unlink(missing_ok=True)
        # Multi-account mode hands out a fresh number per link; the default
        # mode keeps the fixed fixture number every existing test expects.
        result = {"number": next_multi_number() if MULTI_ACCOUNT else LINKED_ACCOUNT}
        emit_receive_after = True
    elif method == "listAccounts":
        if MULTI_ACCOUNT:
            result = [{"number": number} for number in linked_multi_numbers()]
        else:
            result = [{"number": LINKED_ACCOUNT}] if ACCOUNT_LINKED else []
    elif method == "listContacts":
        result = [
            {
                "number": LINKED_ACCOUNT,
                "profile": {"givenName": "Test", "familyName": "User"},
            },
            {
                "number": "+15555550101",
                "name": "Alice Contact",
                "profile": {"givenName": "Alice", "familyName": "Example"},
            },
            {
                "number": "+15555550102",
                "profile": {"givenName": "Bob"},
            },
        ]
    elif method == "listGroups":
        result = [
            {
                "id": "ZmFrZS1ncm91cC0x",
                "name": "Fixture Group",
                "isMember": True,
                "members": [{"number": LINKED_ACCOUNT}, {"number": "+15555550101"}],
            },
            {
                "id": "bm90LWEtbWVtYmVy",
                "name": "Former Group",
                "isMember": False,
                "members": [],
            },
        ]
    elif method == "deleteLocalAccountData":
        if DELETE_MODE == "crash_after_delete_once" and DELETED_MARKER.exists():
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -1, "message": "delete was dispatched twice"},
            }
            emit_json(response)
            continue
        ACCOUNT_LINKED = False
        DELETED_MARKER.touch()
        if DELETE_MODE == "crash_after_delete_once":
            os._exit(19)
        result = {}
    elif method == "send":
        # Record the exact JSON-RPC params so tests can assert what the
        # connector dispatched upstream (e.g. quoteTimestamp/quoteAuthor).
        with WRITE_LOCK:
            with SEND_LOG.open("a") as log:
                log.write(json.dumps(params, separators=(",", ":")) + "\n")
        result = {"timestamp": 99, "results": []}
    elif method == "remoteDelete":
        # Same dispatch-recording discipline as `send`: the exact upstream
        # params, so tests can assert targetTimestamp/recipient/groupId.
        with WRITE_LOCK:
            with SEND_LOG.open("a") as log:
                log.write(json.dumps(params, separators=(",", ":")) + "\n")
        # Slow answer: the result lands after a short connector request
        # timeout, so the mutating outcome is indeterminate (timeout path).
        if params.get("targetTimestamp") == 350:
            response = {"jsonrpc": "2.0", "id": request_id, "result": {}}
            threading.Thread(
                target=emit_json_after,
                args=(response, 0.35),
                daemon=True,
            ).start()
            continue
        # One-shot crash with the mutating call in flight: the delete may or
        # may not have reached the server, which is the indeterminate case
        # (engine-exit path).
        if params.get("targetTimestamp") == 421:
            os._exit(23)
        result = {}
    elif method == "sendReaction":
        # Same dispatch-recording discipline as `send`: the exact upstream
        # params, so tests can assert targetAuthor/targetTimestamp/remove.
        with WRITE_LOCK:
            with SEND_LOG.open("a") as log:
                log.write(json.dumps(params, separators=(",", ":")) + "\n")
        # Slow answer: the result lands after a short connector request
        # timeout, so the mutating outcome is indeterminate (timeout path).
        if params.get("targetTimestamp") == 351:
            response = {"jsonrpc": "2.0", "id": request_id, "result": {}}
            threading.Thread(
                target=emit_json_after,
                args=(response, 0.35),
                daemon=True,
            ).start()
            continue
        # One-shot crash with the mutating call in flight: the reaction may
        # or may not have reached the server, which is the indeterminate
        # case (engine-exit path).
        if params.get("targetTimestamp") == 423:
            os._exit(23)
        result = {}
    elif method == "updateContact":
        # Same dispatch-recording discipline as `send`: the exact upstream
        # params, so tests can assert the single-string recipient contract.
        with WRITE_LOCK:
            with SEND_LOG.open("a") as log:
                log.write(json.dumps(params, separators=(",", ":")) + "\n")
        # One-shot crash with the mutating call in flight: the rename may or
        # may not have reached the server, which is the indeterminate case
        # (engine-exit path).
        if params.get("name") == "[fixture-crash-alias]":
            os._exit(25)
        result = {}
    elif method == "sendTyping":
        # Same dispatch-recording discipline as `send`: the exact upstream
        # params, so tests can assert the recipient array / groupId / explicit
        # stop boolean contract.
        with WRITE_LOCK:
            with SEND_LOG.open("a") as log:
                log.write(json.dumps(params, separators=(",", ":")) + "\n")
        # One-shot crash with the mutating call in flight (the magic peer
        # +15555550999): the indicator may or may not have reached the
        # server, which is the indeterminate case (engine-exit path).
        if "+15555550999" in (params.get("recipient") or []):
            os._exit(27)
        result = {}
    elif method == "emitReceive":
        result = {"method": method}
    elif method == "emitStderr":
        emit_stderr_websocket_error()
        result = {"method": method}
    elif method == "getUserStatus":
        if AUTH_FAILED_MARKER.exists():
            # Contract 1.21: the account holder unlinked this device. The
            # real upstream wraps AuthorizationFailedException in an
            # UnexpectedErrorException (-32603) with this message shape.
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {
                    "code": -32603,
                    "message": "Failed to send message: Authorization failed! (AuthorizationFailedException)",
                },
            }
            emit_json(response)
            continue
        if FAIL_USER_STATUS_MARKER.exists():
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -1, "message": "fixture getUserStatus failure"},
            }
            emit_json(response)
            continue
        # Count-based failure: fail the next N getUserStatus calls, then recover.
        if FAIL_USER_STATUS_COUNT_MARKER.exists():
            try:
                remaining = int(FAIL_USER_STATUS_COUNT_MARKER.read_text().strip() or "0")
            except ValueError:
                remaining = 0
            if remaining > 0:
                FAIL_USER_STATUS_COUNT_MARKER.write_text(str(remaining - 1))
                response = {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": {"code": -1, "message": "fixture getUserStatus counted failure"},
                }
                emit_json(response)
                continue
        result = {"isRegistered": True}
    else:
        result = {"method": method}

    response = {"jsonrpc": "2.0", "id": request_id, "result": result}
    if method == "send" and params.get("message") == "[slow-host-test]":
        threading.Thread(
            target=emit_json_after,
            args=(response, 0.35),
            daemon=True,
        ).start()
        continue
    emit_json(response)

    if method == "duplicate":
        emit_json(response)
    if method == "emitReceive" or emit_receive_after:
        emit_receive()
    if method == "emitEnvelope":
        # Contract 1.15 test hook: inject arbitrary envelope receives so a
        # test can exercise reaction / remote-delete / typing / edit /
        # attachment-metadata normalization end to end through a real engine
        # process. The envelope is passed through verbatim; `envelopes`
        # (array form) emits each in order after the response.
        envelopes = params.get("envelopes")
        if not isinstance(envelopes, list):
            envelopes = [params.get("envelope") or {}]
        account = params.get("account") or receiving_account()
        result = {"method": method, "emitted": len(envelopes)}

        def emit_envelopes(account=account, envelopes=envelopes):
            for envelope in envelopes:
                emit_json({
                    "jsonrpc": "2.0",
                    "method": "receive",
                    "params": {"account": account, "envelope": envelope},
                })

        threading.Thread(target=emit_envelopes, daemon=True).start()
