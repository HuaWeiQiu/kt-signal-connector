#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only

import json
import os
from pathlib import Path
import sys
import threading
import time


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
SEND_LOG = SIGNAL_DATA_DIR / ".fixture-send-log.jsonl"
DELETE_MODE = os.environ.get("KT_FAKE_DELETE_MODE", "")
ACCOUNT_LINKED = not DELETED_MARKER.exists()


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


def emit_receive():
    notification = {
        "jsonrpc": "2.0",
        "method": "receive",
        "params": {
            "account": LINKED_ACCOUNT,
            "envelope": {
                "source": "+15555550101",
                "timestamp": 42,
                "dataMessage": {"message": "private text"},
            },
        },
    }
    emit_json(notification)


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

    if method == "startLink":
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
        result = {"number": LINKED_ACCOUNT}
        emit_receive_after = True
    elif method == "listAccounts":
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
    elif method == "emitReceive":
        result = {"method": method}
    elif method == "emitStderr":
        emit_stderr_websocket_error()
        result = {"method": method}
    elif method == "getUserStatus":
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
