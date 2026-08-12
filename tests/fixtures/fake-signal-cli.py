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
    if os.environ.get("JAVA_OPTS") != expected_java_opts:
        sys.exit(91)
    if any(os.environ.get(name) for name in ("JAVA_TOOL_OPTIONS", "_JAVA_OPTIONS", "JDK_JAVA_OPTIONS")):
        sys.exit(92)

expected_java_home = os.environ.get("KT_FAKE_EXPECT_JAVA_HOME")
if expected_java_home is not None and os.environ.get("JAVA_HOME") != expected_java_home:
    sys.exit(93)


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
DELETED_MARKER = SIGNAL_DATA_DIR / ".fixture-account-deleted"
STDERR_MARKER = SIGNAL_DATA_DIR / ".fixture-stderr-websocket-error"
FAIL_USER_STATUS_MARKER = SIGNAL_DATA_DIR / ".fixture-fail-user-status"
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
# stderr shortly after startup (delayed so the watchdog has subscribed).
if STDERR_MARKER.exists():
    threading.Thread(
        target=lambda: (time.sleep(0.5), emit_stderr_websocket_error()),
        daemon=True,
    ).start()


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
        ACCOUNT_LINKED = True
        DELETED_MARKER.unlink(missing_ok=True)
        result = {"number": LINKED_ACCOUNT}
        emit_receive_after = True
    elif method == "listAccounts":
        result = [{"number": LINKED_ACCOUNT}] if ACCOUNT_LINKED else []
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
        result = {"timestamp": 99, "results": []}
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
