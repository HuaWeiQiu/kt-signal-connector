#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only

import json
import os
import sys
import threading
import time


LINKED_ACCOUNT = "+15555550100"
ACTIVE_LINK_URI = "sgnl://link?uuid=fixture&pub_key=fixture"
WRITE_LOCK = threading.Lock()


def emit_json(value):
    with WRITE_LOCK:
        print(json.dumps(value, separators=(",", ":")), flush=True)


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
        result = {"number": LINKED_ACCOUNT}
        emit_receive_after = True
    elif method == "listAccounts":
        result = [{"number": LINKED_ACCOUNT}]
    elif method == "send":
        result = {"timestamp": 99, "results": []}
    elif method == "emitReceive":
        result = {"method": method}
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
