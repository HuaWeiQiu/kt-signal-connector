#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only

import json
import os
import sys


for line in sys.stdin:
    request = json.loads(line)
    request_id = request.get("id")
    method = request.get("method")

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

    response = {"jsonrpc": "2.0", "id": request_id, "result": {"method": method}}
    print(json.dumps(response, separators=(",", ":")), flush=True)

    if method == "duplicate":
        print(json.dumps(response, separators=(",", ":")), flush=True)
    if method == "emitReceive":
        notification = {
            "jsonrpc": "2.0",
            "method": "receive",
            "params": {
                "account": "+15555550100",
                "envelope": {
                    "source": "+15555550101",
                    "timestamp": 42,
                    "dataMessage": {"message": "private text"},
                },
            },
        }
        print(json.dumps(notification, separators=(",", ":")), flush=True)
