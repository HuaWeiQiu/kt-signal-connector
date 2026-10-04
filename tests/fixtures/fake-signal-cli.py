#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only

import base64
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
# Contract 1.36 pin-sync fixture state: the cloud-order pinned list the
# engine would read from the storage record; setConversationPinned mutates it
# in place so a read-back after a write observes the write.
PINNED_STATE = [
    {"conversationId": "+15555550101", "kind": "contact"},
    {"conversationId": "ZmFrZS1ncm91cC0x", "kind": "group"},
]
# Contract 1.37 sticker-pack sync fixture state: the Storage Service
# StickerPackRecord projection — one installed pack (key + position) and one
# tombstone; setStickerPackSync mutates it in place so a read-back after a
# write observes the write.
PACK_SYNC_STATE = [
    {
        "packId": "abcdef0123456789abcdef0123456789",
        "packKey": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
        "position": 0,
        "deletedAtTimestampMs": None,
    },
    {
        "packId": "11111111111111111111111111112222",
        "packKey": None,
        "position": None,
        "deletedAtTimestampMs": 1727000000000,
    },
]


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


# Contract 1.38 test hook: runtime envelope injection. The connector owns the
# only JSON-RPC channels (the engine socket is private, ADR 0002), so a test
# cannot call the engine's `emitEnvelope` face directly; it instead writes
# `.fixture-emit-envelopes.json` holding {"envelopes": [...]} into this data
# directory and each envelope is delivered verbatim to the receiving account
# as a `receive` notification. The marker is deleted after delivery so a
# later write injects again.
EMIT_MARKER = SIGNAL_DATA_DIR / ".fixture-emit-envelopes.json"


def watch_emit_marker():
    while True:
        try:
            spec = json.loads(EMIT_MARKER.read_text())
        except (OSError, ValueError):
            time.sleep(0.05)
            continue
        envelopes = spec.get("envelopes") if isinstance(spec, dict) else None
        if isinstance(envelopes, list) and envelopes:
            account = spec.get("account") or receiving_account()
            for envelope in envelopes:
                emit_json(
                    {
                        "jsonrpc": "2.0",
                        "method": "receive",
                        "params": {"account": account, "envelope": envelope},
                    }
                )
        try:
            EMIT_MARKER.unlink()
        except OSError:
            pass
        time.sleep(0.05)


threading.Thread(target=watch_emit_marker, daemon=True).start()


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


# Contract 1.27 test hook: `.fixture-extra-receives.json` holds a JSON array
# of raw envelopes. finishLink's receive burst delivers every not-yet-emitted
# entry in order after the default one, and `flush_extra_receives()` re-checks
# the marker on later upstream calls a test controls — sendTyping (via
# presence.setTypingMessage, which changes no conversation state) and
# listContacts — so envelopes appended after link time (e.g. a group reaction
# remove) reach the connector at a chosen point. contacts.sync itself is no
# trigger inside its 60s debounce window: it answers from the cache without
# calling listContacts, so sendTyping is the reliable post-link flush.
EXTRA_RECEIVES_MARKER = SIGNAL_DATA_DIR / ".fixture-extra-receives.json"
EXTRA_RECEIVES_EMITTED = [0]


def pending_extra_receive_notifications():
    if not EXTRA_RECEIVES_MARKER.exists():
        return []
    envelopes = json.loads(EXTRA_RECEIVES_MARKER.read_text())
    if not isinstance(envelopes, list):
        return []
    pending = envelopes[EXTRA_RECEIVES_EMITTED[0]:]
    EXTRA_RECEIVES_EMITTED[0] = len(envelopes)
    return [
        {
            "jsonrpc": "2.0",
            "method": "receive",
            "params": {"account": receiving_account(), "envelope": envelope},
        }
        for envelope in pending
    ]


def flush_extra_receives():
    for notification in pending_extra_receive_notifications():
        emit_json(notification)


def emit_receive():
    emit_json(receive_notification())
    flush_extra_receives()


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
        # Contract 1.27 hook: deliver extra receives appended to the marker
        # after link time before answering. Only reached outside the
        # contacts-sync debounce window.
        flush_extra_receives()
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
        # Member shape mirrors the pinned 0.14.8 JsonGroupMember record
        # (number, uuid, isAdmin — NON_NULL omissions included), so the §4.39
        # roster capture and projection run against the real wire shape.
        result = [
            {
                "id": "ZmFrZS1ncm91cC0x",
                "name": "Fixture Group",
                "isMember": True,
                "members": [
                    {"number": LINKED_ACCOUNT},
                    {
                        "number": "+15555550101",
                        "uuid": "0b7fca57-1234-4d0e-9b0f-4f6c1f8a2e10",
                        "isAdmin": True,
                    },
                ],
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
        # Contract 1.35 test hook: a sticker send whose pack id is all-f
        # crashes with the mutating call in flight — the send may or may not
        # have reached the server, which is the indeterminate case
        # (engine-exit path).
        if (params.get("sticker") or {}).get("packId") == "ffffffff":
            os._exit(23)
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
    elif method in ("sendPinMessage", "sendUnpinMessage", "sendAdminDelete"):
        # Contract 1.33 pin family: same dispatch-recording discipline as
        # `send`: the exact upstream params, so tests can assert
        # targetAuthor/targetTimestamp/pinDurationSeconds/groupId.
        with WRITE_LOCK:
            with SEND_LOG.open("a") as log:
                log.write(json.dumps(params, separators=(",", ":")) + "\n")
        # Slow answer: the result lands after a short connector request
        # timeout, so the mutating outcome is indeterminate (timeout path).
        if method == "sendPinMessage" and params.get("targetTimestamp") == 352:
            response = {"jsonrpc": "2.0", "id": request_id, "result": {}}
            threading.Thread(
                target=emit_json_after,
                args=(response, 0.35),
                daemon=True,
            ).start()
            continue
        # One-shot crash with the mutating call in flight: the pin may or may
        # not have reached the server, which is the indeterminate case
        # (engine-exit path).
        if method == "sendPinMessage" and params.get("targetTimestamp") == 424:
            os._exit(23)
        result = {}
    elif method in ("sendDeliveryReceipt", "sendReadReceipt", "sendViewedReceipt"):
        # Contract 1.34 receipt face: same dispatch-recording discipline as
        # `send`: the exact upstream params, so tests can assert the
        # {account, recipient (single author), timestamps} contract.
        with WRITE_LOCK:
            with SEND_LOG.open("a") as log:
                log.write(json.dumps(params, separators=(",", ":")) + "\n")
        # One-shot crash with the mutating call in flight: the receipt may or
        # may not have reached the server, which is the indeterminate case
        # (engine-exit path). Sentinel 425 keeps the sentinel space disjoint
        # from remoteDelete 421 / reaction 423 / pin 424.
        if 425 in (params.get("timestamps") or []):
            os._exit(23)
        result = {}
    elif method == "sendViewOnceOpen":
        # Contract 1.38 view-once open face: same dispatch-recording
        # discipline as `send`: the exact upstream params, so tests can assert
        # the {account, senderAci, timestamp} contract.
        with WRITE_LOCK:
            with SEND_LOG.open("a") as log:
                log.write(json.dumps(params, separators=(",", ":")) + "\n")
        # One-shot crash with the mutating call in flight: the open sync may
        # or may not have reached the server, which is the indeterminate case
        # (engine-exit path). Sentinel 426 keeps the sentinel space disjoint
        # from remoteDelete 421 / reaction 423 / pin 424 / receipt 425.
        if params.get("timestamp") == 426:
            os._exit(23)
        result = {}
    elif method == "getStickerPackManifest":
        # Contract 1.36 browse face: deterministic manifest keyed by pack id.
        # Sentinel packId ffffffffffffffffffffffffffffffff answers a
        # structured key-invalid error; 0000... answers a retryable fetch
        # failure; 1111... answers a codeless error (the degradation path);
        # anything else returns the fixture manifest (2 stickers + cover).
        if params.get("packId") == "f" * 32:
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -1, "message": "STICKER_PACK_KEY_INVALID"},
            }
            emit_json(response)
            continue
        if params.get("packId") == "0" * 32:
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -1, "message": "STICKER_PACK_FETCH_FAILED"},
            }
            emit_json(response)
            continue
        if params.get("packId") == "1" * 32:
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -1, "message": "fixture malformed"},
            }
            emit_json(response)
            continue
        result = {
            "title": "Fixture Pack",
            "author": "KT Fixture",
            "cover": {"id": 1, "emoji": "🎉", "contentType": "image/webp"},
            "stickers": [
                {"id": 1, "emoji": "🎉", "contentType": "image/webp"},
                {"id": 2, "emoji": "🚀", "contentType": "image/png"},
            ],
            "stickerCount": 2,
        }
    elif method == "getStickerImage":
        # Contract 1.36 browse face: a small deterministic base64 payload.
        # Sentinels mirror the manifest error mapping; stickerId 413 answers
        # STICKER_IMAGE_TOO_LARGE.
        if params.get("packId") == "f" * 32:
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -1, "message": "STICKER_PACK_KEY_INVALID"},
            }
            emit_json(response)
            continue
        if params.get("stickerId") == 404:
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -1, "message": "STICKER_PACK_MALFORMED"},
            }
            emit_json(response)
            continue
        if params.get("stickerId") == 413:
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -1, "message": "STICKER_IMAGE_TOO_LARGE"},
            }
            emit_json(response)
            continue
        payload = base64.b64encode(b"fixture-bytes").decode()
        result = {"dataBase64": payload, "contentType": "image/webp", "size": 13}
    elif method == "getPinnedConversations":
        # Contract 1.36 pin sync read: the cloud order as a fixture constant.
        result = {"pinned": list(PINNED_STATE)}
    elif method == "setConversationPinned":
        # Contract 1.36 pin sync write: record the exact upstream params
        # (tests assert the resolved conversationId/kind/pinned), apply the
        # write to the fixture pin state, answer the post-write cloud state.
        # Peer-key sentinels (locally resolvable, so the connector forwards
        # them): +15555550998 answers a structured STORAGE_UNAVAILABLE,
        # +15555550997 answers CONVERSATION_NOT_RESOLVED, +15555550999
        # crashes with the mutating call in flight.
        with WRITE_LOCK:
            with SEND_LOG.open("a") as log:
                log.write(json.dumps(params, separators=(",", ":")) + "\n")
        conversation_id = params.get("conversationId")
        if conversation_id == "+15555550998":
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -1, "message": "STORAGE_UNAVAILABLE"},
            }
            emit_json(response)
            continue
        if conversation_id == "+15555550997":
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -1, "message": "CONVERSATION_NOT_RESOLVED"},
            }
            emit_json(response)
            continue
        if conversation_id == "+15555550999":
            os._exit(29)
        pinned = [entry for entry in PINNED_STATE if entry["conversationId"] != conversation_id]
        if params.get("pinned"):
            pinned.insert(0, {"conversationId": conversation_id, "kind": params.get("kind")})
        PINNED_STATE[:] = pinned
        result = {"pinned": list(PINNED_STATE)}
    elif method == "getStickerPackSyncs":
        # Contract 1.37 sticker-pack sync read: the record projection as a
        # fixture constant (one installed pack + one tombstone).
        result = {"packs": list(PACK_SYNC_STATE)}
    elif method == "setStickerPackSync":
        # Contract 1.37 sticker-pack sync write: record the exact upstream
        # params (tests assert the resolved account/packId/packKey/position/
        # installed contract), apply the write to the fixture record state,
        # answer the post-write cloud state. Pack-id sentinels: "f"*32
        # answers a structured STORAGE_UNAVAILABLE, "0"*32 answers
        # STORAGE_READ_FAILED, "1"*32 crashes with the mutating call in
        # flight (os._exit(30); disjoint from the pin-sync 29 and the
        # sticker-send 23 paths).
        with WRITE_LOCK:
            with SEND_LOG.open("a") as log:
                log.write(json.dumps(params, separators=(",", ":")) + "\n")
        pack_id = params.get("packId")
        if pack_id == "f" * 32:
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -1, "message": "STORAGE_UNAVAILABLE"},
            }
            emit_json(response)
            continue
        if pack_id == "0" * 32:
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -1, "message": "STORAGE_READ_FAILED"},
            }
            emit_json(response)
            continue
        if pack_id == "1" * 32:
            os._exit(30)
        packs = [entry for entry in PACK_SYNC_STATE if entry["packId"] != pack_id]
        if params.get("installed"):
            # Engine contract: an install carries the key (the connector
            # validated it) and the optional position; the tombstone clears.
            packs.insert(
                0,
                {
                    "packId": pack_id,
                    "packKey": params.get("packKey"),
                    "position": params.get("position"),
                    "deletedAtTimestampMs": None,
                },
            )
        else:
            # An uninstall ignores packKey/position entirely and writes the
            # tombstone with the engine clock.
            packs.append(
                {
                    "packId": pack_id,
                    "packKey": None,
                    "position": None,
                    "deletedAtTimestampMs": int(time.time() * 1000),
                }
            )
        PACK_SYNC_STATE[:] = packs
        result = {"packs": list(PACK_SYNC_STATE)}
    elif method == "updateContact":
        # Same dispatch-recording discipline as `send`: the exact upstream
        # params, so tests can assert the single-string recipient contract.
        with WRITE_LOCK:
            with SEND_LOG.open("a") as log:
                log.write(json.dumps(params, separators=(",", ":")) + "\n")
        # One-shot crash with the mutating call in flight: the rename may or
        # may not have reached the server, which is the indeterminate case
        # (engine-exit path). The same magic rides the timer route's max legal
        # `expiration` value (contract 1.42 §4.41 unknown-outcome drill).
        if params.get("name") == "[fixture-crash-alias]":
            os._exit(25)
        if params.get("expiration") == 2147483647:
            os._exit(25)
        result = {}
    elif method == "setExpirationTimer":
        # Contract 1.42 §4.41: the engine-face timer route. Same
        # dispatch-recording discipline as `updateContact` so tests can assert
        # the seconds-keyed payload, and the same in-flight crash magic on the
        # max legal value.
        with WRITE_LOCK:
            with SEND_LOG.open("a") as log:
                log.write(json.dumps(params, separators=(",", ":")) + "\n")
        if params.get("expirationInSeconds") == 2147483647:
            os._exit(25)
        result = {}
    elif method == "sendTyping":
        # Contract 1.27 hook: deliver extra receives appended to the marker
        # after link time before answering. Typing indicators change no
        # conversation state, so this is the clean post-link flush trigger.
        flush_extra_receives()
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
