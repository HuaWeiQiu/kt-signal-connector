#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only

import base64
import json
import os
from pathlib import Path
import sys
import threading
import time
import uuid
import zlib


# No-Java guard: the engine binary is native (GraalVM) with no JVM, so the
# connector must not set JAVA_OPTS or forward JAVA_HOME to the child at all.
if os.environ.get("KT_FAKE_EXPECT_NO_JAVA") is not None:
    if os.environ.get("JAVA_OPTS") is not None or os.environ.get("JAVA_HOME") is not None:
        sys.exit(94)


LINKED_ACCOUNT = "+15555550100"
ACTIVE_LINK_URI = "sgnl://link?uuid=fixture&pub_key=fixture"
WRITE_LOCK = threading.Lock()
# §4.68 snapshot-import fixture: the archive identity the background import
# task projects into the §4.38 NDJSON (one contact recipient + one chat + two
# messages), matching the seed shape tests/history_import.rs lays out.
IMPORT_ALICE_ACI = "6e08f0b6-1c2d-4e5f-8a9b-0c1d2e3f4a5b"
IMPORT_ALICE_E164 = "+15550000001"
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
# Contract 1.46 account-settings/blocked fixture state: the blocked list is a
# read-only storage-order constant (the §4.45 batch has no unblock write
# face); the three write faces are dispatch-recorded no-ops.
BLOCKED_STATE = [
    {"conversationId": "+15555550103", "kind": "contact"},
    {"conversationId": "bGVnYWN5LWdyb3VwLTE=", "kind": "legacyGroup"},
]
# Contract 1.116 §4.97 conversation-state fixture state: keyed
# (kind:conversationId) → the three storage-record bits, seeded on first
# touch with the all-default projection (the cloud record's unset bits).
# getConversationState reads it, setConversationState mutates it in place so
# a read-back after a write observes the write. Canonicalization faces are
# mirrored from the engine's storage ladder: a contact reference outside the
# fixture's contacted numbers, or a group reference that is not 32-byte
# standard-base64 master-key material, answers CONVERSATION_NOT_RESOLVED;
# the "[fixture-state-unavailable]" sentinel answers STORAGE_UNAVAILABLE.
CONVERSATION_STATE = {}
CONVERSATION_STATE_KNOWN_CONTACTS = ("+15555550101", "+15555550102", "+15555550103")


def conversation_state_default():
    return {"mutedUntilTimestamp": 0, "archived": False, "markedUnread": False}


def canonical_conversation_reference(kind, conversation_id):
    """The engine's storage canonicalization refusal face (§4.97): a
    reference that cannot key a storage record of the named family answers
    None (the caller turns it into CONVERSATION_NOT_RESOLVED)."""
    if kind == "contact":
        return conversation_id in CONVERSATION_STATE_KNOWN_CONTACTS
    if kind in ("group", "legacyGroup"):
        if kind == "group":
            try:
                decoded = base64.b64decode(conversation_id, validate=True)
            except Exception:
                return False
            return len(decoded) == 32
        return bool(conversation_id)
    return None


# Contract 1.117 §4.98 group invite-link family: the fixture mirrors the
# engine's group_link.rs pure-function face with a minimal proto codec —
# the GroupInviteLink wire shape is field 1 = contentsV1 (nested), whose
# fields are 1 = groupMasterKey (32B) and 2 = inviteLinkPassword. Build =
# the official buildGroupLink shape (proto → standard base64 → websafe →
# https://signal.group/#<code>); parse accepts the three official URL
# forms plus the bare invite code and answers None on every tolerance
# failure (empty/malformed/versionless/short-key/empty-password — the
# engine answers -32603 for the whole family, so the fixture folds them).
LINK_PASSWORD_FORBIDDEN = b"\xad" * 16
LINK_PASSWORD_PREVIEW_FAILED = b"\xfe" * 16
LINK_PASSWORD_JOIN_REQUESTED = b"\xf7" * 16


def _fixture_varint(value):
    out = b""
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out += bytes([byte | 0x80])
        else:
            return out + bytes([byte])


def _fixture_parse_varint(buf, pos):
    shift = 0
    value = 0
    while True:
        if pos >= len(buf):
            raise ValueError("truncated varint")
        byte = buf[pos]
        pos += 1
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return value, pos
        shift += 7
        if shift > 63:
            raise ValueError("varint too long")


def _fixture_proto_fields(buf):
    """Decode one length-delimited proto message into {field: bytes} (the
    GroupInviteLink shape carries only length-delimited fields; anything
    else is malformed for this face)."""
    fields = {}
    pos = 0
    while pos < len(buf):
        tag, pos = _fixture_parse_varint(buf, pos)
        if tag & 0x07 != 2:
            raise ValueError("unsupported wire type")
        length, pos = _fixture_parse_varint(buf, pos)
        if pos + length > len(buf):
            raise ValueError("truncated field")
        number = tag >> 3
        if number in fields:
            raise ValueError("duplicate field")
        fields[number] = buf[pos : pos + length]
        pos += length
    return fields


def _fixture_from_web_safe(code):
    restored = code.replace("_", "/").replace("-", "+")
    return restored + "=" * ((-len(restored)) % 4)


def fixture_parse_invite_link(value):
    """Engine parse_invite_link parity: the '#' code segment (or the whole
    trimmed value as the bare invite code) → websafe restore → standard
    base64 → proto → contentsV1 must be present → masterKey exactly 32B,
    password non-empty. Answers None on every tolerance failure."""
    trimmed = value.strip()
    if "#" in trimmed:
        code = trimmed.split("#", 1)[1].strip()
    elif trimmed:
        code = trimmed
    else:
        return None
    try:
        raw = base64.b64decode(_fixture_from_web_safe(code), validate=True)
        link_fields = _fixture_proto_fields(raw)
    except Exception:
        return None
    contents = link_fields.get(1)
    if contents is None:
        return None
    try:
        inner = _fixture_proto_fields(contents)
    except Exception:
        return None
    master_key = inner.get(1)
    password = inner.get(2)
    if master_key is None or len(master_key) != 32 or not password:
        return None
    return master_key, password


def fixture_build_invite_link(master_key, password):
    contents = (
        b"\x0a"
        + _fixture_varint(len(master_key))
        + master_key
        + b"\x12"
        + _fixture_varint(len(password))
        + password
    )
    raw = b"\x0a" + _fixture_varint(len(contents)) + contents
    web_safe = (
        base64.b64encode(raw).decode().replace("/", "_").replace("+", "-").replace("=", "")
    )
    return "https://signal.group/#" + web_safe


# Contract 1.95 chat-folder sync fixture state: the Storage Service
# ChatFolderRecordJson projection in storage order — the full 11-key record
# face the engine projects (id/name/position/booleans/folderType/both
# recipient arrays/deletedAtTimestampMs, the tombstone key always present,
# 0 = live); chatFolders.setRecords mutates it in place so a read-back after
# a write observes the write.
CHAT_FOLDER_STATE = [
    {
        "id": "0d6d50f5-7f9e-4a1b-9c2d-3e4f5a6b7c8d",
        "name": "All chats",
        "position": 0,
        "showOnlyUnread": False,
        "showMutedChats": True,
        "includeAllIndividualChats": True,
        "includeAllGroupChats": True,
        "folderType": "ALL",
        "includedConversationIds": [],
        "excludedConversationIds": [],
        "deletedAtTimestampMs": 0,
    },
    {
        "id": "1a2b3c4d-5e6f-4a1b-8c9d-0e1f2a3b4c5d",
        "name": "Work",
        "position": 1,
        "showOnlyUnread": True,
        "showMutedChats": False,
        "includeAllIndividualChats": False,
        "includeAllGroupChats": False,
        "folderType": "CUSTOM",
        "includedConversationIds": [
            {"conversationId": "6e08f0b6-1c2d-4e5f-8a9b-0c1d2e3f4a5b", "kind": "contact"},
            {"conversationId": "ZmFrZS1ncm91cC0x", "kind": "group"},
            {"conversationId": "bGVnYWN5LWdyb3VwLTE=", "kind": "legacyGroup"},
        ],
        "excludedConversationIds": [
            {"conversationId": "7f19e1c7-2d3e-4f6a-9b0c-1d2e3f4a5b6c", "kind": "contact"}
        ],
        "deletedAtTimestampMs": 0,
    },
]
# Contract 1.48 §4.47 avatar fixture: a tiny PNG magic payload — the engine
# sniffs the MIME from the magic bytes and answers a data URI plus the
# decoded byte count; getAvatar's sentinel conversationId answers the
# structured avatar error faces.
AVATAR_PNG_BYTES = b"\x89PNG\r\n\x1a\nfixture-avatar"
AVATAR_DATA_URI = (
    "data:image/png;base64," + base64.b64encode(AVATAR_PNG_BYTES).decode()
)
# Contract 1.68 §4.61 safety-number faces: the fixture identity's peer — the
# contacted number the integration test's conversation row resolves to (the
# link-time contacts sync materializes the same number into the store), so
# listIdentities keyed by it answers the identity record and trust keyed by
# it records the write.
FIXTURE_PEER_NUMBER = "+15555550103"


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
UNREGISTERED_MARKER = SIGNAL_DATA_DIR / ".fixture-unregistered-user-status"
USERNAME_UNSET_MARKER = SIGNAL_DATA_DIR / ".fixture-username-unset"
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

# --- flake instrumentation (temporary) ---
_flake_log_path = Path(f"/tmp/kt-fixture-debug-{os.getpid()}.log")


def flake_log(msg):
    try:
        with _flake_log_path.open("a") as handle:
            handle.write(f"{time.time():.6f} pid={os.getpid()} {msg}\n")
    except OSError:
        pass


def watch_emit_marker():
    flake_log("emit-marker watcher started")
    while True:
        try:
            spec = json.loads(EMIT_MARKER.read_text())
        except (OSError, ValueError):
            time.sleep(0.05)
            continue
        envelopes = spec.get("envelopes") if isinstance(spec, dict) else None
        if isinstance(envelopes, list) and envelopes:
            account = spec.get("account") or receiving_account()
            flake_log(f"marker consumed: {len(envelopes)} envelopes -> {account}")
            for index, envelope in enumerate(envelopes):
                flake_log(f"emit begin {index} ts={envelope.get('timestamp')}")
                emit_json(
                    {
                        "jsonrpc": "2.0",
                        "method": "receive",
                        "params": {"account": account, "envelope": envelope},
                    }
                )
                flake_log(f"emit done {index} ts={envelope.get('timestamp')}")
            flake_log("marker envelopes emitted")
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
        # isTerminated rides the JsonGroup record the same way (the §4.50
        # verified read source); KT_FAKE_TERMINATED_GROUPS lists the group
        # ids that answer it true, so the sync-capture path is exercisable.
        # requestingMembers rides the JsonGroup too (ListGroupsCommand's
        # resolveJsonMembers(group.requestingMembers()), the §4.53 verified
        # read source): KT_FAKE_REQUESTING_GROUPS carries
        # <groupId>=<comma-separated member addresses> entries, so the
        # approval fold is exercisable against a real JsonGroupMemberAddress
        # shape (number-only entries, the NON_NULL omission real single-
        # device candidates carry). pendingMembers rides the same record the
        # same way (resolveJsonMembers(group.pendingMembers()), the §4.57
        # verified read source): KT_FAKE_PENDING_GROUPS carries
        # <groupId>=<comma-separated member addresses> entries, so the
        # invite-revoke fold and the groups.get pendingMembers projection
        # are exercisable against the same shape. The upstream projects no
        # inviter metadata on a pending entry (no addedByUserId — the
        # recorded §4.57 projection difference), so none is faked.
        # Member labels ride the JsonGroupMember record the same way
        # (labelEmoji/label, both @JsonInclude(NON_NULL), the §4.60 verified
        # read source): KT_FAKE_LABELED_GROUPS lists the group ids whose
        # members answer with the fixture label pair, so the sync-capture
        # path is exercisable; unlisted groups keep the label-less shape.
        terminated_ids = {
            value.strip()
            for value in os.environ.get("KT_FAKE_TERMINATED_GROUPS", "").split(",")
            if value.strip()
        }
        requesting: dict[str, list[str]] = {}
        for entry in os.environ.get("KT_FAKE_REQUESTING_GROUPS", "").split(";"):
            if "=" not in entry:
                continue
            group_id, _, addresses = entry.partition("=")
            requesting[group_id.strip()] = [
                address.strip() for address in addresses.split(",") if address.strip()
            ]
        pending: dict[str, list[str]] = {}
        for entry in os.environ.get("KT_FAKE_PENDING_GROUPS", "").split(";"):
            if "=" not in entry:
                continue
            group_id, _, addresses = entry.partition("=")
            pending[group_id.strip()] = [
                address.strip() for address in addresses.split(",") if address.strip()
            ]
        labeled_ids = {
            value.strip()
            for value in os.environ.get("KT_FAKE_LABELED_GROUPS", "").split(",")
            if value.strip()
        }
        fixture_members = [
            {"number": LINKED_ACCOUNT},
            {
                "number": "+15555550101",
                "uuid": "0b7fca57-1234-4d0e-9b0f-4f6c1f8a2e10",
                "isAdmin": True,
            },
        ]
        if "ZmFrZS1ncm91cC0x" in labeled_ids:
            fixture_members[0]["label"] = "Ops"
            fixture_members[1]["labelEmoji"] = "🛠️"
            fixture_members[1]["label"] = "Builders"
        result = [
            {
                "id": "ZmFrZS1ncm91cC0x",
                "name": "Fixture Group",
                "isMember": True,
                "isTerminated": "ZmFrZS1ncm91cC0x" in terminated_ids,
                "requestingMembers": [
                    {"number": address} for address in requesting.get("ZmFrZS1ncm91cC0x", [])
                ],
                "pendingMembers": [
                    {"number": address} for address in pending.get("ZmFrZS1ncm91cC0x", [])
                ],
                "members": fixture_members,
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
    elif method == "chatFolders.list":
        # Contract 1.95 chat-folder sync read: the storage-order record
        # projection as a fixture constant (one ALL folder + one CUSTOM
        # folder with the three-kind recipient arrays). Account gate mirrors
        # the engine (-32603 on an unregistered account). Markers the driving
        # test writes steer the structured storage ladder:
        # `.fixture-chat-folder-read-failed` answers STORAGE_READ_FAILED,
        # `.fixture-chat-folder-unavailable` answers STORAGE_UNAVAILABLE.
        try:
            state = json.loads((SIGNAL_DATA_DIR / "engine-state.json").read_text())
            numbers = {
                entry.get("number")
                for entry in state.get("accounts") or []
            }
        except (OSError, ValueError, AttributeError):
            numbers = set()
        if params.get("account") not in numbers:
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32603, "message": "account is not registered"},
            }
            emit_json(response)
            continue
        if (SIGNAL_DATA_DIR / ".fixture-chat-folder-read-failed").exists():
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -1, "message": "STORAGE_READ_FAILED"},
            }
            emit_json(response)
            continue
        if (SIGNAL_DATA_DIR / ".fixture-chat-folder-unavailable").exists():
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -1, "message": "STORAGE_UNAVAILABLE"},
            }
            emit_json(response)
            continue
        result = {"folders": list(CHAT_FOLDER_STATE)}
    elif method == "chatFolders.setRecords":
        # Contract 1.95 chat-folder sync write: record the exact upstream
        # params (tests assert the resolved account and the verbatim
        # records/removeIds contract), apply the official upload semantics —
        # upsert by id in place, removeIds out of the set — and answer the
        # post-write cloud state. Account gate mirrors the engine (-32603 on
        # an unregistered account); the structured ladder steers from
        # markers: `.fixture-chat-folder-write-failed` answers
        # STORAGE_WRITE_FAILED, `.fixture-chat-folder-read-failed` answers
        # STORAGE_READ_FAILED (the engine reads before it writes),
        # `.fixture-chat-folder-unavailable` answers STORAGE_UNAVAILABLE.
        with WRITE_LOCK:
            with SEND_LOG.open("a") as log:
                log.write(json.dumps(params, separators=(",", ":")) + "\n")
        try:
            state = json.loads((SIGNAL_DATA_DIR / "engine-state.json").read_text())
            numbers = {
                entry.get("number")
                for entry in state.get("accounts") or []
            }
        except (OSError, ValueError, AttributeError):
            numbers = set()
        if params.get("account") not in numbers:
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32603, "message": "account is not registered"},
            }
            emit_json(response)
            continue
        marker_errors = (
            (".fixture-chat-folder-write-failed", "STORAGE_WRITE_FAILED"),
            (".fixture-chat-folder-read-failed", "STORAGE_READ_FAILED"),
            (".fixture-chat-folder-unavailable", "STORAGE_UNAVAILABLE"),
        )
        for marker, code in marker_errors:
            if (SIGNAL_DATA_DIR / marker).exists():
                response = {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": {"code": -1, "message": code},
                }
                emit_json(response)
                continue
        # The mutating-crash sentinel (record id magic; exit 31 = the shared
        # mutating-in-flight crash code, disjoint from every other face by
        # trigger): the connector must answer SEND_OUTCOME_UNKNOWN and never
        # retry.
        if any(
            isinstance(record, dict)
            and record.get("id") == "cede0000-0000-4000-8000-000000000031"
            for record in params.get("records") or []
        ):
            os._exit(31)
        # The engine's strict record grammar (parse_folder): every record
        # must carry the eight required keys with the right shapes — id (uuid
        # string), name (string), position (u64), the four booleans, and the
        # folderType word — else the engine answers -32603. Optional: the two
        # recipient arrays and deletedAtTimestampMs.
        def _folder_shape_ok(record):
            if not isinstance(record, dict):
                return False
            try:
                uuid.UUID(str(record.get("id")))
            except (ValueError, AttributeError, TypeError):
                return False
            if not isinstance(record.get("name"), str):
                return False
            if not isinstance(record.get("position"), int) or isinstance(
                record.get("position"), bool
            ):
                return False
            for key in (
                "showOnlyUnread",
                "showMutedChats",
                "includeAllIndividualChats",
                "includeAllGroupChats",
            ):
                if not isinstance(record.get(key), bool):
                    return False
            if record.get("folderType") not in ("UNKNOWN", "ALL", "CUSTOM"):
                return False
            return True

        records = params.get("records") or []
        if any(not _folder_shape_ok(record) for record in records):
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32603, "message": "Internal error"},
            }
            emit_json(response)
            continue
        with WRITE_LOCK:
            folders = [
                entry
                for entry in CHAT_FOLDER_STATE
                if entry["id"] not in set(params.get("removeIds") or [])
            ]
            for record in params.get("records") or []:
                record = json.loads(json.dumps(record))
                for index, entry in enumerate(folders):
                    if entry["id"] == record["id"]:
                        folders[index] = record
                        break
                else:
                    folders.append(record)
            CHAT_FOLDER_STATE[:] = folders
        result = {"folders": list(CHAT_FOLDER_STATE)}
    elif method == "setAccountConfiguration":
        # Contract 1.46 §4.45 account settings write: record the exact
        # upstream params (tests assert only the provided keys appear — the
        # partial-write contract) and answer the engine's bare success. No
        # free-text param exists on this face, so the STORAGE_* sentinels ride
        # the pin/sticker/profile faces; the shared UpstreamCode mapping is
        # already integration-proven there.
        with WRITE_LOCK:
            with SEND_LOG.open("a") as log:
                log.write(json.dumps(params, separators=(",", ":")) + "\n")
        result = {}
    elif method == "setProfile":
        # Contract 1.46 §4.45 profile write: record the exact upstream
        # params (tests assert the text-face full-write forwarding and the
        # avatar tri-state). Sentinels: about "[fixture-storage-*]" answers a
        # structured storage error (disjoint codes), givenName
        # "[fixture-crash-profile]" crashes with the mutating call in flight
        # (os._exit(31); disjoint from 25/29/30).
        with WRITE_LOCK:
            with SEND_LOG.open("a") as log:
                log.write(json.dumps(params, separators=(",", ":")) + "\n")
        about = params.get("about")
        if about == "[fixture-storage-unavailable]":
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -1, "message": "STORAGE_UNAVAILABLE"},
            }
            emit_json(response)
            continue
        if about == "[fixture-storage-read-failed]":
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -1, "message": "STORAGE_READ_FAILED"},
            }
            emit_json(response)
            continue
        if about == "[fixture-storage-write-failed]":
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -1, "message": "STORAGE_WRITE_FAILED"},
            }
            emit_json(response)
            continue
        if params.get("givenName") == "[fixture-crash-profile]":
            os._exit(31)
        result = {}
    elif method == "setUniversalExpireTimer":
        # Contract 1.46 §4.45 universal timer write: record the exact
        # upstream params (tests assert the expirationInSeconds key). The max
        # legal u32 value is the in-flight crash magic (os._exit(31)), the
        # 1.42 timer-face discipline with a value the per-conversation face
        # cannot carry (i32 ceiling).
        with WRITE_LOCK:
            with SEND_LOG.open("a") as log:
                log.write(json.dumps(params, separators=(",", ":")) + "\n")
        if params.get("expirationInSeconds") == 4294967295:
            os._exit(31)
        result = {}
    elif method == "updateAccount":
        # Contract 1.59 §4.55 username write: record the exact upstream
        # params (tests assert the username / deleteUsername key). Sentinels
        # (grammar-legal so they pass the connector's local shape gate):
        # username "fixture_username_taken.01" answers the upstream
        # InvalidUsernameException face (-1 "Invalid username: ...");
        # "fixture_crash_username.31" crashes with the mutating call in
        # flight (os._exit(31); disjoint from 25/29/30). The set path answers
        # the upstream write-back shape {username, usernameLink}; the delete
        # path answers {} (no result body upstream).
        with WRITE_LOCK:
            with SEND_LOG.open("a") as log:
                log.write(json.dumps(params, separators=(",", ":")) + "\n")
        username = params.get("username")
        if username == "fixture_username_taken.01":
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {
                    "code": -1,
                    "message": "Invalid username: Username is malformed or already taken",
                },
            }
            emit_json(response)
            continue
        if username == "fixture_crash_username.31":
            os._exit(31)
        if "deleteUsername" in params:
            result = {}
        else:
            result = {
                "username": username,
                "usernameLink": "https://signal.me/username/fixture-link",
            }
    elif method == "getUsername":
        # Contract 1.74 §4.65: the kt-engine username read route. Record the
        # exact upstream params (tests assert the resolved account key). The
        # unset marker answers the both-keys-absent never-set shape; the
        # default answers the set shape (the same link the updateAccount
        # write-back carries).
        with WRITE_LOCK:
            with SEND_LOG.open("a") as log:
                log.write(json.dumps(params, separators=(",", ":")) + "\n")
        if USERNAME_UNSET_MARKER.exists():
            result = {}
        else:
            result = {
                "username": "fixture_user.01",
                "usernameLink": "https://signal.me/username/fixture-link",
            }
    elif method == "engine.deviceCapabilities":
        # Contract 1.100 §4.81 D5: the device-face registration probe. Record
        # the exact upstream params (the test asserts the resolved account key
        # and that the connector strips it from the host answer). The fixture
        # answers the registered shape with the four official upload bits —
        # the pinned DEVICE_CAPABILITIES_UPLOAD mirror the engine's own
        # capabilities face projects (fake/real same-code-path, getUsername
        # precedent).
        with WRITE_LOCK:
            with SEND_LOG.open("a") as log:
                log.write(json.dumps(params, separators=(",", ":")) + "\n")
        result = {
            "account": params.get("account"),
            "registered": True,
            "capabilities": {
                "attachmentBackfill": False,
                "spqr": True,
                "usernameChangeSyncMessage": True,
                "optionalPhoneNumber": False,
            },
        }
    elif method == "usernameLookup":
        # Contract 1.74 §4.65: the kt-engine lookup route — the same top-level
        # array contract as the getUserStatus --username face (first entry
        # registered with username+uuid, the rest the unregistered face; the
        # ratelimit sentinel answers the plain upstream error). The engine
        # record carries no `number` key — that key is the pinned
        # getUserStatus face's alone.
        with WRITE_LOCK:
            with SEND_LOG.open("a") as log:
                log.write(json.dumps(params, separators=(",", ":")) + "\n")
        if "[fixture-username-ratelimited]" in params.get("usernames", []):
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -1, "message": "fixture usernameLookup failure"},
            }
            emit_json(response)
            continue
        entries = []
        for index, name in enumerate(params.get("usernames", [])):
            if index == 0:
                entries.append(
                    {
                        "recipient": name,
                        "username": name,
                        "uuid": "00000000-0000-0000-0000-00000000000" + str(index % 10),
                        "isRegistered": True,
                    }
                )
            else:
                entries.append({"recipient": name, "isRegistered": False})
        result = entries
    elif method == "usernameReserve":
        # Contract 1.75 §4.66: the kt-engine username write face, hop one.
        # Record the exact upstream params (tests assert the resolved account
        # + username keys). Sentinels mirror the engine's structured server
        # codes (code -1 + the literal in the message):
        # "fixture_username_taken.01" answers USERNAME_TAKEN;
        # "fixture_username_ratelimited.01" answers USERNAME_RATE_LIMITED;
        # "fixture_crash_username.31" crashes with the mutating call in
        # flight (os._exit(31); the JVM updateAccount twin never fires in
        # kt-engine mode). Bare nicknames adopt the .07 discriminator (the
        # engine's fake first-candidate discipline); full names echo.
        with WRITE_LOCK:
            with SEND_LOG.open("a") as log:
                log.write(json.dumps(params, separators=(",", ":")) + "\n")
        username = params.get("username") or ""
        if username == "fixture_username_taken.01":
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -1, "message": "USERNAME_TAKEN"},
            }
            emit_json(response)
            continue
        if username == "fixture_username_ratelimited.01":
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -1, "message": "USERNAME_RATE_LIMITED"},
            }
            emit_json(response)
            continue
        if username == "fixture_crash_username.31":
            os._exit(31)
        adopted = username if "." in username else username + ".07"
        result = {
            "username": adopted,
            "usernameHash": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        }
    elif method == "usernameConfirm":
        # Contract 1.75 §4.66: hop two — proof + link land the reservation.
        # "fixture_username_gone.01" answers USERNAME_RESERVATION_GONE (the
        # torn two-hop face). Success answers the write-back shape
        # {username, usernameLink}.
        with WRITE_LOCK:
            with SEND_LOG.open("a") as log:
                log.write(json.dumps(params, separators=(",", ":")) + "\n")
        username = params.get("username") or ""
        if username == "fixture_username_gone.01":
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -1, "message": "USERNAME_RESERVATION_GONE"},
            }
            emit_json(response)
            continue
        result = {
            "username": username,
            "usernameLink": "https://signal.me/username/fixture-link",
        }
    elif method == "usernameDelete":
        # Contract 1.75 §4.66: the delete hop — server DELETE + storage clear,
        # idempotent. No result body (the JVM delete path's shape).
        with WRITE_LOCK:
            with SEND_LOG.open("a") as log:
                log.write(json.dumps(params, separators=(",", ":")) + "\n")
        result = {}
    elif method == "sendStory":
        # Contract 1.76 §4.67: the kt-engine story send face. Record the
        # exact upstream params (tests assert the resolved account, the
        # message/recipients keys, and the omitted-by-default option keys).
        # Sentinel: message "fixture_story_crash.31" crashes with the
        # mutating call in flight (os._exit(31), the usernameReserve
        # discipline; disjoint from the JVM send magic). Success answers the
        # upstream write shape {timestamp, results} — the fake answers an
        # empty per-recipient list (the honest offline face; the real engine
        # lists one success entry per recipient).
        with WRITE_LOCK:
            with SEND_LOG.open("a") as log:
                log.write(json.dumps(params, separators=(",", ":")) + "\n")
        if params.get("message") == "fixture_story_crash.31":
            os._exit(31)
        result = {
            "timestamp": int(time.time() * 1000),
            "results": [],
        }
    elif method == "getBlockedConversations":
        # Contract 1.46 §4.45 blocked read: the storage-order projection as a
        # fixture constant (one contact + one legacy group). Read face, no
        # sentinel input exists on this face — the STORAGE_* passthrough is
        # integration-proven on the sibling faces (getStickerPackSyncs
        # precedent).
        result = {"blocked": list(BLOCKED_STATE)}
    elif method == "listIdentities":
        # Contract 1.68 §4.61 safety-number read: the pinned face answers a
        # JSON array of JsonIdentity records. Read face, no SEND_LOG dispatch
        # recording (read-lane discipline, listContacts precedent). The
        # number param keys the read; the fixture answers the contacted
        # peer's identity record (the contacts fixture's number) or an empty
        # array for any other recipient — the honest no-identity-yet state
        # (never exchanged messages).
        number = params.get("number")
        if number == FIXTURE_PEER_NUMBER:
            result = [{
                "number": FIXTURE_PEER_NUMBER,
                "uuid": "99999999-9999-9999-9999-999999999999",
                "fingerprint": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                "safetyNumber": "12345 67890 12345 67890 12345 67890 12345 67890 12345 67890 12345 67890",
                "scannableSafetyNumber": "AQIDBAUGBwgJCgsMDQ4PEA==",
                "trustLevel": "TRUSTED_UNVERIFIED",
                "addedTimestamp": 1727000000000,
            }]
        else:
            result = []
    elif method == "trust":
        # Contract 1.68 §4.61 verify write: record the exact upstream params
        # (tests assert the resolved recipient + the echoed verification
        # digits), answer the pinned face's empty success object. The
        # verifiedSafetyNumber grammar is the upstream's IdentityVerification
        # Code.parse: the fixture mirrors only the digits-stripped 60-digit
        # form — anything else is the upstream's definite UserError (-1), the
        # honest mismatch face. No crash sentinel: the trust outcome is
        # deterministic upstream (parse-or-apply), the unknown-outcome drill
        # is carried by the sibling mutating faces (25/29/30/31/32/33).
        with WRITE_LOCK:
            with SEND_LOG.open("a") as log:
                log.write(json.dumps(params, separators=(",", ":")) + "\n")
        digits = (params.get("verifiedSafetyNumber") or "").replace(" ", "")
        if digits != "123456789012345678901234567890123456789012345678901234567890":
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -1, "message": "User input error (UserErrorException)"},
            }
            emit_json(response)
            continue
        result = {}
    elif method == "getAvatar":
        # Contract 1.48 §4.47 avatar download: read face, no SEND_LOG
        # dispatch recording (read-lane discipline, listContacts precedent).
        # Sentinel conversationIds: "[fixture-avatar-unavailable]" answers
        # AVATAR_UNAVAILABLE (the honest no-avatar), "[fixture-avatar-failed]"
        # answers AVATAR_FETCH_FAILED, "[fixture-avatar-large]" answers
        # AVATAR_TOO_LARGE — all three disjoint from every other face's
        # magic.
        conversation_id = params.get("conversationId")
        if conversation_id == "[fixture-avatar-unavailable]":
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -1, "message": "AVATAR_UNAVAILABLE"},
            }
            emit_json(response)
            continue
        if conversation_id == "[fixture-avatar-failed]":
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -1, "message": "AVATAR_FETCH_FAILED"},
            }
            emit_json(response)
            continue
        if conversation_id == "[fixture-avatar-large]":
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -1, "message": "AVATAR_TOO_LARGE"},
            }
            emit_json(response)
            continue
        result = {"dataUri": AVATAR_DATA_URI, "size": len(AVATAR_PNG_BYTES)}
    elif method == "getConversationState":
        # Contract 1.116 §4.97 state read: the storage ladder's canonical
        # refusal face first (unknown record family reference →
        # CONVERSATION_NOT_RESOLVED, the engine's ConversationNotResolved),
        # then the three-bit projection — an unseeded (kind, id) reads as the
        # all-default record (the cloud record's unset bits). Sentinel
        # conversationId "[fixture-state-unavailable]" answers
        # STORAGE_UNAVAILABLE (the storage-outage face; disjoint from every
        # other face's magic). Read face, no SEND_LOG dispatch recording
        # (read-lane discipline, listContacts precedent).
        account = params.get("account")
        kind = params.get("kind")
        conversation_id = params.get("conversationId")
        if not account or kind not in ("contact", "group", "legacyGroup") or not conversation_id:
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32603, "message": "invalid getConversationState params"},
            }
            emit_json(response)
            continue
        if conversation_id == "[fixture-state-unavailable]":
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -1, "message": "STORAGE_UNAVAILABLE"},
            }
            emit_json(response)
            continue
        if not canonical_conversation_reference(kind, conversation_id):
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -1, "message": "CONVERSATION_NOT_RESOLVED"},
            }
            emit_json(response)
            continue
        key = f"{kind}:{conversation_id}"
        state = dict(CONVERSATION_STATE.get(key, conversation_state_default()))
        result = {
            "mutedUntilTimestamp": state["mutedUntilTimestamp"],
            "archived": state["archived"],
            "markedUnread": state["markedUnread"],
        }
    elif method == "setConversationBlocked":
        # Contract 1.48 §4.47 blocked write: record the exact upstream params
        # (tests assert the resolved conversationId/kind/blocked contract),
        # apply the write to the fixture blocked state, answer the post-write
        # full list. ConversationId sentinels: "[fixture-blocked-unavailable]"
        # answers STORAGE_UNAVAILABLE, "[fixture-blocked-unresolved]" answers
        # CONVERSATION_NOT_RESOLVED, "[fixture-blocked-crash]" crashes with
        # the mutating call in flight (os._exit(32); disjoint from
        # 25/29/30/31).
        with WRITE_LOCK:
            with SEND_LOG.open("a") as log:
                log.write(json.dumps(params, separators=(",", ":")) + "\n")
        conversation_id = params.get("conversationId")
        if conversation_id == "[fixture-blocked-unavailable]":
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -1, "message": "STORAGE_UNAVAILABLE"},
            }
            emit_json(response)
            continue
        if conversation_id == "[fixture-blocked-unresolved]":
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -1, "message": "CONVERSATION_NOT_RESOLVED"},
            }
            emit_json(response)
            continue
        if conversation_id == "[fixture-blocked-crash]":
            os._exit(32)
        conversation_id = params.get("conversationId")
        kind = params.get("kind")
        blocked = [entry for entry in BLOCKED_STATE if entry["conversationId"] != conversation_id]
        if params.get("blocked"):
            blocked.append({"conversationId": conversation_id, "kind": kind})
        BLOCKED_STATE[:] = blocked
        result = {"blocked": list(BLOCKED_STATE)}
    elif method == "setConversationState":
        # Contract 1.116 §4.97 state write: record the exact upstream params
        # (tests assert the resolved reference + the only-present-flags
        # partial-write contract), apply the write over the fixture state,
        # answer the post-write three-bit projection. Parameter discipline
        # mirrors the engine face verbatim: mutedUntilTimestamp must be a
        # number, archived/markedUnread booleans, at least one flag present —
        # anything else is the -32603 parameter face. Sentinels mirror the
        # read face: "[fixture-state-unavailable]" answers STORAGE_UNAVAILABLE;
        # the canonicalization refusal answers CONVERSATION_NOT_RESOLVED. No
        # crash sentinel: the sibling mutating faces carry the unknown-outcome
        # drills (25/29/30/31/32/33).
        with WRITE_LOCK:
            with SEND_LOG.open("a") as log:
                log.write(json.dumps(params, separators=(",", ":")) + "\n")
        kind = params.get("kind")
        conversation_id = params.get("conversationId")
        if not params.get("account") or kind not in (
            "contact",
            "group",
            "legacyGroup",
        ) or not conversation_id:
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32603, "message": "invalid setConversationState params"},
            }
            emit_json(response)
            continue
        muted_until = params.get("mutedUntilTimestamp")
        archived = params.get("archived")
        marked_unread = params.get("markedUnread")
        if muted_until is None and archived is None and marked_unread is None:
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32603, "message": "no write intent"},
            }
            emit_json(response)
            continue
        # JSON true/false are ints in Python — gate the bool leak first.
        if muted_until is not None and (
            isinstance(muted_until, bool) or not isinstance(muted_until, int)
        ):
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32603, "message": "invalid mutedUntilTimestamp"},
            }
            emit_json(response)
            continue
        if (archived is not None and not isinstance(archived, bool)) or (
            marked_unread is not None and not isinstance(marked_unread, bool)
        ):
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32603, "message": "invalid boolean flag"},
            }
            emit_json(response)
            continue
        if conversation_id == "[fixture-state-unavailable]":
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -1, "message": "STORAGE_UNAVAILABLE"},
            }
            emit_json(response)
            continue
        if not canonical_conversation_reference(kind, conversation_id):
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -1, "message": "CONVERSATION_NOT_RESOLVED"},
            }
            emit_json(response)
            continue
        key = f"{kind}:{conversation_id}"
        state = dict(CONVERSATION_STATE.get(key, conversation_state_default()))
        if muted_until is not None:
            state["mutedUntilTimestamp"] = muted_until
        if archived is not None:
            state["archived"] = archived
        if marked_unread is not None:
            state["markedUnread"] = marked_unread
        CONVERSATION_STATE[key] = state
        result = {
            "mutedUntilTimestamp": state["mutedUntilTimestamp"],
            "archived": state["archived"],
            "markedUnread": state["markedUnread"],
        }
    elif method == "sendCallMessage":
        # Contract 1.50 §4.48 call-signaling outbound face: record the exact
        # upstream params (tests assert the resolved recipient + verbatim
        # payload), answer the engine's {timestamp} envelope. The engine
        # parses no signaling content and neither does this fixture — the
        # data string ships as-is. Sentinel payloads: "[fixture-call-crash]"
        # crashes with the mutating call in flight (os._exit(33); disjoint
        # from 25/29/30/31/32).
        with WRITE_LOCK:
            with SEND_LOG.open("a") as log:
                log.write(json.dumps(params, separators=(",", ":")) + "\n")
        # Contract 1.82 group arm (the negotiated engine face): `group` and
        # `recipient` are mutually exclusive — a malformed envelope answers
        # the engine's -32603 parameter face and is never dispatched.
        if bool(params.get("group")) == bool(params.get("recipient")):
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {
                    "code": -32603,
                    "message": "sendCallMessage requires exactly one of group, recipient",
                },
            }
            emit_json(response)
            continue
        if params.get("data") == "[fixture-call-crash]":
            os._exit(33)
        result = {"timestamp": 1727000000000}
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
    elif method == "updateGroup":
        # Contract 1.45 §4.44: the group-management write route. Same
        # dispatch-recording discipline as `send`/`updateContact`: the exact
        # upstream params land in SEND_LOG so tests assert the change-set keys
        # verbatim. The alias crash magic gets a group twin: the crash fires
        # with the change in flight — the indeterminate case (engine-exit
        # path). Success answers the updateGroup result shape (a timestamp
        # plus the per-recipient fan-out list).
        #
        # 2026-10-06 group-timer batch (engine main @ 216a4c7 parity): the
        # `expiration` route is the engine face's updateGroup {expiration}
        # form. Its crash magic is the timer twin "[fixture-crash-group-timer]"
        # (os._exit(34); disjoint from 25/29/30/31/32/33), and the
        # server-rejection faces mirror the engine's GROUP_UPDATE_FAILED data
        # mapping: groupId "[fixture-group-forbidden]" answers data.status=403
        # (non-admin), "[fixture-group-conflict]" answers data.status=409
        # (revision race). Contract 1.115 §4.96: the sentinels answer on
        # every updateGroup route (the wired groups.update face rides
        # title/member/link-access/avatar changes, never `expiration`).
        with WRITE_LOCK:
            with SEND_LOG.open("a") as log:
                log.write(json.dumps(params, separators=(",", ":")) + "\n")
        if params.get("name") == "[fixture-crash-group]":
            os._exit(25)
        if params.get("expiration") is not None and params.get("groupId") == "[fixture-crash-group-timer]":
            os._exit(34)
        if params.get("groupId") == "[fixture-group-forbidden]":
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {
                    "code": -1,
                    "message": "GROUP_UPDATE_FAILED",
                    "data": {"status": 403},
                },
            }
            emit_json(response)
            continue
        if params.get("groupId") == "[fixture-group-conflict]":
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {
                    "code": -1,
                    "message": "GROUP_UPDATE_FAILED",
                    "data": {"status": 409},
                },
            }
            emit_json(response)
            continue
        # Contract 1.58 §4.53: the join-request approval rides updateGroup's
        # member family (the fold already emptied everything but requesting
        # members). Contract 1.64 §4.57: the invite revocation rides the
        # same updateGroup route's removeMembers family (the fold already
        # emptied everything but pending invitees). The sentinel group ids
        # answer the upstream non-admin refusal (the GroupsV2 user error →
        # UPSTREAM_ERROR family); the crash sentinels ride the
        # members/removeMembers array and crash with the mutating call in
        # flight (os._exit(36)/os._exit(37); disjoint from
        # 25/29/30/31/32/33/34/35 and from each other).
        if params.get("groupId") == "[fixture-approve-forbidden]":
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {
                    "code": -1,
                    "message": "UserErrorException: Failed to send group update",
                },
            }
            emit_json(response)
            continue
        if params.get("groupId") == "[fixture-approve-crash]" and (
            params.get("members") or params.get("removeMembers")
        ):
            os._exit(36)
        if params.get("groupId") == "[fixture-revoke-forbidden]":
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {
                    "code": -1,
                    "message": "UserErrorException: Failed to send group update",
                },
            }
            emit_json(response)
            continue
        if params.get("groupId") == "[fixture-revoke-crash]" and (
            params.get("members") or params.get("removeMembers")
        ):
            os._exit(37)
        result = {"timestamp": 99, "results": []}
    elif method == "leaveGroup":
        # Contract 1.45 §4.44 / contract 1.115 §4.96: the wired groups.quit
        # route — the engine's method name for the upstream quit write (the
        # engine face never carried a `quitGroup` literal). Same recording
        # discipline; the magic group id answers the upstream user error a
        # last-admin quit produces (the deterministic UPSTREAM_ERROR face),
        # every other id leaves cleanly with the engine's {status: "left"}.
        with WRITE_LOCK:
            with SEND_LOG.open("a") as log:
                log.write(json.dumps(params, separators=(",", ":")) + "\n")
        if params.get("groupId") == "ZmFhaWwtZ3JvdXAtMg==":
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -1, "message": "UserErrorException: Last group admin"},
            }
            emit_json(response)
            continue
        result = {"status": "left"}
    elif method == "terminateGroup":
        # Contract 1.52 §4.50: the group-termination write route. Same
        # recording discipline as updateGroup/quitGroup: the exact upstream
        # params land in SEND_LOG so tests assert the {account, groupId}
        # payload verbatim. Sentinel magics (disjoint from the quit-fail
        # group): the forbidden group id answers the upstream non-admin
        # refusal (GroupSendingNotAllowedException user error →
        # UPSTREAM_ERROR family), "[fixture-terminate-crash]" rides the
        # groupId and crashes with the mutating call in flight
        # (os._exit(35); disjoint from 25/29/30/31/32/33/34) — the
        # indeterminate case. Success answers the SendGroupMessageResults
        # shape (a timestamp plus the per-recipient fan-out list).
        with WRITE_LOCK:
            with SEND_LOG.open("a") as log:
                log.write(json.dumps(params, separators=(",", ":")) + "\n")
        if params.get("groupId") == "[fixture-terminate-forbidden]":
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {
                    "code": -1,
                    "message": "UserErrorException: Failed to terminate group: "
                    "GroupSendingNotAllowedException",
                },
            }
            emit_json(response)
            continue
        if params.get("groupId") == "[fixture-terminate-crash]":
            os._exit(35)
        result = {"timestamp": 99, "results": []}
    elif method == "reportSpam":
        # Contract 1.49 §4.48: the report write route. Same recording
        # discipline as `send`: the exact upstream params land in SEND_LOG so
        # tests assert the {account, targetAci, serverGuids} payload verbatim.
        # Sentinel magic on the serverGuid values: `guid-crash` exits with the
        # report in flight (the indeterminate case), `guid-reject-all` answers
        # the structured per-batch refusal (the engine's whole-batch-rejected
        # shape), and `guid-partial` rides back as one server-rejected entry
        # (status 400) inside an otherwise successful batch — the per-id
        # failure shape.
        with WRITE_LOCK:
            with SEND_LOG.open("a") as log:
                log.write(json.dumps(params, separators=(",", ":")) + "\n")
        guids = params.get("serverGuids") or []
        if "guid-crash" in guids:
            os._exit(25)
        if "guid-reject-all" in guids:
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -1, "message": "REPORT_SPAM_FAILED"},
            }
            emit_json(response)
            continue
        reported = len(guids)
        failures = [{"serverGuid": "guid-partial", "status": 400}] if "guid-partial" in guids else []
        result = {"reported": reported, "failures": failures}
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
    elif method == "emitGroupUpdated":
        # 2026-10-06 group-timer batch: inject the engine's `groupUpdated`
        # projection verbatim (kt-signal-engine main @ 216a4c7, compat §4) —
        # one receive notification whose envelope carries `groupUpdated`
        # {groupId, revision, expireTimerSeconds, editor?} instead of a
        # message body. Params mirror the projection keys; absent keys take
        # the fixture group / revision 1 / off defaults, so the common case
        # needs no params at all.
        result = {"method": method}
    elif method == "emitStderr":
        emit_stderr_websocket_error()
        result = {"method": method}
    elif method == "backup.export":
        # §4.46 export-task face: the engine reads the connector's artifact
        # from the registered account directory, fail-closed on the start
        # ladder (account registered → artifact present → first line parses).
        # The directory comes from this process's own registry, exactly what
        # the real engine reads.
        registry = SIGNAL_DATA_DIR / "engine-state.json"
        account_dir = None
        try:
            state = json.loads(registry.read_text())
            for entry in state.get("accounts") or []:
                if entry.get("number") == params.get("account"):
                    account_dir = SIGNAL_DATA_DIR / entry["dir"]
                    break
        except (OSError, ValueError, KeyError, AttributeError):
            account_dir = None
        if account_dir is None:
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32603, "message": "account is not registered"},
            }
            emit_json(response)
            continue
        artifact = account_dir / "backup-export-input.ndjson"
        if not artifact.is_file() or artifact.stat().st_size == 0:
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -1, "message": "BACKUP_ARTIFACT_MISSING"},
            }
            emit_json(response)
            continue
        lines = artifact.read_text().splitlines()
        first = json.loads(lines[0]) if lines else None
        if not isinstance(first, dict) or first.get("type") != "account":
            artifact.unlink(missing_ok=True)
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -1, "message": "BACKUP_ARTIFACT_INVALID"},
            }
            emit_json(response)
            continue
        given_name = first.get("givenName") or ""
        # Export-task sentinels ride the account line's givenName — the one
        # field the connector's builder derives from store data, so a seeded
        # display name steers the failure ladder without a second channel.
        if given_name == "[fixture-artifact-missing]":
            artifact.unlink(missing_ok=True)
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -1, "message": "BACKUP_ARTIFACT_MISSING"},
            }
            emit_json(response)
            continue
        if given_name == "[fixture-artifact-invalid]":
            artifact.write_text("not-json\n")
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -1, "message": "BACKUP_ARTIFACT_INVALID"},
            }
            emit_json(response)
            continue
        if given_name == "[fixture-crash-backup]":
            os._exit(33)
        if given_name == "[fixture-snapshot-exists]":
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -1, "message": "BACKUP_SNAPSHOT_EXISTS"},
            }
            emit_json(response)
            continue
        if given_name == "[fixture-io-failed]":
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -1, "message": "BACKUP_IO_FAILED"},
            }
            emit_json(response)
            continue
        # The happy path: the task runs in the background — writes the §5
        # snapshot shape (metadata + main under SignalBackups/signal-backup-
        # <ts>), then deletes the transitional artifact on leaving running
        # (§10.2), and `started` is answered immediately. A
        # `.fixture-backup-running` marker holds the task in `running` for
        # exportStatus polls until the task finishes.
        running_marker = SIGNAL_DATA_DIR / ".fixture-backup-running"
        running_marker.touch()

        def run_backup_task(account_dir=account_dir, running_marker=running_marker):
            time.sleep(0.2)
            snapshot = account_dir / (
                "SignalBackups/signal-backup-"
                + time.strftime("%Y-%m-%d-%H-%M-%S", time.gmtime())
            )
            snapshot.mkdir(parents=True, exist_ok=True)
            (snapshot / "metadata").write_bytes(b"fixture-snapshot-metadata")
            (snapshot / "main").write_bytes(b"fixture-snapshot-main-bytes-0123456789")
            artifact.unlink(missing_ok=True)
            running_marker.unlink(missing_ok=True)

        threading.Thread(target=run_backup_task, daemon=True).start()
        result = {"status": "started"}
    elif method == "backup.exportStatus":
        # §4.46 status face: the task registry is process memory, mirrored by
        # the `.fixture-backup-running` marker; snapshots are listed from disk
        # (name + main size, newest first, ≤ 32). Keys are honest-absence —
        # `lines`/`frames`/`snapshot`/`error` only when they have a referent.
        running_marker = SIGNAL_DATA_DIR / ".fixture-backup-running"
        account_dir = None
        try:
            state = json.loads((SIGNAL_DATA_DIR / "engine-state.json").read_text())
            for entry in state.get("accounts") or []:
                if entry.get("number") == params.get("account"):
                    account_dir = SIGNAL_DATA_DIR / entry["dir"]
                    break
        except (OSError, ValueError, KeyError, AttributeError):
            account_dir = None
        if account_dir is None:
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32603, "message": "account is not registered"},
            }
            emit_json(response)
            continue
        snapshot_root = account_dir / "SignalBackups"
        names = []
        if snapshot_root.is_dir():
            for entry in sorted(snapshot_root.iterdir(), reverse=True):
                if entry.is_dir() and entry.name.startswith("signal-backup-") and not entry.name.endswith("-tmp"):
                    names.append(entry.name)
        names = names[:32]
        snapshots = []
        for name in names:
            main = snapshot_root / name / "main"
            snapshots.append({
                "name": name,
                "mainBytes": main.stat().st_size if main.is_file() else 0,
            })
        artifact = account_dir / "backup-export-input.ndjson"
        if running_marker.exists():
            result = {
                "state": "running",
                "artifactPresent": artifact.is_file(),
            }
        elif snapshots:
            newest = snapshots[0]
            result = {
                "state": "completed",
                "artifactPresent": False,
                "finishedAtMs": int(time.time() * 1000),
                "snapshot": dict(newest),
                "snapshots": snapshots,
            }
        else:
            result = {
                "state": "idle",
                "artifactPresent": False,
                "snapshots": snapshots,
            }
    elif method == "backup.importSnapshot":
        # §4.68 snapshot-import face: the engine consumes the caller-chosen
        # official snapshot directory. The directory string is the steering
        # channel — sentinel substrings pick the structured-error ladder; the
        # happy path answers `started` immediately and a background thread
        # writes the §4.38 NDJSON archive, with a `.fixture-import-running`
        # marker holding the status polls in `running` (the export pattern).
        snapshot_dir = params.get("snapshotDir") or ""
        account_dir = None
        try:
            state = json.loads((SIGNAL_DATA_DIR / "engine-state.json").read_text())
            for entry in state.get("accounts") or []:
                if entry.get("number") == params.get("account"):
                    account_dir = SIGNAL_DATA_DIR / entry["dir"]
                    break
        except (OSError, ValueError, KeyError, AttributeError):
            account_dir = None
        if account_dir is None:
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32603, "message": "account is not registered"},
            }
            emit_json(response)
            continue
        import_error = None
        if "[fixture-snapshot-missing]" in snapshot_dir:
            import_error = "BACKUP_SNAPSHOT_MISSING"
        elif "[fixture-snapshot-metadata-invalid]" in snapshot_dir:
            import_error = "BACKUP_SNAPSHOT_METADATA_INVALID"
        elif "[fixture-snapshot-key-mismatch]" in snapshot_dir:
            import_error = "BACKUP_SNAPSHOT_KEY_MISMATCH"
        elif "[fixture-io-failed]" in snapshot_dir:
            import_error = "BACKUP_SNAPSHOT_IO_FAILED"
        elif "[fixture-import-running]" in snapshot_dir:
            import_error = "BACKUP_IMPORT_ALREADY_RUNNING"
        if import_error is not None:
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -1, "message": import_error},
            }
            emit_json(response)
            continue

        def run_import_task(account_dir=account_dir, snapshot_dir=snapshot_dir, running_marker=None):
            time.sleep(0.2)
            if "[fixture-import-fails-later]" in snapshot_dir:
                (SIGNAL_DATA_DIR / ".fixture-import-failed").touch()
            else:
                recipient = (
                    '{"type":"recipient","id":1,"kind":"contact","name":"Alice Import",'
                    '"aci":"' + IMPORT_ALICE_ACI + '","e164":"' + IMPORT_ALICE_E164 + '"}'
                )
                chat = '{"type":"chat","id":10,"recipientId":1}'
                incoming = (
                    '{"type":"message","chatId":10,"authorId":1,"dateSent":1000,'
                    '"direction":"incoming","text":"archived hello"}'
                )
                outgoing = (
                    '{"type":"message","chatId":10,"authorId":1,"dateSent":2000,'
                    '"direction":"outgoing","text":"archived reply"}'
                )
                (account_dir / "history-import.ndjson").write_text(
                    recipient + "\n" + chat + "\n" + incoming + "\n" + outgoing + "\n"
                )
            if running_marker is not None:
                running_marker.unlink(missing_ok=True)

        running_marker = SIGNAL_DATA_DIR / ".fixture-import-running"
        running_marker.touch()
        threading.Thread(
            target=run_import_task, kwargs={"running_marker": running_marker}, daemon=True
        ).start()
        result = {"status": "started"}
    elif method == "backup.importSnapshotStatus":
        # §4.68 status face: the import-task registry is process memory,
        # mirrored by the `.fixture-import-running` / `.fixture-import-failed`
        # markers; `ndjsonPresent` is the archive's disk truth and the counts
        # mirror the fixture archive's fixed shape (4 projected rows, 5 frames
        # including the BackupInfo meta-frame).
        account_dir = None
        try:
            state = json.loads((SIGNAL_DATA_DIR / "engine-state.json").read_text())
            for entry in state.get("accounts") or []:
                if entry.get("number") == params.get("account"):
                    account_dir = SIGNAL_DATA_DIR / entry["dir"]
                    break
        except (OSError, ValueError, KeyError, AttributeError):
            account_dir = None
        if account_dir is None:
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32603, "message": "account is not registered"},
            }
            emit_json(response)
            continue
        archive = account_dir / "history-import.ndjson"
        ndjson_present = archive.is_file() and archive.stat().st_size > 0
        if (SIGNAL_DATA_DIR / ".fixture-import-running").exists():
            result = {"state": "running", "ndjsonPresent": ndjson_present}
        elif (SIGNAL_DATA_DIR / ".fixture-import-failed").exists():
            result = {
                "state": "failed",
                "ndjsonPresent": ndjson_present,
                "error": "fixture import failed",
            }
        elif ndjson_present:
            result = {
                "state": "completed",
                "ndjsonPresent": True,
                "finishedAtMs": int(time.time() * 1000),
                "lines": 4,
                "frames": 5,
            }
        else:
            result = {"state": "idle", "ndjsonPresent": False}
    elif method == "transfer.downloadStatus":
        # §4.38 progress face (contract 1.53): the real engine answers from an
        # in-process per-account download snapshot. The fixture mirrors that
        # shape from marker files in the signal data dir —
        # `.fixture-transfer-running` (present = a download is in flight) and
        # `.fixture-transfer-bytes` ("<read> [<total>]", total optional), both
        # written by the driving test. Absent markers answer {present: false},
        # the honest no-download answer.
        result = {"present": False}
        running_marker = SIGNAL_DATA_DIR / ".fixture-transfer-running"
        bytes_marker = SIGNAL_DATA_DIR / ".fixture-transfer-bytes"
        if running_marker.exists():
            try:
                parts = bytes_marker.read_text().split()
                bytes_read = max(int(parts[0]), 0)
            except (OSError, ValueError, IndexError):
                parts = []
                bytes_read = 0
            result = {"present": True, "bytesRead": bytes_read}
            if len(parts) > 1:
                result["bytesTotal"] = max(int(parts[1]), 0)
    elif method == "backups.restoreFromCloud":
        # §4.69 cloud-restore start face (contract 1.83): the negotiated wire
        # mirror of the engine's KT-extension method — the params face is only
        # {account}, the happy path answers {"status": "started"} (the whole
        # chain runs in the engine's background), and the seven-family
        # structured-error ladder steers from markers the driving test writes:
        # `.fixture-cloud-restore-running` present = the engine's atomic
        # re-entry gate (BACKUP_RUNNING, no detail); `.fixture-cloud-restore-error`
        # carries a family sentinel — NOT_PROVISIONED / AUTH / TRANSPORT /
        # DOWNLOAD / DECRYPT / EXPORT — mapped onto the BACKUP_* code, where
        # the four message-carrying families additionally carry data.detail
        # (the engine's CloudRestoreError::rpc_code table; the three state
        # families answer content-free).
        #
        # §4.69 consumption batch (contract 1.84): the happy path also runs
        # the background chain the connector's consumption watcher watches —
        # after a short delay it either writes the NDJSON product
        # `history-import-cloud.ndjson` into the account directory and clears
        # the cloud-status snapshot (the engine's success-clear discipline),
        # or — steered by markers — stalls forever (`.fixture-cloud-restore-stall`,
        # the driving test owns the status face) or flips the snapshot to the
        # failed phase (`.fixture-cloud-restore-fails-later`).
        cloud_error = None
        error_marker = SIGNAL_DATA_DIR / ".fixture-cloud-restore-error"
        if (SIGNAL_DATA_DIR / ".fixture-cloud-restore-running").exists():
            cloud_error = "BACKUP_RUNNING"
        elif error_marker.exists():
            sentinel = error_marker.read_text().strip()
            cloud_error = {
                "NOT_PROVISIONED": "BACKUP_NOT_PROVISIONED",
                "AUTH": "BACKUP_AUTH_FAILED",
                "TRANSPORT": "BACKUP_TRANSPORT_FAILED",
                "DOWNLOAD": "BACKUP_DOWNLOAD_FAILED",
                "DECRYPT": "BACKUP_DECRYPT_FAILED",
                "EXPORT": "BACKUP_EXPORT_FAILED",
            }.get(sentinel)
        cloud_account_dir = None
        try:
            state = json.loads((SIGNAL_DATA_DIR / "engine-state.json").read_text())
            for entry in state.get("accounts") or []:
                if entry.get("number") == params.get("account"):
                    cloud_account_dir = SIGNAL_DATA_DIR / entry["dir"]
                    break
        except (OSError, ValueError, KeyError, AttributeError):
            cloud_account_dir = None
        if cloud_account_dir is None:
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32603, "message": "account is not registered"},
            }
            emit_json(response)
            continue
        if cloud_error is not None:
            response = {"jsonrpc": "2.0", "id": request_id, "error": {"code": -1, "message": cloud_error}}
            if cloud_error in (
                "BACKUP_TRANSPORT_FAILED",
                "BACKUP_DOWNLOAD_FAILED",
                "BACKUP_DECRYPT_FAILED",
                "BACKUP_EXPORT_FAILED",
            ):
                response["error"]["data"] = {"detail": f"fixture {cloud_error}"}
            emit_json(response)
            continue

        def run_cloud_restore_task(account_dir=cloud_account_dir):
            time.sleep(0.2)
            status_marker = SIGNAL_DATA_DIR / ".fixture-cloud-status"
            if (SIGNAL_DATA_DIR / ".fixture-cloud-restore-stall").exists():
                return
            if (SIGNAL_DATA_DIR / ".fixture-cloud-restore-fails-later").exists():
                status_marker.write_text("failed 4096 BACKUP_DOWNLOAD_FAILED")
                return
            recipient = (
                '{"type":"recipient","id":2,"kind":"contact","name":"Cloud Import",'
                '"aci":"' + IMPORT_ALICE_ACI + '","e164":"' + IMPORT_ALICE_E164 + '"}'
            )
            chat = '{"type":"chat","id":20,"recipientId":2}'
            incoming = (
                '{"type":"message","chatId":20,"authorId":2,"dateSent":5000,'
                '"direction":"incoming","text":"cloud archived hello"}'
            )
            outgoing = (
                '{"type":"message","chatId":20,"authorId":2,"dateSent":6000,'
                '"direction":"outgoing","text":"cloud archived reply"}'
            )
            (account_dir / "history-import-cloud.ndjson").write_text(
                recipient + "\n" + chat + "\n" + incoming + "\n" + outgoing + "\n"
            )
            status_marker.unlink(missing_ok=True)

        threading.Thread(target=run_cloud_restore_task, daemon=True).start()
        result = {"status": "started"}
    elif method == "backups.cloudStatus":
        # §4.69 cloud-status face (contract 1.83): the progress snapshot is
        # engine-process memory; the fixture mirrors the three negotiated
        # states from `.fixture-cloud-status` ("<phase> <read> [<total|code>]",
        # written by the driving test) — absent/empty answers {present: false},
        # `downloading <read> [total]` carries the byte counts (total absent =
        # the honest-uncertainty form, never a fake 0), and
        # `failed <read> <code>` carries the structured errorCode.
        result = {"present": False}
        status_marker = SIGNAL_DATA_DIR / ".fixture-cloud-status"
        if status_marker.exists():
            parts = status_marker.read_text().split()
            if len(parts) >= 2 and parts[0] in ("downloading", "failed"):
                result = {
                    "present": True,
                    "phase": parts[0],
                    "bytesRead": max(int(parts[1]), 0),
                }
                if len(parts) > 2:
                    if parts[0] == "downloading":
                        result["bytesTotal"] = max(int(parts[2]), 0)
                    else:
                        result["errorCode"] = parts[2]
    elif method == "backups.uploadToCloud":
        # §4.72 cloud-upload start face (contract 1.87, engine
        # cloud-backup-contract §6.2): the negotiated wire mirror of the
        # engine's KT-extension method — the params face is only {account},
        # the happy path answers {"status": "started"} (the whole chain runs
        # in the engine's background), and the eight-family structured-error
        # ladder steers from markers the driving test writes:
        # `.fixture-cloud-upload-running` present = the engine's process-wide
        # single-slot re-entry gate (BACKUP_UPLOAD_ALREADY_RUNNING, no
        # detail); `.fixture-cloud-upload-error` carries a family sentinel —
        # AUTH / NOT_PROVISIONED / ARTIFACT_MISSING / ARTIFACT_INVALID /
        # TRANSPORT / UPLOAD / IO — mapped onto the BACKUP_* code, where the
        # three message-carrying families (TRANSPORT/UPLOAD/IO, the engine's
        # detail() Some set) additionally carry data.detail and the five
        # state families answer content-free. Without a steering marker the
        # arm mirrors the engine's start artifact gate honestly: the
        # §10.2 input artifact (`backup-export-input.ndjson`) must exist in
        # the account directory — the connector builds it ahead of the
        # start call, so a happy path through the real connector always
        # lands it and its absence answers BACKUP_ARTIFACT_MISSING.
        upload_error = None
        upload_error_marker = SIGNAL_DATA_DIR / ".fixture-cloud-upload-error"
        if (SIGNAL_DATA_DIR / ".fixture-cloud-upload-running").exists():
            upload_error = "BACKUP_UPLOAD_ALREADY_RUNNING"
        elif upload_error_marker.exists():
            sentinel = upload_error_marker.read_text().strip()
            upload_error = {
                "AUTH": "BACKUP_AUTH_FAILED",
                "NOT_PROVISIONED": "BACKUP_NOT_PROVISIONED",
                "ARTIFACT_MISSING": "BACKUP_ARTIFACT_MISSING",
                "ARTIFACT_INVALID": "BACKUP_ARTIFACT_INVALID",
                "TRANSPORT": "BACKUP_TRANSPORT_FAILED",
                "UPLOAD": "BACKUP_UPLOAD_FAILED",
                "IO": "BACKUP_IO_FAILED",
            }.get(sentinel)
        upload_account_dir = None
        try:
            state = json.loads((SIGNAL_DATA_DIR / "engine-state.json").read_text())
            for entry in state.get("accounts") or []:
                if entry.get("number") == params.get("account"):
                    upload_account_dir = SIGNAL_DATA_DIR / entry["dir"]
                    break
        except (OSError, ValueError, KeyError, AttributeError):
            upload_account_dir = None
        if upload_account_dir is None:
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32603, "message": "account is not registered"},
            }
            emit_json(response)
            continue
        if upload_error is None and not (
            upload_account_dir / "backup-export-input.ndjson"
        ).is_file():
            upload_error = "BACKUP_ARTIFACT_MISSING"
        if upload_error is not None:
            response = {"jsonrpc": "2.0", "id": request_id, "error": {"code": -1, "message": upload_error}}
            if upload_error in (
                "BACKUP_TRANSPORT_FAILED",
                "BACKUP_UPLOAD_FAILED",
                "BACKUP_IO_FAILED",
            ):
                response["error"]["data"] = {"detail": f"fixture {upload_error}"}
            emit_json(response)
            continue
        result = {"status": "started"}
    elif method == "backups.uploadStatus":
        # §4.72 cloud-upload status face (contract 1.87): the progress
        # snapshot is engine-process memory (a process-wide single slot);
        # the fixture mirrors the negotiated states from
        # `.fixture-cloud-upload-status` ("<phase> <read> [<total|code>]",
        # written by the driving test) — absent/empty answers
        # {present: false}, `exporting 0` carries the zero-count assembly
        # face, `uploading <read> [total]` carries the block counts (total
        # absent = the honest-uncertainty form, never a fake 0), and
        # `failed <read> <code>` carries the structured errorCode.
        result = {"present": False}
        upload_status_marker = SIGNAL_DATA_DIR / ".fixture-cloud-upload-status"
        if upload_status_marker.exists():
            parts = upload_status_marker.read_text().split()
            if len(parts) >= 2 and parts[0] in ("exporting", "uploading", "failed"):
                result = {
                    "present": True,
                    "phase": parts[0],
                    "bytesRead": max(int(parts[1]), 0),
                }
                if len(parts) > 2:
                    if parts[0] == "uploading":
                        result["bytesTotal"] = max(int(parts[2]), 0)
                    elif parts[0] == "failed":
                        result["errorCode"] = parts[2]
    elif method in (
        "calls.linkPresentation",
        "calls.createLinkAuth",
        "calls.groupProof",
        "calls.groupMembers",
        "calls.endorsementsPublicKey",
    ):
        # §4.73 call-link credential material faces (contract 1.88, engine
        # a3ae39b): the negotiated wire mirror of the engine's five
        # KT-extension methods — the account-scoped four gate the account
        # (engine-state registry, else -32603) and their material shapes
        # fail-closed -32603 (rootKey standard b64 16/21 B, roomId/groupId
        # 32 B — the engine's own parameter gates), endorsementsPublicKey
        # takes no fields and never fails here. The structured ladder steers
        # from markers the driving test writes:
        # `.fixture-call-cred-failed` answers CALL_CREDENTIALS_FAILED —
        # "status <N>" content carries error.data.status (the honest HTTP
        # pass-through), anything else answers data {} (the engine's
        # no-status form); `.fixture-call-cred-unavailable` answers
        # CALL_CREDENTIALS_UNAVAILABLE. Happy paths answer the fake shape
        # stubs (fixed placeholder bytes/proof/members — stub bytes are NOT
        # valid zk material, they only pin the contract shape, the engine
        # fake discipline): presentation/params are the standard base64 of
        # fixed byte strings, proof is a fixed token, members one entry.
        def _call_cred_error():
            failed_marker = SIGNAL_DATA_DIR / ".fixture-call-cred-failed"
            if failed_marker.exists():
                text = failed_marker.read_text().strip()
                data = {}
                if text.startswith("status "):
                    try:
                        data["status"] = max(int(text.split()[1]), 0)
                    except (ValueError, IndexError):
                        pass
                return {"code": -1, "message": "CALL_CREDENTIALS_FAILED", "data": data}
            if (SIGNAL_DATA_DIR / ".fixture-call-cred-unavailable").exists():
                return {"code": -1, "message": "CALL_CREDENTIALS_UNAVAILABLE"}
            return None

        def _b64_fixed(tag):
            return base64.b64encode(bytes([0xF0 ^ tag]) * 16).decode()

        if method != "calls.endorsementsPublicKey":
            cred_account = None
            try:
                state = json.loads((SIGNAL_DATA_DIR / "engine-state.json").read_text())
                for entry in state.get("accounts") or []:
                    if entry.get("number") == params.get("account"):
                        cred_account = entry["dir"]
                        break
            except (OSError, ValueError, KeyError, AttributeError):
                cred_account = None
            if cred_account is None:
                response = {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": {"code": -32603, "message": "account is not registered"},
                }
                emit_json(response)
                continue

            def _decode_exact(value, lengths):
                try:
                    raw = base64.b64decode(value.strip(), validate=True)
                except Exception:
                    return False
                return len(raw) in lengths

            if method in ("calls.linkPresentation", "calls.createLinkAuth"):
                if not _decode_exact(params.get("rootKey") or "", (16, 21)):
                    response = {
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "error": {"code": -32603, "message": "Internal error"},
                    }
                    emit_json(response)
                    continue
            if method == "calls.createLinkAuth" and not _decode_exact(
                params.get("roomId") or "", (32,)
            ):
                response = {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": {"code": -32603, "message": "Internal error"},
                }
                emit_json(response)
                continue
            if method in ("calls.groupProof", "calls.groupMembers") and not _decode_exact(
                params.get("groupId") or "", (32,)
            ):
                response = {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": {"code": -32603, "message": "Internal error"},
                }
                emit_json(response)
                continue

        cred_error = _call_cred_error()
        if cred_error is not None:
            response = {"jsonrpc": "2.0", "id": request_id, "error": cred_error}
            emit_json(response)
            continue
        result = {
            "calls.linkPresentation": {"presentation": _b64_fixed(1)},
            "calls.createLinkAuth": {
                "presentation": _b64_fixed(2),
                "callLinkPublicParams": _b64_fixed(3),
            },
            "calls.groupProof": {"proof": "fixture-group-proof-token"},
            "calls.groupMembers": {
                "members": [
                    {
                        "uuid": "ae45f0d7-9c15-4e0a-9bf5-6cb610e1f2a3",
                        "userIdCipherText": _b64_fixed(4),
                    }
                ]
            },
            "calls.endorsementsPublicKey": {"endorsementPublicKey": _b64_fixed(5)},
        }[method]
    elif method == "generateInviteLinkPassword":
        # Contract 1.117 §4.98: the account-less material face. No dispatch
        # record (the engine's material faces never log), no account: 16
        # CSPRNG bytes as standard base64 — the official
        # GROUP_INVITE_LINK_PASSWORD_LENGTH shape.
        result = {"password": base64.b64encode(os.urandom(16)).decode()}
    elif method == "buildInviteLink":
        # Contract 1.117 §4.98: the build face, read side, no dispatch
        # record. Engine-parity parameter gates: groupId standard base64 of
        # exactly 32 bytes, password (when present) standard base64 decoding
        # to a non-empty byte string — anything else answers -32603. The
        # explicit-password arm is pure construction (a real proto link, the
        # engine fake/real shared code path); the password-absent arm
        # mirrors the engine fake's no-group-state face —
        # GROUP_INVITE_LINK_UNAVAILABLE with no data. The
        # LINK_PASSWORD_FORBIDDEN bytes answer the same code's data.status
        # face (the server-rejection channel).
        try:
            master_key = base64.b64decode(params.get("groupId") or "", validate=True)
            password_raw = params.get("password")
            password_bytes = (
                None
                if password_raw is None
                else base64.b64decode(password_raw, validate=True)
            )
        except Exception:
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32603, "message": "Internal error"},
            }
            emit_json(response)
            continue
        if len(master_key) != 32 or password_bytes == b"":
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32603, "message": "Internal error"},
            }
            emit_json(response)
            continue
        if password_bytes == LINK_PASSWORD_FORBIDDEN:
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {
                    "code": -1,
                    "message": "GROUP_INVITE_LINK_UNAVAILABLE",
                    "data": {"status": 403},
                },
            }
            emit_json(response)
            continue
        if password_bytes is None:
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -1, "message": "GROUP_INVITE_LINK_UNAVAILABLE"},
            }
            emit_json(response)
            continue
        result = {"inviteLink": fixture_build_invite_link(master_key, password_bytes)}
    elif method == "parseInviteLink":
        # Contract 1.117 §4.98: the account-less pure-parse face. Every
        # tolerance failure (empty, bad base64, bad proto, versionless,
        # short key, empty password) answers -32603 — the engine folds the
        # whole parse family there.
        parsed_link = fixture_parse_invite_link(params.get("inviteLink") or "")
        if parsed_link is None:
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32603, "message": "Internal error"},
            }
            emit_json(response)
            continue
        master_key, link_password = parsed_link
        result = {
            "groupId": base64.b64encode(master_key).decode(),
            "password": base64.b64encode(link_password).decode(),
        }
    elif method == "previewGroupViaLink":
        # Contract 1.117 §4.98: the pre-join read face, no dispatch record.
        # The `.fixture-invite-link-unavailable` marker answers
        # STORAGE_UNAVAILABLE for all four account-scoped faces. Parse
        # failure answers -32603; the LINK_PASSWORD_PREVIEW_FAILED bytes
        # answer GROUP_INVITE_LINK_PREVIEW_FAILED with data.status (the
        # server-rejection channel); the happy path answers the full
        # 8-key projection (both optional profile keys present).
        if (SIGNAL_DATA_DIR / ".fixture-invite-link-unavailable").exists():
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -1, "message": "STORAGE_UNAVAILABLE"},
            }
            emit_json(response)
            continue
        parsed_link = fixture_parse_invite_link(params.get("inviteLink") or "")
        if parsed_link is None:
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32603, "message": "Internal error"},
            }
            emit_json(response)
            continue
        master_key, link_password = parsed_link
        if link_password == LINK_PASSWORD_PREVIEW_FAILED:
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {
                    "code": -1,
                    "message": "GROUP_INVITE_LINK_PREVIEW_FAILED",
                    "data": {"status": 404},
                },
            }
            emit_json(response)
            continue
        result = {
            "groupId": base64.b64encode(master_key).decode(),
            "memberCount": 12,
            "revision": 7,
            "addFromInviteLink": "ANY",
            "approvalRequired": False,
            "pendingAdminApproval": False,
            "name": "Fixture Link Group",
            "description": "previewed through the invite link",
        }
    elif method == "joinGroupViaLink":
        # Contract 1.117 §4.98: the join write face — the engine appends
        # the dispatch record BEFORE the parse (a refused write still
        # records its dispatch), so the fixture logs first. The
        # LINK_PASSWORD_PREVIEW_FAILED bytes answer the GROUP_UPDATE_FAILED
        # data.status face; the LINK_PASSWORD_JOIN_REQUESTED bytes answer
        # the approval-gate verb; every other parseable link joins.
        with WRITE_LOCK:
            with SEND_LOG.open("a") as log:
                log.write(json.dumps(params, separators=(",", ":")) + "\n")
        if (SIGNAL_DATA_DIR / ".fixture-invite-link-unavailable").exists():
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -1, "message": "STORAGE_UNAVAILABLE"},
            }
            emit_json(response)
            continue
        parsed_link = fixture_parse_invite_link(params.get("inviteLink") or "")
        if parsed_link is None:
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32603, "message": "Internal error"},
            }
            emit_json(response)
            continue
        _, link_password = parsed_link
        if link_password == LINK_PASSWORD_PREVIEW_FAILED:
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {
                    "code": -1,
                    "message": "GROUP_UPDATE_FAILED",
                    "data": {"status": 403},
                },
            }
            emit_json(response)
            continue
        status = (
            "requested" if link_password == LINK_PASSWORD_JOIN_REQUESTED else "joined"
        )
        result = {"status": status}
    elif method == "cancelJoinRequest":
        # Contract 1.117 §4.98: the withdrawal write face — the same
        # record-before-parse discipline as the join; the
        # LINK_PASSWORD_PREVIEW_FAILED bytes answer GROUP_UPDATE_FAILED
        # data.status=409; every other parseable link cancels.
        with WRITE_LOCK:
            with SEND_LOG.open("a") as log:
                log.write(json.dumps(params, separators=(",", ":")) + "\n")
        if (SIGNAL_DATA_DIR / ".fixture-invite-link-unavailable").exists():
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -1, "message": "STORAGE_UNAVAILABLE"},
            }
            emit_json(response)
            continue
        parsed_link = fixture_parse_invite_link(params.get("inviteLink") or "")
        if parsed_link is None:
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32603, "message": "Internal error"},
            }
            emit_json(response)
            continue
        _, link_password = parsed_link
        if link_password == LINK_PASSWORD_PREVIEW_FAILED:
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {
                    "code": -1,
                    "message": "GROUP_UPDATE_FAILED",
                    "data": {"status": 409},
                },
            }
            emit_json(response)
            continue
        result = {"status": "cancelled"}
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
        if UNREGISTERED_MARKER.exists():
            # Post-mortem unlink form (contract 1.21 §4.17, C2 widening):
            # after the first authorization failure signal-cli persists
            # registered=false and every later account-scoped command fails
            # LOCALLY with UserErrorException "User <account> is not
            # registered." (USER_ERROR = -1) before any network call — the
            # 0-17 ms instant-failure form. Must classify as Unauthorized
            # exactly like the live form above.
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {
                    "code": -1,
                    "message": f"User {LINKED_ACCOUNT} is not registered.",
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
        if params.get("usernames") is not None:
            # Contract 1.59 §4.55 username lookup: the --username face answers
            # a top-level array of JsonUserStatus records. First entry
            # resolves registered (with number+username+uuid), the rest are
            # the unregistered face; a rate-limit sentinel entry answers the
            # upstream CDSI RateLimitException mapping (-1, retryAfter
            # message).
            if "[fixture-username-ratelimited]" in params["usernames"]:
                response = {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": {
                        "code": -1,
                        "message": "Rate limit exceeded; try again after 30 seconds (RateLimitException)",
                    },
                }
                emit_json(response)
                continue
            entries = []
            for index, name in enumerate(params["usernames"]):
                if index == 0:
                    entries.append(
                        {
                            "recipient": name,
                            "number": "+15551230001",
                            "username": name,
                            "uuid": "00000000-0000-0000-0000-00000000000" + str(index % 10),
                            "isRegistered": True,
                        }
                    )
                else:
                    entries.append({"recipient": name, "isRegistered": False})
            result = entries
        else:
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
    if method == "slowRead":
        # Slow read answer (same 0.35s cadence as the mutating slow family):
        # the result lands after a short connector request timeout, so the
        # read-only outcome is a deadline Timeout and the late answer must be
        # dropped as an unknown id, never misattributed to a later request.
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
    if method == "emitGroupUpdated":
        # The emitReceive twin for the groupUpdated projection: emitted after
        # the response, same receive-notification channel.
        update = {
            "groupId": params.get("groupId") or "ZmFrZS1ncm91cC0x",
            "revision": params.get("revision", 1),
            "expireTimerSeconds": params.get("expireTimerSeconds", 0),
        }
        if "editor" in params and params.get("editor") is not None:
            update["editor"] = params.get("editor")
        emit_json({
            "jsonrpc": "2.0",
            "method": "receive",
            "params": {
                "account": receiving_account(),
                "envelope": {
                    "timestamp": int(time.time() * 1000),
                    "groupUpdated": update,
                },
            },
        })
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
