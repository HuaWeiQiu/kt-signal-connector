// SPDX-License-Identifier: AGPL-3.0-only

#![cfg(unix)]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use serde_json::{Value, json};
use sha2::Sha256;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::process::{Child, Command};
use tokio::time::{sleep, timeout};
use tokio_util::codec::{Framed, LinesCodec};

const API_VERSION: &str = "1.0";

/// Phase 3 contract: every spawned connector gets its store key as bootstrap
/// payload line 2. One fixed test key keeps respawns against the same state
/// directory compatible with the database the first spawn encrypted.
const TEST_STORE_KEY: [u8; 32] = [0x5A; 32];

/// The two-line bootstrap payload: line 1 handshake secret, line 2 store key.
fn bootstrap_payload(secret: &[u8; 32]) -> String {
    format!("{}\n{}", hex::encode(secret), hex::encode(TEST_STORE_KEY))
}

#[tokio::test]
async fn binary_serves_authenticated_runtime_lifecycle() {
    let temp = TempDir::new().unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let endpoint = temp.path().join("connector.sock");
    let secret = [7_u8; 32];

    let mut connector = spawn_connector_from_stdin(temp.path(), &endpoint, &secret).await;
    wait_for_path(&endpoint).await;
    assert_eq!(
        fs::metadata(&endpoint).unwrap().permissions().mode() & 0o777,
        0o600
    );

    let stream = UnixStream::connect(&endpoint).await.unwrap();
    let mut unauthenticated = Framed::new(stream, LinesCodec::new());
    reject_authentication(&mut unauthenticated).await;
    drop(unauthenticated);

    let stream = UnixStream::connect(&endpoint).await.unwrap();
    let mut client = Framed::new(stream, LinesCodec::new());
    authenticate(&mut client, &secret).await;

    let initial = request(&mut client, "status-1", "runtime.status", json!({})).await;
    assert_eq!(initial["result"]["state"], "stopped");

    let started = request(&mut client, "start-1", "runtime.start", json!({})).await;
    assert_eq!(started["result"]["state"], "running");
    let first_engine_pid = started["result"]["pid"].as_u64().unwrap() as u32;

    drop(client);
    wait_for_process_exit(first_engine_pid).await;
    assert_clean_exit(&mut connector).await;

    let next_secret_file = temp.path().join("bootstrap-next.secret");
    let next_secret = [8_u8; 32];
    write_secret_file(&next_secret_file, &next_secret);
    connector = spawn_connector(temp.path(), &endpoint, &next_secret_file);
    wait_for_path(&endpoint).await;
    let stream = UnixStream::connect(&endpoint).await.unwrap();
    let mut client = Framed::new(stream, LinesCodec::new());
    authenticate(&mut client, &next_secret).await;
    let reconnected = request(&mut client, "status-2", "runtime.status", json!({})).await;
    assert_eq!(reconnected["result"]["state"], "stopped");

    let restarted = request(&mut client, "start-2", "runtime.start", json!({})).await;
    assert_eq!(restarted["result"]["state"], "running");

    let stopped = request(&mut client, "stop-2", "runtime.stop", json!({})).await;
    assert_eq!(stopped["result"]["state"], "stopped");

    drop(client);
    assert_clean_exit(&mut connector).await;
}

#[tokio::test]
async fn phase2_link_receive_send_and_idempotent_text() {
    let temp = TempDir::new().unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let endpoint = temp.path().join("connector.sock");
    let secret_file = temp.path().join("bootstrap.secret");
    let secret = [7_u8; 32];
    fs::write(&secret_file, bootstrap_payload(&secret)).unwrap();
    fs::set_permissions(&secret_file, fs::Permissions::from_mode(0o600)).unwrap();

    let mut connector = spawn_connector(temp.path(), &endpoint, &secret_file);
    wait_for_path(&endpoint).await;

    let stream = UnixStream::connect(&endpoint).await.unwrap();
    let mut client = Framed::new(stream, LinesCodec::new());
    authenticate(&mut client, &secret).await;

    let started = request(&mut client, "start-1", "runtime.start", json!({})).await;
    assert_eq!(started["result"]["state"], "running");
    let mut engine_pid = started["result"]["pid"].as_u64().unwrap() as u32;

    let slow_link = request(
        &mut client,
        "link-slow-start",
        "link.start",
        json!({ "deviceName": "[slow-link-test]" }),
    )
    .await;
    let slow_link_session_id = slow_link["result"]["linkSessionId"]
        .as_str()
        .unwrap()
        .to_string();
    send_request_frame(
        &mut client,
        "link-slow-finish",
        "link.finish",
        json!({ "linkSessionId": slow_link_session_id }),
    )
    .await;
    let duplicate_finish = request(
        &mut client,
        "link-duplicate-finish",
        "link.finish",
        json!({ "linkSessionId": slow_link_session_id }),
    )
    .await;
    assert_eq!(duplicate_finish["error"]["code"], "LINK_IN_PROGRESS");
    send_request_frame(
        &mut client,
        "link-slow-cancel",
        "link.cancel",
        json!({ "linkSessionId": slow_link_session_id }),
    )
    .await;

    let mut slow_finish_cancelled = false;
    let mut slow_cancelled = false;
    timeout(Duration::from_secs(3), async {
        while !slow_finish_cancelled || !slow_cancelled {
            let response: Value =
                serde_json::from_str(&client.next().await.unwrap().unwrap()).unwrap();
            match response.get("requestId").and_then(Value::as_str) {
                Some("link-slow-finish") => {
                    assert_eq!(response["error"]["code"], "LINK_CANCELLED");
                    slow_finish_cancelled = true;
                }
                Some("link-slow-cancel") => {
                    assert!(response.get("result").is_some());
                    slow_cancelled = true;
                }
                _ => {}
            }
        }
    })
    .await
    .expect("link.cancel must not wait for phone approval");

    wait_for_process_exit(engine_pid).await;
    let after_cancel = request(
        &mut client,
        "status-after-link-cancel",
        "runtime.status",
        json!({}),
    )
    .await;
    assert_eq!(after_cancel["result"]["state"], "running");
    engine_pid = after_cancel["result"]["pid"].as_u64().unwrap() as u32;

    let link = request(
        &mut client,
        "link-1",
        "link.start",
        json!({ "deviceName": "KT-Test" }),
    )
    .await;
    assert!(link["result"]["linkSessionId"].as_str().is_some());
    assert!(
        link["result"]["qrPayload"]
            .as_str()
            .unwrap()
            .starts_with("sgnl://")
    );
    let link_session_id = link["result"]["linkSessionId"]
        .as_str()
        .unwrap()
        .to_string();

    let conflict = request(
        &mut client,
        "link-2",
        "link.start",
        json!({ "deviceName": "KT-Test-2" }),
    )
    .await;
    assert_eq!(conflict["error"]["code"], "LINK_IN_PROGRESS");

    let finished = request(
        &mut client,
        "link-3",
        "link.finish",
        json!({ "linkSessionId": link_session_id }),
    )
    .await;
    assert_eq!(finished["result"]["state"], "ready");
    assert!(finished["result"]["maskedAddress"].as_str().is_some());
    assert!(
        !finished["result"]["maskedAddress"]
            .as_str()
            .unwrap()
            .contains("555555")
    );
    let account_id = finished["result"]["id"].as_str().unwrap().to_string();

    let accounts = request(&mut client, "accounts-1", "accounts.list", json!({})).await;
    assert_eq!(accounts["result"].as_array().unwrap().len(), 1);

    // Fixture finishLink emits one receive notification after the JSON-RPC result.
    sleep(Duration::from_millis(50)).await;
    let conversations = request(
        &mut client,
        "conv-2",
        "conversations.list",
        json!({ "accountId": account_id, "limit": 50 }),
    )
    .await;
    assert_eq!(
        conversations["result"]["items"].as_array().unwrap().len(),
        1
    );
    let conversation_id = conversations["result"]["items"][0]["id"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(conversations["result"]["items"][0]["type"], "direct");

    let messages = request(
        &mut client,
        "msg-1",
        "messages.list",
        json!({
            "accountId": account_id,
            "conversationId": conversation_id,
            "limit": 50
        }),
    )
    .await;
    assert_eq!(messages["result"]["items"].as_array().unwrap().len(), 1);
    assert_eq!(messages["result"]["items"][0]["text"], "private text");
    assert_eq!(messages["result"]["items"][0]["direction"], "incoming");

    let sent = request(
        &mut client,
        "send-1",
        "messages.sendText",
        json!({
            "accountId": account_id,
            "conversationId": conversation_id,
            "text": "hello from kt",
            "clientRequestId": "client-req-1"
        }),
    )
    .await;
    assert_eq!(sent["result"]["status"], "sent");
    assert_eq!(sent["result"]["text"], "hello from kt");
    assert_eq!(sent["result"]["clientRequestId"], "client-req-1");
    let message_id = sent["result"]["id"].as_str().unwrap().to_string();

    let sent_again = request(
        &mut client,
        "send-2",
        "messages.sendText",
        json!({
            "accountId": account_id,
            "conversationId": conversation_id,
            "text": "hello from kt",
            "clientRequestId": "client-req-1"
        }),
    )
    .await;
    assert_eq!(sent_again["result"]["id"], message_id);
    assert_eq!(sent_again["result"]["status"], "sent");

    let messages = request(
        &mut client,
        "msg-2",
        "messages.list",
        json!({
            "accountId": account_id,
            "conversationId": conversation_id,
            "limit": 50
        }),
    )
    .await;
    assert_eq!(messages["result"]["items"].as_array().unwrap().len(), 2);

    // Host dispatch is concurrent: a persisted read must not wait for a slow
    // upstream send, while same-account sends retain request order.
    send_request_frame(
        &mut client,
        "send-slow",
        "messages.sendText",
        json!({
            "accountId": account_id,
            "conversationId": conversation_id,
            "text": "[slow-host-test]",
            "clientRequestId": "client-req-slow"
        }),
    )
    .await;
    send_request_frame(
        &mut client,
        "send-after-slow",
        "messages.sendText",
        json!({
            "accountId": account_id,
            "conversationId": conversation_id,
            "text": "after slow",
            "clientRequestId": "client-req-after-slow"
        }),
    )
    .await;
    send_request_frame(
        &mut client,
        "msg-concurrent",
        "messages.list",
        json!({
            "accountId": account_id,
            "conversationId": conversation_id,
            "limit": 50
        }),
    )
    .await;

    let mut response_order = Vec::new();
    while response_order.len() < 3 {
        let response: Value = serde_json::from_str(&client.next().await.unwrap().unwrap()).unwrap();
        if let Some(request_id) = response.get("requestId").and_then(Value::as_str) {
            if matches!(
                request_id,
                "send-slow" | "send-after-slow" | "msg-concurrent"
            ) {
                response_order.push(request_id.to_string());
            }
        }
    }
    assert_eq!(
        response_order,
        ["msg-concurrent", "send-slow", "send-after-slow"]
    );

    // Account-scoped host admission is bounded independently of frame input.
    for index in 0..33 {
        send_request_frame(
            &mut client,
            &format!("capacity-{index}"),
            "messages.sendText",
            json!({
                "accountId": account_id,
                "conversationId": conversation_id,
                "text": if index == 0 { "[slow-host-test]" } else { "queued" },
                "clientRequestId": format!("capacity-client-{index}")
            }),
        )
        .await;
    }
    loop {
        let response: Value = serde_json::from_str(&client.next().await.unwrap().unwrap()).unwrap();
        if response.get("requestId").and_then(Value::as_str) == Some("capacity-32") {
            assert_eq!(response["error"]["code"], "INTERNAL_ERROR");
            assert_eq!(
                response["error"]["message"],
                "connector request capacity exceeded"
            );
            break;
        }
    }

    drop(client);
    wait_for_process_exit(engine_pid).await;
    assert_clean_exit(&mut connector).await;

    // Disconnect shutdown must resolve a dispatched mutation to unknown before
    // the connection task is dropped. A fresh one-shot connector and secret read
    // the persisted fact only; no retry or second send is issued.
    let next_secret_file = temp.path().join("bootstrap-after-disconnect.secret");
    let next_secret = [9_u8; 32];
    write_secret_file(&next_secret_file, &next_secret);
    connector = spawn_connector(temp.path(), &endpoint, &next_secret_file);
    wait_for_path(&endpoint).await;
    let stream = UnixStream::connect(&endpoint).await.unwrap();
    let mut client = Framed::new(stream, LinesCodec::new());
    authenticate(&mut client, &next_secret).await;
    let after_disconnect = request(
        &mut client,
        "messages-after-disconnect",
        "messages.list",
        json!({
            "accountId": account_id,
            "conversationId": conversation_id,
            "limit": 50
        }),
    )
    .await;
    let slow_statuses = after_disconnect["result"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|message| message["text"] == "[slow-host-test]")
        .map(|message| message["status"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert!(slow_statuses.contains(&"sent"));
    assert!(slow_statuses.contains(&"unknown"));

    let restarted = request(&mut client, "start-for-delete", "runtime.start", json!({})).await;
    assert_eq!(restarted["result"]["state"], "running");

    let next_link = request(
        &mut client,
        "link-next-account",
        "link.start",
        json!({ "deviceName": "KT-Next" }),
    )
    .await;
    let next_link_session_id = next_link["result"]["linkSessionId"]
        .as_str()
        .unwrap()
        .to_string();

    let invalid_delete = request(
        &mut client,
        "delete-invalid-params",
        "accounts.deleteLocalData",
        json!({ "accountId": account_id, "unexpected": true }),
    )
    .await;
    assert_eq!(invalid_delete["error"]["code"], "INVALID_REQUEST");

    let compatible_delete = request(
        &mut client,
        "delete-v1-compatible",
        "accounts.deleteLocalData",
        json!({ "accountId": "already-absent-account" }),
    )
    .await;
    assert!(
        compatible_delete.get("result").is_some(),
        "v1-compatible delete response: {compatible_delete:?}"
    );

    let deleted = request(
        &mut client,
        "delete-account",
        "accounts.deleteLocalData",
        json!({
            "accountId": account_id,
            "operationId": "logout-operation-1"
        }),
    )
    .await;
    assert!(deleted.get("result").is_some());

    let deleted_again = request(
        &mut client,
        "delete-account-again",
        "accounts.deleteLocalData",
        json!({
            "accountId": account_id,
            "operationId": "logout-operation-1"
        }),
    )
    .await;
    assert!(deleted_again.get("result").is_some());

    let link_still_active = request(
        &mut client,
        "link-still-active",
        "link.start",
        json!({ "deviceName": "KT-Should-Wait" }),
    )
    .await;
    assert_eq!(link_still_active["error"]["code"], "LINK_IN_PROGRESS");
    let cancelled = request(
        &mut client,
        "link-next-cancel",
        "link.cancel",
        json!({ "linkSessionId": next_link_session_id }),
    )
    .await;
    assert!(cancelled.get("result").is_some());
    let after_delete = request(
        &mut client,
        "accounts-after-delete",
        "accounts.list",
        json!({}),
    )
    .await;
    assert!(after_delete["result"].as_array().unwrap().is_empty());

    drop(client);
    assert_clean_exit(&mut connector).await;
}

#[tokio::test]
async fn contacts_sync_list_and_send_by_peer() {
    let temp = TempDir::new().unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let endpoint = temp.path().join("connector.sock");
    let secret_file = temp.path().join("bootstrap.secret");
    let secret = [11_u8; 32];
    write_secret_file(&secret_file, &secret);

    let mut connector = spawn_connector(temp.path(), &endpoint, &secret_file);
    wait_for_path(&endpoint).await;
    let stream = UnixStream::connect(&endpoint).await.unwrap();
    let mut client = Framed::new(stream, LinesCodec::new());
    authenticate(&mut client, &secret).await;

    let started = request(&mut client, "start", "runtime.start", json!({})).await;
    let engine_pid = started["result"]["pid"].as_u64().unwrap() as u32;

    let link = request(
        &mut client,
        "link-start",
        "link.start",
        json!({ "deviceName": "KT-Contacts" }),
    )
    .await;
    let finished = request(
        &mut client,
        "link-finish",
        "link.finish",
        json!({ "linkSessionId": link["result"]["linkSessionId"] }),
    )
    .await;
    let account_id = finished["result"]["id"].as_str().unwrap().to_string();

    // finish_link already ran a best-effort sync; an explicit contacts.sync
    // inside the 60s window returns the cached counts.
    let synced = request(
        &mut client,
        "contacts-sync",
        "contacts.sync",
        json!({ "accountId": account_id }),
    )
    .await;
    assert_eq!(synced["result"]["contactCount"], 3);
    assert_eq!(synced["result"]["groupCount"], 1);
    assert!(synced["result"]["syncedAt"].as_u64().unwrap() > 0);

    let listed = request(
        &mut client,
        "contacts-list",
        "contacts.list",
        json!({ "accountId": account_id, "limit": 10 }),
    )
    .await;
    let items = listed["result"]["items"].as_array().unwrap();
    assert_eq!(items.len(), 4);
    let alice = items
        .iter()
        .find(|item| item["peerKey"] == "+15555550101")
        .unwrap();
    assert_eq!(alice["kind"], "contact");
    assert_eq!(alice["title"], "Alice Example");
    let group = items.iter().find(|item| item["kind"] == "group").unwrap();
    assert_eq!(group["peerKey"], "ZmFrZS1ncm91cC0x");
    assert_eq!(group["title"], "Fixture Group");

    let filtered = request(
        &mut client,
        "contacts-filtered",
        "contacts.list",
        json!({ "accountId": account_id, "query": "alice", "limit": 10 }),
    )
    .await;
    assert_eq!(filtered["result"]["items"].as_array().unwrap().len(), 1);

    let first_page = request(
        &mut client,
        "contacts-page-1",
        "contacts.list",
        json!({ "accountId": account_id, "limit": 2 }),
    )
    .await;
    assert_eq!(first_page["result"]["items"].as_array().unwrap().len(), 2);
    let cursor = first_page["result"]["nextCursor"].as_str().unwrap();
    let second_page = request(
        &mut client,
        "contacts-page-2",
        "contacts.list",
        json!({ "accountId": account_id, "limit": 10, "cursor": cursor }),
    )
    .await;
    assert_eq!(second_page["result"]["items"].as_array().unwrap().len(), 2);
    assert!(second_page["result"].get("nextCursor").is_none());

    // contacts.list is read-only: an unknown account is a cache miss, not an
    // upstream lookup.
    let unknown = request(
        &mut client,
        "contacts-unknown-account",
        "contacts.list",
        json!({ "accountId": "absent-account", "limit": 10 }),
    )
    .await;
    assert_eq!(unknown["error"]["code"], "ACCOUNT_NOT_FOUND");

    // Sending to a peer with no conversation yet creates it together with the
    // first message (no empty conversation skeleton).
    let sent = request(
        &mut client,
        "send-peer-1",
        "messages.sendText",
        json!({
            "accountId": account_id,
            "kind": "contact",
            "peerKey": "+15555550103",
            "peerTitle": "Fresh Peer",
            "text": "first message",
            "clientRequestId": "peer-send-1"
        }),
    )
    .await;
    assert_eq!(sent["result"]["status"], "sent");
    let conversation_id = sent["result"]["conversationId"]
        .as_str()
        .unwrap()
        .to_string();

    let conversations = request(
        &mut client,
        "conv-after-peer-send",
        "conversations.list",
        json!({ "accountId": account_id, "limit": 50 }),
    )
    .await;
    let created = conversations["result"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["id"] == conversation_id)
        .unwrap();
    assert_eq!(created["title"], "Fresh Peer");
    assert_eq!(created["type"], "direct");

    // A second send to the same peer reuses the same conversation.
    let sent_again = request(
        &mut client,
        "send-peer-2",
        "messages.sendText",
        json!({
            "accountId": account_id,
            "kind": "direct",
            "peerKey": "+15555550103",
            "text": "second message",
            "clientRequestId": "peer-send-2"
        }),
    )
    .await;
    assert_eq!(sent_again["result"]["conversationId"], conversation_id);

    // Ambiguous or missing targets are rejected before any upstream call.
    let both = request(
        &mut client,
        "send-both-targets",
        "messages.sendText",
        json!({
            "accountId": account_id,
            "conversationId": conversation_id,
            "kind": "contact",
            "peerKey": "+15555550103",
            "text": "ambiguous",
            "clientRequestId": "peer-send-both"
        }),
    )
    .await;
    assert_eq!(both["error"]["code"], "INVALID_REQUEST");
    let neither = request(
        &mut client,
        "send-no-target",
        "messages.sendText",
        json!({
            "accountId": account_id,
            "text": "no target",
            "clientRequestId": "peer-send-none"
        }),
    )
    .await;
    assert_eq!(neither["error"]["code"], "INVALID_REQUEST");
    let bad_kind = request(
        &mut client,
        "send-bad-kind",
        "messages.sendText",
        json!({
            "accountId": account_id,
            "kind": "channel",
            "peerKey": "+15555550103",
            "text": "bad kind",
            "clientRequestId": "peer-send-bad-kind"
        }),
    )
    .await;
    assert_eq!(bad_kind["error"]["code"], "INVALID_REQUEST");

    drop(client);
    wait_for_process_exit(engine_pid).await;
    assert_clean_exit(&mut connector).await;
}

/// Phase 0 happy-path smoke gap (docs/optimization-plan.md): the existing
/// phase2 test proves receive persistence through `messages.list`, but nothing
/// asserted the normalized host event stream, and `messages.getText` had no
/// end-to-end coverage. The fixture emits one receive notification right after
/// its finishLink result; the connector must persist it and deliver
/// message.upserted -> conversation.changed -> account.changed on the
/// authenticated host stream, and getText must return the full stored body.
#[tokio::test]
async fn receive_delivers_host_events_and_get_text_returns_the_full_body() {
    let temp = TempDir::new().unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let endpoint = temp.path().join("connector.sock");
    let secret_file = temp.path().join("bootstrap.secret");
    let secret = [17_u8; 32];
    write_secret_file(&secret_file, &secret);

    let mut connector = spawn_connector(temp.path(), &endpoint, &secret_file);
    wait_for_path(&endpoint).await;
    let stream = UnixStream::connect(&endpoint).await.unwrap();
    let mut client = Framed::new(stream, LinesCodec::new());
    authenticate(&mut client, &secret).await;

    let started = request(&mut client, "start", "runtime.start", json!({})).await;
    let engine_pid = started["result"]["pid"].as_u64().unwrap() as u32;

    let link = request(
        &mut client,
        "link-start",
        "link.start",
        json!({ "deviceName": "KT-Events" }),
    )
    .await;

    // The fixture emits one receive notification right after its finishLink
    // result; the connector may deliver the resulting host events before the
    // finish response reaches the socket, so collect both in one read loop
    // instead of discarding frames inside `request`.
    send_request_frame(
        &mut client,
        "link-finish",
        "link.finish",
        json!({ "linkSessionId": link["result"]["linkSessionId"] }),
    )
    .await;
    let mut finished: Option<Value> = None;
    let mut upserted: Option<Value> = None;
    let mut conversation_changed: Option<Value> = None;
    let mut account_changed: Option<Value> = None;
    timeout(Duration::from_secs(5), async {
        while finished.is_none()
            || upserted.is_none()
            || conversation_changed.is_none()
            || account_changed.is_none()
        {
            let frame: Value =
                serde_json::from_str(&client.next().await.unwrap().unwrap()).unwrap();
            if frame.get("requestId").and_then(Value::as_str) == Some("link-finish") {
                finished = Some(frame);
                continue;
            }
            match frame.get("event").and_then(Value::as_str) {
                Some("message.upserted") => upserted = Some(frame),
                Some("conversation.changed") => conversation_changed = Some(frame),
                // The link flow itself also emits account.changed; keep the one
                // that carries the receive's unread increment.
                Some("account.changed") if frame["data"]["unreadCount"] == 1 => {
                    account_changed = Some(frame);
                }
                _ => {}
            }
        }
    })
    .await
    .expect("receive must deliver message/conversation/account host events");
    let account_id = finished.unwrap()["result"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    let upserted = upserted.unwrap();
    assert_eq!(upserted["apiVersion"], API_VERSION);
    let message = &upserted["data"];
    assert_eq!(message["accountId"], account_id);
    assert_eq!(message["direction"], "incoming");
    assert_eq!(message["text"], "private text");
    assert_eq!(message["status"], "delivered");
    // attachments was removed from MessageRecord in optimization-plan Phase 2
    // (the engine runs signal-cli with --ignore-attachments); the field must
    // not reappear on the wire.
    assert!(message.get("attachments").is_none());
    let conversation_id = message["conversationId"].as_str().unwrap().to_string();
    let message_id = message["id"].as_str().unwrap().to_string();

    let conversation_changed = conversation_changed.unwrap();
    assert_eq!(conversation_changed["data"]["id"], conversation_id);
    assert_eq!(conversation_changed["data"]["type"], "direct");
    assert_eq!(conversation_changed["data"]["unreadCount"], 1);

    let account_changed = account_changed.unwrap();
    assert_eq!(account_changed["data"]["unreadCount"], 1);

    // messages.getText returns the complete persisted body for that message.
    let fetched = request(
        &mut client,
        "get-text",
        "messages.getText",
        json!({
            "accountId": account_id,
            "conversationId": conversation_id,
            "messageId": message_id
        }),
    )
    .await;
    assert_eq!(fetched["result"]["messageId"], message_id);
    assert_eq!(fetched["result"]["text"], "private text");
    assert_eq!(fetched["result"]["textBytes"], "private text".len() as u64);

    drop(client);
    wait_for_process_exit(engine_pid).await;
    assert_clean_exit(&mut connector).await;
}

/// Phase 2 (docs/optimization-plan.md): quoteMessageId is delivered upstream
/// as signal-cli's quoteTimestamp/quoteAuthor send params, send completion
/// emits message.statusChanged, and an unknown quote target is a deterministic
/// validation rejection that leaves no pending row behind.
#[tokio::test]
async fn quote_is_delivered_upstream_and_status_change_is_emitted() {
    let temp = TempDir::new().unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let endpoint = temp.path().join("connector.sock");
    let secret_file = temp.path().join("bootstrap.secret");
    let secret = [23_u8; 32];
    write_secret_file(&secret_file, &secret);

    let mut connector = spawn_connector(temp.path(), &endpoint, &secret_file);
    wait_for_path(&endpoint).await;
    let stream = UnixStream::connect(&endpoint).await.unwrap();
    let mut client = Framed::new(stream, LinesCodec::new());
    authenticate(&mut client, &secret).await;

    let started = request(&mut client, "start", "runtime.start", json!({})).await;
    let engine_pid = started["result"]["pid"].as_u64().unwrap() as u32;

    let link = request(
        &mut client,
        "link-start",
        "link.start",
        json!({ "deviceName": "KT-Quote" }),
    )
    .await;
    // The fixture emits one receive notification after finishLink; the frames
    // can interleave with the finish response, so drain until both arrive.
    send_request_frame(
        &mut client,
        "link-finish",
        "link.finish",
        json!({ "linkSessionId": link["result"]["linkSessionId"] }),
    )
    .await;
    let mut finished: Option<Value> = None;
    let mut upserted: Option<Value> = None;
    timeout(Duration::from_secs(5), async {
        while finished.is_none() || upserted.is_none() {
            let frame: Value =
                serde_json::from_str(&client.next().await.unwrap().unwrap()).unwrap();
            if frame.get("requestId").and_then(Value::as_str) == Some("link-finish") {
                finished = Some(frame);
                continue;
            }
            if frame.get("event").and_then(Value::as_str) == Some("message.upserted") {
                upserted = Some(frame);
            }
        }
    })
    .await
    .expect("link finish must persist the fixture's receive notification");
    let account_id = finished.unwrap()["result"]["id"]
        .as_str()
        .unwrap()
        .to_string();
    // The fixture's receive: source +15555550101, envelope timestamp 42.
    let upserted = upserted.unwrap();
    let quoted_message_id = upserted["data"]["id"].as_str().unwrap().to_string();
    let conversation_id = upserted["data"]["conversationId"]
        .as_str()
        .unwrap()
        .to_string();

    // A quoted send resolves the local quoteMessageId to the upstream quote
    // parameters and completes with a message.statusChanged event.
    send_request_frame(
        &mut client,
        "send-quote",
        "messages.sendText",
        json!({
            "accountId": account_id,
            "conversationId": conversation_id,
            "text": "quoting inbound",
            "clientRequestId": "quote-req-1",
            "quoteMessageId": quoted_message_id
        }),
    )
    .await;
    let mut sent: Option<Value> = None;
    let mut status_changed: Option<Value> = None;
    timeout(Duration::from_secs(5), async {
        while sent.is_none() || status_changed.is_none() {
            let frame: Value =
                serde_json::from_str(&client.next().await.unwrap().unwrap()).unwrap();
            if frame.get("requestId").and_then(Value::as_str) == Some("send-quote") {
                sent = Some(frame);
                continue;
            }
            if frame.get("event").and_then(Value::as_str) == Some("message.statusChanged") {
                status_changed = Some(frame);
            }
        }
    })
    .await
    .expect("send must answer and emit message.statusChanged");
    let sent = sent.unwrap();
    assert_eq!(sent["result"]["status"], "sent");
    assert_eq!(sent["result"]["quoteMessageId"], quoted_message_id);
    let status_changed = status_changed.unwrap();
    assert_eq!(status_changed["apiVersion"], API_VERSION);
    assert_eq!(status_changed["data"]["accountId"], account_id);
    assert_eq!(
        status_changed["data"]["messageId"],
        sent["result"]["id"].as_str().unwrap()
    );
    assert_eq!(status_changed["data"]["status"], "sent");

    // The fake signal-cli logged the exact JSON-RPC send params it received.
    let send_log = fs::read_to_string(
        temp.path()
            .join("signal-data")
            .join(".fixture-send-log.jsonl"),
    )
    .expect("fixture must log send params");
    let quoted_send: Value = send_log
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .find(|params: &Value| params["message"] == "quoting inbound")
        .expect("the quoted send must reach the upstream");
    assert_eq!(quoted_send["quoteTimestamp"], 42);
    assert_eq!(quoted_send["quoteAuthor"], "+15555550101");
    assert_eq!(quoted_send["recipient"], json!(["+15555550101"]));
    assert_eq!(quoted_send["account"], "+15555550100");

    // A quote pointing at a message that does not exist is rejected during
    // validation — before any upstream call or pending row.
    let rejected = request(
        &mut client,
        "send-quote-missing",
        "messages.sendText",
        json!({
            "accountId": account_id,
            "conversationId": conversation_id,
            "text": "quote of nothing",
            "clientRequestId": "quote-req-missing",
            "quoteMessageId": "no-such-message"
        }),
    )
    .await;
    assert_eq!(rejected["error"]["code"], "MESSAGE_NOT_FOUND");
    assert_eq!(rejected["error"]["retryable"], false);

    // No pending row was left behind: the same clientRequestId re-validates as
    // a fresh request instead of replaying a stuck pending record.
    let retried = request(
        &mut client,
        "send-quote-retry",
        "messages.sendText",
        json!({
            "accountId": account_id,
            "conversationId": conversation_id,
            "text": "quote of nothing",
            "clientRequestId": "quote-req-missing"
        }),
    )
    .await;
    assert_eq!(retried["result"]["status"], "sent");

    drop(client);
    wait_for_process_exit(engine_pid).await;
    assert_clean_exit(&mut connector).await;
}

/// messages.remoteDelete (contract revision 1.6, docs/remote-delete-l2-plan.md):
/// the happy path answers {"status":"deleted"} with the exact upstream params
/// (targetTimestamp from the sent row, recipient/groupId by conversation kind);
/// unknown params fail with INVALID_REQUEST; a nonexistent message is
/// MESSAGE_NOT_FOUND without touching the upstream.
#[tokio::test]
async fn remote_delete_maps_the_sent_row_and_answers_upstream_outcome() {
    let temp = TempDir::new().unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let endpoint = temp.path().join("connector.sock");
    let secret_file = temp.path().join("bootstrap.secret");
    let secret = [19_u8; 32];
    write_secret_file(&secret_file, &secret);

    let mut connector = spawn_connector(temp.path(), &endpoint, &secret_file);
    wait_for_path(&endpoint).await;
    let stream = UnixStream::connect(&endpoint).await.unwrap();
    let mut client = Framed::new(stream, LinesCodec::new());
    authenticate(&mut client, &secret).await;

    let started = request(&mut client, "start", "runtime.start", json!({})).await;
    let engine_pid = started["result"]["pid"].as_u64().unwrap() as u32;
    let link = request(
        &mut client,
        "link-start",
        "link.start",
        json!({ "deviceName": "KT-Remote-Delete" }),
    )
    .await;
    // The fixture emits one receive after finishLink; drain both frames.
    send_request_frame(
        &mut client,
        "link-finish",
        "link.finish",
        json!({ "linkSessionId": link["result"]["linkSessionId"] }),
    )
    .await;
    let mut finished: Option<Value> = None;
    timeout(Duration::from_secs(5), async {
        while finished.is_none() {
            let frame: Value =
                serde_json::from_str(&client.next().await.unwrap().unwrap()).unwrap();
            if frame.get("requestId").and_then(Value::as_str) == Some("link-finish") {
                finished = Some(frame);
            }
        }
    })
    .await
    .unwrap();
    let account_id = finished.unwrap()["result"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Direct chat: send, then delete the sent message.
    let sent = request(
        &mut client,
        "send-1",
        "messages.sendText",
        json!({
            "accountId": account_id,
            "kind": "contact",
            "peerKey": "+15555550101",
            "text": "delete me",
            "clientRequestId": "rd-send-1"
        }),
    )
    .await;
    assert_eq!(sent["result"]["status"], "sent");
    // The connector overwrites sentAt with the send response's upstream
    // timestamp (99 in the fixture); the delete must address exactly it.
    let message_id = sent["result"]["id"].as_str().unwrap().to_string();
    let conversation_id = sent["result"]["conversationId"]
        .as_str()
        .unwrap()
        .to_string();

    let deleted = request(
        &mut client,
        "rd-1",
        "messages.remoteDelete",
        json!({
            "accountId": account_id,
            "conversationId": conversation_id,
            "messageId": message_id,
            "operationId": "rd-operation-1"
        }),
    )
    .await;
    assert_eq!(deleted["result"]["status"], "deleted");

    // The exact upstream dispatch was recorded by the fixture.
    let send_log = fs::read_to_string(
        temp.path()
            .join("signal-data")
            .join(".fixture-send-log.jsonl"),
    )
    .expect("fixture must log remoteDelete params");
    let delete_call: Value = send_log
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .find(|params: &Value| params.get("targetTimestamp").is_some())
        .expect("the remoteDelete must reach the upstream");
    assert_eq!(delete_call["account"], "+15555550100");
    assert_eq!(delete_call["targetTimestamp"], 99);
    assert_eq!(delete_call["recipient"], json!(["+15555550101"]));

    // Unknown params are rejected by shape before any local lookup.
    let invalid = request(
        &mut client,
        "rd-invalid",
        "messages.remoteDelete",
        json!({
            "accountId": account_id,
            "conversationId": conversation_id,
            "messageId": message_id,
            "unexpected": true
        }),
    )
    .await;
    assert_eq!(invalid["error"]["code"], "INVALID_REQUEST");

    // A missing message answers MESSAGE_NOT_FOUND without an upstream call.
    let missing = request(
        &mut client,
        "rd-missing",
        "messages.remoteDelete",
        json!({
            "accountId": account_id,
            "conversationId": conversation_id,
            "messageId": "no-such-message"
        }),
    )
    .await;
    assert_eq!(missing["error"]["code"], "MESSAGE_NOT_FOUND");
    assert_eq!(missing["error"]["retryable"], false);

    // A group send resolves groupId addressing instead of recipient.
    let group_sent = request(
        &mut client,
        "send-group",
        "messages.sendText",
        json!({
            "accountId": account_id,
            "kind": "group",
            "peerKey": "ZmFrZS1ncm91cC0x",
            "text": "group delete me",
            "clientRequestId": "rd-send-group"
        }),
    )
    .await;
    assert_eq!(group_sent["result"]["status"], "sent");
    let group_deleted = request(
        &mut client,
        "rd-group",
        "messages.remoteDelete",
        json!({
            "accountId": account_id,
            "conversationId": group_sent["result"]["conversationId"],
            "messageId": group_sent["result"]["id"]
        }),
    )
    .await;
    assert_eq!(group_deleted["result"]["status"], "deleted");
    let send_log = fs::read_to_string(
        temp.path()
            .join("signal-data")
            .join(".fixture-send-log.jsonl"),
    )
    .unwrap();
    let group_call: Value = send_log
        .lines()
        .rev()
        .map(|line| serde_json::from_str(line).unwrap())
        .find(|params: &Value| params.get("targetTimestamp").is_some())
        .expect("the group remoteDelete must reach the upstream");
    assert_eq!(group_call["groupId"], json!("ZmFrZS1ncm91cC0x"));
    assert!(group_call.get("recipient").is_none());

    drop(client);
    wait_for_process_exit(engine_pid).await;
    assert_clean_exit(&mut connector).await;
}

/// messages.sendReaction (contract revision 1.7, docs/remote-delete-l2-plan.md
/// §3.2 shape): reactions on a sent outgoing row and on an incoming row both
/// answer {"status":"sent"} with the direction-derived targetAuthor and the
/// conversation-kind addressing; unknown params fail with INVALID_REQUEST; a
/// missing message is MESSAGE_NOT_FOUND without touching the upstream; a
/// multi-grapheme emoji is rejected by shape before any local lookup.
#[tokio::test]
async fn send_reaction_maps_row_direction_and_answers_upstream_outcome() {
    let temp = TempDir::new().unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let endpoint = temp.path().join("connector.sock");
    let secret_file = temp.path().join("bootstrap.secret");
    let secret = [29_u8; 32];
    write_secret_file(&secret_file, &secret);

    let mut connector = spawn_connector(temp.path(), &endpoint, &secret_file);
    wait_for_path(&endpoint).await;
    let stream = UnixStream::connect(&endpoint).await.unwrap();
    let mut client = Framed::new(stream, LinesCodec::new());
    authenticate(&mut client, &secret).await;

    let started = request(&mut client, "start", "runtime.start", json!({})).await;
    let engine_pid = started["result"]["pid"].as_u64().unwrap() as u32;
    let link = request(
        &mut client,
        "link-start",
        "link.start",
        json!({ "deviceName": "KT-Send-Reaction" }),
    )
    .await;
    // The fixture emits one receive notification after finishLink; the frames
    // can interleave with the finish response, so drain until both arrive.
    send_request_frame(
        &mut client,
        "link-finish",
        "link.finish",
        json!({ "linkSessionId": link["result"]["linkSessionId"] }),
    )
    .await;
    let mut finished: Option<Value> = None;
    let mut upserted: Option<Value> = None;
    timeout(Duration::from_secs(5), async {
        while finished.is_none() || upserted.is_none() {
            let frame: Value =
                serde_json::from_str(&client.next().await.unwrap().unwrap()).unwrap();
            if frame.get("requestId").and_then(Value::as_str) == Some("link-finish") {
                finished = Some(frame);
                continue;
            }
            if frame.get("event").and_then(Value::as_str) == Some("message.upserted") {
                upserted = Some(frame);
            }
        }
    })
    .await
    .expect("link finish must persist the fixture's receive notification");
    let account_id = finished.unwrap()["result"]["id"]
        .as_str()
        .unwrap()
        .to_string();
    // The fixture's receive: source +15555550101, envelope timestamp 42.
    let upserted = upserted.unwrap();
    let incoming_id = upserted["data"]["id"].as_str().unwrap().to_string();
    let conversation_id = upserted["data"]["conversationId"]
        .as_str()
        .unwrap()
        .to_string();

    // React to the incoming row: targetAuthor is the peer, targetTimestamp is
    // the envelope timestamp, remove defaults to false.
    let reacted = request(
        &mut client,
        "sr-incoming",
        "messages.sendReaction",
        json!({
            "accountId": account_id,
            "conversationId": conversation_id,
            "messageId": incoming_id,
            "emoji": "👍",
            "operationId": "sr-operation-in"
        }),
    )
    .await;
    assert_eq!(reacted["result"]["status"], "sent");
    let send_log = fs::read_to_string(
        temp.path()
            .join("signal-data")
            .join(".fixture-send-log.jsonl"),
    )
    .expect("fixture must log sendReaction params");
    let reaction_call: Value = send_log
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .find(|params: &Value| params.get("targetTimestamp").is_some())
        .expect("the sendReaction must reach the upstream");
    assert_eq!(reaction_call["account"], "+15555550100");
    assert_eq!(reaction_call["emoji"], "👍");
    assert_eq!(reaction_call["remove"], false);
    assert_eq!(reaction_call["targetAuthor"], "+15555550101");
    assert_eq!(reaction_call["targetTimestamp"], 42);
    assert_eq!(reaction_call["recipient"], json!(["+15555550101"]));

    // Outgoing `sent` row: targetAuthor is the linked account itself, and
    // remove=true passes through as an explicit boolean.
    let sent = request(
        &mut client,
        "send-1",
        "messages.sendText",
        json!({
            "accountId": account_id,
            "kind": "contact",
            "peerKey": "+15555550101",
            "text": "react to me",
            "clientRequestId": "sr-send-1"
        }),
    )
    .await;
    assert_eq!(sent["result"]["status"], "sent");
    let removed = request(
        &mut client,
        "sr-outgoing",
        "messages.sendReaction",
        json!({
            "accountId": account_id,
            "conversationId": conversation_id,
            "messageId": sent["result"]["id"],
            "emoji": "🎉",
            "remove": true
        }),
    )
    .await;
    assert_eq!(removed["result"]["status"], "sent");
    let send_log = fs::read_to_string(
        temp.path()
            .join("signal-data")
            .join(".fixture-send-log.jsonl"),
    )
    .unwrap();
    let remove_call: Value = send_log
        .lines()
        .rev()
        .map(|line| serde_json::from_str(line).unwrap())
        .find(|params: &Value| params.get("targetTimestamp").is_some())
        .expect("the reaction remove must reach the upstream");
    assert_eq!(remove_call["emoji"], "🎉");
    assert_eq!(remove_call["remove"], true);
    assert_eq!(remove_call["targetAuthor"], "+15555550100");
    assert_eq!(remove_call["targetTimestamp"], 99);

    // Unknown params are rejected by shape before any local lookup.
    let invalid = request(
        &mut client,
        "sr-invalid",
        "messages.sendReaction",
        json!({
            "accountId": account_id,
            "conversationId": conversation_id,
            "messageId": incoming_id,
            "emoji": "👍",
            "unexpected": true
        }),
    )
    .await;
    assert_eq!(invalid["error"]["code"], "INVALID_REQUEST");

    // More than one grapheme cluster is rejected by shape (the schema bounds
    // length; the service enforces the cluster rule deterministically).
    let multi = request(
        &mut client,
        "sr-multi",
        "messages.sendReaction",
        json!({
            "accountId": account_id,
            "conversationId": conversation_id,
            "messageId": incoming_id,
            "emoji": "👍👍"
        }),
    )
    .await;
    assert_eq!(multi["error"]["code"], "INVALID_REQUEST");

    // A missing message answers MESSAGE_NOT_FOUND without an upstream call.
    let missing = request(
        &mut client,
        "sr-missing",
        "messages.sendReaction",
        json!({
            "accountId": account_id,
            "conversationId": conversation_id,
            "messageId": "no-such-message",
            "emoji": "👍"
        }),
    )
    .await;
    assert_eq!(missing["error"]["code"], "MESSAGE_NOT_FOUND");
    assert_eq!(missing["error"]["retryable"], false);

    // A group send resolves groupId addressing instead of recipient.
    let group_sent = request(
        &mut client,
        "send-group",
        "messages.sendText",
        json!({
            "accountId": account_id,
            "kind": "group",
            "peerKey": "ZmFrZS1ncm91cC0x",
            "text": "group react to me",
            "clientRequestId": "sr-send-group"
        }),
    )
    .await;
    assert_eq!(group_sent["result"]["status"], "sent");
    let group_reacted = request(
        &mut client,
        "sr-group",
        "messages.sendReaction",
        json!({
            "accountId": account_id,
            "conversationId": group_sent["result"]["conversationId"],
            "messageId": group_sent["result"]["id"],
            "emoji": "👍"
        }),
    )
    .await;
    assert_eq!(group_reacted["result"]["status"], "sent");
    let send_log = fs::read_to_string(
        temp.path()
            .join("signal-data")
            .join(".fixture-send-log.jsonl"),
    )
    .unwrap();
    let group_call: Value = send_log
        .lines()
        .rev()
        .map(|line| serde_json::from_str(line).unwrap())
        .find(|params: &Value| params.get("targetTimestamp").is_some())
        .expect("the group sendReaction must reach the upstream");
    assert_eq!(group_call["groupId"], json!("ZmFrZS1ncm91cC0x"));
    assert!(group_call.get("recipient").is_none());

    drop(client);
    wait_for_process_exit(engine_pid).await;
    assert_clean_exit(&mut connector).await;
}

#[tokio::test]
async fn socks_proxy_env_is_forwarded_to_the_jvm() {
    let temp = TempDir::new().unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let endpoint = temp.path().join("connector.sock");
    let secret_file = temp.path().join("bootstrap.secret");
    let secret = [13_u8; 32];
    write_secret_file(&secret_file, &secret);

    // The fixture exits 91 immediately unless JAVA_OPTS carries exactly the
    // heap budget plus the SOCKS proxy flags.
    let mut command = Command::new(env!("CARGO_BIN_EXE_kt-signal-connector"));
    command
        .arg("serve")
        .arg("--endpoint")
        .arg(&endpoint)
        .arg("--bootstrap-secret-file")
        .arg(&secret_file)
        .arg("--signal-cli")
        .arg(fixture())
        .arg("--signal-data-dir")
        .arg(temp.path().join("signal-data"))
        .arg("--state-dir")
        .arg(temp.path().join("state"))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .env("KT_SIGNAL_SOCKS_PROXY", "127.0.0.1:11080")
        .env(
            "KT_FAKE_EXPECT_JAVA_OPTS",
            "-Xms16m -Xmx384m -DsocksProxyHost=127.0.0.1 -DsocksProxyPort=11080",
        );
    let mut connector = command.spawn().unwrap();
    wait_for_path(&endpoint).await;
    let stream = UnixStream::connect(&endpoint).await.unwrap();
    let mut client = Framed::new(stream, LinesCodec::new());
    authenticate(&mut client, &secret).await;

    let started = request(&mut client, "start", "runtime.start", json!({})).await;
    assert_eq!(started["result"]["state"], "running");
    // A proxy-flag mismatch kills the fixture at JVM launch; a live answer
    // after a settle delay proves the exact JAVA_OPTS reached the child.
    sleep(Duration::from_millis(200)).await;
    let accounts = request(&mut client, "accounts", "accounts.list", json!({})).await;
    assert!(accounts.get("result").is_some());

    drop(client);
    assert_clean_exit(&mut connector).await;
}

#[tokio::test]
async fn native_mode_serves_without_a_jvm_environment() {
    let temp = TempDir::new().unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let endpoint = temp.path().join("connector.sock");
    let secret_file = temp.path().join("bootstrap.secret");
    let secret = [14_u8; 32];
    write_secret_file(&secret_file, &secret);

    // Native mode via the environment variable (the form Desktop uses), with
    // a JAVA_HOME that JVM mode would reject: it must be ignored entirely.
    // A SOCKS proxy stays configured to prove native+proxy starts; the proxy
    // reaches a real native binary as `-D` argv properties (the fixture
    // ignores argv; the argv shape is unit-tested in engine.rs). The fixture
    // exits 94 immediately if JAVA_OPTS or JAVA_HOME leak into its env.
    let mut command = Command::new(env!("CARGO_BIN_EXE_kt-signal-connector"));
    command
        .arg("serve")
        .arg("--endpoint")
        .arg(&endpoint)
        .arg("--bootstrap-secret-file")
        .arg(&secret_file)
        .arg("--signal-cli")
        .arg(fixture())
        .arg("--java-home")
        .arg(temp.path().join("no-such-jre"))
        .arg("--signal-data-dir")
        .arg(temp.path().join("signal-data"))
        .arg("--state-dir")
        .arg(temp.path().join("state"))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .env("KT_SIGNAL_CLI_NATIVE", "1")
        .env("KT_SIGNAL_SOCKS_PROXY", "127.0.0.1:11080")
        .env("KT_FAKE_EXPECT_NO_JAVA", "1");
    let mut connector = command.spawn().unwrap();
    wait_for_path(&endpoint).await;
    let stream = UnixStream::connect(&endpoint).await.unwrap();
    let mut client = Framed::new(stream, LinesCodec::new());
    authenticate(&mut client, &secret).await;

    let started = request(&mut client, "start", "runtime.start", json!({})).await;
    assert_eq!(started["result"]["state"], "running");
    // A JAVA_* leak kills the fixture at spawn; a live answer after a settle
    // delay proves the native spawn path serves requests without a JVM env.
    sleep(Duration::from_millis(200)).await;
    let accounts = request(&mut client, "accounts", "accounts.list", json!({})).await;
    assert!(accounts.get("result").is_some());

    drop(client);
    assert_clean_exit(&mut connector).await;
}

#[tokio::test]
async fn link_finish_reports_a_non_retryable_unknown_outcome_when_the_engine_dies() {
    let temp = TempDir::new().unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let endpoint = temp.path().join("connector.sock");
    let secret_file = temp.path().join("bootstrap.secret");
    let secret = [9_u8; 32];
    write_secret_file(&secret_file, &secret);

    let mut connector = spawn_connector(temp.path(), &endpoint, &secret_file);
    wait_for_path(&endpoint).await;
    let stream = UnixStream::connect(&endpoint).await.unwrap();
    let mut client = Framed::new(stream, LinesCodec::new());
    authenticate(&mut client, &secret).await;
    let started = request(&mut client, "start", "runtime.start", json!({})).await;
    assert_eq!(started["result"]["state"], "running");

    let link = request(
        &mut client,
        "link-start",
        "link.start",
        json!({ "deviceName": "[crash-link-test]" }),
    )
    .await;
    let finished = request(
        &mut client,
        "link-finish",
        "link.finish",
        json!({ "linkSessionId": link["result"]["linkSessionId"] }),
    )
    .await;

    // finishLink is Mutating, so a lost result must not be reported as a
    // retryable timeout: re-running it could claim a second device slot.
    assert_eq!(finished["error"]["code"], "LINK_OUTCOME_UNKNOWN");
    assert_eq!(finished["error"]["retryable"], false);

    drop(client);
    assert_clean_exit(&mut connector).await;
}

#[tokio::test]
async fn account_delete_unknown_is_reconciled_only_on_explicit_retry() {
    let temp = TempDir::new().unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let endpoint = temp.path().join("connector.sock");
    let secret_file = temp.path().join("bootstrap.secret");
    let secret = [8_u8; 32];
    fs::write(&secret_file, bootstrap_payload(&secret)).unwrap();
    fs::set_permissions(&secret_file, fs::Permissions::from_mode(0o600)).unwrap();

    let mut connector = spawn_connector_with_delete_mode(
        temp.path(),
        &endpoint,
        &secret_file,
        Some("crash_after_delete_once"),
    );
    wait_for_path(&endpoint).await;
    let stream = UnixStream::connect(&endpoint).await.unwrap();
    let mut client = Framed::new(stream, LinesCodec::new());
    authenticate(&mut client, &secret).await;
    let started = request(&mut client, "start", "runtime.start", json!({})).await;
    assert_eq!(started["result"]["state"], "running");

    let link = request(
        &mut client,
        "link-start",
        "link.start",
        json!({ "deviceName": "KT-Delete-Recovery" }),
    )
    .await;
    let finished = request(
        &mut client,
        "link-finish",
        "link.finish",
        json!({ "linkSessionId": link["result"]["linkSessionId"] }),
    )
    .await;
    let account_id = finished["result"]["id"].as_str().unwrap().to_string();

    let unknown = request(
        &mut client,
        "delete-unknown",
        "accounts.deleteLocalData",
        json!({
            "accountId": account_id,
            "operationId": "delete-recovery-operation"
        }),
    )
    .await;
    assert_eq!(unknown["error"]["code"], "ACCOUNT_DELETE_OUTCOME_UNKNOWN");

    let restarted = request(&mut client, "restart", "runtime.start", json!({})).await;
    assert_eq!(restarted["result"]["state"], "running");
    let reconciled = request(
        &mut client,
        "delete-reconcile",
        "accounts.deleteLocalData",
        json!({
            "accountId": account_id,
            "operationId": "delete-recovery-operation"
        }),
    )
    .await;
    assert!(reconciled.get("result").is_some());
    let idempotent = request(
        &mut client,
        "delete-completed",
        "accounts.deleteLocalData",
        json!({
            "accountId": account_id,
            "operationId": "delete-recovery-operation"
        }),
    )
    .await;
    assert!(
        idempotent.get("result").is_some(),
        "idempotent delete response: {idempotent:?}"
    );
    let accounts = request(&mut client, "accounts", "accounts.list", json!({})).await;
    assert!(accounts["result"].as_array().unwrap().is_empty());

    drop(client);
    assert_clean_exit(&mut connector).await;
}

/// Regression for the delete/send race (optimization-plan Phase 1b): without
/// the drain barrier, a delete on the control lane could clear the account
/// rows while an upstream send on the send lane was still in flight — the
/// message went out but the completion found no pending row. With the barrier
/// the in-flight send completes fully before the delete runs, and a send that
/// arrives during the delete is rejected. Linearization point: the per-account
/// dispatch mutex, held across the whole dispatch including the completion
/// write and the host response.
#[tokio::test]
async fn account_delete_drains_in_flight_send_and_rejects_new_sends() {
    let temp = TempDir::new().unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let endpoint = temp.path().join("connector.sock");
    let secret_file = temp.path().join("bootstrap.secret");
    let secret = [13_u8; 32];
    write_secret_file(&secret_file, &secret);

    let mut connector = spawn_connector(temp.path(), &endpoint, &secret_file);
    wait_for_path(&endpoint).await;
    let stream = UnixStream::connect(&endpoint).await.unwrap();
    let mut client = Framed::new(stream, LinesCodec::new());
    authenticate(&mut client, &secret).await;

    let started = request(&mut client, "start", "runtime.start", json!({})).await;
    let engine_pid = started["result"]["pid"].as_u64().unwrap() as u32;
    let link = request(
        &mut client,
        "link-start",
        "link.start",
        json!({ "deviceName": "KT-Delete-Race" }),
    )
    .await;
    let finished = request(
        &mut client,
        "link-finish",
        "link.finish",
        json!({ "linkSessionId": link["result"]["linkSessionId"] }),
    )
    .await;
    let account_id = finished["result"]["id"].as_str().unwrap().to_string();

    // The fixture answers a "[slow-host-test]" send after ~350ms, so this send
    // is upstream-in-flight when the delete arrives.
    send_request_frame(
        &mut client,
        "race-send-slow",
        "messages.sendText",
        json!({
            "accountId": account_id,
            "kind": "contact",
            "peerKey": "+15555550101",
            "text": "[slow-host-test]",
            "clientRequestId": "race-slow"
        }),
    )
    .await;
    // Let the send reach the upstream wait before the delete arrives.
    sleep(Duration::from_millis(100)).await;
    send_request_frame(
        &mut client,
        "race-delete",
        "accounts.deleteLocalData",
        json!({
            "accountId": account_id,
            "operationId": "race-delete-operation"
        }),
    )
    .await;
    // The delete marks the account before draining, so a send issued now is
    // rejected instead of queueing behind the delete.
    sleep(Duration::from_millis(50)).await;
    send_request_frame(
        &mut client,
        "race-send-during-delete",
        "messages.sendText",
        json!({
            "accountId": account_id,
            "kind": "contact",
            "peerKey": "+15555550101",
            "text": "must never reach the upstream",
            "clientRequestId": "race-during-delete"
        }),
    )
    .await;

    let mut order = Vec::new();
    let mut slow = None;
    let mut delete = None;
    let mut during = None;
    while slow.is_none() || delete.is_none() || during.is_none() {
        let response: Value = serde_json::from_str(&client.next().await.unwrap().unwrap()).unwrap();
        match response.get("requestId").and_then(Value::as_str) {
            Some("race-send-slow") => {
                order.push("race-send-slow");
                slow = Some(response);
            }
            Some("race-delete") => {
                order.push("race-delete");
                delete = Some(response);
            }
            Some("race-send-during-delete") => {
                order.push("race-send-during-delete");
                during = Some(response);
            }
            _ => {}
        }
    }

    // The drained send completed and was answered before the delete: its
    // completion write (and response) happened strictly ahead of the delete's
    // upstream call, so no "sent but locally rowless" message can exist.
    let slow = slow.unwrap();
    assert_eq!(slow["result"]["status"], "sent");
    let delete = delete.unwrap();
    assert!(delete.get("result").is_some());
    let send_pos = order.iter().position(|id| *id == "race-send-slow").unwrap();
    let delete_pos = order.iter().position(|id| *id == "race-delete").unwrap();
    assert!(
        send_pos < delete_pos,
        "the delete must answer only after the drained send: {order:?}"
    );

    // The send issued during the delete never reached the upstream.
    let during = during.unwrap();
    assert_eq!(during["error"]["code"], "ACCOUNT_NOT_FOUND");
    assert_eq!(during["error"]["retryable"], false);

    let accounts = request(&mut client, "accounts-after", "accounts.list", json!({})).await;
    assert!(accounts["result"].as_array().unwrap().is_empty());

    drop(client);
    wait_for_process_exit(engine_pid).await;
    assert_clean_exit(&mut connector).await;
}

/// Phase 3 dev/test override: a legacy single-line payload plus the
/// KT_SIGNAL_STORE_KEY environment variable serves normally, and the store on
/// disk is encrypted (no plaintext SQLite header).
#[tokio::test]
async fn store_key_env_override_unlocks_a_secret_only_payload() {
    let temp = TempDir::new().unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let endpoint = temp.path().join("connector.sock");
    let secret_file = temp.path().join("bootstrap.secret");
    let secret = [31_u8; 32];
    // Line 1 only: no store key in the payload, so the env override provides it.
    fs::write(&secret_file, hex::encode(secret)).unwrap();
    fs::set_permissions(&secret_file, fs::Permissions::from_mode(0o600)).unwrap();
    let store_key = hex::encode([0xA5_u8; 32]);

    let mut connector = spawn_connector_with(
        temp.path(),
        &endpoint,
        &secret_file,
        None,
        &[("KT_SIGNAL_STORE_KEY", store_key.as_str())],
    );
    wait_for_path(&endpoint).await;

    let stream = UnixStream::connect(&endpoint).await.unwrap();
    let mut client = Framed::new(stream, LinesCodec::new());
    authenticate(&mut client, &secret).await;
    let started = request(&mut client, "start-1", "runtime.start", json!({})).await;
    assert_eq!(started["result"]["state"], "running");

    drop(client);
    assert_clean_exit(&mut connector).await;

    let db = temp.path().join("state").join("connector.sqlite3");
    let mut header = [0_u8; 16];
    std::io::Read::read_exact(&mut fs::File::open(&db).unwrap(), &mut header).unwrap();
    assert_ne!(&header, b"SQLite format 3\0");
}

/// Phase 3 fail closed: with no store key anywhere (single-line payload, no
/// env override), the connector exits with a classified error before serving
/// and never creates a database.
#[tokio::test]
async fn missing_store_key_fails_closed_before_serving() {
    let temp = TempDir::new().unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let endpoint = temp.path().join("connector.sock");
    let secret_file = temp.path().join("bootstrap.secret");
    fs::write(&secret_file, hex::encode([41_u8; 32])).unwrap();
    fs::set_permissions(&secret_file, fs::Permissions::from_mode(0o600)).unwrap();

    let mut connector = spawn_connector(temp.path(), &endpoint, &secret_file);
    let status = timeout(Duration::from_secs(5), connector.wait())
        .await
        .expect("a connector without a store key should exit promptly")
        .unwrap();
    assert!(!status.success());
    let mut stderr = String::new();
    if let Some(mut pipe) = connector.stderr.take() {
        let _ = pipe.read_to_string(&mut stderr).await;
    }
    assert!(
        stderr.contains("store key is required"),
        "unexpected stderr: {stderr}"
    );
    assert!(
        !temp.path().join("state").join("connector.sqlite3").exists(),
        "fail closed means no database is created"
    );
}

#[tokio::test]
async fn get_attachment_answers_base64_data_or_contract_errors() {
    let temp = TempDir::new().unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let endpoint = temp.path().join("connector.sock");
    let secret_file = temp.path().join("bootstrap.secret");
    let secret = [31_u8; 32];
    write_secret_file(&secret_file, &secret);

    let mut connector = spawn_connector(temp.path(), &endpoint, &secret_file);
    wait_for_path(&endpoint).await;
    let stream = UnixStream::connect(&endpoint).await.unwrap();
    let mut client = Framed::new(stream, LinesCodec::new());
    authenticate(&mut client, &secret).await;

    let started = request(&mut client, "start", "runtime.start", json!({})).await;
    let engine_pid = started["result"]["pid"].as_u64().unwrap() as u32;
    let link = request(
        &mut client,
        "link-start",
        "link.start",
        json!({ "deviceName": "KT-Get-Attachment" }),
    )
    .await;
    // Same drain discipline as send_reaction: the fixture's receive
    // notification can interleave with the finishLink response.
    send_request_frame(
        &mut client,
        "link-finish",
        "link.finish",
        json!({ "linkSessionId": link["result"]["linkSessionId"] }),
    )
    .await;
    let mut finished: Option<Value> = None;
    let mut upserted: Option<Value> = None;
    timeout(Duration::from_secs(5), async {
        while finished.is_none() || upserted.is_none() {
            let frame: Value =
                serde_json::from_str(&client.next().await.unwrap().unwrap()).unwrap();
            if frame.get("requestId").and_then(Value::as_str) == Some("link-finish") {
                finished = Some(frame);
                continue;
            }
            if frame.get("event").and_then(Value::as_str) == Some("message.upserted") {
                upserted = Some(frame);
            }
        }
    })
    .await
    .expect("link finish must persist the fixture's receive notification");
    let account_id = finished.unwrap()["result"]["id"]
        .as_str()
        .unwrap()
        .to_string();
    let upserted = upserted.unwrap();
    let incoming_id = upserted["data"]["id"].as_str().unwrap().to_string();
    let conversation_id = upserted["data"]["conversationId"]
        .as_str()
        .unwrap()
        .to_string();

    // The fixture serves `<data-dir>/attachments/<id>`; plant the file the
    // happy path reads back.
    let attachments_dir = temp.path().join("signal-data").join("attachments");
    fs::create_dir_all(&attachments_dir).unwrap();
    const FIXTURE_ATTACHMENT: &[u8] = b"fixture attachment bytes";
    fs::write(attachments_dir.join("att-1"), FIXTURE_ATTACHMENT).unwrap();

    let fetched = request(
        &mut client,
        "att-happy",
        "messages.attachments.get",
        json!({
            "accountId": account_id,
            "conversationId": conversation_id,
            "messageId": incoming_id,
            "attachmentId": "att-1",
            "sizeBytes": FIXTURE_ATTACHMENT.len() as u64
        }),
    )
    .await;
    assert_eq!(
        fetched["result"]["attachmentId"], "att-1",
        "the response echoes the requested attachment id"
    );
    assert_eq!(
        fetched["result"]["data"], "Zml4dHVyZSBhdHRhY2htZW50IGJ5dGVz",
        "the upstream base64 passthrough decodes to the planted bytes"
    );
    // The upstream call carries exactly the two jsonRpc params.
    let send_log = fs::read_to_string(
        temp.path()
            .join("signal-data")
            .join(".fixture-send-log.jsonl"),
    )
    .expect("fixture must log getAttachment params");
    let get_call: Value = send_log
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .find(|params: &Value| params.get("id") == Some(&json!("att-1")))
        .expect("the getAttachment must reach the upstream");
    assert_eq!(get_call["account"], "+15555550100");
    assert_eq!(get_call["id"], "att-1");

    // sizeBytes below the declared payload length is rejected by shape.
    let mismatched = request(
        &mut client,
        "att-short",
        "messages.attachments.get",
        json!({
            "accountId": account_id,
            "conversationId": conversation_id,
            "messageId": incoming_id,
            "attachmentId": "att-1",
            "sizeBytes": 23
        }),
    )
    .await;
    assert_eq!(mismatched["error"]["code"], "INVALID_REQUEST");

    // sizeBytes beyond the 5 MiB PoC cap is rejected by shape.
    let oversized = request(
        &mut client,
        "att-oversized",
        "messages.attachments.get",
        json!({
            "accountId": account_id,
            "conversationId": conversation_id,
            "messageId": incoming_id,
            "attachmentId": "att-1",
            "sizeBytes": 5242881
        }),
    )
    .await;
    assert_eq!(oversized["error"]["code"], "INVALID_REQUEST");

    // A well-formed id with no downloaded file answers the upstream's
    // UserError as the shared unclassified UPSTREAM_ERROR (retryable, since
    // the connector cannot distinguish transient upstream failures).
    let missing = request(
        &mut client,
        "att-missing-file",
        "messages.attachments.get",
        json!({
            "accountId": account_id,
            "conversationId": conversation_id,
            "messageId": incoming_id,
            "attachmentId": "no-such-file",
            "sizeBytes": 24
        }),
    )
    .await;
    assert_eq!(missing["error"]["code"], "UPSTREAM_ERROR");
    assert_eq!(missing["error"]["retryable"], true);

    // Row lookups keep the addressing honest before any upstream call.
    let wrong_conversation = request(
        &mut client,
        "att-conv",
        "messages.attachments.get",
        json!({
            "accountId": account_id,
            "conversationId": "no-such-conversation",
            "messageId": incoming_id,
            "attachmentId": "att-1",
            "sizeBytes": 24
        }),
    )
    .await;
    assert_eq!(
        wrong_conversation["error"]["code"],
        "CONVERSATION_NOT_FOUND"
    );

    let wrong_message = request(
        &mut client,
        "att-msg",
        "messages.attachments.get",
        json!({
            "accountId": account_id,
            "conversationId": conversation_id,
            "messageId": "no-such-message",
            "attachmentId": "att-1",
            "sizeBytes": 24
        }),
    )
    .await;
    assert_eq!(wrong_message["error"]["code"], "MESSAGE_NOT_FOUND");

    let wrong_account = request(
        &mut client,
        "att-account",
        "messages.attachments.get",
        json!({
            "accountId": "no-such-account",
            "conversationId": conversation_id,
            "messageId": incoming_id,
            "attachmentId": "att-1",
            "sizeBytes": 24
        }),
    )
    .await;
    assert_eq!(wrong_account["error"]["code"], "ACCOUNT_NOT_FOUND");

    drop(client);
    wait_for_process_exit(engine_pid).await;
    assert_clean_exit(&mut connector).await;
}

#[tokio::test]
async fn groups_get_projects_the_synced_group_cache() {
    let temp = TempDir::new().unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let endpoint = temp.path().join("connector.sock");
    let secret_file = temp.path().join("bootstrap.secret");
    let secret = [37_u8; 32];
    write_secret_file(&secret_file, &secret);

    let mut connector = spawn_connector(temp.path(), &endpoint, &secret_file);
    wait_for_path(&endpoint).await;
    let stream = UnixStream::connect(&endpoint).await.unwrap();
    let mut client = Framed::new(stream, LinesCodec::new());
    authenticate(&mut client, &secret).await;

    let started = request(&mut client, "start", "runtime.start", json!({})).await;
    let engine_pid = started["result"]["pid"].as_u64().unwrap() as u32;
    let link = request(
        &mut client,
        "link-start",
        "link.start",
        json!({ "deviceName": "KT-Groups" }),
    )
    .await;
    let finished = request(
        &mut client,
        "link-finish",
        "link.finish",
        json!({ "linkSessionId": link["result"]["linkSessionId"] }),
    )
    .await;
    let account_id = finished["result"]["id"].as_str().unwrap().to_string();

    // finish_link already ran a best-effort sync; the explicit sync inside the
    // 60s window returns the cached counts and guarantees the cache is warm.
    let synced = request(
        &mut client,
        "contacts-sync",
        "contacts.sync",
        json!({ "accountId": account_id }),
    )
    .await;
    assert_eq!(synced["result"]["groupCount"], 1);

    // The membership-filtered cache holds exactly one group: the fixture's
    // isMember=true entry with its member count.
    let group = request(
        &mut client,
        "groups-get",
        "groups.get",
        json!({
            "accountId": account_id,
            "groupKey": "ZmFrZS1ncm91cC0x"
        }),
    )
    .await;
    assert_eq!(group["result"]["peerKey"], "ZmFrZS1ncm91cC0x");
    assert_eq!(group["result"]["title"], "Fixture Group");
    assert_eq!(group["result"]["memberCount"], 2);
    assert!(group["result"]["syncedAt"].as_u64().unwrap() > 0);

    // A group the account has left is filtered out of the cache at sync time
    // (isMember=false), so it answers the deterministic cache miss.
    let departed = request(
        &mut client,
        "groups-departed",
        "groups.get",
        json!({
            "accountId": account_id,
            "groupKey": "bm90LWEtbWVtYmVy"
        }),
    )
    .await;
    assert_eq!(departed["error"]["code"], "GROUP_NOT_FOUND");
    assert_eq!(departed["error"]["retryable"], false);

    // A contact peer key is not a group row.
    let contact_key = request(
        &mut client,
        "groups-contact-key",
        "groups.get",
        json!({
            "accountId": account_id,
            "groupKey": "+15555550101"
        }),
    )
    .await;
    assert_eq!(contact_key["error"]["code"], "GROUP_NOT_FOUND");

    // Unknown params are rejected by shape before any cache lookup.
    let invalid = request(
        &mut client,
        "groups-invalid",
        "groups.get",
        json!({
            "accountId": account_id,
            "groupKey": "ZmFrZS1ncm91cC0x",
            "unexpected": true
        }),
    )
    .await;
    assert_eq!(invalid["error"]["code"], "INVALID_REQUEST");

    // Read-only: an unknown account is a cache miss, not an upstream lookup.
    let unknown_account = request(
        &mut client,
        "groups-unknown-account",
        "groups.get",
        json!({
            "accountId": "absent-account",
            "groupKey": "ZmFrZS1ncm91cC0x"
        }),
    )
    .await;
    assert_eq!(unknown_account["error"]["code"], "ACCOUNT_NOT_FOUND");

    drop(client);
    wait_for_process_exit(engine_pid).await;
    assert_clean_exit(&mut connector).await;
}

#[tokio::test]
async fn contacts_set_local_alias_renames_a_locally_known_peer() {
    let temp = TempDir::new().unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let endpoint = temp.path().join("connector.sock");
    let secret_file = temp.path().join("bootstrap.secret");
    let secret = [43_u8; 32];
    write_secret_file(&secret_file, &secret);

    let mut connector = spawn_connector(temp.path(), &endpoint, &secret_file);
    wait_for_path(&endpoint).await;
    let stream = UnixStream::connect(&endpoint).await.unwrap();
    let mut client = Framed::new(stream, LinesCodec::new());
    authenticate(&mut client, &secret).await;

    let started = request(&mut client, "start", "runtime.start", json!({})).await;
    let engine_pid = started["result"]["pid"].as_u64().unwrap() as u32;
    let link = request(
        &mut client,
        "link-start",
        "link.start",
        json!({ "deviceName": "KT-Set-Alias" }),
    )
    .await;
    let finished = request(
        &mut client,
        "link-finish",
        "link.finish",
        json!({ "linkSessionId": link["result"]["linkSessionId"] }),
    )
    .await;
    let account_id = finished["result"]["id"].as_str().unwrap().to_string();

    // The best-effort sync at finish cached the fixture contacts, so the
    // linked account's peer (+15555550101) is a known contact.
    let renamed = request(
        &mut client,
        "alias-happy",
        "contacts.setLocalAlias",
        json!({
            "accountId": account_id,
            "peerKey": "+15555550101",
            "alias": "Alice Renamed",
            "operationId": "alias-op-1"
        }),
    )
    .await;
    assert_eq!(renamed["result"]["status"], "updated");
    // The upstream call carries recipient as a single string — the pinned
    // UpdateContactCommand reads it with getString (§4.9).
    let send_log = fs::read_to_string(
        temp.path()
            .join("signal-data")
            .join(".fixture-send-log.jsonl"),
    )
    .expect("fixture must log updateContact params");
    let update_call: Value = send_log
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .find(|params: &Value| params.get("name") == Some(&json!("Alice Renamed")))
        .expect("the updateContact must reach the upstream");
    assert_eq!(update_call["account"], "+15555550100");
    assert_eq!(update_call["recipient"], "+15555550101");
    assert!(update_call["recipient"].is_string());

    // An unknown peer is rejected locally before any upstream call.
    let unknown_peer = request(
        &mut client,
        "alias-unknown-peer",
        "contacts.setLocalAlias",
        json!({
            "accountId": account_id,
            "peerKey": "+15555559999",
            "alias": "Nobody"
        }),
    )
    .await;
    assert_eq!(unknown_peer["error"]["code"], "INVALID_REQUEST");

    // A 129-byte alias is rejected by shape.
    let oversized = request(
        &mut client,
        "alias-oversized",
        "contacts.setLocalAlias",
        json!({
            "accountId": account_id,
            "peerKey": "+15555550101",
            "alias": "x".repeat(129)
        }),
    )
    .await;
    assert_eq!(oversized["error"]["code"], "INVALID_REQUEST");

    // Unknown params are rejected by shape.
    let invalid = request(
        &mut client,
        "alias-invalid",
        "contacts.setLocalAlias",
        json!({
            "accountId": account_id,
            "peerKey": "+15555550101",
            "alias": "Still Alice",
            "unexpected": true
        }),
    )
    .await;
    assert_eq!(invalid["error"]["code"], "INVALID_REQUEST");

    // An unknown account is answered without touching the upstream.
    let absent_account = request(
        &mut client,
        "alias-absent-account",
        "contacts.setLocalAlias",
        json!({
            "accountId": "no-such-account",
            "peerKey": "+15555550101",
            "alias": "Alice"
        }),
    )
    .await;
    assert_eq!(absent_account["error"]["code"], "ACCOUNT_NOT_FOUND");

    drop(client);
    wait_for_process_exit(engine_pid).await;
    assert_clean_exit(&mut connector).await;
}

fn write_secret_file(path: &Path, secret: &[u8; 32]) {
    fs::write(path, bootstrap_payload(secret)).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
}

fn spawn_connector(root: &Path, endpoint: &Path, secret_file: &Path) -> Child {
    spawn_connector_with_delete_mode(root, endpoint, secret_file, None)
}

async fn spawn_connector_from_stdin(root: &Path, endpoint: &Path, secret: &[u8; 32]) -> Child {
    let java_home = root.join("jre");
    fs::create_dir_all(&java_home).unwrap();
    fs::write(java_home.join("release"), b"JAVA_VERSION=\"test\"\n").unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_kt-signal-connector"));
    let mut child = command
        .arg("serve")
        .arg("--endpoint")
        .arg(endpoint)
        .arg("--bootstrap-secret-stdin")
        .arg("--signal-cli")
        .arg(fixture())
        .arg("--java-home")
        .arg(&java_home)
        .arg("--signal-data-dir")
        .arg(root.join("signal-data"))
        .arg("--state-dir")
        .arg(root.join("state"))
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .env("KT_FAKE_EXPECT_JAVA_OPTS", "-Xms16m -Xmx384m")
        .env("KT_FAKE_EXPECT_JAVA_HOME", &java_home)
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    stdin
        .write_all(bootstrap_payload(secret).as_bytes())
        .await
        .unwrap();
    stdin.shutdown().await.unwrap();
    child
}

fn spawn_connector_with_delete_mode(
    root: &Path,
    endpoint: &Path,
    secret_file: &Path,
    delete_mode: Option<&str>,
) -> Child {
    spawn_connector_with(root, endpoint, secret_file, delete_mode, &[])
}

fn spawn_connector_with(
    root: &Path,
    endpoint: &Path,
    secret_file: &Path,
    delete_mode: Option<&str>,
    extra_env: &[(&str, &str)],
) -> Child {
    let mut command = Command::new(env!("CARGO_BIN_EXE_kt-signal-connector"));
    command
        .arg("serve")
        .arg("--endpoint")
        .arg(endpoint)
        .arg("--bootstrap-secret-file")
        .arg(secret_file)
        .arg("--signal-cli")
        .arg(fixture())
        .arg("--signal-data-dir")
        .arg(root.join("signal-data"))
        .arg("--state-dir")
        .arg(root.join("state"))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    command
        .env("KT_FAKE_EXPECT_JAVA_OPTS", "-Xms16m -Xmx384m")
        .env("JAVA_TOOL_OPTIONS", "poison")
        .env("_JAVA_OPTIONS", "poison")
        .env("JDK_JAVA_OPTIONS", "poison");
    if let Some(delete_mode) = delete_mode {
        command.env("KT_FAKE_DELETE_MODE", delete_mode);
    }
    for (name, value) in extra_env {
        command.env(name, value);
    }
    command.spawn().unwrap()
}

fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fake-signal-cli.py")
}

/// Wait for the connector to exit and require a clean status. The connector's
/// own stderr is the only place the reason appears, so drain it into the panic
/// rather than leaving a bare `false != true`.
async fn assert_clean_exit(connector: &mut Child) {
    let status = timeout(Duration::from_secs(2), connector.wait())
        .await
        .expect("connector should exit after the host disconnects")
        .unwrap();
    if status.success() {
        return;
    }
    let mut stderr = String::new();
    if let Some(mut pipe) = connector.stderr.take() {
        let _ = pipe.read_to_string(&mut stderr).await;
    }
    panic!("connector exited with {status:?}; stderr: {stderr}");
}

async fn wait_for_path(path: &Path) {
    timeout(Duration::from_secs(3), async {
        while !path.exists() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("connector socket should appear");
}

async fn wait_for_process_exit(pid: u32) {
    timeout(Duration::from_secs(2), async {
        while process_exists(pid) {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("signal-cli fixture should exit after host disconnect");
}

fn process_exists(pid: u32) -> bool {
    std::process::Command::new("/bin/kill")
        .arg("-0")
        .arg(pid.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

async fn authenticate(client: &mut Framed<UnixStream, LinesCodec>, secret: &[u8; 32]) {
    let challenge: Value = serde_json::from_str(&client.next().await.unwrap().unwrap()).unwrap();
    let server_nonce = challenge["data"]["serverNonce"].as_str().unwrap();
    let client_nonce = hex::encode([9_u8; 32]);
    let mut mac = Hmac::<Sha256>::new_from_slice(secret).unwrap();
    mac.update(b"kt-signal-connector-v1\0");
    mac.update(server_nonce.as_bytes());
    mac.update(b"\0");
    mac.update(client_nonce.as_bytes());
    mac.update(b"\0");
    mac.update(API_VERSION.as_bytes());
    let proof = hex::encode(mac.finalize().into_bytes());
    client
        .send(
            json!({
                "apiVersion": API_VERSION,
                "requestId": "handshake-1",
                "method": "handshake",
                "params": { "clientNonce": client_nonce, "proof": proof }
            })
            .to_string(),
        )
        .await
        .unwrap();
    let response: Value = serde_json::from_str(&client.next().await.unwrap().unwrap()).unwrap();
    assert_eq!(response["result"]["apiVersion"], API_VERSION);
}

async fn reject_authentication(client: &mut Framed<UnixStream, LinesCodec>) {
    let challenge: Value = serde_json::from_str(&client.next().await.unwrap().unwrap()).unwrap();
    assert_eq!(challenge["event"], "runtime.challenge");
    client
        .send(
            json!({
                "apiVersion": API_VERSION,
                "requestId": "bad-handshake",
                "method": "handshake",
                "params": {
                    "clientNonce": hex::encode([9_u8; 32]),
                    "proof": hex::encode([0_u8; 32])
                }
            })
            .to_string(),
        )
        .await
        .unwrap();
    let response: Value = serde_json::from_str(&client.next().await.unwrap().unwrap()).unwrap();
    assert_eq!(response["error"]["code"], "AUTHENTICATION_FAILED");
}

async fn request(
    client: &mut Framed<UnixStream, LinesCodec>,
    request_id: &str,
    method: &str,
    params: Value,
) -> Value {
    send_request_frame(client, request_id, method, params).await;
    loop {
        let response: Value = serde_json::from_str(&client.next().await.unwrap().unwrap()).unwrap();
        if response.get("requestId").and_then(Value::as_str) == Some(request_id) {
            return response;
        }
    }
}

async fn send_request_frame(
    client: &mut Framed<UnixStream, LinesCodec>,
    request_id: &str,
    method: &str,
    params: Value,
) {
    client
        .send(
            json!({
                "apiVersion": API_VERSION,
                "requestId": request_id,
                "method": method,
                "params": params
            })
            .to_string(),
        )
        .await
        .unwrap();
}
