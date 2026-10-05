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

    // Fixture finishLink emits one receive notification (+15555550101) after
    // the JSON-RPC result. Contract revision 1.14: the inline contacts.sync
    // materializes skeletons for Alice, Bob, and the fixture group; contract
    // 1.43 (§4.42) adds the linked account's own Note to Self skeleton, so
    // the listing has 4 rows — the messaged conversation first (history
    // ordering), then the empty skeletons.
    sleep(Duration::from_millis(50)).await;
    let conversations = request(
        &mut client,
        "conv-2",
        "conversations.list",
        json!({ "accountId": account_id, "limit": 50 }),
    )
    .await;
    let items = conversations["result"]["items"].as_array().unwrap();
    assert_eq!(items.len(), 4);
    // The messaged conversation first; its title follows the sync cache's
    // profile-name preference ("Alice Example" over the envelope peer name).
    assert_eq!(items[0]["title"], "Alice Example");
    assert_eq!(items[0]["type"], "direct");
    let conversation_id = items[0]["id"].as_str().unwrap().to_string();

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
    // inside the 60s window returns the cached counts. Contract 1.43 (§4.42):
    // the self entry from listContacts joins the cache — 3 contacts + 1 group.
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
    // The self entry rides the contact cache since contract 1.43; its title
    // material is the account's own profile name.
    assert_eq!(items.len(), 4);
    let self_contact = items
        .iter()
        .find(|item| item["peerKey"] == "+15555550100")
        .unwrap();
    assert_eq!(self_contact["kind"], "contact");
    assert_eq!(self_contact["title"], "Test User");
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
    // Contract 1.43 (§4.42): every summary row carries isSelf; the contact
    // send's row is a peer, and exactly the own direct chat is marked.
    assert_eq!(created["isSelf"], false);
    let conv_items = conversations["result"]["items"].as_array().unwrap();
    let self_rows: Vec<_> = conv_items
        .iter()
        .filter(|item| item["isSelf"] == true)
        .collect();
    assert_eq!(self_rows.len(), 1);
    assert_eq!(self_rows[0]["type"], "direct");
    assert_eq!(self_rows[0]["title"], "Test User");

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

/// Contract 1.43 (§4.42) end to end: the contacts sync keeps the linked
/// account's own entry, `conversations.list` marks exactly that skeleton
/// `isSelf`, and `messages.sendText` addressed to the own number takes the
/// ordinary direct-chat path — the upstream `send` dispatch carries the self
/// recipient verbatim (the pinned upstream routes it to the Note-to-Self
/// recipient, from where it mirrors back to every linked device). The
/// connector adds no interception and no copy.
#[tokio::test]
async fn note_to_self_skeleton_marks_the_wire_and_send_reaches_the_upstream() {
    let temp = TempDir::new().unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let endpoint = temp.path().join("connector.sock");
    let secret_file = temp.path().join("bootstrap.secret");
    let secret = [12_u8; 32];
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
        json!({ "deviceName": "KT-NoteToSelf" }),
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

    // The cached-counts answer proves the sync transaction (rows + marker)
    // committed, so the skeletons below are deterministic.
    let synced = request(
        &mut client,
        "contacts-sync",
        "contacts.sync",
        json!({ "accountId": account_id }),
    )
    .await;
    assert_eq!(synced["result"]["contactCount"], 3);

    let listed = request(
        &mut client,
        "conv-list-1",
        "conversations.list",
        json!({ "accountId": account_id, "limit": 50 }),
    )
    .await;
    let items = listed["result"]["items"].as_array().unwrap();
    let self_rows: Vec<_> = items.iter().filter(|item| item["isSelf"] == true).collect();
    assert_eq!(
        self_rows.len(),
        1,
        "exactly the own direct chat is marked: {items:?}"
    );
    let self_row = &self_rows[0];
    assert_eq!(self_row["type"], "direct");
    // Title material is the account's own profile name; the desktop
    // localizes the Note to Self presentation from the marker.
    assert_eq!(self_row["title"], "Test User");
    assert_eq!(self_rows[0]["muted"], false);
    assert_eq!(self_rows[0]["pinned"], false);
    let conversation_id = self_row["id"].as_str().unwrap().to_string();

    // Send-to-self by peer key: the ordinary direct path, no connector
    // interception — the response settles on the same conversation the
    // skeleton materialized (stable identity on both paths).
    let sent = request(
        &mut client,
        "send-self",
        "messages.sendText",
        json!({
            "accountId": account_id,
            "kind": "direct",
            "peerKey": "+15555550100",
            "text": "note to myself",
            "clientRequestId": "self-send-1"
        }),
    )
    .await;
    assert_eq!(sent["result"]["status"], "sent");
    assert_eq!(sent["result"]["conversationId"], conversation_id);

    // The upstream dispatch carries the self recipient verbatim.
    let send_log = fs::read_to_string(
        temp.path()
            .join("signal-data")
            .join(".fixture-send-log.jsonl"),
    )
    .expect("fixture must log the dispatch");
    let dispatch: Value = send_log
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .find(|entry: &Value| entry["message"] == "note to myself")
        .expect("the self send must reach the upstream engine");
    assert_eq!(dispatch["account"], "+15555550100");
    assert_eq!(dispatch["recipient"], json!(["+15555550100"]));

    // The row landed on the self conversation and the marker stays.
    let relisted = request(
        &mut client,
        "conv-list-2",
        "conversations.list",
        json!({ "accountId": account_id, "limit": 50 }),
    )
    .await;
    let after = relisted["result"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["id"] == conversation_id)
        .unwrap()
        .clone();
    assert_eq!(after["isSelf"], true);
    assert_eq!(after["lastMessagePreview"], "note to myself");

    drop(client);
    wait_for_process_exit(engine_pid).await;
    assert_clean_exit(&mut connector).await;
}

/// Contract 1.44 (§4.43) end to end: the compose contact list marks exactly
/// the linked account's own entry `isSelf`, the bare bool is always
/// serialized on every row (no skip-when-false key absence), and the marker
/// is same-source with the conversations face — both lists derive from the
/// one shared number predicate and mark the same entry, so a desktop
/// cross-referencing a list row against its conversation skeleton sees the
/// same self-ness on both faces (identical title material, the §4.42 split).
#[tokio::test]
async fn contacts_list_marks_the_own_entry_is_self_same_source_with_conversations() {
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
        json!({ "deviceName": "KT-ContactsSelf" }),
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

    // The cached-counts answer proves the sync transaction committed, so the
    // listed rows below are deterministic.
    let synced = request(
        &mut client,
        "contacts-sync",
        "contacts.sync",
        json!({ "accountId": account_id }),
    )
    .await;
    assert_eq!(synced["result"]["contactCount"], 3);

    let listed = request(
        &mut client,
        "contacts-list",
        "contacts.list",
        json!({ "accountId": account_id, "limit": 50 }),
    )
    .await;
    let contact_rows = listed["result"]["items"].as_array().unwrap();
    let self_contacts: Vec<_> = contact_rows
        .iter()
        .filter(|row| row["isSelf"] == true)
        .collect();
    assert_eq!(self_contacts.len(), 1, "contact rows: {contact_rows:?}");
    assert_eq!(self_contacts[0]["kind"], "contact");
    assert_eq!(self_contacts[0]["peerKey"], "+15555550100");
    assert_eq!(self_contacts[0]["title"], "Test User");
    // The bare bool is always serialized: every row carries the key, and
    // every non-self row — peer contacts and the group alike — reads false.
    for row in contact_rows {
        assert!(
            row.get("isSelf").is_some(),
            "isSelf must always serialize: {row:?}"
        );
        if row["peerKey"] != "+15555550100" {
            assert_eq!(row["isSelf"], false, "non-self row: {row:?}");
        }
    }

    // Same-source consistency: the conversations face marks exactly one row
    // too, and both marked rows are the same entry — the shared title
    // material (the account's own profile name) on both faces.
    let conversations = request(
        &mut client,
        "conv-list-consistency",
        "conversations.list",
        json!({ "accountId": account_id, "limit": 50 }),
    )
    .await;
    let conv_rows = conversations["result"]["items"].as_array().unwrap();
    let self_convs: Vec<_> = conv_rows
        .iter()
        .filter(|row| row["isSelf"] == true)
        .collect();
    assert_eq!(self_convs.len(), 1, "conversation rows: {conv_rows:?}");
    assert_eq!(self_convs[0]["type"], "direct");
    assert_eq!(self_convs[0]["title"], self_contacts[0]["title"]);
    for row in conv_rows {
        assert!(row.get("isSelf").is_some());
    }

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

/// Contract 1.27 end to end: envelopes carrying `sourceName` — staged in the
/// fixture's `.fixture-extra-receives.json` marker and delivered through the
/// real engine's receive stream — flow into the message rows (`senderName`),
/// the conversation summaries (`lastMessageDirection` /
/// `lastMessageAuthorName` / `lastMessageReactions`), and the per-actor
/// reaction detail on the pills (`actors` with self/name/reactedAt); a later
/// remove empties the pill and drops the summary emoji prefix, and an
/// outgoing ending flips the direction without an author name.
#[tokio::test]
async fn conversation_summary_and_reactions_carry_author_metadata() {
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

    // Stage the first batch before linking: one named group message and one
    // named group reaction onto it, both carrying the sender's display name
    // exactly like a real signal-cli receive envelope. finishLink's receive
    // burst delivers them right after the default unnamed direct receive.
    // Timestamps stay below 99 — the fixture's fixed send timestamp — so the
    // outgoing message sent at the end becomes the conversation's newest row.
    let extra_receives = temp
        .path()
        .join("signal-data")
        .join(".fixture-extra-receives.json");
    fs::create_dir_all(temp.path().join("signal-data")).unwrap();
    fs::write(
        &extra_receives,
        json!([
            {
                "source": "+15555550101",
                "sourceName": "林菲菲",
                "timestamp": 50,
                "dataMessage": {
                    "groupId": "ZmFrZS1ncm91cC0x",
                    "message": "group hello"
                }
            },
            {
                "source": "+15555550101",
                "sourceName": "林菲菲",
                "timestamp": 51,
                "dataMessage": {
                    "groupId": "ZmFrZS1ncm91cC0x",
                    "reaction": {
                        "emoji": "👍",
                        "targetAuthor": "+15555550101",
                        "targetSentTimestamp": 50,
                        "isRemove": false
                    }
                }
            }
        ])
        .to_string(),
    )
    .unwrap();

    let link = request(
        &mut client,
        "link-start",
        "link.start",
        json!({ "deviceName": "KT-Summary-Meta" }),
    )
    .await;
    send_request_frame(
        &mut client,
        "link-finish",
        "link.finish",
        json!({ "linkSessionId": link["result"]["linkSessionId"] }),
    )
    .await;

    // Drain the finish burst: the link answer, the group message upsert, and
    // the add-state summary change (reaction emoji on the new last message).
    let mut account_id: Option<String> = None;
    let mut group_conversation_id: Option<String> = None;
    let mut group_message_id: Option<String> = None;
    timeout(Duration::from_secs(5), async {
        while account_id.is_none() || group_conversation_id.is_none() || group_message_id.is_none()
        {
            let frame: Value =
                serde_json::from_str(&client.next().await.unwrap().unwrap()).unwrap();
            if frame.get("requestId").and_then(Value::as_str) == Some("link-finish") {
                account_id = Some(frame["result"]["id"].as_str().unwrap().to_string());
            }
            if frame.get("event").and_then(Value::as_str) == Some("message.upserted")
                && frame["data"]["sentAt"] == 50
            {
                group_message_id = Some(frame["data"]["id"].as_str().unwrap().to_string());
            }
            if frame.get("event").and_then(Value::as_str) == Some("conversation.changed")
                && frame["data"]["type"] == "group"
                && frame["data"]["lastMessageReactions"] == json!(["👍"])
            {
                group_conversation_id = Some(frame["data"]["id"].as_str().unwrap().to_string());
            }
        }
    })
    .await
    .expect("the finish burst must deliver the named group message and reaction");
    let account_id = account_id.unwrap();
    let group_conversation_id = group_conversation_id.unwrap();
    let group_message_id = group_message_id.unwrap();

    // Persisted add-state projection: direction incoming, captured author
    // name, reaction emoji on the group row; the unnamed direct row from the
    // default receive carries a direction but no author name.
    let conversations = request(
        &mut client,
        "conv-summary",
        "conversations.list",
        json!({ "accountId": account_id, "limit": 50 }),
    )
    .await;
    let items = conversations["result"]["items"].as_array().unwrap();
    let group_row = items
        .iter()
        .find(|item| item["id"] == group_conversation_id.as_str())
        .unwrap();
    assert_eq!(group_row["lastMessageDirection"], "incoming");
    assert_eq!(group_row["lastMessageAuthorName"], "林菲菲");
    assert_eq!(group_row["lastMessageReactions"], json!(["👍"]));
    assert!(group_row.get("lastMessageStatus").is_none());
    let direct_row = items
        .iter()
        .find(|item| item["type"] == "direct" && item.get("lastMessageDirection").is_some())
        .unwrap();
    assert_eq!(direct_row["lastMessageDirection"], "incoming");
    assert!(direct_row.get("lastMessageAuthorName").is_none());
    assert!(direct_row.get("lastMessageStatus").is_none());

    // The message row carries the author name and the per-actor pill detail
    // the official ReactionViewer renders.
    let messages = request(
        &mut client,
        "messages-meta",
        "messages.list",
        json!({
            "accountId": account_id,
            "conversationId": group_conversation_id,
            "limit": 10
        }),
    )
    .await;
    let row = messages["result"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["id"] == group_message_id.as_str())
        .unwrap();
    assert_eq!(row["senderName"], "林菲菲");
    assert_eq!(row["reactions"][0]["emoji"], "👍");
    assert_eq!(row["reactions"][0]["count"], 1);
    assert_eq!(row["reactions"][0]["mine"], false);
    let actors = row["reactions"][0]["actors"].as_array().unwrap();
    assert_eq!(actors.len(), 1);
    assert_eq!(actors[0]["self"], false);
    assert_eq!(actors[0]["name"], "林菲菲");
    assert!(actors[0]["reactedAt"].is_u64());

    // Append the reaction remove and deliver it through a later upstream
    // call: the fixture flushes pending extra receives before answering
    // sendTyping, and a typing indicator changes no conversation state.
    let mut envelopes: Vec<Value> =
        serde_json::from_str(&fs::read_to_string(&extra_receives).unwrap()).unwrap();
    envelopes.push(json!({
        "source": "+15555550101",
        "sourceName": "林菲菲",
        "timestamp": 52,
        "dataMessage": {
            "groupId": "ZmFrZS1ncm91cC0x",
            "reaction": {
                "emoji": "👍",
                "targetAuthor": "+15555550101",
                "targetSentTimestamp": 50,
                "isRemove": true
            }
        }
    }));
    fs::write(&extra_receives, serde_json::to_string(&envelopes).unwrap()).unwrap();

    send_request_frame(
        &mut client,
        "typing-flush",
        "presence.setTypingMessage",
        json!({
            "accountId": account_id,
            "conversationId": group_conversation_id
        }),
    )
    .await;
    let mut remove_seen = false;
    timeout(Duration::from_secs(5), async {
        while !remove_seen {
            let frame: Value =
                serde_json::from_str(&client.next().await.unwrap().unwrap()).unwrap();
            if frame.get("event").and_then(Value::as_str) == Some("conversation.changed")
                && frame["data"]["id"] == group_conversation_id.as_str()
                && frame["data"].get("lastMessageReactions").is_none()
            {
                remove_seen = true;
            }
        }
    })
    .await
    .expect("the reaction remove must clear the summary emoji prefix");

    // Removal semantics: the pill list is always serialized and empties
    // entirely, and the summary row keeps direction and author but loses the
    // reaction prefix.
    let messages = request(
        &mut client,
        "messages-meta-removed",
        "messages.list",
        json!({
            "accountId": account_id,
            "conversationId": group_conversation_id,
            "limit": 10
        }),
    )
    .await;
    let row = messages["result"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["id"] == group_message_id.as_str())
        .unwrap();
    assert!(row["reactions"].as_array().unwrap().is_empty());
    let conversations = request(
        &mut client,
        "conv-summary-removed",
        "conversations.list",
        json!({ "accountId": account_id, "limit": 50 }),
    )
    .await;
    let group_row = conversations["result"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["id"] == group_conversation_id.as_str())
        .unwrap();
    assert_eq!(group_row["lastMessageDirection"], "incoming");
    assert_eq!(group_row["lastMessageAuthorName"], "林菲菲");
    assert!(group_row.get("lastMessageReactions").is_none());

    // An outgoing ending flips the direction and drops the author name: the
    // client renders its own self label.
    let sent = request(
        &mut client,
        "send-group-summary",
        "messages.sendText",
        json!({
            "accountId": account_id,
            "conversationId": group_conversation_id,
            "text": "summary outgoing",
            "clientRequestId": "summary-send-1"
        }),
    )
    .await;
    assert_eq!(sent["result"]["status"], "sent");
    let conversations = request(
        &mut client,
        "conv-summary-outgoing",
        "conversations.list",
        json!({ "accountId": account_id, "limit": 50 }),
    )
    .await;
    let group_row = conversations["result"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["id"] == group_conversation_id.as_str())
        .unwrap();
    assert_eq!(group_row["lastMessageDirection"], "outgoing");
    assert!(group_row.get("lastMessageAuthorName").is_none());
    // Contract 1.28: the completed send carries its row status into the
    // summary — the same vocabulary the message rows use.
    assert_eq!(group_row["lastMessageStatus"], "sent");

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
async fn incoming_mentions_project_author_aci_on_the_wire_row() {
    let temp = TempDir::new().unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let endpoint = temp.path().join("connector.sock");
    let secret_file = temp.path().join("bootstrap.secret");
    let secret = [41_u8; 32];
    write_secret_file(&secret_file, &secret);

    // Two inbound mention envelopes injected through the runtime marker: one
    // signal-cli-shaped (number + uuid), one engine-mode-shaped (the ACI
    // under the number key). The row projection is the §4.40 desktop
    // contract: author unchanged, authorAci only when honestly known.
    // The marker is written only AFTER link.finish (the established
    // convention, see the view-once chain test): injection is single-shot
    // (the fake engine deletes the marker after emitting), so an envelope
    // emitted before the account is registered is dropped fail-closed and
    // the test would flap on load.
    let signal_data = temp.path().join("signal-data");
    fs::create_dir_all(&signal_data).unwrap();

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
        json!({ "deviceName": "KT-MentionAci" }),
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

    fs::write(
        signal_data.join(".fixture-emit-envelopes.json"),
        json!({ "envelopes": [
            {
                "source": "+15555550101",
                "sourceName": "Alice",
                "timestamp": 600,
                "dataMessage": {
                    "message": "hello @you",
                    "mentions": [{
                        "number": "+15555550101",
                        "uuid": "0b7fca57-1234-4d0e-9b0f-4f6c1f8a2e10",
                        "start": 6,
                        "length": 3,
                    }],
                },
            },
            {
                "source": "+15555550101",
                "sourceName": "Alice",
                "timestamp": 601,
                "dataMessage": {
                    "message": "engine face",
                    "mentions": [{
                        "number": "d43e8ea0-3f6c-4a57-8a5c-51f83f8e5df1",
                        "start": 0,
                        "length": 6,
                    }],
                },
            },
        ] })
        .to_string(),
    )
    .unwrap();

    // The injected envelopes land as rows; poll the history read until both
    // mention rows are visible (each poll drains any interleaved events).
    // 全量并发下引擎/连接器处理信封需要真实时间，轮询间隔给处理让路
    // （仓内既有轮询惯例，见 groups/history 各用例的 20-50ms sleep）。
    let mut rows: Vec<Value> = Vec::new();
    for attempt in 0..200 {
        sleep(Duration::from_millis(20)).await;
        let _ = request(
            &mut client,
            &format!("poll-{attempt}"),
            "runtime.status",
            json!({}),
        )
        .await;
        let listed = request(
            &mut client,
            &format!("list-{attempt}"),
            "messages.search",
            json!({ "accountId": account_id, "query": "e", "limit": 50 }),
        )
        .await;
        rows = listed["result"]["items"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|row| row.get("mentions").is_some())
            .cloned()
            .collect();
        if rows.len() == 2 {
            break;
        }
    }
    assert_eq!(rows.len(), 2, "both mention envelopes landed: {rows:?}");
    for row in &rows {
        let mentions = row["mentions"].as_array().expect("mentions on the wire");
        assert_eq!(mentions.len(), 1);
        match row["sentAt"].as_u64().unwrap() {
            600 => {
                assert_eq!(mentions[0]["author"], "+15555550101");
                assert_eq!(
                    mentions[0]["authorAci"],
                    "0b7fca57-1234-4d0e-9b0f-4f6c1f8a2e10"
                );
                assert_eq!(mentions[0]["start"], 6);
                assert_eq!(mentions[0]["length"], 3);
            }
            601 => {
                assert_eq!(
                    mentions[0]["author"],
                    "d43e8ea0-3f6c-4a57-8a5c-51f83f8e5df1"
                );
                assert_eq!(
                    mentions[0]["authorAci"], "d43e8ea0-3f6c-4a57-8a5c-51f83f8e5df1",
                    "the engine-mode ACI promotes by shape"
                );
            }
            other => panic!("unexpected row timestamp {other}"),
        }
    }

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
    // Contract 1.40 (§4.39): the roster the sync captured projects beside
    // memberCount — the number-based self marker on the linked account's own
    // entry, the contacts-cache name plus the upstream uuid and admin mark on
    // Alice's. Contract 1.43 (§4.42): the self entry itself now lives in the
    // contacts cache, so its read-time name resolves to the account's own
    // profile name material (the official roster has no self name special
    // case either).
    assert_eq!(
        group["result"]["members"],
        json!([
            {"id": "+15555550100", "name": "Test User", "self": true},
            {
                "id": "+15555550101",
                "uuid": "0b7fca57-1234-4d0e-9b0f-4f6c1f8a2e10",
                "name": "Alice Example",
                "admin": true,
            },
        ])
    );

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

#[tokio::test]
async fn presence_set_typing_message_signals_ephemeral_upstream_state() {
    let temp = TempDir::new().unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let endpoint = temp.path().join("connector.sock");
    let secret_file = temp.path().join("bootstrap.secret");
    let secret = [47_u8; 32];
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
        json!({ "deviceName": "KT-Typing" }),
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

    // A direct conversation to type into (sendText creates the history row).
    let seeded = request(
        &mut client,
        "typing-seed",
        "messages.sendText",
        json!({
            "accountId": account_id,
            "kind": "direct",
            "peerKey": "+15555550101",
            "text": "typing target",
            "clientRequestId": "typing-seed-1"
        }),
    )
    .await;
    let conversation_id = seeded["result"]["conversationId"]
        .as_str()
        .unwrap()
        .to_string();

    // Start typing: stop defaults to false and is still sent explicitly (§4.10).
    let typing = request(
        &mut client,
        "typing-start",
        "presence.setTypingMessage",
        json!({
            "accountId": account_id,
            "conversationId": conversation_id,
            "operationId": "typing-op-1"
        }),
    )
    .await;
    assert_eq!(typing["result"]["status"], "sent");

    // stop=true clears the indicator early.
    let stopped = request(
        &mut client,
        "typing-stop",
        "presence.setTypingMessage",
        json!({
            "accountId": account_id,
            "conversationId": conversation_id,
            "stop": true
        }),
    )
    .await;
    assert_eq!(stopped["result"]["status"], "sent");

    // A group conversation addresses groupId instead of recipient.
    let group_seeded = request(
        &mut client,
        "typing-group-seed",
        "messages.sendText",
        json!({
            "accountId": account_id,
            "kind": "group",
            "peerKey": "ZmFrZS1ncm91cC0x",
            "text": "group typing target",
            "clientRequestId": "typing-seed-2"
        }),
    )
    .await;
    let group_conversation_id = group_seeded["result"]["conversationId"]
        .as_str()
        .unwrap()
        .to_string();
    let group_typing = request(
        &mut client,
        "typing-group",
        "presence.setTypingMessage",
        json!({
            "accountId": account_id,
            "conversationId": group_conversation_id
        }),
    )
    .await;
    assert_eq!(group_typing["result"]["status"], "sent");

    // The exact upstream dispatch: account + explicit stop boolean + the
    // conversation addressing (§4.10). The seed sends carry no `stop` key,
    // so filtering on it isolates the typing calls.
    let send_log = fs::read_to_string(
        temp.path()
            .join("signal-data")
            .join(".fixture-send-log.jsonl"),
    )
    .expect("fixture must log sendTyping params");
    let typing_calls: Vec<Value> = send_log
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .filter(|params| params.get("stop").is_some())
        .collect();
    assert_eq!(typing_calls.len(), 3, "every typing call must be logged");
    assert_eq!(typing_calls[0]["account"], "+15555550100");
    assert_eq!(typing_calls[0]["stop"], json!(false));
    assert_eq!(typing_calls[0]["recipient"], json!(["+15555550101"]));
    assert_eq!(typing_calls[1]["stop"], json!(true));
    assert_eq!(typing_calls[1]["recipient"], json!(["+15555550101"]));
    assert_eq!(typing_calls[2]["groupId"], json!("ZmFrZS1ncm91cC0x"));
    assert!(typing_calls[2].get("recipient").is_none());

    // A missing conversation answers CONVERSATION_NOT_FOUND without an
    // upstream call.
    let missing = request(
        &mut client,
        "typing-missing",
        "presence.setTypingMessage",
        json!({
            "accountId": account_id,
            "conversationId": "no-such-conversation"
        }),
    )
    .await;
    assert_eq!(missing["error"]["code"], "CONVERSATION_NOT_FOUND");

    // Unknown params are rejected by shape.
    let invalid = request(
        &mut client,
        "typing-invalid",
        "presence.setTypingMessage",
        json!({
            "accountId": account_id,
            "conversationId": conversation_id,
            "unexpected": true
        }),
    )
    .await;
    assert_eq!(invalid["error"]["code"], "INVALID_REQUEST");

    // An unknown account is answered without touching the upstream.
    let absent_account = request(
        &mut client,
        "typing-absent-account",
        "presence.setTypingMessage",
        json!({
            "accountId": "no-such-account",
            "conversationId": conversation_id
        }),
    )
    .await;
    assert_eq!(absent_account["error"]["code"], "ACCOUNT_NOT_FOUND");

    drop(client);
    wait_for_process_exit(engine_pid).await;
    assert_clean_exit(&mut connector).await;
}

/// Contract 1.33 end to end: inbound pin-family envelopes staged in the
/// fixture's `.fixture-extra-receives.json` marker drive the per-conversation
/// pinned state (conversation summaries carry `pinnedMessage`, a matching
/// unpin clears it) and the group adminDelete tombstone (`adminDeleted` on the
/// upserted row, the pinned row's removal clears the pin). The three upstream
/// methods carry the exact reaction addressing — targetAuthor follows the row
/// direction, groups address `groupId`, `pinDurationSeconds` passes through
/// only when supplied — and the guard rails answer deterministically (direct
/// adminDelete INVALID_REQUEST, out-of-range duration INVALID_REQUEST, an
/// unknown message MESSAGE_NOT_FOUND) without touching the upstream.
#[tokio::test]
async fn pin_family_round_trips_pinned_state_and_admin_delete_tombstone() {
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

    // Stage the inbound face before linking: a named group message, then a
    // peer's timed pin of it — both delivered by the finishLink receive burst.
    let extra_receives = temp
        .path()
        .join("signal-data")
        .join(".fixture-extra-receives.json");
    fs::create_dir_all(temp.path().join("signal-data")).unwrap();
    fs::write(
        &extra_receives,
        json!([
            {
                "source": "+15555550101",
                "sourceName": "林菲菲",
                "timestamp": 50,
                "dataMessage": {
                    "groupId": "ZmFrZS1ncm91cC0x",
                    "message": "group pin target"
                }
            },
            {
                "source": "+15555550101",
                "sourceName": "林菲菲",
                "timestamp": 51,
                "dataMessage": {
                    "groupId": "ZmFrZS1ncm91cC0x",
                    "pinMessage": {
                        "targetAuthor": "+15555550101",
                        "targetSentTimestamp": 50,
                        "pinDurationSeconds": 3600
                    }
                }
            }
        ])
        .to_string(),
    )
    .unwrap();

    let link = request(
        &mut client,
        "link-start",
        "link.start",
        json!({ "deviceName": "KT-Pin-Family" }),
    )
    .await;
    send_request_frame(
        &mut client,
        "link-finish",
        "link.finish",
        json!({ "linkSessionId": link["result"]["linkSessionId"] }),
    )
    .await;

    // Drain the finish burst: the link answer, the group message upsert, and
    // the pin-driven conversation change whose summary carries pinnedMessage.
    let mut account_id: Option<String> = None;
    let mut group_message_id: Option<String> = None;
    let mut pinned_change: Option<Value> = None;
    timeout(Duration::from_secs(5), async {
        while account_id.is_none() || group_message_id.is_none() || pinned_change.is_none() {
            let frame: Value =
                serde_json::from_str(&client.next().await.unwrap().unwrap()).unwrap();
            if frame.get("requestId").and_then(Value::as_str) == Some("link-finish") {
                account_id = Some(frame["result"]["id"].as_str().unwrap().to_string());
            }
            if frame.get("event").and_then(Value::as_str) == Some("message.upserted")
                && frame["data"]["sentAt"] == 50
            {
                group_message_id = Some(frame["data"]["id"].as_str().unwrap().to_string());
            }
            if frame.get("event").and_then(Value::as_str) == Some("conversation.changed")
                && frame["data"].get("pinnedMessage").is_some()
            {
                pinned_change = Some(frame);
            }
        }
    })
    .await
    .expect("the finish burst must deliver the group message and its pin");
    let account_id = account_id.unwrap();
    let group_message_id = group_message_id.unwrap();
    let pinned = pinned_change.unwrap()["data"]["pinnedMessage"].clone();
    assert_eq!(pinned["messageId"], json!(group_message_id));
    assert_eq!(pinned["targetAuthor"], json!("+15555550101"));
    assert_eq!(pinned["targetSentTimestamp"], json!(50));
    assert!(pinned["pinnedAt"].is_u64());
    assert!(pinned["expiresAt"].is_u64(), "a timed pin carries expiry");

    // The persisted summary projects the same pin bar (a reloaded window
    // renders it without replaying events).
    let conversations = request(
        &mut client,
        "conv-pinned",
        "conversations.list",
        json!({ "accountId": account_id, "limit": 50 }),
    )
    .await;
    let group_row = conversations["result"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["type"] == "group")
        .unwrap();
    assert_eq!(group_row["pinnedMessage"]["targetSentTimestamp"], json!(50));

    // Append the group adminDelete of the pinned row and deliver it through a
    // later upstream call (sendTyping flushes staged receives, changes no
    // conversation state).
    let mut envelopes: Vec<Value> =
        serde_json::from_str(&fs::read_to_string(&extra_receives).unwrap()).unwrap();
    envelopes.push(json!({
        "source": "+15555550101",
        "sourceName": "林菲菲",
        "timestamp": 53,
        "dataMessage": {
            "groupId": "ZmFrZS1ncm91cC0x",
            "adminDelete": {
                "targetAuthor": "+15555550101",
                "targetSentTimestamp": 50
            }
        }
    }));
    fs::write(&extra_receives, json!(envelopes).to_string()).unwrap();
    // Deliver it through a later upstream call: sendTyping flushes staged
    // receives before answering, and a typing indicator changes no
    // conversation state. The flush and the answer can interleave in either
    // order on the socket, so the frame is sent without waiting and the drain
    // below collects the typing answer together with the projections.
    send_request_frame(
        &mut client,
        "typing-flush",
        "presence.setTypingMessage",
        json!({
            "accountId": account_id,
            "conversationId": group_row["id"],
            "stop": true
        }),
    )
    .await;

    // The tombstone lands as a row-level adminDeleted marker (status ladder
    // untouched, body kept) and the pinned row's removal clears the pin.
    let mut typed: Option<Value> = None;
    let mut upserted: Option<Value> = None;
    let mut unpinned_change: Option<Value> = None;
    timeout(Duration::from_secs(5), async {
        while typed.is_none() || upserted.is_none() || unpinned_change.is_none() {
            let frame: Value =
                serde_json::from_str(&client.next().await.unwrap().unwrap()).unwrap();
            if frame.get("requestId").and_then(Value::as_str) == Some("typing-flush") {
                typed = Some(frame);
                continue;
            }
            if frame.get("event").and_then(Value::as_str) == Some("message.upserted")
                && frame["data"]["sentAt"] == 50
            {
                upserted = Some(frame);
                continue;
            }
            if frame.get("event").and_then(Value::as_str) == Some("conversation.changed")
                && frame["data"].get("pinnedMessage").is_none()
                && frame["data"]["type"] == "group"
            {
                unpinned_change = Some(frame);
            }
        }
    })
    .await
    .expect("the adminDelete must upsert the tombstone and clear the pin");
    assert_eq!(typed.unwrap()["result"]["status"], "sent");
    let tombstone = upserted.unwrap()["data"].clone();
    assert_eq!(tombstone["adminDeleted"], json!(true));
    assert_eq!(tombstone["status"], json!("delivered"));
    assert_eq!(tombstone["text"], json!("group pin target"));
    assert_eq!(
        unpinned_change.unwrap()["data"]["id"],
        group_row["id"].clone()
    );
    let conversations = request(
        &mut client,
        "conv-unpinned",
        "conversations.list",
        json!({ "accountId": account_id, "limit": 50 }),
    )
    .await;
    let group_row = conversations["result"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["type"] == "group")
        .unwrap();
    assert!(group_row.get("pinnedMessage").is_none());

    // Upstream face: send an own group row and pin/unpin/adminDelete it. The
    // send lane mirrors the reaction addressing — targetAuthor is the linked
    // account, groups address groupId, and the duration passes through only
    // when supplied.
    let sent = request(
        &mut client,
        "send-group-pin",
        "messages.sendText",
        json!({
            "accountId": account_id,
            "kind": "group",
            "peerKey": "ZmFrZS1ncm91cC0x",
            "text": "pin me upstream",
            "clientRequestId": "pin-send-1"
        }),
    )
    .await;
    assert_eq!(sent["result"]["status"], "sent");
    let group_conversation_id = sent["result"]["conversationId"].clone();
    let pinned_upstream = request(
        &mut client,
        "pin-upstream",
        "messages.sendPinMessage",
        json!({
            "accountId": account_id,
            "conversationId": group_conversation_id,
            "messageId": sent["result"]["id"],
            "pinDurationSeconds": 86400
        }),
    )
    .await;
    assert_eq!(pinned_upstream["result"]["status"], "sent");
    let send_log = fs::read_to_string(
        temp.path()
            .join("signal-data")
            .join(".fixture-send-log.jsonl"),
    )
    .expect("fixture must log sendPinMessage params");
    let pin_call: Value = send_log
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .find(|params: &Value| params.get("pinDurationSeconds").is_some())
        .expect("the sendPinMessage must reach the upstream");
    assert_eq!(pin_call["account"], "+15555550100");
    assert_eq!(pin_call["targetAuthor"], "+15555550100");
    assert_eq!(pin_call["targetTimestamp"], json!(99));
    assert_eq!(pin_call["pinDurationSeconds"], json!(86400));
    assert_eq!(pin_call["groupId"], json!("ZmFrZS1ncm91cC0x"));
    assert!(pin_call.get("recipient").is_none());

    // Unpin: the same addressing, no duration key on the wire.
    let unpinned_upstream = request(
        &mut client,
        "unpin-upstream",
        "messages.sendUnpinMessage",
        json!({
            "accountId": account_id,
            "conversationId": group_conversation_id,
            "messageId": sent["result"]["id"]
        }),
    )
    .await;
    assert_eq!(unpinned_upstream["result"]["status"], "sent");
    let send_log = fs::read_to_string(
        temp.path()
            .join("signal-data")
            .join(".fixture-send-log.jsonl"),
    )
    .unwrap();
    let unpin_call: Value = send_log
        .lines()
        .rev()
        .map(|line| serde_json::from_str(line).unwrap())
        .find(|params: &Value| params.get("targetTimestamp").is_some())
        .expect("the sendUnpinMessage must reach the upstream");
    assert_eq!(unpin_call["targetAuthor"], "+15555550100");
    assert_eq!(unpin_call["targetTimestamp"], json!(99));
    assert!(unpin_call.get("pinDurationSeconds").is_none());

    // Admin delete: group-only, groupId addressing on the wire.
    let admin_deleted = request(
        &mut client,
        "admin-delete-upstream",
        "messages.sendAdminDelete",
        json!({
            "accountId": account_id,
            "conversationId": group_conversation_id,
            "messageId": sent["result"]["id"]
        }),
    )
    .await;
    assert_eq!(admin_deleted["result"]["status"], "sent");
    let send_log = fs::read_to_string(
        temp.path()
            .join("signal-data")
            .join(".fixture-send-log.jsonl"),
    )
    .unwrap();
    let delete_call: Value = send_log
        .lines()
        .rev()
        .map(|line| serde_json::from_str(line).unwrap())
        .find(|params: &Value| params.get("targetTimestamp").is_some())
        .expect("the sendAdminDelete must reach the upstream");
    assert_eq!(delete_call["targetAuthor"], "+15555550100");
    assert_eq!(delete_call["targetTimestamp"], json!(99));
    assert_eq!(delete_call["groupId"], json!("ZmFrZS1ncm91cC0x"));

    // Guard rails, none of which touch the upstream: a direct chat has no
    // admin concept, the duration range is enforced fail-closed, and an
    // unknown message answers MESSAGE_NOT_FOUND.
    let direct_sent = request(
        &mut client,
        "send-direct-pin",
        "messages.sendText",
        json!({
            "accountId": account_id,
            "kind": "contact",
            "peerKey": "+15555550101",
            "text": "direct admin delete target",
            "clientRequestId": "pin-send-direct"
        }),
    )
    .await;
    assert_eq!(direct_sent["result"]["status"], "sent");
    let direct_conversation = conversations["result"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["type"] == "direct")
        .unwrap();
    let direct_delete = request(
        &mut client,
        "admin-delete-direct",
        "messages.sendAdminDelete",
        json!({
            "accountId": account_id,
            "conversationId": direct_conversation["id"],
            "messageId": direct_sent["result"]["id"]
        }),
    )
    .await;
    assert_eq!(direct_delete["error"]["code"], "INVALID_REQUEST");
    for bad in [-1_i64, 4_294_967_296_i64] {
        let out_of_range = request(
            &mut client,
            "pin-range",
            "messages.sendPinMessage",
            json!({
                "accountId": account_id,
                "conversationId": group_conversation_id,
                "messageId": sent["result"]["id"],
                "pinDurationSeconds": bad
            }),
        )
        .await;
        assert_eq!(
            out_of_range["error"]["code"], "INVALID_REQUEST",
            "pinDurationSeconds {bad} must fail closed"
        );
    }
    let unknown = request(
        &mut client,
        "pin-unknown",
        "messages.sendPinMessage",
        json!({
            "accountId": account_id,
            "conversationId": group_conversation_id,
            "messageId": "no-such-message"
        }),
    )
    .await;
    assert_eq!(unknown["error"]["code"], "MESSAGE_NOT_FOUND");
    assert_eq!(unknown["error"]["retryable"], false);

    drop(client);
    wait_for_process_exit(engine_pid).await;
    assert_clean_exit(&mut connector).await;
}

/// The receipt send-log lines (contract 1.34 delivery/read/viewed all record
/// `{account, recipient, timestamps}`), in file order.
fn receipt_log_lines(contents: &str) -> Vec<Value> {
    contents
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .filter(|params| params.get("timestamps").is_some())
        .collect()
}

/// Contract revision 1.34 end to end: the auto delivery receipt fires once per
/// genuinely-ingested incoming dataMessage (the finishLink burst covers the
/// default fixture receive, a staged direct message, and a staged group
/// message — every receipt addresses the author as a single string, never the
/// group id); `messages.markRead` without messageIds fans the whole direct
/// conversation out as one ascending per-author read receipt;
/// `messages.markViewed` with explicit ids narrows to those rows; and a group
/// markRead is the silent `sent` no-op (local group rows carry no resolvable
/// author, so zero upstream calls).
#[tokio::test]
async fn receipts_mark_read_viewed_and_auto_delivery_reach_the_upstream() {
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

    // Stage a group message and a second direct message behind the default
    // link receive: three incoming dataMessages, three auto delivery receipts.
    let signal_data = temp.path().join("signal-data");
    fs::create_dir_all(&signal_data).unwrap();
    fs::write(
        signal_data.join(".fixture-extra-receives.json"),
        json!([
            {
                "source": "+15555550101",
                "sourceName": "林菲菲",
                "timestamp": 50,
                "dataMessage": {
                    "groupId": "ZmFrZS1ncm91cC0x",
                    "message": "group text"
                }
            },
            {
                "source": "+15555550101",
                "sourceName": "林菲菲",
                "timestamp": 60,
                "dataMessage": { "message": "second direct text" }
            }
        ])
        .to_string(),
    )
    .unwrap();

    let link = request(
        &mut client,
        "link-start",
        "link.start",
        json!({ "deviceName": "KT-Receipts" }),
    )
    .await;
    send_request_frame(
        &mut client,
        "link-finish",
        "link.finish",
        json!({ "linkSessionId": link["result"]["linkSessionId"] }),
    )
    .await;

    // Drain the finish burst: the link answer plus the three message upserts
    // (42 direct, 50 group, 60 direct).
    let mut account_id: Option<String> = None;
    let mut upserted: Vec<Value> = Vec::new();
    timeout(Duration::from_secs(5), async {
        while account_id.is_none() || upserted.len() < 3 {
            let frame: Value =
                serde_json::from_str(&client.next().await.unwrap().unwrap()).unwrap();
            if frame.get("requestId").and_then(Value::as_str) == Some("link-finish") {
                account_id = Some(frame["result"]["id"].as_str().unwrap().to_string());
            }
            if frame.get("event").and_then(Value::as_str) == Some("message.upserted")
                && frame["data"]["direction"] == "incoming"
            {
                upserted.push(frame["data"].clone());
            }
        }
    })
    .await
    .expect("the finish burst must deliver three incoming messages");
    let account_id = account_id.unwrap();

    // Auto delivery receipts: one per ingested message in ingest order, the
    // author as a single-string recipient, never a group id.
    let send_log_path = signal_data.join(".fixture-send-log.jsonl");
    timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(contents) = fs::read_to_string(&send_log_path) {
                if receipt_log_lines(&contents).len() >= 3 {
                    break;
                }
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("three auto delivery receipts must reach the engine");
    let delivery_receipts = receipt_log_lines(&fs::read_to_string(&send_log_path).unwrap());
    assert_eq!(delivery_receipts.len(), 3);
    let timestamps: Vec<Vec<u64>> = delivery_receipts
        .iter()
        .map(|entry| {
            entry["timestamps"]
                .as_array()
                .unwrap()
                .iter()
                .map(|value| value.as_u64().unwrap())
                .collect()
        })
        .collect();
    assert_eq!(timestamps, [vec![42], vec![50], vec![60]]);
    for entry in &delivery_receipts {
        assert_eq!(entry["account"], "+15555550100");
        assert_eq!(entry["recipient"], "+15555550101");
        assert!(entry.get("groupId").is_none());
    }

    // The messaged direct conversation carries both direct rows (history
    // ordering puts it first, ahead of the contacts-sync skeletons).
    let conversations = request(
        &mut client,
        "conv-1",
        "conversations.list",
        json!({ "accountId": account_id, "limit": 50 }),
    )
    .await;
    let items = conversations["result"]["items"].as_array().unwrap();
    let direct = items.iter().find(|item| item["type"] == "direct").unwrap();
    let direct_id = direct["id"].as_str().unwrap().to_string();
    let group = items.iter().find(|item| item["type"] == "group").unwrap();
    let group_id = group["id"].as_str().unwrap().to_string();
    let messages = request(
        &mut client,
        "messages-1",
        "messages.list",
        json!({
            "accountId": account_id,
            "conversationId": direct_id,
            "limit": 50
        }),
    )
    .await;
    let rows = messages["result"]["items"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    let id_42 = rows.iter().find(|row| row["sentAt"] == 42).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let id_60 = rows.iter().find(|row| row["sentAt"] == 60).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Absent messageIds: the whole conversation as one ascending read
    // receipt — distinguishable from every delivery receipt by its [42, 60]
    // shape, and its log line precedes the markRead answer.
    let marked = request(
        &mut client,
        "mark-read",
        "messages.markRead",
        json!({ "accountId": account_id, "conversationId": direct_id }),
    )
    .await;
    assert_eq!(marked["result"]["status"], "sent");
    let after_read = receipt_log_lines(&fs::read_to_string(&send_log_path).unwrap());
    assert_eq!(after_read.len(), 4);
    assert_eq!(after_read[3]["timestamps"], json!([42, 60]));
    assert_eq!(after_read[3]["recipient"], "+15555550101");

    // Explicit messageIds: exactly the addressed rows, receipt order, same
    // single-author fan-out.
    let viewed = request(
        &mut client,
        "mark-viewed",
        "messages.markViewed",
        json!({
            "accountId": account_id,
            "conversationId": direct_id,
            "messageIds": [id_42, id_60]
        }),
    )
    .await;
    assert_eq!(viewed["result"]["status"], "sent");
    let after_viewed = receipt_log_lines(&fs::read_to_string(&send_log_path).unwrap());
    assert_eq!(after_viewed.len(), 5);
    assert_eq!(after_viewed[4]["timestamps"], json!([42, 60]));

    // A group markRead resolves no author locally: the trivial `sent` no-op
    // with zero new upstream calls — no receipt ever carries the group id.
    let group_marked = request(
        &mut client,
        "mark-read-group",
        "messages.markRead",
        json!({ "accountId": account_id, "conversationId": group_id }),
    )
    .await;
    assert_eq!(group_marked["result"]["status"], "sent");
    let after_group = receipt_log_lines(&fs::read_to_string(&send_log_path).unwrap());
    assert_eq!(after_group.len(), 5);
    assert!(
        fs::read_to_string(&send_log_path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .all(|params| params.get("groupId").is_none()),
        "no receipt call may address the group id"
    );

    drop(client);
    wait_for_process_exit(engine_pid).await;
    assert_clean_exit(&mut connector).await;
}

/// Contract revision 1.34 failure face: the auto delivery receipt is real
/// upstream traffic — a receipt whose timestamps carry the fixture's 425
/// sentinel crashes the engine right after link, an explicit restart recovers
/// on the same store, and the same sentinel inside `messages.markRead`
/// degrades the answer to `{status: "unknown"}` — a result, never an error,
/// with no retry.
#[tokio::test]
async fn receipt_upstream_failures_degrade_to_unknown_not_errors() {
    let temp = TempDir::new().unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let endpoint = temp.path().join("connector.sock");
    let secret_file = temp.path().join("bootstrap.secret");
    let secret = [32_u8; 32];
    write_secret_file(&secret_file, &secret);

    let mut connector = spawn_connector(temp.path(), &endpoint, &secret_file);
    wait_for_path(&endpoint).await;
    let stream = UnixStream::connect(&endpoint).await.unwrap();
    let mut client = Framed::new(stream, LinesCodec::new());
    authenticate(&mut client, &secret).await;

    let started = request(&mut client, "start", "runtime.start", json!({})).await;
    let engine_pid = started["result"]["pid"].as_u64().unwrap() as u32;

    // The staged direct message's auto delivery receipt carries 425: the
    // engine dies while the receipt pipeline is demonstrably in flight.
    let signal_data = temp.path().join("signal-data");
    fs::create_dir_all(&signal_data).unwrap();
    fs::write(
        signal_data.join(".fixture-extra-receives.json"),
        json!([
            {
                "source": "+15555550101",
                "sourceName": "林菲菲",
                "timestamp": 425,
                "dataMessage": { "message": "receipt crash target" }
            }
        ])
        .to_string(),
    )
    .unwrap();

    let link = request(
        &mut client,
        "link-start",
        "link.start",
        json!({ "deviceName": "KT-Receipt-Crash" }),
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
    wait_for_process_exit(engine_pid).await;

    // The receipt crash is recoverable: an explicit start brings a fresh
    // engine up on the same data directory (the store kept both rows).
    let restarted = request(&mut client, "restart-1", "runtime.start", json!({})).await;
    assert_eq!(restarted["result"]["state"], "running");
    let recovered_pid = restarted["result"]["pid"].as_u64().unwrap() as u32;
    assert_ne!(recovered_pid, engine_pid);

    let conversations = request(
        &mut client,
        "conv-1",
        "conversations.list",
        json!({ "accountId": account_id, "limit": 50 }),
    )
    .await;
    let direct = conversations["result"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["type"] == "direct")
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    // The default selection includes the 425 row, so the read receipt kills
    // the engine mid-call — and the request still answers, degraded to
    // `unknown`, with no error object and no retry.
    let marked = request(
        &mut client,
        "mark-read",
        "messages.markRead",
        json!({ "accountId": account_id, "conversationId": direct }),
    )
    .await;
    assert_eq!(marked["result"]["status"], "unknown");
    assert!(
        marked.get("error").is_none(),
        "a receipt failure must not fail the request: {marked:?}"
    );
    wait_for_process_exit(recovered_pid).await;
    let again = request(&mut client, "restart-2", "runtime.start", json!({})).await;
    assert_eq!(again["result"]["state"], "running");
    let final_pid = again["result"]["pid"].as_u64().unwrap() as u32;

    drop(client);
    wait_for_process_exit(final_pid).await;
    assert_clean_exit(&mut connector).await;
}

/// Contract revision 1.34 announcement and outbound mentions: the handshake
/// capabilities carry the new methods plus the `send-receipts` feature tag
/// (and the 1.35 `messages.sendSticker` method plus the `send-sticker` tag,
/// and the 1.36 browse/pin faces plus their tags), and `messages.sendText`
/// mentions reach the engine resolved — a UUID-shaped number and the cached
/// contact pass verbatim, the unresolvable number is dropped locally — while
/// an empty mention number fails the request closed.
#[tokio::test]
async fn handshake_advertises_receipts_and_mentions_reach_the_upstream() {
    let temp = TempDir::new().unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let endpoint = temp.path().join("connector.sock");
    let secret_file = temp.path().join("bootstrap.secret");
    let secret = [33_u8; 32];
    write_secret_file(&secret_file, &secret);

    let mut connector = spawn_connector(temp.path(), &endpoint, &secret_file);
    wait_for_path(&endpoint).await;
    let stream = UnixStream::connect(&endpoint).await.unwrap();
    let mut client = Framed::new(stream, LinesCodec::new());

    // Handshake with an inline capability assertion (the shared helper only
    // checks the api version).
    let challenge: Value = serde_json::from_str(&client.next().await.unwrap().unwrap()).unwrap();
    let server_nonce = challenge["data"]["serverNonce"].as_str().unwrap();
    let client_nonce = hex::encode([9_u8; 32]);
    let mut mac = Hmac::<Sha256>::new_from_slice(&secret).unwrap();
    mac.update(b"kt-signal-connector-v1\0");
    mac.update(server_nonce.as_bytes());
    mac.update(b"\0");
    mac.update(client_nonce.as_bytes());
    mac.update(b"\0");
    mac.update(API_VERSION.as_bytes());
    client
        .send(
            json!({
                "apiVersion": API_VERSION,
                "requestId": "handshake-1",
                "method": "handshake",
                "params": {
                    "clientNonce": client_nonce,
                    "proof": hex::encode(mac.finalize().into_bytes())
                }
            })
            .to_string(),
        )
        .await
        .unwrap();
    let handshake: Value = serde_json::from_str(&client.next().await.unwrap().unwrap()).unwrap();
    let capabilities = handshake["result"]["capabilities"].as_array().unwrap();
    for advertised in [
        "messages.markRead",
        "messages.markViewed",
        "send-receipts",
        "messages.sendSticker",
        "send-sticker",
        "stickerPacks.getManifest",
        "stickerPacks.getImage",
        "sticker-pack-browse",
        "conversations.getPinned",
        "conversations.setPinned",
        "conversation-pin-sync",
        "stickerPacks.getSyncs",
        "stickerPacks.setSync",
        "sticker-pack-sync",
    ] {
        assert!(
            capabilities
                .iter()
                .any(|value| value.as_str() == Some(advertised)),
            "capabilities must advertise {advertised}: {capabilities:?}"
        );
    }

    let started = request(&mut client, "start", "runtime.start", json!({})).await;
    assert_eq!(started["result"]["state"], "running");
    let engine_pid = started["result"]["pid"].as_u64().unwrap() as u32;
    let link = request(
        &mut client,
        "link-start",
        "link.start",
        json!({ "deviceName": "KT-Mentions" }),
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

    // Refresh the contacts cache outside the link debounce so the digit
    // suffix resolution has +15555550101 to match against.
    request(
        &mut client,
        "sync-1",
        "contacts.sync",
        json!({ "accountId": account_id }),
    )
    .await;

    let send_log_path = temp
        .path()
        .join("signal-data")
        .join(".fixture-send-log.jsonl");
    let mention_send = request(
        &mut client,
        "send-mentions",
        "messages.sendText",
        json!({
            "accountId": account_id,
            "kind": "contact",
            "peerKey": "+15555550101",
            "text": "ping @alice",
            "clientRequestId": "mentions-1",
            "mentions": [
                {
                    "number": "018c3f2a-1b2c-7cde-9f01-234567890abc",
                    "start": 5,
                    "length": 6
                },
                { "number": "+15555550101", "start": 5, "length": 6 },
                { "number": "+19990000001", "start": 0, "length": 3 }
            ]
        }),
    )
    .await;
    assert_eq!(mention_send["result"]["status"], "sent");
    let mentions: Vec<Value> = fs::read_to_string(&send_log_path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .filter(|params| params.get("mentions").is_some())
        .collect();
    assert_eq!(mentions.len(), 1);
    let entries = mentions[0]["mentions"].as_array().unwrap();
    assert_eq!(entries.len(), 2, "the unresolvable number must be dropped");
    assert_eq!(entries[0]["number"], "018c3f2a-1b2c-7cde-9f01-234567890abc");
    assert_eq!(entries[0]["start"], 5);
    assert_eq!(entries[0]["length"], 6);
    assert_eq!(entries[1]["number"], "+15555550101");

    let invalid = request(
        &mut client,
        "send-empty-mention",
        "messages.sendText",
        json!({
            "accountId": account_id,
            "kind": "contact",
            "peerKey": "+15555550101",
            "text": "bad mention",
            "clientRequestId": "mentions-2",
            "mentions": [{ "number": "  ", "start": 0, "length": 1 }]
        }),
    )
    .await;
    assert_eq!(invalid["error"]["code"], "INVALID_REQUEST");

    drop(client);
    wait_for_process_exit(engine_pid).await;
    assert_clean_exit(&mut connector).await;
}

/// Contract revision 1.35 stickers end to end: the upstream `send` carries
/// only the sticker object (no `message`, no `attachments` key), the pending
/// row settles with the upstream timestamp, an engine crash with the mutating
/// send in flight answers SEND_OUTCOME_UNKNOWN and leaves the row unknown
/// with no automatic retry, bounds failures reject before any row exists, and
/// an inbound sticker envelope projects onto a row with its pack identity
/// beside the metadata-only descriptor (a data-less sticker keeps the skip
/// routing).
#[tokio::test]
async fn sticker_send_and_receive_round_trip() {
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
        json!({ "deviceName": "KT-Stickers" }),
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
    let send_log_path = temp
        .path()
        .join("signal-data")
        .join(".fixture-send-log.jsonl");
    let sticker_calls = || {
        fs::read_to_string(&send_log_path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .filter(|params| params.get("sticker").is_some())
            .collect::<Vec<_>>()
    };

    // Happy path: peer addressing progressive-fills the direct chat, the
    // upstream call carries the sticker object only, and the row settles with
    // the upstream timestamp.
    let sent = request(
        &mut client,
        "sticker-1",
        "messages.sendSticker",
        json!({
            "accountId": account_id,
            "kind": "contact",
            "peerKey": "+15555550101",
            "clientRequestId": "sticker-req-1",
            "packId": "abcdef01",
            "packKey": "AAAAAAAAAAAAAAAAAAAAAA==",
            "stickerId": 7,
            "emoji": "🎉",
            "image": {
                "dataBase64": "iVBORw0KGgo=",
                "sizeBytes": 8,
                "contentType": "image/png",
                "width": 512,
                "height": 512
            }
        }),
    )
    .await;
    assert_eq!(sent["result"]["status"], "sent");
    assert_eq!(sent["result"]["sentAt"], json!(99));
    let sticker_row_id = sent["result"]["id"].clone();
    let conversation_id = sent["result"]["conversationId"].clone();

    let calls = sticker_calls();
    assert_eq!(calls.len(), 1, "exactly one upstream send: {calls:?}");
    let call = &calls[0];
    assert_eq!(call["account"], "+15555550100");
    assert!(
        call.get("message").is_none(),
        "a sticker send carries no message body: {call}"
    );
    assert!(
        call.get("attachments").is_none(),
        "a sticker send carries no regular attachments: {call}"
    );
    assert_eq!(call["recipient"], json!(["+15555550101"]));
    assert_eq!(call["sticker"]["packId"], "abcdef01");
    assert_eq!(call["sticker"]["packKey"], "AAAAAAAAAAAAAAAAAAAAAA==");
    assert_eq!(call["sticker"]["stickerId"], 7);
    assert_eq!(call["sticker"]["emoji"], "🎉");
    assert_eq!(
        call["sticker"]["image"],
        "data:image/png;base64,iVBORw0KGgo="
    );
    assert_eq!(call["sticker"]["width"], 512);
    assert_eq!(call["sticker"]["height"], 512);

    // The sent row renders the sticker identity beside the metadata-only
    // descriptor, read back from the persisted store (what a restart loads).
    let listed = request(
        &mut client,
        "list-sticker",
        "messages.list",
        json!({
            "accountId": account_id,
            "conversationId": conversation_id,
            "limit": 50
        }),
    )
    .await;
    if listed["result"]["items"].is_null() {
        panic!("list response: {listed}");
    }
    let row = listed["result"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["id"] == sticker_row_id)
        .expect("the sent sticker row is listed");
    assert_eq!(row["sticker"]["packId"], "abcdef01");
    assert_eq!(row["sticker"]["packKey"], "AAAAAAAAAAAAAAAAAAAAAA==");
    assert_eq!(row["sticker"]["stickerId"], 7);
    assert_eq!(row["sticker"]["emoji"], "🎉");
    assert_eq!(row["attachments"][0]["contentType"], "image/png");
    assert_eq!(row["attachments"][0]["size"], 8);
    assert_eq!(row["attachments"][0]["width"], 512);
    assert!(
        row.get("text").map(Value::is_null).unwrap_or(true),
        "a sticker row has no body text: {row}"
    );

    // Bounds failure: a video sticker rejects INVALID_REQUEST before any row
    // exists — the same clientRequestId stays a fresh request.
    let invalid = request(
        &mut client,
        "sticker-invalid",
        "messages.sendSticker",
        json!({
            "accountId": account_id,
            "conversationId": conversation_id,
            "clientRequestId": "sticker-req-invalid",
            "packId": "abcdef01",
            "packKey": "AAAAAAAAAAAAAAAAAAAAAA==",
            "stickerId": 8,
            "image": {
                "dataBase64": "iVBORw0KGgo=",
                "sizeBytes": 8,
                "contentType": "video/mp4"
            }
        }),
    )
    .await;
    assert_eq!(invalid["error"]["code"], "INVALID_REQUEST");
    assert_eq!(sticker_calls().len(), 1, "no upstream call for the reject");

    // Indeterminate outcome: the all-f pack id kills the engine with the
    // mutating send in flight, the request still answers SEND_OUTCOME_UNKNOWN,
    // the row settles to `unknown`, and nothing is retried.
    let unknown = request(
        &mut client,
        "sticker-unknown",
        "messages.sendSticker",
        json!({
            "accountId": account_id,
            "conversationId": conversation_id,
            "clientRequestId": "sticker-req-unknown",
            "packId": "ffffffff",
            "packKey": "AAAAAAAAAAAAAAAAAAAAAA==",
            "stickerId": 9,
            "image": {
                "dataBase64": "iVBORw0KGgo=",
                "sizeBytes": 8,
                "contentType": "image/png"
            }
        }),
    )
    .await;
    assert_eq!(unknown["error"]["code"], "SEND_OUTCOME_UNKNOWN");
    wait_for_process_exit(engine_pid).await;

    // The crash is recoverable: an explicit start brings a fresh engine up on
    // the same data directory, and the unknown row survives the reload.
    let restarted = request(&mut client, "restart-sticker", "runtime.start", json!({})).await;
    assert_eq!(restarted["result"]["state"], "running");
    let recovered_pid = restarted["result"]["pid"].as_u64().unwrap() as u32;
    assert_ne!(recovered_pid, engine_pid);

    let unknown_calls = sticker_calls();
    assert_eq!(
        unknown_calls
            .iter()
            .filter(|call| call["sticker"]["packId"] == "ffffffff")
            .count(),
        1,
        "an unknown outcome is never retried: {unknown_calls:?}"
    );
    let listed = request(
        &mut client,
        "list-unknown",
        "messages.list",
        json!({
            "accountId": account_id,
            "conversationId": conversation_id,
            "limit": 50
        }),
    )
    .await;
    let unknown_row = listed["result"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["clientRequestId"] == "sticker-req-unknown")
        .expect("the unknown sticker row survives the engine restart");
    assert_eq!(unknown_row["status"], "unknown");

    // Inbound projection: a sticker with a usable `data` pointer lands as an
    // incoming row; a data-less sticker keeps the skip routing (no row).
    let extra_receives = temp
        .path()
        .join("signal-data")
        .join(".fixture-extra-receives.json");
    fs::write(
        &extra_receives,
        json!([
            {
                "source": "+15555550102",
                "sourceName": "Ada",
                "timestamp": 777,
                "dataMessage": {
                    "sticker": {
                        "packId": "00ff00ff",
                        "packKey": "AAAAAAAAAAAAAAAAAAAAAA==",
                        "stickerId": 3,
                        "emoji": "🚀",
                        "data": {
                            "id": "att-in-1",
                            "contentType": "image/webp",
                            "size": 4096,
                            "width": 256,
                            "height": 256
                        }
                    }
                }
            },
            {
                "source": "+15555550102",
                "sourceName": "Ada",
                "timestamp": 778,
                "dataMessage": {
                    "sticker": { "packId": "00ff00ff", "stickerId": 4 }
                }
            }
        ])
        .to_string(),
    )
    .unwrap();
    // The fixture flushes pending extra receives before answering sendTyping,
    // so the flush response can outrun the receive processing: wait for the
    // conversation.changed event that the stored sticker row emits, and take
    // the progressive-filled conversation id straight from it.
    send_request_frame(
        &mut client,
        "sticker-flush",
        "presence.setTypingMessage",
        json!({
            "accountId": account_id,
            "conversationId": conversation_id
        }),
    )
    .await;
    let incoming_conversation = timeout(Duration::from_secs(5), async {
        loop {
            let frame: Value =
                serde_json::from_str(&client.next().await.unwrap().unwrap()).unwrap();
            if frame.get("event").and_then(Value::as_str) == Some("conversation.changed") {
                return frame["data"]["id"].as_str().unwrap().to_string();
            }
        }
    })
    .await
    .expect("the inbound sticker row must refresh its conversation");
    let listed = request(
        &mut client,
        "list-incoming-sticker",
        "messages.list",
        json!({
            "accountId": account_id,
            "conversationId": incoming_conversation,
            "limit": 50
        }),
    )
    .await;
    let items = listed["result"]["items"].as_array().unwrap();
    assert_eq!(
        items.len(),
        1,
        "the data-less sticker envelope stays skipped: {items:?}"
    );
    let incoming = &items[0];
    assert_eq!(incoming["direction"], "incoming");
    assert_eq!(incoming["sticker"]["packId"], "00ff00ff");
    assert_eq!(incoming["sticker"]["stickerId"], 3);
    assert_eq!(incoming["sticker"]["emoji"], "🚀");
    assert_eq!(incoming["attachments"][0]["id"], "att-in-1");
    assert_eq!(incoming["attachments"][0]["contentType"], "image/webp");
    assert_eq!(incoming["attachments"][0]["width"], 256);

    drop(client);
    wait_for_process_exit(recovered_pid).await;
    assert_clean_exit(&mut connector).await;
}

/// Contract revision 1.36 faces end to end: the two browse methods forward
/// to the engine with bounds-validated pack identity and map structured
/// engine errors (key-invalid deterministic, fetch-failed retryable); the
/// two pin-sync methods resolve the conversation and account, forward with
/// the resolved reference, answer the cloud-order list (entry cap
/// enforced), a storage outage surfaces STORAGE_UNAVAILABLE verbatim, an
/// unknown mutating pin outcome answers SEND_OUTCOME_UNKNOWN with no
/// retry, and an old-engine (method-absent) browse answers CAPABILITY_
/// UNAVAILABLE. Bounds failures reject before any upstream call.
#[tokio::test]
async fn sticker_pack_browse_and_pin_sync_round_trip() {
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

    // Browse works before any runtime start is not asserted: the engine must
    // be up for the forward. Start the runtime and link the fixture account.
    let started = request(&mut client, "start", "runtime.start", json!({})).await;
    let engine_pid = started["result"]["pid"].as_u64().unwrap() as u32;
    let link = request(
        &mut client,
        "link-start",
        "link.start",
        json!({ "deviceName": "KT-PinSync" }),
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

    // Happy-path manifest: the engine's projection comes through structured.
    let manifest = request(
        &mut client,
        "browse-manifest",
        "stickerPacks.getManifest",
        json!({
            "packId": "abcdef0123456789abcdef0123456789",
            "packKey": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
        }),
    )
    .await;
    assert_eq!(manifest["result"]["title"], "Fixture Pack");
    assert_eq!(manifest["result"]["author"], "KT Fixture");
    assert_eq!(manifest["result"]["cover"]["id"], 1);
    assert_eq!(manifest["result"]["cover"]["emoji"], "🎉");
    assert_eq!(manifest["result"]["stickers"].as_array().unwrap().len(), 2);
    assert_eq!(
        manifest["result"]["stickers"][1]["contentType"],
        "image/png"
    );
    assert_eq!(manifest["result"]["stickerCount"], 2);

    // Happy-path image: base64/size/contentType pass through.
    let image = request(
        &mut client,
        "browse-image",
        "stickerPacks.getImage",
        json!({
            "packId": "abcdef0123456789abcdef0123456789",
            "packKey": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
            "stickerId": 1
        }),
    )
    .await;
    assert_eq!(image["result"]["size"], 13);
    assert_eq!(image["result"]["contentType"], "image/webp");
    assert_eq!(image["result"]["dataBase64"], "Zml4dHVyZS1ieXRlcw==");

    // Structured engine errors: key-invalid is deterministic, fetch-failed
    // is retryable — the engine code maps without being swallowed or
    // re-invented.
    let bad_key = request(
        &mut client,
        "browse-bad-key",
        "stickerPacks.getManifest",
        json!({
            "packId": "ffffffffffffffffffffffffffffffff",
            "packKey": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
        }),
    )
    .await;
    assert_eq!(bad_key["error"]["code"], "STICKER_PACK_KEY_INVALID");
    assert_eq!(bad_key["error"]["retryable"], false);
    let fetch_fail = request(
        &mut client,
        "browse-fetch-fail",
        "stickerPacks.getManifest",
        json!({
            "packId": "00000000000000000000000000000000",
            "packKey": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
        }),
    )
    .await;
    assert_eq!(fetch_fail["error"]["code"], "STICKER_PACK_FETCH_FAILED");
    assert_eq!(fetch_fail["error"]["retryable"], true);
    let too_large = request(
        &mut client,
        "browse-too-large",
        "stickerPacks.getImage",
        json!({
            "packId": "abcdef0123456789abcdef0123456789",
            "packKey": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
            "stickerId": 413
        }),
    )
    .await;
    assert_eq!(too_large["error"]["code"], "STICKER_IMAGE_TOO_LARGE");

    // Local bounds failures reject before any upstream call: a 64-hex send-
    // projection pack id is NOT valid on the browse face (32 hex exactly),
    // and a key decoding to 31 bytes is rejected.
    let wrong_length_id = request(
        &mut client,
        "browse-wrong-id",
        "stickerPacks.getManifest",
        json!({
            "packId": "abcdef01",
            "packKey": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
        }),
    )
    .await;
    assert_eq!(wrong_length_id["error"]["code"], "INVALID_REQUEST");
    let wrong_key = request(
        &mut client,
        "browse-wrong-key",
        "stickerPacks.getImage",
        json!({
            "packId": "abcdef0123456789abcdef0123456789",
            "packKey": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==",
            "stickerId": 1
        }),
    )
    .await;
    assert_eq!(wrong_key["error"]["code"], "INVALID_REQUEST");

    // Pin sync: the cloud-order read; then a write whose read-back observes
    // the write in first position.
    let pinned = request(
        &mut client,
        "get-pinned",
        "conversations.getPinned",
        json!({ "accountId": account_id }),
    )
    .await;
    let entries = pinned["result"]["pinned"].as_array().unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0]["conversationId"], "+15555550101");
    assert_eq!(entries[0]["kind"], "contact");
    assert_eq!(entries[1]["kind"], "group");

    // Address a real conversation: materialize one with a text send first.
    let sent = request(
        &mut client,
        "pin-setup-send",
        "messages.sendText",
        json!({
            "accountId": account_id,
            "kind": "contact",
            "peerKey": "+15555550102",
            "text": "pin me",
            "clientRequestId": "pin-setup-1"
        }),
    )
    .await;
    let conversation_id = sent["result"]["conversationId"]
        .as_str()
        .unwrap()
        .to_string();

    let set_pinned = request(
        &mut client,
        "set-pinned",
        "conversations.setPinned",
        json!({
            "accountId": account_id,
            "conversationId": conversation_id,
            "kind": "contact",
            "pinned": true
        }),
    )
    .await;
    let entries = set_pinned["result"]["pinned"].as_array().unwrap();
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0]["conversationId"], "+15555550102");
    assert_eq!(entries[0]["kind"], "contact");
    // The exact upstream contract landed on the wire: resolved reference,
    // kind, boolean.
    let send_log_path = temp
        .path()
        .join("signal-data")
        .join(".fixture-send-log.jsonl");
    let pin_calls: Vec<Value> = fs::read_to_string(&send_log_path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .filter(|params| params.get("pinned").is_some())
        .collect();
    assert_eq!(pin_calls.len(), 1, "{pin_calls:?}");
    assert_eq!(pin_calls[0]["account"], "+15555550100");
    assert_eq!(pin_calls[0]["conversationId"], "+15555550102");
    assert_eq!(pin_calls[0]["kind"], "contact");
    assert_eq!(pin_calls[0]["pinned"], true);

    // Unpin: the write applies symmetrically.
    let unpinned = request(
        &mut client,
        "set-unpinned",
        "conversations.setPinned",
        json!({
            "accountId": account_id,
            "conversationId": conversation_id,
            "kind": "contact",
            "pinned": false
        }),
    )
    .await;
    let entries = unpinned["result"]["pinned"].as_array().unwrap();
    assert_eq!(entries.len(), 2);
    assert!(
        entries
            .iter()
            .all(|entry| entry["conversationId"] != "+15555550102"),
        "the unpinned conversation must be gone: {entries:?}"
    );

    // Unknown conversation: the local ladder answers before any upstream
    // call.
    let unknown_conversation = request(
        &mut client,
        "pin-unknown",
        "conversations.setPinned",
        json!({
            "accountId": account_id,
            "conversationId": "no-such-conversation",
            "kind": "contact",
            "pinned": true
        }),
    )
    .await;
    assert_eq!(
        unknown_conversation["error"]["code"],
        "CONVERSATION_NOT_FOUND"
    );

    // Bounds: an illegal kind and a missing required boolean fail closed
    // locally.
    let bad_kind = request(
        &mut client,
        "pin-bad-kind",
        "conversations.setPinned",
        json!({
            "accountId": account_id,
            "conversationId": conversation_id,
            "kind": "story",
            "pinned": true
        }),
    )
    .await;
    assert_eq!(bad_kind["error"]["code"], "INVALID_REQUEST");
    let missing_pinned = request(
        &mut client,
        "pin-missing-bool",
        "conversations.setPinned",
        json!({
            "accountId": account_id,
            "conversationId": conversation_id,
            "kind": "contact"
        }),
    )
    .await;
    assert_eq!(missing_pinned["error"]["code"], "INVALID_REQUEST");

    // Storage outage surfaces the engine's STORAGE_* code verbatim
    // (retryable): the fixture answers it on the +15555550998 peer. The
    // peer send returns the conversation id directly.
    let storage_setup = request(
        &mut client,
        "pin-storage-setup",
        "messages.sendText",
        json!({
            "accountId": account_id,
            "kind": "contact",
            "peerKey": "+15555550998",
            "text": "storage sentinel",
            "clientRequestId": "pin-storage-0"
        }),
    )
    .await;
    let storage_conversation = storage_setup["result"]["conversationId"]
        .as_str()
        .unwrap()
        .to_string();
    let storage_down = request(
        &mut client,
        "pin-storage-down",
        "conversations.setPinned",
        json!({
            "accountId": account_id,
            "conversationId": storage_conversation,
            "kind": "contact",
            "pinned": true
        }),
    )
    .await;
    assert_eq!(storage_down["error"]["code"], "STORAGE_UNAVAILABLE");
    assert_eq!(storage_down["error"]["retryable"], true);

    drop(client);
    wait_for_process_exit(engine_pid).await;
    assert_clean_exit(&mut connector).await;
}

/// conversations.setExpireTimer end to end (contract 1.42, §4.41): the
/// mutating call lands on the signal-cli face's `updateContact` with the
/// int-seconds `expiration`, the success mirrors the timer into the
/// conversation and rides `conversation.changed`, the result carries the
/// resolved value; a group conversation answers INVALID_REQUEST before any
/// upstream call and an unknown conversation answers CONVERSATION_NOT_FOUND.
#[tokio::test]
async fn conversations_set_expire_timer_round_trip_and_guards() {
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
        json!({ "deviceName": "KT-ExpireTimer" }),
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

    // Materialize a direct conversation with a text send, then set its
    // timer: the mirror lands and the summary carries it.
    let sent = request(
        &mut client,
        "timer-setup-send",
        "messages.sendText",
        json!({
            "accountId": account_id,
            "kind": "contact",
            "peerKey": "+15555550102",
            "text": "timer setup",
            "clientRequestId": "timer-setup-1"
        }),
    )
    .await;
    let conversation_id = sent["result"]["conversationId"]
        .as_str()
        .unwrap()
        .to_string();

    let set_timer = request(
        &mut client,
        "set-timer",
        "conversations.setExpireTimer",
        json!({
            "accountId": account_id,
            "conversationId": conversation_id,
            "expireSeconds": 86400
        }),
    )
    .await;
    assert_eq!(set_timer["result"]["expireTimerSeconds"], 86400);

    // The refreshed summary arrived on conversation.changed with the timer.
    let mut saw_timer_change = false;
    for _ in 0..8 {
        let frame: Value = serde_json::from_str(&client.next().await.unwrap().unwrap()).unwrap();
        if frame["event"] == "conversation.changed"
            && frame["data"]["id"] == conversation_id
            && frame["data"]["expireTimerSeconds"] == 86400
        {
            saw_timer_change = true;
            break;
        }
    }
    assert!(
        saw_timer_change,
        "conversation.changed must carry the mirrored timer"
    );

    // conversations.list agrees with the mirror.
    let listed = request(
        &mut client,
        "list-timers",
        "conversations.list",
        json!({ "accountId": account_id, "limit": 50 }),
    )
    .await;
    let row = listed["result"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["id"] == conversation_id)
        .expect("the conversation is listed")
        .clone();
    assert_eq!(row["expireTimerSeconds"], 86400);

    // The exact upstream contract landed on the wire: int-seconds
    // `expiration` with the single-string resolved recipient.
    let send_log = fs::read_to_string(
        temp.path()
            .join("signal-data")
            .join(".fixture-send-log.jsonl"),
    )
    .expect("fixture must log the dispatch");
    let timer_call: Value = send_log
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .find(|params| params.get("expiration").is_some())
        .expect("the timer call must reach updateContact");
    assert_eq!(timer_call["account"], "+15555550100");
    assert_eq!(timer_call["recipient"], "+15555550102");
    assert_eq!(timer_call["expiration"], 86400);

    // A group conversation answers INVALID_REQUEST before any upstream call:
    // the fixture links with one known group conversation in place.
    let listed = request(
        &mut client,
        "list-for-group",
        "conversations.list",
        json!({ "accountId": account_id, "limit": 50 }),
    )
    .await;
    let group_conversation = listed["result"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["type"] == "group")
        .map(|row| row["id"].as_str().unwrap().to_string())
        .expect("the fixture links with a group conversation");
    let group_timer = request(
        &mut client,
        "group-timer",
        "conversations.setExpireTimer",
        json!({
            "accountId": account_id,
            "conversationId": group_conversation,
            "expireSeconds": 60
        }),
    )
    .await;
    assert_eq!(group_timer["error"]["code"], "INVALID_REQUEST");
    assert_eq!(group_timer["error"]["retryable"], false);

    // Unknown conversation answers CONVERSATION_NOT_FOUND locally.
    let unknown = request(
        &mut client,
        "unknown-timer",
        "conversations.setExpireTimer",
        json!({
            "accountId": account_id,
            "conversationId": "no-such-conversation",
            "expireSeconds": 60
        }),
    )
    .await;
    assert_eq!(unknown["error"]["code"], "CONVERSATION_NOT_FOUND");

    // Out-of-face value and missing field fail closed locally.
    let oversized = request(
        &mut client,
        "oversized-timer",
        "conversations.setExpireTimer",
        json!({
            "accountId": account_id,
            "conversationId": conversation_id,
            "expireSeconds": 2147483648_u64
        }),
    )
    .await;
    assert_eq!(oversized["error"]["code"], "INVALID_REQUEST");
    let missing = request(
        &mut client,
        "missing-timer",
        "conversations.setExpireTimer",
        json!({
            "accountId": account_id,
            "conversationId": conversation_id
        }),
    )
    .await;
    assert_eq!(missing["error"]["code"], "INVALID_REQUEST");

    drop(client);
    wait_for_process_exit(engine_pid).await;
    assert_clean_exit(&mut connector).await;
}

/// Engine-error degradation and the unresolved-reference face (contract
/// 1.36): a codeless engine error degrades to UPSTREAM_ERROR (never a
/// partial answer), and the engine's CONVERSATION_NOT_RESOLVED passes
/// through structurally with retryable=false. The crash-with-in-flight-send
/// path for the mutating write is covered by the sentinel peer
/// +15555550999 in the fixture (os._exit(29)), exercised here to pin the
/// SEND_OUTCOME_UNKNOWN answer and the no-auto-retry discipline.
#[tokio::test]
async fn sticker_and_pin_methods_surface_engine_errors() {
    let temp = TempDir::new().unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let endpoint = temp.path().join("connector.sock");
    let secret_file = temp.path().join("bootstrap.secret");
    let secret = [41_u8; 32];
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
        json!({ "deviceName": "KT-OldEngine" }),
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

    // Codeless engine error (the 0x11 pack id sentinel): degrades to the
    // plain upstream error, never a partial manifest.
    let degraded = request(
        &mut client,
        "browse-degraded",
        "stickerPacks.getManifest",
        json!({
            "packId": "11111111111111111111111111111111",
            "packKey": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
        }),
    )
    .await;
    assert_eq!(degraded["error"]["code"], "UPSTREAM_ERROR");
    assert_eq!(degraded["error"]["retryable"], true);

    // CONVERSATION_NOT_RESOLVED passes through verbatim: the fixture
    // answers it on the +15555550997 peer (the conversation id comes
    // straight from the peer send result).
    let unresolved_setup = request(
        &mut client,
        "pin-unresolved-setup",
        "messages.sendText",
        json!({
            "accountId": account_id,
            "kind": "contact",
            "peerKey": "+15555550997",
            "text": "unresolved sentinel",
            "clientRequestId": "pin-unresolved-0"
        }),
    )
    .await;
    let unresolved_conversation = unresolved_setup["result"]["conversationId"]
        .as_str()
        .unwrap()
        .to_string();
    let unresolved = request(
        &mut client,
        "pin-unresolved",
        "conversations.setPinned",
        json!({
            "accountId": account_id,
            "conversationId": unresolved_conversation,
            "kind": "contact",
            "pinned": true
        }),
    )
    .await;
    assert_eq!(unresolved["error"]["code"], "CONVERSATION_NOT_RESOLVED");
    assert_eq!(unresolved["error"]["retryable"], false);

    // Crash with the mutating pin in flight (fixture peer +15555550999,
    // os._exit(29)): the request answers SEND_OUTCOME_UNKNOWN and nothing
    // is retried — the recovered log shows exactly one pin call for the
    // sentinel peer.
    let crash_setup = request(
        &mut client,
        "pin-crash-setup",
        "messages.sendText",
        json!({
            "accountId": account_id,
            "kind": "contact",
            "peerKey": "+15555550999",
            "text": "crash sentinel",
            "clientRequestId": "pin-crash-0"
        }),
    )
    .await;
    let crash_conversation = crash_setup["result"]["conversationId"]
        .as_str()
        .unwrap()
        .to_string();
    let crash = request(
        &mut client,
        "pin-crash",
        "conversations.setPinned",
        json!({
            "accountId": account_id,
            "conversationId": crash_conversation,
            "kind": "contact",
            "pinned": true
        }),
    )
    .await;
    assert_eq!(crash["error"]["code"], "SEND_OUTCOME_UNKNOWN");
    wait_for_process_exit(engine_pid).await;

    // Recovery: an explicit start brings the engine back; the pin write is
    // not replayed (exactly one crash call in the log).
    let restarted = request(&mut client, "restart-pin", "runtime.start", json!({})).await;
    assert_eq!(restarted["result"]["state"], "running");
    let recovered_pid = restarted["result"]["pid"].as_u64().unwrap() as u32;
    assert_ne!(recovered_pid, engine_pid);
    let send_log_path = temp
        .path()
        .join("signal-data")
        .join(".fixture-send-log.jsonl");
    let crash_calls: Vec<Value> = fs::read_to_string(&send_log_path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .filter(|params| params.get("pinned").is_some())
        .collect();
    assert_eq!(
        crash_calls
            .iter()
            .filter(|params| params["conversationId"] == "+15555550999")
            .count(),
        1,
        "an unknown pin outcome is never retried: {crash_calls:?}"
    );

    drop(client);
    wait_for_process_exit(recovered_pid).await;
    assert_clean_exit(&mut connector).await;
}

/// Contract revision 1.37: the §4.34 sticker-pack sync faces (cloud read,
/// install write with read-back, uninstall tombstone with the ignored
/// key/position dropped, local bounds, structured STORAGE_* passthrough, the
/// crash-with-in-flight-write unknown outcome and its no-retry discipline)
/// and the §4.35 sticker-quote face (sendSticker quoteMessageId resolves
/// through the same ladder as the text face — upstream quote keys beside the
/// sticker object; a rejected quote leaves no row behind).
#[tokio::test]
async fn sticker_pack_sync_and_sticker_quote_round_trip() {
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
        json!({ "deviceName": "KT-PackSync" }),
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
    let send_log_path = temp
        .path()
        .join("signal-data")
        .join(".fixture-send-log.jsonl");
    let sync_calls = || {
        fs::read_to_string(&send_log_path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .filter(|params| params.get("installed").is_some())
            .collect::<Vec<_>>()
    };

    // Initial read: one installed pack (key + position) and one tombstone,
    // projected structurally.
    let initial = request(
        &mut client,
        "sync-read",
        "stickerPacks.getSyncs",
        json!({ "accountId": account_id }),
    )
    .await;
    let packs = initial["result"]["packs"].as_array().unwrap();
    assert_eq!(packs.len(), 2);
    assert_eq!(packs[0]["packId"], "abcdef0123456789abcdef0123456789");
    assert_eq!(
        packs[0]["packKey"],
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
    );
    assert_eq!(packs[0]["position"], 0);
    assert!(packs[0]["deletedAtTimestampMs"].is_null());
    assert_eq!(packs[1]["packId"], "11111111111111111111111111112222");
    assert!(packs[1]["packKey"].is_null());
    assert!(packs[1]["position"].is_null());
    assert_eq!(packs[1]["deletedAtTimestampMs"], 1727000000000u64);

    // Install a new pack: the result is the post-write cloud state read back
    // with the new record first, and the exact upstream contract landed on
    // the wire (resolved account, key, position, installed).
    let new_pack_id = "23456789abcdef0123456789abcdef01";
    let installed = request(
        &mut client,
        "sync-install",
        "stickerPacks.setSync",
        json!({
            "accountId": account_id,
            "packId": new_pack_id,
            "packKey": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
            "installed": true,
            "position": 3
        }),
    )
    .await;
    let packs = installed["result"]["packs"].as_array().unwrap();
    assert_eq!(packs.len(), 3);
    assert_eq!(packs[0]["packId"], new_pack_id);
    assert_eq!(
        packs[0]["packKey"],
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
    );
    assert_eq!(packs[0]["position"], 3);
    assert!(packs[0]["deletedAtTimestampMs"].is_null());
    let calls = sync_calls();
    assert_eq!(calls.len(), 1, "{calls:?}");
    assert_eq!(calls[0]["account"], "+15555550100");
    assert_eq!(calls[0]["packId"], new_pack_id);
    assert_eq!(
        calls[0]["packKey"],
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
    );
    assert_eq!(calls[0]["installed"], true);
    assert_eq!(calls[0]["position"], 3);

    // Uninstall: the tombstone clears key and position (the engine stamps
    // its own clock), and the connector forwards nulls — the caller-supplied
    // packKey/position decoys are dropped, never forwarded.
    let uninstalled = request(
        &mut client,
        "sync-uninstall",
        "stickerPacks.setSync",
        json!({
            "accountId": account_id,
            "packId": new_pack_id,
            "packKey": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
            "installed": false,
            "position": 3
        }),
    )
    .await;
    let packs = uninstalled["result"]["packs"].as_array().unwrap();
    assert_eq!(packs.len(), 3);
    let tombstone = packs
        .iter()
        .find(|entry| entry["packId"] == new_pack_id)
        .unwrap();
    assert!(tombstone["packKey"].is_null());
    assert!(tombstone["position"].is_null());
    assert!(tombstone["deletedAtTimestampMs"].as_u64().unwrap() > 0);
    let calls = sync_calls();
    assert_eq!(calls.len(), 2, "{calls:?}");
    assert_eq!(calls[1]["packId"], new_pack_id);
    assert_eq!(calls[1]["installed"], false);
    assert!(calls[1]["packKey"].is_null());
    assert!(calls[1]["position"].is_null());

    // Local bounds reject before any upstream call: the 1.35 even-hex pack
    // id shape is not valid here (32 hex exactly), an install without a key
    // is refused, and a key decoding to 16 bytes fails the 32-byte rule.
    let wrong_id = request(
        &mut client,
        "sync-wrong-id",
        "stickerPacks.setSync",
        json!({
            "accountId": account_id,
            "packId": "abcdef01",
            "packKey": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
            "installed": true
        }),
    )
    .await;
    assert_eq!(wrong_id["error"]["code"], "INVALID_REQUEST");
    let missing_key = request(
        &mut client,
        "sync-missing-key",
        "stickerPacks.setSync",
        json!({
            "accountId": account_id,
            "packId": new_pack_id,
            "installed": true
        }),
    )
    .await;
    assert_eq!(missing_key["error"]["code"], "INVALID_REQUEST");
    let short_key = request(
        &mut client,
        "sync-short-key",
        "stickerPacks.setSync",
        json!({
            "accountId": account_id,
            "packId": new_pack_id,
            "packKey": "AAAAAAAAAAAAAAAAAAAAAA==",
            "installed": true
        }),
    )
    .await;
    assert_eq!(short_key["error"]["code"], "INVALID_REQUEST");
    assert_eq!(sync_calls().len(), 2, "no upstream call for the rejects");

    // Structured engine errors pass through verbatim (retryable): the
    // fixture answers them on the all-f and all-0 pack ids.
    let storage_down = request(
        &mut client,
        "sync-storage-down",
        "stickerPacks.setSync",
        json!({
            "accountId": account_id,
            "packId": "ffffffffffffffffffffffffffffffff",
            "packKey": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
            "installed": true
        }),
    )
    .await;
    assert_eq!(storage_down["error"]["code"], "STORAGE_UNAVAILABLE");
    assert_eq!(storage_down["error"]["retryable"], true);
    let storage_read = request(
        &mut client,
        "sync-storage-read",
        "stickerPacks.setSync",
        json!({
            "accountId": account_id,
            "packId": "00000000000000000000000000000000",
            "packKey": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
            "installed": true
        }),
    )
    .await;
    assert_eq!(storage_read["error"]["code"], "STORAGE_READ_FAILED");
    assert_eq!(storage_read["error"]["retryable"], true);

    // Crash with the mutating write in flight (fixture pack id all-1,
    // os._exit(30)): the request answers SEND_OUTCOME_UNKNOWN and nothing is
    // retried — after recovery the log shows exactly one crash call.
    let crash = request(
        &mut client,
        "sync-crash",
        "stickerPacks.setSync",
        json!({
            "accountId": account_id,
            "packId": "11111111111111111111111111111111",
            "packKey": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
            "installed": true
        }),
    )
    .await;
    assert_eq!(crash["error"]["code"], "SEND_OUTCOME_UNKNOWN");
    wait_for_process_exit(engine_pid).await;
    let restarted = request(&mut client, "restart-sync", "runtime.start", json!({})).await;
    assert_eq!(restarted["result"]["state"], "running");
    let recovered_pid = restarted["result"]["pid"].as_u64().unwrap() as u32;
    assert_ne!(recovered_pid, engine_pid);
    let crash_calls = sync_calls();
    assert_eq!(
        crash_calls
            .iter()
            .filter(|call| call["packId"] == "11111111111111111111111111111111")
            .count(),
        1,
        "an unknown sync outcome is never retried: {crash_calls:?}"
    );

    // §4.35 sticker quote: a text send materializes the conversation and the
    // quoted row; the sticker send quoting it carries the upstream quote keys
    // beside the sticker object and the row records the target.
    let setup = request(
        &mut client,
        "quote-setup",
        "messages.sendText",
        json!({
            "accountId": account_id,
            "kind": "contact",
            "peerKey": "+15555550102",
            "text": "quote me",
            "clientRequestId": "sticker-quote-setup"
        }),
    )
    .await;
    let quoted_message_id = setup["result"]["id"].as_str().unwrap().to_string();
    let conversation_id = setup["result"]["conversationId"]
        .as_str()
        .unwrap()
        .to_string();
    let quoted = request(
        &mut client,
        "sticker-quote",
        "messages.sendSticker",
        json!({
            "accountId": account_id,
            "conversationId": conversation_id,
            "clientRequestId": "sticker-quote-1",
            "packId": "abcdef01",
            "packKey": "AAAAAAAAAAAAAAAAAAAAAA==",
            "stickerId": 7,
            "image": {
                "dataBase64": "iVBORw0KGgo=",
                "sizeBytes": 8,
                "contentType": "image/png"
            },
            "quoteMessageId": quoted_message_id
        }),
    )
    .await;
    assert_eq!(quoted["result"]["status"], "sent");
    assert_eq!(quoted["result"]["quoteMessageId"], quoted_message_id);
    let sticker_calls: Vec<Value> = fs::read_to_string(&send_log_path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .filter(|params| params.get("sticker").is_some())
        .collect();
    let quote_call = sticker_calls
        .iter()
        .find(|call| call["sticker"]["packId"] == "abcdef01")
        .expect("the quoted sticker send reaches the upstream");
    assert_eq!(quote_call["quoteTimestamp"], 99);
    assert_eq!(quote_call["quoteAuthor"], "+15555550100");
    assert!(
        quote_call.get("message").is_none(),
        "a quoted sticker send still carries no message body: {quote_call}"
    );

    // An unknown quote target answers MESSAGE_NOT_FOUND during validation —
    // no upstream call, no pending row left behind.
    let rejected = request(
        &mut client,
        "sticker-quote-missing",
        "messages.sendSticker",
        json!({
            "accountId": account_id,
            "conversationId": conversation_id,
            "clientRequestId": "sticker-quote-2",
            "packId": "abcdef01",
            "packKey": "AAAAAAAAAAAAAAAAAAAAAA==",
            "stickerId": 8,
            "image": {
                "dataBase64": "iVBORw0KGgo=",
                "sizeBytes": 8,
                "contentType": "image/png"
            },
            "quoteMessageId": "no-such-message"
        }),
    )
    .await;
    assert_eq!(rejected["error"]["code"], "MESSAGE_NOT_FOUND");
    assert_eq!(rejected["error"]["retryable"], false);
    let listed = request(
        &mut client,
        "list-after-reject",
        "messages.list",
        json!({
            "accountId": account_id,
            "conversationId": conversation_id,
            "limit": 50
        }),
    )
    .await;
    let rows = listed["result"]["items"].as_array().unwrap();
    assert!(
        rows.iter()
            .all(|row| row["clientRequestId"] != "sticker-quote-2"),
        "a rejected quote leaves no row: {rows:?}"
    );

    drop(client);
    wait_for_process_exit(recovered_pid).await;
    assert_clean_exit(&mut connector).await;
}

/// The send-log lines one assertion cares about, re-read fresh each poll.
struct SendLog {
    contents: String,
}

impl SendLog {
    fn receipt_lines(&self) -> Vec<Value> {
        self.lines_with("timestamps")
    }

    fn view_once_open_lines(&self) -> Vec<Value> {
        self.lines_with("senderAci")
    }

    fn view_once_send_lines(&self) -> Vec<Value> {
        self.lines_with("viewOnce")
    }

    fn lines_with(&self, key: &str) -> Vec<Value> {
        self.contents
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .filter(|entry| entry.get(key).is_some())
            .collect()
    }
}

async fn read_send_log(signal_data: &Path) -> Option<SendLog> {
    fs::read_to_string(signal_data.join(".fixture-send-log.jsonl"))
        .ok()
        .map(|contents| SendLog { contents })
}

/// Frames the client until the named request's response arrives, collecting
/// every `message.viewOnceOpened` event on the way — the deterministic
/// "no further event" barrier: the connector writes events and responses to
/// the same connection in emission order, so a response proves every event
/// triggered before it was already delivered.
async fn drain_until_response_collecting_opened(
    client: &mut Framed<UnixStream, LinesCodec>,
    request_id: &str,
) -> (Value, Vec<Value>) {
    let mut opened: Vec<Value> = Vec::new();
    loop {
        let frame: Value = serde_json::from_str(&client.next().await.unwrap().unwrap()).unwrap();
        if frame.get("event").and_then(Value::as_str) == Some("message.viewOnceOpened") {
            opened.push(frame["data"].clone());
            continue;
        }
        if frame.get("requestId").and_then(Value::as_str) == Some(request_id) {
            return (frame, opened);
        }
    }
}

/// Frames the client until the first `message.viewOnceOpened` event arrives
/// and returns it — the wait side of a burn whose transition must produce
/// exactly one event (the "exactly once" side is the barrier drain).
async fn drain_until_opened_event(client: &mut Framed<UnixStream, LinesCodec>) -> Value {
    loop {
        let frame: Value = serde_json::from_str(&client.next().await.unwrap().unwrap()).unwrap();
        if frame.get("event").and_then(Value::as_str) == Some("message.viewOnceOpened") {
            return frame["data"].clone();
        }
    }
}

/// Contract revision 1.38 end to end: a `viewOnce` attachment send carries the
/// upstream flag and persists the marker row; `messages.markViewOnceOpened`
/// burns an incoming view-once row once (the official VIEWED-then-ViewOnceOpen
/// pair reaches the engine in order, one host event, the row loses its bytes
/// but keeps its metadata, a replay is a trivial no-op with no fan-out); and
/// the engine's own `syncMessage.viewOnceOpen` sync burns the sender's row
/// through the local ladder with no upstream traffic — duplicate syncs and a
/// sync naming a non-view-once row emit nothing.
#[tokio::test]
async fn view_once_send_mark_open_and_sync_burn_reach_the_upstream() {
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

    // Stage the peer's view-once media message (attachment descriptor, no
    // body — the §4.36 official shape) behind the default link receive.
    let signal_data = temp.path().join("signal-data");
    fs::create_dir_all(&signal_data).unwrap();
    fs::write(
        signal_data.join(".fixture-extra-receives.json"),
        json!([
            {
                "source": "+15555550101",
                "timestamp": 70,
                "dataMessage": {
                    "viewOnce": true,
                    "attachments": [{
                        "id": "att-view-in-1",
                        "contentType": "image/jpeg",
                        "filename": "snap.jpg",
                        "size": 2048
                    }]
                }
            }
        ])
        .to_string(),
    )
    .unwrap();

    let link = request(
        &mut client,
        "link-start",
        "link.start",
        json!({ "deviceName": "KT-ViewOnce" }),
    )
    .await;
    send_request_frame(
        &mut client,
        "link-finish",
        "link.finish",
        json!({ "linkSessionId": link["result"]["linkSessionId"] }),
    )
    .await;

    // Drain the finish burst: the link answer plus the two incoming upserts
    // (the default 42 plain receive and the staged 70 view-once receive).
    let mut account_id: Option<String> = None;
    let mut upserted: Vec<Value> = Vec::new();
    timeout(Duration::from_secs(5), async {
        while account_id.is_none() || upserted.len() < 2 {
            let frame: Value =
                serde_json::from_str(&client.next().await.unwrap().unwrap()).unwrap();
            if frame.get("requestId").and_then(Value::as_str) == Some("link-finish") {
                account_id = Some(frame["result"]["id"].as_str().unwrap().to_string());
            }
            if frame.get("event").and_then(Value::as_str) == Some("message.upserted")
                && frame["data"]["direction"] == "incoming"
            {
                upserted.push(frame["data"].clone());
            }
        }
    })
    .await
    .expect("the finish burst must deliver two incoming messages");
    let account_id = account_id.unwrap();
    let conversation_id = upserted[0]["conversationId"].as_str().unwrap().to_string();
    let view_once_row_id = upserted
        .iter()
        .find(|row| row["sentAt"] == 70)
        .expect("the staged view-once row")["id"]
        .as_str()
        .unwrap()
        .to_string();

    // C1 (§4.36): the view-once attachment send carries the upstream flag and
    // persists the marker on its own row; the fixture's send result pins the
    // row's timestamp at 99.
    let sent = request(
        &mut client,
        "view-send",
        "messages.attachments.send",
        json!({
            "accountId": account_id,
            "conversationId": conversation_id,
            "clientRequestId": "view-once-send-1",
            "dataBase64": "iVBORw0KGgo=",
            "sizeBytes": 8,
            "filename": "snap.png",
            "contentType": "image/png",
            "viewOnce": true
        }),
    )
    .await;
    assert_eq!(sent["result"]["viewOnce"], true);
    assert_eq!(sent["result"]["sentAt"], 99);
    let sent_row_id = sent["result"]["id"].as_str().unwrap().to_string();
    timeout(Duration::from_secs(5), async {
        loop {
            if let Some(log) = read_send_log(&signal_data).await {
                if !log.view_once_send_lines().is_empty() {
                    assert_eq!(log.view_once_send_lines()[0]["viewOnce"], true);
                    assert_eq!(
                        log.view_once_send_lines()[0]["recipient"],
                        json!(["+15555550101"])
                    );
                    break;
                }
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the view-once send must reach the engine with the flag");

    // The finish burst's auto delivery receipts: one per incoming dataMessage.
    timeout(Duration::from_secs(5), async {
        loop {
            if let Some(log) = read_send_log(&signal_data).await {
                if log.receipt_lines().len() >= 2 {
                    break;
                }
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("two auto delivery receipts must reach the engine");

    // C3 (§4.37): marking the incoming view-once row burns it and runs the
    // official pair — the VIEWED receipt to the author, then the open sync.
    send_request_frame(
        &mut client,
        "mark-open-1",
        "messages.markViewOnceOpened",
        json!({
            "accountId": account_id,
            "conversationId": conversation_id,
            "messageId": view_once_row_id
        }),
    )
    .await;
    let (mark, opened) = drain_until_response_collecting_opened(&mut client, "mark-open-1").await;
    assert_eq!(mark["result"]["status"], "sent");
    assert_eq!(opened.len(), 1, "exactly one burn event: {opened:?}");
    assert_eq!(opened[0]["accountId"], account_id);
    assert_eq!(opened[0]["conversationId"], conversation_id);
    assert_eq!(opened[0]["messageId"], view_once_row_id);
    assert!(opened[0]["openedAt"].as_u64().is_some());

    timeout(Duration::from_secs(5), async {
        loop {
            if let Some(log) = read_send_log(&signal_data).await {
                let receipts = log.receipt_lines();
                let opens = log.view_once_open_lines();
                if receipts.len() >= 3 && !opens.is_empty() {
                    // The third receipt line is the VIEWED (delivery receipts
                    // for rows 42 and 70 landed during the burst).
                    assert_eq!(
                        receipts[2]["recipient"], "+15555550101",
                        "receipt lines: {receipts:?}"
                    );
                    assert_eq!(receipts[2]["timestamps"], json!([70]));
                    assert_eq!(opens[0]["senderAci"], "+15555550101");
                    assert_eq!(opens[0]["timestamp"], 70);
                    break;
                }
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the VIEWED receipt and open sync must reach the engine in order");

    // The burned row keeps its metadata and carries the opened stamp.
    let listed = request(
        &mut client,
        "list-burned",
        "messages.list",
        json!({
            "accountId": account_id,
            "conversationId": conversation_id,
            "limit": 50
        }),
    )
    .await;
    let burned = listed["result"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["id"] == view_once_row_id)
        .expect("the burned row stays listed");
    assert!(burned.get("text").is_none(), "the body bytes are erased");
    assert!(burned["viewOnceOpenedAt"].as_u64().is_some());
    assert_eq!(burned["attachments"].as_array().unwrap().len(), 1);

    // The replay is a trivial no-op: `sent`, no event, no upstream legs.
    let replay = request(
        &mut client,
        "mark-open-2",
        "messages.markViewOnceOpened",
        json!({
            "accountId": account_id,
            "conversationId": conversation_id,
            "messageId": view_once_row_id
        }),
    )
    .await;
    assert_eq!(replay["result"]["status"], "sent");
    let (_, replay_opened) = send_status_barrier(&mut client, "barrier-1").await;
    assert!(
        replay_opened.is_empty(),
        "a replay emits no event: {replay_opened:?}"
    );

    // C2 (§4.37): the engine's own view-once open syncs. Three duplicates
    // naming the sender's row (timestamp 99) burn it exactly once; a fourth
    // naming the plain row 42 matches nothing. No upstream traffic escapes.
    let sync_envelope = |timestamp: u64| {
        json!({
            "source": "+15555550101",
            "timestamp": 1000,
            "syncMessage": {
                "viewOnceOpen": {
                    "senderAci": "+15555550101",
                    "timestamp": timestamp
                }
            }
        })
    };
    fs::write(
        signal_data.join(".fixture-emit-envelopes.json"),
        json!({ "envelopes": [
            sync_envelope(99),
            sync_envelope(99),
            sync_envelope(99),
            sync_envelope(42),
        ] })
        .to_string(),
    )
    .unwrap();
    let sync_opened = timeout(
        Duration::from_secs(5),
        drain_until_opened_event(&mut client),
    )
    .await
    .expect("the sync burn must emit its event");
    assert_eq!(sync_opened["messageId"], sent_row_id);
    assert_eq!(sync_opened["conversationId"], conversation_id);
    let (_, sync_settled) = send_status_barrier(&mut client, "barrier-2").await;
    assert!(
        sync_settled.is_empty(),
        "replayed syncs and non-view-once targets emit nothing: {sync_settled:?}"
    );
    // The sync path never sends upstream: the log is unchanged.
    let log = read_send_log(&signal_data).await.expect("the log exists");
    assert_eq!(
        log.receipt_lines().len(),
        3,
        "receipt lines after the sync phase: {:?}",
        log.receipt_lines()
    );
    assert_eq!(log.view_once_open_lines().len(), 1);

    // The sync-burned row carries the opened stamp too.
    let listed = request(
        &mut client,
        "list-sync-burned",
        "messages.list",
        json!({
            "accountId": account_id,
            "conversationId": conversation_id,
            "limit": 50
        }),
    )
    .await;
    let burned = listed["result"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["id"] == sent_row_id)
        .expect("the sync-burned row stays listed");
    assert!(burned["viewOnceOpenedAt"].as_u64().is_some());

    drop(client);
    wait_for_process_exit(engine_pid).await;
    assert_clean_exit(&mut connector).await;
}

/// A `runtime.status` request used as an ordering barrier: returns the
/// status response and every `message.viewOnceOpened` event that was
/// delivered before it — the connector writes frames in emission order.
async fn send_status_barrier(
    client: &mut Framed<UnixStream, LinesCodec>,
    request_id: &str,
) -> (Value, Vec<Value>) {
    send_request_frame(client, request_id, "runtime.status", json!({})).await;
    drain_until_response_collecting_opened(client, request_id).await
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
