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
use tokio::io::AsyncWriteExt;
use tokio::net::UnixStream;
use tokio::process::{Child, Command};
use tokio::time::{sleep, timeout};
use tokio_util::codec::{Framed, LinesCodec};

const API_VERSION: &str = "1.0";

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
    let status = timeout(Duration::from_secs(2), connector.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(status.success());

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
    let status = timeout(Duration::from_secs(2), connector.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(status.success());
}

#[tokio::test]
async fn phase2_link_receive_send_and_idempotent_text() {
    let temp = TempDir::new().unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let endpoint = temp.path().join("connector.sock");
    let secret_file = temp.path().join("bootstrap.secret");
    let secret = [7_u8; 32];
    fs::write(&secret_file, hex::encode(secret)).unwrap();
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
    let status = timeout(Duration::from_secs(2), connector.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(status.success());

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
    assert!(compatible_delete.get("result").is_some());

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
    let status = timeout(Duration::from_secs(2), connector.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(status.success());
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
    let status = timeout(Duration::from_secs(2), connector.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(status.success());
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
    let status = timeout(Duration::from_secs(2), connector.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(status.success());
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
    let status = timeout(Duration::from_secs(2), connector.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(status.success());
}

#[tokio::test]
async fn account_delete_unknown_is_reconciled_only_on_explicit_retry() {
    let temp = TempDir::new().unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let endpoint = temp.path().join("connector.sock");
    let secret_file = temp.path().join("bootstrap.secret");
    let secret = [8_u8; 32];
    fs::write(&secret_file, hex::encode(secret)).unwrap();
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
    assert!(idempotent.get("result").is_some());
    let accounts = request(&mut client, "accounts", "accounts.list", json!({})).await;
    assert!(accounts["result"].as_array().unwrap().is_empty());

    drop(client);
    let status = timeout(Duration::from_secs(2), connector.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(status.success());
}

fn write_secret_file(path: &Path, secret: &[u8; 32]) {
    fs::write(path, hex::encode(secret)).unwrap();
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
        .write_all(hex::encode(secret).as_bytes())
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
    command.spawn().unwrap()
}

fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fake-signal-cli.py")
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
