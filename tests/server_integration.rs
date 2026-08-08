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
    let secret_file = temp.path().join("bootstrap.secret");
    let secret = [7_u8; 32];
    fs::write(&secret_file, hex::encode(secret)).unwrap();
    fs::set_permissions(&secret_file, fs::Permissions::from_mode(0o600)).unwrap();

    let mut connector = spawn_connector(temp.path(), &endpoint, &secret_file);
    wait_for_path(&endpoint).await;
    assert!(!secret_file.exists());
    assert_eq!(
        fs::metadata(&endpoint).unwrap().permissions().mode() & 0o777,
        0o600
    );

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

    let stream = UnixStream::connect(&endpoint).await.unwrap();
    let mut client = Framed::new(stream, LinesCodec::new());
    authenticate(&mut client, &secret).await;
    let reconnected = request(&mut client, "status-2", "runtime.status", json!({})).await;
    assert_eq!(reconnected["result"]["state"], "stopped");

    let restarted = request(&mut client, "start-2", "runtime.start", json!({})).await;
    assert_eq!(restarted["result"]["state"], "running");

    let stopped = request(&mut client, "stop-2", "runtime.stop", json!({})).await;
    assert_eq!(stopped["result"]["state"], "stopped");

    drop(client);
    connector.start_kill().unwrap();
    let status = timeout(Duration::from_secs(2), connector.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(!status.success());
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

    // Disconnect shutdown must resolve a dispatched mutation to unknown before
    // the connection task is dropped. Reconnect reads the persisted fact only;
    // no retry or second send is issued.
    let stream = UnixStream::connect(&endpoint).await.unwrap();
    let mut client = Framed::new(stream, LinesCodec::new());
    authenticate(&mut client, &secret).await;
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

    drop(client);
    connector.start_kill().unwrap();
    let _ = timeout(Duration::from_secs(2), connector.wait()).await;
}

fn spawn_connector(root: &Path, endpoint: &Path, secret_file: &Path) -> Child {
    Command::new(env!("CARGO_BIN_EXE_kt-signal-connector"))
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
        .kill_on_drop(true)
        .spawn()
        .unwrap()
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
