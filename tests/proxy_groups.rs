// SPDX-License-Identifier: AGPL-3.0-only

#![cfg(unix)]

//! Phase 4 end-to-end contract (ADR 0001, implementation-plan §4.4): two
//! extra launcher-defined proxy groups plus the implicit default run as three
//! supervised engines sharing one store, with per-group link lanes, per-group
//! state events, and routing strictly by stored bindings.

use std::collections::HashMap;
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
/// Same fixed key discipline as server_integration.rs.
const TEST_STORE_KEY: [u8; 32] = [0x5A; 32];

fn bootstrap_payload(secret: &[u8; 32]) -> String {
    format!("{}\n{}", hex::encode(secret), hex::encode(TEST_STORE_KEY))
}

fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fake-signal-cli.py")
}

fn write_secret_file(path: &Path, secret: &[u8; 32]) {
    fs::write(path, bootstrap_payload(secret)).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
}

fn spawn_two_group_connector(root: &Path, endpoint: &Path, secret_file: &Path) -> Child {
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
        .arg("--proxy-group")
        .arg("team-a=127.0.0.1:9050")
        .arg("--proxy-group")
        .arg("team-b=127.0.0.1:9051")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .env("KT_FAKE_EXPECT_JAVA_OPTS", "-Xms16m -Xmx384m")
        .env("JAVA_TOOL_OPTIONS", "poison")
        .env("_JAVA_OPTIONS", "poison")
        .env("JDK_JAVA_OPTIONS", "poison")
        .spawn()
        .unwrap()
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

/// Send a request and return its response, skipping interleaved event frames.
async fn request(
    client: &mut Framed<UnixStream, LinesCodec>,
    request_id: &str,
    method: &str,
    params: Value,
) -> Value {
    send_request_frame(client, request_id, method, params).await;
    loop {
        let frame: Value = serde_json::from_str(&client.next().await.unwrap().unwrap()).unwrap();
        if frame.get("requestId").and_then(Value::as_str) == Some(request_id) {
            return frame;
        }
    }
}

/// Read frames until an event matching `predicate` arrives; unrelated frames
/// (including other events and late responses) are skipped.
async fn wait_for_event(
    client: &mut Framed<UnixStream, LinesCodec>,
    event_name: &str,
    mut predicate: impl FnMut(&Value) -> bool,
) -> Value {
    loop {
        let frame: Value = serde_json::from_str(&client.next().await.unwrap().unwrap()).unwrap();
        if frame.get("event").and_then(Value::as_str) == Some(event_name)
            && predicate(&frame["data"])
        {
            return frame;
        }
    }
}

fn group_pid(groups: &[Value], group_id: &str) -> Option<u32> {
    groups
        .iter()
        .find(|group| group["groupId"] == group_id)
        .and_then(|group| group["pid"].as_u64())
        .map(|pid| pid as u32)
}

#[tokio::test]
async fn proxy_groups_launch_route_and_fail_closed_end_to_end() {
    let temp = TempDir::new().unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let endpoint = temp.path().join("connector.sock");
    let secret_file = temp.path().join("bootstrap.secret");
    let secret = [7_u8; 32];
    write_secret_file(&secret_file, &secret);

    let mut connector = spawn_two_group_connector(temp.path(), &endpoint, &secret_file);
    wait_for_path(&endpoint).await;

    let stream = UnixStream::connect(&endpoint).await.unwrap();
    let mut client = Framed::new(stream, LinesCodec::new());
    authenticate(&mut client, &secret).await;

    // ---- stopped status: launcher order, additive array, no top-level pid.
    let status = request(&mut client, "status-1", "runtime.status", json!({})).await;
    assert_eq!(status["result"]["state"], "stopped");
    assert!(status["result"].get("pid").is_none());
    let groups = status["result"]["proxyGroups"].as_array().unwrap();
    assert_eq!(groups.len(), 3);
    let ids: Vec<&str> = groups
        .iter()
        .map(|group| group["groupId"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["default", "team-a", "team-b"]);
    for group in groups {
        assert!(group.get("pid").is_none());
        assert_eq!(group["accountCount"], 0);
    }

    // ---- start: one engine per group, three distinct pids.
    let started = request(&mut client, "start-1", "runtime.start", json!({})).await;
    assert_eq!(started["result"]["state"], "running");
    assert!(started["result"].get("pid").is_none());
    let groups = started["result"]["proxyGroups"].as_array().unwrap();
    let pids: Vec<Option<u32>> = groups
        .iter()
        .map(|group| group["pid"].as_u64().map(|pid| pid as u32))
        .collect();
    let [Some(pid_default), Some(pid_a), Some(pid_b)] = pids[..] else {
        panic!("every group engine must report a pid while running");
    };
    assert_ne!(pid_default, pid_a);
    assert_ne!(pid_default, pid_b);
    assert_ne!(pid_a, pid_b);

    // ---- an unconfigured group fails closed without retry advice (R9).
    let unknown = request(
        &mut client,
        "link-unknown",
        "link.start",
        json!({ "deviceName": "KT-Ghost", "proxyGroup": "ghost" }),
    )
    .await;
    assert_eq!(unknown["error"]["code"], "PROXY_GROUP_NOT_FOUND");
    assert_eq!(unknown["error"]["retryable"], false);
    assert!(
        !unknown["error"].to_string().contains("127.0.0.1"),
        "diagnostics never echo proxy endpoints (R10)"
    );

    // ---- parallel link sessions into different groups never exclude each
    // other (R3): both start requests are answered successfully.
    send_request_frame(
        &mut client,
        "link-a",
        "link.start",
        json!({ "deviceName": "KT-A", "proxyGroup": "team-a" }),
    )
    .await;
    send_request_frame(
        &mut client,
        "link-b",
        "link.start",
        json!({ "deviceName": "KT-B", "proxyGroup": "team-b" }),
    )
    .await;
    let mut responses = HashMap::new();
    while responses.len() < 2 {
        let frame: Value = serde_json::from_str(&client.next().await.unwrap().unwrap()).unwrap();
        if let Some(id) = frame.get("requestId").and_then(Value::as_str) {
            if id == "link-a" || id == "link-b" {
                responses.insert(id.to_string(), frame.clone());
            }
        }
    }
    let start_a = &responses["link-a"];
    let start_b = &responses["link-b"];
    assert!(
        start_a["result"]["linkSessionId"].is_string(),
        "team-a link.start failed: {start_a}"
    );
    assert!(
        start_b["result"]["linkSessionId"].is_string(),
        "team-b link.start failed: {start_b}"
    );
    let session_a = start_a["result"]["linkSessionId"].as_str().unwrap();
    let session_b = start_b["result"]["linkSessionId"].as_str().unwrap();
    assert_ne!(session_a, session_b);

    // ---- sequential finishes land in their own groups and carry the group
    // attribution on the result.
    let finish_a = request(
        &mut client,
        "finish-a",
        "link.finish",
        json!({ "linkSessionId": session_a }),
    )
    .await;
    assert_eq!(finish_a["result"]["state"], "ready");
    assert_eq!(finish_a["result"]["proxyGroup"], "team-a");
    let finish_b = request(
        &mut client,
        "finish-b",
        "link.finish",
        json!({ "linkSessionId": session_b }),
    )
    .await;
    assert_eq!(finish_b["result"]["state"], "ready");
    assert_eq!(finish_b["result"]["proxyGroup"], "team-b");

    // ---- accounts.list reports the immutable binding on every item.
    let accounts = request(&mut client, "accounts-1", "accounts.list", json!({})).await;
    let items = accounts["result"].as_array().unwrap();
    assert_eq!(items.len(), 2);
    let mut by_group: HashMap<String, String> = HashMap::new();
    for item in items {
        let group = item["proxyGroup"].as_str().unwrap().to_string();
        let account = item["id"].as_str().unwrap().to_string();
        by_group.insert(group, account);
    }
    assert!(by_group.contains_key("team-a"));
    assert!(by_group.contains_key("team-b"));
    let account_b = by_group["team-b"].clone();

    // ---- killing one group's engine surfaces as that group's
    // proxyGroup.stateChanged while another group keeps serving sends.
    let status = request(&mut client, "status-2", "runtime.status", json!({})).await;
    let groups = status["result"]["proxyGroups"].as_array().unwrap();
    let pid_a = group_pid(groups, "team-a").expect("team-a engine must be running");
    let killed = std::process::Command::new("/bin/kill")
        .arg("-9")
        .arg(pid_a.to_string())
        .status()
        .unwrap();
    assert!(killed.success());

    let event = timeout(
        Duration::from_secs(10),
        wait_for_event(&mut client, "proxyGroup.stateChanged", |data| {
            data["groupId"] == "team-a" && (data["state"] == "exited" || data["state"] == "faulted")
        }),
    )
    .await
    .expect("the killed group must surface a terminal proxyGroup.stateChanged");
    assert_eq!(event["data"]["groupId"], "team-a");

    // The surviving group routes a send through its own engine.
    let sent = request(
        &mut client,
        "send-team-b",
        "messages.sendText",
        json!({
            "accountId": account_b,
            "kind": "contact",
            "peerKey": "+15555550103",
            "text": "other group still serves",
            "clientRequestId": "pg-send-1"
        }),
    )
    .await;
    assert_eq!(sent["result"]["status"], "sent");

    // ---- stop reaches every group.
    let stopped = request(&mut client, "stop-1", "runtime.stop", json!({})).await;
    assert_eq!(stopped["result"]["state"], "stopped");
    for group in stopped["result"]["proxyGroups"].as_array().unwrap() {
        assert_eq!(group["state"], "stopped");
    }

    drop(client);
    let conn_status = timeout(Duration::from_secs(2), connector.wait())
        .await
        .expect("connector should exit after the host disconnects")
        .unwrap();
    assert!(
        conn_status.success(),
        "connector must exit cleanly after full teardown"
    );
}
