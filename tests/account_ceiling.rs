// SPDX-License-Identifier: AGPL-3.0-only

#![cfg(unix)]

//! Per-engine account ceiling over the real binary (optimization-plan §6.4
//! M3.3, decision D3): a single-group connector with the fake signal-cli in
//! multi-account mode links eight accounts, then `link.start` refuses the
//! ninth with ACCOUNT_LIMIT_REACHED naming the group. The store-level guard
//! (re-link exception included) is covered by the service unit tests; this
//! file pins the wire-visible behavior at both link entries.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use serde_json::{Value, json};
use sha2::Sha256;
use tempfile::TempDir;
use tokio::net::UnixStream;
use tokio::process::Command;
use tokio::time::{sleep, timeout};
use tokio_util::codec::{Framed, LinesCodec};

const API_VERSION: &str = "1.0";
/// Same fixed key discipline as server_integration.rs.
const TEST_STORE_KEY: [u8; 32] = [0x5A; 32];
const CEILING: usize = 8;

fn bootstrap_payload(secret: &[u8; 32]) -> String {
    format!("{}\n{}", hex::encode(secret), hex::encode(TEST_STORE_KEY))
}

fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fake-signal-cli.py")
}

/// Link `index` accounts through the wire protocol, then assert the ceiling
/// refusal at `link.start` (before any QR is minted) and the final account
/// count.
#[tokio::test]
async fn ninth_link_start_is_refused_with_the_group_named() {
    let temp = TempDir::new().unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let endpoint = temp.path().join("connector.sock");
    let secret = [7_u8; 32];
    let secret_file = temp.path().join("bootstrap.secret");
    fs::write(&secret_file, bootstrap_payload(&secret)).unwrap();
    fs::set_permissions(&secret_file, fs::Permissions::from_mode(0o600)).unwrap();

    let mut connector = Command::new(env!("CARGO_BIN_EXE_kt-signal-connector"))
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
        .env("KT_FAKE_MULTI_ACCOUNT", "1")
        .spawn()
        .unwrap();

    let appeared = timeout(Duration::from_secs(3), async {
        while !endpoint.exists() {
            assert!(
                connector
                    .try_wait()
                    .expect("connector process state")
                    .is_none(),
                "connector exited before opening the endpoint"
            );
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(appeared.is_ok(), "connector endpoint should appear");

    let stream = UnixStream::connect(&endpoint).await.unwrap();
    let mut client = Framed::new(stream, LinesCodec::new());

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

    let started = request(&mut client, "start-1", "runtime.start", json!({})).await;
    assert_eq!(started["result"]["state"], "running");

    // Eight links must succeed, each binding a distinct number to default.
    for index in 1..=CEILING {
        let start = request(
            &mut client,
            &format!("link-start-{index}"),
            "link.start",
            json!({"deviceName": "soak"}),
        )
        .await;
        let session = start["result"]["linkSessionId"]
            .as_str()
            .unwrap_or_else(|| panic!("link {index} failed: {start}"))
            .to_string();
        let finish = request(
            &mut client,
            &format!("link-finish-{index}"),
            "link.finish",
            json!({"linkSessionId": session}),
        )
        .await;
        assert_eq!(
            finish["result"]["proxyGroup"], "default",
            "link {index} finish failed: {finish}"
        );
    }
    let accounts = request(&mut client, "list-1", "accounts.list", json!({})).await;
    assert_eq!(accounts["result"].as_array().map(Vec::len), Some(CEILING));

    // The ninth start is refused before the engine mints a QR.
    let refused = request(
        &mut client,
        "link-start-9",
        "link.start",
        json!({"deviceName": "soak"}),
    )
    .await;
    let error = refused["error"]
        .as_object()
        .unwrap_or_else(|| panic!("ninth link.start should fail: {refused}"));
    assert_eq!(error["code"], "ACCOUNT_LIMIT_REACHED");
    assert_eq!(error["retryable"], false);
    assert!(
        error["message"]
            .as_str()
            .is_some_and(|message| message.contains("default")),
        "refusal must name the group: {error:?}"
    );

    // The refusal is a pure admission decision: the running engine and the
    // stored eight accounts are untouched.
    let status = request(&mut client, "status-1", "runtime.status", json!({})).await;
    assert_eq!(status["result"]["state"], "running");
    assert_eq!(
        status["result"]["proxyGroups"][0]["accountCount"],
        json!(CEILING)
    );

    drop(client);
    let _ = timeout(Duration::from_secs(5), connector.wait()).await;
    let _ = connector.kill().await;
}

async fn request(
    client: &mut Framed<UnixStream, LinesCodec>,
    request_id: &str,
    method: &str,
    params: Value,
) -> Value {
    client
        .send(
            json!({
                "apiVersion": API_VERSION,
                "requestId": request_id,
                "method": method,
                "params": params,
            })
            .to_string(),
        )
        .await
        .unwrap();
    loop {
        let line = client.next().await.unwrap().unwrap();
        let value: Value = serde_json::from_str(&line).unwrap();
        if value.get("requestId").and_then(Value::as_str) == Some(request_id) {
            return value;
        }
    }
}
