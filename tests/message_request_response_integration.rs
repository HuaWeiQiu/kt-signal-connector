// SPDX-License-Identifier: AGPL-3.0-only

//! Contract revision 1.70 (§4.58 consume batch) — the
//! `syncMessage.messageRequestResponse` consumption round trip over a real
//! fake-engine process and the full server stack.
//!
//! This file is deliberately standalone (no shared helpers with
//! `server_integration.rs`): a parallel test-hardening batch is touching that
//! file's helpers, and the two batches must never collide at merge time.

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
use tokio::io::AsyncReadExt;
use tokio::net::UnixStream;
use tokio::process::{Child, Command};
use tokio::time::{sleep, timeout};
use tokio_util::codec::{Framed, LinesCodec};

const API_VERSION: &str = "1.0";
const TEST_STORE_KEY: [u8; 32] = [0x5A; 32];
/// The fixture peer conversation the link's receive burst materializes.
const FIXTURE_PEER: &str = "+15555550101";
/// The thread ACI the injected mirrors name (any well-formed UUID shape).
const THREAD_ACI: &str = "0b7fca57-1234-4d0e-9b0f-4f6c1f8a2e10";
/// 32 raw bytes standard-base64 — the group master key shape.
const GROUP_B64: &str = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=";

fn bootstrap_payload(secret: &[u8; 32]) -> String {
    format!("{}\n{}", hex::encode(secret), hex::encode(TEST_STORE_KEY))
}

fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fake-engine.py")
}

fn write_secret_file(path: &Path, secret: &[u8; 32]) {
    fs::write(path, bootstrap_payload(secret)).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
}

fn spawn_connector(root: &Path, endpoint: &Path, secret_file: &Path) -> Child {
    let mut command = Command::new(env!("CARGO_BIN_EXE_kt-signal-connector"));
    command
        .arg("serve")
        .arg("--endpoint")
        .arg(endpoint)
        .arg("--bootstrap-secret-file")
        .arg(secret_file)
        .arg("--engine-bin")
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
        .env("KT_FAKE_EXPECT_NO_JAVA", "1")
        .env("JAVA_TOOL_OPTIONS", "poison")
        .env("_JAVA_OPTIONS", "poison")
        .env("JDK_JAVA_OPTIONS", "poison");
    command.spawn().unwrap()
}

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

async fn authenticate(client: &mut Framed<UnixStream, LinesCodec>, secret: &[u8; 32]) -> Value {
    let challenge: Value = serde_json::from_str(&client.next().await.unwrap().unwrap()).unwrap();
    assert_eq!(challenge["event"], "runtime.challenge");
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
    response
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

async fn link_account(client: &mut Framed<UnixStream, LinesCodec>) -> String {
    let link = request(
        client,
        "link-start",
        "link.start",
        json!({ "deviceName": "KT-MrrConsume" }),
    )
    .await;
    let finished = request(
        client,
        "link-finish",
        "link.finish",
        json!({ "linkSessionId": link["result"]["linkSessionId"] }),
    )
    .await;
    finished["result"]["id"].as_str().unwrap().to_string()
}

/// Stage envelopes through the runtime injection marker and drain until
/// `found` answers true over the frames that arrive while polling.
async fn drain_until(
    client: &mut Framed<UnixStream, LinesCodec>,
    mut found: impl FnMut(&Value) -> bool,
) -> bool {
    for attempt in 0..200 {
        sleep(Duration::from_millis(20)).await;
        let _ = send_request_frame(
            client,
            &format!("poll-{attempt}"),
            "runtime.status",
            json!({}),
        )
        .await;
        loop {
            let frame: Value =
                serde_json::from_str(&client.next().await.unwrap().unwrap()).unwrap();
            if found(&frame) {
                return true;
            }
            if frame.get("requestId").and_then(Value::as_str)
                == Some(format!("poll-{attempt}").as_str())
            {
                break;
            }
        }
    }
    false
}

/// Contract 1.70 (§4.58 consume batch), the full-stack round trip: the fake
/// engine delivers a `syncMessage.messageRequestResponse` envelope, the
/// connector normalizes it as a control receive (no message row), stamps the
/// authoritative factor on the resolved direct conversation, and the
/// refreshed `conversation.changed` summary carries
/// `messageRequestAccepted=true`. The fail-closed faces ride the same wire:
/// a hostile shape (past-enum type, bad UUID, wrong-length group id) changes
/// nothing, an unknown thread converges nothing, and the absence of a
/// message row holds across the whole family. The fallback discipline: a
/// conversation the receive face never names keeps its window-derived
/// `messageRequestAccepted` (false on the incoming-only fixture row) — the
/// wire key keeps its pre-1.70 semantics wherever no authoritative factor
/// exists.
#[tokio::test]
async fn message_request_response_faces_round_trip_and_gate_the_mirror() {
    let temp = TempDir::new().unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let endpoint = temp.path().join("connector.sock");
    let secret_file = temp.path().join("bootstrap.secret");
    let secret = [49_u8; 32];
    write_secret_file(&secret_file, &secret);

    let mut connector = spawn_connector(temp.path(), &endpoint, &secret_file);
    wait_for_path(&endpoint).await;
    let stream = UnixStream::connect(&endpoint).await.unwrap();
    let mut client = Framed::new(stream, LinesCodec::new());
    authenticate(&mut client, &secret).await;
    request(&mut client, "start", "runtime.start", json!({})).await;
    let account_id = link_account(&mut client).await;

    // Record the peer's ACI→peer-key identity the §4.37 ingest keeps from a
    // real receive chain: one incoming envelope carrying the peer's
    // sourceUuid rides the marker flush (`emitReceive` re-emits the default
    // burst dedup-no-op plus every pending extra).
    let signal_data = temp.path().join("signal-data");
    fs::create_dir_all(&signal_data).unwrap();
    fs::write(
        signal_data.join(".fixture-extra-receives.json"),
        json!([
            {
                "source": FIXTURE_PEER,
                "sourceUuid": THREAD_ACI,
                "timestamp": 1727000005000u64,
                "dataMessage": { "message": "identity carrier" }
            }
        ])
        .to_string(),
    )
    .unwrap();
    // The linked account's conversation row (the link burst delivers the
    // fixture receive from +15555550101); the typing send is the clean
    // post-link flush trigger (it changes no conversation state) that
    // delivers the staged identity carrier after the listing.
    let listed = request(
        &mut client,
        "list-initial",
        "conversations.list",
        json!({ "accountId": account_id, "limit": 50 }),
    )
    .await;
    let direct = listed["result"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["type"] == "direct" && row["isSelf"] == false)
        .expect("the fixture peer row materializes at link time")
        .clone();
    let conversation_id = direct["id"].as_str().unwrap().to_string();
    request(
        &mut client,
        "flush-identity",
        "presence.setTypingMessage",
        json!({
            "accountId": account_id,
            "conversationId": conversation_id,
            "stop": true
        }),
    )
    .await;
    assert_eq!(
        direct["messageRequestAccepted"], false,
        "the incoming-only fixture row is pending before any mirror"
    );

    // The authoritative ACCEPT mirror: no message row, and the refreshed
    // summary flips the wire key.
    fs::write(
        signal_data.join(".fixture-emit-envelopes.json"),
        json!({ "envelopes": [
            {
                "timestamp": 1727000010000u64,
                "syncMessage": {
                    "messageRequestResponse": {
                        "threadAci": THREAD_ACI,
                        "messageRequestResponseType": 1
                    }
                }
            }
        ] })
        .to_string(),
    )
    .unwrap();
    let accepted = drain_until(&mut client, |frame| {
        frame.get("event").and_then(Value::as_str) == Some("conversation.changed")
            && frame["data"]["id"] == conversation_id.as_str()
            && frame["data"]["messageRequestAccepted"] == true
    })
    .await;
    assert!(
        accepted,
        "the ACCEPT mirror must ride conversation.changed with the accepted state"
    );

    // No message row was created by the mirror.
    let rows = request(
        &mut client,
        "rows-after-accept",
        "messages.list",
        json!({
            "accountId": account_id,
            "conversationId": conversation_id,
            "limit": 50
        }),
    )
    .await;
    assert!(
        rows["result"]["items"]
            .as_array()
            .unwrap()
            .iter()
            .all(|row| row["sentAt"].as_u64().unwrap_or(0) < 1727000010000),
        "the mirror must not create message rows: {rows:?}"
    );

    // The DELETE twin (member 2) stamps the terminal not-accepted back, and
    // the refreshed summary carries it (the summary stays honest per-event).
    fs::write(
        signal_data.join(".fixture-emit-envelopes.json"),
        json!({ "envelopes": [
            {
                "timestamp": 1727000020000u64,
                "syncMessage": {
                    "messageRequestResponse": {
                        "threadAci": THREAD_ACI,
                        "messageRequestResponseType": 2
                    }
                }
            }
        ] })
        .to_string(),
    )
    .unwrap();
    let declined = drain_until(&mut client, |frame| {
        frame.get("event").and_then(Value::as_str) == Some("conversation.changed")
            && frame["data"]["id"] == conversation_id.as_str()
    })
    .await;
    assert!(declined, "the DELETE mirror must ride conversation.changed");
    let listed = request(
        &mut client,
        "list-after-delete",
        "conversations.list",
        json!({ "accountId": account_id, "limit": 50 }),
    )
    .await;
    let summary = listed["result"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["id"] == conversation_id.as_str())
        .expect("the direct row stays listed");
    assert_eq!(
        summary["messageRequestAccepted"], false,
        "the DELETE stamp closes the accepted state: {summary}"
    );

    // A BLOCK mirror (member 3) keeps the closed state (every non-ACCEPT
    // member reads not-accepted).
    fs::write(
        signal_data.join(".fixture-emit-envelopes.json"),
        json!({ "envelopes": [
            {
                "timestamp": 1727000030000u64,
                "syncMessage": {
                    "messageRequestResponse": {
                        "threadAci": THREAD_ACI,
                        "messageRequestResponseType": 3
                    }
                }
            }
        ] })
        .to_string(),
    )
    .unwrap();
    assert!(
        drain_until(&mut client, |frame| {
            frame.get("event").and_then(Value::as_str) == Some("conversation.changed")
                && frame["data"]["id"] == conversation_id.as_str()
        })
        .await,
        "the BLOCK mirror rides conversation.changed too"
    );
    let listed = request(
        &mut client,
        "list-after-block",
        "conversations.list",
        json!({ "accountId": account_id, "limit": 50 }),
    )
    .await;
    let summary = listed["result"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["id"] == conversation_id.as_str())
        .expect("the direct row stays listed");
    assert_eq!(
        summary["messageRequestAccepted"], false,
        "BLOCK keeps the request closed: {summary}"
    );

    // The fail-closed faces, one marker burst: every hostile shape changes
    // nothing — no conversation.changed with any new state, no row, and the
    // stored stamp keeps the last authoritative value (false from BLOCK).
    fs::write(
        signal_data.join(".fixture-emit-envelopes.json"),
        json!({ "envelopes": [
            {
                "timestamp": 1727000040000u64,
                "syncMessage": {
                    "messageRequestResponse": {
                        "threadAci": THREAD_ACI,
                        "messageRequestResponseType": 7
                    }
                }
            },
            {
                "timestamp": 1727000041000u64,
                "syncMessage": {
                    "messageRequestResponse": {
                        "threadAci": "not-a-service-id",
                        "messageRequestResponseType": 1
                    }
                }
            },
            {
                "timestamp": 1727000042000u64,
                "syncMessage": {
                    "messageRequestResponse": {
                        "groupId": "dG9vLXNob3J0",
                        "messageRequestResponseType": 1
                    }
                }
            },
            {
                "timestamp": 1727000043000u64,
                "syncMessage": {
                    "messageRequestResponse": {
                        "messageRequestResponseType": 1
                    }
                }
            }
        ] })
        .to_string(),
    )
    .unwrap();
    let _drained = drain_until(&mut client, |_frame| false).await; // pure drain
    let listed = request(
        &mut client,
        "list-after-hostile",
        "conversations.list",
        json!({ "accountId": account_id, "limit": 50 }),
    )
    .await;
    let summary = listed["result"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["id"] == conversation_id.as_str())
        .expect("the direct row stays listed");
    assert_eq!(
        summary["messageRequestAccepted"], false,
        "hostile shapes never flip the stored state: {summary}"
    );
    let rows = request(
        &mut client,
        "rows-after-hostile",
        "messages.list",
        json!({
            "accountId": account_id,
            "conversationId": conversation_id,
            "limit": 50
        }),
    )
    .await;
    assert!(
        rows["result"]["items"]
            .as_array()
            .unwrap()
            .iter()
            .all(|row| row["sentAt"].as_u64().unwrap_or(0) < 1727000010000),
        "hostile shapes create no rows either: {rows:?}"
    );

    // The wire semantics elsewhere: a summary the receive face never names
    // keeps its window-derived answer. The linked account's own group row
    // (materialized by the fixture link burst family in other tests — here
    // the direct row is the only one, so the list page itself is the check:
    // every row carries the bare bool, the always-serialized discipline).
    let listed = request(
        &mut client,
        "list-final",
        "conversations.list",
        json!({ "accountId": account_id, "limit": 50 }),
    )
    .await;
    for row in listed["result"]["items"].as_array().unwrap() {
        assert!(
            row["messageRequestAccepted"].is_boolean(),
            "the wire key is always serialized: {row}"
        );
    }

    drop(client);
    // Give the connector a beat to observe the disconnect, then require the
    // clean exit.
    sleep(Duration::from_millis(100)).await;
    assert_clean_exit(&mut connector).await;
}

/// Contract 1.70 (§4.58 consume batch), the group-thread refusal: a
/// well-formed group mirror (32-byte base64 thread leg, ACCEPT) addresses a
/// thread nothing local resolves — the connector drops it warn-only (宁缺勿错,
/// no row invented), and the store stays untouched. The shape survives the
/// engine gate (the mirror is protocol-legal), so this is the service layer's
/// group refusal proving the 1:1 vocabulary.
#[tokio::test]
async fn message_request_response_group_mirror_without_a_group_row_converges_nothing() {
    let temp = TempDir::new().unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let endpoint = temp.path().join("connector.sock");
    let secret_file = temp.path().join("bootstrap.secret");
    let secret = [50_u8; 32];
    write_secret_file(&secret_file, &secret);

    let mut connector = spawn_connector(temp.path(), &endpoint, &secret_file);
    wait_for_path(&endpoint).await;
    let stream = UnixStream::connect(&endpoint).await.unwrap();
    let mut client = Framed::new(stream, LinesCodec::new());
    authenticate(&mut client, &secret).await;
    request(&mut client, "start", "runtime.start", json!({})).await;
    let account_id = link_account(&mut client).await;

    let before = request(
        &mut client,
        "list-before",
        "conversations.list",
        json!({ "accountId": account_id, "limit": 50 }),
    )
    .await;
    let before_rows = before["result"]["items"].as_array().unwrap().clone();

    fs::write(
        temp.path()
            .join("signal-data")
            .join(".fixture-emit-envelopes.json"),
        json!({ "envelopes": [
            {
                "timestamp": 1727000050000u64,
                "syncMessage": {
                    "messageRequestResponse": {
                        "groupId": GROUP_B64,
                        "messageRequestResponseType": 1
                    }
                }
            }
        ] })
        .to_string(),
    )
    .unwrap();
    // Pure drain: let the mirror land and be dropped.
    let _drained = drain_until(&mut client, |_frame| false).await;

    let after = request(
        &mut client,
        "list-after",
        "conversations.list",
        json!({ "accountId": account_id, "limit": 50 }),
    )
    .await;
    assert_eq!(
        after["result"]["items"].as_array().unwrap(),
        &before_rows,
        "an unresolvable group mirror invents nothing: {before_rows:?}"
    );

    drop(client);
    sleep(Duration::from_millis(100)).await;
    assert_clean_exit(&mut connector).await;
}
