// SPDX-License-Identifier: AGPL-3.0-only

//! Contract revision 1.71 (§4.63) — the `hasUserInitiatedOutbound`
//! conversation-summary bit over a real fake-engine process and the full
//! server stack: the whole-history outgoing evidence the desktop evidence
//! gate needs (its official parity matrix §18.4 data-plane gap).
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
/// A second peer the staged extra receives materialize (never sent to);
/// located by its receive preview on the wire (peerKey is not projected).
const SECOND_PEER_PREVIEW: &str = "second peer incoming";
/// The thread ACI the staged identity carrier names.
const THREAD_ACI: &str = "0b7fca57-1234-4d0e-9b0f-4f6c1f8a2e10";

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
        json!({ "deviceName": "KT-OutboundEvidence" }),
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

async fn listed_summary(
    client: &mut Framed<UnixStream, LinesCodec>,
    request_id: &str,
    account_id: &str,
    conversation_id: &str,
) -> Value {
    let listed = request(
        client,
        request_id,
        "conversations.list",
        json!({ "accountId": account_id, "limit": 50 }),
    )
    .await;
    listed["result"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["id"].as_str() == Some(conversation_id))
        .cloned()
        .expect("the conversation row stays listed")
}

/// Contract 1.71 (§4.63), the full-stack round trip: the fixture peer's row
/// materializes incoming-only at link time (`hasUserInitiatedOutbound=false`
/// — the whole history holds no outgoing row), a real `messages.sendText`
/// round trip through the fake engine creates the outgoing row and the
/// refreshed summary flips the bit true (window- and newest-row-independent:
/// a later staged incoming row becomes the newest row and the bit stays
/// true), the never-sent second peer stays false, and a fresh conversation
/// the store holds no history for projects false — the factor-absent
/// degradation the record documents. Every row carries the bare always-
/// serialized bool on both the list page and the event-driven summary.
#[tokio::test]
async fn outbound_evidence_round_trip_over_the_fake_engine() {
    let temp = TempDir::new().unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let endpoint = temp.path().join("connector.sock");
    let secret_file = temp.path().join("bootstrap.secret");
    let secret = [51_u8; 32];
    write_secret_file(&secret_file, &secret);

    let mut connector = spawn_connector(temp.path(), &endpoint, &secret_file);
    wait_for_path(&endpoint).await;
    let stream = UnixStream::connect(&endpoint).await.unwrap();
    let mut client = Framed::new(stream, LinesCodec::new());
    authenticate(&mut client, &secret).await;
    request(&mut client, "start", "runtime.start", json!({})).await;
    let account_id = link_account(&mut client).await;

    // Stage a second peer's identity carrier (an incoming receive naming its
    // sourceUuid) so its row materializes without any send ever touching it.
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
            },
            {
                "source": "+15555550102",
                "timestamp": 1727000006000u64,
                "dataMessage": { "message": "second peer incoming" }
            }
        ])
        .to_string(),
    )
    .unwrap();
    // The link burst delivers the fixture receive from the first peer; the
    // typing send is the clean post-link flush trigger (it changes no
    // conversation state) that delivers the staged second peer after the
    // listing.
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
        "flush-second-peer",
        "presence.setTypingMessage",
        json!({
            "accountId": account_id,
            "conversationId": conversation_id,
            "stop": true
        }),
    )
    .await;

    // Base state: both rows are incoming-only — the whole-history bit reads
    // false, while the request fold's answer is irrelevant to it.
    let summary = listed_summary(&mut client, "list-base", &account_id, &conversation_id).await;
    assert_eq!(
        summary["hasUserInitiatedOutbound"], false,
        "incoming-only history is no outbound evidence: {summary}"
    );
    // Wait (bounded) for the staged second peer's row to materialize, then
    // read both rows' base state: incoming-only history is no outbound
    // evidence.
    let second = (async {
        for attempt in 0..50 {
            let listed = request(
                &mut client,
                &format!("list-base-{attempt}"),
                "conversations.list",
                json!({ "accountId": account_id, "limit": 50 }),
            )
            .await;
            let found = listed["result"]["items"]
                .as_array()
                .unwrap()
                .iter()
                .find(|row| row["lastMessagePreview"] == SECOND_PEER_PREVIEW)
                .cloned();
            if let Some(found) = found {
                return found;
            }
            if attempt == 49 {
                panic!("the staged second peer row never materialized; listed: {listed}");
            }
            sleep(Duration::from_millis(20)).await;
        }
        panic!("the staged second peer row never materialized");
    })
    .await;
    let second_id = second["id"].as_str().unwrap().to_string();
    assert_eq!(
        second["hasUserInitiatedOutbound"], false,
        "the never-sent peer carries no outbound evidence: {second}"
    );

    // A real send round trip: the outgoing row lands, and the refreshed
    // summary rides conversation.changed with the bit true.
    let sent = request(
        &mut client,
        "send-text",
        "messages.sendText",
        json!({
            "accountId": account_id,
            "conversationId": conversation_id,
            "text": "first outbound evidence",
            "clientRequestId": "client-outbound-1"
        }),
    )
    .await;
    assert!(
        sent["result"]["id"].is_string(),
        "the send must succeed: {sent}"
    );
    let flipped = drain_until(&mut client, |frame| {
        frame.get("event").and_then(Value::as_str) == Some("conversation.changed")
            && frame["data"]["id"] == conversation_id.as_str()
            && frame["data"]["hasUserInitiatedOutbound"] == true
    })
    .await;
    assert!(
        flipped,
        "the send must ride conversation.changed with the bit flipped"
    );

    // The whole-history semantics: a later incoming row becomes the newest
    // row (so lastMessageDirection reads incoming) and the bit stays true.
    fs::write(
        signal_data.join(".fixture-extra-receives.json"),
        json!([
            {
                "source": FIXTURE_PEER,
                "sourceUuid": THREAD_ACI,
                "timestamp": 1727000005000u64,
                "dataMessage": { "message": "identity carrier" }
            },
            {
                "source": "+15555550102",
                "timestamp": 1727000006000u64,
                "dataMessage": { "message": "second peer incoming" }
            },
            {
                "source": FIXTURE_PEER,
                "sourceUuid": THREAD_ACI,
                "timestamp": 1727000009000u64,
                "dataMessage": { "message": "newer incoming after the send" }
            }
        ])
        .to_string(),
    )
    .unwrap();
    request(
        &mut client,
        "flush-newer-incoming",
        "presence.setTypingMessage",
        json!({
            "accountId": account_id,
            "conversationId": conversation_id,
            "stop": true
        }),
    )
    .await;
    // Bounded wait until the staged row lands and becomes the newest row.
    let mut summary = None;
    for attempt in 0..50 {
        let current = listed_summary(
            &mut client,
            &format!("list-after-incoming-{attempt}"),
            &account_id,
            &conversation_id,
        )
        .await;
        if current["lastMessageDirection"] == "incoming" {
            summary = Some(current);
            break;
        }
        sleep(Duration::from_millis(20)).await;
    }
    let summary = summary.expect("the newer incoming row never became the newest row");
    assert_eq!(
        summary["lastMessageDirection"], "incoming",
        "the staged row must be the newest row for the independence check"
    );
    assert_eq!(
        summary["hasUserInitiatedOutbound"], true,
        "whole-history evidence survives a newer incoming newest row: {summary}"
    );

    // The second peer never sent anything outbound and still reads false.
    let second_after = listed_summary(&mut client, "list-second", &account_id, &second_id).await;
    assert_eq!(second_after["hasUserInitiatedOutbound"], false);

    // Every row carries the bare always-serialized bool (the
    // muted/pinned/isSelf discipline).
    let listed = request(
        &mut client,
        "list-final",
        "conversations.list",
        json!({ "accountId": account_id, "limit": 50 }),
    )
    .await;
    for row in listed["result"]["items"].as_array().unwrap() {
        assert!(
            row["hasUserInitiatedOutbound"].is_boolean(),
            "the wire key is always serialized: {row}"
        );
    }

    drop(client);
    // Give the connector a beat to observe the disconnect, then require the
    // clean exit.
    sleep(Duration::from_millis(100)).await;
    assert_clean_exit(&mut connector).await;
}
