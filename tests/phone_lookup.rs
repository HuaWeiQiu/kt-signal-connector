// SPDX-License-Identifier: AGPL-3.0-only

#![cfg(unix)]

//! `phone.lookup` (contract revision 1.164, §4.162; the CDSI number-batch
//! relay batch): the wire face dispatches the engine's `phoneLookup` reader
//! (engine d088a59) and projects the answer entry-for-entry — the keys ride
//! the engine's names verbatim, a miss arrives pre-degraded
//! (`{number, isRegistered: false}`), nothing lands in the connector store,
//! and the three structured error faces pass through untouched
//! (PHONE_INVALID deterministic; PHONE_RATE_LIMITED carrying the promised
//! `data.retryAfterSeconds`; PHONE_SERVER_FAILED may-succeed-later — the
//! wait decision is the caller's and nothing retries automatically). The
//! local gates (batch cap 1..20, the E.164 grammar `^\+\d{7,15}$`) mirror
//! the desktop hostAdapter's fast-fail before any engine traffic.

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
    let client_nonce = hex::encode([7_u8; 32]);
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
    loop {
        let response: Value = serde_json::from_str(&client.next().await.unwrap().unwrap()).unwrap();
        if response.get("requestId").and_then(Value::as_str) == Some(request_id) {
            return response;
        }
    }
}

async fn link_account(client: &mut Framed<UnixStream, LinesCodec>) -> String {
    let link = request(
        client,
        "link-start",
        "link.start",
        json!({ "deviceName": "KT-PhoneLookup" }),
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

fn phone_frames(send_log: &str) -> Vec<Value> {
    send_log
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .filter(|entry: &Value| entry.get("numbers").is_some())
        .collect()
}

/// The full relay: the hit and the miss ride the engine projection verbatim
/// (number echo, both service ids on the hit, the pre-degraded miss face
/// with no invented keys), the engine frame carries the resolved signal
/// account under `account` with the numbers untouched, the unknown host
/// account answers ACCOUNT_NOT_FOUND, and zero rows land anywhere — the
/// answer is a memory projection only.
#[tokio::test]
async fn phone_lookup_rides_the_engine_projection_verbatim_hit_and_miss() {
    let temp = TempDir::new().unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let endpoint = temp.path().join("connector.sock");
    let secret_file = temp.path().join("bootstrap.secret");
    let secret = [67_u8; 32];
    write_secret_file(&secret_file, &secret);

    let mut connector = spawn_connector(temp.path(), &endpoint, &secret_file);
    wait_for_path(&endpoint).await;
    let stream = UnixStream::connect(&endpoint).await.unwrap();
    let mut client = Framed::new(stream, LinesCodec::new());
    authenticate(&mut client, &secret).await;
    request(&mut client, "start", "runtime.start", json!({})).await;
    let account_id = link_account(&mut client).await;

    let lookup = request(
        &mut client,
        "phone-lookup",
        "phone.lookup",
        json!({
            "account": account_id,
            "numbers": ["+15551234567", "+15557654321"]
        }),
    )
    .await;
    let entries = lookup["result"]["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 2, "entry-for-entry, none dropped: {lookup}");

    // The hit: the number echoed verbatim, both service ids riding the
    // engine's names, the registered face.
    assert_eq!(entries[0]["number"], "+15551234567");
    assert_eq!(entries[0]["isRegistered"], true);
    assert!(
        entries[0]["aci"].is_string(),
        "the hit carries its ACI: {}",
        entries[0]
    );
    assert!(
        entries[0]["pni"].is_string(),
        "the hit carries its PNI: {}",
        entries[0]
    );

    // The miss: pre-degraded {number, isRegistered: false} — no service id
    // invented, no entry dropped, absent keys absent (never null).
    assert_eq!(entries[1]["number"], "+15557654321");
    assert_eq!(entries[1]["isRegistered"], false);
    assert!(
        entries[1].get("aci").is_none(),
        "a miss invents no service id: {}",
        entries[1]
    );
    assert!(entries[1].get("pni").is_none());

    // The engine frame: the resolved signal account under `account`, the
    // numbers verbatim.
    let send_log = fs::read_to_string(
        temp.path()
            .join("signal-data")
            .join(".fixture-send-log.jsonl"),
    )
    .expect("fixture must log the dispatch");
    let dispatch = &phone_frames(&send_log)[0];
    assert_eq!(dispatch["account"], "+15555550100");
    assert_eq!(
        dispatch["numbers"],
        json!(["+15551234567", "+15557654321"]),
        "the numbers ride the engine frame verbatim"
    );

    // The unknown host account answers ACCOUNT_NOT_FOUND (the ladder's
    // precedence).
    let missing = request(
        &mut client,
        "phone-lookup-missing",
        "phone.lookup",
        json!({
            "account": "acct-absent",
            "numbers": ["+15551234567"]
        }),
    )
    .await;
    assert_eq!(missing["error"]["code"], "ACCOUNT_NOT_FOUND");
    assert_eq!(missing["error"]["retryable"], false);

    drop(client);
    assert_clean_exit(&mut connector).await;
}

/// The local gates refuse before any engine traffic: the batch cap (1..20),
/// the E.164 grammar, and the empty batch answer INVALID_REQUEST
/// retryable=false with zero phoneLookup frames dispatched.
#[tokio::test]
async fn phone_lookup_local_gates_refuse_before_any_engine_traffic() {
    let temp = TempDir::new().unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let endpoint = temp.path().join("connector.sock");
    let secret_file = temp.path().join("bootstrap.secret");
    let secret = [68_u8; 32];
    write_secret_file(&secret_file, &secret);

    let mut connector = spawn_connector(temp.path(), &endpoint, &secret_file);
    wait_for_path(&endpoint).await;
    let stream = UnixStream::connect(&endpoint).await.unwrap();
    let mut client = Framed::new(stream, LinesCodec::new());
    authenticate(&mut client, &secret).await;
    request(&mut client, "start", "runtime.start", json!({})).await;
    let account_id = link_account(&mut client).await;

    // Empty batch.
    let empty = request(
        &mut client,
        "phone-empty",
        "phone.lookup",
        json!({ "account": account_id, "numbers": [] }),
    )
    .await;
    assert_eq!(empty["error"]["code"], "INVALID_REQUEST");
    assert_eq!(empty["error"]["retryable"], false);

    // Over-cap batch: 21 entries.
    let oversized: Vec<String> = std::iter::repeat_n("+15550000001".to_string(), 21).collect();
    let too_many = request(
        &mut client,
        "phone-oversized",
        "phone.lookup",
        json!({ "account": account_id, "numbers": oversized }),
    )
    .await;
    assert_eq!(too_many["error"]["code"], "INVALID_REQUEST");
    assert_eq!(too_many["error"]["retryable"], false);

    // The E.164 grammar, one malformed entry poisoning a well-formed batch.
    for malformed in [
        "15550000001",
        "+1555abc23456",
        "+1555 000 0001",
        "++15550000001",
    ] {
        let bad = request(
            &mut client,
            "phone-malformed",
            "phone.lookup",
            json!({
                "account": account_id,
                "numbers": ["+15551234567", malformed]
            }),
        )
        .await;
        assert_eq!(
            bad["error"]["code"], "INVALID_REQUEST",
            "malformed E.164 must refuse locally: {malformed}"
        );
        assert_eq!(bad["error"]["retryable"], false);
    }

    // The cap boundary: exactly 20 entries pass the gate and reach the
    // engine (the fake answers entry-for-entry).
    let at_cap: Vec<String> = std::iter::repeat_n("+15550000009".to_string(), 20).collect();
    let boundary = request(
        &mut client,
        "phone-at-cap",
        "phone.lookup",
        json!({ "account": account_id, "numbers": at_cap }),
    )
    .await;
    let entries = boundary["result"]["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 20, "20 entries ride: {boundary}");

    // The refusals stayed local: only the at-cap frame ever reached the
    // engine.
    let send_log = fs::read_to_string(
        temp.path()
            .join("signal-data")
            .join(".fixture-send-log.jsonl"),
    )
    .expect("fixture must log the dispatch");
    assert_eq!(
        phone_frames(&send_log).len(),
        1,
        "the local refusals never dial the engine: {send_log}"
    );

    drop(client);
    assert_clean_exit(&mut connector).await;
}

/// The three structured error faces ride the engine's literals untouched:
/// PHONE_RATE_LIMITED carries the promised data.retryAfterSeconds verbatim
/// (retryable=true — a later attempt may succeed; the wait decision is the
/// caller's, nothing retries automatically), PHONE_SERVER_FAILED
/// may-succeed-later, PHONE_INVALID is deterministic.
#[tokio::test]
async fn phone_lookup_error_faces_ride_the_engine_literals_verbatim() {
    let temp = TempDir::new().unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let endpoint = temp.path().join("connector.sock");
    let secret_file = temp.path().join("bootstrap.secret");
    let secret = [69_u8; 32];
    write_secret_file(&secret_file, &secret);

    let mut connector = spawn_connector(temp.path(), &endpoint, &secret_file);
    wait_for_path(&endpoint).await;
    let stream = UnixStream::connect(&endpoint).await.unwrap();
    let mut client = Framed::new(stream, LinesCodec::new());
    authenticate(&mut client, &secret).await;
    request(&mut client, "start", "runtime.start", json!({})).await;
    let account_id = link_account(&mut client).await;

    // The rate-limit sentinel: the code plus the engine's backoff fact,
    // verbatim and un-nested.
    let rate_limited = request(
        &mut client,
        "phone-rate-limited",
        "phone.lookup",
        json!({
            "account": account_id,
            "numbers": ["+15550000001"]
        }),
    )
    .await;
    assert_eq!(rate_limited["error"]["code"], "PHONE_RATE_LIMITED");
    assert_eq!(rate_limited["error"]["retryable"], true);
    assert_eq!(
        rate_limited["error"]["data"]["retryAfterSeconds"], 42,
        "the engine's retry-after seconds ride verbatim: {rate_limited}"
    );

    // The server-failure family: may succeed on a later attempt.
    let server_failed = request(
        &mut client,
        "phone-server-failed",
        "phone.lookup",
        json!({
            "account": account_id,
            "numbers": ["+15550000002"]
        }),
    )
    .await;
    assert_eq!(server_failed["error"]["code"], "PHONE_SERVER_FAILED");
    assert_eq!(server_failed["error"]["retryable"], true);
    assert!(
        server_failed["error"].get("data").is_none(),
        "no data object is invented: {server_failed}"
    );

    // The deterministic grammar face: the same shape never resolves.
    let invalid = request(
        &mut client,
        "phone-invalid",
        "phone.lookup",
        json!({
            "account": account_id,
            "numbers": ["+15550000003"]
        }),
    )
    .await;
    assert_eq!(invalid["error"]["code"], "PHONE_INVALID");
    assert_eq!(invalid["error"]["retryable"], false);

    // Each error is terminal per call: the three sentinels dispatched
    // exactly once each — nothing retried anything automatically.
    let send_log = fs::read_to_string(
        temp.path()
            .join("signal-data")
            .join(".fixture-send-log.jsonl"),
    )
    .expect("fixture must log the dispatch");
    let frames = phone_frames(&send_log);
    assert_eq!(frames.len(), 3, "one frame per call, zero auto-retries");

    drop(client);
    assert_clean_exit(&mut connector).await;
}
