// SPDX-License-Identifier: AGPL-3.0-only

//! End-to-end coverage for the read-only `runtime.doctor` face (contract
//! revision 1.96, §4.79): the whole payload over a real authenticated socket
//! against the fake engine — the version chain (both engine faces ride the
//! fake's own answers through verbatim; the fake echoes any method it does
//! not know, so both arrive `present`, and the unit suite pins the honest
//! `absent`-with-reason side), the data-dir and lock faces (the connector
//! holds its own occupancy locks, so `held` with its own pid), and the
//! recent-error ring fed by real failed dispatches. The unit suite in
//! `src/doctor.rs` pins the pure halves — structure, unreachable
//! degradation, the three lock forms, and the read-only discipline.

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
use tokio::process::Child;
use tokio::process::Command;
use tokio::time::{sleep, timeout};
use tokio_util::codec::{Framed, LinesCodec};

const API_VERSION: &str = "1.0";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
const AUTH_TIMEOUT: Duration = Duration::from_secs(30);
const TEST_STORE_KEY: [u8; 32] = [0x5A; 32];

fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fake-signal-cli.py")
}

fn spawn_connector(root: &Path, endpoint: &Path) -> Child {
    let secret_file = root.join("bootstrap.secret");
    let secret = [7_u8; 32];
    fs::write(
        &secret_file,
        format!("{}\n{}", hex::encode(secret), hex::encode(TEST_STORE_KEY)),
    )
    .unwrap();
    fs::set_permissions(&secret_file, fs::Permissions::from_mode(0o600)).unwrap();
    Command::new(env!("CARGO_BIN_EXE_kt-signal-connector"))
        .arg("serve")
        .arg("--endpoint")
        .arg(endpoint)
        .arg("--bootstrap-secret-file")
        .arg(&secret_file)
        .arg("--signal-cli")
        .arg(fixture())
        .arg("--signal-data-dir")
        .arg(root.join("signal-data"))
        .arg("--state-dir")
        .arg(root.join("state"))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap()
}

async fn wait_for_path(path: &Path) {
    // Cold starts on a loaded machine can exceed a couple of seconds; the
    // handshake timeout below is already 30s, so give the bind the same
    // order of headroom instead of flaking on the first poll window.
    timeout(Duration::from_secs(10), async {
        while !path.exists() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("endpoint path must appear");
}

async fn authenticate(client: &mut Framed<UnixStream, LinesCodec>, secret: &[u8; 32]) {
    let challenge: Value = serde_json::from_str(
        &timeout(AUTH_TIMEOUT, client.next())
            .await
            .expect("challenge")
            .expect("challenge frame")
            .unwrap(),
    )
    .unwrap();
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
    let response: Value = serde_json::from_str(
        &timeout(AUTH_TIMEOUT, client.next())
            .await
            .expect("handshake response")
            .expect("handshake frame")
            .unwrap(),
    )
    .unwrap();
    assert_eq!(response["result"]["apiVersion"], API_VERSION);
}

async fn request(
    client: &mut Framed<UnixStream, LinesCodec>,
    request_id: &str,
    method: &str,
    params: Value,
) -> Value {
    timeout(REQUEST_TIMEOUT, async {
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
            let response: Value =
                serde_json::from_str(&client.next().await.unwrap().unwrap()).unwrap();
            if response.get("requestId").and_then(Value::as_str) == Some(request_id) {
                return response;
            }
        }
    })
    .await
    .expect("request round trip")
}

/// The whole doctor payload over the wire: the version chain with the fake
/// engine live (the fake echoes any method it does not know, so both engine
/// faces arrive `present` with their verbatim answers — the `absent` with
/// reason half is pinned by the unit suite), the connector's own lock
/// reported held with its own pid, and real failed dispatches feeding the
/// ring.
#[tokio::test]
async fn doctor_reports_versions_data_dir_lock_errors_and_reachability() {
    let temp = TempDir::new().unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let endpoint = temp.path().join("connector.sock");
    let secret = [7_u8; 32];
    let mut connector = spawn_connector(temp.path(), &endpoint);
    wait_for_path(&endpoint).await;

    let stream = UnixStream::connect(&endpoint).await.unwrap();
    let mut client = Framed::new(stream, LinesCodec::new());
    authenticate(&mut client, &secret).await;

    let started = request(&mut client, "start-1", "runtime.start", json!({})).await;
    assert_eq!(started["result"]["state"], "running");

    let empty = request(
        &mut client,
        "doctor-params",
        "runtime.doctor",
        json!({"extra": 1}),
    )
    .await;
    assert_eq!(empty["error"]["code"], "INVALID_REQUEST");

    // One real failed dispatch to feed the recent-error ring.
    let seeded = request(
        &mut client,
        "seed-1",
        "accounts.list",
        json!({"bogus": true}),
    )
    .await;
    assert_eq!(seeded["error"]["code"], "INVALID_REQUEST");

    let report = request(&mut client, "doctor-1", "runtime.doctor", json!({})).await;
    let result = &report["result"];

    let connector_face = &result["connector"];
    assert!(
        connector_face["version"]
            .as_str()
            .is_some_and(|version| !version.is_empty()),
        "connector version present"
    );
    assert_eq!(connector_face["apiVersion"], json!(API_VERSION));
    assert!(connector_face["pid"].is_u64());

    let group = &result["groups"][0];
    assert_eq!(group["groupId"], json!("default"));

    let engine = &group["engine"];
    assert_eq!(engine["state"], json!("running"));
    assert_eq!(engine["reachable"], json!(true));
    assert!(engine["pid"].is_u64());
    assert!(engine.get("unreachableReason").is_none());
    // The fake engine carries `version` — the connector passes the answer
    // through verbatim instead of re-deriving it.
    let version = &engine["version"];
    assert_eq!(version["present"], json!(true));
    assert!(version["face"].is_object(), "engine answer rides verbatim");
    // The fake is signal-cli-shaped: its fallback answers any method it does
    // not implement by echoing `{method}`, so the doctor sees a carried
    // self-certification face and rides the answer through verbatim. The two
    // forms are mutually exclusive by contract: either the engine answered
    // (present, the answer verbatim) or it did not (absent, with a reason).
    let capabilities = &engine["protocolCapabilities"];
    if capabilities["present"] == json!(true) {
        assert!(
            capabilities["face"].is_object(),
            "a carried face answers verbatim"
        );
    } else {
        assert_eq!(capabilities["present"], json!(false));
        assert!(
            capabilities["reason"]
                .as_str()
                .is_some_and(|reason| !reason.is_empty()),
            "the unavailable self-certification face carries its reason"
        );
    }

    let data_dir = &group["dataDir"];
    assert_eq!(data_dir["exists"], json!(true));
    assert_eq!(data_dir["writable"], json!(true));
    assert!(
        data_dir["freeBytes"]
            .as_u64()
            .is_some_and(|bytes| bytes > 0)
    );
    assert!(
        data_dir["totalBytes"]
            .as_u64()
            .is_some_and(|bytes| bytes > 0)
    );
    assert!(
        data_dir["path"]
            .as_str()
            .is_some_and(|path| !path.is_empty())
    );

    // The connector holds its own plan locks for process lifetime: held,
    // with the pid it wrote at acquire time — its own.
    let lock = &group["lock"];
    assert_eq!(lock["present"], json!(true));
    assert_eq!(lock["held"], json!(true));
    assert_eq!(lock["classification"], json!("held"));
    assert_eq!(lock["holderAlive"], json!(true));
    assert_eq!(lock["pid"], connector_face["pid"]);

    let recent = &result["recentErrors"];
    assert_eq!(recent["window"], json!(64));
    assert!(
        recent["recordedTotal"]
            .as_u64()
            .is_some_and(|total| total >= 2)
    );
    let invalid = recent["families"]
        .as_array()
        .unwrap()
        .iter()
        .find(|family| family["family"] == "INVALID_REQUEST")
        .expect("the seeded dispatch failure feeds the ring");
    assert_eq!(invalid["source"], json!("host"));
    assert_eq!(invalid["retryable"], json!(false));
    assert!(invalid["count"].as_u64().is_some_and(|count| count >= 2));

    let network = &result["network"];
    assert!(network["watchdogRestartsTotal"].is_u64());
    assert!(network["receiveDroppedTotal"].is_u64());

    drop(client);
    let _ = connector.kill().await;
}
