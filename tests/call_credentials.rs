// SPDX-License-Identifier: AGPL-3.0-only

#![cfg(unix)]

//! Call-link credential material passthrough end to end (contract revision
//! 1.88, §4.73; engine a3ae39b): the five `calls.*` faces gate their shapes
//! and resolve the account, forward `{account, ...material}` to the engine
//! verbatim, and pass the engine's result bytes through untouched — the
//! connector parses nothing, logs nothing, caches nothing. The fixture plays
//! the engine (the engine fake discipline: stub bytes are NOT valid zk
//! material, they pin the wire shape), the structured two-code ladder steers
//! from markers. These
//! tests pin the connector's routing, gates, and error fidelity, never real
//! engine credential behavior (the real-device zkgroup chain is the engine
//! batch's own acceptance).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use kt_signal_connector::engine::EngineConfig;
use kt_signal_connector::store::{Store, StoreKey};
use kt_signal_connector::supervisor::RuntimeSupervisor;
use serde_json::json;
use tempfile::TempDir;

fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fake-engine.py")
}

/// Standard padded base64 — the canonical form every material field carries
/// (the connector's own `base64_encode` alphabet, mirrored here so the
/// driver speaks the same wire encoding without a host-surface dependency).
fn b64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut encoded = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let byte1 = chunk.first().copied().unwrap_or_default() as u32;
        let byte2 = chunk.get(1).copied().unwrap_or_default() as u32;
        let byte3 = chunk.get(2).copied().unwrap_or_default() as u32;
        let triple = (byte1 << 16) | (byte2 << 8) | byte3;
        encoded.push(ALPHABET[(triple >> 18) as usize & 0x3F] as char);
        encoded.push(ALPHABET[(triple >> 12) as usize & 0x3F] as char);
        if chunk.len() > 1 {
            encoded.push(ALPHABET[(triple >> 6) as usize & 0x3F] as char);
        } else {
            encoded.push('=');
        }
        if chunk.len() > 2 {
            encoded.push(ALPHABET[triple as usize & 0x3F] as char);
        } else {
            encoded.push('=');
        }
    }
    encoded
}

fn test_store(dir: &Path) -> Arc<Store> {
    Arc::new(Store::open(dir, Some(StoreKey::from_bytes([0x5A; 32]))).unwrap())
}

const SIGNAL_ACCOUNT: &str = "+15555550100";

fn seed_engine_registry(data_dir: &Path) {
    let account_dir = data_dir.join("accounts").join("lk-abc");
    std::fs::create_dir_all(&account_dir).unwrap();
    std::fs::write(
        data_dir.join("engine-state.json"),
        format!(
            r#"{{"version":2,"accounts":[{{"number":"{SIGNAL_ACCOUNT}","dir":"accounts/lk-abc"}}]}}"#
        ),
    )
    .unwrap();
}

fn call_cred_supervisor(
    temp: &TempDir,
    data_dir: &Path,
) -> (Arc<Store>, Arc<RuntimeSupervisor>, String) {
    seed_engine_registry(data_dir);
    let store = test_store(temp.path());
    let account = store
        .upsert_account_from_signal(
            SIGNAL_ACCOUNT,
            Some(1),
            kt_signal_connector::DEFAULT_PROXY_GROUP_ID,
        )
        .unwrap();
    let mut config = EngineConfig::new(fixture().to_path_buf(), data_dir.to_path_buf());
    config.request_timeout = Duration::from_secs(5);
    config.shutdown_grace = Duration::from_millis(100);
    let supervisor = Arc::new(RuntimeSupervisor::new(
        config,
        Arc::clone(&store),
        kt_signal_connector::DEFAULT_PROXY_GROUP_ID.to_string(),
        None,
    ));
    (store, supervisor, account.id)
}

/// The five happy paths: every face forwards the resolved number plus the
/// caller's material verbatim and passes the engine's result through
/// untouched — the stub shapes pin the wire face (presentation/params base64
/// strings, the proof token, the member list, the endorsement key).
#[tokio::test]
async fn call_material_faces_pass_through_happy_shapes() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("signal-data");
    let (_store, supervisor, account_id) = call_cred_supervisor(&temp, &data_dir);
    supervisor.start().await.unwrap();

    let root_key_16 = b64(&[0x11; 16]);
    let root_key_21 = b64(&[0x11; 21]);
    let room_id = b64(&[0x22; 32]);
    let group_id = b64(&[0x33; 32]);

    // The fixture stubs are the fixed-tag 16-byte runs (bytes 0xF0 ^ tag) —
    // the exact base64 pins the pass-through byte-for-byte.
    let presentation = supervisor
        .calls_link_presentation(&account_id, &root_key_16)
        .await
        .unwrap();
    assert_eq!(presentation["presentation"], b64(&[0xF1; 16]));

    let auth = supervisor
        .calls_create_link_auth(&account_id, &root_key_21, &room_id)
        .await
        .unwrap();
    assert_eq!(auth["presentation"], b64(&[0xF2; 16]));
    assert_eq!(auth["callLinkPublicParams"], b64(&[0xF3; 16]));

    let proof = supervisor
        .calls_group_proof(&account_id, &group_id)
        .await
        .unwrap();
    assert_eq!(proof["proof"], "fixture-group-proof-token");

    let members = supervisor
        .calls_group_members(&account_id, &group_id)
        .await
        .unwrap();
    let members = members["members"].as_array().unwrap();
    assert_eq!(members.len(), 1);
    assert_eq!(members[0]["uuid"], "ae45f0d7-9c15-4e0a-9bf5-6cb610e1f2a3");
    assert_eq!(members[0]["userIdCipherText"], b64(&[0xF4; 16]));

    let key = supervisor.calls_endorsements_public_key().await.unwrap();
    assert_eq!(key["endorsementPublicKey"], b64(&[0xF5; 16]));
}

/// The two-code structured ladder rides the wire verbatim with the
/// contract-fixed faces: CALL_CREDENTIALS_FAILED retryable=false with the
/// engine's data.status pass-through (present when the server answered with
/// one, the empty-object form when it did not), CALL_CREDENTIALS_UNAVAILABLE
/// retryable=true and content-free.
#[tokio::test]
async fn call_credential_error_ladder_rides_the_wire() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("signal-data");
    let (_store, supervisor, account_id) = call_cred_supervisor(&temp, &data_dir);
    supervisor.start().await.unwrap();
    let root_key = b64(&[0x11; 16]);
    let room_id = b64(&[0x22; 32]);
    let group_id = b64(&[0x33; 32]);
    let failed_marker = data_dir.join(".fixture-call-cred-failed");
    let unavailable_marker = data_dir.join(".fixture-call-cred-unavailable");

    // The status face: the engine's data.status rides verbatim.
    std::fs::write(&failed_marker, "status 409").unwrap();
    let error = supervisor
        .calls_link_presentation(&account_id, &root_key)
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "CALL_CREDENTIALS_FAILED");
    assert!(!error.retryable);
    assert_eq!(error.data, Some(json!({ "status": 409 })));

    // The no-status form: the engine's empty data object passes through as
    // the same empty object (pass-through only, no invention, no stripping).
    std::fs::write(&failed_marker, "no status").unwrap();
    let error = supervisor
        .calls_group_proof(&account_id, &group_id)
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "CALL_CREDENTIALS_FAILED");
    assert!(!error.retryable);
    assert_eq!(error.data, Some(json!({})));

    // The unavailable face: retryable, content-free (nothing promised).
    std::fs::remove_file(&failed_marker).unwrap();
    std::fs::write(&unavailable_marker, "1").unwrap();
    let error = supervisor
        .calls_create_link_auth(&account_id, &root_key, &room_id)
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "CALL_CREDENTIALS_UNAVAILABLE");
    assert!(error.retryable);
    assert!(error.data.is_none(), "{error:?}");

    std::fs::remove_file(&unavailable_marker).unwrap();
}

/// The shape gates answer INVALID_REQUEST locally — wrong decoded byte
/// lengths and non-base64 material never reach the engine (the fixture's own
/// -32603 shape gate would surface as the generic upstream error, so an
/// INVALID_REQUEST answer here is the connector's own voice). The gate ladder
/// order holds: shape before resolution, resolution before the seam.
#[tokio::test]
async fn call_material_shape_gates_answer_locally() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("signal-data");
    let (_store, supervisor, account_id) = call_cred_supervisor(&temp, &data_dir);
    supervisor.start().await.unwrap();

    for bad in [
        "not-b64!!",
        "eA",
        &b64(&[0x11; 15]),
        &b64(&[0x11; 17]),
        &b64(&[0x11; 20]),
        &b64(&[0x11; 22]),
        &b64(&[0x11; 32]),
    ] {
        let error = supervisor
            .calls_link_presentation(&account_id, bad)
            .await
            .err()
            .unwrap()
            .into_api();
        assert_eq!(error.code, "INVALID_REQUEST", "{bad}");
        assert!(!error.retryable, "{bad}");
    }

    // roomId and groupId: exactly 32 bytes.
    let root_key = b64(&[0x11; 16]);
    for bad in [b64(&[0x22; 16]), b64(&[0x22; 31]), b64(&[0x22; 33])] {
        let error = supervisor
            .calls_create_link_auth(&account_id, &root_key, &bad)
            .await
            .err()
            .unwrap()
            .into_api();
        assert_eq!(error.code, "INVALID_REQUEST");
        let error = supervisor
            .calls_group_proof(&account_id, &bad)
            .await
            .err()
            .unwrap()
            .into_api();
        assert_eq!(error.code, "INVALID_REQUEST");
    }

    // The gate ladder: shape wins over resolution, resolution wins over the
    // seam (observed through the kt-engine supervisor, where the seam passes
    // and the shape/resolution faces stand alone).
    let error = supervisor
        .calls_link_presentation("kt-acct-missing", "not-b64!!")
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "INVALID_REQUEST");
    let error = supervisor
        .calls_link_presentation("kt-acct-missing", &root_key)
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "ACCOUNT_NOT_FOUND");
}
