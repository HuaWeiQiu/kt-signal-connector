// SPDX-License-Identifier: AGPL-3.0-only

#![cfg(unix)]

//! `calls.relays` proxy end to end (contract revision 1.145, §4.143; engine
//! e2f2e6d): the connector resolves the account, forwards `{account}` on the
//! read lane, and passes the engine's answer through untouched — the relay
//! material (the TURN credentials among it) crosses this process exactly
//! once, never parsed, never persisted, never logged. The fixture plays the
//! engine (the stub TURN material is NOT valid credentials, it pins the wire
//! shape, the engine fake discipline); the error faces are the engine's
//! verbatim — the plain -32603 unknown-account face, the 401/403 unbound
//! message riding the existing §4.17 authorization matcher — and these
//! tests pin the connector's routing and error fidelity, never real engine
//! relay behavior (the real-device chat-service chain is the engine batch's
//! own acceptance).

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

fn test_store(dir: &Path) -> Arc<Store> {
    Arc::new(Store::open(dir, Some(StoreKey::from_bytes([0x5A; 32]))).unwrap())
}

const SIGNAL_ACCOUNT: &str = "+15555550100";

/// `registry_accounts` is the raw JSON array body of the engine-side
/// registry — the store row and the engine registry are two separate
/// truths, and the unknown-account test needs them to disagree.
fn relay_supervisor(
    temp: &TempDir,
    data_dir: &Path,
    registry_accounts: &str,
) -> (Arc<Store>, Arc<RuntimeSupervisor>, String) {
    std::fs::create_dir_all(data_dir).unwrap();
    std::fs::write(
        data_dir.join("engine-state.json"),
        format!(r#"{{"version":2,"accounts":[{registry_accounts}]}}"#),
    )
    .unwrap();
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

/// The happy pass-through pinned byte-for-byte: every optional relay field
/// present, the freshness pair riding, then the empty-state and
/// refresh-failure faces with the absent keys staying absent — the engine's
/// no-null discipline mirrored at the pipe (the proxy invents no key, strips
/// no key, repairs nothing).
#[tokio::test]
async fn call_relays_pass_through_rides_the_engine_answer_verbatim() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("signal-data");
    let (_store, supervisor, account_id) = relay_supervisor(
        &temp,
        &data_dir,
        r#"{"number":"+15555550100","dir":"accounts/lk-abc"}"#,
    );
    supervisor.start().await.unwrap();

    let relays = supervisor.calls_relays(&account_id).await.unwrap();
    assert_eq!(
        relays,
        json!({
            "account": "+15555550100",
            "relays": [{
                "username": "fixture-relay-user",
                "password": "fixture-relay-pass",
                "urls": ["turn:turn.example.org:80?transport=tcp"],
                "urlsWithIps": ["1.2.3.4:3478"],
                "hostname": "turn.example.org",
                "ttl": 86400,
            }],
            "fetchedAtMs": 1_000,
            "expiresAtMs": 86_501_000,
            "stale": false,
        })
    );

    // The empty state: the no-cache face — fetchedAtMs/expiresAtMs absent,
    // never null.
    std::fs::write(data_dir.join(".fixture-call-relays-empty"), "1").unwrap();
    let relays = supervisor.calls_relays(&account_id).await.unwrap();
    assert_eq!(
        relays,
        json!({ "account": "+15555550100", "relays": [], "stale": true })
    );

    // The refresh-failure face: the honest lastError rides the empty state
    // (the engine answers 200 — a failed refresh never errors the face).
    std::fs::remove_file(data_dir.join(".fixture-call-relays-empty")).unwrap();
    std::fs::write(data_dir.join(".fixture-call-relays-refresh-failed"), "1").unwrap();
    let relays = supervisor.calls_relays(&account_id).await.unwrap();
    assert_eq!(
        relays,
        json!({
            "account": "+15555550100",
            "relays": [],
            "stale": true,
            "lastError": "http 500",
        })
    );
    std::fs::remove_file(data_dir.join(".fixture-call-relays-refresh-failed")).unwrap();
}

/// The engine-side unknown account: the store row resolves (connector-local
/// truth), the engine's own registry does not know the number, and the
/// engine's plain -32603 degrades through the structured ladder to
/// UPSTREAM_ERROR — no matcher fires, no account state moves.
#[tokio::test]
async fn call_relays_unknown_engine_account_answers_upstream_error() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("signal-data");
    let (_store, supervisor, account_id) = relay_supervisor(
        &temp,
        &data_dir,
        r#"{"number":"+19999999999","dir":"accounts/lk-other"}"#,
    );
    supervisor.start().await.unwrap();

    let error = supervisor
        .calls_relays(&account_id)
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "UPSTREAM_ERROR");
    assert!(error.retryable, "{error:?}");
}

/// The 401/403 face: the engine's unbound message rides word for word
/// through the §4.17 authorization matcher — ACCOUNT_UNLINKED retryable
/// false, the relink recovery the whole unlink chain shares. The connector
/// adds no matcher of its own (the get_device_capabilities posture).
#[tokio::test]
async fn call_relays_unbound_face_answers_account_unlinked() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("signal-data");
    let (_store, supervisor, account_id) = relay_supervisor(
        &temp,
        &data_dir,
        r#"{"number":"+15555550100","dir":"accounts/lk-abc"}"#,
    );
    supervisor.start().await.unwrap();
    std::fs::write(data_dir.join(".fixture-call-relays-unbound"), "1").unwrap();

    let error = supervisor
        .calls_relays(&account_id)
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "ACCOUNT_UNLINKED");
    assert_eq!(error.message, "device was unlinked by the account holder");
    assert!(!error.retryable, "{error:?}");
    std::fs::remove_file(data_dir.join(".fixture-call-relays-unbound")).unwrap();
}

/// The engine-down shape: the proxy answers RUNTIME_NOT_RUNNING — the read
/// lane never invents relay material, the absent engine is the honest face.
#[tokio::test]
async fn call_relays_engine_down_answers_runtime_not_running() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("signal-data");
    let (_store, supervisor, account_id) = relay_supervisor(
        &temp,
        &data_dir,
        r#"{"number":"+15555550100","dir":"accounts/lk-abc"}"#,
    );
    // No start: the store row resolves, the engine was never spawned.

    let error = supervisor
        .calls_relays(&account_id)
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "RUNTIME_NOT_RUNNING");
    assert!(!error.retryable, "{error:?}");
}
