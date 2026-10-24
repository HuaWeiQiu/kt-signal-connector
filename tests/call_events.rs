// SPDX-License-Identifier: AGPL-3.0-only

#![cfg(unix)]

//! `calls.sendCallEvent` proxy end to end (contract revision 1.151, §4.150;
//! engine 6d56660): the connector resolves the account, forwards the
//! call-event self-sync on the send lane, and passes the engine's
//! `{timestamp}` answer and error faces through untouched — the three word
//! lists, the per-leg conversation shapes, the callId domain, and the
//! per-leg callId requirement are the engine's own fail-closed parse gate,
//! never re-gated or re-keyed here (the event source wiring lives in the
//! calling sidecar, desktop consumption is a later batch). The fixture
//! plays the engine (the stub answer pins the wire shape, the engine fake
//! discipline); these tests pin the connector's verbatim forward and error
//! fidelity, never real engine call behavior (the real-device sync chain
//! is the engine batch's own acceptance).

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use kt_signal_connector::engine::EngineConfig;
use kt_signal_connector::service::CallsSendCallEventParams;
use kt_signal_connector::store::{Store, StoreKey};
use kt_signal_connector::supervisor::RuntimeSupervisor;
use serde_json::{Value, json};
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
fn call_event_supervisor(
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

fn log_lines(data_dir: &Path) -> Vec<Value> {
    fs::read_to_string(data_dir.join(".fixture-send-log.jsonl"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect()
}

/// The verbatim forward pinned byte-for-byte on both legs: the direct leg
/// carries every documented key (the u64-wide decimal callId string, the
/// explicit timestamp) and the engine's `{timestamp}` echo rides through;
/// the ad-hoc leg omits both optional keys and the upstream frame shows
/// them absent, never null — the proxy invents no key, strips no key,
/// repairs nothing.
#[tokio::test]
async fn call_event_forward_rides_the_engine_verbatim() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("signal-data");
    let (_store, supervisor, account_id) = call_event_supervisor(
        &temp,
        &data_dir,
        r#"{"number":"+15555550100","dir":"accounts/lk-abc"}"#,
    );
    supervisor.start().await.unwrap();

    // The direct leg: every key present, the callId string past 2^53 (the
    // official longToString face pinned at the u64 edge the JSON number
    // face cannot carry), the explicit timestamp echoed back.
    let answered = supervisor
        .send_call_event(CallsSendCallEventParams {
            account_id: account_id.clone(),
            call_type: "audio".to_string(),
            direction: "incoming".to_string(),
            event: "accepted".to_string(),
            conversation_id: "+15555550199".to_string(),
            call_id: Some("9007199254740993".to_string()),
            timestamp: Some(json!(1_770_000_000_000u64)),
        })
        .await
        .unwrap();
    assert_eq!(answered, json!({ "timestamp": 1_770_000_000_000u64 }));

    let log = log_lines(&data_dir);
    assert_eq!(log.len(), 1, "{log:?}");
    assert_eq!(
        log[0],
        json!({
            "account": SIGNAL_ACCOUNT,
            "type": "audio",
            "direction": "incoming",
            "event": "accepted",
            "conversationId": "+15555550199",
            "callId": "9007199254740993",
            "timestamp": 1_770_000_000_000u64,
        })
    );

    // The ad-hoc leg: callId and timestamp both absent (the official
    // call-link placeholder face) — the optional keys stay absent on the
    // upstream wire, and the stub answers the envelope moment.
    let answered = supervisor
        .send_call_event(CallsSendCallEventParams {
            account_id: account_id.clone(),
            call_type: "ad-hoc".to_string(),
            direction: "outgoing".to_string(),
            event: "observed".to_string(),
            conversation_id: "abcdef0123456789abcdef0123456789".to_string(),
            call_id: None,
            timestamp: None,
        })
        .await
        .unwrap();
    assert_eq!(answered, json!({ "timestamp": 1_000 }));

    let log = log_lines(&data_dir);
    assert_eq!(log.len(), 2, "{log:?}");
    assert_eq!(
        log[1],
        json!({
            "account": SIGNAL_ACCOUNT,
            "type": "ad-hoc",
            "direction": "outgoing",
            "event": "observed",
            "conversationId": "abcdef0123456789abcdef0123456789",
        })
    );
    assert!(log[1].get("callId").is_none());
    assert!(log[1].get("timestamp").is_none());
}

/// The error faces are the engine's, verbatim: the parse-gate face (the
/// plain -32603 the real engine answers for its word-list, shape, and
/// callId-domain refusals) degrades through the structured ladder to
/// UPSTREAM_ERROR retryable — no connector matcher fires, no account state
/// moves; the engine-side unknown account answers the same plain face; the
/// engine-down shape answers RUNTIME_NOT_RUNNING (the absent engine is the
/// honest face, the send lane invents no answer).
#[tokio::test]
async fn call_event_error_faces_ride_the_wire_verbatim() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("signal-data");
    let (_store, supervisor, account_id) = call_event_supervisor(
        &temp,
        &data_dir,
        r#"{"number":"+15555550100","dir":"accounts/lk-abc"}"#,
    );
    supervisor.start().await.unwrap();

    // The engine's parse-gate refusal: the fixture marker answers the
    // plain -32603 face the real engine's fail-closed parse answers for
    // every semantic violation (an out-of-vocabulary word here).
    std::fs::write(data_dir.join(".fixture-call-event-rejected"), "1").unwrap();
    let error = supervisor
        .send_call_event(CallsSendCallEventParams {
            account_id: account_id.clone(),
            call_type: "video".to_string(),
            direction: "outgoing".to_string(),
            event: "delete".to_string(),
            conversation_id: "+15555550199".to_string(),
            call_id: Some("7".to_string()),
            timestamp: None,
        })
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "UPSTREAM_ERROR");
    assert!(error.retryable, "{error:?}");
    std::fs::remove_file(data_dir.join(".fixture-call-event-rejected")).unwrap();

    // The engine-side unknown account: the store row resolves
    // (connector-local truth), the engine's own registry does not know the
    // number — the same plain -32603 through the ladder.
    let other = TempDir::new().unwrap();
    let other_data = other.path().join("signal-data");
    let (_other_store, other_supervisor, other_account) = call_event_supervisor(
        &other,
        &other_data,
        r#"{"number":"+19999999999","dir":"accounts/lk-other"}"#,
    );
    other_supervisor.start().await.unwrap();
    let error = other_supervisor
        .send_call_event(CallsSendCallEventParams {
            account_id: other_account,
            call_type: "group".to_string(),
            direction: "incoming".to_string(),
            event: "not-accepted".to_string(),
            conversation_id: "ZmFrZS1ncm91cC1tYXN0ZXIta2V5LTIzNDU2Nzg5MA==".to_string(),
            call_id: Some("7".to_string()),
            timestamp: None,
        })
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "UPSTREAM_ERROR");
    assert!(error.retryable, "{error:?}");

    // Engine down: no start, the store row resolves, the engine was never
    // spawned — RUNTIME_NOT_RUNNING, non-retryable.
    let down = TempDir::new().unwrap();
    let down_data = down.path().join("signal-data");
    let (_down_store, down_supervisor, down_account) = call_event_supervisor(
        &down,
        &down_data,
        r#"{"number":"+15555550100","dir":"accounts/lk-abc"}"#,
    );
    let error = down_supervisor
        .send_call_event(CallsSendCallEventParams {
            account_id: down_account,
            call_type: "audio".to_string(),
            direction: "incoming".to_string(),
            event: "accepted".to_string(),
            conversation_id: "+15555550199".to_string(),
            call_id: Some("7".to_string()),
            timestamp: None,
        })
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "RUNTIME_NOT_RUNNING");
    assert!(!error.retryable, "{error:?}");
}
