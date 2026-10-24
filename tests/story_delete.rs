// SPDX-License-Identifier: AGPL-3.0-only

#![cfg(unix)]

//! `stories.deleteForEveryone` proxy end to end (contract revision 1.153,
//! §4.152; engine 7320393): the §4.150 call-event proxy posture on the
//! stories family — the connector resolves the account, forwards the
//! engine's `deleteStoryForEveryone` on the send lane as its own mutating
//! call, and the engine's `{timestamp}` answer and error faces pass through
//! untouched. The whole value domain (the timestamp domain, the recipients
//! entries and the rejected single-string form, the updatedStoryRecipients
//! member shapes, the author resolution) is the engine's fail-closed parse,
//! never re-gated or re-keyed here; the optional `recipients` key stays
//! absent when absent, never null. The fixture plays the engine (the stub
//! answer pins the wire shape, the engine fake discipline); these tests pin
//! the connector's verbatim forward and error fidelity, never real engine
//! delete behavior (the real-device fan-out is the engine batch's own
//! acceptance).

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use kt_signal_connector::engine::EngineConfig;
use kt_signal_connector::service::StoriesDeleteForEveryoneParams;
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
fn story_delete_supervisor(
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

fn updated() -> Value {
    json!([
        {
            "destinationUuid": "9d6f9f4f-2f3c-4a5b-8c7d-6e5f4a3b2c1d",
            "isAllowedToReply": true,
        },
        {
            "destinationUuid": "0a1b2c3d-4e5f-6a7b-8c9d-0e1f2a3b4c5d",
            "isAllowedToReply": false,
            "distributionListIds": ["list-1"],
        },
    ])
}

fn deletion(
    account_id: &str,
    recipients: Option<Value>,
    updated: Value,
) -> StoriesDeleteForEveryoneParams {
    StoriesDeleteForEveryoneParams {
        account_id: account_id.to_string(),
        target_timestamp: json!(1_727_000_000_000u64),
        recipients,
        updated_story_recipients: updated,
        operation_id: None,
    }
}

/// The verbatim forward pinned byte-for-byte: the engine's `{timestamp}`
/// answer (the DOE envelope ts) passes through untouched, the upstream
/// frame carries the resolved signal account with the three story keys
/// verbatim, and the optional `recipients` key stays absent when absent —
/// never null (the no-null discipline; the engine reads absence as the
/// empty batch, the frame carries the caller's shape).
#[tokio::test]
async fn story_delete_forward_rides_the_engine_verbatim() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("signal-data");
    let (_store, supervisor, account_id) = story_delete_supervisor(
        &temp,
        &data_dir,
        r#"{"number":"+15555550100","dir":"accounts/lk-abc"}"#,
    );
    supervisor.start().await.unwrap();

    // The fanned-out leg: every key present.
    let answered = supervisor
        .send_story_delete_for_everyone(deletion(
            &account_id,
            Some(json!(["+15555550101", "+15555550102"])),
            updated(),
        ))
        .await
        .unwrap();
    assert_eq!(answered, json!({ "timestamp": 1727000999999u64 }));

    let log = log_lines(&data_dir);
    assert_eq!(log.len(), 1, "{log:?}");
    assert_eq!(
        log[0],
        json!({
            "account": SIGNAL_ACCOUNT,
            "targetTimestamp": 1_727_000_000_000u64,
            "recipients": ["+15555550101", "+15555550102"],
            "updatedStoryRecipients": updated(),
        })
    );

    // The no-recipient leg: `recipients` absent, the frame carries exactly
    // the three keys — the engine's empty-batch short-circuit to the
    // self-sync leg is the engine's reading, the connector invents no key.
    let answered = supervisor
        .send_story_delete_for_everyone(deletion(&account_id, None, updated()))
        .await
        .unwrap();
    assert_eq!(answered, json!({ "timestamp": 1727000999999u64 }));
    let log = log_lines(&data_dir);
    assert_eq!(log.len(), 2, "{log:?}");
    assert_eq!(
        log[1],
        json!({
            "account": SIGNAL_ACCOUNT,
            "targetTimestamp": 1_727_000_000_000u64,
            "updatedStoryRecipients": updated(),
        })
    );
    assert!(log[1].get("recipients").is_none());
}

/// The error faces: the engine's parse-gate refusal (the marker standing
/// for the timestamp/recipients/updatedStoryRecipients value-domain faces)
/// and the engine-side unknown account both degrade through the structured
/// ladder to UPSTREAM_ERROR retryable; an unshaped opaque account id
/// answers INVALID_REQUEST locally before any engine traffic; the
/// engine-down shape answers RUNTIME_NOT_RUNNING non-retryable.
#[tokio::test]
async fn story_delete_error_faces_ride_the_wire_verbatim() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("signal-data");
    let (_store, supervisor, account_id) = story_delete_supervisor(
        &temp,
        &data_dir,
        r#"{"number":"+15555550100","dir":"accounts/lk-abc"}"#,
    );
    supervisor.start().await.unwrap();

    // The connector-local envelope gate: an unshaped opaque account id
    // never reaches the engine.
    let error = supervisor
        .send_story_delete_for_everyone(deletion(&"k".repeat(129), None, updated()))
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "INVALID_REQUEST");
    assert!(!error.retryable, "{error:?}");
    assert!(
        !data_dir.join(".fixture-send-log.jsonl").exists(),
        "the unshaped account id must not reach the engine"
    );

    // The engine's parse-gate refusal: the fixture marker answers the
    // plain -32603 face the real engine's fail-closed parse answers for
    // every value-domain violation.
    std::fs::write(data_dir.join(".fixture-story-delete-rejected"), "1").unwrap();
    let error = supervisor
        .send_story_delete_for_everyone(deletion(&account_id, None, updated()))
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "UPSTREAM_ERROR");
    assert!(error.retryable, "{error:?}");
    std::fs::remove_file(data_dir.join(".fixture-story-delete-rejected")).unwrap();

    // The engine-side unknown account: the store row resolves
    // (connector-local truth), the engine's own registry does not know the
    // number — the same plain -32603 through the ladder.
    let other = TempDir::new().unwrap();
    let other_data = other.path().join("signal-data");
    let (_other_store, other_supervisor, other_account) = story_delete_supervisor(
        &other,
        &other_data,
        r#"{"number":"+19999999999","dir":"accounts/lk-other"}"#,
    );
    other_supervisor.start().await.unwrap();
    let error = other_supervisor
        .send_story_delete_for_everyone(deletion(&other_account, None, updated()))
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "UPSTREAM_ERROR");
    assert!(error.retryable, "{error:?}");

    // Engine down: no start, the store row resolves, the engine was never
    // spawned — RUNTIME_NOT_RUNNING, non-retryable (the send lane invents
    // no answer).
    let down = TempDir::new().unwrap();
    let down_data = down.path().join("signal-data");
    let (_down_store, down_supervisor, down_account) = story_delete_supervisor(
        &down,
        &down_data,
        r#"{"number":"+15555550100","dir":"accounts/lk-abc"}"#,
    );
    let error = down_supervisor
        .send_story_delete_for_everyone(deletion(&down_account, None, updated()))
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "RUNTIME_NOT_RUNNING");
    assert!(!error.retryable, "{error:?}");
}
