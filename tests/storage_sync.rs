// SPDX-License-Identifier: AGPL-3.0-only

#![cfg(unix)]

//! Storage merge/import/call-link faces end to end (contract revision
//! 1.131, §4.112): the four `storage.*` faces resolve the account, forward
//! the official frame, and pass the engine's own summaries and call-link
//! projections through untouched — the connector neither interprets nor
//! re-keys the bodies and persists nothing. The fixture plays the engine,
//! the structured STORAGE ladder steers from markers, the mutating-crash
//! sentinel pins the unknown-outcome discipline.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use kt_signal_connector::engine::SignalCliConfig;
use kt_signal_connector::service::{
    StorageGetCallLinksParams, StorageIngestParams, StorageSetCallLinkParams,
};
use kt_signal_connector::store::{Store, StoreKey};
use kt_signal_connector::supervisor::RuntimeSupervisor;
use serde_json::Value;
use tempfile::TempDir;

fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fake-signal-cli.py")
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

fn storage_supervisor(
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
    let mut config = SignalCliConfig::new(fixture().to_path_buf(), data_dir.to_path_buf());
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

/// The ingest summaries ride verbatim: both faces answer the engine's own
/// camelCase tallies key for key, and the upstream frame carries exactly the
/// resolved signal account.
#[tokio::test]
async fn storage_ingest_summaries_pass_through_verbatim() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("signal-data");
    let (_store, supervisor, account_id) = storage_supervisor(&temp, &data_dir);
    supervisor.start().await.unwrap();

    let merged = supervisor
        .storage_merge_contacts(StorageIngestParams {
            account_id: account_id.clone(),
        })
        .await
        .unwrap();
    assert_eq!(merged.records_read, 12);
    assert_eq!(merged.skipped, 1);
    assert_eq!(merged.identities_new, 3);
    assert_eq!(merged.identities_replaced, 0);
    assert_eq!(merged.profile_keys_set, 2);
    assert_eq!(merged.contacts_created, 4);
    assert_eq!(merged.trust_states_synced, 5);

    let imported = supervisor
        .storage_import_groups(StorageIngestParams {
            account_id: account_id.clone(),
        })
        .await
        .unwrap();
    assert_eq!(imported.records_read, 7);
    assert_eq!(imported.imported, 5);
    assert_eq!(imported.already_known, 1);
    assert_eq!(imported.failed, 1);

    // The exact upstream frames: the resolved signal account (never the
    // host's opaque id), nothing else.
    let log = log_lines(&data_dir);
    let ingest_calls: Vec<&Value> = log
        .iter()
        .filter(|params| {
            params.get("account").is_some() && params.as_object().is_some_and(|o| o.len() == 1)
        })
        .collect();
    assert_eq!(ingest_calls.len(), 2, "{ingest_calls:?}");
    assert_eq!(ingest_calls[0]["account"], SIGNAL_ACCOUNT);
    assert_eq!(ingest_calls[1]["account"], SIGNAL_ACCOUNT);
}

/// The call-link faces round-trip: the read projects the fixture links
/// verbatim (live-with-passkey and tombstoned-without), the write forwards
/// exactly the provided arms and answers the post-write cloud state, and
/// the local intent ladder answers INVALID_REQUEST before any upstream
/// call.
#[tokio::test]
async fn storage_call_link_faces_round_trip() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("signal-data");
    let (_store, supervisor, account_id) = storage_supervisor(&temp, &data_dir);
    supervisor.start().await.unwrap();

    let links = supervisor
        .storage_get_call_links(StorageGetCallLinksParams {
            account_id: account_id.clone(),
        })
        .await
        .unwrap();
    assert_eq!(links.call_links.len(), 2);
    assert_eq!(
        links.call_links[0]["rootKey"],
        "Zml4dHVyZS1jYWxsLWxpbmtcm9vdGtleTE="
    );
    assert_eq!(
        links.call_links[0]["adminPasskey"],
        "cGFzc2tleS1ieXRlcy0xNg=="
    );
    assert!(links.call_links[0]["deletedAtTimestampMs"].is_null());
    assert_eq!(
        links.call_links[1]["rootKey"],
        "Zml4dHVyZS10b21ic3RvbmUtbGluaw=="
    );
    assert!(links.call_links[1]["adminPasskey"].is_null());
    assert_eq!(
        links.call_links[1]["deletedAtTimestampMs"],
        1720000600000_u64
    );

    // The active-arm write: the frame carries account + rootKey +
    // adminPasskey and no deletedAtTimestampMs key; the read-back answers
    // the post-write cloud state.
    let written = supervisor
        .storage_set_call_link(StorageSetCallLinkParams {
            account_id: account_id.clone(),
            root_key: "bmV3LWxpbmstcm9vdGtleS0zMmJ5dGVzIQ==".to_string(),
            admin_passkey: Some("YWRtaW4tcGFzc2tleS0zMmJ5dGVzIQ==".to_string()),
            deleted_at_timestamp_ms: None,
        })
        .await
        .unwrap();
    assert_eq!(written.call_links.len(), 3);
    let new_link = written
        .call_links
        .iter()
        .find(|link| link["rootKey"] == "bmV3LWxpbmstcm9vdGtleS0zMmJ5dGVzIQ==")
        .unwrap();
    assert_eq!(new_link["adminPasskey"], "YWRtaW4tcGFzc2tleS0zMmJ5dGVzIQ==");
    assert!(new_link["deletedAtTimestampMs"].is_null());
    let log = log_lines(&data_dir);
    let write_calls: Vec<&Value> = log
        .iter()
        .filter(|params| params.get("rootKey").is_some())
        .collect();
    assert_eq!(write_calls.len(), 1, "{write_calls:?}");
    assert_eq!(write_calls[0]["account"], SIGNAL_ACCOUNT);
    assert_eq!(
        write_calls[0]["rootKey"],
        "bmV3LWxpbmstcm9vdGtleS0zMmJ5dGVzIQ=="
    );
    assert_eq!(
        write_calls[0]["adminPasskey"],
        "YWRtaW4tcGFzc2tleS0zMmJ5dGVzIQ=="
    );
    assert!(write_calls[0].get("deletedAtTimestampMs").is_none());

    // The delete-arm write: symmetric framing — the tombstone key rides,
    // the passkey key stays absent, and the tombstoned rootKey is replaced
    // in place (upsert by rootKey).
    let written = supervisor
        .storage_set_call_link(StorageSetCallLinkParams {
            account_id: account_id.clone(),
            root_key: "bmV3LWxpbmstcm9vdGtleS0zMmJ5dGVzIQ==".to_string(),
            admin_passkey: None,
            deleted_at_timestamp_ms: Some(1720000700000),
        })
        .await
        .unwrap();
    assert_eq!(written.call_links.len(), 3);
    let tombstoned = written
        .call_links
        .iter()
        .find(|link| link["rootKey"] == "bmV3LWxpbmstcm9vdGtleS0zMmJ5dGVzIQ==")
        .unwrap();
    assert!(tombstoned["adminPasskey"].is_null());
    assert_eq!(tombstoned["deletedAtTimestampMs"], 1720000700000_u64);
    let log = log_lines(&data_dir);
    let write_calls: Vec<&Value> = log
        .iter()
        .filter(|params| params.get("rootKey").is_some())
        .collect();
    assert_eq!(write_calls.len(), 2, "{write_calls:?}");
    assert!(write_calls[1].get("adminPasskey").is_none());
    assert_eq!(write_calls[1]["deletedAtTimestampMs"], 1720000700000_u64);

    // The local intent ladder (all INVALID_REQUEST, nothing upstream — the
    // log stays at two writes): the exactly-one rule both ways, the
    // zero-timestamp refusal, and the rootKey identity face.
    for bad in [
        StorageSetCallLinkParams {
            account_id: account_id.clone(),
            root_key: "c29tZS1yb290LWtleQ==".to_string(),
            admin_passkey: Some("cGFzcw==".to_string()),
            deleted_at_timestamp_ms: Some(1),
        },
        StorageSetCallLinkParams {
            account_id: account_id.clone(),
            root_key: "c29tZS1yb290LWtleQ==".to_string(),
            admin_passkey: None,
            deleted_at_timestamp_ms: None,
        },
        StorageSetCallLinkParams {
            account_id: account_id.clone(),
            root_key: "c29tZS1yb290LWtleQ==".to_string(),
            admin_passkey: None,
            deleted_at_timestamp_ms: Some(0),
        },
        StorageSetCallLinkParams {
            account_id: account_id.clone(),
            root_key: String::new(),
            admin_passkey: Some("cGFzcw==".to_string()),
            deleted_at_timestamp_ms: None,
        },
        StorageSetCallLinkParams {
            account_id: account_id.clone(),
            root_key: "a".repeat(129),
            admin_passkey: Some("cGFzcw==".to_string()),
            deleted_at_timestamp_ms: None,
        },
    ] {
        let error = supervisor
            .storage_set_call_link(bad)
            .await
            .err()
            .unwrap()
            .into_api();
        assert_eq!(error.code, "INVALID_REQUEST");
        assert!(!error.retryable);
    }
    let log = log_lines(&data_dir);
    let write_calls: Vec<&Value> = log
        .iter()
        .filter(|params| params.get("rootKey").is_some())
        .collect();
    assert_eq!(write_calls.len(), 2, "refusals never spend a frame");

    // Unknown account: ACCOUNT_NOT_FOUND before anything travels.
    let error = supervisor
        .storage_merge_contacts(StorageIngestParams {
            account_id: "kt-acct-missing".to_string(),
        })
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "ACCOUNT_NOT_FOUND");
    let error = supervisor
        .storage_get_call_links(StorageGetCallLinksParams {
            account_id: "kt-acct-missing".to_string(),
        })
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "ACCOUNT_NOT_FOUND");
}

/// The structured storage ladder rides the wire verbatim on all four faces
/// (retryable=true — the engine owns the family's final set): markers land
/// STORAGE_WRITE_FAILED on the write, STORAGE_READ_FAILED on the reads,
/// STORAGE_UNAVAILABLE everywhere, and an engine-unknown account degrades
/// to UPSTREAM_ERROR.
#[tokio::test]
async fn storage_error_ladder_rides_the_wire() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("signal-data");
    let (store, supervisor, account_id) = storage_supervisor(&temp, &data_dir);
    supervisor.start().await.unwrap();
    let marker = |name: &str| data_dir.join(name);

    fs::write(marker(".fixture-storage-sync-read-failed"), "1").unwrap();
    let error = supervisor
        .storage_merge_contacts(StorageIngestParams {
            account_id: account_id.clone(),
        })
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "STORAGE_READ_FAILED");
    assert!(error.retryable);
    let error = supervisor
        .storage_get_call_links(StorageGetCallLinksParams {
            account_id: account_id.clone(),
        })
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "STORAGE_READ_FAILED");
    fs::remove_file(marker(".fixture-storage-sync-read-failed")).unwrap();

    fs::write(marker(".fixture-storage-sync-unavailable"), "1").unwrap();
    let error = supervisor
        .storage_import_groups(StorageIngestParams {
            account_id: account_id.clone(),
        })
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "STORAGE_UNAVAILABLE");
    assert!(error.retryable);
    let error = supervisor
        .storage_get_call_links(StorageGetCallLinksParams {
            account_id: account_id.clone(),
        })
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "STORAGE_UNAVAILABLE");
    fs::remove_file(marker(".fixture-storage-sync-unavailable")).unwrap();

    fs::write(marker(".fixture-storage-sync-write-failed"), "1").unwrap();
    let error = supervisor
        .storage_set_call_link(StorageSetCallLinkParams {
            account_id: account_id.clone(),
            root_key: "c29tZS1yb290LWtleQ==".to_string(),
            admin_passkey: Some("cGFzcw==".to_string()),
            deleted_at_timestamp_ms: None,
        })
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "STORAGE_WRITE_FAILED");
    assert!(error.retryable);
    fs::remove_file(marker(".fixture-storage-sync-write-failed")).unwrap();

    // An account registered connector-side but absent from the engine's own
    // registry: the engine's -32603 degrades to the plain upstream error.
    let ghost = store
        .upsert_account_from_signal(
            "+15565550004",
            Some(1),
            kt_signal_connector::DEFAULT_PROXY_GROUP_ID,
        )
        .unwrap();
    let error = supervisor
        .storage_merge_contacts(StorageIngestParams {
            account_id: ghost.id,
        })
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "UPSTREAM_ERROR");
    assert!(error.retryable);
}

/// The mutating-crash sentinel: the engine dying with the write in flight
/// answers SEND_OUTCOME_UNKNOWN (retryable=false) and nothing retries —
/// exactly one such call in the fixture log.
#[tokio::test]
async fn storage_set_call_link_crash_answers_unknown_and_never_retries() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("signal-data");
    let (_store, supervisor, account_id) = storage_supervisor(&temp, &data_dir);
    supervisor.start().await.unwrap();

    let error = supervisor
        .storage_set_call_link(StorageSetCallLinkParams {
            account_id: account_id.clone(),
            root_key: "Zml4dHVyZS1jcmFzaC0zMg==".to_string(),
            admin_passkey: Some("cGFzcw==".to_string()),
            deleted_at_timestamp_ms: None,
        })
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "SEND_OUTCOME_UNKNOWN");
    assert!(!error.retryable);

    let log = log_lines(&data_dir);
    let crash_calls: Vec<&Value> = log
        .iter()
        .filter(|params| {
            params.get("rootKey").and_then(Value::as_str) == Some("Zml4dHVyZS1jcmFzaC0zMg==")
        })
        .collect();
    assert_eq!(
        crash_calls.len(),
        1,
        "an unknown write outcome is never retried: {crash_calls:?}"
    );
}
