// SPDX-License-Identifier: AGPL-3.0-only

#![cfg(unix)]

//! Notification-profile sync passthrough end to end (contract revision
//! 1.129, §4.110): the two `notificationProfiles.*` faces resolve the
//! account, forward the official frame verbatim, and pass the engine's
//! NotificationProfileJson entries through untouched — the connector neither
//! interprets nor re-keys the record body and persists nothing (the §4.78
//! chat-folder discipline; engine compat §3/§5: the record content never
//! lands connector-side). The fixture plays the engine (the stub records pin
//! the wire shape, never real cloud state), the structured STORAGE ladder
//! steers from markers, the mutating-crash sentinel pins the unknown-outcome
//! discipline.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use kt_signal_connector::engine::EngineConfig;
use kt_signal_connector::service::{
    NotificationProfilesListParams, NotificationProfilesSetRecordsParams,
};
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

fn profile_supervisor(
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

fn log_lines(data_dir: &Path) -> Vec<Value> {
    fs::read_to_string(data_dir.join(".fixture-send-log.jsonl"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect()
}

/// The full happy passthrough: the read projects the engine's
/// NotificationProfileJson entries verbatim (both fixture profiles pinned
/// key for key — the schedule-less profile and the scheduled profile with an
/// allowed member and the optional time windows), and the write forwards
/// only the non-empty half — records verbatim on one call, removeIds
/// verbatim on the other — and answers the post-write cloud state.
#[tokio::test]
async fn notification_profile_faces_pass_through_verbatim() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("signal-data");
    let (_store, supervisor, account_id) = profile_supervisor(&temp, &data_dir);
    supervisor.start().await.unwrap();

    let list = supervisor
        .notification_profiles_list(NotificationProfilesListParams {
            account_id: account_id.clone(),
        })
        .await
        .unwrap();
    let profiles = &list.profiles;
    assert_eq!(profiles.len(), 2);
    let work = &profiles[0];
    assert_eq!(work["id"], "a3f1c2d4e5b6478901a2b3c4d5e6f701");
    assert_eq!(work["name"], "Work");
    assert_eq!(work["color"], 4);
    assert_eq!(work["createdAtMs"], 1720000000000_u64);
    assert_eq!(work["allowAllCalls"], false);
    assert_eq!(work["allowAllMentions"], true);
    assert_eq!(work["allowedMembers"], json!([]));
    assert_eq!(work["scheduleEnabled"], false);
    assert_eq!(work["scheduleDaysEnabled"], json!([]));
    assert!(work.get("emoji").is_none());
    assert!(work.get("scheduleStartTime").is_none());
    assert!(work.get("scheduleEndTime").is_none());
    assert!(work.get("deletedAtTimestampMs").is_none());
    let sleep = &profiles[1];
    assert_eq!(sleep["id"], "b7e2d3c4f5a6081922b3c4d5e6f70812");
    assert_eq!(sleep["emoji"], "🌙");
    assert_eq!(sleep["scheduleEnabled"], true);
    assert_eq!(sleep["scheduleStartTime"], 1320);
    assert_eq!(sleep["scheduleEndTime"], 420);
    assert_eq!(sleep["scheduleDaysEnabled"], json!([1, 2, 3, 4, 5]));
    assert_eq!(sleep["allowedMembers"][0]["kind"], "contact");
    assert_eq!(
        sleep["allowedMembers"][0]["conversationId"],
        "6e08f0b6-1c2d-4e5f-8a9b-0c1d2e3f4a5b"
    );

    // The records half: the entry crosses verbatim (the caller's keys, no
    // connector key added or stripped) and the read-back answers the
    // post-write cloud state, not a local echo.
    let record = json!({
        "id": "c9d3e4f5a6b7182933c4d5e6f7081923",
        "name": "Focus",
        "emoji": "🎯",
        "color": 2,
        "createdAtMs": 1720000200000_u64,
        "allowAllCalls": false,
        "allowAllMentions": false,
        "allowedMembers": [],
        "scheduleEnabled": true,
        "scheduleDaysEnabled": [6, 7],
        "deletedAtTimestampMs": 0
    });
    let written = supervisor
        .notification_profiles_set_records(NotificationProfilesSetRecordsParams {
            account_id: account_id.clone(),
            records: Some(vec![record.clone()]),
            remove_ids: None,
        })
        .await
        .unwrap();
    assert_eq!(written.profiles.len(), 3);
    assert_eq!(written.profiles[2]["name"], "Focus");
    assert_eq!(written.profiles[2]["scheduleDaysEnabled"], json!([6, 7]));

    // The exact upstream frame on the records-only write: the resolved
    // signal account (never the host's opaque id), the record verbatim, and
    // no removeIds key at all (the optional half stays absent).
    let log = log_lines(&data_dir);
    let write_calls: Vec<&Value> = log
        .iter()
        .filter(|params| params.get("records").is_some())
        .collect();
    assert_eq!(write_calls.len(), 1, "{write_calls:?}");
    assert_eq!(write_calls[0]["account"], SIGNAL_ACCOUNT);
    assert_eq!(write_calls[0]["records"], json!([record]));
    assert!(write_calls[0].get("removeIds").is_none());

    // The removeIds half: the official deleteKey face, symmetric framing.
    let removed = supervisor
        .notification_profiles_set_records(NotificationProfilesSetRecordsParams {
            account_id: account_id.clone(),
            records: None,
            remove_ids: Some(vec!["a3f1c2d4e5b6478901a2b3c4d5e6f701".to_string()]),
        })
        .await
        .unwrap();
    assert_eq!(removed.profiles.len(), 2);
    assert_eq!(
        removed.profiles[0]["id"],
        "b7e2d3c4f5a6081922b3c4d5e6f70812"
    );
    let log = log_lines(&data_dir);
    let remove_calls: Vec<&Value> = log
        .iter()
        .filter(|params| params.get("removeIds").is_some())
        .collect();
    assert_eq!(remove_calls.len(), 1, "{remove_calls:?}");
    assert_eq!(remove_calls[0]["account"], SIGNAL_ACCOUNT);
    assert_eq!(
        remove_calls[0]["removeIds"],
        json!(["a3f1c2d4e5b6478901a2b3c4d5e6f701"])
    );
    assert!(remove_calls[0].get("records").is_none());
}

/// The structured storage ladder rides the wire verbatim (retryable=true —
/// the engine owns the family's final set, the connector neither swallows nor
/// invents codes): STORAGE_WRITE_FAILED on the write, STORAGE_READ_FAILED on
/// the read, STORAGE_UNAVAILABLE on both.
#[tokio::test]
async fn notification_profile_error_ladder_rides_the_wire() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("signal-data");
    let (_store, supervisor, account_id) = profile_supervisor(&temp, &data_dir);
    supervisor.start().await.unwrap();
    let marker = |name: &str| data_dir.join(name);

    fs::write(marker(".fixture-profile-write-failed"), "1").unwrap();
    let error = supervisor
        .notification_profiles_set_records(NotificationProfilesSetRecordsParams {
            account_id: account_id.clone(),
            records: Some(vec![json!({
                "id": "a3f1c2d4e5b6478901a2b3c4d5e6f701",
                "name": "tombstone",
                "color": 0,
                "createdAtMs": 0,
                "allowAllCalls": false,
                "allowAllMentions": false,
                "allowedMembers": [],
                "scheduleEnabled": false,
                "scheduleDaysEnabled": [],
                "deletedAtTimestampMs": 1720000300000_u64
            })]),
            remove_ids: None,
        })
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "STORAGE_WRITE_FAILED");
    assert!(error.retryable);
    fs::remove_file(marker(".fixture-profile-write-failed")).unwrap();

    fs::write(marker(".fixture-profile-read-failed"), "1").unwrap();
    let error = supervisor
        .notification_profiles_list(NotificationProfilesListParams {
            account_id: account_id.clone(),
        })
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "STORAGE_READ_FAILED");
    assert!(error.retryable);

    fs::remove_file(marker(".fixture-profile-read-failed")).unwrap();
    fs::write(marker(".fixture-profile-unavailable"), "1").unwrap();
    let error = supervisor
        .notification_profiles_list(NotificationProfilesListParams {
            account_id: account_id.clone(),
        })
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "STORAGE_UNAVAILABLE");
    assert!(error.retryable);
    let error = supervisor
        .notification_profiles_set_records(NotificationProfilesSetRecordsParams {
            account_id: account_id.clone(),
            records: None,
            remove_ids: Some(vec!["a3f1c2d4e5b6478901a2b3c4d5e6f701".to_string()]),
        })
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "STORAGE_UNAVAILABLE");
    assert!(error.retryable);
    fs::remove_file(marker(".fixture-profile-unavailable")).unwrap();

    // An engine-side shape refusal (a record the engine's grammar rejects)
    // is not a connector voice: it degrades to the plain upstream error (the
    // generic fold, retryable=true — only the allowlisted structured codes
    // carry the family's own retryability).
    let error = supervisor
        .notification_profiles_set_records(NotificationProfilesSetRecordsParams {
            account_id: account_id.clone(),
            records: Some(vec![json!({
                "id": "a3f1c2d4e5b6478901a2b3c4d5e6f701",
                "name": ""
            })]),
            remove_ids: None,
        })
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "UPSTREAM_ERROR");
    assert!(error.retryable);
}

/// The local gates answer INVALID_REQUEST before any upstream call: the
/// at-least-one rule, the batch caps, the records identity face (object with
/// a non-empty bounded string id), and the removeId bounds. The gate ladder
/// holds: shape wins over resolution, resolution wins over everything after.
#[tokio::test]
async fn notification_profile_gates_answer_locally() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("signal-data");
    let (store, supervisor, account_id) = profile_supervisor(&temp, &data_dir);
    supervisor.start().await.unwrap();
    let id = "a3f1c2d4e5b6478901a2b3c4d5e6f701";

    // The at-least-one rule (the accounts.updateConfiguration discipline).
    let error = supervisor
        .notification_profiles_set_records(NotificationProfilesSetRecordsParams {
            account_id: account_id.clone(),
            records: Some(Vec::new()),
            remove_ids: Some(Vec::new()),
        })
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "INVALID_REQUEST");

    // The records identity face.
    for bad in [
        json!("not-an-object"),
        json!({"name": "no id"}),
        json!({"id": ""}),
        json!({"id": 7}),
    ] {
        let error = supervisor
            .notification_profiles_set_records(NotificationProfilesSetRecordsParams {
                account_id: account_id.clone(),
                records: Some(vec![bad.clone()]),
                remove_ids: None,
            })
            .await
            .err()
            .unwrap()
            .into_api();
        assert_eq!(error.code, "INVALID_REQUEST", "{bad}");
        assert!(!error.retryable, "{bad}");
    }

    // The id bound (129 characters).
    let overlong: String = "a".repeat(129);
    let error = supervisor
        .notification_profiles_set_records(NotificationProfilesSetRecordsParams {
            account_id: account_id.clone(),
            records: Some(vec![json!({"id": overlong})]),
            remove_ids: None,
        })
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "INVALID_REQUEST");
    let error = supervisor
        .notification_profiles_set_records(NotificationProfilesSetRecordsParams {
            account_id: account_id.clone(),
            records: None,
            remove_ids: Some(vec!["a".repeat(129)]),
        })
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "INVALID_REQUEST");

    // The batch caps: 65 entries on either half.
    let big_batch: Vec<Value> = (0..65)
        .map(|index| json!({"id": format!("{index:032x}")}))
        .collect();
    let error = supervisor
        .notification_profiles_set_records(NotificationProfilesSetRecordsParams {
            account_id: account_id.clone(),
            records: Some(big_batch),
            remove_ids: None,
        })
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "INVALID_REQUEST");
    let big_remove: Vec<String> = (0..65).map(|index| format!("{index:032x}")).collect();
    let error = supervisor
        .notification_profiles_set_records(NotificationProfilesSetRecordsParams {
            account_id: account_id.clone(),
            records: None,
            remove_ids: Some(big_remove),
        })
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "INVALID_REQUEST");

    // The ladder: shape wins over resolution (a bad record with an unknown
    // account still answers the shape), resolution wins over everything the
    // seam would say (an unknown account with a clean frame answers
    // ACCOUNT_NOT_FOUND).
    let error = supervisor
        .notification_profiles_set_records(NotificationProfilesSetRecordsParams {
            account_id: "kt-acct-missing".to_string(),
            records: Some(vec![json!({"name": "no id"})]),
            remove_ids: None,
        })
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "INVALID_REQUEST");
    let error = supervisor
        .notification_profiles_list(NotificationProfilesListParams {
            account_id: "kt-acct-missing".to_string(),
        })
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "ACCOUNT_NOT_FOUND");
    let error = supervisor
        .notification_profiles_set_records(NotificationProfilesSetRecordsParams {
            account_id: "kt-acct-missing".to_string(),
            records: None,
            remove_ids: Some(vec![id.to_string()]),
        })
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "ACCOUNT_NOT_FOUND");

    // An account the engine does not know: the engine's -32603 degrades to
    // the plain upstream error, never a connector code. The account is
    // registered in the connector's store (resolvable), but absent from the
    // engine's own registry.
    let ghost = store
        .upsert_account_from_signal(
            "+15565550002",
            Some(1),
            kt_signal_connector::DEFAULT_PROXY_GROUP_ID,
        )
        .unwrap();
    let error = supervisor
        .notification_profiles_list(NotificationProfilesListParams {
            account_id: ghost.id,
        })
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "UPSTREAM_ERROR");
}

/// The mutating-crash sentinel: the engine dying with the write in flight
/// answers SEND_OUTCOME_UNKNOWN (retryable=false) and nothing retries —
/// exactly one such call in the fixture log.
#[tokio::test]
async fn notification_profile_crash_answers_unknown_and_never_retries() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("signal-data");
    let (_store, supervisor, account_id) = profile_supervisor(&temp, &data_dir);
    supervisor.start().await.unwrap();

    let error = supervisor
        .notification_profiles_set_records(NotificationProfilesSetRecordsParams {
            account_id: account_id.clone(),
            records: Some(vec![json!({
                "id": "cede000000000000400080000000000032",
                "name": "crash carrier",
                "color": 9,
                "createdAtMs": 1720000400000_u64,
                "allowAllCalls": false,
                "allowAllMentions": false,
                "allowedMembers": [],
                "scheduleEnabled": false,
                "scheduleDaysEnabled": []
            })]),
            remove_ids: None,
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
            params
                .get("records")
                .and_then(Value::as_array)
                .is_some_and(|records| {
                    records
                        .iter()
                        .any(|record| record["id"] == "cede000000000000400080000000000032")
                })
        })
        .collect();
    assert_eq!(
        crash_calls.len(),
        1,
        "an unknown write outcome is never retried: {crash_calls:?}"
    );
}
