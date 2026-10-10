// SPDX-License-Identifier: AGPL-3.0-only

#![cfg(unix)]

//! Chat-folder sync passthrough end to end (contract revision 1.95, §4.78):
//! the two `chatFolders.*` faces resolve the account, forward the official
//! frame verbatim, and pass the engine's ChatFolderRecordJson entries through
//! untouched — the connector neither interprets nor re-keys the record body
//! and persists nothing (engine compat §3/§5: the record content never lands
//! connector-side). The fixture plays the engine (the stub records pin the
//! wire shape, never real cloud state), the structured STORAGE ladder steers
//! from markers, the mutating-crash sentinel pins the unknown-outcome
//! discipline, and the mode seam answers before any engine starts.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use kt_signal_connector::engine::{SignalCliConfig, SignalCliMode};
use kt_signal_connector::service::{ChatFoldersListParams, ChatFoldersSetRecordsParams};
use kt_signal_connector::store::{Store, StoreKey};
use kt_signal_connector::supervisor::RuntimeSupervisor;
use serde_json::{Value, json};
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

fn chat_folder_supervisor(
    temp: &TempDir,
    data_dir: &Path,
    mode: SignalCliMode,
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
    config.mode = mode;
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
/// ChatFolderRecordJson entries verbatim (the whole 11-key face pinned key
/// for key, the three-kind recipient taxonomy included), and the write
/// forwards only the non-empty half — records verbatim on one call,
/// removeIds verbatim on the other — and answers the post-write cloud state.
#[tokio::test]
async fn chat_folder_faces_pass_through_verbatim() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("signal-data");
    let (_store, supervisor, account_id) =
        chat_folder_supervisor(&temp, &data_dir, SignalCliMode::KtEngine);
    supervisor.start().await.unwrap();

    let list = supervisor
        .chat_folders_list(ChatFoldersListParams {
            account_id: account_id.clone(),
        })
        .await
        .unwrap();
    let folders = &list.folders;
    assert_eq!(folders.len(), 2);
    let all_chats = &folders[0];
    assert_eq!(all_chats["id"], "0d6d50f5-7f9e-4a1b-9c2d-3e4f5a6b7c8d");
    assert_eq!(all_chats["name"], "All chats");
    assert_eq!(all_chats["position"], 0);
    assert_eq!(all_chats["showOnlyUnread"], false);
    assert_eq!(all_chats["showMutedChats"], true);
    assert_eq!(all_chats["includeAllIndividualChats"], true);
    assert_eq!(all_chats["includeAllGroupChats"], true);
    assert_eq!(all_chats["folderType"], "ALL");
    assert_eq!(all_chats["includedConversationIds"], json!([]));
    assert_eq!(all_chats["excludedConversationIds"], json!([]));
    assert_eq!(all_chats["deletedAtTimestampMs"], 0);
    let work = &folders[1];
    assert_eq!(work["folderType"], "CUSTOM");
    let included = work["includedConversationIds"].as_array().unwrap();
    assert_eq!(included.len(), 3);
    assert_eq!(included[0]["kind"], "contact");
    assert_eq!(
        included[0]["conversationId"],
        "6e08f0b6-1c2d-4e5f-8a9b-0c1d2e3f4a5b"
    );
    assert_eq!(included[1]["kind"], "group");
    assert_eq!(included[1]["conversationId"], "ZmFrZS1ncm91cC0x");
    assert_eq!(included[2]["kind"], "legacyGroup");
    assert_eq!(work["excludedConversationIds"][0]["kind"], "contact");
    assert_eq!(
        work["excludedConversationIds"][0]["conversationId"],
        "7f19e1c7-2d3e-4f6a-9b0c-1d2e3f4a5b6c"
    );

    // The records half: the entry crosses verbatim (the caller's keys, no
    // connector key added or stripped) and the read-back answers the
    // post-write cloud state, not a local echo.
    let record = json!({
        "id": "0d6d50f5-7f9e-4a1b-9c2d-3e4f5a6b7c8d",
        "name": "Everything",
        "position": 0,
        "showOnlyUnread": true,
        "showMutedChats": false,
        "includeAllIndividualChats": true,
        "includeAllGroupChats": false,
        "folderType": "ALL",
        "includedConversationIds": [],
        "excludedConversationIds": [],
        "deletedAtTimestampMs": 0
    });
    let written = supervisor
        .chat_folders_set_records(ChatFoldersSetRecordsParams {
            account_id: account_id.clone(),
            records: Some(vec![record.clone()]),
            remove_ids: None,
        })
        .await
        .unwrap();
    assert_eq!(written.folders.len(), 2);
    assert_eq!(written.folders[0]["name"], "Everything");
    assert_eq!(written.folders[0]["showOnlyUnread"], true);
    assert_eq!(written.folders[0]["includeAllGroupChats"], false);

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
        .chat_folders_set_records(ChatFoldersSetRecordsParams {
            account_id: account_id.clone(),
            records: None,
            remove_ids: Some(vec!["1a2b3c4d-5e6f-4a1b-8c9d-0e1f2a3b4c5d".to_string()]),
        })
        .await
        .unwrap();
    assert_eq!(removed.folders.len(), 1);
    assert_eq!(
        removed.folders[0]["id"],
        "0d6d50f5-7f9e-4a1b-9c2d-3e4f5a6b7c8d"
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
        json!(["1a2b3c4d-5e6f-4a1b-8c9d-0e1f2a3b4c5d"])
    );
    assert!(remove_calls[0].get("records").is_none());
}

/// The structured storage ladder rides the wire verbatim (retryable=true —
/// the engine owns the family's final set, the connector neither swallows nor
/// invents codes): STORAGE_WRITE_FAILED on the write, STORAGE_READ_FAILED on
/// the read, STORAGE_UNAVAILABLE on both.
#[tokio::test]
async fn chat_folder_error_ladder_rides_the_wire() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("signal-data");
    let (_store, supervisor, account_id) =
        chat_folder_supervisor(&temp, &data_dir, SignalCliMode::KtEngine);
    supervisor.start().await.unwrap();
    let marker = |name: &str| data_dir.join(name);

    fs::write(marker(".fixture-chat-folder-write-failed"), "1").unwrap();
    let error = supervisor
        .chat_folders_set_records(ChatFoldersSetRecordsParams {
            account_id: account_id.clone(),
            records: Some(vec![json!({"id": "0d6d50f5-7f9e-4a1b-9c2d-3e4f5a6b7c8d"})]),
            remove_ids: None,
        })
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "STORAGE_WRITE_FAILED");
    assert!(error.retryable);
    fs::remove_file(marker(".fixture-chat-folder-write-failed")).unwrap();

    fs::write(marker(".fixture-chat-folder-read-failed"), "1").unwrap();
    let error = supervisor
        .chat_folders_list(ChatFoldersListParams {
            account_id: account_id.clone(),
        })
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "STORAGE_READ_FAILED");
    assert!(error.retryable);

    fs::remove_file(marker(".fixture-chat-folder-read-failed")).unwrap();
    fs::write(marker(".fixture-chat-folder-unavailable"), "1").unwrap();
    let error = supervisor
        .chat_folders_list(ChatFoldersListParams {
            account_id: account_id.clone(),
        })
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "STORAGE_UNAVAILABLE");
    assert!(error.retryable);
    let error = supervisor
        .chat_folders_set_records(ChatFoldersSetRecordsParams {
            account_id: account_id.clone(),
            records: None,
            remove_ids: Some(vec!["0d6d50f5-7f9e-4a1b-9c2d-3e4f5a6b7c8d".to_string()]),
        })
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "STORAGE_UNAVAILABLE");
    assert!(error.retryable);
    fs::remove_file(marker(".fixture-chat-folder-unavailable")).unwrap();

    // An engine-side shape refusal (a record the engine's grammar rejects)
    // is not a connector voice: it degrades to the plain upstream error (the
    // generic fold, retryable=true — only the allowlisted structured codes
    // carry the family's own retryability).
    let error = supervisor
        .chat_folders_set_records(ChatFoldersSetRecordsParams {
            account_id: account_id.clone(),
            records: Some(vec![json!({"id": "0d6d50f5-7f9e-4a1b-9c2d-3e4f5a6b7c8d"})]),
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
/// holds: shape wins over resolution, resolution wins over the seam.
#[tokio::test]
async fn chat_folder_gates_answer_locally() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("signal-data");
    let (store, supervisor, account_id) =
        chat_folder_supervisor(&temp, &data_dir, SignalCliMode::KtEngine);
    supervisor.start().await.unwrap();
    let id = "0d6d50f5-7f9e-4a1b-9c2d-3e4f5a6b7c8d";

    // The at-least-one rule (the accounts.updateConfiguration discipline).
    let error = supervisor
        .chat_folders_set_records(ChatFoldersSetRecordsParams {
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
            .chat_folders_set_records(ChatFoldersSetRecordsParams {
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
        .chat_folders_set_records(ChatFoldersSetRecordsParams {
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
        .chat_folders_set_records(ChatFoldersSetRecordsParams {
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
        .chat_folders_set_records(ChatFoldersSetRecordsParams {
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
        .chat_folders_set_records(ChatFoldersSetRecordsParams {
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
        .chat_folders_set_records(ChatFoldersSetRecordsParams {
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
        .chat_folders_list(ChatFoldersListParams {
            account_id: "kt-acct-missing".to_string(),
        })
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "ACCOUNT_NOT_FOUND");
    let error = supervisor
        .chat_folders_set_records(ChatFoldersSetRecordsParams {
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
            "+15565550001",
            Some(1),
            kt_signal_connector::DEFAULT_PROXY_GROUP_ID,
        )
        .unwrap();
    let error = supervisor
        .chat_folders_list(ChatFoldersListParams {
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
async fn chat_folder_crash_answers_unknown_and_never_retries() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("signal-data");
    let (_store, supervisor, account_id) =
        chat_folder_supervisor(&temp, &data_dir, SignalCliMode::KtEngine);
    supervisor.start().await.unwrap();

    let error = supervisor
        .chat_folders_set_records(ChatFoldersSetRecordsParams {
            account_id: account_id.clone(),
            records: Some(vec![json!({
                "id": "cede0000-0000-4000-8000-000000000031",
                "name": "crash carrier",
                "position": 9,
                "showOnlyUnread": false,
                "showMutedChats": false,
                "includeAllIndividualChats": false,
                "includeAllGroupChats": false,
                "folderType": "CUSTOM"
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
                        .any(|record| record["id"] == "cede0000-0000-4000-8000-000000000031")
                })
        })
        .collect();
    assert_eq!(
        crash_calls.len(),
        1,
        "an unknown write outcome is never retried: {crash_calls:?}"
    );
}

/// The mode seam: JVM mode answers CAPABILITY_UNAVAILABLE on both faces —
/// before any engine starts (no engine traffic, the §4.73 ladder mirrored).
#[tokio::test]
async fn chat_folder_faces_gate_the_mode_seam() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("signal-data");
    let (_store, supervisor, account_id) =
        chat_folder_supervisor(&temp, &data_dir, SignalCliMode::Jvm);

    let list_error = supervisor
        .chat_folders_list(ChatFoldersListParams {
            account_id: account_id.clone(),
        })
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(list_error.code, "CAPABILITY_UNAVAILABLE");
    assert!(!list_error.retryable);

    let write_error = supervisor
        .chat_folders_set_records(ChatFoldersSetRecordsParams {
            account_id,
            records: Some(vec![json!({"id": "0d6d50f5-7f9e-4a1b-9c2d-3e4f5a6b7c8d"})]),
            remove_ids: None,
        })
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(write_error.code, "CAPABILITY_UNAVAILABLE");
    assert!(!write_error.retryable);

    // The seam never logged a frame: no engine process existed to receive
    // one.
    assert!(
        !data_dir.join(".fixture-send-log.jsonl").exists(),
        "the seam must answer before any upstream frame"
    );
}
