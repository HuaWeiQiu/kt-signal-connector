// SPDX-License-Identifier: AGPL-3.0-only

#![cfg(unix)]

//! Cloud-backup upload end to end (contract revision 1.87, §4.72; engine
//! cloud-backup-contract §6): `backups.uploadToCloud` (re)builds the §10.2
//! transitional artifact with the §4.46 machine and forwards the start to
//! the engine's background upload chain, and `backups.uploadStatus` mirrors
//! the engine's process-wide single-slot progress snapshot honestly. The
//! fixture plays the engine and mirrors the negotiated wire face — marker
//! files steer the eight-family structured error ladder and the status
//! states, and the start arm mirrors the engine's artifact gate (no §10.2
//! artifact in the account directory answers BACKUP_ARTIFACT_MISSING), so
//! these tests pin the connector's routing, artifact-build wiring, and
//! error fidelity, never real engine upload behavior (the real-device
//! upload chain is the engine batch's own acceptance, not this batch's).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use kt_signal_connector::engine::EngineConfig;
use kt_signal_connector::ids::stable_hash_id;
use kt_signal_connector::store::{MessageRecord, Store, StoreKey};
use kt_signal_connector::supervisor::RuntimeSupervisor;
use serde_json::json;
use tempfile::TempDir;

fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fake-engine.py")
}

/// All supervisor-test stores are encrypted with one fixed key, so a seeded
/// store and the supervisor's own open see the same database.
fn test_store(dir: &Path) -> Arc<Store> {
    Arc::new(Store::open(dir, Some(StoreKey::from_bytes([0x5A; 32]))).unwrap())
}

const SIGNAL_ACCOUNT: &str = "+15555550100";
const ALICE_E164: &str = "+15550000001";

/// The engine face the tests steer: a v2 registry naming one account
/// directory (`accounts/lk-abc`) — the built §10.2 artifact lands there.
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

fn cloud_upload_config(fixture: &Path, data_dir: &Path) -> EngineConfig {
    let mut config = EngineConfig::new(fixture.to_path_buf(), data_dir.to_path_buf());
    config.request_timeout = Duration::from_secs(5);
    config.shutdown_grace = Duration::from_millis(100);
    config
}

fn cloud_upload_supervisor(
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
    let supervisor = Arc::new(RuntimeSupervisor::new(
        cloud_upload_config(&fixture(), data_dir),
        Arc::clone(&store),
        kt_signal_connector::DEFAULT_PROXY_GROUP_ID.to_string(),
        None,
    ));
    (store, supervisor, account.id)
}

/// Seed one direct conversation with Alice and one exchange, so the artifact
/// build projects real rows (the empty-store shape is the §4.46 batch's own
/// pin; here the artifact only needs to be a real build).
fn seed_direct_history(store: &Store, account_id: &str) {
    store
        .upsert_contact(account_id, "contact", ALICE_E164, "Alice Contact", None, 1)
        .unwrap();
    let conversation = store
        .ensure_conversation(account_id, "direct", ALICE_E164, "Alice Contact")
        .unwrap();
    let incoming = MessageRecord {
        author_aci: None,
        id: "m-in".into(),
        account_id: account_id.into(),
        conversation_id: conversation.id.clone(),
        direction: "incoming",
        sender_id: stable_hash_id(&[account_id, "direct", ALICE_E164]),
        sender_name: None,
        mentions_self: false,
        server_guid: None,
        server_timestamp: None,
        is_story: false,
        story_message: None,
        sent_at: 1_000,
        received_at: Some(1_005),
        text: Some("hello from alice".into()),
        text_bytes: Some(16),
        text_truncated: false,
        text_retrievable: true,
        status: "read",
        client_request_id: None,
        quote_message_id: None,
        quote_snapshot: None,
        quote: None,
        attachments: Vec::new(),
        edited_at: None,
        rich: None,
        reactions: Vec::new(),
        edits: Vec::new(),
        delivered_at: None,
        read_at: None,
        viewed_at: None,
        send_outcomes: None,
        sticker: None,
        expire_seconds: None,
        expire_start_at: None,
        admin_deleted: false,
    };
    store.insert_message(&incoming, None, None, true).unwrap();
}

/// The §10.2 artifact path inside the seeded engine account directory.
fn artifact_path(data_dir: &Path) -> PathBuf {
    data_dir
        .join("accounts")
        .join("lk-abc")
        .join(kt_signal_connector::backup_export::ARTIFACT_FILE_NAME)
}

/// The start face's connector-owned half: a definite acceptance answers
/// {"status": "started"}, and the §10.2 artifact the engine's start gate
/// checks was (re)built by the connector ahead of the call — the fixture's
/// artifact gate would answer BACKUP_ARTIFACT_MISSING had the build not
/// landed. The artifact's first line is the §10.2 account line (the
/// engine's first-line validity gate), and a pre-existing stale artifact
/// is rebuilt rather than served (the §4.46 write-once posture).
#[tokio::test]
async fn cloud_upload_start_builds_artifact_and_accepts() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("signal-data");
    let (store, supervisor, account_id) = cloud_upload_supervisor(&temp, &data_dir);
    seed_direct_history(&store, &account_id);
    supervisor.start().await.unwrap();

    // A stale artifact from an earlier world is rebuilt, never served.
    let artifact = artifact_path(&data_dir);
    std::fs::write(&artifact, "stale garbage\n").unwrap();

    let started = supervisor.upload_to_cloud(&account_id).await.unwrap();
    assert_eq!(started, json!({ "status": "started" }));

    let raw = std::fs::read_to_string(&artifact).unwrap();
    assert_ne!(raw, "stale garbage\n", "the artifact was rebuilt");
    let first: serde_json::Value =
        serde_json::from_str(raw.lines().next().expect("a built artifact has lines")).unwrap();
    assert_eq!(
        first["type"], "account",
        "the first line is the §10.2 account line: {first}"
    );
}

/// The eight-family structured error ladder rides the wire verbatim with
/// the contract-fixed retryability — the five state families content-free,
/// the three message-carrying families (TRANSPORT/UPLOAD/IO, the engine's
/// `detail()` Some set) with the engine's data.detail passed through. The
/// per-face divergence holds across the wire: BACKUP_IO_FAILED answers
/// retryable=false on this face (its §4.46 export face stays
/// retryable=true), and BACKUP_TRANSPORT_FAILED carries the upload text.
#[tokio::test]
async fn cloud_upload_start_and_eight_family_error_ladder() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("signal-data");
    let (store, supervisor, account_id) = cloud_upload_supervisor(&temp, &data_dir);
    seed_direct_history(&store, &account_id);
    supervisor.start().await.unwrap();
    let error_marker = data_dir.join(".fixture-cloud-upload-error");
    let running_marker = data_dir.join(".fixture-cloud-upload-running");

    for (sentinel, code, retryable, detail) in [
        ("AUTH", "BACKUP_AUTH_FAILED", false, None),
        ("NOT_PROVISIONED", "BACKUP_NOT_PROVISIONED", false, None),
        ("ARTIFACT_MISSING", "BACKUP_ARTIFACT_MISSING", false, None),
        ("ARTIFACT_INVALID", "BACKUP_ARTIFACT_INVALID", false, None),
        (
            "TRANSPORT",
            "BACKUP_TRANSPORT_FAILED",
            true,
            Some("fixture BACKUP_TRANSPORT_FAILED"),
        ),
        (
            "UPLOAD",
            "BACKUP_UPLOAD_FAILED",
            true,
            Some("fixture BACKUP_UPLOAD_FAILED"),
        ),
        (
            "IO",
            "BACKUP_IO_FAILED",
            false,
            Some("fixture BACKUP_IO_FAILED"),
        ),
    ] {
        std::fs::write(&error_marker, sentinel).unwrap();
        let error = supervisor
            .upload_to_cloud(&account_id)
            .await
            .err()
            .unwrap_or_else(|| panic!("{sentinel} must answer a structured error"))
            .into_api();
        assert_eq!(error.code, code, "{sentinel}");
        assert_eq!(error.retryable, retryable, "{sentinel} retryable face");
        match detail {
            Some(detail) => assert_eq!(
                error.data,
                Some(json!({ "detail": detail })),
                "{sentinel} detail face"
            ),
            None => assert!(
                error.data.is_none(),
                "{sentinel} stays content-free: {error:?}"
            ),
        }
    }

    // The re-entry gate rides its own marker (the engine's process-wide
    // single-slot CAS is engine-internal; the wire face is the code,
    // content-free).
    std::fs::remove_file(&error_marker).unwrap();
    std::fs::write(&running_marker, "1").unwrap();
    let running = supervisor
        .upload_to_cloud(&account_id)
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(running.code, "BACKUP_UPLOAD_ALREADY_RUNNING");
    assert!(!running.retryable);
    assert!(running.data.is_none(), "{running:?}");
}

/// The status snapshot mirror: absent → {present: false}; the exporting
/// face carries the zero-count assembly shape (no total key); an uploading
/// snapshot carries the block counts with an absent `bytesTotal` staying an
/// absent key (the honest-uncertainty form, never a zero); the failed phase
/// carries the structured errorCode; clearing the marker restores absence.
#[tokio::test]
async fn cloud_upload_status_mirrors_the_negotiated_states() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("signal-data");
    let (_store, supervisor, account_id) = cloud_upload_supervisor(&temp, &data_dir);
    supervisor.start().await.unwrap();
    let status_marker = data_dir.join(".fixture-cloud-upload-status");

    let absent = supervisor.cloud_upload_status(&account_id).await.unwrap();
    assert_eq!(absent, json!({ "present": false }));

    std::fs::write(&status_marker, "exporting 0").unwrap();
    let exporting = supervisor.cloud_upload_status(&account_id).await.unwrap();
    assert_eq!(
        exporting,
        json!({ "present": true, "phase": "exporting", "bytesRead": 0 })
    );
    assert!(exporting.get("bytesTotal").is_none());

    std::fs::write(&status_marker, "uploading 131072 1048576").unwrap();
    let uploading = supervisor.cloud_upload_status(&account_id).await.unwrap();
    assert_eq!(
        uploading,
        json!({
            "present": true,
            "phase": "uploading",
            "bytesRead": 131072,
            "bytesTotal": 1048576
        })
    );

    std::fs::write(&status_marker, "uploading 131072").unwrap();
    let totalless = supervisor.cloud_upload_status(&account_id).await.unwrap();
    assert_eq!(
        totalless,
        json!({ "present": true, "phase": "uploading", "bytesRead": 131072 })
    );
    assert!(
        totalless.get("bytesTotal").is_none(),
        "a missing total stays a missing key, never a zero: {totalless}"
    );

    std::fs::write(&status_marker, "failed 131072 BACKUP_UPLOAD_FAILED").unwrap();
    let failed = supervisor.cloud_upload_status(&account_id).await.unwrap();
    assert_eq!(
        failed,
        json!({
            "present": true,
            "phase": "failed",
            "bytesRead": 131072,
            "errorCode": "BACKUP_UPLOAD_FAILED"
        })
    );

    std::fs::remove_file(&status_marker).unwrap();
    let cleared = supervisor.cloud_upload_status(&account_id).await.unwrap();
    assert_eq!(cleared, json!({ "present": false }));
}

/// The connector's own gates: an unknown account answers
/// ACCOUNT_NOT_FOUND before any engine traffic, and a non-opaque accountId
/// fails INVALID_REQUEST — validation precedes resolution, and no engine
/// starts.
#[tokio::test]
async fn cloud_upload_gates_account_and_params() {
    let engine = TempDir::new().unwrap();
    let (_store, engine_supervisor, _account_id) =
        cloud_upload_supervisor(&engine, &engine.path().join("signal-data"));

    // An unknown account answers the account face before any engine call.
    let error = engine_supervisor
        .upload_to_cloud("kt-acct-missing")
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "ACCOUNT_NOT_FOUND");

    // The params face: a non-opaque accountId fails INVALID_REQUEST through
    // the same supervisor ladder (validation precedes resolution).
    let error = engine_supervisor
        .upload_to_cloud("")
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "INVALID_REQUEST");
}
