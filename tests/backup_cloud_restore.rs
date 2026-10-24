// SPDX-License-Identifier: AGPL-3.0-only

#![cfg(unix)]

//! Cloud-backup restore end to end (contract revisions 1.83 §4.69 + 1.84
//! consumption): `backups.restoreFromCloud` forwards the start to the
//! engine's background restore chain, `backups.cloudStatus` mirrors the
//! engine's progress snapshot honestly, and the consumption watcher follows
//! a started restore — a fresh `history-import-cloud.ndjson` is driven
//! through the §4.38 import pass onto the `history.importStatus` ledger,
//! while a failed phase lands an honest engine-class row and a stale
//! archive from a previous restore is never imported. The fixture plays the
//! engine and mirrors the negotiated wire face — marker files steer the
//! seven-family structured error ladder and the three status states, the
//! happy path runs the background chain (NDJSON write + success-clear,
//! stall, or failed-phase flip), so these tests pin the connector's
//! routing, consumption, and error fidelity, never real engine behavior
//! (the real-device restore chain is the engine batch's own acceptance,
//! not this batch's).

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

/// All supervisor-test stores are encrypted with one fixed key, so a seeded
/// store and the supervisor's own open see the same database.
fn test_store(dir: &Path) -> Arc<Store> {
    Arc::new(Store::open(dir, Some(StoreKey::from_bytes([0x5A; 32]))).unwrap())
}

const SIGNAL_ACCOUNT: &str = "+15555550100";

/// The engine face the tests steer: a v2 registry naming one account
/// directory (`accounts/lk-abc`) — the produced cloud NDJSON lands there.
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

/// The cloud NDJSON the watcher must never import twice: the fixture's
/// fresh-write shape, planted by a test as the stale archive of a previous
/// restore.
fn write_stale_cloud_archive(data_dir: &Path) {
    let archive = data_dir
        .join("accounts")
        .join("lk-abc")
        .join("history-import-cloud.ndjson");
    std::fs::write(
        archive,
        concat!(
            r#"{"type":"recipient","id":2,"kind":"contact","name":"Cloud Import","aci":"6e08f0b6-1c2d-4e5f-8a9b-0c1d2e3f4a5b","e164":"+15550000001"}"#,
            "\n",
            r#"{"type":"chat","id":20,"recipientId":2}"#,
            "\n",
            r#"{"type":"message","chatId":20,"authorId":2,"dateSent":5000,"direction":"incoming","text":"cloud archived hello"}"#,
            "\n",
            r#"{"type":"message","chatId":20,"authorId":2,"dateSent":6000,"direction":"outgoing","text":"cloud archived reply"}"#,
            "\n",
        ),
    )
    .unwrap();
}

fn cloud_restore_config(fixture: &Path, data_dir: &Path) -> EngineConfig {
    let mut config = EngineConfig::new(fixture.to_path_buf(), data_dir.to_path_buf());
    config.request_timeout = Duration::from_secs(5);
    config.shutdown_grace = Duration::from_millis(100);
    config
}

fn cloud_restore_supervisor(
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
        cloud_restore_config(&fixture(), data_dir),
        Arc::clone(&store),
        kt_signal_connector::DEFAULT_PROXY_GROUP_ID.to_string(),
        None,
    ));
    (store, supervisor, account.id)
}

/// The status snapshot mirror: absent → {present: false}; a downloading
/// snapshot carries the byte counts with an absent `bytesTotal` staying an
/// absent key (the honest-uncertainty form, never a zero); the failed phase
/// carries the structured errorCode; clearing the marker restores absence.
#[tokio::test]
async fn cloud_status_mirrors_the_three_negotiated_states() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("signal-data");
    let (_store, supervisor, account_id) = cloud_restore_supervisor(&temp, &data_dir);
    supervisor.start().await.unwrap();
    let status_marker = data_dir.join(".fixture-cloud-status");

    let absent = supervisor.cloud_backup_status(&account_id).await.unwrap();
    assert_eq!(absent, json!({ "present": false }));

    std::fs::write(&status_marker, "downloading 40 100").unwrap();
    let downloading = supervisor.cloud_backup_status(&account_id).await.unwrap();
    assert_eq!(
        downloading,
        json!({
            "present": true,
            "phase": "downloading",
            "bytesRead": 40,
            "bytesTotal": 100
        })
    );

    std::fs::write(&status_marker, "downloading 40").unwrap();
    let totalless = supervisor.cloud_backup_status(&account_id).await.unwrap();
    assert_eq!(
        totalless,
        json!({ "present": true, "phase": "downloading", "bytesRead": 40 })
    );
    assert!(
        totalless.get("bytesTotal").is_none(),
        "a missing total stays a missing key, never a zero: {totalless}"
    );

    std::fs::write(&status_marker, "failed 40 BACKUP_DOWNLOAD_FAILED").unwrap();
    let failed = supervisor.cloud_backup_status(&account_id).await.unwrap();
    assert_eq!(
        failed,
        json!({
            "present": true,
            "phase": "failed",
            "bytesRead": 40,
            "errorCode": "BACKUP_DOWNLOAD_FAILED"
        })
    );

    std::fs::remove_file(&status_marker).unwrap();
    let cleared = supervisor.cloud_backup_status(&account_id).await.unwrap();
    assert_eq!(cleared, json!({ "present": false }));
}

/// The start face: a definite acceptance answers {"status": "started"}, and
/// the seven structured engine families ride the wire verbatim with the
/// contract-fixed retryability — the three state families content-free, the
/// four message-carrying families with the engine's data.detail passed
/// through (the connector never swallows a code nor invents one).
#[tokio::test]
async fn cloud_restore_start_and_seven_family_error_ladder() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("signal-data");
    let (_store, supervisor, account_id) = cloud_restore_supervisor(&temp, &data_dir);
    supervisor.start().await.unwrap();
    let error_marker = data_dir.join(".fixture-cloud-restore-error");
    let running_marker = data_dir.join(".fixture-cloud-restore-running");

    let started = supervisor.restore_from_cloud(&account_id).await.unwrap();
    assert_eq!(started, json!({ "status": "started" }));

    for (sentinel, code, retryable, detail) in [
        ("NOT_PROVISIONED", "BACKUP_NOT_PROVISIONED", false, None),
        ("AUTH", "BACKUP_AUTH_FAILED", false, None),
        (
            "TRANSPORT",
            "BACKUP_TRANSPORT_FAILED",
            true,
            Some("fixture BACKUP_TRANSPORT_FAILED"),
        ),
        (
            "DOWNLOAD",
            "BACKUP_DOWNLOAD_FAILED",
            true,
            Some("fixture BACKUP_DOWNLOAD_FAILED"),
        ),
        (
            "DECRYPT",
            "BACKUP_DECRYPT_FAILED",
            false,
            Some("fixture BACKUP_DECRYPT_FAILED"),
        ),
        (
            "EXPORT",
            "BACKUP_EXPORT_FAILED",
            false,
            Some("fixture BACKUP_EXPORT_FAILED"),
        ),
    ] {
        std::fs::write(&error_marker, sentinel).unwrap();
        let error = supervisor
            .restore_from_cloud(&account_id)
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

    // The re-entry gate rides its own marker (the engine's atomic CAS gate is
    // engine-internal; the wire face is the code, content-free).
    std::fs::remove_file(&error_marker).unwrap();
    std::fs::write(&running_marker, "1").unwrap();
    let running = supervisor
        .restore_from_cloud(&account_id)
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(running.code, "BACKUP_RUNNING");
    assert!(!running.retryable);
    assert!(running.data.is_none(), "{running:?}");
}

/// The connector's own gates: an unknown account answers
/// ACCOUNT_NOT_FOUND before any engine traffic, and a non-opaque accountId
/// fails INVALID_REQUEST — validation precedes resolution, and no engine
/// starts.
#[tokio::test]
async fn cloud_restore_gates_account_and_params() {
    let engine = TempDir::new().unwrap();
    let (_store, engine_supervisor, _account_id) =
        cloud_restore_supervisor(&engine, &engine.path().join("signal-data"));

    // An unknown account answers the account face before any engine call.
    let error = engine_supervisor
        .restore_from_cloud("kt-acct-missing")
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "ACCOUNT_NOT_FOUND");

    // The params face: a non-opaque accountId fails INVALID_REQUEST through
    // the same supervisor ladder (validation precedes resolution).
    let error = engine_supervisor
        .restore_from_cloud("")
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "INVALID_REQUEST");
}

/// Poll the ledger row until it lands on `state` (the watcher polls at its
/// own ~3 s cadence, then the import pass runs on the blocking pool — the
/// ceiling covers both hops several times over).
async fn wait_for_ledger_state(
    store: &Store,
    account_id: &str,
    state: &str,
) -> kt_signal_connector::store::HistoryImportRow {
    for _ in 0..80 {
        if let Ok(Some(row)) = store.history_import_row(account_id)
            && row.state == state
        {
            return row;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!(
        "import ledger never reached {state}: {:?}",
        store.history_import_row(account_id)
    );
}

/// The consumption chain (contract revision 1.84): a started restore's
/// fresh `history-import-cloud.ndjson` is driven through the §4.38 import
/// pass — real rows land (imported=2; the transfer artifact never existed
/// in this test, so the counters prove the cloud file was the source), the
/// `history.importStatus` face renders the completion, and the engine's
/// snapshot rides cleared (`present: false`, the success-clear discipline)
/// while the ledger carries the landed rows — the two faces compose
/// honestly.
#[tokio::test]
async fn cloud_restore_happy_chain_consumes_the_ndjson() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("signal-data");
    let (store, supervisor, account_id) = cloud_restore_supervisor(&temp, &data_dir);
    supervisor.start().await.unwrap();

    let started = supervisor.restore_from_cloud(&account_id).await.unwrap();
    assert_eq!(started, json!({ "status": "started" }));

    let row = wait_for_ledger_state(&store, &account_id, "completed").await;
    assert_eq!(row.imported_messages, 2);
    assert_eq!(row.skipped_messages, 0);

    let status = supervisor
        .history_import_status(account_id.clone())
        .await
        .unwrap();
    assert_eq!(status["state"], "completed");
    assert_eq!(status["importedMessages"], 2);

    let cloud = supervisor.cloud_backup_status(&account_id).await.unwrap();
    assert_eq!(cloud, json!({ "present": false }));
    assert!(
        !data_dir
            .join("accounts/lk-abc/history-import.ndjson")
            .exists()
    );
}

/// A failed phase is the engine's terminal snapshot: the watcher lands the
/// honest failed ledger row with the engine class, `history.importStatus`
/// renders it, and `history.importRetry` answers NO_ARCHIVE — retry is
/// re-invoking backups.restoreFromCloud (the §4.68 official-Retry
/// semantics). The artifacts never landed, so nothing claims landed rows.
#[tokio::test]
async fn cloud_restore_failed_phase_lands_honest_row() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("signal-data");
    let (store, supervisor, account_id) = cloud_restore_supervisor(&temp, &data_dir);
    supervisor.start().await.unwrap();
    std::fs::write(
        data_dir.join(".fixture-cloud-status"),
        "downloading 100 500",
    )
    .unwrap();
    std::fs::write(data_dir.join(".fixture-cloud-restore-fails-later"), "1").unwrap();

    let started = supervisor.restore_from_cloud(&account_id).await.unwrap();
    assert_eq!(started, json!({ "status": "started" }));

    let row = wait_for_ledger_state(&store, &account_id, "failed").await;
    assert_eq!(row.error_class.as_deref(), Some("engine"));
    assert_eq!(row.imported_messages, 0);

    let status = supervisor
        .history_import_status(account_id.clone())
        .await
        .unwrap();
    assert_eq!(status["state"], "failed");

    let retry = supervisor
        .history_import_retry(account_id.clone())
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(retry.code, "NO_ARCHIVE");
    assert!(!retry.retryable);
}

/// The stale-archive guard: a second restore over an existing (previous
/// restore's) archive must not import it while the freshness face is
/// unchanged — the watcher ticks over a stalled downloading phase and the
/// ledger stays untouched. Only a face that changes during the watch is the
/// engine's fresh write.
#[tokio::test]
async fn cloud_restore_stale_archive_never_consumed() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("signal-data");
    let (store, supervisor, account_id) = cloud_restore_supervisor(&temp, &data_dir);
    supervisor.start().await.unwrap();
    write_stale_cloud_archive(&data_dir);
    std::fs::write(data_dir.join(".fixture-cloud-restore-stall"), "1").unwrap();
    std::fs::write(data_dir.join(".fixture-cloud-status"), "downloading 10").unwrap();

    let started = supervisor.restore_from_cloud(&account_id).await.unwrap();
    assert_eq!(started, json!({ "status": "started" }));

    // Two watcher ticks pass over the stalled restore.
    tokio::time::sleep(Duration::from_millis(7500)).await;
    assert!(
        store.history_import_row(&account_id).unwrap().is_none(),
        "a stale archive must never be imported"
    );
}

/// The freshness face cuts both ways: a re-restore over a consumed archive
/// rewrites the artifact (a fresh mtime), the new watcher's baseline sees
/// the change, and the import runs again — landing on the same row
/// identities, so the second run completes with deduped counters (0
/// imported, the §4.38 idempotency discipline).
#[tokio::test]
async fn cloud_restore_rerun_consumes_the_fresh_archive() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("signal-data");
    let (store, supervisor, account_id) = cloud_restore_supervisor(&temp, &data_dir);
    supervisor.start().await.unwrap();

    let started = supervisor.restore_from_cloud(&account_id).await.unwrap();
    assert_eq!(started, json!({ "status": "started" }));
    let first = wait_for_ledger_state(&store, &account_id, "completed").await;
    assert_eq!(first.imported_messages, 2);

    // The second restore rewrites the same frames (a fresh face) — wait for
    // the ledger to complete AGAIN with the deduped counters.
    let started = supervisor.restore_from_cloud(&account_id).await.unwrap();
    assert_eq!(started, json!({ "status": "started" }));
    let mut second = None;
    for _ in 0..80 {
        if let Ok(Some(row)) = store.history_import_row(&account_id)
            && row.state == "completed"
            && row.imported_messages == 0
            && row.updated_at > first.updated_at
        {
            second = Some(row);
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let row = second.expect("the re-run must complete with deduped counters");
    // The §4.38 counter semantics: duplicates are the identity discipline
    // working and surface in the honest `skippedMessages` counter (the
    // importer's own unit pin) — the re-run lands nothing new.
    assert_eq!(row.skipped_messages, 2);
}
