// SPDX-License-Identifier: AGPL-3.0-only

#![cfg(unix)]

//! Cloud-backup restore passthrough end to end (contract revision 1.83,
//! §4.69): `backups.restoreFromCloud` forwards the start to the engine's
//! background restore chain, `backups.cloudStatus` mirrors the engine's
//! progress snapshot honestly. The fixture plays the engine and mirrors the
//! negotiated wire face only — marker files steer the seven-family structured
//! error ladder and the three status states, so these tests pin the
//! connector's routing and error fidelity, never real engine behavior (the
//! real-device restore chain is the engine batch's own acceptance, not this
//! batch's).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use kt_signal_connector::engine::{SignalCliConfig, SignalCliMode};
use kt_signal_connector::store::{Store, StoreKey};
use kt_signal_connector::supervisor::RuntimeSupervisor;
use serde_json::json;
use tempfile::TempDir;

fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fake-signal-cli.py")
}

/// All supervisor-test stores are encrypted with one fixed key, so a seeded
/// store and the supervisor's own open see the same database.
fn test_store(dir: &Path) -> Arc<Store> {
    Arc::new(Store::open(dir, Some(StoreKey::from_bytes([0x5A; 32]))).unwrap())
}

const SIGNAL_ACCOUNT: &str = "+15555550100";

fn cloud_restore_config(fixture: &Path, data_dir: &Path, mode: SignalCliMode) -> SignalCliConfig {
    let mut config = SignalCliConfig::new(fixture.to_path_buf(), data_dir.to_path_buf());
    config.mode = mode;
    config.request_timeout = Duration::from_secs(5);
    config.shutdown_grace = Duration::from_millis(100);
    config
}

fn cloud_restore_supervisor(
    temp: &TempDir,
    data_dir: &Path,
    mode: SignalCliMode,
) -> (Arc<Store>, Arc<RuntimeSupervisor>, String) {
    let store = test_store(temp.path());
    let account = store
        .upsert_account_from_signal(
            SIGNAL_ACCOUNT,
            Some(1),
            kt_signal_connector::DEFAULT_PROXY_GROUP_ID,
        )
        .unwrap();
    let supervisor = Arc::new(RuntimeSupervisor::new(
        cloud_restore_config(&fixture(), data_dir, mode),
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
    let (_store, supervisor, account_id) =
        cloud_restore_supervisor(&temp, &data_dir, SignalCliMode::KtEngine);
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
    let (_store, supervisor, account_id) =
        cloud_restore_supervisor(&temp, &data_dir, SignalCliMode::KtEngine);
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

/// The connector's own gates: the JVM/native mode seam answers
/// CAPABILITY_UNAVAILABLE on both faces (the history-import capability is
/// kt-engine only, the §4.68 ladder), an unknown account answers
/// ACCOUNT_NOT_FOUND before the seam, and a non-opaque accountId fails
/// INVALID_REQUEST — all before any engine traffic, so no engine starts.
#[tokio::test]
async fn cloud_restore_gates_mode_account_and_params() {
    let jvm = TempDir::new().unwrap();
    let (_store, jvm_supervisor, jvm_account_id) =
        cloud_restore_supervisor(&jvm, &jvm.path().join("signal-data"), SignalCliMode::Jvm);

    let error = jvm_supervisor
        .restore_from_cloud(&jvm_account_id)
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "CAPABILITY_UNAVAILABLE");
    assert!(!error.retryable);

    let error = jvm_supervisor
        .cloud_backup_status(&jvm_account_id)
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "CAPABILITY_UNAVAILABLE");
    assert!(!error.retryable);

    let engine = TempDir::new().unwrap();
    let (_store, engine_supervisor, _account_id) = cloud_restore_supervisor(
        &engine,
        &engine.path().join("signal-data"),
        SignalCliMode::KtEngine,
    );

    // The kt-engine face still resolves the account first: an unknown account
    // answers the account face, not the capability face.
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
