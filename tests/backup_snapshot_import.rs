// SPDX-License-Identifier: AGPL-3.0-only

#![cfg(unix)]

//! Official-desktop snapshot import end to end (contract revision 1.78,
//! §4.68): `backups.importSnapshot` forwards the caller-chosen snapshot
//! directory to the engine's import task, the consumption watcher polls the
//! engine status and drives the §4.38 import pass on a terminal state, and
//! `backups.importSnapshotStatus` mirrors the engine honestly with the raw
//! account echo stripped. The fixture plays the engine: the directory string's
//! sentinel substrings steer the structured-error ladder, the happy path
//! writes the §4.38 NDJSON archive in the background, and a fails-later
//! sentinel turns a started task into an engine-side failure.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use kt_signal_connector::engine::SignalCliConfig;
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

/// A snapshot directory that never exists but passes every shape gate —
/// the engine fixture steers on its sentinel substrings, never on the path.
const SNAPSHOT_DIR: &str = "/tmp/official-desktop/signal-backup-2026-10-08-10-00-00";

/// The engine face the tests steer: a v2 registry naming one account
/// directory (`accounts/lk-abc`) — the produced archive lands there.
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

fn snapshot_config(fixture: &Path, data_dir: &Path) -> SignalCliConfig {
    let mut config = SignalCliConfig::new(fixture.to_path_buf(), data_dir.to_path_buf());
    config.request_timeout = Duration::from_secs(5);
    config.shutdown_grace = Duration::from_millis(100);
    config
}

fn snapshot_supervisor(
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
        snapshot_config(&fixture(), data_dir),
        Arc::clone(&store),
        kt_signal_connector::DEFAULT_PROXY_GROUP_ID.to_string(),
        None,
    ));
    (store, supervisor, account.id)
}

/// Poll the ledger row until it lands on `state` (the watcher polls the
/// engine at its own ~3 s cadence, then the import pass runs on the blocking
/// pool — the ceiling covers both hops several times over).
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

/// The started → watcher → engine completed → §4.38 import chain: real
/// message rows land in the store under the dedupe keys, the ledger reports
/// the completed counters, and the status projection strips the raw account
/// echo in favor of `accountId`.
#[tokio::test]
async fn snapshot_import_happy_chain_lands_store_rows() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("signal-data");
    let (store, supervisor, account_id) = snapshot_supervisor(&temp, &data_dir);
    supervisor.start().await.unwrap();

    let started = supervisor
        .import_local_snapshot(&account_id, SNAPSHOT_DIR)
        .await
        .unwrap();
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

    // The engine's status projection rides verbatim minus the raw echo: the
    // projected rows/frames counts are the engine's own, `accountId` replaces
    // the upstream `account`, and the archive's disk truth shows present.
    let status = supervisor
        .import_snapshot_status(&account_id)
        .await
        .unwrap();
    assert_eq!(status["accountId"], account_id.as_str());
    assert!(status.get("account").is_none(), "raw echo must be stripped");
    assert_eq!(status["state"], "completed");
    assert_eq!(status["ndjsonPresent"], true);
    assert_eq!(status["lines"], 4);
    assert_eq!(status["frames"], 5);
}

/// The five engine structured codes ride the wire verbatim with the mapped
/// retryability: the deterministic refusals (absent snapshot, failed metadata
/// verification, foreign account, duplicate start) are terminal, the
/// filesystem failure may be retried.
#[tokio::test]
async fn snapshot_import_structured_codes_pass_through() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("signal-data");
    let (_store, supervisor, account_id) = snapshot_supervisor(&temp, &data_dir);
    supervisor.start().await.unwrap();

    for (sentinel, code, retryable) in [
        (
            "[fixture-snapshot-missing]",
            "BACKUP_SNAPSHOT_MISSING",
            false,
        ),
        (
            "[fixture-snapshot-metadata-invalid]",
            "BACKUP_SNAPSHOT_METADATA_INVALID",
            false,
        ),
        (
            "[fixture-snapshot-key-mismatch]",
            "BACKUP_SNAPSHOT_KEY_MISMATCH",
            false,
        ),
        ("[fixture-io-failed]", "BACKUP_SNAPSHOT_IO_FAILED", true),
        (
            "[fixture-import-running]",
            "BACKUP_IMPORT_ALREADY_RUNNING",
            false,
        ),
    ] {
        let dir = format!("{SNAPSHOT_DIR} {sentinel}");
        let error = supervisor
            .import_local_snapshot(&account_id, &dir)
            .await
            .err()
            .unwrap_or_else(|| panic!("{sentinel} must answer a structured error"))
            .into_api();
        assert_eq!(error.code, code, "{sentinel}");
        assert_eq!(error.retryable, retryable, "{sentinel} retryable face");
    }
}

/// The connector's own gates: shape failures (empty, relative, oversized
/// snapshotDir) fail closed before any engine traffic.
#[tokio::test]
async fn snapshot_import_param_gates() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("signal-data");
    let (_store, supervisor, account_id) = snapshot_supervisor(&temp, &data_dir);

    let dirs: [String; 3] = [
        String::new(),
        "relative/path".to_string(),
        format!("/{}", "x".repeat(4096)),
    ];
    for dir in dirs {
        let error = supervisor
            .import_local_snapshot(&account_id, &dir)
            .await
            .err()
            .unwrap()
            .into_api();
        assert_eq!(error.code, "INVALID_REQUEST", "dir {dir:?}");
        assert!(!error.retryable, "shape refusals are deterministic");
    }
}

/// The engine's failed status lands an honest failed ledger row with the
/// engine class; `history.importRetry` then answers NO_ARCHIVE — the archive
/// never landed, so retry means re-invoking backups.importSnapshot (the
/// official Retry semantics).
#[tokio::test]
async fn snapshot_import_engine_failure_lands_honest_row() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("signal-data");
    let (store, supervisor, account_id) = snapshot_supervisor(&temp, &data_dir);
    supervisor.start().await.unwrap();

    let started = supervisor
        .import_local_snapshot(
            &account_id,
            &format!("{SNAPSHOT_DIR} [fixture-import-fails-later]"),
        )
        .await
        .unwrap();
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
