// SPDX-License-Identifier: AGPL-3.0-only

#![cfg(unix)]

//! Link-time history import end to end (contract revision 1.39, §4.38): a
//! kt-engine launch consumes the archive the engine's registry names and the
//! status face walks pending → completed; the signal-cli modes answer
//! `unavailable (engine-mode)` instead. The fixture plays the engine, so the
//! "archive" is the NDJSON the engine's S2 export projects.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use kt_signal_connector::engine::{SignalCliConfig, SignalCliMode};
use kt_signal_connector::ids::stable_hash_id;
use kt_signal_connector::store::{Store, StoreKey};
use kt_signal_connector::supervisor::RuntimeSupervisor;
use serde_json::Value;
use tempfile::TempDir;
use tokio::time::{sleep, timeout};

fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fake-signal-cli.py")
}

/// All supervisor-test stores are encrypted with one fixed key, so a seeded
/// store and the supervisor's own open see the same database.
fn test_store(dir: &std::path::Path) -> Arc<Store> {
    Arc::new(Store::open(dir, Some(StoreKey::from_bytes([0x5A; 32]))).unwrap())
}

const SIGNAL_ACCOUNT: &str = "+15555550100";
const ALICE_ACI: &str = "6e08f0b6-1c2d-4e5f-8a9b-0c1d2e3f4a5b";
const ALICE_E164: &str = "+15550000001";

/// Lay out the engine face the plan §5 registry documents: v2 registry plus
/// one account directory holding the NDJSON export.
fn seed_engine_archive(data_dir: &std::path::Path) {
    let account_dir = data_dir.join("accounts").join("lk-abc");
    std::fs::create_dir_all(&account_dir).unwrap();
    std::fs::write(
        data_dir.join("engine-state.json"),
        format!(
            r#"{{"version":2,"accounts":[{{"number":"{SIGNAL_ACCOUNT}","dir":"accounts/lk-abc"}}]}}"#
        ),
    )
    .unwrap();
    let recipient = format!(
        r#"{{"type":"recipient","id":1,"kind":"contact","name":"Alice Import","aci":"{ALICE_ACI}","e164":"{ALICE_E164}"}}"#
    );
    let chat = r#"{"type":"chat","id":10,"recipientId":1}"#;
    let incoming = r#"{"type":"message","chatId":10,"authorId":1,"dateSent":1000,"direction":"incoming","text":"archived hello"}"#.to_string();
    let outgoing = r#"{"type":"message","chatId":10,"authorId":1,"dateSent":2000,"direction":"outgoing","text":"archived reply","quote":{"targetSentTimestamp":1000,"authorId":1,"text":"archived hello"}}"#.to_string();
    std::fs::write(
        account_dir.join("history-import.ndjson"),
        format!("{recipient}\n{chat}\n{incoming}\n{outgoing}\n"),
    )
    .unwrap();
}

fn kt_engine_config(fixture: &std::path::Path, data_dir: &std::path::Path) -> SignalCliConfig {
    let mut config = SignalCliConfig::new(fixture.to_path_buf(), data_dir.to_path_buf());
    config.mode = SignalCliMode::KtEngine;
    config.request_timeout = Duration::from_secs(1);
    config.shutdown_grace = Duration::from_millis(100);
    config
}

async fn status(supervisor: &RuntimeSupervisor, account_id: &str) -> Value {
    supervisor
        .history_import_status(account_id.to_string())
        .await
        .unwrap()
}

/// The full arc: engine start sweep finds the archive, the import lands the
/// rows under the live-receive identity, and the status face reports the
/// honest completed counters. The import must reuse the conversation skeleton
/// peer identity — both messages land in one conversation.
#[tokio::test]
async fn kt_engine_start_imports_the_link_archive_and_reports_completion() {
    let temp = TempDir::new().unwrap();
    let signal_data = temp.path().join("signal-data");
    seed_engine_archive(&signal_data);
    let seeded = {
        let seed = test_store(temp.path());
        seed.upsert_account_from_signal(
            SIGNAL_ACCOUNT,
            Some(1),
            kt_signal_connector::DEFAULT_PROXY_GROUP_ID,
        )
        .unwrap()
    };
    let store = test_store(temp.path());
    let supervisor = Arc::new(RuntimeSupervisor::new(
        kt_engine_config(&fixture(), &signal_data),
        Arc::clone(&store),
        kt_signal_connector::DEFAULT_PROXY_GROUP_ID.to_string(),
        None,
    ));

    supervisor.start().await.unwrap();

    // Background pass: settle delay (5s) plus the blocking import.
    timeout(Duration::from_secs(20), async {
        loop {
            if status(&supervisor, &seeded.id).await["state"] == "completed" {
                break;
            }
            sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .expect("the startup sweep must import the link archive");

    let final_status = status(&supervisor, &seeded.id).await;
    assert_eq!(final_status["importedMessages"], 2);
    assert_eq!(final_status["skippedMessages"], 0);
    assert_eq!(final_status["skippedChats"], 0);
    assert_eq!(final_status["skippedLines"], 0);
    assert_eq!(final_status["attempts"], 1);

    // The rows sit in the store under the same identity live traffic uses:
    // one conversation keyed by the archive's e164, two messages, and the
    // quote resolved against the in-run incoming row.
    let conversation_id = stable_hash_id(&[&seeded.id, "direct", ALICE_E164]);
    let messages = store
        .list_messages(&seeded.id, &conversation_id, 10, None)
        .unwrap();
    assert_eq!(messages.items.len(), 2);
    let incoming = messages
        .items
        .iter()
        .find(|row| row.direction == "incoming")
        .expect("imported incoming row");
    assert_eq!(incoming.text.as_deref(), Some("archived hello"));
    let outgoing = messages
        .items
        .iter()
        .find(|row| row.direction == "outgoing")
        .expect("imported outgoing row");
    assert_eq!(
        outgoing.quote_message_id.as_deref(),
        Some(incoming.id.as_str())
    );

    supervisor.shutdown().await.unwrap();
}

/// Linked inside the wait window with no archive yet: the honest answer is
/// `pending` — the engine's best-effort download may still be in flight.
#[tokio::test]
async fn kt_engine_without_archive_reports_pending_inside_the_link_window() {
    let temp = TempDir::new().unwrap();
    let signal_data = temp.path().join("signal-data");
    let account_dir = signal_data.join("accounts").join("lk-abc");
    std::fs::create_dir_all(&account_dir).unwrap();
    std::fs::write(
        signal_data.join("engine-state.json"),
        format!(
            r#"{{"version":2,"accounts":[{{"number":"{SIGNAL_ACCOUNT}","dir":"accounts/lk-abc"}}]}}"#
        ),
    )
    .unwrap();
    let seeded = {
        let seed = test_store(temp.path());
        seed.upsert_account_from_signal(
            SIGNAL_ACCOUNT,
            Some(kt_signal_connector::link::now_ms()),
            kt_signal_connector::DEFAULT_PROXY_GROUP_ID,
        )
        .unwrap()
    };
    let store = test_store(temp.path());
    let supervisor = Arc::new(RuntimeSupervisor::new(
        kt_engine_config(&fixture(), &signal_data),
        Arc::clone(&store),
        kt_signal_connector::DEFAULT_PROXY_GROUP_ID.to_string(),
        None,
    ));

    let pending = status(&supervisor, &seeded.id).await;
    assert_eq!(pending["state"], "pending");
    assert!(pending.get("reason").is_none());
}

/// The signal-cli modes have no backup5 capability at all: the face answers
/// `unavailable (engine-mode)` — even when an archive from an earlier
/// kt-engine life of the data directory still sits on disk.
#[tokio::test]
async fn signal_cli_modes_report_unavailable_engine_mode() {
    let temp = TempDir::new().unwrap();
    let signal_data = temp.path().join("signal-data");
    seed_engine_archive(&signal_data);
    let seeded = {
        let seed = test_store(temp.path());
        seed.upsert_account_from_signal(
            SIGNAL_ACCOUNT,
            Some(1),
            kt_signal_connector::DEFAULT_PROXY_GROUP_ID,
        )
        .unwrap()
    };
    let store = test_store(temp.path());
    let mut config = SignalCliConfig::new(fixture(), signal_data);
    config.request_timeout = Duration::from_secs(1);
    config.shutdown_grace = Duration::from_millis(100);
    let supervisor = Arc::new(RuntimeSupervisor::new(
        config,
        Arc::clone(&store),
        kt_signal_connector::DEFAULT_PROXY_GROUP_ID.to_string(),
        None,
    ));

    let unavailable = status(&supervisor, &seeded.id).await;
    assert_eq!(unavailable["state"], "unavailable");
    assert_eq!(unavailable["reason"], "engine-mode");
}

/// Unknown accounts answer ACCOUNT_NOT_FOUND through the shared read path.
#[tokio::test]
async fn unknown_account_answers_account_not_found() {
    let temp = TempDir::new().unwrap();
    let store = test_store(temp.path());
    let supervisor = Arc::new(RuntimeSupervisor::new(
        kt_engine_config(&fixture(), &temp.path().join("signal-data")),
        Arc::clone(&store),
        kt_signal_connector::DEFAULT_PROXY_GROUP_ID.to_string(),
        None,
    ));
    let error = supervisor
        .history_import_status("a".repeat(64))
        .await
        .expect_err("unknown account must fail");
    assert!(matches!(
        error,
        kt_signal_connector::service::ServiceError::Store(
            kt_signal_connector::store::StoreError::AccountNotFound
        )
    ));
}

// ---------------------------------------------------------------------------
// Contract revision 1.53 (§4.51): the `running` progress face and the manual
// retry face. The fixture plays the engine for `transfer.downloadStatus`
// through marker files in the signal data dir.
// ---------------------------------------------------------------------------

/// Mark a fixture transfer download in flight ("<read> [<total>]" byte shape),
/// or clear the markers (argument `None`).
fn set_transfer_snapshot(signal_data: &std::path::Path, snapshot: Option<(u64, Option<u64>)>) {
    let running = signal_data.join(".fixture-transfer-running");
    let bytes = signal_data.join(".fixture-transfer-bytes");
    match snapshot {
        Some((read, total)) => {
            std::fs::write(&running, b"1").unwrap();
            match total {
                Some(total) => std::fs::write(&bytes, format!("{read} {total}")),
                None => std::fs::write(&bytes, format!("{read}")),
            }
            .unwrap();
        }
        None => {
            let _ = std::fs::remove_file(&running);
            let _ = std::fs::remove_file(&bytes);
        }
    }
}

/// Seed a completed import row (the 1.53 progress face must not fire on a
/// terminal state).
fn seed_completed_row(store: &Store, account_id: &str) {
    store
        .begin_history_import(account_id, kt_signal_connector::link::now_ms())
        .unwrap();
    store
        .complete_history_import(
            account_id,
            "completed",
            &kt_signal_connector::store::ImportCounters::default(),
            None,
            kt_signal_connector::link::now_ms(),
        )
        .unwrap();
}

/// `running` + engine download snapshot = stage downloading with the byte
/// brothers (u64 shape, transferred ≤ total).
#[tokio::test]
async fn running_with_engine_snapshot_reports_downloading_progress() {
    let temp = TempDir::new().unwrap();
    let signal_data = temp.path().join("signal-data");
    seed_engine_archive(&signal_data);
    let seeded = {
        let seed = test_store(temp.path());
        seed.upsert_account_from_signal(
            SIGNAL_ACCOUNT,
            Some(1),
            kt_signal_connector::DEFAULT_PROXY_GROUP_ID,
        )
        .unwrap()
    };
    let store = test_store(temp.path());
    store
        .begin_history_import(&seeded.id, kt_signal_connector::link::now_ms())
        .unwrap();
    set_transfer_snapshot(&signal_data, Some((300, Some(900))));
    let supervisor = Arc::new(RuntimeSupervisor::new(
        kt_engine_config(&fixture(), &signal_data),
        Arc::clone(&store),
        kt_signal_connector::DEFAULT_PROXY_GROUP_ID.to_string(),
        None,
    ));

    supervisor.start().await.unwrap();

    let status = status(&supervisor, &seeded.id).await;
    assert_eq!(status["state"], "running");
    assert_eq!(status["progress"]["stage"], "downloading");
    assert_eq!(status["progress"]["bytesTransferred"], 300);
    assert_eq!(status["progress"]["bytesTotal"], 900);

    supervisor.shutdown().await.unwrap();
}

/// content-length absent: stage-only downloading progress (no zero-filled
/// bytes — the honest shape the contract pins).
#[tokio::test]
async fn running_without_content_length_reports_stage_only_progress() {
    let temp = TempDir::new().unwrap();
    let signal_data = temp.path().join("signal-data");
    seed_engine_archive(&signal_data);
    let seeded = {
        let seed = test_store(temp.path());
        seed.upsert_account_from_signal(
            SIGNAL_ACCOUNT,
            Some(1),
            kt_signal_connector::DEFAULT_PROXY_GROUP_ID,
        )
        .unwrap()
    };
    let store = test_store(temp.path());
    store
        .begin_history_import(&seeded.id, kt_signal_connector::link::now_ms())
        .unwrap();
    set_transfer_snapshot(&signal_data, Some((120, None)));
    let supervisor = Arc::new(RuntimeSupervisor::new(
        kt_engine_config(&fixture(), &signal_data),
        Arc::clone(&store),
        kt_signal_connector::DEFAULT_PROXY_GROUP_ID.to_string(),
        None,
    ));

    supervisor.start().await.unwrap();

    let status = status(&supervisor, &seeded.id).await;
    assert_eq!(status["state"], "running");
    assert_eq!(status["progress"]["stage"], "downloading");
    assert!(status["progress"].get("bytesTransferred").is_none());
    assert!(status["progress"].get("bytesTotal").is_none());

    supervisor.shutdown().await.unwrap();
}

/// No engine download snapshot while running = stage processing (the
/// connector's own NDJSON pass), never byte fields. The startup sweep itself
/// supplies the running pass: it claims the slot and runs the real import on
/// the seeded running row, so the terminal completion proves the pass ran and
/// the honest terminal shape drops the progress object again. The
/// processing-stage projection itself is pinned deterministically by the
/// supervisor unit test (`compose_import_progress`).
#[tokio::test]
async fn running_without_snapshot_reports_processing_progress() {
    let temp = TempDir::new().unwrap();
    let signal_data = temp.path().join("signal-data");
    seed_engine_archive(&signal_data);
    let seeded = {
        let seed = test_store(temp.path());
        seed.upsert_account_from_signal(
            SIGNAL_ACCOUNT,
            Some(1),
            kt_signal_connector::DEFAULT_PROXY_GROUP_ID,
        )
        .unwrap()
    };
    let store = test_store(temp.path());
    store
        .begin_history_import(&seeded.id, kt_signal_connector::link::now_ms())
        .unwrap();
    let supervisor = Arc::new(RuntimeSupervisor::new(
        kt_engine_config(&fixture(), &signal_data),
        Arc::clone(&store),
        kt_signal_connector::DEFAULT_PROXY_GROUP_ID.to_string(),
        None,
    ));

    supervisor.start().await.unwrap();

    timeout(Duration::from_secs(20), async {
        loop {
            if status(&supervisor, &seeded.id).await["state"] == "completed" {
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the sweep must finish the claimed import");
    let final_status = status(&supervisor, &seeded.id).await;
    assert_eq!(final_status["state"], "completed");
    assert!(final_status.get("progress").is_none());

    supervisor.shutdown().await.unwrap();
}

/// Terminal states never carry progress — even with a stale download marker
/// on the fixture face, the completed projection stays byte-identical to 1.39.
#[tokio::test]
async fn terminal_states_never_carry_progress() {
    let temp = TempDir::new().unwrap();
    let signal_data = temp.path().join("signal-data");
    seed_engine_archive(&signal_data);
    let seeded = {
        let seed = test_store(temp.path());
        seed.upsert_account_from_signal(
            SIGNAL_ACCOUNT,
            Some(1),
            kt_signal_connector::DEFAULT_PROXY_GROUP_ID,
        )
        .unwrap()
    };
    let store = test_store(temp.path());
    seed_completed_row(&store, &seeded.id);
    set_transfer_snapshot(&signal_data, Some((1, Some(2))));
    let supervisor = Arc::new(RuntimeSupervisor::new(
        kt_engine_config(&fixture(), &signal_data),
        Arc::clone(&store),
        kt_signal_connector::DEFAULT_PROXY_GROUP_ID.to_string(),
        None,
    ));

    supervisor.start().await.unwrap();

    let status = status(&supervisor, &seeded.id).await;
    assert_eq!(status["state"], "completed");
    assert!(status.get("progress").is_none());

    supervisor.shutdown().await.unwrap();
}

/// Malformed engine snapshot (transferred > total, or a non-numeric byte
/// field) drops the progress object but keeps the running state.
#[tokio::test]
async fn malformed_snapshot_drops_progress_keeps_state() {
    let temp = TempDir::new().unwrap();
    let signal_data = temp.path().join("signal-data");
    seed_engine_archive(&signal_data);
    let seeded = {
        let seed = test_store(temp.path());
        seed.upsert_account_from_signal(
            SIGNAL_ACCOUNT,
            Some(1),
            kt_signal_connector::DEFAULT_PROXY_GROUP_ID,
        )
        .unwrap()
    };
    let store = test_store(temp.path());
    store
        .begin_history_import(&seeded.id, kt_signal_connector::link::now_ms())
        .unwrap();
    // transferred > total: unrenderable, drop the object.
    set_transfer_snapshot(&signal_data, Some((900, Some(300))));
    let supervisor = Arc::new(RuntimeSupervisor::new(
        kt_engine_config(&fixture(), &signal_data),
        Arc::clone(&store),
        kt_signal_connector::DEFAULT_PROXY_GROUP_ID.to_string(),
        None,
    ));

    supervisor.start().await.unwrap();

    let status = status(&supervisor, &seeded.id).await;
    assert_eq!(status["state"], "running");
    assert!(status.get("progress").is_none());

    supervisor.shutdown().await.unwrap();
}

/// history.importRetry: a failed row + archive on disk clears the row and
/// re-arms (pending), and a later status reads `pending` (no row, archive
/// inside the link window... the row is gone so the archive presence decides).
#[tokio::test]
async fn retry_clears_failed_row_and_rearms() {
    let temp = TempDir::new().unwrap();
    let signal_data = temp.path().join("signal-data");
    seed_engine_archive(&signal_data);
    let seeded = {
        let seed = test_store(temp.path());
        seed.upsert_account_from_signal(
            SIGNAL_ACCOUNT,
            Some(1),
            kt_signal_connector::DEFAULT_PROXY_GROUP_ID,
        )
        .unwrap()
    };
    let store = test_store(temp.path());
    store
        .begin_history_import(&seeded.id, kt_signal_connector::link::now_ms())
        .unwrap();
    store
        .complete_history_import(
            &seeded.id,
            "failed",
            &kt_signal_connector::store::ImportCounters::default(),
            Some("malformed"),
            kt_signal_connector::link::now_ms(),
        )
        .unwrap();
    let supervisor = Arc::new(RuntimeSupervisor::new(
        kt_engine_config(&fixture(), &signal_data),
        Arc::clone(&store),
        kt_signal_connector::DEFAULT_PROXY_GROUP_ID.to_string(),
        None,
    ));

    let retry = supervisor
        .history_import_retry(seeded.id.clone())
        .await
        .unwrap();
    assert_eq!(retry["armed"], true);
    assert_eq!(retry["state"], "pending");
    assert!(store.history_import_row(&seeded.id).unwrap().is_none());

    supervisor.shutdown().await.unwrap();
}

/// The ladder: a completed row is NOT_RETRYABLE, and without any importable
/// archive NO_ARCHIVE answers (both retryable=false).
#[tokio::test]
async fn retry_ladder_answers_not_retryable_and_no_archive() {
    let temp = TempDir::new().unwrap();
    let signal_data = temp.path().join("signal-data");
    seed_engine_archive(&signal_data);
    let seeded = {
        let seed = test_store(temp.path());
        seed.upsert_account_from_signal(
            SIGNAL_ACCOUNT,
            Some(1),
            kt_signal_connector::DEFAULT_PROXY_GROUP_ID,
        )
        .unwrap()
    };
    let store = test_store(temp.path());
    let supervisor = Arc::new(RuntimeSupervisor::new(
        kt_engine_config(&fixture(), &signal_data),
        Arc::clone(&store),
        kt_signal_connector::DEFAULT_PROXY_GROUP_ID.to_string(),
        None,
    ));

    // A failed row whose archive vanished: NO_ARCHIVE (retryable=false).
    store
        .begin_history_import(&seeded.id, kt_signal_connector::link::now_ms())
        .unwrap();
    store
        .complete_history_import(
            &seeded.id,
            "failed",
            &kt_signal_connector::store::ImportCounters::default(),
            Some("malformed"),
            kt_signal_connector::link::now_ms(),
        )
        .unwrap();
    std::fs::remove_file(crate_tests_archive_path(&signal_data)).unwrap();
    let missing = supervisor
        .history_import_retry(seeded.id.clone())
        .await
        .unwrap_err();
    let kt_signal_connector::service::ServiceError::Api(error) = &missing else {
        panic!("expected an Api ladder error, got {missing:?}");
    };
    assert_eq!(error.code, "NO_ARCHIVE");
    assert!(!error.retryable);

    // A completed row with the archive present: NOT_RETRYABLE (completed is
    // the consumption marker — re-running an import is not a failure retry).
    std::fs::write(
        crate_tests_archive_path(&signal_data),
        "{\"type\":\"recipient\"}\n",
    )
    .unwrap();
    store
        .begin_history_import(&seeded.id, kt_signal_connector::link::now_ms())
        .unwrap();
    store
        .complete_history_import(
            &seeded.id,
            "completed",
            &kt_signal_connector::store::ImportCounters::default(),
            None,
            kt_signal_connector::link::now_ms(),
        )
        .unwrap();
    let completed = supervisor
        .history_import_retry(seeded.id.clone())
        .await
        .unwrap_err();
    let kt_signal_connector::service::ServiceError::Api(error) = &completed else {
        panic!("expected an Api ladder error, got {completed:?}");
    };
    assert_eq!(error.code, "NOT_RETRYABLE");
    assert!(!error.retryable);

    supervisor.shutdown().await.unwrap();
}

fn crate_tests_archive_path(signal_data: &std::path::Path) -> std::path::PathBuf {
    signal_data
        .join("accounts")
        .join("lk-abc")
        .join("history-import.ndjson")
}

/// Host-frame shape gate: missing accountId answers INVALID_REQUEST before
/// any state inspection.
#[tokio::test]
async fn retry_params_gate_answers_invalid_request() {
    let temp = TempDir::new().unwrap();
    let signal_data = temp.path().join("signal-data");
    std::fs::create_dir_all(signal_data.join("accounts").join("lk-abc")).unwrap();
    std::fs::write(
        signal_data.join("engine-state.json"),
        r#"{"version":2,"accounts":[]}"#,
    )
    .unwrap();
    let store = test_store(temp.path());
    let supervisor = Arc::new(RuntimeSupervisor::new(
        kt_engine_config(&fixture(), &signal_data),
        Arc::clone(&store),
        kt_signal_connector::DEFAULT_PROXY_GROUP_ID.to_string(),
        None,
    ));

    // Unknown account also rides the store path: ACCOUNT_NOT_FOUND (the
    // shared read answer), not a ladder code — the ladder only speaks about
    // accounts that exist.
    let error = supervisor
        .history_import_retry("a".repeat(64))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        kt_signal_connector::service::ServiceError::Store(
            kt_signal_connector::store::StoreError::AccountNotFound
        )
    ));

    supervisor.shutdown().await.unwrap();
}
