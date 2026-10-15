// SPDX-License-Identifier: AGPL-3.0-only

#![cfg(unix)]

//! Link-time history import end to end (contract revision 1.39, §4.38): a
//! launch consumes the archive the engine's registry names and the status
//! face walks pending → completed. The fixture plays the engine, so the
//! "archive" is the NDJSON the engine's S2 export projects.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use kt_signal_connector::engine::EngineConfig;
use kt_signal_connector::ids::stable_hash_id;
use kt_signal_connector::store::{Store, StoreKey};
use kt_signal_connector::supervisor::RuntimeSupervisor;
use serde_json::Value;
use tempfile::TempDir;
use tokio::time::{sleep, timeout};

fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fake-engine.py")
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

fn kt_engine_config(fixture: &std::path::Path, data_dir: &std::path::Path) -> EngineConfig {
    let mut config = EngineConfig::new(fixture.to_path_buf(), data_dir.to_path_buf());
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

// ---------------------------------------------------------------------------
// Contract revision 1.148 (§4.146): the `history.importCancel` face and the
// honest engine-failure consumption. The fixture plays the engine: the
// `history.importCancel` arm answers from the transfer markers (a live
// download aborts and clears them), and `.fixture-transfer-error` carries the
// terminal failure code the status face consumes.
// ---------------------------------------------------------------------------

/// Mark a fixture transfer download terminally failed with the engine's short
/// code, or clear the marker (argument `None`).
fn set_transfer_error(signal_data: &std::path::Path, code: Option<&str>) {
    let marker = signal_data.join(".fixture-transfer-error");
    match code {
        Some(code) => std::fs::write(&marker, code).unwrap(),
        None => {
            let _ = std::fs::remove_file(&marker);
        }
    }
}

/// Lay out the engine face with a many-message NDJSON export — large enough
/// that the sweep's import pass is observably live (the mid-pass cancel needs
/// a `running` window wider than one poll).
fn seed_large_engine_archive(data_dir: &std::path::Path, messages: usize) {
    let account_dir = data_dir.join("accounts").join("lk-abc");
    std::fs::create_dir_all(&account_dir).unwrap();
    std::fs::write(
        data_dir.join("engine-state.json"),
        format!(
            r#"{{"version":2,"accounts":[{{"number":"{SIGNAL_ACCOUNT}","dir":"accounts/lk-abc"}}]}}"#
        ),
    )
    .unwrap();
    let mut body = String::new();
    body.push_str(&format!(
        r#"{{"type":"recipient","id":1,"kind":"contact","name":"Alice Import","aci":"{ALICE_ACI}","e164":"{ALICE_E164}"}}"#
    ));
    body.push('\n');
    body.push_str(r#"{"type":"chat","id":10,"recipientId":1}"#);
    body.push('\n');
    for index in 0..messages {
        let direction = if index % 2 == 0 {
            "incoming"
        } else {
            "outgoing"
        };
        body.push_str(&format!(
            r#"{{"type":"message","chatId":10,"authorId":1,"dateSent":{},"direction":"{}","text":"archived line {index} with some padding so the file stays realistic"}}"#,
            1000 + index as u64, direction
        ));
        body.push('\n');
    }
    std::fs::write(account_dir.join("history-import.ndjson"), body).unwrap();
}

/// The full cancel arc against a live pass: the sweep claims the large
/// archive, the cancel aborts it mid-run (the engine forward answers `true`
/// against the in-flight fixture download), the run lands
/// `failed`/`cancelled` having spent exactly its one attempt, a repeated
/// cancel is the idempotent no-op, and the failed row stays
/// `history.importRetry`-able — the re-arm clears the marker and the face
/// returns to the honest disk-pending.
#[tokio::test]
async fn cancel_mid_pass_lands_cancelled_failed_row_and_retry_rearms() {
    let temp = TempDir::new().unwrap();
    let signal_data = temp.path().join("signal-data");
    seed_large_engine_archive(&signal_data, 40_000);
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

    supervisor.start().await.unwrap();

    // The sweep's settle (5s) plus claim: wait for the live pass.
    timeout(Duration::from_secs(20), async {
        loop {
            if status(&supervisor, &seeded.id).await["state"] == "running" {
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the sweep must claim the archive");

    // A live engine download on the fixture face: the cancel forward aborts
    // it and the answer mirrors the engine's `true`.
    set_transfer_snapshot(&signal_data, Some((10, Some(100))));
    let cancelled = supervisor
        .history_import_cancel(seeded.id.clone())
        .await
        .unwrap();
    assert_eq!(cancelled["accountId"], seeded.id.as_str());
    assert_eq!(cancelled["cancelled"], true);
    assert_eq!(cancelled["state"], "failed");

    // The aborted pass lands its honest failed row; the cancel itself spent
    // no budget — exactly the run's own one attempt.
    timeout(Duration::from_secs(20), async {
        loop {
            if status(&supervisor, &seeded.id).await["state"] == "failed" {
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the aborted pass must land failed");
    let failed = status(&supervisor, &seeded.id).await;
    assert_eq!(failed["state"], "failed");
    assert_eq!(failed["errorClass"], "cancelled");
    assert_eq!(failed["attempts"], 1);

    // Idempotent repeat: nothing is in flight anymore (the fixture's markers
    // cleared on the first cancel), the ledger answer repeats.
    let again = supervisor
        .history_import_cancel(seeded.id.clone())
        .await
        .unwrap();
    assert_eq!(again["cancelled"], false);
    assert_eq!(again["state"], "failed");

    // The canceled failed row stays retry-able verbatim; the re-arm clears
    // the marker and the face returns to the disk-pending answer.
    let retry = supervisor
        .history_import_retry(seeded.id.clone())
        .await
        .unwrap();
    assert_eq!(retry["armed"], true);
    assert_eq!(retry["state"], "pending");
    assert!(store.history_import_row(&seeded.id).unwrap().is_none());
    let rearmed = status(&supervisor, &seeded.id).await;
    assert_eq!(rearmed["state"], "pending");
    assert!(rearmed.get("reason").is_none());

    supervisor.shutdown().await.unwrap();
}

/// A cancel before the sweep's settle blocks the whole pickup: the probe
/// refuses the account, nothing ever imports, the face answers
/// `unavailable`/`cancelled` (never `pending`, never a phantom row), and the
/// re-arm returns the account to the ordinary pending face.
#[tokio::test]
async fn cancel_marker_blocks_the_startup_sweep() {
    let temp = TempDir::new().unwrap();
    let signal_data = temp.path().join("signal-data");
    seed_engine_archive(&signal_data);
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

    supervisor.start().await.unwrap();

    // Cancel inside the settle window: nothing in flight on the engine face,
    // no ledger row yet — the marker face answers.
    let cancelled = supervisor
        .history_import_cancel(seeded.id.clone())
        .await
        .unwrap();
    assert_eq!(cancelled["cancelled"], false);
    assert_eq!(cancelled["state"], "unavailable");

    // Outlast the settle plus any claim: the probe must have refused — no
    // row, no import, the marker's honest answer stands.
    sleep(Duration::from_secs(8)).await;
    let after = status(&supervisor, &seeded.id).await;
    assert_eq!(after["state"], "unavailable");
    assert_eq!(after["reason"], "cancelled");
    assert!(store.history_import_row(&seeded.id).unwrap().is_none());

    // The re-arm clears the marker: the disk face (the once-per-start sweep
    // has passed) answers pending again.
    let retry = supervisor
        .history_import_retry(seeded.id.clone())
        .await
        .unwrap();
    assert_eq!(retry["armed"], true);
    let rearmed = status(&supervisor, &seeded.id).await;
    assert_eq!(rearmed["state"], "pending");
    assert!(rearmed.get("reason").is_none());

    supervisor.shutdown().await.unwrap();
}

/// The cancel ladder (engine down — `cancelled` is honestly `false` on every
/// arm): unknown account fails the shared store path, a completed row never
/// regresses, a failed row stays itself and retry-able, and a rowless
/// account answers the marker's `unavailable` — every arm idempotent.
#[tokio::test]
async fn cancel_ladder_unknown_completed_failed_and_unavailable() {
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
    let failed_account = {
        let seed = test_store(temp.path());
        seed.upsert_account_from_signal(
            "+15555550101",
            Some(1),
            kt_signal_connector::DEFAULT_PROXY_GROUP_ID,
        )
        .unwrap()
    };
    // The failed arm's archive must be on that account's engine face for the
    // re-arm: register it beside the seeded account and drop the file.
    {
        let failed_dir = signal_data.join("accounts").join("lk-failed");
        std::fs::create_dir_all(&failed_dir).unwrap();
        std::fs::write(
            signal_data.join("engine-state.json"),
            format!(
                r#"{{"version":2,"accounts":[
                    {{"number":"{SIGNAL_ACCOUNT}","dir":"accounts/lk-abc"}},
                    {{"number":"+15555550101","dir":"accounts/lk-failed"}}
                ]}}"#
            ),
        )
        .unwrap();
        std::fs::write(
            failed_dir.join("history-import.ndjson"),
            "{\"type\":\"recipient\"}\n",
        )
        .unwrap();
    }
    let rowless_account = {
        let seed = test_store(temp.path());
        seed.upsert_account_from_signal(
            "+15555550102",
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

    // Unknown account: ACCOUNT_NOT_FOUND, the shared store answer.
    let error = supervisor
        .history_import_cancel("a".repeat(64))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        kt_signal_connector::service::ServiceError::Store(
            kt_signal_connector::store::StoreError::AccountNotFound
        )
    ));

    // A completed row never regresses; the repeat answers the same.
    seed_completed_row(&store, &seeded.id);
    for _ in 0..2 {
        let done = supervisor
            .history_import_cancel(seeded.id.clone())
            .await
            .unwrap();
        assert_eq!(done["cancelled"], false);
        assert_eq!(done["state"], "completed");
    }
    let row = store.history_import_row(&seeded.id).unwrap().unwrap();
    assert_eq!(row.state, "completed");
    assert!(row.error_class.is_none());

    // A failed row stays itself (no flip — the conditional update only ever
    // touched `running` rows) and stays retry-able after the cancel; the
    // re-arm clears the marker and the face returns to pending.
    store
        .begin_history_import(&failed_account.id, kt_signal_connector::link::now_ms())
        .unwrap();
    store
        .complete_history_import(
            &failed_account.id,
            "failed",
            &kt_signal_connector::store::ImportCounters::default(),
            Some("malformed"),
            kt_signal_connector::link::now_ms(),
        )
        .unwrap();
    let failed = supervisor
        .history_import_cancel(failed_account.id.clone())
        .await
        .unwrap();
    assert_eq!(failed["state"], "failed");
    let row = store
        .history_import_row(&failed_account.id)
        .unwrap()
        .unwrap();
    assert_eq!(row.state, "failed");
    assert_eq!(row.error_class.as_deref(), Some("malformed"));
    let retry = supervisor
        .history_import_retry(failed_account.id.clone())
        .await
        .unwrap();
    assert_eq!(retry["armed"], true);
    let rearmed = status(&supervisor, &failed_account.id).await;
    assert_eq!(rearmed["state"], "pending");
    assert!(rearmed.get("reason").is_none());

    // No row, no archive, window expired: the cancel answers the
    // `unavailable` face and the idempotent repeat does too.
    for _ in 0..2 {
        let unavailable = supervisor
            .history_import_cancel(rowless_account.id.clone())
            .await
            .unwrap();
        assert_eq!(unavailable["cancelled"], false);
        assert_eq!(unavailable["state"], "unavailable");
    }

    supervisor.shutdown().await.unwrap();
}

/// `no_archive` is the information state, consumed immediately: inside the
/// wait window with no row and no archive the face answers
/// `unavailable`/`no-archive` at once (not after ten minutes), no ledger row
/// ever lands, and a live-download snapshot without `errorCode` beside it
/// keeps the pre-1.48 `pending`.
#[tokio::test]
async fn no_archive_code_answers_informational_without_a_row() {
    let temp = TempDir::new().unwrap();
    let signal_data = temp.path().join("signal-data");
    // No engine-state registry: the archive face has nothing on disk, the
    // consult fires against the store's signal number alone.
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

    supervisor.start().await.unwrap();

    // A live download face (no errorCode): the consult happened, there is
    // no signal to consume — the pre-1.48 pending stands.
    set_transfer_snapshot(&signal_data, Some((5, Some(9))));
    let pending = status(&supervisor, &seeded.id).await;
    assert_eq!(pending["state"], "pending");
    assert!(pending.get("reason").is_none());
    assert!(store.history_import_row(&seeded.id).unwrap().is_none());

    // The terminal `no_archive` face: the information state, at once.
    set_transfer_error(&signal_data, Some("no_archive"));
    let face = status(&supervisor, &seeded.id).await;
    assert_eq!(face["state"], "unavailable");
    assert_eq!(face["reason"], "no-archive");
    assert!(face.get("progress").is_none());
    // The information state never lands a row and never spends budget.
    assert!(store.history_import_row(&seeded.id).unwrap().is_none());

    // Stable on repeat: still informational, still no row.
    let face = status(&supervisor, &seeded.id).await;
    assert_eq!(face["state"], "unavailable");
    assert_eq!(face["reason"], "no-archive");
    assert!(store.history_import_row(&seeded.id).unwrap().is_none());

    supervisor.shutdown().await.unwrap();
}

/// Every failure code but `no_archive` lands an honest persisted failed row
/// with the engine's class verbatim, one attempt, at once — here the known
/// `cdn_unreachable` and the 0a2ab18 `network` on a second account. With no
/// archive on disk the row's retry ladder answers NO_ARCHIVE (nothing ever
/// landed to re-import).
#[tokio::test]
async fn engine_failure_code_lands_failed_row_verbatim() {
    let temp = TempDir::new().unwrap();
    let signal_data = temp.path().join("signal-data");
    let first = {
        let seed = test_store(temp.path());
        seed.upsert_account_from_signal(
            SIGNAL_ACCOUNT,
            Some(kt_signal_connector::link::now_ms()),
            kt_signal_connector::DEFAULT_PROXY_GROUP_ID,
        )
        .unwrap()
    };
    let second = {
        let seed = test_store(temp.path());
        seed.upsert_account_from_signal(
            "+15555550101",
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

    supervisor.start().await.unwrap();

    set_transfer_error(&signal_data, Some("cdn_unreachable"));
    let face = status(&supervisor, &first.id).await;
    assert_eq!(face["state"], "failed");
    assert_eq!(face["errorClass"], "cdn_unreachable");
    assert_eq!(face["attempts"], 1);
    assert_eq!(face["accountId"], first.id.as_str());
    // Persisted verbatim: the store row is the same answer a restart reads.
    let row = store.history_import_row(&first.id).unwrap().unwrap();
    assert_eq!(row.state, "failed");
    assert_eq!(row.error_class.as_deref(), Some("cdn_unreachable"));
    assert_eq!(row.attempts, 1);

    // The second account consumes the 0a2ab18 `network` code the same way.
    set_transfer_error(&signal_data, Some("network"));
    let face = status(&supervisor, &second.id).await;
    assert_eq!(face["state"], "failed");
    assert_eq!(face["errorClass"], "network");
    assert_eq!(face["attempts"], 1);

    // The honest retry ladder on a landed failure whose archive never
    // arrived: NO_ARCHIVE (nothing on the engine face to re-import).
    let error = supervisor
        .history_import_retry(first.id.clone())
        .await
        .unwrap_err();
    let kt_signal_connector::service::ServiceError::Api(error) = &error else {
        panic!("expected an Api ladder error, got {error:?}");
    };
    assert_eq!(error.code, "NO_ARCHIVE");
    assert!(!error.retryable);

    supervisor.shutdown().await.unwrap();
}

/// A malformed engine code — empty, or past the 64-char cap — is no signal:
/// the pre-1.48 pending stands, nothing lands, no budget moves.
#[tokio::test]
async fn malformed_failure_code_keeps_pre_148_pending() {
    let temp = TempDir::new().unwrap();
    let signal_data = temp.path().join("signal-data");
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

    supervisor.start().await.unwrap();

    for junk in ["", &"x".repeat(65)] {
        set_transfer_error(&signal_data, Some(junk));
        let face = status(&supervisor, &seeded.id).await;
        assert_eq!(face["state"], "pending", "code {junk:?} must not consume");
        assert!(face.get("reason").is_none());
        assert!(store.history_import_row(&seeded.id).unwrap().is_none());
    }

    supervisor.shutdown().await.unwrap();
}

/// An `errorCode`-carrying snapshot is never a byte-progress source: a
/// `running` row beside a stale terminal face keeps the honest no-byte
/// answer (the §4.51 parser rejects the failure face), and the row is not
/// consumed — the failure consumption only ever fires on the wait-window
/// `pending`.
#[tokio::test]
async fn running_row_with_failure_snapshot_drops_progress_keeps_running() {
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
    // Both markers up: the fixture's terminal face wins over the live bytes.
    set_transfer_snapshot(&signal_data, Some((300, Some(900))));
    set_transfer_error(&signal_data, Some("io"));
    let supervisor = Arc::new(RuntimeSupervisor::new(
        kt_engine_config(&fixture(), &signal_data),
        Arc::clone(&store),
        kt_signal_connector::DEFAULT_PROXY_GROUP_ID.to_string(),
        None,
    ));

    supervisor.start().await.unwrap();

    // Read inside the settle window: the seeded row is still the live pass's
    // ledger truth, and the failure snapshot rides along.
    let face = status(&supervisor, &seeded.id).await;
    assert_eq!(face["state"], "running");
    assert!(face.get("progress").is_none());
    // Not consumed: the running row stays the running row.
    let row = store.history_import_row(&seeded.id).unwrap().unwrap();
    assert_eq!(row.state, "running");
    assert_eq!(row.attempts, 1);

    supervisor.shutdown().await.unwrap();
}

// ---------------------------------------------------------------------------
// Contract revision 1.166 (§4.164): the wait-window `pending` carries the
// live download face. The same single consult that §4.146 used for failure
// consumption now projects the engine's live transfer snapshot as the §4.51
// progress face — failure terminal keeps priority, absent/malformed keeps
// the bare pending, and the disk-pending still never consults.
// ---------------------------------------------------------------------------

/// Contract 1.166: inside the wait window with no archive the live engine
/// snapshot rides the pending answer — byte brothers when the engine read a
/// content-length, stage alone when it did not (the long-poll stretch), and
/// the bare pre-1.166 pending when the snapshot is absent. A disk-pending
/// (archive present) still never consults: stale markers produce no
/// progress beside an archive the connector is about to import.
#[tokio::test]
async fn wait_window_pending_projects_live_download_progress() {
    let temp = TempDir::new().unwrap();
    let signal_data = temp.path().join("signal-data");
    // Registry names the account directory, no archive on it yet: the
    // wait-window pending branch owns the answer.
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

    supervisor.start().await.unwrap();

    // CDN streaming stretch with a content-length: the byte brothers ride
    // the pending face exactly as they ride the running face.
    set_transfer_snapshot(&signal_data, Some((120, Some(480))));
    let face = status(&supervisor, &seeded.id).await;
    assert_eq!(face["state"], "pending");
    assert_eq!(face["progress"]["stage"], "downloading");
    assert_eq!(face["progress"]["bytesTransferred"], 120);
    assert_eq!(face["progress"]["bytesTotal"], 480);

    // Long-poll stretch (bytesRead 0, no total): stage alone, never a
    // zero-filled total.
    set_transfer_snapshot(&signal_data, Some((0, None)));
    let face = status(&supervisor, &seeded.id).await;
    assert_eq!(face["state"], "pending");
    assert_eq!(face["progress"]["stage"], "downloading");
    assert!(face["progress"].get("bytesTransferred").is_none());
    assert!(face["progress"].get("bytesTotal").is_none());

    // Malformed byte shape (transferred > total): the object drops and the
    // bare pending stands — no progress invented.
    set_transfer_snapshot(&signal_data, Some((900, Some(300))));
    let face = status(&supervisor, &seeded.id).await;
    assert_eq!(face["state"], "pending");
    assert!(face.get("progress").is_none());

    // Snapshot absent (engine down/restarted/success-cleared): the honest
    // pre-1.166 pending, byte for byte.
    set_transfer_snapshot(&signal_data, None);
    let face = status(&supervisor, &seeded.id).await;
    assert_eq!(face["state"], "pending");
    assert!(face.get("reason").is_none());
    assert!(face.get("progress").is_none());

    // The disk-pending boundary: archive on the engine face flips the
    // projection to the never-consults branch. Stale live markers stay up —
    // and no progress may appear beside an archive the connector is about
    // to import.
    set_transfer_snapshot(&signal_data, Some((120, Some(480))));
    std::fs::write(
        crate_tests_archive_path(&signal_data),
        "{\"type\":\"recipient\",\"id\":1,\"kind\":\"contact\",\"name\":\"Alice Import\"}\n",
    )
    .unwrap();
    let face = status(&supervisor, &seeded.id).await;
    assert_eq!(face["state"], "pending");
    assert!(face.get("progress").is_none());
    // And nothing landed: the disk-pending consumes nothing either.
    assert!(store.history_import_row(&seeded.id).unwrap().is_none());

    supervisor.shutdown().await.unwrap();
}

/// The failure-code face stays a verbatim pass-through (§4.146, re-pinned by
/// §4.164): the engine's new `relink_requested` and `continue_without_upload`
/// terminal codes land immediate persisted failed rows with `errorClass`
/// exactly as the engine spelled them — one attempt each, no normalization,
/// no connector-side semantics (the engine owns the word list).
#[tokio::test]
async fn engine_relink_and_continue_without_upload_codes_land_failed_rows_verbatim() {
    let temp = TempDir::new().unwrap();
    let signal_data = temp.path().join("signal-data");
    let first = {
        let seed = test_store(temp.path());
        seed.upsert_account_from_signal(
            SIGNAL_ACCOUNT,
            Some(kt_signal_connector::link::now_ms()),
            kt_signal_connector::DEFAULT_PROXY_GROUP_ID,
        )
        .unwrap()
    };
    let second = {
        let seed = test_store(temp.path());
        seed.upsert_account_from_signal(
            "+15555550101",
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

    supervisor.start().await.unwrap();

    set_transfer_error(&signal_data, Some("relink_requested"));
    let face = status(&supervisor, &first.id).await;
    assert_eq!(face["state"], "failed");
    assert_eq!(face["errorClass"], "relink_requested");
    assert_eq!(face["attempts"], 1);
    assert_eq!(face["accountId"], first.id.as_str());
    let row = store.history_import_row(&first.id).unwrap().unwrap();
    assert_eq!(row.state, "failed");
    assert_eq!(row.error_class.as_deref(), Some("relink_requested"));
    assert_eq!(row.attempts, 1);

    set_transfer_error(&signal_data, Some("continue_without_upload"));
    let face = status(&supervisor, &second.id).await;
    assert_eq!(face["state"], "failed");
    assert_eq!(face["errorClass"], "continue_without_upload");
    assert_eq!(face["attempts"], 1);
    let row = store.history_import_row(&second.id).unwrap().unwrap();
    assert_eq!(row.error_class.as_deref(), Some("continue_without_upload"));

    supervisor.shutdown().await.unwrap();
}
