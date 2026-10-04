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
