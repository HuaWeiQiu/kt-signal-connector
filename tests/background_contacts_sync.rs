// SPDX-License-Identifier: AGPL-3.0-only

#![cfg(unix)]

//! Batch I: engine completion notifications drive the connector's own
//! contacts/groups store merge. The desktop's manual `contacts.sync` stays a
//! refresh face; the background worker consumes the same completion frames
//! the desktop receives (`engine.storagePull` both-ok and
//! `engine.initialSyncComplete` manifest-ok) and runs the ordinary sync
//! implementation. These tests use the fake engine's raw-notification marker
//! (contract 1.134) so no desktop/RPC trigger is involved.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use kt_signal_connector::engine::EngineConfig;
use kt_signal_connector::service::HostSideEvent;
use kt_signal_connector::store::{Store, StoreKey};
use kt_signal_connector::supervisor::RuntimeSupervisor;
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::time::{sleep, timeout};

const SIGNAL_ACCOUNT: &str = "+15555550100";

fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fake-engine.py")
}

fn test_store(dir: &Path) -> Arc<Store> {
    Arc::new(Store::open(dir, Some(StoreKey::from_bytes([0x5A; 32]))).unwrap())
}

fn seed_engine_registry(data_dir: &Path) {
    let account_dir = data_dir.join("accounts").join("lk-abc");
    fs::create_dir_all(&account_dir).unwrap();
    fs::write(
        data_dir.join("engine-state.json"),
        format!(
            r#"{{"version":2,"accounts":[{{"number":"{SIGNAL_ACCOUNT}","dir":"accounts/lk-abc"}}]}}"#
        ),
    )
    .unwrap();
}

fn supervisor(temp: &TempDir, store: Arc<Store>) -> (Arc<RuntimeSupervisor>, PathBuf) {
    let data_dir = temp.path().join("signal-data");
    seed_engine_registry(&data_dir);
    let mut config = EngineConfig::new(fixture(), data_dir.clone());
    config.request_timeout = Duration::from_secs(2);
    config.shutdown_grace = Duration::from_millis(100);
    let supervisor = Arc::new(RuntimeSupervisor::new(
        config,
        store,
        kt_signal_connector::DEFAULT_PROXY_GROUP_ID.to_string(),
        None,
    ));
    supervisor.spawn_background_sync_worker();
    (supervisor, data_dir)
}

fn seed_account(store: &Store, number: &str) -> String {
    store
        .upsert_account_from_signal(number, Some(1), kt_signal_connector::DEFAULT_PROXY_GROUP_ID)
        .unwrap()
        .id
}

async fn wait_for_fixture_group(store: &Store, account_id: &str) {
    timeout(Duration::from_secs(20), async {
        loop {
            let page = store.list_conversations(account_id, 50, None).unwrap();
            if page
                .items
                .iter()
                .any(|row| row.kind == "group" && row.title == "Fixture Group")
            {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("account {account_id} never materialized the fixture group"));
}

async fn wait_for_completion_frame(
    lane: &mut tokio::sync::broadcast::Receiver<HostSideEvent>,
    account_id: &str,
) -> HostSideEvent {
    timeout(Duration::from_secs(5), async {
        loop {
            match lane.recv().await {
                Ok(event) => {
                    let matches = match &event {
                        HostSideEvent::StoragePull { account_id: id, .. }
                        | HostSideEvent::InitialSyncComplete { account_id: id, .. } => {
                            id == account_id
                        }
                        _ => false,
                    };
                    if matches {
                        return event;
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    panic!("host lane closed before the completion frame")
                }
            }
        }
    })
    .await
    .expect("completion frame within timeout")
}

fn write_notifications(data_dir: &Path, notifications: Vec<Value>) {
    fs::write(
        data_dir.join(".fixture-emit-notifications.json"),
        json!({ "notifications": notifications }).to_string(),
    )
    .unwrap();
}

fn storage_pull(account: &str, trigger: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "method": "engine.storagePull",
        "params": {
            "account": account,
            "trigger": trigger,
            "contactsOk": true,
            "groupsOk": true,
        }
    })
}

fn initial_sync_complete(account: &str, manifest_ok: bool) -> Value {
    json!({
        "jsonrpc": "2.0",
        "method": "engine.initialSyncComplete",
        "params": {
            "account": account,
            "manifestOk": manifest_ok,
            "profilesOk": false,
        }
    })
}

/// The desktop gate is not involved: a both-ok storage completion alone
/// materializes the group row, and a second completion inside the 60 s window
/// is a read-only no-op (the marker does not advance).
#[tokio::test]
async fn storage_pull_completion_merges_without_any_desktop_call() {
    let temp = TempDir::new().unwrap();
    let store = test_store(temp.path());
    let first = seed_account(&store, SIGNAL_ACCOUNT);
    let (supervisor, data_dir) = supervisor(&temp, Arc::clone(&store));
    supervisor.start().await.unwrap();
    // Let the startup/landing pass finish before adding the second account;
    // this test is about the event kick, not the landing pass.
    wait_for_fixture_group(&store, &first).await;

    let second = seed_account(&store, "+15555550300");
    let mut lane = supervisor.subscribe_host();
    write_notifications(&data_dir, vec![storage_pull("+15555550300", "login")]);

    let frame = wait_for_completion_frame(&mut lane, &second).await;
    assert!(matches!(frame, HostSideEvent::StoragePull { .. }));
    wait_for_fixture_group(&store, &second).await;
    let marker = store
        .contacts_synced_at(&second)
        .unwrap()
        .expect("marker stamped");

    // A second completion frame arrives (trigger fetchLatest), the worker
    // runs the shared sync face again, and the 60 s marker folds it into a
    // cached no-op: no second full pass, no marker movement, no duplicate row.
    sleep(Duration::from_millis(100)).await;
    write_notifications(&data_dir, vec![storage_pull("+15555550300", "fetchLatest")]);
    let _ = wait_for_completion_frame(&mut lane, &second).await;
    sleep(Duration::from_millis(400)).await;
    assert_eq!(
        store.contacts_synced_at(&second).unwrap(),
        Some(marker),
        "a completion inside the 60 s window must not run a second full sync"
    );
    let page = store.list_conversations(&second, 50, None).unwrap();
    assert_eq!(
        page.items
            .iter()
            .filter(|row| row.kind == "group" && row.title == "Fixture Group")
            .count(),
        1,
        "the merge is idempotent"
    );

    supervisor.shutdown().await.unwrap();
}

/// The link-window completion frame is the second trigger: manifest-ok is the
/// contacts/groups leg (profiles are a separate refresh), so it alone runs
/// the merge; a false manifest is not a successful completion and queues
/// nothing (the next authenticated connection retries).
#[tokio::test]
async fn initial_sync_complete_manifest_face_merges_and_false_leg_does_not() {
    let temp = TempDir::new().unwrap();
    let store = test_store(temp.path());
    let first = seed_account(&store, SIGNAL_ACCOUNT);
    let (supervisor, data_dir) = supervisor(&temp, Arc::clone(&store));
    supervisor.start().await.unwrap();
    wait_for_fixture_group(&store, &first).await;

    let ok_account = seed_account(&store, "+15555550400");
    let false_account = seed_account(&store, "+15555550500");
    let mut lane = supervisor.subscribe_host();

    // A false manifest still forwards the completion frame to the desktop
    // (its gate keeps waiting), but it is not a successful completion for the
    // background merge.
    write_notifications(
        &data_dir,
        vec![initial_sync_complete("+15555550500", false)],
    );
    let frame = wait_for_completion_frame(&mut lane, &false_account).await;
    assert!(matches!(frame, HostSideEvent::InitialSyncComplete { .. }));
    sleep(Duration::from_millis(800)).await;
    assert!(
        store.contacts_synced_at(&false_account).unwrap().is_none(),
        "a false manifest must not run the background merge"
    );
    assert!(
        store
            .list_conversations(&false_account, 50, None)
            .unwrap()
            .items
            .is_empty(),
        "a false manifest materialized rows"
    );

    write_notifications(&data_dir, vec![initial_sync_complete("+15555550400", true)]);
    let frame = wait_for_completion_frame(&mut lane, &ok_account).await;
    assert!(matches!(frame, HostSideEvent::InitialSyncComplete { .. }));
    wait_for_fixture_group(&store, &ok_account).await;
    assert!(store.contacts_synced_at(&ok_account).unwrap().is_some());

    supervisor.shutdown().await.unwrap();
}

/// A failed first pass is retried exactly once, silently, after the settle
/// delay: the fixture fails one `listContacts` (-32603), the worker waits,
/// and the retry materializes the row set. The host lane saw only the
/// completion frame — no error frame rides a background failure.
#[tokio::test]
async fn failed_background_sync_retries_once_and_converges() {
    let temp = TempDir::new().unwrap();
    let store = test_store(temp.path());
    let first = seed_account(&store, SIGNAL_ACCOUNT);
    let (supervisor, data_dir) = supervisor(&temp, Arc::clone(&store));
    supervisor.start().await.unwrap();
    wait_for_fixture_group(&store, &first).await;

    let second = seed_account(&store, "+15555550600");
    let fail_hook = data_dir.join(".fixture-list-contacts-fail-once");
    fs::write(&fail_hook, "1").unwrap();
    let mut lane = supervisor.subscribe_host();
    let started = Instant::now();
    write_notifications(&data_dir, vec![storage_pull("+15555550600", "login")]);
    let frame = wait_for_completion_frame(&mut lane, &second).await;
    assert!(matches!(frame, HostSideEvent::StoragePull { .. }));

    wait_for_fixture_group(&store, &second).await;
    assert!(
        started.elapsed() >= Duration::from_millis(1500),
        "the first attempt must have failed and the retry waited its settle delay"
    );
    assert!(
        !fail_hook.exists(),
        "the first `listContacts` attempt must have consumed the one-shot failure hook"
    );
    assert!(store.contacts_synced_at(&second).unwrap().is_some());

    supervisor.shutdown().await.unwrap();
}
