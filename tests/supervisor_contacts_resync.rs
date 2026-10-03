// SPDX-License-Identifier: AGPL-3.0-only

#![cfg(unix)]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use kt_signal_connector::engine::SignalCliConfig;
use kt_signal_connector::store::{Store, StoreKey};
use kt_signal_connector::supervisor::RuntimeSupervisor;
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

/// A fresh engine start must re-sync contacts/groups for already-linked
/// accounts: the sync materializes the contract-1.14 conversation skeletons,
/// which is the only path existing chats surface after a restart (pinned
/// signal-cli has no message-history backfill). Covers an account whose
/// link-time inline sync failed or predates the skeleton revision.
#[tokio::test]
async fn engine_start_resyncs_conversation_skeletons_for_linked_accounts() {
    let temp = TempDir::new().unwrap();
    let seeded = {
        let seed = test_store(temp.path());
        seed.upsert_account_from_signal(
            "+15555550100",
            Some(1),
            kt_signal_connector::DEFAULT_PROXY_GROUP_ID,
        )
        .unwrap()
    };
    let store = test_store(temp.path());
    let mut config = SignalCliConfig::new(fixture(), temp.path().join("signal-data"));
    config.request_timeout = Duration::from_secs(1);
    config.shutdown_grace = Duration::from_millis(100);
    let supervisor = Arc::new(RuntimeSupervisor::new(
        config,
        Arc::clone(&store),
        kt_signal_connector::DEFAULT_PROXY_GROUP_ID.to_string(),
        None,
    ));

    supervisor.start().await.unwrap();

    // Background pass: settle delay plus read-only fixture calls.
    timeout(Duration::from_secs(15), async {
        loop {
            let page = store.list_conversations(&seeded.id, 50, None).unwrap();
            if page.items.len() >= 3 {
                break;
            }
            sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("engine start must materialize contact/group skeletons");

    let final_page = store.list_conversations(&seeded.id, 50, None).unwrap();
    let mut names: Vec<&str> = final_page
        .items
        .iter()
        .map(|row| row.title.as_str())
        .collect();
    names.sort_unstable();
    // The linked account itself ("note to self") must not become a skeleton;
    // the non-member group must be skipped. Profile names win over the raw
    // contact name in the title composition.
    assert_eq!(names, ["Alice Example", "Bob", "Fixture Group"]);

    supervisor.shutdown().await.unwrap();
}
