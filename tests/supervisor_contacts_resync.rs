// SPDX-License-Identifier: AGPL-3.0-only

#![cfg(unix)]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use kt_signal_connector::engine::{SignalCliConfig, SignalCliMode};
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
/// link-time inline sync failed or predates the skeleton revision. Since
/// contract 1.43 (§4.42) the linked account's own entry ("note to self") is
/// no longer dropped: its skeleton materializes like any contact's and the
/// summary marks it `isSelf`.
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
            if page.items.len() >= 4 {
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
    // Contract 1.43: the self entry ("Test User", the account's own profile
    // name — raw material, the desktop localizes the title) joins the
    // skeletons; the non-member group stays skipped. Profile names win over
    // the raw contact name in the title composition.
    assert_eq!(
        names,
        ["Alice Example", "Bob", "Fixture Group", "Test User"]
    );
    // Exactly the own direct chat is marked; the three peer skeletons are not.
    let self_rows: Vec<_> = final_page.items.iter().filter(|row| row.is_self).collect();
    assert_eq!(self_rows.len(), 1);
    assert_eq!(self_rows[0].kind, "direct");
    assert_eq!(self_rows[0].title, "Test User");
    assert_eq!(
        final_page.items.iter().filter(|row| !row.is_self).count(),
        3
    );

    supervisor.shutdown().await.unwrap();
}

/// The engine-mode face runs the same sync loop against the same engine RPC
/// names, so the self skeleton must materialize identically there (contract
/// 1.43 shape normalization across engine modes). The hermetic fixture serves
/// both modes; the real kt-signal-engine's own `listContacts` self-inclusion
/// stays an engine-repository fact (recorded boundary).
#[tokio::test]
async fn engine_mode_resync_materializes_the_self_skeleton_too() {
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
    config.mode = SignalCliMode::KtEngine;
    config.request_timeout = Duration::from_secs(1);
    config.shutdown_grace = Duration::from_millis(100);
    let supervisor = Arc::new(RuntimeSupervisor::new(
        config,
        Arc::clone(&store),
        kt_signal_connector::DEFAULT_PROXY_GROUP_ID.to_string(),
        None,
    ));

    supervisor.start().await.unwrap();

    timeout(Duration::from_secs(15), async {
        loop {
            let page = store.list_conversations(&seeded.id, 50, None).unwrap();
            if page.items.iter().any(|row| row.is_self) {
                break;
            }
            sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("engine-mode sync must materialize the self skeleton");

    let final_page = store.list_conversations(&seeded.id, 50, None).unwrap();
    let self_rows: Vec<_> = final_page.items.iter().filter(|row| row.is_self).collect();
    assert_eq!(self_rows.len(), 1);
    assert_eq!(self_rows[0].kind, "direct");
    assert_eq!(self_rows[0].title, "Test User");

    supervisor.shutdown().await.unwrap();
}
