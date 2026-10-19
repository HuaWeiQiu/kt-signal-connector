// SPDX-License-Identifier: AGPL-3.0-only

#![cfg(unix)]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use kt_signal_connector::engine::EngineConfig;
use kt_signal_connector::store::{Store, StoreKey};
use kt_signal_connector::supervisor::RuntimeSupervisor;
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

/// A fresh engine start must re-sync contacts/groups for already-linked
/// accounts. Contract 1.201 (§6.5 amendment): that resync materializes only
/// **structurally-owned** chats — a group the account is in, and its own
/// Note-to-Self row — never a direct conversation for a bare contact. A peer
/// becomes a chat when a message flows, not because their profile synced.
/// Since contract 1.43 (§4.42) the linked account's own entry ("note to
/// self") is no longer dropped and its summary marks it `isSelf`.
#[tokio::test]
async fn engine_start_resyncs_structurally_owned_chats_only() {
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
    let mut config = EngineConfig::new(fixture(), temp.path().join("signal-data"));
    config.request_timeout = Duration::from_secs(1);
    config.shutdown_grace = Duration::from_millis(100);
    let supervisor = Arc::new(RuntimeSupervisor::new(
        config,
        Arc::clone(&store),
        kt_signal_connector::DEFAULT_PROXY_GROUP_ID.to_string(),
        None,
    ));

    supervisor.start().await.unwrap();

    // Background pass: settle delay plus read-only fixture calls. Two
    // structurally-owned chats (the fixture group and the self row) is the
    // full expected count under 1.201.
    timeout(Duration::from_secs(15), async {
        loop {
            let page = store.list_conversations(&seeded.id, 50, None).unwrap();
            if page.items.len() >= 2 {
                break;
            }
            sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("engine start must materialize the structurally-owned chats");

    let final_page = store.list_conversations(&seeded.id, 50, None).unwrap();
    let mut names: Vec<&str> = final_page
        .items
        .iter()
        .map(|row| row.title.as_str())
        .collect();
    names.sort_unstable();
    // Contract 1.201: only the group and the self entry materialize. Alice and
    // Bob stay bare contacts (P0 crosstalk fix) — a profile sync must not
    // conjure a chat. The non-member group stays skipped.
    assert_eq!(names, ["Fixture Group", "Test User"]);
    // Exactly the own direct chat is marked; the group is the only other row.
    let self_rows: Vec<_> = final_page.items.iter().filter(|row| row.is_self).collect();
    assert_eq!(self_rows.len(), 1);
    assert_eq!(self_rows[0].kind, "direct");
    assert_eq!(self_rows[0].title, "Test User");
    assert_eq!(
        final_page.items.iter().filter(|row| !row.is_self).count(),
        1
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
    let mut config = EngineConfig::new(fixture(), temp.path().join("signal-data"));
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
