// SPDX-License-Identifier: AGPL-3.0-only

//! §4.167 group-history resync integration: a `groups.get` cache miss
//! triggers the throttled single-group refresh, backfills the cache row, and
//! replays the read; the throttle holds within the TTL after a success; a
//! definitive engine negative marks the §4.50 appendix terminated face and
//! stops re-pulling; and the boot-window degrade answers the untouched
//! `GROUP_NOT_FOUND` face and self-heals once the engine lands — the
//! incident shape the batch was cut for (2026-10-16, the "gggg" group).

#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use kt_signal_connector::DEFAULT_PROXY_GROUP_ID;
use kt_signal_connector::engine::EngineConfig;
use kt_signal_connector::service::GroupDetails;
use kt_signal_connector::store::{Store, StoreKey, SyncedContact};
use kt_signal_connector::supervisor::RuntimeSupervisor;
use tempfile::TempDir;
use tokio::time::{sleep, timeout};

fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fake-engine.py")
}

/// All supervisor-test stores are encrypted with one fixed key, so a seeded
/// store and the supervisor's own open see the same database.
fn test_store(dir: &Path) -> Arc<Store> {
    Arc::new(Store::open(dir, Some(StoreKey::from_bytes([0x5A; 32]))).unwrap())
}

fn config_for(temp: &TempDir) -> EngineConfig {
    let mut config = EngineConfig::new(fixture(), temp.path().join("signal-data"));
    config.request_timeout = Duration::from_secs(3);
    config.shutdown_grace = Duration::from_millis(100);
    config
}

/// Seed the linked fixture account (+15555550100); returns the account id.
fn seed_account(store: &Store) -> String {
    store
        .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
        .unwrap()
        .id
}

/// Arm the fixture's empty-listing answer in THIS test's own data dir: every
/// `listGroups` answers `[]` — the engine knows no groups — so the
/// definitive-negative arm (the group absent from a successful single-group
/// listing) is exercisable.
fn arm_empty_listing(temp: &TempDir) {
    std::fs::create_dir_all(temp.path().join("signal-data")).unwrap();
    std::fs::write(temp.path().join("signal-data/.fixture-groups-empty"), b"").unwrap();
}

fn group_row(store: &Store, account_id: &str, group_key: &str) -> Option<String> {
    store
        .contact_by_peer(account_id, "group", group_key)
        .unwrap()
        .and_then(|(_, extra, _)| extra)
}

/// The group roster read through the resync face; polls past the boot window
/// (each failed refresh clears its stamp, so every beat retries immediately).
async fn wait_for_group_details(
    supervisor: &RuntimeSupervisor,
    account_id: &str,
    group_key: &str,
) -> GroupDetails {
    timeout(Duration::from_secs(15), async {
        loop {
            if let Ok(details) = supervisor
                .get_group(account_id.to_string(), group_key.to_string())
                .await
            {
                return details;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the resync must land once the engine answers")
}

/// Overwrite the group's cache row sparse (no extra: no memberCount, no
/// roster) through the legitimate passive upsert — the next read must see an
/// empty roster and trigger the resync gate again.
fn make_roster_sparse(store: &Store, account_id: &str, group_key: &str) {
    let entries = [SyncedContact {
        kind: "group",
        peer_key: group_key,
        title: "Fixture Group",
        extra: None,
    }];
    store
        .upsert_passive_contacts(account_id, &entries, 1)
        .unwrap();
}

/// Audit core: the cache miss resyncs, backfills, and replays the read. The
/// account is seeded only after the startup resync window has fully passed,
/// so nothing else could have materialized the group row — the refresh behind
/// the read is the only possible writer.
#[tokio::test]
async fn group_cache_miss_resyncs_replays_and_backfills_the_roster() {
    let temp = TempDir::new().unwrap();
    let store = test_store(temp.path());
    let supervisor = Arc::new(RuntimeSupervisor::new(
        config_for(&temp),
        Arc::clone(&store),
        DEFAULT_PROXY_GROUP_ID.to_string(),
        None,
    ));
    supervisor.start().await.unwrap();
    // The startup resync pass settles (and finds no accounts); the fixture
    // group was never synced.
    sleep(Duration::from_millis(2500)).await;
    let account_id = seed_account(&store);
    assert!(
        group_row(&store, &account_id, "ZmFrZS1ncm91cC0x").is_none(),
        "precondition: the group cache row is missing"
    );

    let details = wait_for_group_details(&supervisor, &account_id, "ZmFrZS1ncm91cC0x").await;
    assert_eq!(details.title, "Fixture Group");
    assert_eq!(details.member_count, Some(2));
    let members = details
        .members
        .expect("the refreshed roster rides the read");
    assert_eq!(members.len(), 2);
    assert!(members.iter().any(|member| member.is_self));

    // The cache row is backfilled with the bounded roster projection, and a
    // second read answers the same face from the cache.
    let extra = group_row(&store, &account_id, "ZmFrZS1ncm91cC0x")
        .expect("the refresh backfilled the contacts row");
    let parsed: serde_json::Value = serde_json::from_str(&extra).unwrap();
    assert_eq!(parsed["memberCount"], 2);
    let again = supervisor
        .get_group(account_id.clone(), "ZmFrZS1ncm91cC0x".to_string())
        .await
        .unwrap();
    assert_eq!(again.member_count, Some(2));
    supervisor.shutdown().await.unwrap();
}

/// The throttle: a successful refresh stamps the (account, group) key, and a
/// read inside the five-minute TTL answers the cache face without a second
/// refresh — the backfilled row removed and the engine switched to an empty
/// listing, the suppressed read must answer the sparse non-terminated row,
/// not a refresh outcome.
#[tokio::test]
async fn group_resync_throttles_within_the_ttl_after_success() {
    let temp = TempDir::new().unwrap();
    let store = test_store(temp.path());
    let supervisor = Arc::new(RuntimeSupervisor::new(
        config_for(&temp),
        Arc::clone(&store),
        DEFAULT_PROXY_GROUP_ID.to_string(),
        None,
    ));
    supervisor.start().await.unwrap();
    sleep(Duration::from_millis(2500)).await;
    let account_id = seed_account(&store);

    // First read: cache miss, refresh lands, the stamp is recorded.
    let details = wait_for_group_details(&supervisor, &account_id, "ZmFrZS1ncm91cC0x").await;
    assert_eq!(details.member_count, Some(2));

    // Inside the TTL: make the roster empty and switch the engine to an
    // empty listing — a refresh would either refill the roster or mark the
    // row terminated; suppression must answer the sparse row untouched.
    arm_empty_listing(&temp);
    make_roster_sparse(&store, &account_id, "ZmFrZS1ncm91cC0x");
    let suppressed = supervisor
        .get_group(account_id.clone(), "ZmFrZS1ncm91cC0x".to_string())
        .await
        .unwrap();
    assert_eq!(
        suppressed.member_count, None,
        "the sparse cache row answers untouched inside the TTL"
    );
    assert!(
        !suppressed.is_terminated,
        "no refresh ran inside the TTL, so no negative was stamped"
    );
    supervisor.shutdown().await.unwrap();
}

/// The definitive negative: the engine answered the listing and the group is
/// absent from it (`isMember=false` on the fixture's former-group entry, or
/// missing from an empty listing) — the §4.50 appendix terminated face is
/// written onto the group's existing cache row (nothing materializes for a
/// key no conversation answers), the read carries `isTerminated`, and the
/// next read answers the terminated face without a refresh.
#[tokio::test]
async fn group_true_negative_marks_terminated_and_stops_repulling() {
    let temp = TempDir::new().unwrap();
    let store = test_store(temp.path());
    let supervisor = Arc::new(RuntimeSupervisor::new(
        config_for(&temp),
        Arc::clone(&store),
        DEFAULT_PROXY_GROUP_ID.to_string(),
        None,
    ));
    supervisor.start().await.unwrap();
    sleep(Duration::from_millis(2500)).await;
    let account_id = seed_account(&store);

    // Leg 1 — present but not a member: the conversation exists (the user's
    // own), the engine says isMember=false.
    store
        .ensure_conversation(&account_id, "group", "bm90LWEtbWVtYmVy", "Former Group")
        .unwrap();
    let details = wait_for_group_details(&supervisor, &account_id, "bm90LWEtbWVtYmVy").await;
    assert!(details.is_terminated, "isMember=false folds to terminated");
    let extra = group_row(&store, &account_id, "bm90LWEtbWVtYmVy")
        .expect("the negative stamp persisted on the cache row");
    let parsed: serde_json::Value = serde_json::from_str(&extra).unwrap();
    assert_eq!(parsed["isTerminated"], true);

    // Leg 2 — absent from a successful listing: the empty-listing marker.
    arm_empty_listing(&temp);
    store
        .ensure_conversation(&account_id, "group", "c3VjaC1ncm91cC1pcy1nb25l", "Ghost")
        .unwrap();
    let ghost = wait_for_group_details(&supervisor, &account_id, "c3VjaC1ncm91cC1pcy1nb25l").await;
    assert!(
        ghost.is_terminated,
        "absent from the listing folds the same"
    );

    // The terminated row never re-pulls: the read answers the terminated
    // face from the cache.
    let again = supervisor
        .get_group(account_id.clone(), "bm90LWEtbWVtYmVy".to_string())
        .await
        .unwrap();
    assert!(again.is_terminated);
    supervisor.shutdown().await.unwrap();
}

/// The boot-window shape (the incident): with no engine the read answers the
/// untouched `GROUP_NOT_FOUND` face — the exact pre-1.167 degrade — and once
/// the engine lands the same read self-heals with no manual `contacts.sync`
/// (a failed refresh never leaves a stamp behind).
#[tokio::test]
async fn group_boot_window_degrades_then_self_heals_once_the_engine_lands() {
    let temp = TempDir::new().unwrap();
    let store = test_store(temp.path());
    let supervisor = Arc::new(RuntimeSupervisor::new(
        config_for(&temp),
        Arc::clone(&store),
        DEFAULT_PROXY_GROUP_ID.to_string(),
        None,
    ));
    let account_id = seed_account(&store);
    let before = supervisor
        .get_group(account_id.clone(), "ZmFrZS1ncm91cC0x".to_string())
        .await
        .expect_err("no engine: the cache miss answers the degrade face")
        .into_api();
    assert_eq!(before.code, "GROUP_NOT_FOUND");
    assert!(!before.retryable, "{before:?}");
    assert!(
        group_row(&store, &account_id, "ZmFrZS1ncm91cC0x").is_none(),
        "a failed refresh writes nothing"
    );

    supervisor.start().await.unwrap();
    let healed = wait_for_group_details(&supervisor, &account_id, "ZmFrZS1ncm91cC0x").await;
    assert_eq!(healed.title, "Fixture Group");
    assert_eq!(healed.member_count, Some(2));
    supervisor.shutdown().await.unwrap();
}
