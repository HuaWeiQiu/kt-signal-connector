// SPDX-License-Identifier: AGPL-3.0-only

//! §4.123 outbox drain integration: the background worker drains the
//! automatic send-retry queue against the fake engine. Covers the
//! engine-landed drain hook, the definitive-failure → requeue → auto-retry
//! loop end to end, and the offline engine leaving the queue untouched.

#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use kt_signal_connector::DEFAULT_PROXY_GROUP_ID;
use kt_signal_connector::engine::EngineConfig;
use kt_signal_connector::ids::stable_hash_id;
use kt_signal_connector::link::now_ms;
use kt_signal_connector::service::SendTarget;
use kt_signal_connector::store::{MessageRecord, SendRetrySchedule, Store, StoreKey};
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
    config.request_timeout = Duration::from_secs(1);
    config.shutdown_grace = Duration::from_millis(100);
    config
}

/// Seed the linked fixture account (+15555550100) and one direct peer
/// conversation; returns (account_id, conversation_id).
fn seed_account_and_conversation(store: &Store) -> (String, String) {
    let account = store
        .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
        .unwrap();
    let conversation = store
        .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
        .unwrap();
    (account.id.clone(), conversation.id.clone())
}

/// Seed one outgoing text row through the production settle face: the row is
/// inserted pending with a `client_request_id`, then settled failed through
/// the automatic schedule arm — the exact §4.123 store path a retryable
/// engine error takes — leaving it failed, queued, and due one backoff beat
/// (~190 ms) after the settle stamp.
fn seed_queued_row(store: &Store, account_id: &str, conversation_id: &str, request: &str) {
    let id = stable_hash_id(&[account_id, conversation_id, "outgoing", request]);
    let record = MessageRecord {
        required_protocol_version: None,
        author_aci: None,
        id: id.clone(),
        account_id: account_id.to_string(),
        conversation_id: conversation_id.to_string(),
        direction: "outgoing",
        sender_id: account_id.to_string(),
        sender_name: None,
        mentions_self: false,
        sent_at: now_ms() - 1_000,
        received_at: None,
        text: Some("seeded text".into()),
        text_bytes: None,
        text_truncated: false,
        text_retrievable: true,
        status: "pending",
        client_request_id: None,
        quote_message_id: None,
        quote_snapshot: None,
        quote: None,
        attachments: Vec::new(),
        rich: None,
        edited_at: None,
        reactions: Vec::new(),
        edits: Vec::new(),
        delivered_at: None,
        read_at: None,
        viewed_at: None,
        send_outcomes: None,
        sticker: None,
        expire_seconds: None,
        expire_start_at: None,
        admin_deleted: false,
        server_guid: None,
        server_timestamp: None,
        is_story: false,
        story_message: None,
    };
    store
        .insert_message(&record, Some(request), None, false)
        .unwrap();
    store
        .settle_send_failed(&id, now_ms(), SendRetrySchedule::Backoff, None)
        .unwrap()
        .expect("the seeded pending row settles failed-and-queued");
}

/// The row's status through the public read face; None when no such row.
fn status_of(store: &Store, account_id: &str, request: &str) -> Option<String> {
    store
        .message_by_client_request(account_id, request)
        .unwrap()
        .map(|record| record.status.to_string())
}

fn send_log(temp: &TempDir) -> Vec<serde_json::Value> {
    let path = temp.path().join("signal-data/.fixture-send-log.jsonl");
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

/// The engine-landed drain hook: a row already queued (definitively failed,
/// due now) drains through the worker the moment the engine lands — the
/// official online-event re-dispatch — and settles sent through the
/// ordinary send funnel, exactly one row in the history.
#[tokio::test]
async fn engine_land_drains_a_queued_failed_row() {
    let temp = TempDir::new().unwrap();
    let store = test_store(temp.path());
    let (account_id, conversation_id) = seed_account_and_conversation(&store);
    seed_queued_row(&store, &account_id, &conversation_id, "req-drain-1");

    let supervisor = Arc::new(RuntimeSupervisor::new(
        config_for(&temp),
        Arc::clone(&store),
        DEFAULT_PROXY_GROUP_ID.to_string(),
        None,
    ));
    supervisor.spawn_outbox_worker();
    supervisor.start().await.unwrap();

    timeout(Duration::from_secs(15), async {
        loop {
            if status_of(&store, &account_id, "req-drain-1").as_deref() == Some("sent") {
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the engine-landed hook must drain the due queue");

    let sends = send_log(&temp);
    assert_eq!(sends.len(), 1, "exactly one upstream send");
    assert_eq!(sends[0]["message"], serde_json::json!("seeded text"));
    assert_eq!(
        status_of(&store, &account_id, "req-drain-1").as_deref(),
        Some("sent")
    );
    assert_eq!(
        store.next_retry_due_at().unwrap(),
        None,
        "success retires the queue face"
    );
    let rows = store
        .list_messages(&account_id, &conversation_id, 10, None)
        .unwrap();
    assert_eq!(rows.items.len(), 1, "no second row for the retried send");
}

/// The full loop end to end: the send fails with the definitive
/// server-unavailable error, the funnel enrolls the row into the bounded
/// queue, and the worker's own backoff beat retries it — the same
/// clientRequestId, the same single row, and exactly two upstream send
/// calls (the original + the automatic retry).
#[tokio::test]
async fn definitive_failure_requeues_and_the_auto_retry_succeeds() {
    let temp = TempDir::new().unwrap();
    // Arm the fixture's one-shot server-unavailable answer in THIS test's
    // own data dir (test-local marker, never a process-global env).
    std::fs::create_dir_all(temp.path().join("signal-data")).unwrap();
    std::fs::write(
        temp.path()
            .join("signal-data/.fixture-send-unavailable-armed"),
        b"",
    )
    .unwrap();
    let store = test_store(temp.path());
    let (account_id, conversation_id) = seed_account_and_conversation(&store);

    let supervisor = Arc::new(RuntimeSupervisor::new(
        config_for(&temp),
        Arc::clone(&store),
        DEFAULT_PROXY_GROUP_ID.to_string(),
        None,
    ));
    supervisor.spawn_outbox_worker();
    supervisor.start().await.unwrap();

    // The first send answers the transport error: the API call fails, but
    // the row is settled failed and enrolled for the automatic retry.
    let first = supervisor
        .send_text(
            account_id.clone(),
            SendTarget::Conversation(conversation_id.clone()),
            "retry me once".into(),
            "req-auto-1".into(),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await;
    assert!(
        first.is_err(),
        "the one-shot unavailable answer must fail the initial send"
    );
    assert_eq!(
        status_of(&store, &account_id, "req-auto-1").as_deref(),
        Some("failed")
    );
    assert!(
        store.next_retry_due_at().unwrap().is_some(),
        "the retryable failure must arm the queue"
    );

    // The worker's 190 ms beat retries and the fixture answers normally.
    timeout(Duration::from_secs(15), async {
        loop {
            if status_of(&store, &account_id, "req-auto-1").as_deref() == Some("sent") {
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the automatic retry must settle the row sent");

    let sends = send_log(&temp);
    assert_eq!(sends.len(), 2, "original + automatic retry, nothing more");
    for send in &sends {
        assert_eq!(send["message"], serde_json::json!("retry me once"));
    }
    assert_eq!(
        sends[0]["recipient"], sends[1]["recipient"],
        "the retry addresses the same recipient"
    );
    assert_eq!(
        store.next_retry_due_at().unwrap(),
        None,
        "the settled success leaves nothing queued"
    );
    let rows = store
        .list_messages(&account_id, &conversation_id, 10, None)
        .unwrap();
    assert_eq!(rows.items.len(), 1, "one row per logical message");
    assert_eq!(
        rows.items[0].client_request_id.as_deref(),
        Some("req-auto-1")
    );
}

/// Contract 1.143 (§4.139): the rate-limit family joins the queue, but on
/// the Retry-After beat, not the curve — the engine's structured
/// rate-limit answer (the §4.80 anchor "(RateLimitExceeded)") settles the
/// row failed-and-enrolled at the official one-minute default (this wire
/// carries no Retry-After value, so the fallback IS the beat), decisively
/// beyond any curve beat. The row keeps its single identity and waits its
/// pause; nothing hammers.
#[tokio::test]
async fn rate_limited_failure_enrolls_at_the_retry_after_beat() {
    let temp = TempDir::new().unwrap();
    // Arm the fixture's one-shot rate-limit answer in THIS test's own data
    // dir (test-local marker, never a process-global env).
    std::fs::create_dir_all(temp.path().join("signal-data")).unwrap();
    std::fs::write(
        temp.path()
            .join("signal-data/.fixture-send-ratelimit-armed"),
        b"",
    )
    .unwrap();
    let store = test_store(temp.path());
    let (account_id, conversation_id) = seed_account_and_conversation(&store);

    let supervisor = Arc::new(RuntimeSupervisor::new(
        config_for(&temp),
        Arc::clone(&store),
        DEFAULT_PROXY_GROUP_ID.to_string(),
        None,
    ));
    supervisor.spawn_outbox_worker();
    supervisor.start().await.unwrap();

    let before = now_ms();
    let first = supervisor
        .send_text(
            account_id.clone(),
            SendTarget::Conversation(conversation_id.clone()),
            "slow down".into(),
            "req-ratelimit-1".into(),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await;
    assert!(
        first.is_err(),
        "the one-shot rate-limit answer must fail the send"
    );
    assert_eq!(
        status_of(&store, &account_id, "req-ratelimit-1").as_deref(),
        Some("failed")
    );
    let due = store
        .next_retry_due_at()
        .unwrap()
        .expect("the rate-limit failure must arm the queue");
    assert!(
        due >= before + kt_signal_connector::store::RATE_LIMIT_RETRY_AFTER_MS - 1_000
            && due <= now_ms() + kt_signal_connector::store::RATE_LIMIT_RETRY_AFTER_MS + 1_000,
        "the beat is the Retry-After default, not the curve: {due}"
    );
    let rows = store
        .list_messages(&account_id, &conversation_id, 10, None)
        .unwrap();
    assert_eq!(rows.items.len(), 1, "one row per logical message");
}

/// Offline is not failure: with no engine running, the drain pass claims
/// nothing — the queued row keeps its status and its schedule and waits for
/// recovery (the engine-landed hook or the tick).
#[tokio::test]
async fn offline_engine_leaves_the_retry_queue_untouched() {
    let temp = TempDir::new().unwrap();
    let store = test_store(temp.path());
    let (account_id, conversation_id) = seed_account_and_conversation(&store);
    seed_queued_row(&store, &account_id, &conversation_id, "req-offline-1");
    let queued_due = store
        .next_retry_due_at()
        .unwrap()
        .expect("the seeded row stands queued");

    let supervisor = Arc::new(RuntimeSupervisor::new(
        config_for(&temp),
        Arc::clone(&store),
        DEFAULT_PROXY_GROUP_ID.to_string(),
        None,
    ));
    supervisor.spawn_outbox_worker();
    // No start(): the engine stays down. The worker's first pass runs
    // immediately at spawn and must abort before any claim.
    sleep(Duration::from_millis(1_500)).await;

    assert_eq!(
        status_of(&store, &account_id, "req-offline-1").as_deref(),
        Some("failed"),
        "an offline engine never fails the row further"
    );
    assert_eq!(
        store.next_retry_due_at().unwrap(),
        Some(queued_due),
        "the schedule stands untouched"
    );
    assert!(send_log(&temp).is_empty(), "no send ever left for upstream");
    let _ = conversation_id;
}
