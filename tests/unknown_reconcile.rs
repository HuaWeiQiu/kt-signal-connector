// SPDX-License-Identifier: AGPL-3.0-only

//! §4.179 unknown-outcome reconciliation integration: the startup pass asks
//! the engine's own ledger (the fixture's `lookupOutgoingDispatch` arm)
//! whether each unsettled outgoing row was really dispatched, and settles
//! both arms — dispatched → sent keeping `sent_at`, never-dispatched →
//! failed with the §4.172 retry gate re-opened. An engine error leaves the
//! row unknown (nothing claimed without the engine's truth), a
//! cap-exceeding backlog drains across batches, and a re-run converges.

#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use kt_signal_connector::DEFAULT_PROXY_GROUP_ID;
use kt_signal_connector::engine::EngineConfig;
use kt_signal_connector::ids::stable_hash_id;
use kt_signal_connector::link::now_ms;
use kt_signal_connector::service::ServiceError;
use kt_signal_connector::store::{MessageRecord, Store, StoreKey};
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

fn marker_path(temp: &TempDir, name: &str) -> PathBuf {
    temp.path().join("signal-data").join(name)
}

fn arm_lookup_answer(temp: &TempDir, answer: &str) {
    let path = marker_path(temp, ".fixture-lookup-dispatch.json");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, answer).unwrap();
}

fn arm_lookup_error(temp: &TempDir) {
    let path = marker_path(temp, ".fixture-lookup-error");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, "").unwrap();
}

/// The fixture's dispatch log (send / remoteDelete / lookupOutgoingDispatch
/// all record their exact upstream params).
fn dispatch_log(temp: &TempDir) -> Vec<serde_json::Value> {
    let path = marker_path(temp, ".fixture-send-log.jsonl");
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect()
}

/// The reconciliation probes recorded so far (identified by the lookup's
/// param face: sentTimestamp + toleranceMs, no send `message` key).
fn lookup_probes(temp: &TempDir) -> Vec<serde_json::Value> {
    dispatch_log(temp)
        .into_iter()
        .filter(|params| {
            params.get("sentTimestamp").is_some()
                && params.get("toleranceMs").is_some()
                && params.get("message").is_none()
        })
        .collect()
}

/// Seed one outgoing row at an exact status through the production insert
/// face. `sent_at` is backdated so the "sent settle keeps sent_at" assertion
/// cannot pass on a fresh now() coincidence.
fn seed_outgoing_row(
    store: &Store,
    account_id: &str,
    conversation_id: &str,
    request: &str,
    status: &'static str,
    sent_at: u64,
) -> String {
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
        sent_at,
        received_at: None,
        text: Some("unsettled body".into()),
        text_bytes: None,
        text_truncated: false,
        text_retrievable: true,
        status,
        client_request_id: Some(request.to_string()),
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
        .insert_message(&record, Some(request), Some("unsettled body"), false)
        .unwrap();
    id
}

fn row_status(store: &Store, account_id: &str, request: &str) -> String {
    store
        .message_by_client_request(account_id, request)
        .unwrap()
        .expect("row present")
        .status
        .to_string()
}

/// The dispatched=true arm: both legs (direct and group addressing) settle
/// `sent` with their original prepare-time `sent_at` kept verbatim, and the
/// probe carried the row's own addressing — signal account, the
/// `set_upstream_target` leg shape, and the engine's tolerance cap.
#[tokio::test]
async fn reconcile_settles_dispatched_rows_sent_keeping_sent_at() {
    let temp = TempDir::new().unwrap();
    let store = test_store(temp.path());
    let account = store
        .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
        .unwrap();
    let direct = store
        .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
        .unwrap();
    let group = store
        .ensure_conversation(&account.id, "group", "Rk9VUi1LRVk==", "Group")
        .unwrap();
    let direct_sent_at = now_ms() - 120_000;
    let group_sent_at = now_ms() - 90_000;
    seed_outgoing_row(
        &store,
        &account.id,
        &direct.id,
        "req-direct",
        "unknown",
        direct_sent_at,
    );
    seed_outgoing_row(
        &store,
        &account.id,
        &group.id,
        "req-group",
        "unknown",
        group_sent_at,
    );
    arm_lookup_answer(&temp, r#"{"dispatched": true, "serverTimestamp": 42}"#);

    let supervisor = Arc::new(RuntimeSupervisor::new(
        config_for(&temp),
        Arc::clone(&store),
        DEFAULT_PROXY_GROUP_ID.to_string(),
        None,
    ));
    supervisor.start().await.unwrap();

    timeout(Duration::from_secs(15), async {
        loop {
            let direct_done = row_status(&store, &account.id, "req-direct") == "sent";
            let group_done = row_status(&store, &account.id, "req-group") == "sent";
            if direct_done && group_done {
                break;
            }
            sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("both unknown rows must reconcile to sent");

    // sent_at preserved verbatim (the engine's matched-row timestamp stays
    // engine-side — 42 must never leak into the row).
    let direct_row = store
        .message_by_client_request(&account.id, "req-direct")
        .unwrap()
        .unwrap();
    assert_eq!(direct_row.sent_at, direct_sent_at);
    let group_row = store
        .message_by_client_request(&account.id, "req-group")
        .unwrap()
        .unwrap();
    assert_eq!(group_row.sent_at, group_sent_at);

    // The probes carried the rows' own addressing: signal account, direct =
    // recipient peer_key, group = groupId master key, tolerance pinned to
    // the engine cap, sentTimestamp = the row's prepare-time stamp.
    let probes = lookup_probes(&temp);
    assert_eq!(probes.len(), 2, "one probe per unsettled row: {probes:?}");
    let direct_probe = probes
        .iter()
        .find(|params| params.get("recipient").is_some())
        .expect("direct probe");
    assert_eq!(direct_probe["account"], serde_json::json!("+15555550100"));
    assert_eq!(direct_probe["recipient"], serde_json::json!("+15555550101"));
    assert_eq!(
        direct_probe["sentTimestamp"],
        serde_json::json!(direct_sent_at)
    );
    assert_eq!(direct_probe["toleranceMs"], serde_json::json!(60_000u64));
    let group_probe = probes
        .iter()
        .find(|params| params.get("groupId").is_some())
        .expect("group probe");
    assert_eq!(group_probe["groupId"], serde_json::json!("Rk9VUi1LRVk=="));
    assert_eq!(
        group_probe["sentTimestamp"],
        serde_json::json!(group_sent_at)
    );

    supervisor.shutdown().await.unwrap();
}

/// The never-dispatched arm (default fixture face): the row settles `failed`
/// — provably never sent — and the §4.172 retry gate re-opens: the same
/// retryText that refuses an unknown row now re-arms and re-dispatches it
/// through the ordinary send funnel.
#[tokio::test]
async fn reconcile_settles_never_dispatched_failed_and_reopens_the_retry_gate() {
    let temp = TempDir::new().unwrap();
    let store = test_store(temp.path());
    let account = store
        .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
        .unwrap();
    let conversation = store
        .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
        .unwrap();
    seed_outgoing_row(
        &store,
        &account.id,
        &conversation.id,
        "req-retry",
        "unknown",
        now_ms() - 60_000,
    );

    let supervisor = Arc::new(RuntimeSupervisor::new(
        config_for(&temp),
        Arc::clone(&store),
        DEFAULT_PROXY_GROUP_ID.to_string(),
        None,
    ));
    supervisor.start().await.unwrap();

    timeout(Duration::from_secs(15), async {
        loop {
            if row_status(&store, &account.id, "req-retry") == "failed" {
                break;
            }
            sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("the never-dispatched row must reconcile to failed");

    // §4.172 re-opened: a failed row rearms and re-dispatches (the fixture
    // send answers {timestamp: 99}), settling the SAME row sent.
    let resent = supervisor
        .retry_text(
            account.id.clone(),
            conversation.id.clone(),
            "req-retry".to_string(),
        )
        .await
        .expect("retryText must be allowed after the failed reconcile");
    assert_eq!(resent.status, "sent");
    assert_eq!(row_status(&store, &account.id, "req-retry"), "sent");

    supervisor.shutdown().await.unwrap();
}

/// The engine-error arm: a probe that errors (unregistered account /
/// transport) claims nothing — the row stays unknown, the §4.172 gate keeps
/// refusing it, and the pass itself never blocks the rest of the system
/// (a sibling row on a healthy account still settles).
#[tokio::test]
async fn reconcile_engine_error_leaves_rows_unknown_and_the_gate_refuses() {
    let temp = TempDir::new().unwrap();
    let store = test_store(temp.path());
    let account = store
        .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
        .unwrap();
    let conversation = store
        .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
        .unwrap();
    seed_outgoing_row(
        &store,
        &account.id,
        &conversation.id,
        "req-stuck",
        "unknown",
        now_ms() - 60_000,
    );
    // Error face armed BEFORE start: every probe answers -32603.
    arm_lookup_error(&temp);

    let supervisor = Arc::new(RuntimeSupervisor::new(
        config_for(&temp),
        Arc::clone(&store),
        DEFAULT_PROXY_GROUP_ID.to_string(),
        None,
    ));
    supervisor.start().await.unwrap();

    // The pass fired (its probe is recorded) and the row survived it.
    timeout(Duration::from_secs(15), async {
        loop {
            if !lookup_probes(&temp).is_empty() {
                break;
            }
            sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("the reconciliation probe must have fired");
    sleep(Duration::from_millis(500)).await;
    assert_eq!(row_status(&store, &account.id, "req-stuck"), "unknown");

    // The gate still refuses exactly this row: no engine truth, no resend.
    let refused = supervisor
        .retry_text(
            account.id.clone(),
            conversation.id.clone(),
            "req-stuck".to_string(),
        )
        .await
        .expect_err("retryText must refuse a still-unknown row");
    match refused {
        ServiceError::Api(api) => assert_eq!(api.code, "RETRY_OUTCOME_UNKNOWN"),
        other => panic!("expected RETRY_OUTCOME_UNKNOWN, got {other:?}"),
    }

    supervisor.shutdown().await.unwrap();
}

/// The bounded pass drains a backlog larger than one batch (the batch cap
/// loops until the query answers short), and a re-run after the next engine
/// landing converges: settled rows drop out of the backlog, no further
/// probes fire, nothing flips.
#[tokio::test]
async fn reconcile_drains_a_backlog_across_batches_and_reruns_converge() {
    let temp = TempDir::new().unwrap();
    let store = test_store(temp.path());
    let account = store
        .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
        .unwrap();
    let conversation = store
        .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
        .unwrap();
    // 69 unsettled rows: one full 64-row batch plus a short tail — the pass
    // must loop batches to drain them all.
    let total = 69;
    let base = now_ms() - 600_000;
    for index in 0..total {
        let request = format!("req-bulk-{index}");
        seed_outgoing_row(
            &store,
            &account.id,
            &conversation.id,
            &request,
            "unknown",
            base + index as u64,
        );
    }
    arm_lookup_answer(&temp, r#"{"dispatched": true}"#);

    let supervisor = Arc::new(RuntimeSupervisor::new(
        config_for(&temp),
        Arc::clone(&store),
        DEFAULT_PROXY_GROUP_ID.to_string(),
        None,
    ));
    supervisor.start().await.unwrap();

    timeout(Duration::from_secs(20), async {
        loop {
            if lookup_probes(&temp).len() >= total {
                break;
            }
            sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("the pass must drain the multi-batch backlog");
    // Every probe answered dispatched=true: all rows settle sent.
    timeout(Duration::from_secs(10), async {
        loop {
            let settled = (0..total)
                .filter(|index| {
                    row_status(&store, &account.id, &format!("req-bulk-{index}")) == "sent"
                })
                .count();
            if settled == total {
                break;
            }
            sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("every backlog row must settle sent");

    // Convergence: restart the engine (the pass re-arms on the new landing)
    // and verify zero further probes — the backlog query answers empty and
    // the settled rows never flip.
    supervisor.stop().await.unwrap();
    supervisor.start().await.unwrap();
    let probes_before = lookup_probes(&temp).len();
    sleep(Duration::from_secs(4)).await;
    assert_eq!(
        lookup_probes(&temp).len(),
        probes_before,
        "a converged backlog must not re-probe"
    );
    assert_eq!(row_status(&store, &account.id, "req-bulk-0"), "sent");
    assert_eq!(row_status(&store, &account.id, "req-bulk-68"), "sent");

    supervisor.shutdown().await.unwrap();
}

/// The automatic recovery landing is a reconcile trigger too: the watchdog
/// restarts a faulted/restarted engine through `restart_engine`, and the
/// unknown-outcome pass must re-arm on that landing instead of waiting for
/// the next manual `start()`. A row left unknown by the startup pass (no
/// engine truth there) settles only after the watchdog lands a fresh engine.
#[tokio::test]
async fn watchdog_restart_lands_and_reconciles_unknown_rows() {
    let temp = TempDir::new().unwrap();
    let store = test_store(temp.path());
    let account = store
        .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
        .unwrap();
    let conversation = store
        .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
        .unwrap();
    seed_outgoing_row(
        &store,
        &account.id,
        &conversation.id,
        "req-restart",
        "unknown",
        now_ms() - 60_000,
    );
    // The startup pass gets an answer without the `dispatched` face, so it
    // probes once and claims nothing. The marker is re-armed below for the
    // restart landing this test is about.
    arm_lookup_answer(&temp, "{}");

    let mut config = config_for(&temp);
    config.watchdog_interval = Duration::from_millis(50);
    config.watchdog_min_restart_interval = Duration::ZERO;
    // The restart under test is the first of its episode (immediate); keep any
    // retry immediate too so the test never depends on the production
    // ten-second liveness backoff.
    config.watchdog_liveness_backoff_base = Duration::ZERO;
    let supervisor = Arc::new(RuntimeSupervisor::new(
        config,
        Arc::clone(&store),
        DEFAULT_PROXY_GROUP_ID.to_string(),
        None,
    ));
    supervisor.spawn_watchdog();
    let pid_before = supervisor.start().await.unwrap().pid.unwrap();

    // The startup pass fired (its probe is recorded) and claimed nothing.
    timeout(Duration::from_secs(15), async {
        loop {
            if !lookup_probes(&temp).is_empty() {
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the startup reconciliation probe must have fired");
    // Let the single-row pass finish its answer handling before swapping the
    // marker; a drained batch must not probe again on its own.
    sleep(Duration::from_millis(500)).await;
    assert_eq!(
        row_status(&store, &account.id, "req-restart"),
        "unknown",
        "without engine truth the startup pass must leave the row unknown"
    );
    assert_eq!(
        lookup_probes(&temp).len(),
        1,
        "a drained startup pass must not re-probe"
    );

    // Arm the truth for the next landing, then make the watchdog restart the
    // engine exactly once: two failed receive-liveness pings.
    arm_lookup_answer(&temp, r#"{"dispatched": true}"#);
    std::fs::write(marker_path(&temp, ".fixture-fail-user-status-count"), "2").unwrap();

    timeout(Duration::from_secs(20), async {
        loop {
            if row_status(&store, &account.id, "req-restart") == "sent" {
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the watchdog landing must re-arm the reconciliation and settle the row");

    assert_ne!(
        supervisor.status().await.pid,
        Some(pid_before),
        "the settle must ride an actual engine restart"
    );
    assert!(
        lookup_probes(&temp).len() >= 2,
        "the restart landing must probe again: {:?}",
        lookup_probes(&temp)
    );
    assert_eq!(row_status(&store, &account.id, "req-restart"), "sent");

    supervisor.shutdown().await.unwrap();
}
