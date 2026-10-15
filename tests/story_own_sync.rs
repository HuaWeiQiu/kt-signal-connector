// SPDX-License-Identifier: AGPL-3.0-only

#![cfg(unix)]

//! The own-story multi-device transcript end to end (contract revision
//! 1.160, §4.158): the engine's `syncMessage.sentMessage.storyMessage`
//! projection rides the receive lane into the outgoing `message.upserted`
//! row face — the two additive story keys on the record (`isStory` +
//! `storyMessage`, the face verbatim) and no store row (the §4.67 story
//! posture). The pre-1.160 shapes stay byte-identical: an ordinary sent
//! transcript keeps landing its stored row without either story key, and
//! an oversized story face keeps the silent skip (宁缺勿错). The fixture
//! plays the engine (the emit-marker discipline); these tests pin the
//! connector's projection fidelity, never real engine sync behavior (the
//! real multi-device fan-out is the engine batch's own acceptance).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use kt_signal_connector::engine::EngineConfig;
use kt_signal_connector::ids::stable_hash_id;
use kt_signal_connector::service::HostSideEvent;
use kt_signal_connector::store::{MessageRecord, Store, StoreKey};
use kt_signal_connector::supervisor::RuntimeSupervisor;
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::sync::broadcast::Receiver;
use tokio::time::{sleep, timeout};

fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fake-engine.py")
}

const SIGNAL_ACCOUNT: &str = "+15555550100";

/// The store handle the assertions read through — a second connection on
/// the same file (the supervisor owns its own), WAL-legal, public API only.
fn test_store(dir: &Path) -> Arc<Store> {
    Arc::new(Store::open(dir, Some(StoreKey::from_bytes([0x5A; 32]))).unwrap())
}

async fn story_sync_supervisor(
    temp: &TempDir,
) -> (
    Arc<RuntimeSupervisor>,
    Arc<Store>,
    String,
    Receiver<HostSideEvent>,
) {
    let store = test_store(temp.path());
    let mut config = EngineConfig::new(fixture(), temp.path().join("signal-data"));
    config.request_timeout = Duration::from_secs(5);
    config.shutdown_grace = Duration::from_millis(100);
    let supervisor = Arc::new(RuntimeSupervisor::new(
        config,
        Arc::clone(&store),
        kt_signal_connector::DEFAULT_PROXY_GROUP_ID.to_string(),
        None,
    ));
    let mut host_lane = supervisor.subscribe_host();
    supervisor.start().await.unwrap();
    let account = store
        .upsert_account_from_signal(
            SIGNAL_ACCOUNT,
            Some(1),
            kt_signal_connector::DEFAULT_PROXY_GROUP_ID,
        )
        .unwrap();
    // Let the link-burst receives settle, then flush the lane so the
    // snapshots below see a quiet world.
    sleep(Duration::from_millis(500)).await;
    while host_lane.try_recv().is_ok() {}
    (supervisor, store, account.id, host_lane)
}

/// The bounded story face verbatim (the media locator's full face): the
/// same face the lib pins ride.
fn story_face() -> Value {
    json!({
        "text": "story body",
        "allowsReplies": true,
        "fileAttachment": {
            "id": "att-story-1",
            "contentType": "image/jpeg",
            "size": 12345,
            "key": "a2V5",
            "cdnKey": "Y2RuLWtleQ==",
            "cdnNumber": 3,
            "uploadTimestamp": 1726999996000u64,
            "digest": "ZGlnZXN0",
            "plaintextHash": "cGxhaW50ZXh0aGFzaA==",
        },
    })
}

fn emit_envelopes(signal_data: &Path, envelopes: Value) {
    std::fs::write(
        signal_data.join(".fixture-emit-envelopes.json"),
        json!({ "envelopes": envelopes }).to_string(),
    )
    .unwrap();
}

/// The store's whole shape for the account: (conversation count, stored
/// row count) — the delta the no-row assertions compare against.
fn store_snapshot(store: &Store, account_id: &str) -> (usize, usize) {
    let conversations = store.list_conversations(account_id, 50, None).unwrap();
    let rows = conversations
        .items
        .iter()
        .map(|conversation| {
            store
                .list_messages(account_id, &conversation.id, 50, None)
                .unwrap()
                .items
                .len()
        })
        .sum();
    (conversations.items.len(), rows)
}

/// The next story row face on the host lane (the transcript event the
/// desktop's story intercept consumes).
async fn next_story_event(host_lane: &mut Receiver<HostSideEvent>) -> MessageRecord {
    loop {
        let event = timeout(Duration::from_secs(5), host_lane.recv())
            .await
            .expect("the story transcript event within timeout")
            .unwrap();
        if let HostSideEvent::MessageUpserted(record) = event
            && record.is_story
        {
            return record;
        }
    }
}

/// The transcript rides the upserted row face byte for byte (the 1:1 face
/// and the group face alike) and the store stays untouched: no row, no
/// conversation — the row face is event payload only.
#[tokio::test]
async fn own_story_sync_rides_the_upserted_row_face_verbatim_without_a_row() {
    let temp = TempDir::new().unwrap();
    let signal_data = temp.path().join("signal-data");
    let (supervisor, store, account_id, mut host_lane) = story_sync_supervisor(&temp).await;
    let before = store_snapshot(&store, &account_id);

    let face = story_face();
    emit_envelopes(
        &signal_data,
        json!([
            {
                "timestamp": 1727000000000u64,
                "syncMessage": {
                    "sentMessage": {
                        "timestamp": 1726999997000u64,
                        "storyMessage": face,
                    }
                }
            },
            {
                "timestamp": 1727000001000u64,
                "syncMessage": {
                    "sentMessage": {
                        "timestamp": 1726999998000u64,
                        "groupInfo": { "groupId": "ZmFrZS1ncm91cC0x" },
                        "storyMessage": { "text": "group story" },
                    }
                }
            }
        ]),
    );

    // The 1:1 face: the outgoing row shape the desktop's story intercept
    // consumes — the self sender key, the additive keys on the wire.
    let record = next_story_event(&mut host_lane).await;
    assert_eq!(
        record.id,
        stable_hash_id(&["signal-story-sync", &account_id, "1726999997000"])
    );
    assert_eq!(record.account_id, account_id);
    assert_eq!(
        record.conversation_id, "",
        "a story addresses no conversation"
    );
    assert_eq!(record.direction, "outgoing");
    assert_eq!(record.sender_id, account_id, "the self sender key");
    assert_eq!(record.status, "sent");
    assert_eq!(record.sent_at, 1726999997000);
    assert!(record.text.is_none());
    assert_eq!(
        record.story_message.as_ref(),
        Some(&face),
        "the bounded story face forwards verbatim"
    );
    let wire = serde_json::to_value(&record).unwrap();
    assert_eq!(wire["isStory"], Value::Bool(true), "{wire}");
    assert_eq!(wire["storyMessage"], face, "{wire}");

    // The group face rides the same treatment: verbatim face, outgoing row
    // shape (the desktop's group-story boundary is its own shape gate's
    // call — the connector invents no conversation).
    let group = next_story_event(&mut host_lane).await;
    assert_eq!(group.sent_at, 1726999998000);
    assert_eq!(group.direction, "outgoing");
    assert_eq!(group.conversation_id, "");
    assert_eq!(
        group.story_message.as_ref(),
        Some(&json!({ "text": "group story" })),
        "the group story face forwards verbatim too"
    );

    // The store stays clean: the row faces never became rows.
    sleep(Duration::from_millis(300)).await;
    while host_lane.try_recv().is_ok() {}
    let after = store_snapshot(&store, &account_id);
    assert_eq!(after, before, "the story transcript stores nothing");
    supervisor.shutdown().await.unwrap();
}

/// The pre-1.160 shapes stay byte-identical: an ordinary sent transcript
/// keeps landing its stored outgoing row — no `isStory`, no
/// `storyMessage` key on the event record or the stored row — and an
/// oversized story face keeps the silent skip (宁缺勿错): no event, no
/// row, no conversation.
#[tokio::test]
async fn pre_160_shapes_stay_byte_identical_and_oversized_skips_silently() {
    let temp = TempDir::new().unwrap();
    let signal_data = temp.path().join("signal-data");
    let (supervisor, store, account_id, mut host_lane) = story_sync_supervisor(&temp).await;
    let before = store_snapshot(&store, &account_id);

    // The ordinary sent transcript (the pre-1.160 sync shape).
    emit_envelopes(
        &signal_data,
        json!([
            {
                "timestamp": 1727000002000u64,
                "syncMessage": {
                    "sentMessage": {
                        "timestamp": 1726999999000u64,
                        "destinationNumber": "+15555550101",
                        "message": "from my phone",
                    }
                }
            }
        ]),
    );
    let mirror = loop {
        let event = timeout(Duration::from_secs(5), host_lane.recv())
            .await
            .expect("the mirror row event within timeout")
            .unwrap();
        if let HostSideEvent::MessageUpserted(record) = event
            && record.text.as_deref() == Some("from my phone")
        {
            break record;
        }
    };
    assert_eq!(mirror.direction, "outgoing");
    assert!(!mirror.is_story);
    assert!(mirror.story_message.is_none());
    assert!(
        !mirror.conversation_id.is_empty(),
        "the mirror row lands in its conversation"
    );
    let wire = serde_json::to_value(&mirror).unwrap();
    assert!(wire.get("isStory").is_none(), "{wire}");
    assert!(wire.get("storyMessage").is_none(), "{wire}");

    // The store holds exactly the mirror row, and the stored shape carries
    // neither story key (the additive keys are event-payload-only by
    // construction — every stored row predates them).
    sleep(Duration::from_millis(300)).await;
    while host_lane.try_recv().is_ok() {}
    let mid = store_snapshot(&store, &account_id);
    // The mirror lands in the direct conversation (created by the link
    // burst when it survived the account-materialization race, created by
    // the mirror otherwise) — exactly one new row either way.
    assert_eq!(
        mid,
        (before.0.max(1), before.1 + 1),
        "exactly the mirror row, in exactly one conversation"
    );
    let stored = store
        .message_by_id(&account_id, &mirror.conversation_id, &mirror.id)
        .unwrap()
        .expect("the mirror row is stored");
    let stored_wire = serde_json::to_value(&stored).unwrap();
    assert!(stored_wire.get("isStory").is_none(), "{stored_wire}");
    assert!(stored_wire.get("storyMessage").is_none(), "{stored_wire}");

    // The oversized story face and the anchorless face (no timestamp
    // anywhere — the sent leg and the envelope leg both absent) keep the
    // silent skip: no event, no row.
    emit_envelopes(
        &signal_data,
        json!([
            {
                "timestamp": 1727000003000u64,
                "syncMessage": {
                    "sentMessage": {
                        "timestamp": 1726999999500u64,
                        "storyMessage": { "text": "x".repeat(3 * 1024 * 1024 + 1) },
                    }
                }
            },
            {
                "syncMessage": {
                    "sentMessage": {
                        "storyMessage": { "text": "no anchor" },
                    }
                }
            }
        ]),
    );
    sleep(Duration::from_millis(900)).await;
    while let Ok(event) = host_lane.try_recv() {
        if let HostSideEvent::MessageUpserted(record) = event {
            assert!(
                !record.is_story,
                "no oversized story face may land: {record:?}"
            );
        }
    }
    let after = store_snapshot(&store, &account_id);
    assert_eq!(after, mid, "the oversized face stores nothing");
    supervisor.shutdown().await.unwrap();
}
