// SPDX-License-Identifier: AGPL-3.0-only

#![cfg(unix)]

//! `stories.sendReaction` group leg (contract revision 1.159, §4.157; the
//! engine's group story-reaction batch, engine `a6c0520`, the official
//! ordinary-group-channel alignment at
//! enqueueReactionForSend.preload.ts:84-91 — the DataMessage{groupV2,
//! reaction, storyContext} three-key face, not the story channel): the
//! optional `group` key rides the engine `sendStoryReaction` frame
//! verbatim, the additive answer face `group: {groupId, timestamp}` passes
//! through untouched (this face has no local projection to re-key), the
//! `remove:true` local refusal keeps its face ahead of the group key on
//! both legs, and every pre-1.159 frame and answer reads byte-identically.
//! The fixture plays the engine (the stub answer pins the wire shape, the
//! engine fake discipline); these tests pin the connector's verbatim
//! forward, the answer passthrough, and the error fidelity — never real
//! engine group-reaction behavior (the real-device group story reaction is
//! the engine batch's own acceptance).

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use kt_signal_connector::engine::EngineConfig;
use kt_signal_connector::service::StoriesSendReactionParams;
use kt_signal_connector::store::{Store, StoreKey};
use kt_signal_connector::supervisor::RuntimeSupervisor;
use serde_json::{Value, json};
use tempfile::TempDir;

fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fake-engine.py")
}

fn test_store(dir: &Path) -> Arc<Store> {
    Arc::new(Store::open(dir, Some(StoreKey::from_bytes([0x5A; 32]))).unwrap())
}

const SIGNAL_ACCOUNT: &str = "+15555550100";

/// A well-shaped engine groupId (the group master key standard base64, the
/// `sendTextGroup` group-id caliber).
const GROUP_ID: &str = "pUUd9rRO9u2Yffq7vIYYOXNTlNLAWzCvTfgAFpBYK5Q=";

/// `registry_accounts` is the raw JSON array body of the engine-side
/// registry — the store row and the engine registry are two separate
/// truths, and the unknown-account test needs them to disagree.
fn story_reaction_supervisor(
    temp: &TempDir,
    data_dir: &Path,
    registry_accounts: &str,
) -> (Arc<Store>, Arc<RuntimeSupervisor>, String) {
    std::fs::create_dir_all(data_dir).unwrap();
    std::fs::write(
        data_dir.join("engine-state.json"),
        format!(r#"{{"version":2,"accounts":[{registry_accounts}]}}"#),
    )
    .unwrap();
    let store = test_store(temp.path());
    let account = store
        .upsert_account_from_signal(
            SIGNAL_ACCOUNT,
            Some(1),
            kt_signal_connector::DEFAULT_PROXY_GROUP_ID,
        )
        .unwrap();
    let mut config = EngineConfig::new(fixture().to_path_buf(), data_dir.to_path_buf());
    config.request_timeout = Duration::from_secs(5);
    config.shutdown_grace = Duration::from_millis(100);
    let supervisor = Arc::new(RuntimeSupervisor::new(
        config,
        Arc::clone(&store),
        kt_signal_connector::DEFAULT_PROXY_GROUP_ID.to_string(),
        None,
    ));
    (store, supervisor, account.id)
}

fn log_lines(data_dir: &Path) -> Vec<Value> {
    fs::read_to_string(data_dir.join(".fixture-send-log.jsonl"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect()
}

fn reaction(
    account_id: &str,
    author: &str,
    sent_at: u64,
    emoji: &str,
    group: Option<&str>,
) -> StoriesSendReactionParams {
    StoriesSendReactionParams {
        account_id: account_id.to_string(),
        story_author: author.to_string(),
        story_sent_at: sent_at,
        emoji: emoji.to_string(),
        remove: false,
        group: group.map(str::to_string).map(Value::String),
        operation_id: None,
    }
}

/// The group single leg pinned byte-for-byte: the groupId forwards
/// verbatim onto the engine frame (beside the unchanged story anchor and
/// emoji keys), the additive answer face `group: {groupId, timestamp}`
/// passes through untouched, and the pre-1.159 shape stays byte-identical
/// (no group key on the frame, the bare `{}` success answer with no group
/// field).
#[tokio::test]
async fn story_reaction_group_leg_rides_the_engine_frame_and_answer_verbatim() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("signal-data");
    let (_store, supervisor, account_id) = story_reaction_supervisor(
        &temp,
        &data_dir,
        r#"{"number":"+15555550100","dir":"accounts/lk-abc"}"#,
    );
    supervisor.start().await.unwrap();

    // The group leg: the groupId rides the frame verbatim and the additive
    // answer face passes through untouched.
    let answered = supervisor
        .send_story_reaction(reaction(
            &account_id,
            "+15555550177",
            900,
            "❤️",
            Some(GROUP_ID),
        ))
        .await
        .unwrap();
    let face = answered
        .get("group")
        .expect("the group face rides the answer");
    assert_eq!(face["groupId"], json!(GROUP_ID));
    let face_timestamp = face["timestamp"].as_u64().expect("a u64 group timestamp");
    assert!(face_timestamp > 0);
    let log = log_lines(&data_dir);
    assert_eq!(log.len(), 1, "{log:?}");
    assert_eq!(
        log[0],
        json!({
            "account": SIGNAL_ACCOUNT,
            "storyAuthor": "+15555550177",
            "storyTimestamp": 900,
            "emoji": "❤️",
            "groupId": GROUP_ID,
        })
    );

    // The pre-1.159 shape stays byte-identical: no group key on the frame,
    // the bare `{}` success answer with no group field.
    let answered = supervisor
        .send_story_reaction(reaction(&account_id, "+15555550177", 901, "👍", None))
        .await
        .unwrap();
    assert_eq!(answered, json!({}));
    let log = log_lines(&data_dir);
    assert_eq!(log.len(), 2, "{log:?}");
    assert_eq!(
        log[1],
        json!({
            "account": SIGNAL_ACCOUNT,
            "storyAuthor": "+15555550177",
            "storyTimestamp": 901,
            "emoji": "👍",
        })
    );

    // A raw non-string group value rides ungated — the whole value domain
    // is the engine's fail-closed parse, never re-gated or re-keyed here;
    // the fixture answers its group face regardless of the shape.
    let raw = reaction(&account_id, "+15555550177", 902, "😊", None);
    let raw_params = StoriesSendReactionParams {
        group: Some(json!({"masterKey": "abc", "revision": 3})),
        ..raw
    };
    let answered = supervisor.send_story_reaction(raw_params).await.unwrap();
    assert_eq!(
        answered["group"]["groupId"],
        json!({"masterKey": "abc", "revision": 3})
    );
}

/// The error faces on the group path: the `remove:true` local refusal
/// keeps its face ahead of the group key (a remove intent never reaches
/// the engine on either leg), the engine's parse-gate refusal (the marker
/// standing for the whole group value domain) degrades through the
/// structured ladder to UPSTREAM_ERROR retryable, and the engine-down
/// shape answers RUNTIME_NOT_RUNNING non-retryable.
#[tokio::test]
async fn story_reaction_group_error_faces_ride_the_wire_verbatim() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("signal-data");
    let (_store, supervisor, account_id) = story_reaction_supervisor(
        &temp,
        &data_dir,
        r#"{"number":"+15555550100","dir":"accounts/lk-abc"}"#,
    );
    supervisor.start().await.unwrap();

    // The remove-plus-group combination answers locally before any engine
    // traffic — the engine face carries no remove parameter on either leg.
    let remove_params = StoriesSendReactionParams {
        remove: true,
        ..reaction(&account_id, "+15555550177", 903, "❤️", Some(GROUP_ID))
    };
    let error = supervisor
        .send_story_reaction(remove_params)
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "INVALID_REQUEST");
    assert!(!error.retryable, "{error:?}");
    assert!(
        !data_dir.join(".fixture-send-log.jsonl").exists(),
        "the remove refusal must not reach the engine"
    );

    // The engine's refusal on the group path: the fixture marker answers
    // the plain -32603 face the real engine's fail-closed parse answers
    // for every group value-domain violation — the structured ladder
    // degrades it to UPSTREAM_ERROR retryable.
    fs::write(data_dir.join(".fixture-story-reaction-rejected"), "1").unwrap();
    let error = supervisor
        .send_story_reaction(reaction(
            &account_id,
            "+15555550177",
            904,
            "❤️",
            Some(GROUP_ID),
        ))
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "UPSTREAM_ERROR");
    assert!(error.retryable, "{error:?}");
    fs::remove_file(data_dir.join(".fixture-story-reaction-rejected")).unwrap();

    // Engine down: no start, the store row resolves, the engine was never
    // spawned — RUNTIME_NOT_RUNNING, non-retryable (the send lane invents
    // no answer).
    let down = TempDir::new().unwrap();
    let down_data = down.path().join("signal-data");
    let (_down_store, down_supervisor, down_account) = story_reaction_supervisor(
        &down,
        &down_data,
        r#"{"number":"+15555550100","dir":"accounts/lk-abc"}"#,
    );
    let error = down_supervisor
        .send_story_reaction(reaction(
            &down_account,
            "+15555550177",
            905,
            "❤️",
            Some(GROUP_ID),
        ))
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "RUNTIME_NOT_RUNNING");
    assert!(!error.retryable, "{error:?}");
}
