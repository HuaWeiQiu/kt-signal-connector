// SPDX-License-Identifier: AGPL-3.0-only

#![cfg(unix)]

//! `messages.sendStory` group audience leg (contract revision 1.157,
//! §4.155; the engine's group story batch, engine `ffab2a8`, the official
//! `StoryMessage.group` GroupContextV2 alignment at
//! protos/SignalService.proto:473-482 and
//! sendStoryMessage.preload.ts:40-63): the optional `group` key rides the
//! engine `sendStory` frame verbatim, the recipients empty batch — locally
//! refused since 1.76 — forwards when the group leg rides (the group
//! single leg, the engine's audience-at-least-one-domain gate mirrored),
//! and the additive answer face `group: {groupId, timestamp}` passes the
//! projection through untouched. The fixture plays the engine (the stub
//! answer pins the wire shape, the engine fake discipline); these tests
//! pin the connector's verbatim forward, the gate flip, and the response
//! passthrough — never real engine group-story behavior (the real-device
//! group story is the engine batch's own acceptance).

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use kt_signal_connector::engine::EngineConfig;
use kt_signal_connector::service::{MessagesSendStoryParams, StorySendResult};
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
fn story_send_supervisor(
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

fn params(account_id: &str, recipients: Vec<&str>, group: Option<&str>) -> MessagesSendStoryParams {
    MessagesSendStoryParams {
        account_id: account_id.to_string(),
        message: Some("story body".to_string()),
        recipients: recipients.into_iter().map(str::to_string).collect(),
        distribution_list_id: None,
        allows_replies: false,
        text_style: None,
        body_ranges: None,
        preview_image: None,
        preview_url: None,
        preview_title: None,
        attachment: None,
        caption: None,
        group: group.map(str::to_string).map(Value::String),
    }
}

/// The verbatim forward pinned byte-for-byte on the audience states: the
/// group single leg (the empty batch riding the frame, the group id
/// verbatim), the mixed audience (group and recipients co-present, the
/// multi-domain orchestration left to the engine), the additive answer
/// face passing through on both (the single leg shares the batch
/// timestamp, the mixed audience answers the +1 offset), and the
/// pre-1.157 shape staying byte-identical (no group key on the frame, no
/// group face on the answer).
#[tokio::test]
async fn story_group_leg_rides_the_engine_frame_and_answer_verbatim() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("signal-data");
    let (_store, supervisor, account_id) = story_send_supervisor(
        &temp,
        &data_dir,
        r#"{"number":"+15555550100","dir":"accounts/lk-abc"}"#,
    );
    supervisor.start().await.unwrap();

    // The group single leg: the empty batch rides the frame, the group id
    // forwards verbatim, and the additive answer face passes through (the
    // single leg shares the batch timestamp).
    let answered: StorySendResult = supervisor
        .send_story(params(&account_id, Vec::new(), Some(GROUP_ID)))
        .await
        .unwrap();
    assert!(answered.timestamp > 0);
    let face = answered.group.as_ref().expect("the group face rides");
    assert_eq!(face.group_id, GROUP_ID);
    assert_eq!(face.timestamp, answered.timestamp);
    let log = log_lines(&data_dir);
    assert_eq!(log.len(), 1, "{log:?}");
    assert_eq!(
        log[0],
        json!({
            "account": SIGNAL_ACCOUNT,
            "recipients": [],
            "allowsReplies": false,
            "message": "story body",
            "group": GROUP_ID,
        })
    );

    // The mixed audience: group and recipients co-present forwards as-is;
    // the answer face rides with the official +1 offset.
    let answered: StorySendResult = supervisor
        .send_story(params(&account_id, vec!["+15555550101"], Some(GROUP_ID)))
        .await
        .unwrap();
    let face = answered.group.as_ref().expect("the group face rides");
    assert_eq!(face.group_id, GROUP_ID);
    assert_eq!(face.timestamp, answered.timestamp + 1);
    let log = log_lines(&data_dir);
    assert_eq!(log.len(), 2, "{log:?}");
    assert_eq!(log[1]["group"], json!(GROUP_ID));
    assert_eq!(log[1]["recipients"], json!(["+15555550101"]));

    // The pre-1.157 shape stays byte-identical: no group key on the frame,
    // no group face on the answer.
    let answered: StorySendResult = supervisor
        .send_story(params(&account_id, vec!["+15555550101"], None))
        .await
        .unwrap();
    assert!(answered.group.is_none());
    assert!(
        serde_json::to_value(&answered)
            .unwrap()
            .get("group")
            .is_none(),
        "the pre-1.157 answer never serializes a group key"
    );
    let log = log_lines(&data_dir);
    assert_eq!(log.len(), 3, "{log:?}");
    assert!(log[2].get("group").is_none());
}

/// The error faces: the flipped gate keeps its local refusal (the empty
/// batch without the group answers INVALID_REQUEST before any engine
/// traffic), the engine's parse-gate refusal (the marker standing for the
/// whole group value domain) degrading through the structured ladder to
/// UPSTREAM_ERROR retryable, and the engine-down shape answering
/// RUNTIME_NOT_RUNNING non-retryable.
#[tokio::test]
async fn story_group_leg_error_faces_ride_the_wire_verbatim() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("signal-data");
    let (_store, supervisor, account_id) = story_send_supervisor(
        &temp,
        &data_dir,
        r#"{"number":"+15555550100","dir":"accounts/lk-abc"}"#,
    );
    supervisor.start().await.unwrap();

    // The empty batch without the group keeps the pre-1.157 local refusal,
    // answered before any engine traffic.
    let error = supervisor
        .send_story(params(&account_id, Vec::new(), None))
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "INVALID_REQUEST");
    assert!(!error.retryable, "{error:?}");
    assert!(
        !data_dir.join(".fixture-send-log.jsonl").exists(),
        "the empty-batch refusal must not reach the engine"
    );

    // The engine's refusal on the group path: the fixture marker answers
    // the plain -32603 face the real engine's fail-closed parse answers
    // for every group value-domain violation — the structured ladder
    // degrades it to UPSTREAM_ERROR retryable.
    fs::write(data_dir.join(".fixture-story-send-rejected"), "1").unwrap();
    let error = supervisor
        .send_story(params(&account_id, Vec::new(), Some(GROUP_ID)))
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "UPSTREAM_ERROR");
    assert!(error.retryable, "{error:?}");

    // Engine down: no start, the store row resolves, the engine was never
    // spawned — RUNTIME_NOT_RUNNING, non-retryable (the send lane invents
    // no answer).
    let down = TempDir::new().unwrap();
    let down_data = down.path().join("signal-data");
    let (_down_store, down_supervisor, down_account) = story_send_supervisor(
        &down,
        &down_data,
        r#"{"number":"+15555550100","dir":"accounts/lk-abc"}"#,
    );
    let error = down_supervisor
        .send_story(params(&down_account, Vec::new(), Some(GROUP_ID)))
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "RUNTIME_NOT_RUNNING");
    assert!(!error.retryable, "{error:?}");
}
