// SPDX-License-Identifier: AGPL-3.0-only

#![cfg(unix)]

//! `messages.sendStory` media story leg (contract revision 1.156, §4.154;
//! the engine's media story batch, engine `7e3652e`, the official
//! `StoryMessage.fileAttachment` oneof alignment at
//! protos/SignalService.proto:476-479 and sendStory.preload.ts:186-190):
//! the optional `attachment` data URI rides the engine `sendStory` frame
//! verbatim, and `message` becomes optional — the two content legs, both
//! optional on the connector face. The connector performs no
//! leg-exclusivity gate: co-presence, a text-leg key on the attachment
//! leg, and the bare no-payload frame are the engine's fail-closed parse;
//! the attachment data URI is verbatim transit material (no deep parse,
//! no cap check — the upload gates are the engine's transit-CDN face).
//! The fixture plays the engine (the stub answer pins the wire shape, the
//! engine fake discipline); these tests pin the connector's verbatim
//! forward and error fidelity, never real engine media-story behavior
//! (the real-device file story is the engine batch's own acceptance).

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

fn params(
    account_id: &str,
    message: Option<&str>,
    attachment: Option<&str>,
) -> MessagesSendStoryParams {
    MessagesSendStoryParams {
        account_id: account_id.to_string(),
        message: message.map(str::to_string),
        recipients: vec!["+15555550101".into()],
        distribution_list_id: None,
        allows_replies: false,
        text_style: None,
        body_ranges: None,
        preview_image: None,
        preview_url: None,
        preview_title: None,
        attachment: attachment.map(str::to_string),
    }
}

/// The verbatim forward pinned byte-for-byte on the leg states: the media
/// leg alone (no message key on the frame, the data URI verbatim), the
/// text leg alone (the pre-1.156 shape, no attachment key), the two legs
/// riding together (no connector-side exclusivity gate — the co-presence
/// is the engine's refusal), and the empty-string attachment riding too
/// (the zero-local-gate posture: the value domain is the engine's).
#[tokio::test]
async fn story_media_leg_rides_the_engine_frame_verbatim() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("signal-data");
    let (_store, supervisor, account_id) = story_send_supervisor(
        &temp,
        &data_dir,
        r#"{"number":"+15555550100","dir":"accounts/lk-abc"}"#,
    );
    supervisor.start().await.unwrap();

    // The media leg alone: no message key on the frame, the data URI
    // byte-verbatim (filename parameter and base64 payload untouched).
    let media_uri = "data:image/jpeg;filename=story%20pic.jpg;base64,QUJDREVGRw==";
    let answered: StorySendResult = supervisor
        .send_story(params(&account_id, None, Some(media_uri)))
        .await
        .unwrap();
    assert!(answered.timestamp > 0);
    let log = log_lines(&data_dir);
    assert_eq!(log.len(), 1, "{log:?}");
    assert_eq!(
        log[0],
        json!({
            "account": SIGNAL_ACCOUNT,
            "recipients": ["+15555550101"],
            "allowsReplies": false,
            "attachment": media_uri,
        })
    );

    // The text leg alone: the pre-1.156 shape, no attachment key.
    supervisor
        .send_story(params(&account_id, Some("text story body"), None))
        .await
        .unwrap();
    let log = log_lines(&data_dir);
    assert_eq!(log.len(), 2, "{log:?}");
    assert_eq!(log[1]["message"], json!("text story body"));
    assert!(log[1].get("attachment").is_none());

    // Both legs: the connector forwards what arrived — the co-presence
    // refusal is the engine's face, never a connector gate.
    supervisor
        .send_story(params(
            &account_id,
            Some("caption body"),
            Some("data:image/png;base64,AAAA"),
        ))
        .await
        .unwrap();
    let log = log_lines(&data_dir);
    assert_eq!(log.len(), 3, "{log:?}");
    assert_eq!(log[2]["message"], json!("caption body"));
    assert_eq!(log[2]["attachment"], json!("data:image/png;base64,AAAA"));

    // The empty-string attachment rides as-is: the zero-local-gate
    // posture (the engine's parse owns the refusal).
    supervisor
        .send_story(params(&account_id, None, Some("")))
        .await
        .unwrap();
    let log = log_lines(&data_dir);
    assert_eq!(log.len(), 4, "{log:?}");
    assert_eq!(log[3]["attachment"], json!(""));

    // The bare no-payload frame (no message, no attachment) forwards
    // as-is and is the engine's refusal, never a connector gate.
    supervisor
        .send_story(params(&account_id, None, None))
        .await
        .unwrap();
    let log = log_lines(&data_dir);
    assert_eq!(log.len(), 5, "{log:?}");
    assert!(log[4].get("message").is_none());
    assert!(log[4].get("attachment").is_none());
}

/// The error faces: the connector-local envelope gate (an unshaped opaque
/// account id answers INVALID_REQUEST before any engine traffic), the
/// engine's parse-gate refusal (the marker standing for the two-leg value
/// domain) degrading through the structured ladder to UPSTREAM_ERROR
/// retryable, and the engine-down shape answering RUNTIME_NOT_RUNNING
/// non-retryable.
#[tokio::test]
async fn story_media_leg_error_faces_ride_the_wire_verbatim() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("signal-data");
    let (_store, supervisor, account_id) = story_send_supervisor(
        &temp,
        &data_dir,
        r#"{"number":"+15555550100","dir":"accounts/lk-abc"}"#,
    );
    supervisor.start().await.unwrap();

    // The connector-local envelope gate: an unshaped opaque account id
    // never reaches the engine.
    let error = supervisor
        .send_story(params(
            &"k".repeat(129),
            None,
            Some("data:image/png;base64,AAAA"),
        ))
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "INVALID_REQUEST");
    assert!(!error.retryable, "{error:?}");
    assert!(
        !data_dir.join(".fixture-send-log.jsonl").exists(),
        "the unshaped account id must not reach the engine"
    );

    // The engine's parse-gate refusal: the fixture marker answers the
    // plain -32603 face the real engine's fail-closed parse answers for
    // every two-leg value-domain violation.
    fs::write(data_dir.join(".fixture-story-send-rejected"), "1").unwrap();
    let error = supervisor
        .send_story(params(
            &account_id,
            Some("caption body"),
            Some("data:image/png;base64,AAAA"),
        ))
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
        .send_story(params(
            &down_account,
            None,
            Some("data:image/png;base64,AAAA"),
        ))
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "RUNTIME_NOT_RUNNING");
    assert!(!error.retryable, "{error:?}");
}
