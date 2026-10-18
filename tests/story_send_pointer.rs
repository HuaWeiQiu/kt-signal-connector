// SPDX-License-Identifier: AGPL-3.0-only

#![cfg(unix)]

//! `messages.sendStory` media pointer answer faces (contract revision
//! 1.161, §4.159; the engine's sendStory pointer batch, engine `941c091`,
//! the official sender-holds-the-upload-pointer fact face): when the media
//! leg uploaded, the answer carries `fileAttachment` (the roster leg's
//! upload AttachmentPointer projection, on the result's top level) and/or
//! `group.fileAttachment` (the group leg's own, inside the group face) —
//! the mixed audience's two independent uploads being distinct objects,
//! hence the split keys. The projection passes both faces through as raw
//! values; the fixture plays the engine (the stub pointer face pins the
//! wire shape, the engine fake discipline: metadata-only stub, no
//! invented crypto material). These tests pin the connector's verbatim
//! passthrough, the leg split, and the absence shapes — never real CDN
//! upload behavior (the connector performs zero CDN resolution and zero
//! pointer consumption; the desktop's own read-back chain is the
//! downstream batch).

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

/// A well-shaped engine groupId (the group master key standard base64).
const GROUP_ID: &str = "pUUd9rRO9u2Yffq7vIYYOXNTlNLAWzCvTfgAFpBYK5Q=";

const ATTACHMENT: &str = "data:image/jpeg;base64,aGVsbG8=";

/// `registry_accounts` is the raw JSON array body of the engine-side
/// registry — the store row and the engine registry are two separate
/// truths.
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

#[allow(clippy::too_many_arguments)]
fn params(
    account_id: &str,
    recipients: Vec<&str>,
    attachment: Option<&str>,
    group: Option<&str>,
) -> MessagesSendStoryParams {
    MessagesSendStoryParams {
        account_id: account_id.to_string(),
        message: None,
        recipients: recipients.into_iter().map(str::to_string).collect(),
        distribution_list_id: None,
        allows_replies: false,
        text_style: None,
        body_ranges: None,
        preview_image: None,
        preview_url: None,
        preview_title: None,
        attachment: attachment.map(str::to_string),
        caption: None,
        group: group.map(str::to_string).map(Value::String),
    }
}

/// The stub pointer face the fixture answers (the engine fake discipline
/// mirrored: the metadata family with the constant empty id and the three
/// false booleans, the locator carrying cdnKey/cdnNumber/uploadTimestamp —
/// no key/digest material invented, no caption invented without input).
fn assert_stub_pointer(face: &Value, context: &str) {
    assert!(face.is_object(), "{context}: {face}");
    assert_eq!(face["id"], json!(""), "{context}: the stub id");
    assert_eq!(
        face["contentType"],
        json!("application/octet-stream"),
        "{context}: the stub content type"
    );
    assert_eq!(face["isVoiceNote"], json!(false), "{context}");
    assert_eq!(face["isBorderless"], json!(false), "{context}");
    assert_eq!(face["isGif"], json!(false), "{context}");
    assert_eq!(
        face["locator"]["cdnKey"],
        json!("fake-story-attachment"),
        "{context}: the stub cdn key"
    );
    assert_eq!(face["locator"]["cdnNumber"], json!(3), "{context}");
    assert!(
        face["locator"]["uploadTimestamp"].is_u64(),
        "{context}: the stub moment"
    );
    assert!(
        face.get("caption").is_none()
            && face["locator"].get("key").is_none()
            && face["locator"].get("digest").is_none(),
        "{context}: the stub invents no caption and no crypto material"
    );
}

/// The pointer faces ride the answer verbatim on both legs: the roster
/// media leg on the result's top level, the group media single leg inside
/// the group face (the top level absent), the mixed audience carrying both
/// under their split keys without merging, and the group text leg's group
/// face staying pointer-free.
#[tokio::test]
async fn story_send_pointer_faces_ride_the_answer_verbatim() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("signal-data");
    let (_store, supervisor, account_id) = story_send_supervisor(
        &temp,
        &data_dir,
        r#"{"number":"+15555550100","dir":"accounts/lk-abc"}"#,
    );
    supervisor.start().await.unwrap();

    // The roster media single leg: the top-level pointer rides with the
    // full stub face; the frame carried the attachment data URI verbatim.
    let answered: StorySendResult = supervisor
        .send_story(params(
            &account_id,
            vec!["+15555550101"],
            Some(ATTACHMENT),
            None,
        ))
        .await
        .unwrap();
    let pointer = answered
        .file_attachment
        .as_ref()
        .expect("the roster pointer rides");
    assert_stub_pointer(pointer, "roster leg");
    let log = log_lines(&data_dir);
    assert_eq!(log.len(), 1, "{log:?}");
    assert_eq!(log[0]["attachment"], json!(ATTACHMENT));
    assert!(log[0].get("group").is_none());

    // The group media single leg (the empty batch riding with the group):
    // the pointer rides inside the group face, the top level stays absent.
    let answered: StorySendResult = supervisor
        .send_story(params(
            &account_id,
            Vec::new(),
            Some(ATTACHMENT),
            Some(GROUP_ID),
        ))
        .await
        .unwrap();
    assert!(
        answered.file_attachment.is_none(),
        "the group single leg leaves the top-level pointer absent"
    );
    let face = answered.group.as_ref().expect("the group face rides");
    assert_eq!(face.group_id, GROUP_ID);
    assert_eq!(face.timestamp, answered.timestamp);
    let pointer = face
        .file_attachment
        .as_ref()
        .expect("the group pointer rides");
    assert_stub_pointer(pointer, "group single leg");

    // The mixed audience: both pointers ride under their split keys, each
    // with its own stub face — the projection never merges the legs.
    let answered: StorySendResult = supervisor
        .send_story(params(
            &account_id,
            vec!["+15555550101"],
            Some(ATTACHMENT),
            Some(GROUP_ID),
        ))
        .await
        .unwrap();
    let roster_pointer = answered
        .file_attachment
        .as_ref()
        .expect("the mixed roster pointer rides");
    assert_stub_pointer(roster_pointer, "mixed roster leg");
    let face = answered.group.as_ref().expect("the mixed group face rides");
    let group_pointer = face
        .file_attachment
        .as_ref()
        .expect("the mixed group pointer rides");
    assert_stub_pointer(group_pointer, "mixed group leg");
    assert_eq!(face.timestamp, answered.timestamp + 1);

    // The group text leg: the group face stays pointer-free.
    let answered: StorySendResult = supervisor
        .send_story(params(&account_id, Vec::new(), None, Some(GROUP_ID)))
        .await
        .unwrap_or_else(|_| {
            panic!("the group text leg sends");
        });
    let face = answered.group.as_ref().expect("the group face rides");
    assert!(
        face.file_attachment.is_none(),
        "the group text leg answers no pointer"
    );

    let log = log_lines(&data_dir);
    assert_eq!(log.len(), 4, "{log:?}");
}

/// The pointer-less answers keep the pre-1.161 shapes byte-identically:
/// the text leg's answer serializes neither pointer key (and neither does
/// the frame carry an attachment key), the stub face carries exactly its
/// documented key set on the wire, and the send stays rowless.
#[tokio::test]
async fn story_send_pointer_absence_keeps_the_pre161_shapes() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("signal-data");
    let (store, supervisor, account_id) = story_send_supervisor(
        &temp,
        &data_dir,
        r#"{"number":"+15555550100","dir":"accounts/lk-abc"}"#,
    );
    supervisor.start().await.unwrap();

    // The text leg: no pointer key on the frame, no pointer key on the
    // answer — the serialized result reads exactly the pre-1.161 shape.
    let mut text_params = params(&account_id, vec!["+15555550101"], None, None);
    text_params.message = Some("story body".to_string());
    let answered: StorySendResult = supervisor.send_story(text_params).await.unwrap();
    assert!(answered.file_attachment.is_none() && answered.group.is_none());
    let serialized = serde_json::to_value(&answered).unwrap();
    assert_eq!(
        serialized,
        json!({
            "timestamp": answered.timestamp,
            "results": [],
        }),
        "the pre-1.161 answer shape, byte-identical"
    );
    let log = log_lines(&data_dir);
    assert_eq!(log.len(), 1, "{log:?}");
    assert!(log[0].get("attachment").is_none());

    // The media leg's stub face carries exactly its documented key set —
    // the fixture invents nothing beyond the engine fake discipline.
    let answered: StorySendResult = supervisor
        .send_story(params(
            &account_id,
            vec!["+15555550101"],
            Some(ATTACHMENT),
            None,
        ))
        .await
        .unwrap();
    let serialized = serde_json::to_value(&answered).unwrap();
    let pointer = &serialized["fileAttachment"];
    let mut keys: Vec<String> = pointer
        .as_object()
        .expect("the pointer is an object")
        .keys()
        .cloned()
        .collect();
    keys.sort();
    assert_eq!(
        keys,
        vec![
            "contentType".to_string(),
            "id".to_string(),
            "isBorderless".to_string(),
            "isGif".to_string(),
            "isVoiceNote".to_string(),
            "locator".to_string()
        ],
        "the stub face's exact key set: {pointer}"
    );

    // The send stays rowless: no conversation row, no message row behind
    // either answer (the §4.67 story posture).
    let conversations = store.list_conversations(&account_id, 50, None).unwrap();
    assert!(
        conversations.items.is_empty(),
        "a story send lands in no conversation row"
    );
}
