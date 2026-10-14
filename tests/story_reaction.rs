// SPDX-License-Identifier: AGPL-3.0-only

#![cfg(unix)]

//! `stories.sendReaction` proxy end to end (contract revision 1.152, §4.151;
//! engine 4822e99): the §4.150 call-event proxy posture on the stories
//! family — the connector resolves the account, forwards the engine's
//! `sendStoryReaction` on the send lane as its own mutating call, and the
//! engine's `{}` success answer and error faces pass through untouched.
//! The whole value domain (the author/emoji shape, the zero timestamp, the
//! author resolution, the identity-reject and profileSharing gates) is the
//! engine's fail-closed parse, never re-gated or re-keyed here; the one
//! local refusal is `remove: true` (the official reaction is place-only,
//! remove:false hardcoded upstream). The fixture plays the engine (the stub
//! answer pins the wire shape, the engine fake discipline); these tests pin
//! the connector's verbatim forward and error fidelity, never real engine
//! reaction behavior (the real-device fan-out is the engine batch's own
//! acceptance).

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
) -> StoriesSendReactionParams {
    StoriesSendReactionParams {
        account_id: account_id.to_string(),
        story_author: author.to_string(),
        story_sent_at: sent_at,
        emoji: emoji.to_string(),
        remove: false,
        operation_id: None,
    }
}

/// The verbatim forward pinned byte-for-byte: the engine's `{}` success
/// object passes through untouched (the former `{status: sent|unknown}`
/// settlement face is gone), the upstream frame carries exactly the four
/// keys under the engine's names with no `remove` key (the face is
/// place-only), and the keys the §4.77 local gates used to refuse — the
/// blank author, the zero timestamp, the bare emoji — ride through for the
/// engine's parse to answer (the 1.151 timestamp rationale: connector-side
/// value gates would eat part of the engine's error face).
#[tokio::test]
async fn story_reaction_forward_rides_the_engine_verbatim() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("signal-data");
    let (_store, supervisor, account_id) = story_reaction_supervisor(
        &temp,
        &data_dir,
        r#"{"number":"+15555550100","dir":"accounts/lk-abc"}"#,
    );
    supervisor.start().await.unwrap();

    let answered = supervisor
        .send_story_reaction(reaction(
            &account_id,
            "+15555550101",
            1_727_000_000_000,
            "👍",
        ))
        .await
        .unwrap();
    assert_eq!(answered, json!({}));

    let log = log_lines(&data_dir);
    assert_eq!(log.len(), 1, "{log:?}");
    assert_eq!(
        log[0],
        json!({
            "account": SIGNAL_ACCOUNT,
            "storyAuthor": "+15555550101",
            "storyTimestamp": 1_727_000_000_000u64,
            "emoji": "👍",
        })
    );
    assert!(log[0].get("remove").is_none());

    // The value domain forwards, gate-free: the former local faces (blank
    // author, zero storySentAt) now cross for the engine's parse to own.
    let answered = supervisor
        .send_story_reaction(reaction(&account_id, "", 0, "👍"))
        .await
        .unwrap();
    assert_eq!(answered, json!({}));
    let log = log_lines(&data_dir);
    assert_eq!(log.len(), 2, "{log:?}");
    assert_eq!(
        log[1],
        json!({
            "account": SIGNAL_ACCOUNT,
            "storyAuthor": "",
            "storyTimestamp": 0,
            "emoji": "👍",
        })
    );
}

/// The refusal and error faces: `remove: true` is the connector's one
/// INVALID_REQUEST (non-retryable) before any engine traffic — the official
/// reaction is place-only, so a remove intent has no upstream face (zero
/// send-log lines with the engine up); the engine's parse-gate refusal (the
/// marker standing for the identity-reject/profileSharing/shape faces) and
/// the engine-side unknown account both degrade through the structured
/// ladder to UPSTREAM_ERROR retryable; the engine-down shape answers
/// RUNTIME_NOT_RUNNING non-retryable.
#[tokio::test]
async fn story_reaction_refusal_and_error_faces() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("signal-data");
    let (_store, supervisor, account_id) = story_reaction_supervisor(
        &temp,
        &data_dir,
        r#"{"number":"+15555550100","dir":"accounts/lk-abc"}"#,
    );
    supervisor.start().await.unwrap();

    // The one local refusal: remove intent, never forwarded.
    let mut removal = reaction(&account_id, "+15555550101", 1_727_000_000_000, "👍");
    removal.remove = true;
    let error = supervisor
        .send_story_reaction(removal)
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "INVALID_REQUEST");
    assert!(!error.retryable, "{error:?}");
    assert!(
        !data_dir.join(".fixture-send-log.jsonl").exists(),
        "the remove intent must not reach the engine"
    );

    // The engine's parse-gate refusal: the fixture marker answers the
    // plain -32603 face the real engine's fail-closed parse answers for
    // every value-domain violation.
    std::fs::write(data_dir.join(".fixture-story-reaction-rejected"), "1").unwrap();
    let error = supervisor
        .send_story_reaction(reaction(
            &account_id,
            "+15555550101",
            1_727_000_000_000,
            "👍",
        ))
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "UPSTREAM_ERROR");
    assert!(error.retryable, "{error:?}");
    std::fs::remove_file(data_dir.join(".fixture-story-reaction-rejected")).unwrap();

    // The engine-side unknown account: the store row resolves
    // (connector-local truth), the engine's own registry does not know the
    // number — the same plain -32603 through the ladder.
    let other = TempDir::new().unwrap();
    let other_data = other.path().join("signal-data");
    let (_other_store, other_supervisor, other_account) = story_reaction_supervisor(
        &other,
        &other_data,
        r#"{"number":"+19999999999","dir":"accounts/lk-other"}"#,
    );
    other_supervisor.start().await.unwrap();
    let error = other_supervisor
        .send_story_reaction(reaction(
            &other_account,
            "+15555550101",
            1_727_000_000_000,
            "👍",
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
    let (_down_store, down_supervisor, down_account) = story_reaction_supervisor(
        &down,
        &down_data,
        r#"{"number":"+15555550100","dir":"accounts/lk-abc"}"#,
    );
    let error = down_supervisor
        .send_story_reaction(reaction(
            &down_account,
            "+15555550101",
            1_727_000_000_000,
            "👍",
        ))
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "RUNTIME_NOT_RUNNING");
    assert!(!error.retryable, "{error:?}");
}
