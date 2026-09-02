// SPDX-License-Identifier: AGPL-3.0-only

#![cfg(unix)]

use std::path::PathBuf;
use std::time::Duration;

use kt_signal_connector::engine::{
    CallClass, EngineError, EngineEvent, EngineHandle, EngineState, SignalCliConfig, SignalCliMode,
    SocksProxy, event_channel, receive_channel,
};
use serde_json::json;
use tempfile::TempDir;
use tokio::task::JoinSet;
use tokio::time::timeout;

fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fake-signal-cli.py")
}

async fn engine(
    request_timeout: Duration,
) -> (
    TempDir,
    EngineHandle,
    tokio::sync::mpsc::Receiver<kt_signal_connector::engine::QueuedReceive>,
) {
    let temp = TempDir::new().unwrap();
    let mut config = SignalCliConfig::new(fixture(), temp.path().join("signal-data"));
    config.request_timeout = request_timeout;
    config.shutdown_grace = Duration::from_millis(100);
    let (events, _) = event_channel();
    let (receive_ingress, receives) = receive_channel();
    let engine = EngineHandle::start(config, events, receive_ingress)
        .await
        .unwrap();
    (temp, engine, receives)
}

#[tokio::test]
async fn matches_success_and_normalizes_receive_without_private_fields() {
    let (_temp, engine, mut receives) = engine(Duration::from_secs(1)).await;

    let result = engine
        .call("emitReceive", json!({}), CallClass::ReadOnly)
        .await
        .unwrap();
    assert_eq!(result, json!({ "method": "emitReceive" }));

    let queued = timeout(Duration::from_secs(1), receives.recv())
        .await
        .unwrap()
        .unwrap();
    let receive = queued.receive();
    let encoded = serde_json::to_string(&receive).unwrap();
    assert_eq!(receive.timestamp, Some(42));
    assert!(!encoded.contains("private text"));
    assert!(!encoded.contains("+155"));
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn malformed_output_faults_runtime() {
    let (_temp, engine, _receives) = engine(Duration::from_secs(3)).await;
    let running_pid = engine.status().pid;
    let mut events = engine.subscribe();
    let result = engine
        .call("malformed", json!({}), CallClass::ReadOnly)
        .await;
    assert_eq!(result, Err(EngineError::Protocol));
    assert_eq!(engine.status().state, EngineState::Faulted);
    let faulted = timeout(Duration::from_secs(1), async {
        loop {
            if let EngineEvent::StateChanged(status) = events.recv().await.unwrap()
                && status.state == EngineState::Faulted
            {
                break status;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(faulted.pid, running_pid);
}

#[tokio::test]
async fn read_timeout_and_send_timeout_have_distinct_outcomes() {
    let (_temp, engine, _receives) = engine(Duration::from_millis(50)).await;
    assert_eq!(
        engine.call("hang", json!({}), CallClass::ReadOnly).await,
        Err(EngineError::Timeout)
    );
    assert_eq!(
        engine
            .call("sendHang", json!({}), CallClass::Mutating)
            .await,
        Err(EngineError::UnknownOutcome)
    );
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn duplicate_response_is_reported_without_resolving_twice() {
    let (_temp, engine, _receives) = engine(Duration::from_secs(1)).await;
    let mut events = engine.subscribe();
    let result = engine
        .call("duplicate", json!({}), CallClass::ReadOnly)
        .await
        .unwrap();
    assert_eq!(result, json!({ "method": "duplicate" }));

    let warning = timeout(Duration::from_secs(1), async {
        loop {
            if let EngineEvent::ProtocolWarning { kind } = events.recv().await.unwrap() {
                break kind;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(warning, "unknownOrDuplicateResponseId");
    engine.shutdown().await.unwrap();
}

/// Native mode: the same fixture script is spawned directly, JAVA_HOME
/// validation is skipped (a bogus path is accepted and ignored), and a
/// configured SOCKS proxy does not fail startup — it travels as `-D` argv
/// properties (unit-tested in engine.rs) instead of JAVA_OPTS.
#[tokio::test]
async fn native_mode_skips_java_validation_and_serves_requests() {
    let temp = TempDir::new().unwrap();
    let mut config = SignalCliConfig::new(fixture(), temp.path().join("signal-data"));
    config.mode = SignalCliMode::Native;
    // JVM mode rejects this JAVA_HOME (relative would also fail; here it is
    // absolute but has no `release` file). Native mode must not look at it.
    config.java_home = Some(temp.path().join("no-such-jre"));
    config.proxy = Some(SocksProxy {
        host: "127.0.0.1".into(),
        port: 11080,
    });
    config.request_timeout = Duration::from_secs(1);
    config.shutdown_grace = Duration::from_millis(100);
    let (events, _) = event_channel();
    let (receive_ingress, _receives) = receive_channel();
    let engine = EngineHandle::start(config, events, receive_ingress)
        .await
        .unwrap();

    let result = engine
        .call("version", json!({}), CallClass::ReadOnly)
        .await
        .unwrap();
    assert_eq!(result, json!({ "method": "version" }));
    engine.shutdown().await.unwrap();
}

/// The same bogus JAVA_HOME must still fail fast in JVM mode: the validation
/// gate is only skipped for native binaries, not relaxed globally.
#[tokio::test]
async fn jvm_mode_still_rejects_a_bogus_java_home() {
    let temp = TempDir::new().unwrap();
    let mut config = SignalCliConfig::new(fixture(), temp.path().join("signal-data"));
    config.java_home = Some(temp.path().join("no-such-jre"));
    let (events, _) = event_channel();
    let (receive_ingress, _receives) = receive_channel();
    let result = EngineHandle::start(config, events, receive_ingress).await;
    assert_eq!(result.err(), Some(EngineError::StartFailed));
}

#[tokio::test]
async fn crash_exits_and_a_new_engine_can_start() {
    let (temp, engine, _receives) = engine(Duration::from_secs(1)).await;
    let result = engine.call("crash", json!({}), CallClass::ReadOnly).await;
    assert_eq!(result, Err(EngineError::Exited));
    assert_eq!(engine.status().state, EngineState::Exited);

    let mut config = SignalCliConfig::new(fixture(), temp.path().join("signal-data"));
    config.shutdown_grace = Duration::from_millis(100);
    let (events, _) = event_channel();
    let (receive_ingress, _receives) = receive_channel();
    let restarted = EngineHandle::start(config, events, receive_ingress)
        .await
        .unwrap();
    let result = restarted
        .call("version", json!({}), CallClass::ReadOnly)
        .await
        .unwrap();
    assert_eq!(result, json!({ "method": "version" }));
    restarted.shutdown().await.unwrap();
}

#[tokio::test]
async fn mutating_request_crash_is_an_unknown_outcome() {
    let (_temp, engine, _receives) = engine(Duration::from_secs(3)).await;
    let result = engine
        .call("crashSend", json!({}), CallClass::Mutating)
        .await;
    assert_eq!(result, Err(EngineError::UnknownOutcome));
    assert_eq!(engine.status().state, EngineState::Exited);
}

/// remoteDelete is Mutating on both indeterminate paths: a slow answer past
/// the request timeout (fixture targetTimestamp 350, answered after 0.35s)
/// and a one-shot crash mid-call (fixture targetTimestamp 421) each resolve
/// to UnknownOutcome — never to a retryable timeout — so the supervisor can
/// surface the explicit `{"status":"unknown"}` response.
#[tokio::test]
async fn remote_delete_slow_answer_and_crash_are_unknown_outcomes() {
    let (_temp, slow_engine, _receives) = engine(Duration::from_millis(100)).await;
    assert_eq!(
        slow_engine
            .call(
                "remoteDelete",
                json!({ "targetTimestamp": 350 }),
                CallClass::Mutating
            )
            .await,
        Err(EngineError::UnknownOutcome)
    );
    // The late fixture answer must not resolve anything afterwards.
    tokio::time::sleep(Duration::from_millis(400)).await;
    slow_engine.shutdown().await.unwrap();

    let (_temp, crashed_engine, _receives) = engine(Duration::from_secs(3)).await;
    assert_eq!(
        crashed_engine
            .call(
                "remoteDelete",
                json!({ "targetTimestamp": 421 }),
                CallClass::Mutating
            )
            .await,
        Err(EngineError::UnknownOutcome)
    );
    assert_eq!(crashed_engine.status().state, EngineState::Exited);
}

/// sendReaction is Mutating on both indeterminate paths: a slow answer past
/// the request timeout (fixture targetTimestamp 351, answered after 0.35s)
/// and a one-shot crash mid-call (fixture targetTimestamp 423) each resolve
/// to UnknownOutcome — never to a retryable timeout — so the supervisor can
/// surface the explicit `{"status":"unknown"}` response.
#[tokio::test]
async fn send_reaction_slow_answer_and_crash_are_unknown_outcomes() {
    let (_temp, slow_engine, _receives) = engine(Duration::from_millis(100)).await;
    assert_eq!(
        slow_engine
            .call(
                "sendReaction",
                json!({ "targetTimestamp": 351 }),
                CallClass::Mutating
            )
            .await,
        Err(EngineError::UnknownOutcome)
    );
    // The late fixture answer must not resolve anything afterwards.
    tokio::time::sleep(Duration::from_millis(400)).await;
    slow_engine.shutdown().await.unwrap();

    let (_temp, crashed_engine, _receives) = engine(Duration::from_secs(3)).await;
    assert_eq!(
        crashed_engine
            .call(
                "sendReaction",
                json!({ "targetTimestamp": 423 }),
                CallClass::Mutating
            )
            .await,
        Err(EngineError::UnknownOutcome)
    );
    assert_eq!(crashed_engine.status().state, EngineState::Exited);
}

#[tokio::test]
async fn oversized_output_faults_runtime() {
    let temp = TempDir::new().unwrap();
    let mut config = SignalCliConfig::new(fixture(), temp.path().join("signal-data"));
    config.line_limit = 64;
    config.request_timeout = Duration::from_secs(3);
    config.shutdown_grace = Duration::from_millis(100);
    let (events, _) = event_channel();
    let (receive_ingress, _receives) = receive_channel();
    let engine = EngineHandle::start(config, events, receive_ingress)
        .await
        .unwrap();

    let result = engine
        .call("oversized", json!({}), CallClass::ReadOnly)
        .await;
    assert_eq!(result, Err(EngineError::Protocol));
    assert_eq!(engine.status().state, EngineState::Faulted);
}

#[tokio::test]
async fn pending_requests_apply_backpressure_at_the_hard_limit() {
    let (_temp, engine, _receives) = engine(Duration::from_millis(250)).await;
    let mut tasks = JoinSet::new();
    for _ in 0..129 {
        let engine = engine.clone();
        tasks.spawn(async move { engine.call("hang", json!({}), CallClass::ReadOnly).await });
    }

    let mut backpressure = 0;
    while let Some(result) = tasks.join_next().await {
        if result.unwrap() == Err(EngineError::Backpressure) {
            backpressure += 1;
        }
    }
    assert!(backpressure >= 1);
    engine.shutdown().await.unwrap();
}
