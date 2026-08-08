// SPDX-License-Identifier: AGPL-3.0-only

#![cfg(unix)]

use std::path::PathBuf;
use std::time::Duration;

use kt_signal_connector::engine::{
    CallClass, EngineError, EngineEvent, EngineHandle, EngineState, SignalCliConfig, event_channel,
};
use serde_json::json;
use tempfile::TempDir;
use tokio::task::JoinSet;
use tokio::time::timeout;

fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fake-signal-cli.py")
}

async fn engine(request_timeout: Duration) -> (TempDir, EngineHandle) {
    let temp = TempDir::new().unwrap();
    let mut config = SignalCliConfig::new(fixture(), temp.path().join("signal-data"));
    config.request_timeout = request_timeout;
    config.shutdown_grace = Duration::from_millis(100);
    let (events, _) = event_channel();
    let engine = EngineHandle::start(config, events).await.unwrap();
    (temp, engine)
}

#[tokio::test]
async fn matches_success_and_normalizes_receive_without_private_fields() {
    let (_temp, engine) = engine(Duration::from_secs(1)).await;
    let mut events = engine.subscribe();

    let result = engine
        .call("emitReceive", json!({}), CallClass::ReadOnly)
        .await
        .unwrap();
    assert_eq!(result, json!({ "method": "emitReceive" }));

    let receive = timeout(Duration::from_secs(1), async {
        loop {
            if let EngineEvent::Receive(receive) = events.recv().await.unwrap() {
                break receive;
            }
        }
    })
    .await
    .unwrap();
    let encoded = serde_json::to_string(&receive).unwrap();
    assert_eq!(receive.timestamp, Some(42));
    assert!(!encoded.contains("private text"));
    assert!(!encoded.contains("+155"));
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn malformed_output_faults_runtime() {
    let (_temp, engine) = engine(Duration::from_secs(3)).await;
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
    let (_temp, engine) = engine(Duration::from_millis(50)).await;
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
    let (_temp, engine) = engine(Duration::from_secs(1)).await;
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

#[tokio::test]
async fn crash_exits_and_a_new_engine_can_start() {
    let (temp, engine) = engine(Duration::from_secs(1)).await;
    let result = engine.call("crash", json!({}), CallClass::ReadOnly).await;
    assert_eq!(result, Err(EngineError::Exited));
    assert_eq!(engine.status().state, EngineState::Exited);

    let mut config = SignalCliConfig::new(fixture(), temp.path().join("signal-data"));
    config.shutdown_grace = Duration::from_millis(100);
    let (events, _) = event_channel();
    let restarted = EngineHandle::start(config, events).await.unwrap();
    let result = restarted
        .call("version", json!({}), CallClass::ReadOnly)
        .await
        .unwrap();
    assert_eq!(result, json!({ "method": "version" }));
    restarted.shutdown().await.unwrap();
}

#[tokio::test]
async fn mutating_request_crash_is_an_unknown_outcome() {
    let (_temp, engine) = engine(Duration::from_secs(3)).await;
    let result = engine
        .call("crashSend", json!({}), CallClass::Mutating)
        .await;
    assert_eq!(result, Err(EngineError::UnknownOutcome));
    assert_eq!(engine.status().state, EngineState::Exited);
}

#[tokio::test]
async fn oversized_output_faults_runtime() {
    let temp = TempDir::new().unwrap();
    let mut config = SignalCliConfig::new(fixture(), temp.path().join("signal-data"));
    config.line_limit = 64;
    config.request_timeout = Duration::from_secs(3);
    config.shutdown_grace = Duration::from_millis(100);
    let (events, _) = event_channel();
    let engine = EngineHandle::start(config, events).await.unwrap();

    let result = engine
        .call("oversized", json!({}), CallClass::ReadOnly)
        .await;
    assert_eq!(result, Err(EngineError::Protocol));
    assert_eq!(engine.status().state, EngineState::Faulted);
}

#[tokio::test]
async fn pending_requests_apply_backpressure_at_the_hard_limit() {
    let (_temp, engine) = engine(Duration::from_millis(250)).await;
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
