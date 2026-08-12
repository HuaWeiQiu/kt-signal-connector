// SPDX-License-Identifier: AGPL-3.0-only

#![cfg(unix)]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use kt_signal_connector::engine::{
    CallClass, EngineHandle, SignalCliConfig, event_channel, receive_channel,
};
use kt_signal_connector::store::Store;
use kt_signal_connector::supervisor::RuntimeSupervisor;
use serde_json::json;
use tempfile::TempDir;
use tokio::time::{sleep, timeout};

fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fake-signal-cli.py")
}

fn watchdog_supervisor(temp: &TempDir) -> (Arc<RuntimeSupervisor>, PathBuf) {
    let store = Store::open(temp.path()).unwrap();
    let data_dir = temp.path().join("signal-data");
    let mut config = SignalCliConfig::new(fixture(), data_dir.clone());
    config.request_timeout = Duration::from_secs(1);
    config.shutdown_grace = Duration::from_millis(100);
    config.watchdog_interval = Duration::from_millis(50);
    config.watchdog_min_restart_interval = Duration::ZERO;
    let supervisor = Arc::new(RuntimeSupervisor::new(config, store));
    supervisor.spawn_watchdog();
    (supervisor, data_dir)
}

async fn wait_for_pid_change(supervisor: &Arc<RuntimeSupervisor>, pid_before: u32) -> u32 {
    timeout(Duration::from_secs(10), async {
        loop {
            if let Some(pid) = supervisor.status().await.pid
                && pid != pid_before
            {
                break pid;
            }
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("watchdog restarts the engine within the timeout")
}

#[tokio::test]
async fn engine_stderr_lines_are_broadcast_to_subscribers() {
    let temp = TempDir::new().unwrap();
    let mut config = SignalCliConfig::new(fixture(), temp.path().join("signal-data"));
    config.request_timeout = Duration::from_secs(1);
    config.shutdown_grace = Duration::from_millis(100);
    let (events, _) = event_channel();
    let (receive_ingress, _receives) = receive_channel();
    let engine = EngineHandle::start(config, events, receive_ingress)
        .await
        .unwrap();
    let mut stderr = engine.subscribe_stderr();

    engine
        .call("emitStderr", json!({}), CallClass::ReadOnly)
        .await
        .unwrap();
    let line = timeout(Duration::from_secs(2), stderr.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(line.to_lowercase().contains("websocket"));
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn watchdog_restarts_engine_on_fatal_receive_stderr() {
    let temp = TempDir::new().unwrap();
    let (supervisor, data_dir) = watchdog_supervisor(&temp);
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::write(data_dir.join(".fixture-stderr-websocket-error"), "").unwrap();

    let pid_before = supervisor.start().await.unwrap().pid.unwrap();
    let pid_after = wait_for_pid_change(&supervisor, pid_before).await;
    assert_ne!(pid_after, pid_before);
    supervisor.shutdown().await.unwrap();
}

#[tokio::test]
async fn watchdog_restarts_engine_after_repeated_ping_failures() {
    let temp = TempDir::new().unwrap();
    // Seed one linked account so the watchdog has a ping target.
    {
        let seed = Store::open(temp.path()).unwrap();
        seed.upsert_account_from_signal("+15555550100", Some(1))
            .unwrap();
    }
    let (supervisor, data_dir) = watchdog_supervisor(&temp);
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::write(data_dir.join(".fixture-fail-user-status"), "").unwrap();

    let pid_before = supervisor.start().await.unwrap().pid.unwrap();
    let pid_after = wait_for_pid_change(&supervisor, pid_before).await;
    assert_ne!(pid_after, pid_before);
    supervisor.shutdown().await.unwrap();
}

#[tokio::test]
async fn watchdog_leaves_a_healthy_engine_alone() {
    let temp = TempDir::new().unwrap();
    {
        let seed = Store::open(temp.path()).unwrap();
        seed.upsert_account_from_signal("+15555550100", Some(1))
            .unwrap();
    }
    let (supervisor, _data_dir) = watchdog_supervisor(&temp);

    let pid_before = supervisor.start().await.unwrap().pid.unwrap();
    // Several watchdog ticks with successful pings: no restart.
    sleep(Duration::from_millis(500)).await;
    assert_eq!(supervisor.status().await.pid, Some(pid_before));
    supervisor.shutdown().await.unwrap();
}
