// SPDX-License-Identifier: AGPL-3.0-only

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures_util::StreamExt;
use serde::Serialize;
use serde_json::{Value, json};
use thiserror::Error;
use tokio::io::{AsyncWriteExt, sink};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tokio::time::timeout;
use tokio_util::codec::{FramedRead, LinesCodec};

use crate::{DEFAULT_UPSTREAM_LINE_LIMIT, MAX_PENDING_UPSTREAM_REQUESTS};

const COMMAND_QUEUE_CAPACITY: usize = 128;
const EVENT_QUEUE_CAPACITY: usize = 1024;

#[derive(Clone, Debug)]
pub struct SignalCliConfig {
    pub executable: PathBuf,
    pub data_dir: PathBuf,
    pub line_limit: usize,
    pub request_timeout: Duration,
    pub shutdown_grace: Duration,
}

impl SignalCliConfig {
    pub fn new(executable: PathBuf, data_dir: PathBuf) -> Self {
        Self {
            executable,
            data_dir,
            line_limit: DEFAULT_UPSTREAM_LINE_LIMIT,
            request_timeout: Duration::from_secs(15),
            shutdown_grace: Duration::from_secs(3),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum EngineState {
    Running,
    Stopped,
    Exited,
    Faulted,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EngineStatus {
    pub state: EngineState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallClass {
    ReadOnly,
    Mutating,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum EngineError {
    #[error("signal-cli process could not be started")]
    StartFailed,
    #[error("signal-cli is not running")]
    NotRunning,
    #[error("signal-cli request queue is full")]
    Backpressure,
    #[error("signal-cli request timed out")]
    Timeout,
    #[error("signal-cli mutating request has an unknown outcome")]
    UnknownOutcome,
    #[error("signal-cli exited before completing the request")]
    Exited,
    #[error("signal-cli produced invalid protocol output")]
    Protocol,
    #[error("signal-cli returned an error")]
    Upstream,
}

#[derive(Clone, Debug)]
pub enum EngineEvent {
    StateChanged(EngineStatus),
    Receive(NormalizedReceive),
    ProtocolWarning { kind: &'static str },
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NormalizedReceive {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<u64>,
    pub content_kind: &'static str,
    pub account_present: bool,
}

#[derive(Clone)]
pub struct EngineHandle {
    commands: mpsc::Sender<EngineCommand>,
    status: watch::Receiver<EngineStatus>,
    events: broadcast::Sender<EngineEvent>,
    next_id: Arc<AtomicU64>,
    request_timeout: Duration,
}

impl EngineHandle {
    pub async fn start(
        config: SignalCliConfig,
        events: broadcast::Sender<EngineEvent>,
    ) -> Result<Self, EngineError> {
        if !config.executable.is_absolute()
            || !std::fs::metadata(&config.executable).is_ok_and(|metadata| metadata.is_file())
        {
            return Err(EngineError::StartFailed);
        }
        prepare_data_dir(&config.data_dir).map_err(|_| EngineError::StartFailed)?;

        let mut child = Command::new(&config.executable)
            .arg("--data-dir")
            .arg(&config.data_dir)
            .arg("jsonRpc")
            .arg("--ignore-attachments")
            .arg("--ignore-stories")
            .arg("--ignore-stickers")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|_| EngineError::StartFailed)?;

        let pid = child.id();
        let stdin = child.stdin.take().ok_or(EngineError::StartFailed)?;
        let stdout = child.stdout.take().ok_or(EngineError::StartFailed)?;
        let stderr = child.stderr.take().ok_or(EngineError::StartFailed)?;
        let initial_status = EngineStatus {
            state: EngineState::Running,
            pid,
        };
        let (status_tx, status_rx) = watch::channel(initial_status.clone());
        let (command_tx, command_rx) = mpsc::channel(COMMAND_QUEUE_CAPACITY);

        let _ = events.send(EngineEvent::StateChanged(initial_status));
        let actor_events = events.clone();
        tokio::spawn(async move {
            let mut stderr = stderr;
            let stderr_drain = tokio::spawn(async move {
                let _ = tokio::io::copy(&mut stderr, &mut sink()).await;
            });
            run_actor(
                child,
                stdin,
                stdout,
                command_rx,
                status_tx,
                actor_events,
                ActorLimits {
                    line_limit: config.line_limit,
                    shutdown_grace: config.shutdown_grace,
                },
            )
            .await;
            stderr_drain.abort();
        });

        Ok(Self {
            commands: command_tx,
            status: status_rx,
            events,
            next_id: Arc::new(AtomicU64::new(1)),
            request_timeout: config.request_timeout,
        })
    }

    pub fn status(&self) -> EngineStatus {
        self.status.borrow().clone()
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self.status().state,
            EngineState::Stopped | EngineState::Exited | EngineState::Faulted
        )
    }

    pub fn subscribe(&self) -> broadcast::Receiver<EngineEvent> {
        self.events.subscribe()
    }

    pub async fn call(
        &self,
        method: &'static str,
        params: Value,
        class: CallClass,
    ) -> Result<Value, EngineError> {
        if self.is_terminal() {
            return Err(EngineError::NotRunning);
        }
        let id = format!("kt-{}", self.next_id.fetch_add(1, Ordering::Relaxed));
        let (response_tx, response_rx) = oneshot::channel();
        match self.commands.try_send(EngineCommand::Request {
            id: id.clone(),
            method,
            params,
            class,
            response: response_tx,
        }) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => return Err(EngineError::Backpressure),
            Err(mpsc::error::TrySendError::Closed(_)) => return Err(EngineError::NotRunning),
        }

        match timeout(self.request_timeout, response_rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(EngineError::Exited),
            Err(_) => {
                let commands = self.commands.clone();
                tokio::spawn(async move {
                    let _ = commands.send(EngineCommand::Cancel { id }).await;
                });
                match class {
                    CallClass::ReadOnly => Err(EngineError::Timeout),
                    CallClass::Mutating => Err(EngineError::UnknownOutcome),
                }
            }
        }
    }

    pub async fn shutdown(&self) -> Result<(), EngineError> {
        if self.is_terminal() {
            return Ok(());
        }
        let (response_tx, response_rx) = oneshot::channel();
        self.commands
            .send(EngineCommand::Shutdown {
                response: response_tx,
            })
            .await
            .map_err(|_| EngineError::Exited)?;
        response_rx.await.map_err(|_| EngineError::Exited)
    }
}

enum EngineCommand {
    Request {
        id: String,
        method: &'static str,
        params: Value,
        class: CallClass,
        response: oneshot::Sender<Result<Value, EngineError>>,
    },
    Cancel {
        id: String,
    },
    Shutdown {
        response: oneshot::Sender<()>,
    },
}

struct PendingRequest {
    class: CallClass,
    response: oneshot::Sender<Result<Value, EngineError>>,
}

struct ActorLimits {
    line_limit: usize,
    shutdown_grace: Duration,
}

async fn run_actor(
    mut child: Child,
    stdin: ChildStdin,
    stdout: tokio::process::ChildStdout,
    mut commands: mpsc::Receiver<EngineCommand>,
    status_tx: watch::Sender<EngineStatus>,
    events: broadcast::Sender<EngineEvent>,
    limits: ActorLimits,
) {
    let mut stdin = Some(stdin);
    let mut lines = FramedRead::new(stdout, LinesCodec::new_with_max_length(limits.line_limit));
    let mut pending = HashMap::<String, PendingRequest>::new();
    let terminal_state;

    loop {
        tokio::select! {
            command = commands.recv() => {
                match command {
                    Some(EngineCommand::Request { id, method, params, class, response }) => {
                        if pending.len() >= MAX_PENDING_UPSTREAM_REQUESTS {
                            let _ = response.send(Err(EngineError::Backpressure));
                            continue;
                        }
                        let request = json!({
                            "jsonrpc": "2.0",
                            "id": id,
                            "method": method,
                            "params": params,
                        });
                        let mut encoded = match serde_json::to_vec(&request) {
                            Ok(value) => value,
                            Err(_) => {
                                let _ = response.send(Err(EngineError::Protocol));
                                continue;
                            }
                        };
                        encoded.push(b'\n');
                        let Some(writer) = stdin.as_mut() else {
                            let _ = response.send(Err(EngineError::Exited));
                            terminal_state = EngineState::Exited;
                            break;
                        };
                        if writer.write_all(&encoded).await.is_err() || writer.flush().await.is_err() {
                            let failure = match class {
                                CallClass::ReadOnly => EngineError::Exited,
                                CallClass::Mutating => EngineError::UnknownOutcome,
                            };
                            let _ = response.send(Err(failure));
                            terminal_state = EngineState::Exited;
                            break;
                        }
                        pending.insert(id, PendingRequest { class, response });
                    }
                    Some(EngineCommand::Cancel { id }) => {
                        pending.remove(&id);
                    }
                    Some(EngineCommand::Shutdown { response }) => {
                        stdin.take();
                        stop_child(&mut child, limits.shutdown_grace).await;
                        let _ = response.send(());
                        terminal_state = EngineState::Stopped;
                        break;
                    }
                    None => {
                        stdin.take();
                        stop_child(&mut child, limits.shutdown_grace).await;
                        terminal_state = EngineState::Stopped;
                        break;
                    }
                }
            }
            line = lines.next() => {
                match line {
                    Some(Ok(line)) => {
                        if handle_upstream_line(&line, &mut pending, &events).is_err() {
                            terminal_state = EngineState::Faulted;
                            break;
                        }
                    }
                    Some(Err(_)) => {
                        terminal_state = EngineState::Faulted;
                        break;
                    }
                    None => {
                        terminal_state = EngineState::Exited;
                        break;
                    }
                }
            }
        }
    }

    if !matches!(terminal_state, EngineState::Stopped) {
        stdin.take();
        stop_child(&mut child, limits.shutdown_grace).await;
    }
    let readonly_failure = match terminal_state {
        EngineState::Faulted => EngineError::Protocol,
        _ => EngineError::Exited,
    };
    let status = EngineStatus {
        state: terminal_state,
        pid: None,
    };
    let _ = status_tx.send(status.clone());
    let _ = events.send(EngineEvent::StateChanged(status));
    for (_, request) in pending {
        let failure = match request.class {
            CallClass::ReadOnly => readonly_failure.clone(),
            CallClass::Mutating => EngineError::UnknownOutcome,
        };
        let _ = request.response.send(Err(failure));
    }
}

fn handle_upstream_line(
    line: &str,
    pending: &mut HashMap<String, PendingRequest>,
    events: &broadcast::Sender<EngineEvent>,
) -> Result<(), EngineError> {
    let message: Value = serde_json::from_str(line).map_err(|_| EngineError::Protocol)?;
    let object = message.as_object().ok_or(EngineError::Protocol)?;
    if object.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return Err(EngineError::Protocol);
    }

    if let Some(method) = object.get("method").and_then(Value::as_str) {
        if object.contains_key("id") {
            return Err(EngineError::Protocol);
        }
        if method == "receive" {
            let params = object.get("params").ok_or(EngineError::Protocol)?;
            let normalized = normalize_receive(params)?;
            let _ = events.send(EngineEvent::Receive(normalized));
        } else {
            let _ = events.send(EngineEvent::ProtocolWarning {
                kind: "unknownNotification",
            });
        }
        return Ok(());
    }

    let id = object
        .get("id")
        .and_then(Value::as_str)
        .ok_or(EngineError::Protocol)?;
    let Some(request) = pending.remove(id) else {
        let _ = events.send(EngineEvent::ProtocolWarning {
            kind: "unknownOrDuplicateResponseId",
        });
        return Ok(());
    };
    let has_result = object.contains_key("result");
    let has_error = object.contains_key("error");
    if has_result == has_error {
        let error = match request.class {
            CallClass::ReadOnly => EngineError::Protocol,
            CallClass::Mutating => EngineError::UnknownOutcome,
        };
        let _ = request.response.send(Err(error));
        return Err(EngineError::Protocol);
    }
    if has_error {
        let _ = request.response.send(Err(EngineError::Upstream));
    } else {
        let result = object.get("result").cloned().unwrap_or(Value::Null);
        let _ = request.response.send(Ok(result));
    }
    Ok(())
}

fn normalize_receive(params: &Value) -> Result<NormalizedReceive, EngineError> {
    let payload = params.get("result").unwrap_or(params);
    let envelope = payload
        .get("envelope")
        .and_then(Value::as_object)
        .ok_or(EngineError::Protocol)?;
    let content_kind = if envelope.contains_key("dataMessage") {
        "dataMessage"
    } else if envelope.contains_key("syncMessage") {
        "syncMessage"
    } else if envelope.contains_key("receiptMessage") {
        "receiptMessage"
    } else if envelope.contains_key("typingMessage") {
        "typingMessage"
    } else {
        "other"
    };
    Ok(NormalizedReceive {
        timestamp: envelope.get("timestamp").and_then(Value::as_u64),
        content_kind,
        account_present: payload.get("account").is_some(),
    })
}

async fn stop_child(child: &mut Child, grace: Duration) {
    if timeout(grace, child.wait()).await.is_err() {
        let _ = child.start_kill();
        let _ = child.wait().await;
    }
}

fn prepare_data_dir(path: &Path) -> std::io::Result<()> {
    if !path.is_absolute() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "signal data directory must be absolute",
        ));
    }
    std::fs::create_dir_all(path)?;
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "signal data directory must be a real directory",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        if metadata.uid() != rustix::process::getuid().as_raw() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "signal data directory must belong to the current user",
            ));
        }
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

pub fn event_channel() -> (
    broadcast::Sender<EngineEvent>,
    broadcast::Receiver<EngineEvent>,
) {
    broadcast::channel(EVENT_QUEUE_CAPACITY)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn receive_normalization_does_not_expose_message_or_identity() {
        let input = json!({
            "account": "+15555550100",
            "envelope": {
                "source": "+15555550101",
                "timestamp": 42,
                "dataMessage": { "message": "private text" }
            }
        });
        let normalized = normalize_receive(&input).unwrap();
        let encoded = serde_json::to_string(&normalized).unwrap();
        assert_eq!(normalized.timestamp, Some(42));
        assert_eq!(normalized.content_kind, "dataMessage");
        assert!(!encoded.contains("private text"));
        assert!(!encoded.contains("+155"));
    }
}
