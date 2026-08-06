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
    /// incoming | outgoing | system | skip
    pub direction: &'static str,
    pub account_present: bool,
    #[serde(skip)]
    pub account: Option<String>,
    /// Peer: inbound source or multi-device sent destination.
    #[serde(skip)]
    pub source: Option<String>,
    /// Human label from signal-cli envelope.sourceName when present.
    #[serde(skip)]
    pub peer_name: Option<String>,
    #[serde(skip)]
    pub group_id: Option<String>,
    #[serde(skip)]
    pub text: Option<String>,
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
            // Explicit: pull server messages as soon as the daemon is up.
            .arg("--receive-mode")
            .arg("on-start")
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
        self.call_with_timeout(method, params, class, self.request_timeout)
            .await
    }

    pub async fn call_with_timeout(
        &self,
        method: &'static str,
        params: Value,
        class: CallClass,
        request_timeout: Duration,
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

        match timeout(request_timeout, response_rx).await {
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

fn envelope_peer_source(envelope: &serde_json::Map<String, Value>) -> Option<String> {
    envelope
        .get("source")
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| {
            envelope
                .get("sourceNumber")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .or_else(|| {
            envelope
                .get("sourceUuid")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
}

fn data_message_group_id(message: &serde_json::Map<String, Value>) -> Option<String> {
    message
        .get("groupInfo")
        .and_then(|info| info.get("groupId"))
        .or_else(|| message.get("groupId"))
        .and_then(Value::as_str)
        .map(str::to_string)
}

fn data_message_text(message: &serde_json::Map<String, Value>) -> Option<String> {
    message
        .get("message")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

fn normalize_receive(params: &Value) -> Result<NormalizedReceive, EngineError> {
    let payload = params.get("result").unwrap_or(params);
    let envelope = payload
        .get("envelope")
        .and_then(Value::as_object)
        .ok_or(EngineError::Protocol)?;
    let account = payload
        .get("account")
        .and_then(Value::as_str)
        .map(str::to_string);
    let account_present = payload.get("account").is_some();
    let timestamp = envelope.get("timestamp").and_then(Value::as_u64);
    let peer_source = envelope_peer_source(envelope);
    let peer_name = envelope
        .get("sourceName")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.chars().take(64).collect::<String>());

    let make = |timestamp: Option<u64>,
                content_kind: &'static str,
                direction: &'static str,
                source: Option<String>,
                peer_name: Option<String>,
                group_id: Option<String>,
                text: Option<String>| NormalizedReceive {
        timestamp,
        content_kind,
        direction,
        account_present,
        account: account.clone(),
        source,
        peer_name,
        group_id,
        text,
    };

    // Multi-device: phone/other linked device sent a text → show as our outgoing.
    if let Some(sync) = envelope.get("syncMessage").and_then(Value::as_object) {
        if let Some(sent) = sync.get("sentMessage").and_then(Value::as_object) {
            // JsonUnwrapped dataMessage fields sit on sentMessage itself.
            let text = data_message_text(sent);
            let group_id = data_message_group_id(sent);
            let destination = sent
                .get("destinationNumber")
                .and_then(Value::as_str)
                .map(str::to_string)
                .or_else(|| {
                    sent.get("destinationUuid")
                        .and_then(Value::as_str)
                        .map(str::to_string)
                })
                .or_else(|| {
                    sent.get("destination")
                        .and_then(Value::as_str)
                        .map(str::to_string)
                });
            let sent_ts = sent
                .get("timestamp")
                .and_then(Value::as_u64)
                .or(timestamp);
            if let Some(text) = text {
                return Ok(make(
                    sent_ts,
                    "syncMessage",
                    "outgoing",
                    destination,
                    None,
                    group_id,
                    Some(text),
                ));
            }
            // sent without body (sticker/attachment sync) — skip for now
            return Ok(make(
                sent_ts,
                "syncMessage",
                "skip",
                destination,
                None,
                group_id,
                None,
            ));
        }
        // contacts/groups/read sync — ignore
        return Ok(make(
            timestamp,
            "syncMessage",
            "skip",
            peer_source,
            peer_name,
            None,
            None,
        ));
    }

    if let Some(data_message) = envelope.get("dataMessage").and_then(Value::as_object) {
        let group_id = data_message_group_id(data_message);
        let text = data_message_text(data_message);
        if let Some(text) = text {
            return Ok(make(
                timestamp,
                "dataMessage",
                "incoming",
                peer_source,
                peer_name,
                group_id,
                Some(text),
            ));
        }
        // Empty body: map a few control shapes to system rows; drop the rest.
        if data_message
            .get("isExpirationUpdate")
            .and_then(Value::as_bool)
            == Some(true)
        {
            return Ok(make(
                timestamp,
                "dataMessage",
                "system",
                peer_source,
                peer_name,
                group_id,
                Some("已更新消息定时消失".to_string()),
            ));
        }
        // Heuristic: empty control-only → system "已接受消息请求" when we have a peer
        if peer_source.is_some()
            && data_message.get("sticker").is_none()
            && data_message.get("reaction").is_none()
            && data_message.get("attachments").is_none()
        {
            return Ok(make(
                timestamp,
                "dataMessage",
                "system",
                peer_source,
                peer_name,
                group_id,
                Some("已接受消息请求".to_string()),
            ));
        }
        return Ok(make(
            timestamp,
            "dataMessage",
            "skip",
            peer_source,
            peer_name,
            group_id,
            None,
        ));
    }

    let content_kind = if envelope.contains_key("receiptMessage") {
        "receiptMessage"
    } else if envelope.contains_key("typingMessage") {
        "typingMessage"
    } else if envelope.contains_key("editMessage") {
        "editMessage"
    } else {
        "other"
    };
    Ok(make(
        timestamp,
        content_kind,
        "skip",
        peer_source,
        peer_name,
        None,
        None,
    ))
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
        assert_eq!(normalized.direction, "incoming");
        assert_eq!(normalized.text.as_deref(), Some("private text"));
        assert_eq!(normalized.account.as_deref(), Some("+15555550100"));
        assert_eq!(normalized.source.as_deref(), Some("+15555550101"));
        // Host-facing serialization must not include identity or body fields.
        assert!(!encoded.contains("private text"));
        assert!(!encoded.contains("+155"));
        assert!(!encoded.contains("source"));
        assert!(!encoded.contains("\"text\""));
        assert!(encoded.contains("accountPresent"));
        assert!(encoded.contains("incoming"));
    }

    #[test]
    fn receive_normalization_accepts_source_uuid_and_result_wrapper() {
        let input = json!({
            "subscription": 0,
            "result": {
                "envelope": {
                    "sourceUuid": "11111111-1111-1111-1111-111111111111",
                    "timestamp": 99,
                    "dataMessage": { "message": "uuid only" }
                }
            }
        });
        let normalized = normalize_receive(&input).unwrap();
        assert_eq!(normalized.content_kind, "dataMessage");
        assert_eq!(normalized.direction, "incoming");
        assert_eq!(normalized.text.as_deref(), Some("uuid only"));
        assert_eq!(
            normalized.source.as_deref(),
            Some("11111111-1111-1111-1111-111111111111")
        );
        assert!(normalized.account.is_none());
    }

    #[test]
    fn receive_normalization_sync_sent_is_outgoing() {
        let input = json!({
            "account": "+15555550100",
            "envelope": {
                "timestamp": 100,
                "syncMessage": {
                    "sentMessage": {
                        "destinationNumber": "+15555550101",
                        "timestamp": 101,
                        "message": "from phone"
                    }
                }
            }
        });
        let normalized = normalize_receive(&input).unwrap();
        assert_eq!(normalized.direction, "outgoing");
        assert_eq!(normalized.text.as_deref(), Some("from phone"));
        assert_eq!(normalized.source.as_deref(), Some("+15555550101"));
        assert_eq!(normalized.timestamp, Some(101));
    }

    #[test]
    fn receive_normalization_captures_source_name() {
        let normalized = normalize_receive(&json!({
            "envelope": {
                "source": "uuid-peer",
                "sourceName": "林菲菲",
                "timestamp": 1,
                "dataMessage": { "message": "hi" }
            }
        }))
        .unwrap();
        assert_eq!(normalized.peer_name.as_deref(), Some("林菲菲"));
        assert_eq!(normalized.direction, "incoming");
    }

    #[test]
    fn receive_normalization_empty_data_message_becomes_system_or_skip() {
        let system = normalize_receive(&json!({
            "envelope": {
                "source": "+15555550101",
                "timestamp": 50,
                "dataMessage": { "timestamp": 50 }
            }
        }))
        .unwrap();
        assert_eq!(system.direction, "system");
        assert_eq!(system.text.as_deref(), Some("已接受消息请求"));

        let skip_sticker = normalize_receive(&json!({
            "envelope": {
                "source": "+15555550101",
                "timestamp": 51,
                "dataMessage": { "sticker": { "packId": "x" } }
            }
        }))
        .unwrap();
        assert_eq!(skip_sticker.direction, "skip");
    }
}
