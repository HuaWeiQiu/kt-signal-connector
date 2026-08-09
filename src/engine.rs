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
use tokio::sync::{OwnedSemaphorePermit, Semaphore, broadcast, mpsc, oneshot, watch};
use tokio::time::timeout;
use tokio::time::{MissedTickBehavior, interval};
use tokio_util::codec::{FramedRead, LinesCodec};

use crate::resource::{
    DEFAULT_RSS_PRESSURE_BYTES, DEFAULT_RSS_PRESSURE_SAMPLES, DEFAULT_RSS_RECOVERY_BYTES,
    DEFAULT_RSS_RECOVERY_SAMPLES, ResourcePressureState, ResourcePressureTracker, sample_rss,
};
use crate::{DEFAULT_UPSTREAM_LINE_LIMIT, MAX_PENDING_UPSTREAM_REQUESTS};

const COMMAND_QUEUE_CAPACITY: usize = 128;
const EVENT_QUEUE_CAPACITY: usize = 1024;
const RECEIVE_QUEUE_CAPACITY: usize = 256;
const RECEIVE_QUEUE_BYTE_CAPACITY: usize = 2 * 1024 * 1024;
const MAX_INBOUND_TEXT_BYTES: usize = 128 * 1024;
const MAX_INBOUND_TEXT_PREVIEW_BYTES: usize = 4 * 1024;
const MAX_RECEIVE_ID_CHARS: usize = 256;
const SIGNAL_CLI_JAVA_OPTS: &str = "-Xms16m -Xmx384m";

#[derive(Clone, Debug)]
pub struct SignalCliConfig {
    pub executable: PathBuf,
    pub data_dir: PathBuf,
    pub java_home: Option<PathBuf>,
    pub line_limit: usize,
    pub request_timeout: Duration,
    pub shutdown_grace: Duration,
    pub resource_sample_interval: Duration,
}

impl SignalCliConfig {
    pub fn new(executable: PathBuf, data_dir: PathBuf) -> Self {
        Self {
            executable,
            data_dir,
            java_home: None,
            line_limit: DEFAULT_UPSTREAM_LINE_LIMIT,
            request_timeout: Duration::from_secs(15),
            shutdown_grace: Duration::from_secs(3),
            resource_sample_interval: Duration::from_secs(30),
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rss_bytes: Option<u64>,
    pub resource_pressure: bool,
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
    ProtocolWarning {
        kind: &'static str,
    },
    ResourcePressure {
        state: ResourcePressureState,
        pid: u32,
        rss_bytes: u64,
    },
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
    #[serde(skip)]
    pub text_bytes: Option<u32>,
    #[serde(skip)]
    pub text_truncated: bool,
}

pub struct QueuedReceive {
    receive: NormalizedReceive,
    _byte_permit: OwnedSemaphorePermit,
}

impl QueuedReceive {
    pub fn receive(&self) -> &NormalizedReceive {
        &self.receive
    }
}

#[derive(Clone)]
pub struct ReceiveIngress {
    sender: mpsc::Sender<QueuedReceive>,
    byte_budget: Arc<Semaphore>,
}

impl ReceiveIngress {
    pub(crate) async fn enqueue(&self, receive: NormalizedReceive) -> Result<(), EngineError> {
        let weight = receive.estimated_bytes().max(1);
        if weight > RECEIVE_QUEUE_BYTE_CAPACITY {
            return Err(EngineError::Protocol);
        }
        let permit = self
            .byte_budget
            .clone()
            .acquire_many_owned(weight as u32)
            .await
            .map_err(|_| EngineError::Exited)?;
        self.sender
            .send(QueuedReceive {
                receive,
                _byte_permit: permit,
            })
            .await
            .map_err(|_| EngineError::Exited)
    }
}

impl NormalizedReceive {
    fn estimated_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.account.as_ref().map_or(0, String::len)
            + self.source.as_ref().map_or(0, String::len)
            + self.peer_name.as_ref().map_or(0, String::len)
            + self.group_id.as_ref().map_or(0, String::len)
            + self.text.as_ref().map_or(0, String::len)
    }
}

pub fn receive_channel() -> (ReceiveIngress, mpsc::Receiver<QueuedReceive>) {
    let (sender, receiver) = mpsc::channel(RECEIVE_QUEUE_CAPACITY);
    (
        ReceiveIngress {
            sender,
            byte_budget: Arc::new(Semaphore::new(RECEIVE_QUEUE_BYTE_CAPACITY)),
        },
        receiver,
    )
}

#[derive(Clone)]
pub struct EngineHandle {
    commands: mpsc::Sender<EngineCommand>,
    status: watch::Receiver<EngineStatus>,
    events: broadcast::Sender<EngineEvent>,
    next_id: Arc<AtomicU64>,
    request_timeout: Duration,
    resource_status: watch::Receiver<(Option<u64>, bool)>,
}

impl EngineHandle {
    pub async fn start(
        config: SignalCliConfig,
        events: broadcast::Sender<EngineEvent>,
        receive_ingress: ReceiveIngress,
    ) -> Result<Self, EngineError> {
        if !config.executable.is_absolute()
            || !std::fs::symlink_metadata(&config.executable)
                .is_ok_and(|metadata| metadata.is_file() && !metadata.file_type().is_symlink())
            || config.java_home.as_ref().is_some_and(|java_home| {
                !java_home.is_absolute()
                    || !std::fs::symlink_metadata(java_home).is_ok_and(|metadata| {
                        metadata.is_dir() && !metadata.file_type().is_symlink()
                    })
                    || !java_home.join("release").is_file()
            })
            || config.resource_sample_interval < Duration::from_secs(1)
        {
            return Err(EngineError::StartFailed);
        }
        prepare_data_dir(&config.data_dir).map_err(|_| EngineError::StartFailed)?;

        let mut command = Command::new(&config.executable);
        command
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
            // Keep the documented text-runtime heap budget authoritative. Java's global
            // injection variables are removed so a parent shell cannot silently defeat it.
            .env("JAVA_OPTS", SIGNAL_CLI_JAVA_OPTS)
            .env_remove("JAVA_TOOL_OPTIONS")
            .env_remove("_JAVA_OPTIONS")
            .env_remove("JDK_JAVA_OPTIONS")
            .kill_on_drop(true);
        if let Some(java_home) = &config.java_home {
            command.env("JAVA_HOME", java_home);
        }
        let mut child = command.spawn().map_err(|_| EngineError::StartFailed)?;

        let pid = child.id().ok_or(EngineError::StartFailed)?;
        let stdin = child.stdin.take().ok_or(EngineError::StartFailed)?;
        let stdout = child.stdout.take().ok_or(EngineError::StartFailed)?;
        let stderr = child.stderr.take().ok_or(EngineError::StartFailed)?;
        let initial_status = EngineStatus {
            state: EngineState::Running,
            pid: Some(pid),
            rss_bytes: None,
            resource_pressure: false,
        };
        let (status_tx, status_rx) = watch::channel(initial_status.clone());
        let (resource_tx, resource_rx) = watch::channel((None, false));
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
                ActorChannels {
                    commands: command_rx,
                    status: status_tx,
                    events: actor_events,
                    receive_ingress,
                },
                ActorLimits {
                    line_limit: config.line_limit,
                    shutdown_grace: config.shutdown_grace,
                },
            )
            .await;
            stderr_drain.abort();
        });
        let mut monitor_status = status_rx.clone();
        let monitor_events = events.clone();
        let sample_interval = config.resource_sample_interval;
        tokio::spawn(async move {
            let mut ticker = interval(sample_interval);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            ticker.tick().await;
            let mut tracker = ResourcePressureTracker::new(
                DEFAULT_RSS_PRESSURE_BYTES,
                DEFAULT_RSS_RECOVERY_BYTES,
                DEFAULT_RSS_PRESSURE_SAMPLES,
                DEFAULT_RSS_RECOVERY_SAMPLES,
            )
            .expect("static resource policy is valid");
            loop {
                tokio::select! {
                    changed = monitor_status.changed() => {
                        if changed.is_err() || matches!(
                            monitor_status.borrow().state,
                            EngineState::Stopped | EngineState::Exited | EngineState::Faulted
                        ) {
                            let _ = resource_tx.send((None, false));
                            break;
                        }
                    }
                    _ = ticker.tick() => {
                        let sample = tokio::task::spawn_blocking(move || sample_rss(pid)).await;
                        let Ok(Ok(rss_bytes)) = sample else {
                            continue;
                        };
                        let transition = tracker.observe(rss_bytes);
                        let elevated = match transition {
                            Some(ResourcePressureState::Elevated) => true,
                            Some(ResourcePressureState::Recovered) => false,
                            None => resource_tx.borrow().1,
                        };
                        let _ = resource_tx.send((Some(rss_bytes), elevated));
                        if let Some(state) = transition {
                            let _ = monitor_events.send(EngineEvent::ResourcePressure {
                                state,
                                pid,
                                rss_bytes,
                            });
                        }
                    }
                }
            }
        });

        Ok(Self {
            commands: command_tx,
            status: status_rx,
            events,
            next_id: Arc::new(AtomicU64::new(1)),
            request_timeout: config.request_timeout,
            resource_status: resource_rx,
        })
    }

    pub fn status(&self) -> EngineStatus {
        let mut status = self.status.borrow().clone();
        let resource = *self.resource_status.borrow();
        status.rss_bytes = resource.0;
        status.resource_pressure = resource.1;
        status
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

struct ActorChannels {
    commands: mpsc::Receiver<EngineCommand>,
    status: watch::Sender<EngineStatus>,
    events: broadcast::Sender<EngineEvent>,
    receive_ingress: ReceiveIngress,
}

async fn run_actor(
    mut child: Child,
    stdin: ChildStdin,
    stdout: tokio::process::ChildStdout,
    channels: ActorChannels,
    limits: ActorLimits,
) {
    let ActorChannels {
        mut commands,
        status: status_tx,
        events,
        receive_ingress,
    } = channels;
    let process_pid = child.id();
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
                        if handle_upstream_line(
                            &line,
                            &mut pending,
                            &events,
                            &receive_ingress,
                        ).await.is_err() {
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
        rss_bytes: None,
        resource_pressure: false,
    };
    let _ = status_tx.send(status.clone());
    let _ = events.send(EngineEvent::StateChanged(EngineStatus {
        pid: process_pid,
        ..status
    }));
    for (_, request) in pending {
        let failure = match request.class {
            CallClass::ReadOnly => readonly_failure.clone(),
            CallClass::Mutating => EngineError::UnknownOutcome,
        };
        let _ = request.response.send(Err(failure));
    }
}

async fn handle_upstream_line(
    line: &str,
    pending: &mut HashMap<String, PendingRequest>,
    events: &broadcast::Sender<EngineEvent>,
    receive_ingress: &ReceiveIngress,
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
            if let Some(normalized) = normalize_receive(params)? {
                receive_ingress.enqueue(normalized).await?;
            }
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

#[derive(Clone, Debug)]
struct NormalizedText {
    value: String,
    bytes: u32,
    truncated: bool,
}

fn truncate_utf8_bytes(text: &str, max_bytes: usize) -> String {
    if text.len() <= max_bytes {
        return text.to_string();
    }
    let mut end = max_bytes;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text[..end].to_string()
}

fn normalized_text(text: &str) -> Option<NormalizedText> {
    if text.is_empty() {
        return None;
    }
    let bytes = text.len().min(u32::MAX as usize) as u32;
    let truncated = text.len() > MAX_INBOUND_TEXT_BYTES;
    Some(NormalizedText {
        value: if truncated {
            truncate_utf8_bytes(text, MAX_INBOUND_TEXT_PREVIEW_BYTES)
        } else {
            text.to_string()
        },
        bytes,
        truncated,
    })
}

fn data_message_text(message: &serde_json::Map<String, Value>) -> Option<NormalizedText> {
    message
        .get("message")
        .and_then(Value::as_str)
        .and_then(normalized_text)
}

fn normalize_receive(params: &Value) -> Result<Option<NormalizedReceive>, EngineError> {
    let normalized = normalize_receive_fields(params)?;
    // Identifier fields route conversations; an oversized one would exhaust the receive
    // queue byte budget and fault the engine. Drop the notification instead of
    // truncating the id (which would misroute) or killing the engine.
    if normalized
        .account
        .iter()
        .chain(normalized.source.iter())
        .chain(normalized.group_id.iter())
        .any(|id| id.chars().count() > MAX_RECEIVE_ID_CHARS)
    {
        return Ok(None);
    }
    Ok(Some(normalized))
}

fn normalize_receive_fields(params: &Value) -> Result<NormalizedReceive, EngineError> {
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
                text: Option<NormalizedText>| NormalizedReceive {
        timestamp,
        content_kind,
        direction,
        account_present,
        account: account.clone(),
        source,
        peer_name,
        group_id,
        text: text.as_ref().map(|value| value.value.clone()),
        text_bytes: text.as_ref().map(|value| value.bytes),
        text_truncated: text.is_some_and(|value| value.truncated),
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
            let sent_ts = sent.get("timestamp").and_then(Value::as_u64).or(timestamp);
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
                normalized_text("已更新消息定时消失"),
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
        let normalized = normalize_receive(&input).unwrap().unwrap();
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
    fn oversized_receive_text_is_projected_before_queueing() {
        let input = json!({
            "account": "+15555550100",
            "envelope": {
                "source": "+15555550101",
                "timestamp": 42,
                "dataMessage": { "message": "a".repeat(MAX_INBOUND_TEXT_BYTES + 1) }
            }
        });
        let normalized = normalize_receive(&input).unwrap().unwrap();
        assert_eq!(
            normalized.text.as_ref().map(String::len),
            Some(MAX_INBOUND_TEXT_PREVIEW_BYTES)
        );
        assert_eq!(
            normalized.text_bytes,
            Some((MAX_INBOUND_TEXT_BYTES + 1) as u32)
        );
        assert!(normalized.text_truncated);
    }

    #[tokio::test]
    async fn receive_queue_enforces_byte_budget_and_releases_it_on_consume() {
        let (ingress, mut receives) = receive_channel();
        let receive = || NormalizedReceive {
            timestamp: Some(42),
            content_kind: "dataMessage",
            direction: "incoming",
            account_present: true,
            account: Some("+15555550100".into()),
            source: Some("+15555550101".into()),
            peer_name: None,
            group_id: None,
            text: Some("a".repeat(MAX_INBOUND_TEXT_BYTES)),
            text_bytes: Some(MAX_INBOUND_TEXT_BYTES as u32),
            text_truncated: false,
        };
        let mut admitted = 0;
        while matches!(
            timeout(Duration::from_millis(10), ingress.enqueue(receive())).await,
            Ok(Ok(()))
        ) {
            admitted += 1;
        }
        assert!(admitted > 1);
        assert!(admitted < RECEIVE_QUEUE_CAPACITY);

        drop(receives.recv().await.unwrap());
        timeout(Duration::from_secs(1), ingress.enqueue(receive()))
            .await
            .unwrap()
            .unwrap();
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
        let normalized = normalize_receive(&input).unwrap().unwrap();
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
        let normalized = normalize_receive(&input).unwrap().unwrap();
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
        .unwrap()
        .unwrap();
        assert_eq!(normalized.peer_name.as_deref(), Some("林菲菲"));
        assert_eq!(normalized.direction, "incoming");
    }

    #[test]
    fn receive_normalization_only_marks_explicit_controls_as_system() {
        let empty = normalize_receive(&json!({
            "envelope": {
                "source": "+15555550101",
                "timestamp": 50,
                "dataMessage": { "timestamp": 50 }
            }
        }))
        .unwrap()
        .unwrap();
        assert_eq!(empty.direction, "skip");
        assert!(empty.text.is_none());

        let system = normalize_receive(&json!({
            "envelope": {
                "source": "+15555550101",
                "timestamp": 51,
                "dataMessage": { "isExpirationUpdate": true }
            }
        }))
        .unwrap()
        .unwrap();
        assert_eq!(system.direction, "system");
        assert_eq!(system.text.as_deref(), Some("已更新消息定时消失"));

        let skip_sticker = normalize_receive(&json!({
            "envelope": {
                "source": "+15555550101",
                "timestamp": 52,
                "dataMessage": { "sticker": { "packId": "x" } }
            }
        }))
        .unwrap()
        .unwrap();
        assert_eq!(skip_sticker.direction, "skip");
    }

    #[test]
    fn receive_normalization_preserves_text_whitespace() {
        let normalized = normalize_receive(&json!({
            "account": "+15555550100",
            "envelope": {
                "source": "+15555550101",
                "timestamp": 53,
                "dataMessage": { "message": "  exact body\n" }
            }
        }))
        .unwrap()
        .unwrap();
        assert_eq!(normalized.text.as_deref(), Some("  exact body\n"));
        assert_eq!(normalized.text_bytes, Some(13));
    }

    #[test]
    fn receive_normalization_drops_oversized_identifier_fields() {
        let with_group = |group_id: String| {
            json!({
                "account": "+15555550100",
                "envelope": {
                    "source": "+15555550101",
                    "timestamp": 42,
                    "dataMessage": {
                        "message": "group text",
                        "groupInfo": { "groupId": group_id }
                    }
                }
            })
        };
        assert!(
            normalize_receive(&with_group("g".repeat(MAX_RECEIVE_ID_CHARS + 1)))
                .unwrap()
                .is_none()
        );
        assert!(
            normalize_receive(&with_group("g".repeat(MAX_RECEIVE_ID_CHARS)))
                .unwrap()
                .is_some()
        );

        let oversized_source = json!({
            "account": "+15555550100",
            "envelope": {
                "source": "s".repeat(MAX_RECEIVE_ID_CHARS + 1),
                "timestamp": 42,
                "dataMessage": { "message": "hi" }
            }
        });
        assert!(normalize_receive(&oversized_source).unwrap().is_none());

        let oversized_account = json!({
            "account": "a".repeat(MAX_RECEIVE_ID_CHARS + 1),
            "envelope": {
                "source": "+15555550101",
                "timestamp": 42,
                "dataMessage": { "message": "hi" }
            }
        });
        assert!(normalize_receive(&oversized_account).unwrap().is_none());
    }

    #[tokio::test]
    async fn oversized_receive_notification_is_dropped_without_faulting() {
        let (events, _unused) = event_channel();
        let (ingress, mut receiver) = receive_channel();
        let mut pending = HashMap::new();

        let oversized = serde_json::to_string(&json!({
            "jsonrpc": "2.0",
            "method": "receive",
            "params": {
                "account": "+15555550100",
                "envelope": {
                    "source": "+15555550101",
                    "timestamp": 1,
                    "dataMessage": {
                        "message": "oversized group",
                        "groupInfo": { "groupId": "g".repeat(MAX_RECEIVE_ID_CHARS + 1) }
                    }
                }
            }
        }))
        .unwrap();
        handle_upstream_line(&oversized, &mut pending, &events, &ingress)
            .await
            .unwrap();

        let well_formed = serde_json::to_string(&json!({
            "jsonrpc": "2.0",
            "method": "receive",
            "params": {
                "account": "+15555550100",
                "envelope": {
                    "source": "+15555550101",
                    "timestamp": 2,
                    "dataMessage": { "message": "after oversized" }
                }
            }
        }))
        .unwrap();
        handle_upstream_line(&well_formed, &mut pending, &events, &ingress)
            .await
            .unwrap();

        let queued = receiver.recv().await.unwrap();
        assert_eq!(queued.receive().text.as_deref(), Some("after oversized"));
        assert!(receiver.try_recv().is_err());
    }
}
