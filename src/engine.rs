// SPDX-License-Identifier: AGPL-3.0-only

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use serde::Serialize;
use serde_json::{Value, json};
use thiserror::Error;
use tokio::io::{AsyncWriteExt, sink};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, broadcast, mpsc, oneshot, watch};
use tokio::time::timeout;
use tokio::time::{MissedTickBehavior, interval};
use tokio_util::codec::{FramedRead, LinesCodec};

use crate::resource::{
    DEFAULT_RSS_PRESSURE_BYTES, DEFAULT_RSS_PRESSURE_SAMPLES, DEFAULT_RSS_RECOVERY_BYTES,
    DEFAULT_RSS_RECOVERY_SAMPLES, ResourcePressureState, ResourcePressureTracker, sample_rss,
};
use crate::{DEFAULT_UPSTREAM_LINE_LIMIT, MAX_PENDING_UPSTREAM_REQUESTS};

const COMMAND_QUEUE_CAPACITY: usize = 128;
/// Bounded queue of framed requests waiting for the child's stdin (A7): the
/// actor hands whole encoded requests to a dedicated writer task, so a child
/// that stops reading its stdin parks only that task, never the actor loop.
/// Frames are the connector's own validated requests (tens of KiB at most),
/// so a full queue bounds to well under 2 MiB of in-flight bytes. A full
/// queue answers Backpressure, the same admission failure as the pending map
/// cap; the queue can only fill when the pipe itself is already full.
pub const STDIN_WRITE_QUEUE_CAPACITY: usize = 16;
const EVENT_QUEUE_CAPACITY: usize = 1024;
const RECEIVE_QUEUE_CAPACITY: usize = 256;
const RECEIVE_QUEUE_BYTE_CAPACITY: usize = 2 * 1024 * 1024;
/// How long receive admission may wait on queue capacity before the receive
/// is dropped instead of parking the engine actor. Healthy persistence drains
/// the queue in milliseconds; a queue that stays full for this long means
/// storage is not draining, and the actor must stay responsive to its command
/// lane (including shutdown) instead of backpressuring forever.
const RECEIVE_ENQUEUE_TIMEOUT: Duration = Duration::from_secs(30);
/// Slack over the child graceful-exit budget after which an unanswered
/// shutdown request is considered stuck and the child is killed directly.
const SHUTDOWN_TIMEOUT_MARGIN: Duration = Duration::from_secs(2);
const MAX_INBOUND_TEXT_BYTES: usize = 128 * 1024;
const MAX_INBOUND_TEXT_PREVIEW_BYTES: usize = 4 * 1024;
/// Receive routing ids (account, source, group id). These ids become stored
/// peer keys, which the schema bounds at 128 (opaqueId/peerKey family in
/// schemas/connector-api-v1.schema.json); accepting a longer one inbound would
/// create a conversation the host contract forbids addressing.
const MAX_RECEIVE_ID_CHARS: usize = 128;
const STDERR_QUEUE_CAPACITY: usize = 64;
const STDERR_LINE_LIMIT: usize = 4 * 1024;
const SIGNAL_CLI_JAVA_OPTS: &str = "-Xms16m -Xmx384m";

/// How the configured signal-cli executable is launched.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SignalCliMode {
    /// The upstream launcher script: a JVM is started and the heap budget and
    /// SOCKS proxy reach it through the `JAVA_OPTS` environment variable.
    #[default]
    Jvm,
    /// A GraalVM native-image single-file binary (Phase 5): spawned directly
    /// with identical CLI arguments. There is no JVM, so `JAVA_HOME` is not
    /// validated or forwarded and no `JAVA_OPTS` are injected; a configured
    /// SOCKS proxy is passed as leading `-DsocksProxyHost/-DsocksProxyPort`
    /// argv entries, which the native-image launcher applies as runtime
    /// system properties (verified against signal-cli 0.14.7 native on
    /// 2026-08-26: the binary spoke SOCKS5 to a local relay and completed a
    /// staging provisioning round-trip through it). Note this necessarily
    /// puts proxy host:port on the child command line; the JVM mode keeps it
    /// in the environment.
    Native,
}

#[derive(Clone, Debug)]
pub struct SignalCliConfig {
    pub executable: PathBuf,
    pub data_dir: PathBuf,
    pub mode: SignalCliMode,
    pub java_home: Option<PathBuf>,
    pub proxy: Option<SocksProxy>,
    pub line_limit: usize,
    pub request_timeout: Duration,
    pub shutdown_grace: Duration,
    pub receive_enqueue_timeout: Duration,
    pub resource_sample_interval: Duration,
    pub watchdog_interval: Duration,
    pub watchdog_min_restart_interval: Duration,
}

impl SignalCliConfig {
    pub fn new(executable: PathBuf, data_dir: PathBuf) -> Self {
        Self {
            executable,
            data_dir,
            mode: SignalCliMode::Jvm,
            java_home: None,
            proxy: None,
            line_limit: DEFAULT_UPSTREAM_LINE_LIMIT,
            request_timeout: Duration::from_secs(15),
            shutdown_grace: Duration::from_secs(3),
            receive_enqueue_timeout: RECEIVE_ENQUEUE_TIMEOUT,
            resource_sample_interval: Duration::from_secs(30),
            watchdog_interval: Duration::from_secs(30),
            watchdog_min_restart_interval: Duration::from_secs(300),
        }
    }
}

/// Optional SOCKS proxy for the signal-cli child. signal-cli does not read OS
/// proxy settings in either mode: in JVM mode the proxy is injected into the
/// launcher via `-DsocksProxyHost/-DsocksProxyPort` inside `JAVA_OPTS`; in
/// native mode the same properties are passed as leading argv entries, which
/// the GraalVM native-image launcher applies at runtime (see
/// [`SignalCliMode::Native`]).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SocksProxy {
    pub host: String,
    pub port: u16,
}

impl SocksProxy {
    /// Parse `host:port`. The host must be non-empty, at most 253 bytes, and
    /// free of whitespace and ':' (no IPv6 literals — JAVA_OPTS is
    /// whitespace-split by the signal-cli launcher script); the port must be
    /// 1-65535.
    pub fn parse(value: &str) -> Result<Self, String> {
        let (host, port) = value
            .split_once(':')
            .ok_or_else(|| "proxy must use host:port".to_string())?;
        if host.is_empty()
            || host.len() > 253
            || host.chars().any(|c| c.is_whitespace() || c == ':')
        {
            return Err(
                "proxy host must be non-empty and contain no whitespace or ':'".to_string(),
            );
        }
        let port: u16 = port
            .parse()
            .map_err(|_| "proxy port must be a number between 1 and 65535".to_string())?;
        if port == 0 {
            return Err("proxy port must be a number between 1 and 65535".to_string());
        }
        Ok(Self {
            host: host.to_string(),
            port,
        })
    }
}

impl std::str::FromStr for SocksProxy {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

/// JAVA_OPTS passed to the signal-cli launcher: the documented heap budget
/// plus the SOCKS proxy flags when a proxy is configured. JVM mode only.
fn java_opts(proxy: Option<&SocksProxy>) -> String {
    match proxy {
        Some(proxy) => format!(
            "{SIGNAL_CLI_JAVA_OPTS} -DsocksProxyHost={} -DsocksProxyPort={}",
            proxy.host, proxy.port
        ),
        None => SIGNAL_CLI_JAVA_OPTS.to_string(),
    }
}

/// signal-cli CLI arguments, identical in both modes except that native mode
/// prepends the SOCKS proxy as runtime system properties for the GraalVM
/// native-image launcher. The launcher consumes `-D` entries anywhere on the
/// command line before the app parses its own arguments; leading position is
/// convention, not requirement.
fn signal_cli_args(
    mode: SignalCliMode,
    proxy: Option<&SocksProxy>,
    data_dir: &Path,
) -> Vec<std::ffi::OsString> {
    let mut args: Vec<std::ffi::OsString> = Vec::new();
    if mode == SignalCliMode::Native
        && let Some(proxy) = proxy
    {
        args.push(format!("-DsocksProxyHost={}", proxy.host).into());
        args.push(format!("-DsocksProxyPort={}", proxy.port).into());
    }
    args.push("--data-dir".into());
    args.push(data_dir.as_os_str().into());
    args.push("jsonRpc".into());
    // Explicit: pull server messages as soon as the daemon is up.
    args.push("--receive-mode".into());
    args.push("on-start".into());
    args.push("--ignore-attachments".into());
    args.push("--ignore-stories".into());
    args.push("--ignore-stickers".into());
    args
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

impl EngineError {
    /// Log-safe variant classification: the variants are deliberately
    /// content-free, and logs carry this class rather than any detail.
    fn class(&self) -> &'static str {
        match self {
            EngineError::StartFailed => "start_failed",
            EngineError::NotRunning => "not_running",
            EngineError::Backpressure => "backpressure",
            EngineError::Timeout => "timeout",
            EngineError::UnknownOutcome => "unknown_outcome",
            EngineError::Exited => "exited",
            EngineError::Protocol => "protocol",
            EngineError::Upstream => "upstream",
        }
    }
}

fn call_class_label(class: CallClass) -> &'static str {
    match class {
        CallClass::ReadOnly => "read",
        CallClass::Mutating => "mutating",
    }
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
    /// The receive queue could not be drained for a full enqueue timeout, so
    /// receives are being dropped: local persistence is degraded even though
    /// the engine process itself is alive. `state` is `unavailable` on the
    /// first drop and `recovered` when admission succeeds again; the host
    /// serializes it as the existing `runtime.storageChanged` event.
    StorageChanged {
        state: &'static str,
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

/// Outcome of one receive admission attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EnqueueOutcome {
    Enqueued,
    /// The queue stayed full for the whole enqueue timeout, so the receive
    /// was dropped instead of parking the engine actor. This is the degraded
    /// path for a store that stopped draining the queue; the persistence
    /// loop reports `runtime.storageChanged` for the underlying failure.
    Dropped,
}

#[derive(Clone)]
pub struct ReceiveIngress {
    sender: mpsc::Sender<QueuedReceive>,
    byte_budget: Arc<Semaphore>,
    enqueue_timeout: Duration,
}

impl ReceiveIngress {
    pub(crate) async fn enqueue(
        &self,
        receive: NormalizedReceive,
    ) -> Result<EnqueueOutcome, EngineError> {
        let weight = receive.estimated_bytes().max(1);
        if weight > RECEIVE_QUEUE_BYTE_CAPACITY {
            return Err(EngineError::Protocol);
        }
        // Both waits are bounded. While storage is down the persistence loop
        // keeps retrying the head of the queue, so an unbounded wait here
        // would park the engine actor and starve its command lane (including
        // shutdown). Both futures are cancel-safe, so a timeout cannot lose a
        // half-acquired permit; the permit taken by a timed-out send drops
        // with it and releases its budget.
        let acquire = self.byte_budget.clone().acquire_many_owned(weight as u32);
        let permit = match timeout(self.enqueue_timeout, acquire).await {
            Ok(Ok(permit)) => permit,
            Ok(Err(_)) => return Err(EngineError::Exited),
            Err(_) => return Ok(EnqueueOutcome::Dropped),
        };
        match timeout(
            self.enqueue_timeout,
            self.sender.send(QueuedReceive {
                receive,
                _byte_permit: permit,
            }),
        )
        .await
        {
            Ok(Ok(())) => {
                crate::metrics::receive_queue_enqueued();
                Ok(EnqueueOutcome::Enqueued)
            }
            Ok(Err(_)) => Err(EngineError::Exited),
            Err(_) => Ok(EnqueueOutcome::Dropped),
        }
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
            enqueue_timeout: RECEIVE_ENQUEUE_TIMEOUT,
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
    /// Total budget for a graceful shutdown answer before the child is killed
    /// directly. Derived from the graceful-exit budget plus a fixed margin.
    shutdown_timeout: Duration,
    /// Shared with the actor so a stuck actor cannot veto killing the child:
    /// the actor only locks it briefly inside `stop_child`, never while
    /// parked on queue backpressure.
    child: Arc<Mutex<Child>>,
    resource_status: watch::Receiver<(Option<u64>, bool)>,
    stderr: broadcast::Sender<String>,
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
            || (config.mode == SignalCliMode::Jvm
                && config.java_home.as_ref().is_some_and(|java_home| {
                    !java_home.is_absolute()
                        || !std::fs::symlink_metadata(java_home).is_ok_and(|metadata| {
                            metadata.is_dir() && !metadata.file_type().is_symlink()
                        })
                        || !java_home.join("release").is_file()
                }))
            || config.resource_sample_interval < Duration::from_secs(1)
        {
            return Err(EngineError::StartFailed);
        }
        prepare_data_dir(&config.data_dir).map_err(|_| EngineError::StartFailed)?;
        if config.mode == SignalCliMode::Native && config.java_home.is_some() {
            tracing::warn!("native signal-cli mode ignores the configured JAVA_HOME");
        }

        let mut command = Command::new(&config.executable);
        command
            .args(signal_cli_args(
                config.mode,
                config.proxy.as_ref(),
                &config.data_dir,
            ))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            // The dev/test store-key override must never leak into the child's
            // environment (Phase 3: keys travel the bootstrap channel only).
            .env_remove(crate::store::STORE_KEY_ENV)
            .kill_on_drop(true);
        if config.mode == SignalCliMode::Jvm {
            // Keep the documented text-runtime heap budget authoritative. Java's global
            // injection variables are removed so a parent shell cannot silently defeat it.
            // A configured SOCKS proxy is injected here because signal-cli does not read
            // OS proxy settings; watchdog restarts reuse the same config unchanged.
            command
                .env("JAVA_OPTS", java_opts(config.proxy.as_ref()))
                .env_remove("JAVA_TOOL_OPTIONS")
                .env_remove("_JAVA_OPTIONS")
                .env_remove("JDK_JAVA_OPTIONS");
            if let Some(java_home) = &config.java_home {
                command.env("JAVA_HOME", java_home);
            }
        }
        let mut child = command.spawn().map_err(|_| EngineError::StartFailed)?;

        let pid = child.id().ok_or(EngineError::StartFailed)?;
        let stdin = child.stdin.take().ok_or(EngineError::StartFailed)?;
        let stdout = child.stdout.take().ok_or(EngineError::StartFailed)?;
        let stderr = child.stderr.take().ok_or(EngineError::StartFailed)?;
        let child = Arc::new(Mutex::new(child));
        // The configured enqueue timeout is authoritative for every engine
        // started from this config, regardless of how the ingress was built.
        let receive_ingress = ReceiveIngress {
            enqueue_timeout: config.receive_enqueue_timeout,
            ..receive_ingress
        };
        let initial_status = EngineStatus {
            state: EngineState::Running,
            pid: Some(pid),
            rss_bytes: None,
            resource_pressure: false,
        };
        let (status_tx, status_rx) = watch::channel(initial_status.clone());
        let (resource_tx, resource_rx) = watch::channel((None, false));
        let (command_tx, command_rx) = mpsc::channel(COMMAND_QUEUE_CAPACITY);
        let (stderr_tx, _) = broadcast::channel::<String>(STDERR_QUEUE_CAPACITY);

        let _ = events.send(EngineEvent::StateChanged(initial_status));
        let actor_events = events.clone();
        let stderr_lines = stderr_tx.clone();
        let actor_child = Arc::clone(&child);
        tokio::spawn(async move {
            let stderr_drain = tokio::spawn(async move {
                // Forward signal-cli stderr line by line (bounded) so the watchdog can
                // observe receive WebSocket failures. Send failures/lag are ignored;
                // the drain must never panic or block the actor. If framing fails the
                // remaining bytes are discarded so the child never blocks on a full pipe.
                let mut lines =
                    FramedRead::new(stderr, LinesCodec::new_with_max_length(STDERR_LINE_LIMIT));
                loop {
                    match lines.next().await {
                        Some(Ok(line)) => {
                            let _ = stderr_lines.send(line);
                        }
                        Some(Err(_)) => {
                            let mut rest = lines.into_inner();
                            let _ = tokio::io::copy(&mut rest, &mut sink()).await;
                            break;
                        }
                        None => break,
                    }
                }
            });
            run_actor(
                actor_child,
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

        tracing::info!(pid, "signal-cli engine started");
        Ok(Self {
            commands: command_tx,
            status: status_rx,
            events,
            next_id: Arc::new(AtomicU64::new(1)),
            request_timeout: config.request_timeout,
            shutdown_timeout: config.shutdown_grace + SHUTDOWN_TIMEOUT_MARGIN,
            child,
            resource_status: resource_rx,
            stderr: stderr_tx,
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

    /// Line-based stream of the signal-cli stderr output (bounded, lossy under lag).
    pub fn subscribe_stderr(&self) -> broadcast::Receiver<String> {
        self.stderr.subscribe()
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
        let started = Instant::now();
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

        let outcome = match timeout(request_timeout, response_rx).await {
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
        };
        // signal-cli method names are fixed crate-internal literals; only the
        // classified error variant is logged, never upstream error content.
        match &outcome {
            Ok(_) => tracing::debug!(
                upstream_method = method,
                call_class = call_class_label(class),
                duration_ms = started.elapsed().as_millis() as u64,
                result = "ok",
                "signal-cli call completed"
            ),
            Err(error) => tracing::warn!(
                upstream_method = method,
                call_class = call_class_label(class),
                duration_ms = started.elapsed().as_millis() as u64,
                error_class = error.class(),
                "signal-cli call failed"
            ),
        }
        outcome
    }

    /// Graceful stop with a total timeout. The command lane can be starved
    /// when the actor is parked outside its `select!` (e.g. on receive-queue
    /// backpressure while storage is down), so an unanswered shutdown kills
    /// the child directly instead of waiting forever. The kill is the same
    /// forced-stop step `stop_child` falls back to after its grace period;
    /// the actor then observes the closed pipes and unwinds on its own.
    pub async fn shutdown(&self) -> Result<(), EngineError> {
        if self.is_terminal() {
            return Ok(());
        }
        let (response_tx, response_rx) = oneshot::channel();
        let graceful = async {
            self.commands
                .send(EngineCommand::Shutdown {
                    response: response_tx,
                })
                .await
                .map_err(|_| EngineError::Exited)?;
            response_rx.await.map_err(|_| EngineError::Exited)
        };
        match timeout(self.shutdown_timeout, graceful).await {
            Ok(result) => result,
            Err(_) => {
                tracing::warn!(
                    timeout_ms = self.shutdown_timeout.as_millis() as u64,
                    "engine shutdown unanswered; killing signal-cli process"
                );
                // The actor only holds this lock briefly inside `stop_child`,
                // so a parked actor cannot block the kill.
                let _ = self.child.lock().await.start_kill();
                Ok(())
            }
        }
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
    child: Arc<Mutex<Child>>,
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
    let process_pid = child.lock().await.id();
    // The single stdin writer lives in its own task (A7): an inline write_all
    // parked the whole actor whenever the child stopped reading its stdin,
    // stalling the stdout pump and the command lane with it. The actor now
    // queues whole frames; the writer keeps writing them in order, and a
    // full queue or a dead writer is reported through the queue instead of
    // blocking the loop.
    let (write_tx, write_rx) = mpsc::channel::<Vec<u8>>(STDIN_WRITE_QUEUE_CAPACITY);
    let (writer_failed_tx, mut writer_failed_rx) = mpsc::channel::<()>(1);
    let writer = tokio::spawn(run_stdin_writer(stdin, write_rx, writer_failed_tx));
    let mut lines = FramedRead::new(stdout, LinesCodec::new_with_max_length(limits.line_limit));
    let mut pending = HashMap::<String, PendingRequest>::new();
    // Whether dropped receives have been reported as degraded storage; reset
    // when admission succeeds again so a `recovered` state is emitted once.
    let mut receive_degraded = false;
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
                        match write_tx.try_send(encoded) {
                            Ok(()) => {
                                pending.insert(id, PendingRequest { class, response });
                            }
                            Err(mpsc::error::TrySendError::Full(_)) => {
                                // The child's stdin pipe and the writer queue
                                // are both full: same admission failure as the
                                // pending cap, so the caller hears Backpressure
                                // instead of parking the actor.
                                let _ = response.send(Err(EngineError::Backpressure));
                                continue;
                            }
                            Err(mpsc::error::TrySendError::Closed(_)) => {
                                let failure = match class {
                                    CallClass::ReadOnly => EngineError::Exited,
                                    CallClass::Mutating => EngineError::UnknownOutcome,
                                };
                                let _ = response.send(Err(failure));
                                terminal_state = EngineState::Exited;
                                break;
                            }
                        }
                    }
                    Some(EngineCommand::Cancel { id }) => {
                        pending.remove(&id);
                    }
                    Some(EngineCommand::Shutdown { response }) => {
                        writer.abort();
                        stop_child(&child, limits.shutdown_grace).await;
                        let _ = response.send(());
                        terminal_state = EngineState::Stopped;
                        break;
                    }
                    None => {
                        writer.abort();
                        stop_child(&child, limits.shutdown_grace).await;
                        terminal_state = EngineState::Stopped;
                        break;
                    }
                }
            }
            // The writer could not deliver a frame (broken pipe: the child is
            // gone or closed its stdin). Drain pending with the class-correct
            // failures, exactly as an inline write error used to.
            _ = writer_failed_rx.recv() => {
                terminal_state = EngineState::Exited;
                break;
            }
            line = lines.next() => {
                match line {
                    Some(Ok(line)) => {
                        if handle_upstream_line(
                            &line,
                            &mut pending,
                            &events,
                            &receive_ingress,
                            &mut receive_degraded,
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

    let terminal_class = match terminal_state {
        EngineState::Stopped => "stopped",
        EngineState::Exited => "exited",
        EngineState::Faulted => "faulted",
        // The loop only breaks on a terminal state; this arm is unreachable.
        EngineState::Running => "running",
    };
    if matches!(terminal_state, EngineState::Stopped) {
        tracing::info!(
            state = terminal_class,
            pid = process_pid,
            "signal-cli engine stopped"
        );
    } else {
        tracing::warn!(
            state = terminal_class,
            pid = process_pid,
            "signal-cli engine terminated without a shutdown request"
        );
    }
    // Aborting the writer drops any half-written frame and closes the child's
    // stdin pipe immediately — the same close `stdin.take()` used to perform —
    // so a writer parked on a full pipe cannot delay the child's shutdown.
    // Aborting an already-finished writer is a no-op.
    writer.abort();
    if !matches!(terminal_state, EngineState::Stopped) {
        stop_child(&child, limits.shutdown_grace).await;
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

/// The engine's only stdin writer (A7): frames arrive pre-encoded from the
/// actor, in dispatch order, and leave one `write_all` + flush at a time —
/// one writer, one order. A child that stops reading its stdin parks this
/// task alone; the actor keeps pumping stdout and answering commands. On a
/// write failure the writer reports to the actor (which drains pending with
/// the class-correct failures) and exits; when the actor is already gone the
/// report is dropped and this task simply returns. Returning drops `stdin`,
/// closing the pipe, the same EOF a taken stdin used to deliver.
async fn run_stdin_writer(
    mut stdin: ChildStdin,
    mut frames: mpsc::Receiver<Vec<u8>>,
    failed: mpsc::Sender<()>,
) {
    while let Some(frame) = frames.recv().await {
        if stdin.write_all(&frame).await.is_err() || stdin.flush().await.is_err() {
            let _ = failed.try_send(());
            break;
        }
    }
}

async fn handle_upstream_line(
    line: &str,
    pending: &mut HashMap<String, PendingRequest>,
    events: &broadcast::Sender<EngineEvent>,
    receive_ingress: &ReceiveIngress,
    receive_degraded: &mut bool,
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
                match receive_ingress.enqueue(normalized).await? {
                    EnqueueOutcome::Enqueued => {
                        if *receive_degraded {
                            *receive_degraded = false;
                            tracing::info!(
                                state = "recovered",
                                "receive queue admission recovered"
                            );
                            let _ = events.send(EngineEvent::StorageChanged { state: "recovered" });
                        }
                    }
                    EnqueueOutcome::Dropped => {
                        // Storage is not draining the queue; drop the receive
                        // rather than park the actor. No payload or identity
                        // is logged. The persistence loop reports the
                        // underlying store failure on the same event; this
                        // transition covers a saturated queue whose head the
                        // store has not even failed on yet.
                        crate::metrics::record_receive_drop();
                        tracing::warn!("receive queue saturated; dropping a receive");
                        if !*receive_degraded {
                            *receive_degraded = true;
                            let _ = events.send(EngineEvent::StorageChanged {
                                state: "unavailable",
                            });
                        }
                    }
                }
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

async fn stop_child(child: &Mutex<Child>, grace: Duration) {
    let mut child = child.lock().await;
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
    fn socks_proxy_parse_accepts_host_port() {
        assert_eq!(
            SocksProxy::parse("127.0.0.1:1080").unwrap(),
            SocksProxy {
                host: "127.0.0.1".into(),
                port: 1080,
            }
        );
        assert_eq!(SocksProxy::parse("proxy.local:7890").unwrap().port, 7890);
    }

    #[test]
    fn socks_proxy_parse_rejects_malformed_values() {
        for value in [
            "",
            "127.0.0.1",
            "127.0.0.1:",
            ":1080",
            "127.0.0.1:0",
            "127.0.0.1:65536",
            "127.0.0.1:abc",
            "127. 0.0.1:1080",
            "127.0.0.1:1080 ",
            "::1:1080",
            "fe80::1:1080",
        ] {
            assert!(
                SocksProxy::parse(value).is_err(),
                "proxy value must be rejected: {value:?}"
            );
        }
    }

    #[test]
    fn java_opts_only_carry_proxy_flags_when_configured() {
        assert_eq!(java_opts(None), "-Xms16m -Xmx384m");
        let proxy = SocksProxy {
            host: "127.0.0.1".into(),
            port: 1080,
        };
        assert_eq!(
            java_opts(Some(&proxy)),
            "-Xms16m -Xmx384m -DsocksProxyHost=127.0.0.1 -DsocksProxyPort=1080"
        );
    }

    #[test]
    fn config_defaults_to_direct_connection() {
        let config = SignalCliConfig::new(PathBuf::from("/bin/signal-cli"), PathBuf::from("/data"));
        assert_eq!(config.proxy, None);
    }

    #[test]
    fn config_defaults_to_jvm_mode() {
        let config = SignalCliConfig::new(PathBuf::from("/bin/signal-cli"), PathBuf::from("/data"));
        assert_eq!(config.mode, SignalCliMode::Jvm);
    }

    #[test]
    fn signal_cli_args_match_the_jvm_launcher_shape() {
        let proxy = SocksProxy {
            host: "127.0.0.1".into(),
            port: 1080,
        };
        let expected: Vec<std::ffi::OsString> = [
            "--data-dir",
            "/data",
            "jsonRpc",
            "--receive-mode",
            "on-start",
            "--ignore-attachments",
            "--ignore-stories",
            "--ignore-stickers",
        ]
        .into_iter()
        .map(Into::into)
        .collect();
        // JVM mode: proxy travels in JAVA_OPTS, never on the command line.
        assert_eq!(
            signal_cli_args(SignalCliMode::Jvm, Some(&proxy), Path::new("/data")),
            expected
        );
        assert_eq!(
            signal_cli_args(SignalCliMode::Native, None, Path::new("/data")),
            expected
        );
    }

    #[test]
    fn native_mode_prepends_socks_proxy_properties_to_args() {
        let proxy = SocksProxy {
            host: "127.0.0.1".into(),
            port: 1080,
        };
        let args = signal_cli_args(SignalCliMode::Native, Some(&proxy), Path::new("/data"));
        assert_eq!(
            args[..2],
            [
                std::ffi::OsString::from("-DsocksProxyHost=127.0.0.1"),
                std::ffi::OsString::from("-DsocksProxyPort=1080"),
            ]
        );
        assert_eq!(args[2], "--data-dir");
    }

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
            Ok(Ok(EnqueueOutcome::Enqueued))
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
        let mut degraded = false;

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
        handle_upstream_line(&oversized, &mut pending, &events, &ingress, &mut degraded)
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
        handle_upstream_line(&well_formed, &mut pending, &events, &ingress, &mut degraded)
            .await
            .unwrap();

        let queued = receiver.recv().await.unwrap();
        assert_eq!(queued.receive().text.as_deref(), Some("after oversized"));
        assert!(receiver.try_recv().is_err());
        assert!(!degraded);
    }

    /// The regression at the heart of the liveness fix: a receive queue that
    /// never drains must turn admission into a bounded wait plus a drop, not
    /// into a permanently parked engine actor.
    #[tokio::test]
    async fn receive_enqueue_drops_instead_of_blocking_when_the_queue_stays_full() {
        let (mut ingress, mut receives) = receive_channel();
        ingress.enqueue_timeout = Duration::from_millis(50);
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
            ingress.enqueue(receive()).await,
            Ok(EnqueueOutcome::Enqueued)
        ) {
            admitted += 1;
        }
        assert!(admitted > 1);

        // Once the budget frees up, admission works again.
        drop(receives.recv().await.unwrap());
        assert_eq!(
            ingress.enqueue(receive()).await.unwrap(),
            EnqueueOutcome::Enqueued
        );
    }

    /// A dropped receive flips the engine-side degraded storage signal to
    /// `unavailable` once; the next admitted receive reports `recovered`.
    #[tokio::test]
    async fn receive_backpressure_reports_storage_degraded_and_recovers() {
        let (events, mut event_rx) = event_channel();
        let (mut ingress, mut receiver) = receive_channel();
        ingress.enqueue_timeout = Duration::from_millis(50);
        let mut pending = HashMap::new();
        let mut degraded = false;

        // Fill the byte budget so the next receive cannot be admitted.
        let filler = NormalizedReceive {
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
        while matches!(
            ingress.enqueue(filler.clone()).await,
            Ok(EnqueueOutcome::Enqueued)
        ) {}

        // The probe receives must be as heavy as the filler: the fill loop
        // stops as soon as one filler no longer fits, leaving up to one
        // filler-weight of byte budget free — a small message would still be
        // admitted and never exercise the drop path.
        let heavy = "a".repeat(MAX_INBOUND_TEXT_BYTES);
        let line = |timestamp: u64| {
            serde_json::to_string(&json!({
                "jsonrpc": "2.0",
                "method": "receive",
                "params": {
                    "account": "+15555550100",
                    "envelope": {
                        "source": "+15555550101",
                        "timestamp": timestamp,
                        "dataMessage": { "message": heavy.as_str() }
                    }
                }
            }))
            .unwrap()
        };

        handle_upstream_line(&line(1), &mut pending, &events, &ingress, &mut degraded)
            .await
            .unwrap();
        assert!(degraded);
        assert!(matches!(
            event_rx.recv().await.unwrap(),
            EngineEvent::StorageChanged {
                state: "unavailable"
            }
        ));

        // Drain the queue: admission succeeds again and reports recovery once.
        while receiver.try_recv().is_ok() {}
        handle_upstream_line(&line(2), &mut pending, &events, &ingress, &mut degraded)
            .await
            .unwrap();
        assert!(!degraded);
        assert!(matches!(
            event_rx.recv().await.unwrap(),
            EngineEvent::StorageChanged { state: "recovered" }
        ));
    }
}
