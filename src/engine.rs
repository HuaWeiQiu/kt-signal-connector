// SPDX-License-Identifier: AGPL-3.0-only

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
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
/// Bounded queue of lines pumped from the child's stdout to the actor. The
/// pump is the pipe's only reader (A7, symmetric with the stdin writer), so a
/// burst of engine output parks the pump alone until the actor drains it;
/// capacity bounds the buffered lines (each capped at `line_limit`).
const STDOUT_LINE_QUEUE_CAPACITY: usize = 64;
/// A line pumped off the child's stdout. `FramingError` marks a codec failure
/// (oversized or malformed line); end-of-stream arrives as the channel
/// closing, the same EOF signal the inline arm used to observe.
#[derive(Debug, PartialEq)]
enum PumpedLine {
    Line(String),
    FramingError,
}
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

/// How the configured engine executable is launched. Selected explicitly by
/// the trusted launcher (`serve --signal-cli-engine` / `KT_SIGNAL_CLI_ENGINE`,
/// or the legacy `--signal-cli-native` flag); never probed from the file.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, clap::ValueEnum)]
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
    /// KT's own `kt-signal-engine` sidecar (separate AGPL-3.0 repository):
    /// spawned directly with the same argv tail, but the `-D` SOCKS prepend
    /// is never emitted because the engine's argv parser is fail-closed on
    /// unknown arguments. Proxy parity holds at the environment level: the
    /// engine consumes the same `KT_SIGNAL_SOCKS_PROXY` variable this
    /// process injects (it maps it onto reqwest's `ALL_PROXY`, which covers
    /// the libsignal-service HTTP and WebSocket transport). Media ingest is
    /// NOT rejected in this mode: it is downgraded (implementation plan §5) —
    /// the engine cannot download attachments yet, so a launcher that arms
    /// ingest gets the pre-1.17 shape instead (`--ignore-attachments` on the
    /// argv, no media governor) rather than a failed launch, and every media
    /// method answers CAPABILITY_UNAVAILABLE. `JAVA_HOME` is ignored
    /// with a warning, like native mode.
    KtEngine,
}

#[derive(Clone, Debug)]
pub struct SignalCliConfig {
    pub executable: PathBuf,
    pub data_dir: PathBuf,
    pub mode: SignalCliMode,
    pub java_home: Option<PathBuf>,
    pub proxy: Option<SocksProxy>,
    /// Media ingest opt-in (ADR 0002): when false the engine spawns with
    /// `--ignore-attachments` exactly as before Phase 1.17 and every media
    /// method answers `CAPABILITY_UNAVAILABLE`; when true the flag is dropped
    /// so signal-cli downloads inbound attachments into its data directory,
    /// under the connector's media governor (bounded TTL + quota retention)
    /// and chunked handle delivery. Launcher input only — never IPC input.
    pub media_ingest: bool,
    pub line_limit: usize,
    pub request_timeout: Duration,
    pub shutdown_grace: Duration,
    pub receive_enqueue_timeout: Duration,
    pub resource_sample_interval: Duration,
    pub watchdog_interval: Duration,
    pub watchdog_min_restart_interval: Duration,
    /// Disappearing-message sweep cadence (contract 1.42, §4.41): how often
    /// each group's supervisor looks for rows whose timer ran out. The
    /// launcher may tighten it (integration tests); production keeps the
    /// default — a disappearing message legitimately outlives this by design.
    pub expire_sweep_interval: Duration,
    /// Transit-CDN base override for the §4.38 step-3 attachment download
    /// chain. `None` keeps the official production CDN map
    /// (`cdn.signal.org`/`cdn2`/`cdn3` per cdnNumber); `Some(base)` aims
    /// every history-attachment GET at one base instead — the same seam the
    /// official desktop carries as window config values, used by tests and
    /// never set by the production launcher.
    pub history_cdn_base: Option<String>,
}

impl SignalCliConfig {
    pub fn new(executable: PathBuf, data_dir: PathBuf) -> Self {
        Self {
            executable,
            data_dir,
            mode: SignalCliMode::Jvm,
            java_home: None,
            proxy: None,
            media_ingest: false,
            line_limit: DEFAULT_UPSTREAM_LINE_LIMIT,
            history_cdn_base: None,
            request_timeout: Duration::from_secs(15),
            shutdown_grace: Duration::from_secs(3),
            receive_enqueue_timeout: RECEIVE_ENQUEUE_TIMEOUT,
            resource_sample_interval: Duration::from_secs(30),
            watchdog_interval: Duration::from_secs(30),
            watchdog_min_restart_interval: Duration::from_secs(300),
            expire_sweep_interval: Duration::from_secs(30),
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

/// signal-cli CLI arguments, identical in every mode except that native mode
/// prepends the SOCKS proxy as runtime system properties for the GraalVM
/// native-image launcher. The launcher consumes `-D` entries anywhere on the
/// command line before the app parses its own arguments; leading position is
/// convention, not requirement. `media_ingest` (ADR 0002) only decides
/// whether `--ignore-attachments` is present: with it the rest of the argv —
/// including `--ignore-stories` and `--ignore-stickers` — is unchanged, and
/// without it the argv is byte-identical to the pre-media-POC launcher.
/// `KtEngine` never receives the `-D` prepend (its argv parser is fail-closed
/// on unknown arguments); the launcher-level validation rejects a configured
/// proxy in that mode before any engine is built.
fn signal_cli_args(
    mode: SignalCliMode,
    proxy: Option<&SocksProxy>,
    data_dir: &Path,
    media_ingest: bool,
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
    // ADR 0002: dropping --ignore-attachments is the entire media opt-in —
    // signal-cli then downloads inbound attachments into its data directory,
    // where the connector's media governor bounds retention. Stories and
    // stickers stay ignored in every mode; they are not part of the media
    // PoC and would add unbounded surface.
    if !media_ingest {
        args.push("--ignore-attachments".into());
    }
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
    #[error("signal-cli rejected the device credentials")]
    Unauthorized,
    /// The engine answered a JSON-RPC error object with an upstream error
    /// code (contract 1.36 sticker/pin faces): the code string is carried
    /// structurally so the connector maps engine error codes onto the host
    /// contract without swallowing them, while the message text stays
    /// content-free (the code is a fixed literal, never user data).
    /// Contract 1.50: the engine's optional `error.data` object rides along
    /// verbatim (`profileKeyUpdate`-era partial-success facts such as
    /// PROFILE_UPLOAD_FAILED's `{retryable, textSaved}`); a missing data
    /// object serializes as None and older hosts see the 1.49 shape.
    #[error("signal-cli returned a structured error")]
    UpstreamCode {
        code: &'static str,
        #[allow(dead_code)]
        data: Option<Value>,
    },
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
            EngineError::Unauthorized => "unauthorized",
            EngineError::UpstreamCode { .. } => "upstream_code",
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
    /// The Signal server rejected a request because the account holder
    /// unlinked this device on their phone (contract 1.21). `account` is the
    /// signal-cli account number from the failed request's params; requests
    /// without one cannot be attributed and only fail with
    /// [`EngineError::Unauthorized`].
    AccountUnauthorized {
        account: String,
    },
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NormalizedReceive {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<u64>,
    pub content_kind: &'static str,
    /// incoming | outgoing | system | control | skip
    pub direction: &'static str,
    pub account_present: bool,
    #[serde(skip)]
    pub account: Option<String>,
    /// Peer: inbound source or multi-device sent destination.
    #[serde(skip)]
    pub source: Option<String>,
    /// Envelope `sourceUuid` (contract 1.38): the sender's ACI identity as
    /// the engine projects it — present on every engine inbound envelope even
    /// when the legacy `source` field is absent. The service layer records the
    /// ACI→peer-key mapping from it on ingest (§4.37 peer_identities).
    #[serde(skip)]
    pub source_uuid: Option<String>,
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
    /// Quoted message snapshot (upstream timestamp, author, bounded preview).
    #[serde(skip)]
    pub quote: Option<NormalizedQuote>,
    /// Bounded inbound attachment descriptors (metadata only, no bytes).
    #[serde(skip)]
    pub attachments: Vec<NormalizedAttachment>,
    /// Bounded rich-body payload (contract 1.25): link previews, @mentions,
    /// text-style ranges and the view-once marker. Absent on plain rows.
    #[serde(skip)]
    pub rich: Option<NormalizedRich>,
    /// Sticker metadata (contract revision 1.35). Absent on non-sticker rows;
    /// the sticker's byte pointer rides `attachments`.
    #[serde(skip)]
    pub sticker: Option<NormalizedSticker>,
    /// Serialized per-conversation control payload (reaction / remote delete /
    /// typing) for `direction == "control"`; shape mirrors the protocol field.
    #[serde(skip)]
    pub control: Option<ControlReceive>,
    /// Disappearing-message timer as the envelope stated it (contract 1.42,
    /// §4.41): `Some(0)` = stated off, `None` = no stated value. Both engine
    /// faces share this parse; only the engine face states a start stamp.
    #[serde(skip)]
    pub expire_seconds: Option<u64>,
    /// Engine-reported countdown start (>0 stamps only). signal-cli faces
    /// never carry it — the deadline then falls back to receive time.
    #[serde(skip)]
    pub expire_start_at: Option<u64>,
    /// Envelope `serverGuid` (contract revision 1.49, §4.48): the
    /// server-assigned message GUID — the upstream report API's path
    /// parameter, captured at receive and persisted on the row so
    /// `messages.reportSpam` resolves it later. Connector-internal, never
    /// serialized; absent on envelopes without the key.
    #[serde(skip)]
    pub server_guid: Option<String>,
}

/// Inbound quote snapshot: the quoted message's upstream identity plus a
/// bounded preview the host renders without a history lookup.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NormalizedQuote {
    /// Quoted message's Signal timestamp.
    pub id: u64,
    /// Quoted author's number (or UUID when the number is absent).
    pub author: String,
    /// Bounded quoted-body preview.
    pub text: String,
}

/// One inbound attachment descriptor: metadata only, never bytes (§4.13).
/// Since contract revision 1.61 the receive face also captures the
/// envelope's transit locator when the engine projects one (the §4.38
/// attachv2 pointer face, the same ingest caps as the 1.56 import parser);
/// the pinned 0.14.8 signal-cli and the kt-engine receive face project
/// metadata only today, so the field stays `None` until an engine ships the
/// receive-side projection — the download face then becomes live without a
/// further connector change.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NormalizedAttachment {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub width: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub height: Option<u32>,
    pub is_voice_note: bool,
    /// Ingest-side transit pointer face, internal only — it rides the store's
    /// schema-19 live locator column and never the host wire (the §4.13
    /// metadata-only discipline covers it too). `None` on every engine face
    /// that does not project a receive locator.
    #[serde(skip)]
    pub locator: Option<crate::store::HistoryAttachmentLocator>,
}

/// One inbound link-preview card (contract 1.25): bounded metadata only — the
/// image rides the same metadata-only attachment descriptor; bytes are never
/// fetched by the connector.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NormalizedPreview {
    pub url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image: Option<NormalizedAttachment>,
}

/// One inbound @mention range (contract 1.25; authorAci added in 1.40): the
/// author resolves to the mentioned peer; the styled range
/// [start, start+length) is already part of the received body text.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NormalizedMention {
    /// Mentioned peer's number, or UUID when the number is absent.
    pub author: String,
    /// The mentioned peer's ACI when the connector can honestly name it
    /// (contract 1.40, §4.40): the envelope `uuid` field, or the
    /// number-keyed value when it is UUID-shaped (the engine-mode receive
    /// face puts the ACI there). Absent keeps `author` the only identity,
    /// byte-identical to pre-1.40 rows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub author_aci: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub start: u32,
    pub length: u32,
}

/// One inbound text-style range (contract 1.25). Only the styles the protocol
/// renders pass validation; `NONE` and unknown values drop individually.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NormalizedTextStyle {
    pub style: String,
    pub start: u32,
    pub length: u32,
}

/// Bounded rich-body payload (contract 1.25): link previews, @mentions and
/// text-style ranges off one dataMessage, plus the view-once marker. An
/// all-default shape collapses to `None` so plain messages carry nothing.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NormalizedRich {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub previews: Vec<NormalizedPreview>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mentions: Vec<NormalizedMention>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub text_styles: Vec<NormalizedTextStyle>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub view_once: bool,
    /// Metadata-only view-once marker (contract 1.25 shape): the sender
    /// flagged the message view-once but the payload carried no renderable
    /// media. The row still burns on the §4.37 flow.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub view_once_invalid: bool,
    /// Connector-set burn stamp (contract 1.38): wall-clock ms the row's
    /// bytes were erased. Never set by receive parsing — written by the
    /// service layer's markViewOnceOpened / viewOnceOpen consumption and read
    /// back off the row to gate media opens and replays.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub view_once_opened_at: Option<u64>,
    /// Disappearing-message timer-change marker (contract 1.42, §4.41): the
    /// message announced a timer change for its conversation (`true` only
    /// when the envelope flagged `isExpirationUpdate`). Timer-notice rows
    /// carry it as their only rich content; a body message that happened to
    /// carry the flag keeps it beside the rest.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub is_expiration_update: bool,
    /// Group-state-change marker (contract 1.45, §4.44): the dataMessage's
    /// `groupInfo.type` was `UPDATE`. Bodyless updates project as system rows
    /// (the pre-1.45 skip path swallowed them); a body-carrying update keeps
    /// its normal row and the marker rides beside the rest.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub is_group_update: bool,
    /// The engine's post-change group title off the update's `groupInfo`
    /// (contract 1.45, §4.44): the receive face's `JsonGroupInfo` exposes only
    /// the resulting name — no actor, no before/after — so the desktop gets it
    /// as mirror material, not as a diff.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_name: Option<String>,
    /// Story-reply context (contract 1.76, §4.67): the dataMessage's
    /// `storyReply` face — {authorAci, sentTimestamp} verbatim, the story the
    /// reply answers. The reply itself is a normal DM/group row; absent on
    /// plain and pre-1.76 rows. A malformed face (missing/empty author,
    /// missing timestamp) drops the key, never the row.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub story_reply: Option<NormalizedStoryReply>,
}

/// One story-reply context (contract 1.76, §4.67): the addressed story's
/// author ACI and its upstream timestamp, exactly the two keys the engine
/// projects off `dataMessage.storyContext`. The reply row is a normal
/// message; this face only names the story it answers.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NormalizedStoryReply {
    pub author_aci: String,
    pub sent_timestamp: u64,
}

impl NormalizedRich {
    fn is_empty(&self) -> bool {
        self.previews.is_empty()
            && self.mentions.is_empty()
            && self.text_styles.is_empty()
            && !self.view_once
            && !self.view_once_invalid
            && self.view_once_opened_at.is_none()
            && !self.is_expiration_update
            && !self.is_group_update
            && self.group_name.is_none()
            && self.story_reply.is_none()
    }
}

/// Inbound sticker metadata (contract revision 1.35): the official
/// `DataMessage.sticker` identity — pack id as hex, pack key as standard
/// base64 (the serialization conventions the engine projects), the pack-local
/// sticker id and the optional emoji label. The sticker's byte pointer is not
/// part of this shape: `data` rides the row's metadata-only attachment
/// descriptor list, so fetches go through the existing media channel and the
/// connector never downloads sticker bytes (§6.7).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NormalizedSticker {
    /// Sticker pack id, hex as the official client serializes it.
    pub pack_id: String,
    /// Sticker pack key, standard base64 as `Bytes.fromBase64` produces.
    pub pack_key: String,
    /// Pack-local sticker id.
    pub sticker_id: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub emoji: Option<String>,
}

/// Delivery tier of an inbound receipt (`envelope.receiptMessage`). Order is
/// the protocol's monotonic upgrade path: a later tier never downgrades an
/// earlier one (`read` over `delivered` over `sent`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReceiptKind {
    Delivered,
    Read,
    Viewed,
}

impl ReceiptKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ReceiptKind::Delivered => "delivered",
            ReceiptKind::Read => "read",
            ReceiptKind::Viewed => "read",
        }
    }
}

/// Control-plane receive payloads that are not messages: reaction add/remove,
/// remote delete of an earlier message, typing START/STOP, and edit of an
/// earlier message (new body rides `NormalizedReceive.text`).
#[derive(Clone, Debug)]
pub enum ControlReceive {
    Reaction {
        emoji: String,
        target_author: String,
        target_timestamp: u64,
        remove: bool,
    },
    RemoteDelete {
        target_timestamp: u64,
    },
    Typing {
        action: String,
    },
    Edit {
        target_timestamp: u64,
    },
    /// Peer receipt confirming earlier outgoing rows (bounded batch). `when`
    /// is the receipt envelope's own timestamp (contract 1.32) — when the
    /// peer sent the receipt — falling back to connector wall-clock at the
    /// service layer when the envelope carries none.
    Receipt {
        kind: ReceiptKind,
        timestamps: Vec<u64>,
        when: Option<u64>,
    },
    /// Pin of an earlier message (contract 1.33): the target identity plus
    /// the optional timed-pin duration — absent means the official forever
    /// pin.
    PinMessage {
        target_author: String,
        target_timestamp: u64,
        duration_seconds: Option<u32>,
    },
    /// Unpin of the earlier message it names (contract 1.33).
    UnpinMessage {
        target_author: String,
        target_timestamp: u64,
    },
    /// Group admin removed the earlier message it names (contract 1.33).
    AdminDelete {
        target_author: String,
        target_timestamp: u64,
    },
    /// A peer's client notified us their view-once message was viewed on one
    /// of our own linked devices (contract 1.38): the official
    /// `syncMessage.viewOnceOpen {senderAci, timestamp}` mirror. `timestamp`
    /// normalizes to 0 when the envelope omits it (the official unknown
    /// marker); the service layer drops that shape warn-only.
    ViewOnceOpen {
        sender_aci: String,
        timestamp: u64,
    },
    /// The engine's `groupUpdated {groupId, revision, expireTimerSeconds,
    /// editor?}` projection (2026-10-06 engine batch, kt-signal-engine main
    /// @ 216a4c7): a decrypted group change carrying the expiration-timer
    /// action, envelope-replaced before it ever reaches the connector. The
    /// service layer mirrors `expireTimerSeconds` into the conversation row
    /// (0 = off) and converges through `conversation.changed`; no message row
    /// exists for the notification — the group timer change rides the change
    /// event, and the notice-row presentation is a later desktop decision.
    GroupTimerUpdate {
        expire_timer_seconds: u32,
    },
    /// Call signaling (contract 1.51, §4.49): the engine's `callMessage`
    /// envelope projection — the official Content.callMessage=3 face. The
    /// connector is a signaling pipe: `call_id` is the 1:1 call identity the
    /// RingRTC frames share, `signal_type` mirrors the projected frame
    /// family (`offer` / `answer` / `ice` / `busy` / `hangup`, `opaque`
    /// when only the modern opaque payload rides), and `payload` is the
    /// bounded signaling material passed through byte-for-byte — never
    /// parsed or interpreted connector-side (the engine projects, the
    /// RingRTC state machine consumes).
    Call {
        call_id: u64,
        signal_type: &'static str,
        payload: Value,
    },
    /// The official `syncMessage.messageRequestResponse` mirror (contract
    /// 1.70, §4.58 consume batch): one of our own linked devices answered the
    /// conversation's message request. `response_type` is the protocol enum
    /// member 0..=6 (UNKNOWN/ACCEPT/DELETE/BLOCK/BLOCK_AND_DELETE/SPAM/
    /// BLOCK_AND_SPAM — the desktop's `isKnownProtoEnumMember` gate forces an
    /// out-of-enum value back to UNKNOWN before this connector ever sees it,
    /// and the shapes that survive the engine-side gates carry only 1..=6),
    /// `thread_aci` names the 1:1 thread (the ServiceId UUID string), and
    /// `group_id` the 32-byte group master key's standard base64 — exactly
    /// one of the two is present (the proto oneof). No message row exists for
    /// the notification; the service layer stamps the authoritative factor
    /// and the summary rides `conversation.changed`.
    MessageRequestResponse {
        response_type: u8,
        thread_aci: Option<String>,
        group_id: Option<String>,
    },
    /// Story receives (contract 1.76, §4.67): the engine's envelope-level
    /// `storyMessage` projection — the official Content.storyMessage=8 face.
    /// The connector is a story pipe like the call-signaling pipe: `message`
    /// is the bounded projection verbatim ({textAttachment{…}|
    /// fileAttachment, allowsReplies, profileKey?, group?}) — never parsed
    /// or re-shaped connector-side (the desktop owns the story timeline; the
    /// connector keeps no story row). The stories-disabled gate (the
    /// official getStoriesBlocked leg) is the service layer's.
    Story {
        message: Value,
    },
}

/// Protocol-side bounds for inbound control-plane data (§4.13).
const MAX_QUOTE_TEXT_CHARS: usize = 128;
pub(crate) const MAX_INBOUND_ATTACHMENTS: usize = 32;
const MAX_ATTACHMENT_ID_CHARS: usize = 128;
const MAX_ATTACHMENT_FILENAME_BYTES: usize = 128;
const MAX_ATTACHMENT_CONTENT_TYPE_CHARS: usize = 64;
const MAX_EMOJI_CHARS: usize = 16;
/// One receipt confirms at most this many earlier rows; a larger batch is
/// truncated (Signal Desktop batches receipts per conversation, so real
/// batches stay far below).
const MAX_RECEIPT_TIMESTAMPS: usize = 256;
/// Rich-body bounds (contract 1.25, §4.13 parity): caps on card/range counts
/// and string sizes. Entries past a cap drop, oversized strings truncate — a
/// hostile payload can never fail the message itself.
const MAX_PREVIEWS: usize = 4;
const MAX_MENTIONS: usize = 64;
const MAX_TEXT_STYLES: usize = 64;
const MAX_PREVIEW_URL_CHARS: usize = 2048;
const MAX_PREVIEW_FIELD_BYTES: usize = 512;
const MAX_MENTION_NAME_BYTES: usize = 128;
/// Sticker bounds (contract revision 1.35): the same face bounds the send
/// method enforces — pack id 64 hex chars, pack key 128 base64 chars, emoji
/// label 32 chars.
const MAX_STICKER_PACK_ID_CHARS: usize = 64;
const MAX_STICKER_PACK_KEY_CHARS: usize = 128;
const MAX_STICKER_EMOJI_CHARS: usize = 32;
/// Group-update title mirror cap (contract 1.45, §4.44): the update's
/// resulting `groupName` truncates to the same 64-char budget the envelope's
/// `sourceName` peer-name material uses before it rides the rich payload.
const GROUP_UPDATE_TITLE_MIRROR_CHARS: usize = 64;
/// Call-signaling bounds (contract 1.50, §4.48): the engine's legacy-frame
/// projection (`offer`/`answer`/`iceUpdate`/`busy`/`hangup` id fields) and
/// the bounded opaque payloads the connector forwards without parsing. The
/// base64 cap is the same coarse guard the §4.12 attachment envelope rides
/// (a RingRTC SDP offer/ICE batch stays orders of magnitude below it); the
/// frame count caps the legacy `iceUpdate` list projection.
const MAX_CALL_ID: u64 = u32::MAX as u64;
const MAX_CALL_OPAQUE_CHARS: usize = 3 * 1024 * 1024;
const MAX_CALL_ICE_FRAMES: usize = 64;
/// The official desktop's `GROUPV2_ID_LENGTH` (MessageReceiver.preload.ts
/// :3274-3309, contract 1.70 §4.58 consume batch): the group master key is a
/// 32-byte value carried on the wire as standard base64; any other decoded
/// length is not the protocol's face.
const GROUPV2_ID_LENGTH: usize = 32;
/// The highest `messageRequestResponseType` enum member this connector
/// consumes (UNKNOWN=0 … BLOCK_AND_SPAM=6; `SignalService.proto:666-678`).
const MRR_RESPONSE_TYPE_MAX: u8 = 6;

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
            + self.source_uuid.as_ref().map_or(0, String::len)
            + self.peer_name.as_ref().map_or(0, String::len)
            + self.group_id.as_ref().map_or(0, String::len)
            + self.text.as_ref().map_or(0, String::len)
            // Attachment ids dominate descriptor size; the rest is fixed-width.
            + self
                .attachments
                .iter()
                .map(|attachment| {
                    attachment.id.len()
                        + attachment.content_type.as_ref().map_or(0, String::len)
                        + attachment.filename.as_ref().map_or(0, String::len)
                        + std::mem::size_of::<NormalizedAttachment>()
                })
                .sum::<usize>()
            // Quotes carry a bounded preview; control payloads are enum-sized.
            + self.quote.as_ref().map_or(0, |quote| {
                quote.author.len() + quote.text.len() + std::mem::size_of::<NormalizedQuote>()
            })
            // Rich bodies carry capped urls/titles/names; ranges are fixed-width.
            + self.rich.as_ref().map_or(0, |rich| {
                std::mem::size_of::<NormalizedRich>()
                    + rich
                        .previews
                        .iter()
                        .map(|preview| {
                            preview.url.len()
                                + preview.title.as_ref().map_or(0, String::len)
                                + preview.description.as_ref().map_or(0, String::len)
                                + std::mem::size_of::<NormalizedPreview>()
                        })
                        .sum::<usize>()
                    + rich
                        .mentions
                        .iter()
                        .map(|mention| {
                            mention.author.len()
                                + mention.name.as_ref().map_or(0, String::len)
                                + std::mem::size_of::<NormalizedMention>()
                        })
                        .sum::<usize>()
                    + rich.text_styles.len() * std::mem::size_of::<NormalizedTextStyle>()
            })
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
        if !matches!(config.mode, SignalCliMode::Jvm) && config.java_home.is_some() {
            tracing::warn!("the selected engine mode ignores the configured JAVA_HOME");
        }

        let mut command = Command::new(&config.executable);
        // The engine's stdout is an owned blocking pipe, not tokio's piped
        // stdio: the read end is consumed by the dedicated blocking pump
        // thread below, whose plain `read` cannot miss a readiness wakeup —
        // engine output drains no matter what the async side is doing.
        // Command holds the parent-side write end until dropped (end of this
        // function), so the reader sees EOF when the child exits.
        let (stdout_reader, stdout_writer) =
            std::io::pipe().map_err(|_| EngineError::StartFailed)?;
        command
            .args(signal_cli_args(
                config.mode,
                config.proxy.as_ref(),
                &config.data_dir,
                config.media_ingest,
            ))
            .stdin(Stdio::piped())
            .stdout(stdout_writer)
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
        // The stdout pump is a blocking OS thread: a blocking `read` cannot
        // miss a readiness wakeup, and the engine's output must keep draining
        // no matter what the async consumers of this pipe are doing. Lines
        // are handed to the actor through a bounded queue; end-of-stream
        // closes it, the same EOF signal the actor would observe inline.
        let (stdout_tx, stdout_rx) = mpsc::channel::<PumpedLine>(STDOUT_LINE_QUEUE_CAPACITY);
        let stdout_limit = config.line_limit;
        std::thread::Builder::new()
            .name(format!("engine-stdout-{pid}"))
            .spawn(move || run_stdout_pump(stdout_reader, stdout_limit, stdout_tx))
            .map_err(|_| EngineError::StartFailed)?;
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
                stdout_rx,
                ActorChannels {
                    commands: command_rx,
                    status: status_tx,
                    events: actor_events,
                    receive_ingress,
                },
                ActorLimits {
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
    /// signal-cli account param of the in-flight request, when present — used
    /// solely to attribute upstream authorization failures (contract 1.21).
    account: Option<String>,
    response: oneshot::Sender<Result<Value, EngineError>>,
}

struct ActorLimits {
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
    mut lines: mpsc::Receiver<PumpedLine>,
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
                                let account = params
                                    .get("account")
                                    .and_then(Value::as_str)
                                    .map(str::to_string);
                                pending.insert(id, PendingRequest { class, account, response });
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
            line = lines.recv() => {
                match line {
                    Some(PumpedLine::Line(line)) => {
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
                    Some(PumpedLine::FramingError) => {
                        terminal_state = EngineState::Faulted;
                        break;
                    }
                    // The pump returned: the child's stdout hit EOF, so the
                    // child is gone or closed its output — same as the old
                    // inline `None` arm.
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

/// The engine's only stdout reader: a blocking OS thread. A blocking `read`
/// delivers bytes regardless of any readiness-event machinery, so engine
/// output drains even when the async side never observes a wakeup; the
/// actor's queue arm (`PumpedLine`) is the only coupling. A full queue parks
/// this thread alone (the same backpressure an inline reader had), a codec
/// failure reports `FramingError` once, and end-of-stream closes the queue —
/// the actor's EOF signal.
fn run_stdout_pump(stdout: std::io::PipeReader, line_limit: usize, tx: mpsc::Sender<PumpedLine>) {
    let mut reader = std::io::BufReader::new(stdout);
    loop {
        match read_bounded_line(&mut reader, line_limit) {
            Ok(Some(line)) => {
                if tx.blocking_send(line).is_err() {
                    return;
                }
            }
            Ok(None) => return,
            Err(BoundedLineError::Io(error)) => {
                tracing::warn!(error = %error, "engine stdout pump read failed");
                let _ = tx.blocking_send(PumpedLine::FramingError);
                return;
            }
            Err(_) => {
                let _ = tx.blocking_send(PumpedLine::FramingError);
                return;
            }
        }
    }
}

/// One newline-terminated line from a blocking reader, capped at `limit`
/// bytes with the same observable behavior as the line codec it replaces: a
/// partial final line at EOF is delivered, a trailing `\r` before the
/// newline is stripped, a longer line or invalid UTF-8 is `FramingError`,
/// and a clean EOF with nothing buffered is `None`.
fn read_bounded_line(
    reader: &mut impl std::io::BufRead,
    limit: usize,
) -> Result<Option<PumpedLine>, BoundedLineError> {
    let mut line: Vec<u8> = Vec::new();
    loop {
        let available = match reader.fill_buf() {
            Ok(available) => available,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(BoundedLineError::Io(error)),
        };
        if available.is_empty() {
            // End of stream: an empty buffer ends the stream, a partial
            // final line is delivered the way the codec's EOF decode did.
            return if line.is_empty() {
                Ok(None)
            } else {
                Ok(Some(line_from_buffer(line)?))
            };
        }
        match available.iter().position(|byte| *byte == b'\n') {
            Some(newline) => {
                if line.len() + newline > limit {
                    return Err(BoundedLineError::TooLong);
                }
                line.extend_from_slice(&available[..newline]);
                reader.consume(newline + 1);
                return Ok(Some(line_from_buffer(line)?));
            }
            None => {
                if line.len() + available.len() > limit {
                    return Err(BoundedLineError::TooLong);
                }
                line.extend_from_slice(available);
                let buffered = available.len();
                reader.consume(buffered);
            }
        }
    }
}

#[derive(Debug)]
enum BoundedLineError {
    TooLong,
    InvalidUtf8,
    Io(std::io::Error),
}

fn line_from_buffer(mut line: Vec<u8>) -> Result<PumpedLine, BoundedLineError> {
    if line.last() == Some(&b'\r') {
        line.pop();
    }
    match String::from_utf8(line) {
        Ok(text) => Ok(PumpedLine::Line(text)),
        Err(_) => Err(BoundedLineError::InvalidUtf8),
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
        if upstream_error_is_unauthorized(object.get("error")) {
            if let Some(account) = &request.account {
                let _ = events.send(EngineEvent::AccountUnauthorized {
                    account: account.clone(),
                });
            }
            let _ = request.response.send(Err(EngineError::Unauthorized));
        } else if let Some(code) = upstream_error_code(object.get("error")) {
            // Contract 1.36 faces: the engine answers structured errors with
            // a code field (STICKER_*, STORAGE_*, CONVERSATION_NOT_RESOLVED
            // families). The code is a fixed engine literal, carried as such;
            // unrecognized codes degrade to the plain upstream error so an
            // engine newer than this connector never invents host codes.
            // Contract 1.50: the engine's error.data object (structured
            // facts such as PROFILE_UPLOAD_FAILED's {retryable, textSaved})
            // rides along verbatim when present — pass-through only, no
            // key invention, no stripping.
            let data = object
                .get("error")
                .and_then(|error| error.get("data"))
                .filter(|data| data.is_object())
                .cloned();
            let _ = request
                .response
                .send(Err(EngineError::UpstreamCode { code, data }));
        } else {
            let _ = request.response.send(Err(EngineError::Upstream));
        }
    } else {
        let result = object.get("result").cloned().unwrap_or(Value::Null);
        let _ = request.response.send(Ok(result));
    }
    Ok(())
}

/// signal-cli wraps transport failures as UnexpectedErrorException whose
/// message embeds the upstream cause class. A device unlinked on the phone
/// (contract 1.21) surfaces as AuthorizationFailedException / "Authorization
/// failed!" on the next network call, and this is the single observation
/// point that still sees the raw upstream error body — everything above the
/// engine boundary stays content-free. A local-only failure (e.g. listAccounts
/// never touches the network) cannot match, so the classification is safe to
/// trust as "the server rejected this device's credentials".
/// Upstream (signal-cli JSON-RPC) error shapes that mean the LOCAL account's
/// linked-device credential is dead — classified `EngineError::Unauthorized`
/// and attributed to the request's account (contract 1.21 §4.17). Two
/// surfaces, one root cause:
///
/// 1. live authorization failure: upstream wraps `AuthorizationFailedException`
///    in an UnexpectedErrorException (-32603) ("Authorization failed!").
/// 2. post-mortem short-circuit: after the first authorization failure
///    signal-cli persists `registered=false` (signal-cli
///    `AccountHelper.java:119`) and every later account-scoped command fails
///    LOCALLY with `UserErrorException("User <account> is not registered.")`
///    (`App.java:342` wrapping `NotRegisteredException`) before any network
///    call — the 0-17 ms instant-failure form. Matched by the strict prefix/
///    suffix pair; the recipient-level "The user <id> is not registered."
///    (unblock/updateContact/sendPoll* family) starts "The user " and must
///    NOT mark the account unlinked.
fn upstream_error_is_unauthorized(error: Option<&Value>) -> bool {
    error
        .and_then(|value| value.get("message"))
        .and_then(Value::as_str)
        .is_some_and(|message| {
            message.contains("AuthorizationFailedException")
                || message.contains("Authorization failed")
                || (message.starts_with("User ") && message.ends_with(" is not registered."))
        })
}

/// Known engine error codes the connector maps onto structured host errors
/// (contract 1.36, §4.32/§4.33). The allowlist is the whole mapping surface:
/// an engine code outside it degrades to the plain upstream error, so the
/// host never sees a code this connector's schema does not declare, and a
/// future engine adding codes cannot silently widen the host contract.
const ENGINE_STRUCTURED_ERROR_CODES: &[&str] = &[
    // §4.32 sticker pack browsing (engine-owned: key check, CDN fetch,
    // decrypt/decode, image size budget).
    "STICKER_PACK_KEY_INVALID",
    "STICKER_PACK_FETCH_FAILED",
    "STICKER_PACK_MALFORMED",
    "STICKER_IMAGE_TOO_LARGE",
    // §4.47 avatar download (contract 1.48, engine alignment batch ③a:
    // honest no-avatar, download/decrypt failure, the 10 MiB decoded cap).
    "AVATAR_UNAVAILABLE",
    "AVATAR_FETCH_FAILED",
    "AVATAR_TOO_LARGE",
    // §4.33 conversation pin sync (engine-owned: cloud reference resolution,
    // storage-service failures surface under the engine's STORAGE_* family).
    "CONVERSATION_NOT_RESOLVED",
    "STORAGE_UNAVAILABLE",
    "STORAGE_WRITE_FAILED",
    "STORAGE_READ_FAILED",
    // §4.46 Android local-backup export (engine backup-export-contract
    // §10.7): the start-ladder and task-failure codes. ALREADY_RUNNING is
    // deliberately not retryable — the desktop polls backups.exportStatus
    // instead of re-starting; SNAPSHOT_EXISTS/IO_FAILED are transient disk
    // races/failures the desktop may retry after the connector clears.
    "BACKUP_EXPORT_ALREADY_RUNNING",
    "BACKUP_ARTIFACT_MISSING",
    "BACKUP_ARTIFACT_INVALID",
    "BACKUP_SNAPSHOT_EXISTS",
    "BACKUP_IO_FAILED",
    // §4.66 username write face (contract 1.75, engine username write-face
    // batch): the reserve/confirm/delete server-state ladder. TAKEN and
    // RESERVATION_GONE are deterministic refusals (a new name / a fresh
    // reserve is the only way forward); RATE_LIMITED and SERVER_FAILED may
    // succeed on a later attempt. The honest partial-success family rides
    // the STORAGE_* codes already above (the server confirmed but the
    // storage write failed).
    "USERNAME_TAKEN",
    "USERNAME_RESERVATION_GONE",
    "USERNAME_RATE_LIMITED",
    "USERNAME_SERVER_FAILED",
    // §4.48 report spam (engine-owned: the upstream report API refused the
    // batch — every id's serverGuid was rejected).
    "REPORT_SPAM_FAILED",
    // §4.45 profile write partial-success face (engine alignment batch ②):
    // the text face landed but the avatar upload chain failed; the engine's
    // data object carries {retryable, textSaved} verbatim (contract 1.50).
    "PROFILE_UPLOAD_FAILED",
];

/// Extract a known structured engine error code from a JSON-RPC error object.
/// The real engine carries the literal in `message` with a numeric `code`
/// (-1; JSON-RPC 2.0 keeps error codes numeric), e.g. `{"code": -1,
/// "message": "STICKER_PACK_KEY_INVALID"}`. A string `code` field carrying
/// the literal is accepted as a fallback. The literal must match the
/// allowlist exactly; anything else (missing fields, unknown literals)
/// answers `None` and the caller falls back to the plain upstream error.
fn upstream_error_code(error: Option<&Value>) -> Option<&'static str> {
    let error = error?;
    let candidates = [
        error.get("message").and_then(Value::as_str),
        error.get("code").and_then(Value::as_str),
    ];
    for literal in candidates.into_iter().flatten() {
        if let Some(known) = ENGINE_STRUCTURED_ERROR_CODES
            .iter()
            .find(|known| **known == literal)
        {
            return Some(known);
        }
    }
    None
}

/// Test-only re-export of the allowlist so the service-layer mapping test can
/// prove the two sides stay total over the same set (the constant itself
/// stays private in normal builds).
#[cfg(test)]
pub(crate) const ENGINE_STRUCTURED_ERROR_CODES_FOR_TESTS: &[&str] = ENGINE_STRUCTURED_ERROR_CODES;

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

/// The `syncMessage.messageRequestResponse` face (contract 1.70, §4.58
/// consume batch). The official desktop anchors: `SignalService.proto:865`
/// (SyncMessage.content oneof member 14), `:666-678` (the MessageRequestResponse
/// record: `threadAci` = the thread's ServiceId UUID string, `groupId` = the
/// 32-byte group master key's standard base64, `type` = the numbered enum
/// UNKNOWN=0 / ACCEPT=1 / DELETE=2 / BLOCK=3 / BLOCK_AND_DELETE=4 / SPAM=5 /
/// BLOCK_AND_SPAM=6), and the consumer's shape gates (`MessageReceiver
/// .preload.ts:3274-3309` — `GROUPV2_ID_LENGTH = 32`, the UUID shape, and
/// `isKnownProtoEnumMember` forcing an out-of-enum `type` to UNKNOWN).
///
/// The projection returns the parsed face only when the envelope carries a
/// well-formed one — a hostile or foreign shape (bad UUID / bad base64 /
/// wrong byte length / a `type` outside 1..=6 / neither or both thread legs)
/// fails closed here and the envelope falls through to the plain skip arm,
/// exactly like every other gateless drop (宁缺勿错). A `type` of UNKNOWN
/// (0, the enum default) carries no session-actionable information, so it is
/// dropped as skip rather than stamped: the desktop consumes the same face
/// only for the known member values.
fn normalized_message_request_response(
    face: &serde_json::Map<String, Value>,
) -> Option<ControlReceive> {
    let response_type = face
        .get("messageRequestResponseType")
        .and_then(Value::as_u64)
        .and_then(|value| u8::try_from(value).ok())
        .filter(|value| (1..=MRR_RESPONSE_TYPE_MAX).contains(value))?;
    let thread_aci = face.get("threadAci").and_then(Value::as_str);
    let group_id = face.get("groupId").and_then(Value::as_str);
    // The proto oneof: exactly one of the two thread legs. A shape carrying
    // neither, both, or a malformed leg is not the protocol's — dropped.
    let resolved = match (thread_aci, group_id) {
        (Some(aci), None) => {
            let aci = aci.trim();
            if !is_service_id_uuid(aci) {
                return None;
            }
            (Some(aci.to_string()), None)
        }
        (None, Some(group)) => {
            let group = group.trim();
            let mut decoded = Vec::new();
            crate::service::base64_decode_to_vec(group, &mut decoded).ok()?;
            if decoded.len() != GROUPV2_ID_LENGTH {
                return None;
            }
            (None, Some(group.to_string()))
        }
        _ => return None,
    };
    Some(ControlReceive::MessageRequestResponse {
        response_type,
        thread_aci: resolved.0,
        group_id: resolved.1,
    })
}

/// The ServiceId UUID shape gate (the desktop's UUID regex,
/// MessageReceiver.preload.ts:3274-3309): 8-4-4-4-12 hex groups.
fn is_service_id_uuid(value: &str) -> bool {
    let parts: Vec<&str> = value.split('-').collect();
    parts.len() == 5
        && parts
            .iter()
            .zip([8usize, 4, 4, 4, 12])
            .all(|(part, len)| part.len() == len && part.chars().all(|c| c.is_ascii_hexdigit()))
}

#[derive(Clone, Debug)]
struct NormalizedText {
    value: String,
    bytes: u32,
    truncated: bool,
}

pub(crate) fn truncate_utf8_bytes(text: &str, max_bytes: usize) -> String {
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

/// Bounded inbound quote snapshot (§4.13): upstream timestamp, author, and a
/// short preview. Malformed or oversized quotes drop the preview rather than
/// the whole message.
fn normalized_quote(value: &Value) -> Option<NormalizedQuote> {
    let object = value.as_object()?;
    let id = object.get("id").and_then(Value::as_u64)?;
    let author = ["authorNumber", "authorUuid", "author"]
        .iter()
        .find_map(|key| object.get(*key).and_then(Value::as_str))
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.chars().take(MAX_RECEIVE_ID_CHARS).collect::<String>())?;
    let text = object
        .get("text")
        .and_then(Value::as_str)
        .map(|text| text.chars().take(MAX_QUOTE_TEXT_CHARS).collect::<String>())
        .unwrap_or_default();
    Some(NormalizedQuote { id, author, text })
}

/// Bounded inbound attachment descriptors (§4.13): metadata only, entries past
/// the cap are dropped, oversized strings are truncated to their bound.
fn normalized_attachments(value: &Value) -> Vec<NormalizedAttachment> {
    let Some(entries) = value.as_array() else {
        return Vec::new();
    };
    entries
        .iter()
        .take(MAX_INBOUND_ATTACHMENTS)
        .filter_map(|entry| entry.as_object().and_then(normalized_attachment))
        .collect()
}

/// One bounded attachment descriptor off an attachment-shaped object (§4.13,
/// reused by message attachments and link-preview images alike). The transit
/// locator face (contract revision 1.61) parses with the 1.56 import caps —
/// wrong-shaped keys drop, never guess (the §4.38 ingest discipline).
fn normalized_attachment(object: &serde_json::Map<String, Value>) -> Option<NormalizedAttachment> {
    const MAX_LOCATOR_BASE64_CHARS: usize = 512;
    const MAX_LOCATOR_HEX_CHARS: usize = 128;
    let id = object
        .get("id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.chars().take(MAX_ATTACHMENT_ID_CHARS).collect::<String>())?;
    let content_type = object
        .get("contentType")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| {
            s.chars()
                .take(MAX_ATTACHMENT_CONTENT_TYPE_CHARS)
                .collect::<String>()
        });
    let filename = object
        .get("filename")
        .and_then(Value::as_str)
        .map(|s| truncate_utf8_bytes(s, MAX_ATTACHMENT_FILENAME_BYTES))
        .filter(|s| !s.is_empty());
    let locator = object.get("locator").and_then(|raw| {
        let raw = raw.as_object()?;
        let bounded_b64 = |name: &str| {
            raw.get(name)
                .and_then(Value::as_str)
                .filter(|text| !text.is_empty() && text.len() <= MAX_LOCATOR_BASE64_CHARS)
                .map(str::to_string)
        };
        let plaintext_hash = raw
            .get("plaintextHash")
            .and_then(Value::as_str)
            .filter(|text| {
                !text.is_empty()
                    && text.len() <= MAX_LOCATOR_HEX_CHARS
                    && text.bytes().all(|byte| byte.is_ascii_hexdigit())
            })
            .map(|text| text.to_ascii_lowercase());
        let face = crate::store::HistoryAttachmentLocator {
            key: bounded_b64("key"),
            cdn_key: bounded_b64("cdnKey"),
            cdn_number: raw
                .get("cdnNumber")
                .and_then(Value::as_u64)
                .filter(|number| *number <= u32::MAX as u64)
                .map(|number| number as u32),
            upload_timestamp: raw
                .get("uploadTimestamp")
                .and_then(Value::as_u64)
                .filter(|timestamp| *timestamp > 0),
            digest: bounded_b64("digest"),
            plaintext_hash,
        };
        (face != crate::store::HistoryAttachmentLocator::default()).then_some(face)
    });
    Some(NormalizedAttachment {
        id,
        content_type,
        filename,
        size: object.get("size").and_then(Value::as_u64),
        width: object
            .get("width")
            .and_then(Value::as_u64)
            .map(|value| value.min(u32::MAX as u64) as u32),
        height: object
            .get("height")
            .and_then(Value::as_u64)
            .map(|value| value.min(u32::MAX as u64) as u32),
        is_voice_note: object
            .get("isVoiceNote")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        locator,
    })
}

/// One inbound sticker (contract revision 1.35): bounded pack identity beside
/// the message plus the `data` pointer as a regular metadata-only attachment
/// descriptor. A sticker missing any identity field or a usable data pointer
/// answers None so the envelope keeps its plain routing instead of carrying
/// half a sticker.
fn normalized_sticker(
    sticker: &serde_json::Map<String, Value>,
) -> Option<(NormalizedSticker, NormalizedAttachment)> {
    let pack_id = sticker
        .get("packId")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| {
            s.chars()
                .take(MAX_STICKER_PACK_ID_CHARS)
                .collect::<String>()
        })?;
    let pack_key = sticker
        .get("packKey")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| {
            s.chars()
                .take(MAX_STICKER_PACK_KEY_CHARS)
                .collect::<String>()
        })?;
    let sticker_id = bounded_range_u32(sticker, "stickerId")?;
    let emoji = sticker
        .get("emoji")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.chars().take(MAX_STICKER_EMOJI_CHARS).collect::<String>());
    let data = sticker
        .get("data")
        .and_then(Value::as_object)
        .and_then(normalized_attachment)?;
    Some((
        NormalizedSticker {
            pack_id,
            pack_key,
            sticker_id,
            emoji,
        },
        data,
    ))
}

/// Strict UUID grammar (8-4-4-4-12 hex, case-insensitive) — the §4.40
/// promotion test before an identity string is treated as an ACI. A phone
/// number never matches, and neither does a truncated or decorated value.
fn is_uuid_shaped(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() == 36
        && bytes.iter().enumerate().all(|(index, byte)| match index {
            8 | 13 | 18 | 23 => *byte == b'-',
            _ => byte.is_ascii_hexdigit(),
        })
}

/// Bounded rich-body payload (contract 1.25) off a dataMessage-shaped object:
/// link previews, @mentions, text-style ranges and the view-once marker.
/// Malformed or oversized entries drop individually; an all-default result is
/// None so plain messages carry nothing.
fn normalized_rich(message: &serde_json::Map<String, Value>) -> Option<NormalizedRich> {
    let mut rich = NormalizedRich::default();
    if let Some(previews) = message.get("previews").and_then(Value::as_array) {
        rich.previews = previews
            .iter()
            .take(MAX_PREVIEWS)
            .filter_map(|entry| {
                let object = entry.as_object()?;
                let url = object
                    .get("url")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(|s| s.chars().take(MAX_PREVIEW_URL_CHARS).collect::<String>())?;
                Some(NormalizedPreview {
                    url,
                    title: object
                        .get("title")
                        .and_then(Value::as_str)
                        .map(|s| truncate_utf8_bytes(s, MAX_PREVIEW_FIELD_BYTES))
                        .filter(|s| !s.is_empty()),
                    description: object
                        .get("description")
                        .and_then(Value::as_str)
                        .map(|s| truncate_utf8_bytes(s, MAX_PREVIEW_FIELD_BYTES))
                        .filter(|s| !s.is_empty()),
                    image: object
                        .get("image")
                        .and_then(Value::as_object)
                        .and_then(normalized_attachment),
                })
            })
            .collect();
    }
    if let Some(mentions) = message.get("mentions").and_then(Value::as_array) {
        rich.mentions = mentions
            .iter()
            .take(MAX_MENTIONS)
            .filter_map(|entry| {
                let object = entry.as_object()?;
                let number = object
                    .get("number")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(|s| s.chars().take(MAX_RECEIVE_ID_CHARS).collect::<String>());
                let uuid = object
                    .get("uuid")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(|s| s.chars().take(MAX_RECEIVE_ID_CHARS).collect::<String>());
                // Contract 1.40 (§4.40): the author keeps the pre-1.40
                // number-first ladder; the ACI is promoted only when the
                // upstream actually supplied one — the envelope `uuid`
                // field, or a UUID-shaped number key (the engine-mode
                // receive face puts the ACI there).
                let author = number.clone().or_else(|| uuid.clone())?;
                let author_aci = uuid.or_else(|| number.filter(|value| is_uuid_shaped(value)));
                Some(NormalizedMention {
                    author,
                    author_aci,
                    name: object
                        .get("name")
                        .and_then(Value::as_str)
                        .map(|s| truncate_utf8_bytes(s, MAX_MENTION_NAME_BYTES))
                        .filter(|s| !s.is_empty()),
                    start: bounded_range_u32(object, "start")?,
                    length: bounded_range_u32(object, "length")?,
                })
            })
            .collect();
    }
    if let Some(styles) = message.get("textStyles").and_then(Value::as_array) {
        rich.text_styles = styles
            .iter()
            .take(MAX_TEXT_STYLES)
            .filter_map(|entry| {
                let object = entry.as_object()?;
                let style = match object.get("style").and_then(Value::as_str) {
                    Some("BOLD") => "BOLD",
                    Some("ITALIC") => "ITALIC",
                    Some("STRIKETHROUGH") => "STRIKETHROUGH",
                    Some("MONOSPACE") => "MONOSPACE",
                    Some("SPOILER") => "SPOILER",
                    _ => return None,
                };
                Some(NormalizedTextStyle {
                    style: style.to_string(),
                    start: bounded_range_u32(object, "start")?,
                    length: bounded_range_u32(object, "length")?,
                })
            })
            .collect();
    }
    // Story-reply context (contract 1.76, §4.67): `dataMessage.storyReply` →
    // {authorAci, sentTimestamp}, both keys required — the engine projects the
    // face only when the author resolves, and a degraded shape here drops the
    // key (never the row), the §4.20 per-entry discipline.
    if let Some(reply) = message.get("storyReply").and_then(Value::as_object) {
        rich.story_reply = reply
            .get("authorAci")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| s.chars().take(MAX_RECEIVE_ID_CHARS).collect::<String>())
            .zip(reply.get("sentTimestamp").and_then(Value::as_u64))
            .map(|(author_aci, sent_timestamp)| NormalizedStoryReply {
                author_aci,
                sent_timestamp,
            });
    }
    rich.view_once = message
        .get("viewOnce")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    rich.view_once_invalid = message
        .get("viewOnceInvalid")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    rich.is_expiration_update = message
        .get("isExpirationUpdate")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    // Group-state change (contract 1.45, §4.44): `JsonGroupInfo.type == UPDATE`
    // marks a group edit; the resulting title is mirror material only. DELIVER
    // (every normal group message) and the absent key leave both fields off.
    if let Some(info) = message.get("groupInfo").and_then(Value::as_object) {
        rich.is_group_update = info
            .get("type")
            .and_then(Value::as_str)
            .is_some_and(|kind| kind == "UPDATE");
        rich.group_name = info
            .get("groupName")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| {
                s.chars()
                    .take(GROUP_UPDATE_TITLE_MIRROR_CHARS)
                    .collect::<String>()
            });
    }
    if rich.is_empty() {
        return None;
    }
    Some(rich)
}

/// One protocol range coordinate: missing, non-numeric or out-of-u32 values
/// drop the entry rather than clamp into a wrong rendered range.
fn bounded_range_u32(object: &serde_json::Map<String, Value>, key: &str) -> Option<u32> {
    object
        .get(key)
        .and_then(Value::as_u64)
        .filter(|value| *value <= u32::MAX as u64)
        .map(|value| value as u32)
}

/// Story receive material bound (contract 1.76, §4.67): the coarse guard
/// family the call-signaling opaque face rides (one host frame discipline) —
/// real story projections are far smaller; only hostile or absurd material
/// trips it, and an oversized face drops the whole envelope. The projection
/// itself passes through verbatim — the connector neither re-shapes nor
/// prunes it.
const MAX_STORY_MATERIAL_BYTES: usize = 3 * 1024 * 1024;

fn bounded_story_material(story: &serde_json::Map<String, Value>) -> Option<Value> {
    let value = Value::Object(story.clone());
    let serialized = serde_json::to_string(&value).ok()?;
    (serialized.len() <= MAX_STORY_MATERIAL_BYTES).then_some(value)
}

/// Bounded reaction payload: emoji shortened, author bounded, malformed shapes
/// answer None so the envelope skips instead of inventing protocol state.
fn normalized_reaction(value: &Value) -> Option<ControlReceive> {
    let object = value.as_object()?;
    let emoji = object
        .get("emoji")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.chars().take(MAX_EMOJI_CHARS).collect::<String>())?;
    let target_author = ["targetAuthorNumber", "targetAuthorUuid", "targetAuthor"]
        .iter()
        .find_map(|key| object.get(*key).and_then(Value::as_str))
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.chars().take(MAX_RECEIVE_ID_CHARS).collect::<String>())?;
    let target_timestamp = object.get("targetSentTimestamp").and_then(Value::as_u64)?;
    let remove = object
        .get("isRemove")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    Some(ControlReceive::Reaction {
        emoji,
        target_author,
        target_timestamp,
        remove,
    })
}

/// The pin-family target identity (contract 1.33), bounded like the reaction
/// target: the author as the upstream resolved it (E.164 on a contacts hit,
/// ACI string otherwise) and the pinned message's upstream timestamp.
fn normalized_pin_target(value: &Value) -> Option<(String, u64)> {
    let object = value.as_object()?;
    let target_author = ["targetAuthorNumber", "targetAuthorUuid", "targetAuthor"]
        .iter()
        .find_map(|key| object.get(*key).and_then(Value::as_str))
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.chars().take(MAX_RECEIVE_ID_CHARS).collect::<String>())?;
    let target_timestamp = object.get("targetSentTimestamp").and_then(Value::as_u64)?;
    Some((target_author, target_timestamp))
}

/// `dataMessage.pinMessage`: `{targetAuthor, targetSentTimestamp,
/// pinDurationSeconds?}` — absent/null duration is the official forever pin,
/// an out-of-u32 duration drops the field rather than inventing a wrong
/// expiry.
fn normalized_pin_message(value: &Value) -> Option<ControlReceive> {
    let object = value.as_object()?;
    let (target_author, target_timestamp) = normalized_pin_target(value)?;
    let duration_seconds = object
        .get("pinDurationSeconds")
        .and_then(Value::as_u64)
        .filter(|value| *value <= u32::MAX as u64)
        .map(|value| value as u32);
    Some(ControlReceive::PinMessage {
        target_author,
        target_timestamp,
        duration_seconds,
    })
}

/// `dataMessage.unpinMessage`: `{targetAuthor, targetSentTimestamp}` —
/// target identity only.
fn normalized_unpin_message(value: &Value) -> Option<ControlReceive> {
    normalized_pin_target(value).map(|(target_author, target_timestamp)| {
        ControlReceive::UnpinMessage {
            target_author,
            target_timestamp,
        }
    })
}

/// `dataMessage.adminDelete`: `{targetAuthor, targetSentTimestamp}` —
/// target identity only.
fn normalized_admin_delete(value: &Value) -> Option<ControlReceive> {
    normalized_pin_target(value).map(|(target_author, target_timestamp)| {
        ControlReceive::AdminDelete {
            target_author,
            target_timestamp,
        }
    })
}

/// Call signaling (contract 1.50, §4.48): the engine's `callMessage` face —
/// the official Content.callMessage=3 projection. The modern line rides
/// `opaque` (base64 + urgency); the legacy 1:1 frames project when present.
/// A blank CallMessage (nothing projectable) routes nothing — the envelope
/// falls through to the plain skip like any other unclaimed face, never
/// invented into content.
fn normalized_call_message(call: &serde_json::Map<String, Value>) -> Option<ControlReceive> {
    // The modern wire face first: the opaque signaling payload is the
    // framing the current desktop ships; the engine already dropped a blank
    // one (宁缺勿错), so an `opaque` key here always carries material.
    if let Some(data) = call.get("opaque").and_then(Value::as_str) {
        if data.is_empty() || data.chars().count() > MAX_CALL_OPAQUE_CHARS {
            return None;
        }
        return Some(ControlReceive::Call {
            call_id: 0,
            signal_type: "opaque",
            payload: json!({
                "opaque": data,
                "urgency": call.get("urgency").and_then(Value::as_str).unwrap_or("droppable"),
                "destinationDeviceId": call.get("destinationDeviceId").cloned().unwrap_or(Value::Null),
            }),
        });
    }
    // The legacy 1:1 frames, present-keyed: the id is the frame's call
    // identity, the payload is the projected face verbatim. An unparseable
    // id fails the whole face (宁缺勿错 — a frame without a call id has no
    // state machine to address).
    let call_id = |key: &str| -> Option<u64> {
        call.get(key)
            .and_then(Value::as_object)
            .and_then(|frame| frame.get("id"))
            .and_then(Value::as_u64)
            .filter(|id| *id > 0 && *id <= MAX_CALL_ID)
    };
    for (key, signal_type) in [
        ("offer", "offer"),
        ("answer", "answer"),
        ("busy", "busy"),
        ("hangup", "hangup"),
    ] {
        if let Some(id) = call_id(key) {
            let payload = json!({ key: call.get(key) });
            return Some(ControlReceive::Call {
                call_id: id,
                signal_type,
                payload,
            });
        }
    }
    // The legacy ICE list: the first frame names the call, the whole bounded
    // list rides the payload.
    if let Some(ice) = call.get("iceUpdate").and_then(Value::as_array) {
        let first = ice.first().and_then(Value::as_object).and_then(|frame| {
            frame
                .get("id")
                .and_then(Value::as_u64)
                .filter(|id| *id > 0 && *id <= MAX_CALL_ID)
        });
        if let Some(id) = first {
            let mut frames = ice.to_vec();
            frames.truncate(MAX_CALL_ICE_FRAMES);
            return Some(ControlReceive::Call {
                call_id: id,
                signal_type: "ice",
                payload: json!({ "iceUpdate": frames }),
            });
        }
    }
    None
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
        .chain(normalized.source_uuid.iter())
        .chain(normalized.group_id.iter())
        .any(|id| id.chars().count() > MAX_RECEIVE_ID_CHARS)
    {
        return Ok(None);
    }
    Ok(Some(normalized))
}

/// A receive that routes to a conversation but carries no message row of its
/// own (reaction / remote delete / typing). The service layer resolves the
/// conversation the same way it does for messages.
struct ControlRouting {
    direction: &'static str,
    source: Option<String>,
    group_id: Option<String>,
    control: ControlReceive,
}

fn control_routing(envelope: &serde_json::Map<String, Value>) -> Option<ControlRouting> {
    // Peer reaction / remote delete riding a dataMessage without a body.
    if let Some(data_message) = envelope.get("dataMessage").and_then(Value::as_object) {
        let group_id = data_message_group_id(data_message);
        if let Some(reaction) = data_message.get("reaction").and_then(normalized_reaction) {
            return Some(ControlRouting {
                direction: "incoming",
                source: envelope_peer_source(envelope),
                group_id,
                control: reaction,
            });
        }
        if let Some(target) = data_message
            .get("remoteDelete")
            .and_then(Value::as_object)
            .and_then(|object| object.get("timestamp"))
            .and_then(Value::as_u64)
        {
            return Some(ControlRouting {
                direction: "incoming",
                source: envelope_peer_source(envelope),
                group_id,
                control: ControlReceive::RemoteDelete {
                    target_timestamp: target,
                },
            });
        }
        // Pin family (contract 1.33): per-conversation pin state and the
        // admin-delete tombstone, projected by the service layer.
        if let Some(pin) = data_message
            .get("pinMessage")
            .and_then(normalized_pin_message)
        {
            return Some(ControlRouting {
                direction: "incoming",
                source: envelope_peer_source(envelope),
                group_id,
                control: pin,
            });
        }
        if let Some(unpin) = data_message
            .get("unpinMessage")
            .and_then(normalized_unpin_message)
        {
            return Some(ControlRouting {
                direction: "incoming",
                source: envelope_peer_source(envelope),
                group_id,
                control: unpin,
            });
        }
        if let Some(admin_delete) = data_message
            .get("adminDelete")
            .and_then(normalized_admin_delete)
        {
            return Some(ControlRouting {
                direction: "incoming",
                source: envelope_peer_source(envelope),
                group_id,
                control: admin_delete,
            });
        }
        // A body-less editUpdate rides editMessage at the envelope level; a
        // dataMessage with only editUpdate content has no local meaning.
    }
    // The engine's `groupUpdated` projection (2026-10-06 engine batch): a
    // decrypted group expiration-timer change envelope-replaced before it
    // reaches the connector — the notification carries no message row of its
    // own. It routes as a control receive addressed by the group master key;
    // the missing-`expireTimerSeconds`/non-u32 shapes fail closed (the
    // projection guarantees the key, so its absence is a protocol error, not
    // an unknown-timer state). `groupId` absence routes warn-only drop at the
    // service layer like every unresolvable group reference.
    if let Some(update) = envelope.get("groupUpdated").and_then(Value::as_object) {
        let expire_timer_seconds = update
            .get("expireTimerSeconds")
            .and_then(Value::as_u64)
            .and_then(|seconds| u32::try_from(seconds).ok())?;
        let group_id = update
            .get("groupId")
            .and_then(Value::as_str)
            .map(str::to_string);
        return Some(ControlRouting {
            direction: "incoming",
            source: None,
            group_id,
            control: ControlReceive::GroupTimerUpdate {
                expire_timer_seconds,
            },
        });
    }
    // The official `syncMessage.messageRequestResponse` mirror (contract
    // 1.70, §4.58 consume batch): our own linked device answered a
    // conversation's message request — the ACCEPT stamp the §4.58 read
    // derivation previously recorded as having no honest source. It routes as
    // a control receive (the groupUpdated projection precedent) addressed by
    // the thread leg; a malformed or UNKNOWN-shaped face returns None and the
    // envelope falls through to the plain syncMessage skip.
    if let Some(sync) = envelope.get("syncMessage").and_then(Value::as_object)
        && let Some(face) = sync
            .get("messageRequestResponse")
            .and_then(Value::as_object)
        && let Some(control) = normalized_message_request_response(face)
    {
        // The ControlReceive carries the thread leg twice by design (the
        // routing group id and the stamped payload); one clone keeps both.
        let ControlReceive::MessageRequestResponse { group_id, .. } = &control else {
            unreachable!("the projection only answers this variant");
        };
        return Some(ControlRouting {
            direction: "incoming",
            source: None,
            group_id: group_id.clone(),
            control,
        });
    }
    // Call signaling (contract 1.51, §4.49): the engine's callMessage
    // projection routes like every body-less control face — an empty or
    // out-of-budget face returns None and the envelope falls through to the
    // plain skip.
    if let Some(call) = envelope
        .get("callMessage")
        .and_then(Value::as_object)
        .and_then(normalized_call_message)
    {
        return Some(ControlRouting {
            direction: "incoming",
            source: envelope_peer_source(envelope),
            group_id: None,
            control: call,
        });
    }
    // Ephemeral typing indicator.
    if let Some(typing) = envelope.get("typingMessage").and_then(Value::as_object) {
        let action = typing
            .get("action")
            .and_then(Value::as_str)
            .unwrap_or("START")
            .to_string();
        let group_id = typing
            .get("groupId")
            .and_then(Value::as_str)
            .map(str::to_string);
        return Some(ControlRouting {
            direction: "incoming",
            source: envelope_peer_source(envelope),
            group_id,
            control: ControlReceive::Typing { action },
        });
    }
    // Peer receipt confirming our outgoing rows (delivery / read / viewed).
    // Viewed maps to `read` on the wire: the desktop's status ladder is
    // sent → delivered → read; there is no fourth tier to surface.
    if let Some(receipt) = envelope.get("receiptMessage").and_then(Value::as_object) {
        let kind = if receipt.get("isViewed").and_then(Value::as_bool) == Some(true) {
            ReceiptKind::Viewed
        } else if receipt.get("isRead").and_then(Value::as_bool) == Some(true) {
            ReceiptKind::Read
        } else {
            ReceiptKind::Delivered
        };
        let mut timestamps: Vec<u64> = receipt
            .get("timestamp")
            .and_then(Value::as_array)
            .map(|list| list.iter().filter_map(Value::as_u64).collect())
            .unwrap_or_default();
        timestamps.truncate(MAX_RECEIPT_TIMESTAMPS);
        timestamps.dedup();
        if timestamps.is_empty() {
            return None;
        }
        return Some(ControlRouting {
            direction: "incoming",
            source: envelope_peer_source(envelope),
            group_id: None,
            control: ControlReceive::Receipt {
                kind,
                timestamps,
                // Contract 1.32: the receipt's own envelope timestamp backs the
                // desktop's delivered/read timeline.
                when: envelope.get("timestamp").and_then(Value::as_u64),
            },
        });
    }
    None
}

/// The disappearing-message keys one dataMessage face carries (contract
/// 1.42, §4.41). Both engine faces share the shape: `expiresInSeconds` is the
/// stated timer — every signal-cli `JsonDataMessage` carries it (0 = the
/// sender's timer is off), the engine face omits the key when it has nothing
/// to say — and `expirationStartTimestamp` (>0) is the engine-only countdown
/// start the signal-cli face never exposes.
fn envelope_expire_keys(message: &serde_json::Map<String, Value>) -> (Option<u64>, Option<u64>) {
    let seconds = message.get("expiresInSeconds").and_then(Value::as_u64);
    let start_at = message
        .get("expirationStartTimestamp")
        .and_then(Value::as_u64)
        .filter(|value| *value > 0);
    (seconds, start_at)
}

/// Decorate a normalized receive with the face's expire keys.
fn with_expire(
    mut receive: NormalizedReceive,
    seconds: Option<u64>,
    start_at: Option<u64>,
) -> NormalizedReceive {
    receive.expire_seconds = seconds;
    receive.expire_start_at = start_at;
    receive
}

/// The timer-notice system row's text material (contract 1.42, §4.41): the
/// announced value rides the text so the desktop renders the change without
/// parsing protocol state; `0` and the absent-key close form both read as
/// off. Localized copy is the desktop's job — this is the raw material.
fn timer_change_text(seconds: Option<u64>) -> Option<NormalizedText> {
    let text = match seconds {
        Some(value) if value > 0 => format!("已更新消息定时消失：{value} 秒"),
        _ => "已更新消息定时消失：已关闭".to_string(),
    };
    normalized_text(&text)
}

/// The rich marker a timer-notice row carries (its only rich content).
fn timer_change_rich() -> NormalizedRich {
    NormalizedRich {
        is_expiration_update: true,
        ..Default::default()
    }
}

/// The group-update system row's text material (contract 1.45, §4.44): the
/// receive face's `JsonGroupInfo` carries no actor and no before/after, so
/// the copy is one generic line the desktop localizes — the resulting title
/// rides `rich.group_name` as mirror material, never as rendered text.
fn group_update_text() -> Option<NormalizedText> {
    normalized_text("群聊信息已更新")
}

/// The rich marker a group-update row carries (§4.44): the flag plus the
/// engine's post-change title when the update supplied one.
fn group_update_rich(group_name: Option<String>) -> NormalizedRich {
    NormalizedRich {
        is_group_update: true,
        group_name,
        ..Default::default()
    }
}

/// Our own multi-device control echo inside `syncMessage.sentMessage`: edit /
/// remote delete / reaction of an earlier message we sent from the phone.
fn sync_sent_control(sent: &serde_json::Map<String, Value>) -> Option<ControlReceive> {
    if let Some(edit) = sent.get("editMessage").and_then(Value::as_object)
        && let Some(target) = edit.get("targetSentTimestamp").and_then(Value::as_u64)
    {
        return Some(ControlReceive::Edit {
            target_timestamp: target,
        });
    }
    if let Some(target) = sent
        .get("remoteDelete")
        .and_then(Value::as_object)
        .and_then(|object| object.get("timestamp"))
        .and_then(Value::as_u64)
    {
        return Some(ControlReceive::RemoteDelete {
            target_timestamp: target,
        });
    }
    sent.get("reaction").and_then(normalized_reaction)
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
    let source_uuid = envelope
        .get("sourceUuid")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    // The reportSpam companion bit (contract revision 1.49, §4.48): the
    // engine projects the server-assigned message GUID on the envelope's
    // public metadata when present (proto only projects, never invents).
    let server_guid = envelope
        .get("serverGuid")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.chars().take(MAX_RECEIVE_ID_CHARS).collect::<String>());

    let make = |timestamp: Option<u64>,
                content_kind: &'static str,
                direction: &'static str,
                source: Option<String>,
                group_id: Option<String>,
                text: Option<NormalizedText>,
                quote: Option<NormalizedQuote>,
                attachments: Vec<NormalizedAttachment>,
                rich: Option<NormalizedRich>,
                control: Option<ControlReceive>| NormalizedReceive {
        timestamp,
        content_kind,
        direction,
        account_present,
        account: account.clone(),
        source,
        source_uuid: source_uuid.clone(),
        peer_name: peer_name.clone(),
        group_id,
        text: text.as_ref().map(|value| value.value.clone()),
        text_bytes: text.as_ref().map(|value| value.bytes),
        text_truncated: text.is_some_and(|value| value.truncated),
        quote,
        attachments,
        rich,
        sticker: None,
        control,
        expire_seconds: None,
        expire_start_at: None,
        server_guid: server_guid.clone(),
    };

    // Typing and body-less reaction / remote-delete receives carry no message
    // row; they route as control events and never persist.
    if let Some(routing) = control_routing(envelope) {
        let ControlRouting {
            direction,
            source,
            group_id,
            control,
        } = routing;
        return Ok(make(
            timestamp,
            "control",
            direction,
            source,
            group_id,
            None,
            None,
            Vec::new(),
            None,
            Some(control),
        ));
    }

    // Story receives (contract 1.76, §4.67): the envelope-level `storyMessage`
    // face. The story body is not a conversation message — it routes as
    // control-plane material (the call-signaling posture): no row, the
    // bounded projection forwards verbatim, and the stories-disabled gate is
    // the service layer's. The envelope `isStory` bit is the same fact and
    // rides story sends and group story replies alike; the bit alone never
    // routes (a remote delete under a story bit was already consumed by the
    // control ladder above — the official exempt leg). An oversized or
    // otherwise unusable face drops the whole envelope (宁缺勿错).
    if let Some(story_message) = envelope.get("storyMessage").and_then(Value::as_object) {
        return Ok(match bounded_story_material(story_message) {
            Some(message) => make(
                timestamp,
                "storyMessage",
                "incoming",
                peer_source,
                None,
                None,
                None,
                Vec::new(),
                None,
                Some(ControlReceive::Story { message }),
            ),
            None => make(
                timestamp,
                "storyMessage",
                "skip",
                peer_source,
                None,
                None,
                None,
                Vec::new(),
                None,
                None,
            ),
        });
    }

    // Multi-device: phone/other linked device sent a text → show as our outgoing.
    if let Some(sync) = envelope.get("syncMessage").and_then(Value::as_object) {
        // Our own linked device received a peer's view-once open notification
        // (contract 1.38): the service layer resolves the target row across
        // conversations, so no session routing applies. A missing senderAci
        // has no resolvable target — drop as skip rather than invent one.
        if let Some(open) = sync.get("viewOnceOpen").and_then(Value::as_object) {
            let sender_aci = open
                .get("senderAci")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(|s| s.chars().take(MAX_RECEIVE_ID_CHARS).collect::<String>());
            let Some(sender_aci) = sender_aci else {
                return Ok(make(
                    timestamp,
                    "syncMessage",
                    "skip",
                    None,
                    None,
                    None,
                    None,
                    Vec::new(),
                    None,
                    None,
                ));
            };
            let open_timestamp = open.get("timestamp").and_then(Value::as_u64).unwrap_or(0);
            return Ok(make(
                timestamp,
                "control",
                "incoming",
                None,
                None,
                None,
                None,
                Vec::new(),
                None,
                Some(ControlReceive::ViewOnceOpen {
                    sender_aci,
                    timestamp: open_timestamp,
                }),
            ));
        }
        if let Some(sent) = sync.get("sentMessage").and_then(Value::as_object) {
            // JsonUnwrapped dataMessage fields sit on sentMessage itself.
            let text = data_message_text(sent);
            let group_id = data_message_group_id(sent);
            let quote = sent.get("quote").and_then(normalized_quote);
            let attachments = sent
                .get("attachments")
                .map(normalized_attachments)
                .unwrap_or_default();
            let rich = normalized_rich(sent);
            let (expire_seconds, expire_start_at) = envelope_expire_keys(sent);
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
            // Our own multi-device edit / delete / reaction of an earlier
            // message mirrors as a control receive keyed by its target.
            if text.is_none()
                && quote.is_none()
                && attachments.is_empty()
                && rich.is_none()
                && let Some(control) = sync_sent_control(sent)
            {
                return Ok(make(
                    sent.get("timestamp").and_then(Value::as_u64).or(timestamp),
                    "control",
                    "outgoing",
                    destination,
                    group_id,
                    None,
                    None,
                    Vec::new(),
                    None,
                    Some(control),
                ));
            }
            let sent_ts = sent.get("timestamp").and_then(Value::as_u64).or(timestamp);
            // Our own device set the conversation's disappearing timer (the
            // phone-side toggle): the mirror is the same timer-notice system
            // row a peer's change lands as (§4.41), addressed by destination.
            if text.is_none()
                && quote.is_none()
                && attachments.is_empty()
                && rich.as_ref().is_some_and(|rich| rich.is_expiration_update)
            {
                return Ok(with_expire(
                    make(
                        sent_ts,
                        "syncMessage",
                        "system",
                        destination,
                        group_id,
                        timer_change_text(expire_seconds),
                        None,
                        Vec::new(),
                        Some(timer_change_rich()),
                        None,
                    ),
                    expire_seconds,
                    expire_start_at,
                ));
            }
            // Our own device edited the group from the phone (contract 1.45,
            // §4.44): the bodyless mirror lands as the same generic system row
            // a peer's change does — the title mirror is the service layer's
            // job off the rich material. A timer flag outranks it.
            if text.is_none()
                && quote.is_none()
                && attachments.is_empty()
                && rich.as_ref().is_some_and(|rich| rich.is_group_update)
            {
                return Ok(make(
                    sent_ts,
                    "syncMessage",
                    "system",
                    destination,
                    group_id,
                    group_update_text(),
                    None,
                    Vec::new(),
                    Some(group_update_rich(
                        rich.as_ref().and_then(|rich| rich.group_name.clone()),
                    )),
                    None,
                ));
            }
            if let Some(text) = text {
                return Ok(with_expire(
                    make(
                        sent_ts,
                        "syncMessage",
                        "outgoing",
                        destination,
                        group_id,
                        Some(text),
                        quote,
                        attachments,
                        rich,
                        None,
                    ),
                    expire_seconds,
                    expire_start_at,
                ));
            }
            // Attachment-only multi-device send: no body but real descriptors.
            if !attachments.is_empty() {
                return Ok(with_expire(
                    make(
                        sent_ts,
                        "syncMessage",
                        "outgoing",
                        destination,
                        group_id,
                        None,
                        quote,
                        attachments,
                        rich,
                        None,
                    ),
                    expire_seconds,
                    expire_start_at,
                ));
            }
            // sent without body (sticker/other control sync) — skip for now
            return Ok(make(
                sent_ts,
                "syncMessage",
                "skip",
                destination,
                group_id,
                None,
                None,
                Vec::new(),
                rich,
                None,
            ));
        }
        // contacts/groups/read sync — ignore
        return Ok(make(
            timestamp,
            "syncMessage",
            "skip",
            peer_source,
            None,
            None,
            None,
            Vec::new(),
            None,
            None,
        ));
    }

    if let Some(data_message) = envelope.get("dataMessage").and_then(Value::as_object) {
        let group_id = data_message_group_id(data_message);
        let text = data_message_text(data_message);
        let quote = data_message.get("quote").and_then(normalized_quote);
        let mut attachments = data_message
            .get("attachments")
            .map(normalized_attachments)
            .unwrap_or_default();
        // Sticker (contract revision 1.35): the metadata normalizes beside the
        // message, and the `data` pointer joins the attachment descriptor list
        // as a regular metadata-only descriptor — byte fetches stay on the
        // existing media channel. A sticker with no usable data pointer has
        // nothing renderable and keeps the plain skip routing.
        let sticker = data_message
            .get("sticker")
            .and_then(Value::as_object)
            .and_then(normalized_sticker);
        if let Some((_, sticker_image)) = sticker.as_ref() {
            attachments.push(sticker_image.clone());
        }
        let rich = normalized_rich(data_message);
        let (expire_seconds, expire_start_at) = envelope_expire_keys(data_message);
        let with_sticker =
            |mut receive: NormalizedReceive,
             sticker: Option<(NormalizedSticker, NormalizedAttachment)>| {
                receive.sticker = sticker.map(|(meta, _)| meta);
                receive
            };
        if let Some(text) = text {
            return Ok(with_expire(
                with_sticker(
                    make(
                        timestamp,
                        "dataMessage",
                        "incoming",
                        peer_source,
                        group_id,
                        Some(text),
                        quote,
                        attachments,
                        rich,
                        None,
                    ),
                    sticker,
                ),
                expire_seconds,
                expire_start_at,
            ));
        }
        // Attachment-only message: real descriptors, no body — a sticker row
        // lands here through its data descriptor.
        if !attachments.is_empty() {
            return Ok(with_expire(
                with_sticker(
                    make(
                        timestamp,
                        "dataMessage",
                        "incoming",
                        peer_source,
                        group_id,
                        None,
                        quote,
                        attachments,
                        rich,
                        None,
                    ),
                    sticker,
                ),
                expire_seconds,
                expire_start_at,
            ));
        }
        // Empty body: map a few control shapes to system rows; drop the rest.
        // A timer change is one of them (contract 1.42, §4.41): the
        // announced value rides the text material and the receive's
        // expire_seconds so the service layer mirrors the conversation state.
        if rich.as_ref().is_some_and(|rich| rich.is_expiration_update) {
            return Ok(with_expire(
                make(
                    timestamp,
                    "dataMessage",
                    "system",
                    peer_source,
                    group_id,
                    timer_change_text(expire_seconds),
                    None,
                    Vec::new(),
                    Some(timer_change_rich()),
                    None,
                ),
                expire_seconds,
                expire_start_at,
            ));
        }
        // Bodyless group-state change (contract 1.45, §4.44): the pre-1.45
        // shape fell through to the skip below and was swallowed. It projects
        // as a generic system row; the rich marker carries the resulting
        // title for the service layer's conversation-title mirror.
        if rich.as_ref().is_some_and(|rich| rich.is_group_update) {
            return Ok(make(
                timestamp,
                "dataMessage",
                "system",
                peer_source,
                group_id,
                group_update_text(),
                None,
                Vec::new(),
                Some(group_update_rich(
                    rich.as_ref().and_then(|rich| rich.group_name.clone()),
                )),
                None,
            ));
        }
        return Ok(make(
            timestamp,
            "dataMessage",
            "skip",
            peer_source,
            group_id,
            None,
            None,
            Vec::new(),
            None,
            None,
        ));
    }

    // Top-level editMessage envelope: a peer edited an earlier message.
    if let Some(edit) = envelope.get("editMessage").and_then(Value::as_object) {
        if let Some(target) = edit.get("targetSentTimestamp").and_then(Value::as_u64) {
            let new_text = edit
                .get("dataMessage")
                .and_then(Value::as_object)
                .and_then(|data| data.get("message"))
                .and_then(Value::as_str)
                .and_then(normalized_text);
            if let Some(new_text) = new_text {
                return Ok(make(
                    timestamp,
                    "editMessage",
                    "incoming",
                    peer_source,
                    data_message_group_id(
                        edit.get("dataMessage")
                            .and_then(Value::as_object)
                            .unwrap_or(&serde_json::Map::new()),
                    ),
                    Some(new_text),
                    None,
                    Vec::new(),
                    None,
                    Some(ControlReceive::Edit {
                        target_timestamp: target,
                    }),
                ));
            }
        }
        return Ok(make(
            timestamp,
            "editMessage",
            "skip",
            peer_source,
            None,
            None,
            None,
            Vec::new(),
            None,
            None,
        ));
    }

    let content_kind = if envelope.contains_key("receiptMessage") {
        "receiptMessage"
    } else {
        "other"
    };
    Ok(make(
        timestamp,
        content_kind,
        "skip",
        peer_source,
        None,
        None,
        None,
        Vec::new(),
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
    use std::io::Write as _;

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
            signal_cli_args(SignalCliMode::Jvm, Some(&proxy), Path::new("/data"), false),
            expected
        );
        assert_eq!(
            signal_cli_args(SignalCliMode::Native, None, Path::new("/data"), false),
            expected
        );
    }

    /// Media opt-in (ADR 0002 gate 5): enabling `--media-ingest` changes the
    /// spawn argv by exactly one line — `--ignore-attachments` disappears;
    /// stories and stickers stay ignored and every other byte is unchanged.
    #[test]
    fn media_ingest_only_drops_ignore_attachments_from_the_argv() {
        let baseline = signal_cli_args(SignalCliMode::Jvm, None, Path::new("/data"), false);
        let media = signal_cli_args(SignalCliMode::Jvm, None, Path::new("/data"), true);
        let dropped: Vec<_> = baseline
            .iter()
            .filter(|arg| !media.contains(arg))
            .cloned()
            .collect();
        let added: Vec<_> = media
            .iter()
            .filter(|arg| !baseline.contains(arg))
            .cloned()
            .collect();
        assert_eq!(
            dropped,
            vec![std::ffi::OsString::from("--ignore-attachments")]
        );
        assert!(added.is_empty());
        // Stories and stickers are outside the media PoC in both shapes.
        assert!(baseline.contains(&std::ffi::OsString::from("--ignore-stories")));
        assert!(baseline.contains(&std::ffi::OsString::from("--ignore-stickers")));
        assert!(media.contains(&std::ffi::OsString::from("--ignore-stories")));
        assert!(media.contains(&std::ffi::OsString::from("--ignore-stickers")));
        // The config default stays opt-out.
        let config = SignalCliConfig::new(PathBuf::from("/bin/signal-cli"), PathBuf::from("/data"));
        assert!(!config.media_ingest);
    }

    #[test]
    fn native_mode_prepends_socks_proxy_properties_to_args() {
        let proxy = SocksProxy {
            host: "127.0.0.1".into(),
            port: 1080,
        };
        let args = signal_cli_args(
            SignalCliMode::Native,
            Some(&proxy),
            Path::new("/data"),
            false,
        );
        assert_eq!(
            args[..2],
            [
                std::ffi::OsString::from("-DsocksProxyHost=127.0.0.1"),
                std::ffi::OsString::from("-DsocksProxyPort=1080"),
            ]
        );
        assert_eq!(args[2], "--data-dir");
    }

    /// kt-engine mode (implementation plan §5): the engine's argv parser is
    /// fail-closed on unknown arguments, so the `-D` SOCKS prepend of native
    /// mode is never emitted — even if a proxy somehow reaches this function,
    /// the tail stays byte-identical to the signal-cli launcher shape. The
    /// launcher-level validation is what rejects the combination outright.
    #[test]
    fn kt_engine_mode_never_prepends_socks_properties_to_args() {
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
        assert_eq!(
            signal_cli_args(
                SignalCliMode::KtEngine,
                Some(&proxy),
                Path::new("/data"),
                false
            ),
            expected
        );
        assert_eq!(
            signal_cli_args(SignalCliMode::KtEngine, None, Path::new("/data"), false),
            expected
        );
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
            source_uuid: None,
            peer_name: None,
            group_id: None,
            text: Some("a".repeat(MAX_INBOUND_TEXT_BYTES)),
            text_bytes: Some(MAX_INBOUND_TEXT_BYTES as u32),
            text_truncated: false,
            quote: None,
            attachments: Vec::new(),
            rich: None,
            sticker: None,
            control: None,
            expire_seconds: None,
            expire_start_at: None,
            server_guid: None,
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
        // Close form: the flag without a stated value reads as off (§4.41).
        assert_eq!(system.text.as_deref(), Some("已更新消息定时消失：已关闭"));
        assert_eq!(system.expire_seconds, None);
        assert_eq!(
            system.rich.as_ref().map(|rich| rich.is_expiration_update),
            Some(true)
        );

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

    /// Sticker receive projection (contract revision 1.35): a sticker with a
    /// usable `data` pointer lands as an incoming row carrying the pack
    /// identity beside a metadata-only descriptor (the byte pointer rides the
    /// attachment list); a sticker without one keeps the plain skip routing,
    /// and absent sticker keys stay absent on the wire.
    #[test]
    fn receive_normalization_projects_sticker_metadata_and_data_descriptor() {
        let with_data = normalize_receive(&json!({
            "account": "+15555550100",
            "envelope": {
                "source": "+15555550101",
                "timestamp": 54,
                "dataMessage": {
                    "sticker": {
                        "packId": "abcdef01",
                        "packKey": "AAAAAAAAAAAAAAAAAAAAAA==",
                        "stickerId": 7,
                        "emoji": "🎉",
                        "data": {
                            "id": "att-1",
                            "contentType": "image/webp",
                            "size": 8192,
                            "width": 512,
                            "height": 512
                        }
                    }
                }
            }
        }))
        .unwrap()
        .unwrap();
        assert_eq!(with_data.direction, "incoming");
        let sticker = with_data.sticker.as_ref().expect("sticker metadata");
        assert_eq!(sticker.pack_id, "abcdef01");
        assert_eq!(sticker.pack_key, "AAAAAAAAAAAAAAAAAAAAAA==");
        assert_eq!(sticker.sticker_id, 7);
        assert_eq!(sticker.emoji.as_deref(), Some("🎉"));
        assert_eq!(with_data.attachments.len(), 1);
        let descriptor = &with_data.attachments[0];
        assert_eq!(descriptor.id, "att-1");
        assert_eq!(descriptor.content_type.as_deref(), Some("image/webp"));
        assert_eq!(descriptor.size, Some(8192));
        assert_eq!(descriptor.width, Some(512));
        assert_eq!(descriptor.height, Some(512));
        assert!(!descriptor.is_voice_note);
        // The metadata-only engines project no locator: the ingest face stays
        // the honest hole (contract revision 1.61).
        assert_eq!(descriptor.locator, None);

        // A receive envelope that does project the transit locator parses it
        // under the 1.56 ingest caps, index-aligned with its descriptor.
        let with_locator = normalize_receive(&json!({
            "account": "+15555550100",
            "envelope": {
                "source": "+15555550101",
                "timestamp": 57,
                "dataMessage": {
                    "attachments": [{
                        "id": "att-loc",
                        "contentType": "image/jpeg",
                        "size": 4096,
                        "locator": {
                            "key": crate::service::base64_encode(&[0x33_u8; 64]),
                            "cdnKey": "live-cdn-key",
                            "cdnNumber": 2,
                            "digest": crate::service::base64_encode(&[0x44_u8; 32]),
                            "plaintextHash": "AABBCCDD"
                        }
                    }]
                }
            }
        }))
        .unwrap()
        .unwrap();
        let face = with_locator.attachments[0]
            .locator
            .as_ref()
            .expect("a projected locator parses");
        assert_eq!(face.cdn_key.as_deref(), Some("live-cdn-key"));
        assert_eq!(face.cdn_number, Some(2));
        assert!(face.key.is_some());
        assert!(face.digest.is_some());
        assert_eq!(face.plaintext_hash.as_deref(), Some("aabbccdd"));
        // A locator whose every field is missing or wrong-shaped collapses to
        // the hole, never a half-face.
        let hostile = normalize_receive(&json!({
            "account": "+15555550100",
            "envelope": {
                "source": "+15555550101",
                "timestamp": 58,
                "dataMessage": {
                    "attachments": [{
                        "id": "att-bad",
                        "locator": { "key": 7, "cdnNumber": -1, "plaintextHash": "zzz" }
                    }]
                }
            }
        }))
        .unwrap()
        .unwrap();
        assert_eq!(hostile.attachments[0].locator, None);

        // The pack identity does not ride the engine-side normalization wire
        // shape (the service layer projects it onto MessageRecord, asserted in
        // the store round-trip test); it is reachable programmatically here.
        let plain = normalize_receive(&json!({
            "account": "+15555550100",
            "envelope": {
                "source": "+15555550101",
                "timestamp": 55,
                "dataMessage": { "message": "plain" }
            }
        }))
        .unwrap()
        .unwrap();
        assert!(plain.sticker.is_none());
        assert!(plain.attachments.is_empty());

        // No usable data pointer → nothing renderable: the plain skip
        // routing, no half-carried sticker state.
        let without_data = normalize_receive(&json!({
            "account": "+15555550100",
            "envelope": {
                "source": "+15555550101",
                "timestamp": 56,
                "dataMessage": {
                    "sticker": {
                        "packId": "abcdef01",
                        "packKey": "AAAAAAAAAAAAAAAAAAAAAA==",
                        "stickerId": 7
                    }
                }
            }
        }))
        .unwrap()
        .unwrap();
        assert_eq!(without_data.direction, "skip");
        assert!(without_data.sticker.is_none());
        assert!(without_data.attachments.is_empty());
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
            source_uuid: None,
            peer_name: None,
            group_id: None,
            text: Some("a".repeat(MAX_INBOUND_TEXT_BYTES)),
            text_bytes: Some(MAX_INBOUND_TEXT_BYTES as u32),
            text_truncated: false,
            quote: None,
            attachments: Vec::new(),
            rich: None,
            sticker: None,
            control: None,
            expire_seconds: None,
            expire_start_at: None,
            server_guid: None,
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
            source_uuid: None,
            peer_name: None,
            group_id: None,
            text: Some("a".repeat(MAX_INBOUND_TEXT_BYTES)),
            text_bytes: Some(MAX_INBOUND_TEXT_BYTES as u32),
            text_truncated: false,
            quote: None,
            attachments: Vec::new(),
            rich: None,
            sticker: None,
            control: None,
            expire_seconds: None,
            expire_start_at: None,
            server_guid: None,
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

    /// contract 1.21: an upstream error carrying the unlink signature is
    /// classified as Unauthorized and attributed to the request's account.
    #[tokio::test]
    async fn unauthorized_error_response_is_classified_and_attributed() {
        let (events, mut event_rx) = event_channel();
        let (ingress, _receiver) = receive_channel();
        let mut pending = HashMap::new();
        let mut degraded = false;
        let (response_tx, response_rx) = oneshot::channel();
        pending.insert(
            "kt-1".to_string(),
            PendingRequest {
                class: CallClass::ReadOnly,
                account: Some("+15555550100".to_string()),
                response: response_tx,
            },
        );

        let line = serde_json::to_string(&json!({
            "jsonrpc": "2.0",
            "id": "kt-1",
            "error": {
                "code": -32603,
                "message": "Failed to send message: Authorization failed! (AuthorizationFailedException)"
            }
        }))
        .unwrap();
        handle_upstream_line(&line, &mut pending, &events, &ingress, &mut degraded)
            .await
            .unwrap();

        assert_eq!(response_rx.await.unwrap(), Err(EngineError::Unauthorized));
        assert!(matches!(
            event_rx.recv().await.unwrap(),
            EngineEvent::AccountUnauthorized { ref account } if account == "+15555550100"
        ));
    }

    /// The post-mortem short-circuit form ("User <account> is not
    /// registered.", signal-cli App.java:342 after persisted
    /// `registered=false`) classifies as Unauthorized and is attributed to
    /// the account exactly like the live authorization-failure form.
    #[tokio::test]
    async fn unregistered_post_mortem_error_is_classified_and_attributed() {
        let (events, mut event_rx) = event_channel();
        let (ingress, _receiver) = receive_channel();
        let mut pending = HashMap::new();
        let mut degraded = false;
        let (response_tx, response_rx) = oneshot::channel();
        pending.insert(
            "kt-1".to_string(),
            PendingRequest {
                class: CallClass::ReadOnly,
                account: Some("+15555550100".to_string()),
                response: response_tx,
            },
        );

        let line = serde_json::to_string(&json!({
            "jsonrpc": "2.0",
            "id": "kt-1",
            "error": {
                "code": -1,
                "message": "User +15555550100 is not registered."
            }
        }))
        .unwrap();
        handle_upstream_line(&line, &mut pending, &events, &ingress, &mut degraded)
            .await
            .unwrap();

        assert_eq!(response_rx.await.unwrap(), Err(EngineError::Unauthorized));
        assert!(matches!(
            event_rx.recv().await.unwrap(),
            EngineEvent::AccountUnauthorized { ref account } if account == "+15555550100"
        ));
    }

    /// The recipient-level form ("The user <id> is not registered.",
    /// unblock/updateContact/sendPoll* family) targets a CONTACT, not the
    /// local account: it must stay a generic upstream error and must not
    /// emit an account-unauthorized event.
    #[tokio::test]
    async fn recipient_not_registered_error_stays_unclassified() {
        let (events, mut event_rx) = event_channel();
        let (ingress, _receiver) = receive_channel();
        let mut pending = HashMap::new();
        let mut degraded = false;
        let (response_tx, response_rx) = oneshot::channel();
        pending.insert(
            "kt-1".to_string(),
            PendingRequest {
                class: CallClass::ReadOnly,
                account: Some("+15555550100".to_string()),
                response: response_tx,
            },
        );

        let line = serde_json::to_string(&json!({
            "jsonrpc": "2.0",
            "id": "kt-1",
            "error": {
                "code": -1,
                "message": "The user +15555550200 is not registered."
            }
        }))
        .unwrap();
        handle_upstream_line(&line, &mut pending, &events, &ingress, &mut degraded)
            .await
            .unwrap();

        assert_eq!(response_rx.await.unwrap(), Err(EngineError::Upstream));
        assert!(event_rx.try_recv().is_err());
    }

    /// Any other upstream error keeps the generic Upstream classification and
    /// must not emit an account event.
    #[tokio::test]
    async fn generic_upstream_error_stays_unclassified() {
        let (events, mut event_rx) = event_channel();
        let (ingress, _receiver) = receive_channel();
        let mut pending = HashMap::new();
        let mut degraded = false;
        let (response_tx, response_rx) = oneshot::channel();
        pending.insert(
            "kt-2".to_string(),
            PendingRequest {
                class: CallClass::ReadOnly,
                account: Some("+15555550100".to_string()),
                response: response_tx,
            },
        );

        let line = serde_json::to_string(&json!({
            "jsonrpc": "2.0",
            "id": "kt-2",
            "error": { "code": -1, "message": "User input error (UserErrorException)" }
        }))
        .unwrap();
        handle_upstream_line(&line, &mut pending, &events, &ingress, &mut degraded)
            .await
            .unwrap();

        assert_eq!(response_rx.await.unwrap(), Err(EngineError::Upstream));
        assert!(event_rx.try_recv().is_err());
    }

    /// Structured engine errors (contract 1.36): the real engine carries the
    /// allowlisted literal in the error `message` with a numeric `code` (-1,
    /// JSON-RPC 2.0 numeric slot); it surfaces as UpstreamCode with the
    /// allowlisted code. A string `code` carrying the literal is accepted as
    /// a fallback. An unknown literal degrades to the plain Upstream
    /// classification so an engine newer than this connector cannot widen
    /// the host contract; an unrelated message stays plain as before.
    #[tokio::test]
    async fn structured_engine_errors_carry_the_allowlisted_code() {
        let classify = |error: serde_json::Value| async move {
            let (events, mut event_rx) = event_channel();
            let (ingress, _receiver) = receive_channel();
            let mut pending = HashMap::new();
            let mut degraded = false;
            let (response_tx, response_rx) = oneshot::channel();
            pending.insert(
                "kt-9".to_string(),
                PendingRequest {
                    class: CallClass::ReadOnly,
                    account: None,
                    response: response_tx,
                },
            );
            let line = serde_json::to_string(&json!({
                "jsonrpc": "2.0",
                "id": "kt-9",
                "error": error
            }))
            .unwrap();
            handle_upstream_line(&line, &mut pending, &events, &ingress, &mut degraded)
                .await
                .unwrap();
            assert!(event_rx.try_recv().is_err());
            response_rx.await.unwrap()
        };
        // Real engine wire shape: numeric code, allowlisted literal in
        // `message`.
        assert_eq!(
            classify(json!({
                "code": -1,
                "message": "STICKER_PACK_FETCH_FAILED"
            }))
            .await,
            Err(EngineError::UpstreamCode {
                code: "STICKER_PACK_FETCH_FAILED",
                data: None,
            })
        );
        assert_eq!(
            classify(json!({
                "code": -1,
                "message": "CONVERSATION_NOT_RESOLVED"
            }))
            .await,
            Err(EngineError::UpstreamCode {
                code: "CONVERSATION_NOT_RESOLVED",
                data: None,
            })
        );
        // Fallback shape: string `code` carrying the literal directly.
        assert_eq!(
            classify(json!({
                "code": "STORAGE_WRITE_FAILED",
                "message": "fixture write failed"
            }))
            .await,
            Err(EngineError::UpstreamCode {
                code: "STORAGE_WRITE_FAILED",
                data: None,
            })
        );
        // Contract 1.50: the engine's error.data object rides along verbatim
        // on an allowlisted literal (the PROFILE_UPLOAD_FAILED
        // partial-success facts).
        assert_eq!(
            classify(json!({
                "code": -1,
                "message": "PROFILE_UPLOAD_FAILED",
                "data": { "retryable": true, "textSaved": true }
            }))
            .await,
            Err(EngineError::UpstreamCode {
                code: "PROFILE_UPLOAD_FAILED",
                data: Some(json!({ "retryable": true, "textSaved": true })),
            })
        );
        // A non-object data member is dropped rather than passed through.
        assert_eq!(
            classify(json!({
                "code": -1,
                "message": "PROFILE_UPLOAD_FAILED",
                "data": "garbage"
            }))
            .await,
            Err(EngineError::UpstreamCode {
                code: "PROFILE_UPLOAD_FAILED",
                data: None,
            })
        );
        // Unknown literal: degrades to plain Upstream.
        assert_eq!(
            classify(json!({
                "code": -1,
                "message": "SOME_FUTURE_ENGINE_CODE"
            }))
            .await,
            Err(EngineError::Upstream)
        );
        // Ordinary JSON-RPC error shape (unrelated message): plain Upstream.
        assert_eq!(
            classify(json!({ "code": -1, "message": "fixture malformed" })).await,
            Err(EngineError::Upstream)
        );
    }

    /// A peer receiptMessage envelope routes to `ControlReceive::Receipt`:
    /// tier selection follows the isViewed > isRead > isDelivery flags, the
    /// confirmed timestamps ride through bounded and de-duplicated, and an
    /// empty batch is not routed at all.
    #[test]
    fn receipt_message_envelope_routes_to_receipt_control() {
        let envelope_of = |flags: serde_json::Value| -> serde_json::Value {
            serde_json::from_value(json!({
                "timestamp": 1727000000000u64,
                "source": "+15555550101",
                "sourceDevice": 1,
                "receiptMessage": flags,
            }))
            .unwrap()
        };
        let routing = |flags| {
            control_routing(envelope_of(flags).as_object().unwrap()).map(|routing| routing.control)
        };

        let delivered = routing(json!({
            "timestamp": [1726999000000u64, 1726999000000u64, 1726999001000u64],
            "when": 1727000000000u64,
            "isDelivery": true,
        }))
        .expect("delivery receipt routes");
        match delivered {
            ControlReceive::Receipt {
                kind,
                timestamps,
                when,
            } => {
                assert!(matches!(kind, ReceiptKind::Delivered));
                assert_eq!(timestamps, vec![1726999000000, 1726999001000]);
                // Contract 1.32: the envelope's own timestamp backs the
                // delivered/read timeline.
                assert_eq!(when, Some(1727000000000));
            }
            other => panic!("unexpected control: {other:?}"),
        }

        let read = routing(json!({
            "timestamp": [1726999000000u64],
            "isDelivery": true,
            "isRead": true,
        }))
        .expect("read receipt routes");
        assert!(matches!(
            read,
            ControlReceive::Receipt {
                kind: ReceiptKind::Read,
                ..
            }
        ));

        let viewed = routing(json!({
            "timestamp": [1726999000000u64],
            "isRead": true,
            "isViewed": true,
        }))
        .expect("viewed receipt routes");
        assert!(matches!(
            viewed,
            ControlReceive::Receipt {
                kind: ReceiptKind::Viewed,
                ..
            }
        ));

        assert!(routing(json!({ "timestamp": [], "isDelivery": true })).is_none());
        assert!(routing(json!({ "isDelivery": true })).is_none());
    }

    /// The pin family (contract 1.33) routes off `dataMessage` with the
    /// reaction targetAuthor resolution: pin carries the optional duration
    /// (absent = forever), unpin and adminDelete carry the target identity
    /// only, and a malformed payload is not routed at all.
    #[test]
    fn pin_family_envelopes_route_to_pin_controls() {
        let routing = |data: serde_json::Value| {
            let envelope: serde_json::Value = serde_json::from_value(json!({
                "timestamp": 1727000000000u64,
                "source": "+15555550101",
                "sourceDevice": 1,
                "dataMessage": data,
            }))
            .unwrap();
            control_routing(envelope.as_object().unwrap()).map(|routing| routing.control)
        };

        let pin = routing(json!({
            "pinMessage": {
                "targetAuthor": "+15555550101",
                "targetSentTimestamp": 1726999000000u64,
                "pinDurationSeconds": 604800u64,
            },
        }))
        .expect("timed pin routes");
        assert!(matches!(
            pin,
            ControlReceive::PinMessage {
                ref target_author,
                target_timestamp,
                duration_seconds: Some(604800),
            } if target_author == "+15555550101" && target_timestamp == 1726999000000
        ));

        let forever = routing(json!({
            "pinMessage": {
                "targetAuthor": "9d7762bb-1fb5-4cfd-9b15-aef4f8ef9c14",
                "targetSentTimestamp": 1726999000000u64,
            },
        }))
        .expect("forever pin routes");
        assert!(matches!(
            forever,
            ControlReceive::PinMessage {
                duration_seconds: None,
                ..
            }
        ));

        let unpin = routing(json!({
            "unpinMessage": {
                "targetAuthor": "+15555550101",
                "targetSentTimestamp": 1726999000000u64,
            },
        }))
        .expect("unpin routes");
        assert!(matches!(unpin, ControlReceive::UnpinMessage { .. }));

        let admin_delete = routing(json!({
            "adminDelete": {
                "targetAuthor": "+15555550101",
                "targetSentTimestamp": 1726999000000u64,
            },
        }))
        .expect("adminDelete routes");
        assert!(matches!(admin_delete, ControlReceive::AdminDelete { .. }));

        // Malformed shapes drop instead of inventing protocol state.
        assert!(routing(json!({ "pinMessage": { "targetSentTimestamp": 1 } })).is_none());
        assert!(routing(json!({ "pinMessage": { "targetAuthor": "+15555550101" } })).is_none());
        assert!(routing(json!({ "unpinMessage": { "targetAuthor": "+15555550101" } })).is_none());
        assert!(routing(json!({ "adminDelete": { "targetSentTimestamp": 1 } })).is_none());
    }

    /// The engine's `groupUpdated` projection (2026-10-06 engine batch,
    /// kt-signal-engine main @ 216a4c7) routes as a control receive off the
    /// envelope-level key: the timer value normalizes through the u32 face,
    /// 0 = off rides verbatim, and the shapes the projection never produces —
    /// missing or non-u32 `expireTimerSeconds` — drop instead of inventing
    /// timer state.
    #[test]
    fn group_updated_projection_routes_to_a_control_receive() {
        let routing = |envelope: serde_json::Value| {
            control_routing(envelope.as_object().unwrap())
                .map(|routing| (routing.control, routing.group_id, routing.direction))
        };

        let (control, group_id, direction) = routing(json!({
            "timestamp": 1727000000000u64,
            "groupUpdated": {
                "groupId": "ZmFrZS1ncm91cC0x",
                "revision": 5,
                "expireTimerSeconds": 3600,
                "editor": "9d7762bb-1fb5-4cfd-9b15-aef4f8ef9c14",
            },
        }))
        .expect("groupUpdated routes");
        assert!(matches!(
            control,
            ControlReceive::GroupTimerUpdate {
                expire_timer_seconds: 3600,
            }
        ));
        assert_eq!(group_id.as_deref(), Some("ZmFrZS1ncm91cC0x"));
        assert_eq!(direction, "incoming");

        // 0 = off rides verbatim (the projection guarantees the key).
        let (control, _, _) = routing(json!({
            "timestamp": 1727000000000u64,
            "groupUpdated": {
                "groupId": "ZmFrZS1ncm91cC0x",
                "revision": 6,
                "expireTimerSeconds": 0,
            },
        }))
        .expect("the off form routes");
        assert!(matches!(
            control,
            ControlReceive::GroupTimerUpdate {
                expire_timer_seconds: 0,
            }
        ));

        // Shapes the projection never produces drop (fail closed): the key is
        // guaranteed present, so its absence is a protocol error, and no timer
        // state is invented from a malformed value.
        assert!(
            routing(json!({
                "timestamp": 1727000000000u64,
                "groupUpdated": { "groupId": "ZmFrZS1ncm91cC0x", "revision": 7 },
            }))
            .is_none()
        );
        assert!(
            routing(json!({
                "timestamp": 1727000000000u64,
                "groupUpdated": {
                    "groupId": "ZmFrZS1ncm91cC0x",
                    "revision": 7,
                    "expireTimerSeconds": 4294967296u64,
                },
            }))
            .is_none()
        );

        // A missing groupId still routes the control (the value is honest);
        // the service layer drops it warn-only when nothing resolves.
        let (control, group_id, _) = routing(json!({
            "timestamp": 1727000000000u64,
            "groupUpdated": { "revision": 8, "expireTimerSeconds": 60 },
        }))
        .expect("the value survives a missing group reference");
        assert!(matches!(
            control,
            ControlReceive::GroupTimerUpdate {
                expire_timer_seconds: 60,
            }
        ));
        assert!(group_id.is_none());
    }

    /// Rich-body fields (contract 1.25) ride a dataMessage through
    /// normalization bounded: cards/ranges capped, strings truncated, invalid
    /// styles dropped — and a plain message carries no rich payload at all.
    #[test]
    fn data_message_rich_fields_normalize_bounded() {
        let normalize = |data: serde_json::Value| {
            normalize_receive_fields(&json!({
                "envelope": {
                    "timestamp": 1727000000000u64,
                    "source": "+15555550101",
                    "dataMessage": data,
                },
            }))
            .unwrap()
        };
        let rich = normalize(json!({
            "message": "hello @peer check this link",
            "viewOnce": true,
            "previews": [{
                "url": "https://example.com/article",
                "title": "An article",
                "description": "Readable summary",
                "image": { "id": "att-1", "contentType": "image/jpeg",
                           "filename": "card.jpg", "size": 2048,
                           "width": 64, "height": 32 },
            }],
            "mentions": [{ "number": "+15555550101", "name": "Peer",
                           "start": 6, "length": 4 }],
            "textStyles": [
                { "style": "BOLD", "start": 0, "length": 5 },
                { "style": "SPOILER", "start": 6, "length": 4 },
            ],
        }))
        .rich
        .expect("rich payload");
        assert_eq!(rich.previews.len(), 1);
        assert_eq!(rich.previews[0].url, "https://example.com/article");
        assert_eq!(rich.previews[0].title.as_deref(), Some("An article"));
        assert_eq!(
            rich.previews[0]
                .image
                .as_ref()
                .map(|image| image.id.as_str()),
            Some("att-1")
        );
        assert_eq!(rich.mentions.len(), 1);
        assert_eq!(rich.mentions[0].author, "+15555550101");
        assert_eq!(rich.mentions[0].name.as_deref(), Some("Peer"));
        assert_eq!(rich.mentions[0].start, 6);
        assert_eq!(rich.mentions[0].length, 4);
        // Contract 1.40 (§4.40): a number-only envelope has no honest ACI.
        assert_eq!(rich.mentions[0].author_aci, None);
        assert_eq!(
            rich.text_styles
                .iter()
                .map(|style| style.style.as_str())
                .collect::<Vec<_>>(),
            ["BOLD", "SPOILER"]
        );
        assert!(rich.view_once);

        // A plain message carries nothing.
        let plain = normalize(json!({ "message": "hello" }));
        assert!(plain.rich.is_none());
    }

    /// Rich bounds (§4.20): entries past a cap drop, oversized strings
    /// truncate, malformed entries and unknown styles drop individually.
    #[test]
    fn rich_fields_enforce_bounds() {
        let normalize = |data: serde_json::Value| {
            normalize_receive_fields(&json!({
                "envelope": {
                    "timestamp": 1727000000000u64,
                    "source": "+15555550101",
                    "dataMessage": data,
                },
            }))
            .unwrap()
        };
        let previews = (0..8)
            .map(|index| json!({ "url": format!("https://example.com/{index}"), "title": "t" }))
            .collect::<Vec<_>>();
        let mentions = (0..70)
            .map(
                |index| json!({ "number": format!("+1555555010{index}"), "start": 0, "length": 1 }),
            )
            .collect::<Vec<_>>();
        let long_url = "https://example.com/".repeat(300);
        let rich = normalize(json!({
            "message": "rich",
            "previews": previews,
            "mentions": mentions,
            "textStyles": [
                { "style": "NONE", "start": 0, "length": 1 },
                { "style": "MAGIC", "start": 0, "length": 1 },
                { "style": "MONOSPACE", "start": 0, "length": 1 },
                { "style": "BOLD", "start": 0 },
                { "style": "BOLD" },
            ],
        }))
        .rich
        .expect("rich payload");
        assert_eq!(rich.previews.len(), MAX_PREVIEWS);
        assert_eq!(rich.mentions.len(), MAX_MENTIONS);
        assert_eq!(
            rich.text_styles.len(),
            1,
            "only the valid MONOSPACE range survives"
        );
        assert_eq!(rich.text_styles[0].style, "MONOSPACE");

        let truncated = normalize(json!({
            "message": "long",
            "previews": [{ "url": long_url, "title": "x".repeat(2000) }],
        }))
        .rich
        .expect("rich payload");
        assert_eq!(
            truncated.previews[0].url.chars().count(),
            MAX_PREVIEW_URL_CHARS
        );
        assert!(
            truncated.previews[0].title.as_ref().unwrap().len() <= MAX_PREVIEW_FIELD_BYTES,
            "title truncates to its byte bound"
        );

        // All-default shapes collapse to None, malformed entries never fail
        // the message.
        let none = normalize(json!({
            "message": "plain",
            "previews": [{ "title": "no url" }],
            "mentions": [{ "name": "no author", "start": 0, "length": 1 }],
            "textStyles": [{ "style": "MAGIC", "start": 0, "length": 1 }],
            "viewOnce": false,
        }));
        assert!(none.rich.is_none());
    }

    /// The §4.40 authorAci promotion (contract 1.40): the ACI appears only
    /// when the upstream honestly supplies one — the signal-cli `uuid` field,
    /// or a UUID-shaped number key (the engine-mode receive face puts the
    /// ACI there). The author value itself never moves off the pre-1.40
    /// number-first ladder, and a phone number never promotes.
    #[test]
    fn mention_author_aci_promotes_only_from_honest_sources() {
        let normalize = |data: serde_json::Value| {
            normalize_receive_fields(&json!({
                "envelope": {
                    "timestamp": 1727000000000u64,
                    "source": "+15555550101",
                    "dataMessage": data,
                },
            }))
            .unwrap()
        };
        // signal-cli shape: number + uuid — author keeps the number, the ACI
        // rides authorAci.
        let rich = normalize(json!({
            "message": "hi",
            "mentions": [{
                "number": "+15555550101",
                "uuid": "0b7fca57-1234-4d0e-9b0f-4f6c1f8a2e10",
                "name": "Peer",
                "start": 0,
                "length": 2,
            }],
        }))
        .rich
        .expect("rich payload");
        assert_eq!(rich.mentions[0].author, "+15555550101");
        assert_eq!(
            rich.mentions[0].author_aci.as_deref(),
            Some("0b7fca57-1234-4d0e-9b0f-4f6c1f8a2e10")
        );

        // signal-cli shape with an unknown number: only the uuid — the author
        // is the uuid and the ACI is the same value.
        let rich = normalize(json!({
            "message": "hi",
            "mentions": [{
                "uuid": "0B7FCA57-1234-4D0E-9B0F-4F6C1F8A2E10",
                "start": 0,
                "length": 2,
            }],
        }))
        .rich
        .expect("rich payload");
        assert_eq!(
            rich.mentions[0].author,
            "0B7FCA57-1234-4D0E-9B0F-4F6C1F8A2E10"
        );
        assert_eq!(
            rich.mentions[0].author_aci.as_deref(),
            Some("0B7FCA57-1234-4D0E-9B0F-4F6C1F8A2E10"),
            "the grammar is case-insensitive"
        );

        // Engine-mode receive face: the ACI under the number key — promoted
        // by shape, author unchanged.
        let rich = normalize(json!({
            "message": "hi",
            "mentions": [{
                "number": "d43e8ea0-3f6c-4a57-8a5c-51f83f8e5df1",
                "start": 0,
                "length": 2,
            }],
        }))
        .rich
        .expect("rich payload");
        assert_eq!(
            rich.mentions[0].author,
            "d43e8ea0-3f6c-4a57-8a5c-51f83f8e5df1"
        );
        assert_eq!(
            rich.mentions[0].author_aci.as_deref(),
            Some("d43e8ea0-3f6c-4a57-8a5c-51f83f8e5df1")
        );

        // Phone numbers, truncated and decorated values never promote.
        let rich = normalize(json!({
            "message": "hi",
            "mentions": [
                { "number": "+15555550101", "start": 0, "length": 2 },
                { "number": "d43e8ea0-3f6c-4a57-8a5c-51f83f8e5df", "start": 0, "length": 2 },
                { "number": "not-a-uuid-at-all-00000000000000", "start": 0, "length": 2 },
            ],
        }))
        .rich
        .expect("rich payload");
        assert!(
            rich.mentions
                .iter()
                .all(|mention| mention.author_aci.is_none()),
            "no entry in this envelope carries an honest ACI"
        );
    }

    /// The multi-device sentMessage mirror carries the same rich payload as a
    /// direct dataMessage (contract 1.25), so our own phone-originated rich
    /// sends render identically after a restart.
    #[test]
    fn sync_sent_message_mirror_carries_rich_fields() {
        let normalized = normalize_receive_fields(&json!({
            "account": "+15555550100",
            "envelope": {
                "timestamp": 1727000000000u64,
                "syncMessage": {
                    "sentMessage": {
                        "timestamp": 1727000000000u64,
                        "destinationNumber": "+15555550101",
                        "message": "from my phone",
                        "previews": [{ "url": "https://example.com" }],
                    },
                },
            },
        }))
        .unwrap();
        assert_eq!(normalized.direction, "outgoing");
        let rich = normalized.rich.expect("rich payload");
        assert_eq!(rich.previews[0].url, "https://example.com");
    }

    /// Contract 1.38 (§4.37): a peer's view-once open notification mirrors to
    /// our own devices as `syncMessage.viewOnceOpen` — projected as an
    /// incoming control addressed to no conversation (the service layer
    /// resolves the target row by (author ACI, timestamp)).
    #[test]
    fn sync_view_once_open_projects_as_incoming_control() {
        let normalized = normalize_receive(&json!({
            "account": "+15555550100",
            "envelope": {
                "timestamp": 1727000000000u64,
                "syncMessage": {
                    "viewOnceOpen": {
                        "senderAci": "11111111-1111-1111-1111-111111111111",
                        "timestamp": 1726999999000u64,
                    },
                },
            },
        }))
        .unwrap()
        .unwrap();
        assert_eq!(normalized.content_kind, "control");
        assert_eq!(normalized.direction, "incoming");
        assert!(normalized.source.is_none());
        let Some(ControlReceive::ViewOnceOpen {
            sender_aci,
            timestamp,
        }) = normalized.control
        else {
            panic!("expected a view-once open control");
        };
        assert_eq!(sender_aci, "11111111-1111-1111-1111-111111111111");
        assert_eq!(timestamp, 1726999999000);
    }

    /// Contract 1.38: an open sync without a resolvable author has no target
    /// the service layer could name — dropped as skip before persistence.
    #[test]
    fn sync_view_once_open_without_sender_aci_is_skip() {
        let normalized = normalize_receive(&json!({
            "envelope": {
                "timestamp": 1727000000000u64,
                "syncMessage": { "viewOnceOpen": { "timestamp": 1726999999000u64 } },
            },
        }))
        .unwrap()
        .unwrap();
        assert_eq!(normalized.content_kind, "syncMessage");
        assert_eq!(normalized.direction, "skip");
        assert!(normalized.control.is_none());
    }

    /// Contract 1.38: the open sync's timestamp is the official unknown
    /// marker (0) when the engine omits it — the service layer drops those.
    #[test]
    fn sync_view_once_open_defaults_missing_timestamp_to_zero() {
        let normalized = normalize_receive(&json!({
            "envelope": {
                "timestamp": 1727000000000u64,
                "syncMessage": {
                    "viewOnceOpen": { "senderAci": "11111111-1111-1111-1111-111111111111" },
                },
            },
        }))
        .unwrap()
        .unwrap();
        let Some(ControlReceive::ViewOnceOpen { timestamp, .. }) = normalized.control else {
            panic!("expected a view-once open control");
        };
        assert_eq!(timestamp, 0);
    }

    /// Call signaling (contract 1.50, §4.48): the engine's `callMessage`
    /// envelope face routes as a body-less control receive — the modern
    /// opaque payload first, the legacy frames keyed by presence, ICE by
    /// its list head. An out-of-budget or blank face routes nothing and the
    /// envelope falls through to the plain skip.
    #[test]
    fn call_message_envelopes_route_to_call_controls() {
        let routing = |envelope: serde_json::Value| {
            control_routing(envelope.as_object().unwrap()).map(|routing| routing.control)
        };
        let envelope = |call: serde_json::Value| {
            json!({
                "timestamp": 1727000000000u64,
                "source": "+15555550101",
                "sourceDevice": 1,
                "callMessage": call,
            })
        };

        // The modern opaque face: id-less by design — the RingRTC payload
        // carries its own call identity; urgency defaults to the official
        // droppable when the engine leaves it out.
        let Some(ControlReceive::Call {
            call_id,
            signal_type,
            payload,
        }) = routing(envelope(json!({ "opaque": "c2RwLW9mZmVy" })))
        else {
            panic!("expected a call control");
        };
        assert_eq!(call_id, 0);
        assert_eq!(signal_type, "opaque");
        assert_eq!(payload["opaque"], "c2RwLW9mZmVy");
        assert_eq!(payload["urgency"], "droppable");
        assert_eq!(payload["destinationDeviceId"], Value::Null);

        // urgency + destinationDeviceId pass through verbatim when present.
        let Some(ControlReceive::Call { payload, .. }) = routing(envelope(json!({
            "opaque": "c2RwLWFuc3dlcg==",
            "urgency": "handleImmediately",
            "destinationDeviceId": 2u64,
        }))) else {
            panic!("expected a call control");
        };
        assert_eq!(payload["urgency"], "handleImmediately");
        assert_eq!(payload["destinationDeviceId"], 2u64);

        // The legacy frames: the id is the frame's call identity, the
        // projected face rides verbatim under its own key.
        for (key, signal_type, id) in [
            ("offer", "offer", 7u64),
            ("answer", "answer", 7u64),
            ("busy", "busy", 7u64),
            ("hangup", "hangup", 7u64),
        ] {
            let Some(ControlReceive::Call {
                call_id,
                signal_type: kind,
                payload,
            }) = routing(envelope(json!({ key: { "id": id, "opaque": "eA==" } })))
            else {
                panic!("expected a call control for {key}");
            };
            assert_eq!(call_id, id);
            assert_eq!(kind, signal_type);
            assert_eq!(payload[key]["id"], id);
            assert_eq!(payload[key]["opaque"], "eA==");
        }

        // The legacy ICE list: the first frame names the call, the list
        // rides bounded.
        let Some(ControlReceive::Call {
            call_id,
            signal_type,
            payload,
        }) = routing(envelope(json!({
            "iceUpdate": [
                { "id": 7u64, "opaque": "aWNlLTE=" },
                { "id": 7u64, "opaque": "aWNlLTI=" },
            ],
        })))
        else {
            panic!("expected a call control");
        };
        assert_eq!(call_id, 7);
        assert_eq!(signal_type, "ice");
        assert_eq!(payload["iceUpdate"].as_array().unwrap().len(), 2);

        // Blank / unaddressable faces route nothing: no opaque, frames
        // without a parsable id, an id beyond the u32 call-id face, and an
        // over-budget opaque payload.
        assert!(routing(envelope(json!({}))).is_none());
        assert!(routing(envelope(json!({ "offer": { "opaque": "eA==" } }))).is_none());
        assert!(routing(envelope(json!({ "offer": { "id": "x" } }))).is_none());
        assert!(
            routing(envelope(json!({ "offer": { "id": 4294967296u64 } }))).is_none(),
            "the call-id face is u32 — beyond it the frame is unaddressable"
        );
        let oversized = "A".repeat(3 * 1024 * 1024 + 1);
        assert!(routing(envelope(json!({ "opaque": oversized }))).is_none());
    }

    /// Story receives (contract 1.76, §4.67): the envelope-level
    /// `storyMessage` face routes as a body-less control receive — the
    /// bounded projection forwards verbatim (identities ride along), an
    /// oversized face drops the whole envelope into the skip routing (the
    /// coarse one-frame discipline the call-signaling opaque face rides),
    /// and the `isStory` bit alone never routes (it rides story sends and
    /// story replies alike; the plain dataMessage ladder keeps them).
    #[test]
    fn story_envelopes_route_verbatim_and_oversized_skip() {
        let normalize = |envelope: serde_json::Value| {
            normalize_receive(&json!({ "envelope": envelope }))
                .unwrap()
                .unwrap()
        };
        let base = json!({
            "timestamp": 1727000000000u64,
            "source": "+15555550101",
            "sourceUuid": "11111111-1111-1111-1111-111111111111",
            "sourceName": "Peer",
            "isStory": true,
        });

        // The text-attachment projection: every key rides verbatim.
        let normalized = normalize(json!({
            "timestamp": 1727000000000u64,
            "source": "+15555550101",
            "sourceUuid": "11111111-1111-1111-1111-111111111111",
            "sourceName": "Peer",
            "storyMessage": {
                "textAttachment": {
                    "text": "story body",
                    "textStyle": "BOLD",
                },
                "allowsReplies": true,
                "profileKey": "cHJvZmlsZQ==",
            },
        }));
        assert_eq!(normalized.content_kind, "storyMessage");
        assert_eq!(normalized.direction, "incoming");
        assert_eq!(normalized.source.as_deref(), Some("+15555550101"));
        assert_eq!(normalized.timestamp, Some(1727000000000));
        let Some(ControlReceive::Story { message }) = normalized.control else {
            panic!("expected a story control");
        };
        assert_eq!(message["textAttachment"]["text"], "story body");
        assert_eq!(message["textAttachment"]["textStyle"], "BOLD");
        assert_eq!(message["allowsReplies"], true);
        assert_eq!(message["profileKey"], "cHJvZmlsZQ==");
        let _ = &base;

        // The file-attachment shape routes the same way (no text key).
        let Some(ControlReceive::Story { message }) = normalize(json!({
            "timestamp": 1727000000000u64,
            "source": "+15555550101",
            "storyMessage": { "fileAttachment": {} },
        }))
        .control
        else {
            panic!("expected a story control");
        };
        assert!(message.get("fileAttachment").is_some());

        // Oversized material drops the whole envelope: the skip routing,
        // no control, no row material.
        let mut giant_attachment = String::from("data:image/png;base64,");
        giant_attachment.push_str(&"A".repeat(3 * 1024 * 1024));
        let oversized = normalize(json!({
            "timestamp": 1727000000000u64,
            "source": "+15555550101",
            "storyMessage": {
                "fileAttachment": { "data": giant_attachment },
            },
        }));
        assert_eq!(oversized.content_kind, "storyMessage");
        assert_eq!(oversized.direction, "skip");
        assert!(oversized.control.is_none());

        // The bit alone never routes: a plain dataMessage under `isStory`
        // keeps the normal message ladder (no story control).
        let plain = normalize(json!({
            "timestamp": 1727000000000u64,
            "source": "+15555550101",
            "isStory": true,
            "dataMessage": { "message": "hello" },
        }));
        assert_eq!(plain.direction, "incoming");
        assert!(plain.control.is_none());
        assert!(plain.rich.is_none() || !plain.rich.as_ref().unwrap().is_group_update);
    }

    /// Story replies (contract 1.76, §4.67): `dataMessage.storyReply`
    /// parses onto the rich payload as {authorAci, sentTimestamp}; a
    /// degraded face (missing timestamp, blank author) drops the key and
    /// never the row — the reply stays a normal message without story
    /// context.
    #[test]
    fn story_reply_parses_onto_rich_and_malformed_drops_key() {
        let normalize = |data: serde_json::Value| {
            normalize_receive(&json!({
                "envelope": {
                    "timestamp": 1727000000000u64,
                    "source": "+15555550101",
                    "dataMessage": data,
                },
            }))
            .unwrap()
            .unwrap()
        };

        // The well-formed face: both keys ride verbatim.
        let rich = normalize(json!({
            "message": "nice story",
            "storyReply": {
                "authorAci": "22222222-2222-2222-2222-222222222222",
                "sentTimestamp": 1726999999000u64,
            },
        }))
        .rich
        .expect("rich payload");
        assert_eq!(
            rich.story_reply,
            Some(crate::engine::NormalizedStoryReply {
                author_aci: "22222222-2222-2222-2222-222222222222".to_string(),
                sent_timestamp: 1726999999000,
            })
        );

        // Missing timestamp: the key drops, the row stays.
        let degraded = normalize(json!({
            "message": "nice story",
            "storyReply": { "authorAci": "22222222-2222-2222-2222-222222222222" },
        }));
        assert_eq!(degraded.direction, "incoming");
        assert!(
            degraded
                .rich
                .as_ref()
                .and_then(|rich| rich.story_reply.as_ref())
                .is_none(),
            "a degraded storyReply drops the key, never the row"
        );

        // Blank author: same drop.
        let blank = normalize(json!({
            "message": "nice story",
            "storyReply": { "authorAci": "  ", "sentTimestamp": 1726999999000u64 },
        }));
        assert!(
            blank
                .rich
                .as_ref()
                .and_then(|rich| rich.story_reply.as_ref())
                .is_none()
        );
    }

    /// Contract 1.38 (§4.36): a metadata-only view-once marker
    /// (`viewOnceInvalid`) parses onto the rich payload and marks the row
    /// burnable even without renderable media. A dataMessage whose every
    /// other payload is default keeps the §4.20 skip routing — the marker
    /// alone does not manufacture a row.
    #[test]
    fn data_message_view_once_invalid_parses() {
        let normalize = |data: serde_json::Value| {
            normalize_receive(&json!({
                "envelope": {
                    "timestamp": 1727000000000u64,
                    "source": "+15555550101",
                    "dataMessage": data,
                },
            }))
            .unwrap()
            .unwrap()
        };
        let rich = normalize(json!({ "message": "gone", "viewOnceInvalid": true }))
            .rich
            .expect("rich payload");
        assert!(!rich.view_once);
        assert!(rich.view_once_invalid);
        assert!(rich.view_once_opened_at.is_none());

        let bare = normalize(json!({ "viewOnceInvalid": true }));
        assert_eq!(bare.direction, "skip");
        assert!(bare.rich.is_none());
    }

    /// Contract 1.38 (§4.37): every receive envelope's `sourceUuid` is
    /// captured — the raw author identity the service layer records into
    /// `peer_identities` so later open syncs can resolve their target.
    #[test]
    fn receive_captures_source_uuid() {
        let normalized = normalize_receive(&json!({
            "envelope": {
                "timestamp": 1727000000000u64,
                "source": "+15555550101",
                "sourceUuid": "11111111-1111-1111-1111-111111111111",
                "dataMessage": { "message": "hi" },
            },
        }))
        .unwrap()
        .unwrap();
        assert_eq!(
            normalized.source_uuid.as_deref(),
            Some("11111111-1111-1111-1111-111111111111")
        );
        // A normalized receive projected before contract 1.38 (or from an
        // envelope without one) leaves the field empty.
        let bare = normalize_receive(&json!({
            "envelope": {
                "timestamp": 1727000000000u64,
                "source": "+15555550101",
                "dataMessage": { "message": "hi" },
            },
        }))
        .unwrap()
        .unwrap();
        assert!(bare.source_uuid.is_none());
    }

    /// Contract 1.49 (§4.48): every receive envelope's `serverGuid` is
    /// captured — the server-assigned message GUID the reportSpam face
    /// persists row-level and addresses the upstream report API with. It is
    /// connector-internal (never serialized), trimmed, capped at the id
    /// budget, and an envelope without one normalizes to None.
    #[test]
    fn receive_captures_server_guid() {
        let normalized = normalize_receive(&json!({
            "envelope": {
                "timestamp": 1727000000000u64,
                "source": "+15555550101",
                "serverGuid": "  guid-abc  ",
                "dataMessage": { "message": "hi" },
            },
        }))
        .unwrap()
        .unwrap();
        assert_eq!(normalized.server_guid.as_deref(), Some("guid-abc"));
        let bare = normalize_receive(&json!({
            "envelope": {
                "timestamp": 1727000000000u64,
                "source": "+15555550101",
                "dataMessage": { "message": "hi" },
            },
        }))
        .unwrap()
        .unwrap();
        assert!(bare.server_guid.is_none());
        let blank = normalize_receive(&json!({
            "envelope": {
                "timestamp": 1727000000000u64,
                "source": "+15555550101",
                "serverGuid": "   ",
                "dataMessage": { "message": "hi" },
            },
        }))
        .unwrap()
        .unwrap();
        assert!(blank.server_guid.is_none());
    }

    /// Contract 1.42 (§4.41): a dataMessage states its conversation's
    /// disappearing timer with `expiresInSeconds` (absent = no value,
    /// 0 = stated off) and may carry the engine-reported countdown start
    /// `expirationStartTimestamp`; both ride the normalized receive
    /// unchanged, and a stated start stamp of 0 is treated as absent.
    #[test]
    fn data_message_expire_metadata_normalizes() {
        let normalize = |data: serde_json::Value| {
            normalize_receive_fields(&json!({
                "envelope": {
                    "timestamp": 1727000000000u64,
                    "source": "+15555550101",
                    "dataMessage": data,
                },
            }))
            .unwrap()
        };
        let live = normalize(json!({
            "message": "vanishing soon",
            "expiresInSeconds": 86400u64,
            "expirationStartTimestamp": 1726999900000u64,
        }));
        assert_eq!(live.expire_seconds, Some(86400));
        assert_eq!(live.expire_start_at, Some(1726999900000));
        assert!(!live.rich.is_some_and(|rich| rich.is_expiration_update));
        assert_eq!(live.direction, "incoming");

        let stated_off = normalize(json!({
            "message": "plain",
            "expiresInSeconds": 0u64,
        }));
        assert_eq!(stated_off.expire_seconds, Some(0));
        assert_eq!(stated_off.expire_start_at, None);

        let silent = normalize(json!({ "message": "plain" }));
        assert_eq!(silent.expire_seconds, None);
        assert_eq!(silent.expire_start_at, None);

        let zero_start = normalize(json!({
            "message": "plain",
            "expiresInSeconds": 60u64,
            "expirationStartTimestamp": 0u64,
        }));
        assert_eq!(zero_start.expire_seconds, Some(60));
        assert_eq!(zero_start.expire_start_at, None);
    }

    /// Contract 1.42 (§4.41): a peer's timer change lands as a bodyless
    /// dataMessage flagged `isExpirationUpdate` — projected as a system row
    /// whose text material carries the announced value (0 and the absent
    /// close form both read as off), with the rich marker as the row's only
    /// rich content. A body message never carries the marker.
    #[test]
    fn peer_timer_change_projects_system_row() {
        let normalize = |data: serde_json::Value| {
            normalize_receive_fields(&json!({
                "envelope": {
                    "timestamp": 1727000000000u64,
                    "source": "+15555550101",
                    "dataMessage": data,
                },
            }))
            .unwrap()
        };
        let open = normalize(json!({
            "isExpirationUpdate": true,
            "expiresInSeconds": 86400u64,
        }));
        assert_eq!(open.direction, "system");
        assert_eq!(open.text.as_deref(), Some("已更新消息定时消失：86400 秒"));
        assert!(open.rich.is_some_and(|rich| rich.is_expiration_update));
        assert_eq!(open.expire_seconds, Some(86400));

        let close = normalize(json!({
            "isExpirationUpdate": true,
            "expiresInSeconds": 0u64,
        }));
        assert_eq!(close.direction, "system");
        assert_eq!(close.text.as_deref(), Some("已更新消息定时消失：已关闭"));
        assert_eq!(close.expire_seconds, Some(0));

        let body = normalize(json!({
            "message": "hello",
            "isExpirationUpdate": false,
        }));
        assert_eq!(body.direction, "incoming");
        assert!(!body.rich.is_some_and(|rich| rich.is_expiration_update));
    }

    /// Contract 1.42 (§4.41): our own phone-side timer toggle mirrors through
    /// `syncMessage.sentMessage` (`isExpirationUpdate`, no body) as the same
    /// system row a peer's change lands as, addressed by the sent
    /// destination — so every linked device's history agrees.
    #[test]
    fn sync_sent_timer_change_projects_system_row() {
        let normalize = |data: serde_json::Value| {
            normalize_receive_fields(&json!({
                "account": "+15555550100",
                "envelope": {
                    "timestamp": 1727000000000u64,
                    "syncMessage": { "sentMessage": data },
                },
            }))
            .unwrap()
        };
        let open = normalize(json!({
            "timestamp": 1727000000000u64,
            "destinationNumber": "+15555550101",
            "isExpirationUpdate": true,
            "expiresInSeconds": 604800u64,
        }));
        assert_eq!(open.direction, "system");
        assert_eq!(open.content_kind, "syncMessage");
        assert_eq!(open.text.as_deref(), Some("已更新消息定时消失：604800 秒"));
        assert!(open.rich.is_some_and(|rich| rich.is_expiration_update));
        assert_eq!(open.expire_seconds, Some(604800));

        // A real sent body keeps flowing as outgoing with the timer keys.
        let sent = normalize(json!({
            "timestamp": 1727000000000u64,
            "destinationNumber": "+15555550101",
            "message": "from my phone",
            "expiresInSeconds": 3600u64,
        }));
        assert_eq!(sent.direction, "outgoing");
        assert_eq!(sent.expire_seconds, Some(3600));
    }

    /// Contract 1.45 (§4.44): a bodyless group-state change
    /// (`groupInfo.type == UPDATE`) projects as a generic system row — the
    /// pre-1.45 shape fell to skip and was swallowed. The resulting title
    /// rides `rich.groupName` (camelCase on the wire) capped at 64 chars; a
    /// DELIVER envelope (every normal group message) stays marker-free and a
    /// body-carrying UPDATE keeps its ordinary row with the marker beside it.
    #[test]
    fn peer_group_update_projects_system_row() {
        let normalize = |data: serde_json::Value| {
            normalize_receive_fields(&json!({
                "envelope": {
                    "timestamp": 1727000000000u64,
                    "source": "+15555550101",
                    "dataMessage": data,
                },
            }))
            .unwrap()
        };
        let update = normalize(json!({
            "groupInfo": {
                "groupId": "ZmFrZS1ncm91cC0x",
                "type": "UPDATE",
                "groupName": "Fixture Group",
                "revision": 7u64,
            },
        }));
        assert_eq!(update.direction, "system");
        assert_eq!(update.content_kind, "dataMessage");
        assert_eq!(update.group_id.as_deref(), Some("ZmFrZS1ncm91cC0x"));
        assert_eq!(update.text.as_deref(), Some("群聊信息已更新"));
        let rich = update.rich.expect("group-update row carries rich");
        assert!(rich.is_group_update);
        assert_eq!(rich.group_name.as_deref(), Some("Fixture Group"));
        assert!(!rich.is_expiration_update);

        // The title mirrors the engine's post-change value, capped at 64.
        let long = normalize(json!({
            "groupInfo": {
                "groupId": "ZmFrZS1ncm91cC0x",
                "type": "UPDATE",
                "groupName": "長".repeat(80),
            },
        }));
        let rich = long.rich.expect("long title still projects");
        assert_eq!(rich.group_name.as_deref(), Some("長".repeat(64).as_str()));

        // A bodyless DELIVER stays a skip — only UPDATE marks a group edit.
        let deliver = normalize(json!({
            "groupInfo": {
                "groupId": "ZmFrZS1ncm91cC0x",
                "type": "DELIVER",
            },
        }));
        assert_eq!(deliver.direction, "skip");
        assert!(deliver.rich.is_none());

        // A body-carrying UPDATE is an ordinary message row; the marker and
        // title ride beside the body instead of replacing it.
        let body = normalize(json!({
            "message": "renamed us",
            "groupInfo": {
                "groupId": "ZmFrZS1ncm91cC0x",
                "type": "UPDATE",
                "groupName": "New Name",
            },
        }));
        assert_eq!(body.direction, "incoming");
        assert_eq!(body.text.as_deref(), Some("renamed us"));
        let rich = body.rich.expect("body row carries the marker");
        assert!(rich.is_group_update);
        assert_eq!(rich.group_name.as_deref(), Some("New Name"));
    }

    /// Contract 1.45 (§4.44): our own phone-side group edit mirrors through
    /// `syncMessage.sentMessage` as the same generic system row a peer's
    /// change lands as, keyed by the sent group — every linked device's
    /// history agrees.
    #[test]
    fn sync_sent_group_update_projects_system_row() {
        let normalize = |data: serde_json::Value| {
            normalize_receive_fields(&json!({
                "account": "+15555550100",
                "envelope": {
                    "timestamp": 1727000000000u64,
                    "syncMessage": { "sentMessage": data },
                },
            }))
            .unwrap()
        };
        let update = normalize(json!({
            "timestamp": 1727000000000u64,
            "groupInfo": {
                "groupId": "ZmFrZS1ncm91cC0x",
                "type": "UPDATE",
                "groupName": "Fixture Group",
            },
        }));
        assert_eq!(update.direction, "system");
        assert_eq!(update.content_kind, "syncMessage");
        assert_eq!(update.group_id.as_deref(), Some("ZmFrZS1ncm91cC0x"));
        assert_eq!(update.text.as_deref(), Some("群聊信息已更新"));
        let rich = update.rich.expect("group-update mirror carries rich");
        assert!(rich.is_group_update);
        assert_eq!(rich.group_name.as_deref(), Some("Fixture Group"));
    }

    /// Contract 1.45 (§4.44): a timer change outranks a group update — the
    /// §4.41 timer notice keeps its row when the envelope happens to carry
    /// both markers.
    #[test]
    fn timer_change_outranks_group_update() {
        let normalize = normalize_receive_fields(&json!({
            "envelope": {
                "timestamp": 1727000000000u64,
                "source": "+15555550101",
                "dataMessage": {
                    "isExpirationUpdate": true,
                    "expiresInSeconds": 3600u64,
                    "groupInfo": {
                        "groupId": "ZmFrZS1ncm91cC0x",
                        "type": "UPDATE",
                        "groupName": "Fixture Group",
                    },
                },
            },
        }))
        .unwrap();
        assert_eq!(normalize.direction, "system");
        assert_eq!(
            normalize.text.as_deref(),
            Some("已更新消息定时消失：3600 秒")
        );
        let rich = normalize.rich.expect("timer row carries rich");
        assert!(rich.is_expiration_update);
        assert!(!rich.is_group_update);
    }

    #[test]
    fn stdout_pump_reader_frames_lines_strips_cr_and_delivers_eof_partial() {
        let (pipe_reader, writer) = std::io::pipe().unwrap();
        let mut reader = std::io::BufReader::new(pipe_reader);
        {
            let mut writer = writer;
            writer.write_all(b"alpha\nbeta\r\ngamma-partial").unwrap();
        }
        assert_eq!(
            read_bounded_line(&mut reader, 1024).unwrap(),
            Some(PumpedLine::Line("alpha".into()))
        );
        // A `\r` before the newline is transport padding, not content.
        assert_eq!(
            read_bounded_line(&mut reader, 1024).unwrap(),
            Some(PumpedLine::Line("beta".into()))
        );
        // A partial final line at EOF is delivered, then the stream ends.
        assert_eq!(
            read_bounded_line(&mut reader, 1024).unwrap(),
            Some(PumpedLine::Line("gamma-partial".into()))
        );
        assert_eq!(read_bounded_line(&mut reader, 1024).unwrap(), None);
    }

    #[test]
    fn stdout_pump_reader_allows_line_up_to_the_cap_and_rejects_longer() {
        let (pipe_reader, writer) = std::io::pipe().unwrap();
        let mut reader = std::io::BufReader::new(pipe_reader);
        {
            let mut writer = writer;
            writer.write_all(b"12345678\n12345678901234\n").unwrap();
        }
        assert_eq!(
            read_bounded_line(&mut reader, 8).unwrap(),
            Some(PumpedLine::Line("12345678".into()))
        );
        assert!(matches!(
            read_bounded_line(&mut reader, 8),
            Err(BoundedLineError::TooLong)
        ));
    }

    #[test]
    fn stdout_pump_reader_rejects_invalid_utf8_as_framing_error() {
        let (pipe_reader, writer) = std::io::pipe().unwrap();
        let mut reader = std::io::BufReader::new(pipe_reader);
        {
            let mut writer = writer;
            writer.write_all(&[0xff, 0xfe, b'\n']).unwrap();
        }
        assert!(matches!(
            read_bounded_line(&mut reader, 1024),
            Err(BoundedLineError::InvalidUtf8)
        ));
    }

    /// The `syncMessage.messageRequestResponse` face (contract 1.70, §4.58
    /// consume batch): a well-formed direct-thread ACCEPT mirror routes as a
    /// control receive carrying the thread ACI, and every hostile or foreign
    /// shape falls through to the plain syncMessage skip — the fail-closed
    /// discipline. The enum family (DELETE/BLOCK/…), the group thread leg,
    /// and the shape gates all ride the same normalize face.
    #[test]
    fn message_request_response_normalize_routes_and_gates_the_face() {
        const THREAD_ACI: &str = "0b7fca57-1234-4d0e-9b0f-4f6c1f8a2e10";
        // 32 raw bytes = the group master key; standard base64 (44 chars).
        const GROUP_B64: &str = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=";

        let normalize = |sync: Value| {
            normalize_receive_fields(&json!({
                "envelope": {
                    "timestamp": 1727000000000u64,
                    "syncMessage": sync,
                },
            }))
            .unwrap()
        };

        // ACCEPT on a direct thread: the authoritative stamp material.
        let direct = normalize(json!({
            "messageRequestResponse": {
                "threadAci": THREAD_ACI,
                "messageRequestResponseType": 1,
            }
        }));
        assert_eq!(direct.content_kind, "control");
        assert_eq!(direct.direction, "incoming");
        assert!(direct.source.is_none());
        match direct.control.expect("direct face") {
            ControlReceive::MessageRequestResponse {
                response_type,
                thread_aci,
                group_id,
            } => {
                assert_eq!(response_type, 1);
                assert_eq!(thread_aci.as_deref(), Some(THREAD_ACI));
                assert!(group_id.is_none());
            }
            other => panic!("unexpected control: {other:?}"),
        }

        // DELETE and BLOCK ride the same face with the enum's numeric value.
        for response_type in [2u8, 3] {
            let face = normalize(json!({
                "messageRequestResponse": {
                    "threadAci": THREAD_ACI,
                    "messageRequestResponseType": response_type,
                }
            }));
            assert_eq!(face.content_kind, "control");
            assert!(
                matches!(
                    face.control,
                    Some(ControlReceive::MessageRequestResponse {
                        response_type: got,
                        ..
                    }) if got == response_type
                ),
                "member {response_type} must keep its numeric value"
            );
        }

        // The group thread leg: the 32-byte standard-base64 group id, no ACI.
        let group = normalize(json!({
            "messageRequestResponse": {
                "groupId": GROUP_B64,
                "messageRequestResponseType": 1,
            }
        }));
        match group.control.expect("group face") {
            ControlReceive::MessageRequestResponse {
                thread_aci,
                group_id,
                ..
            } => {
                assert!(thread_aci.is_none());
                assert_eq!(group_id.as_deref(), Some(GROUP_B64));
            }
            other => panic!("unexpected control: {other:?}"),
        }

        // Upper-case hex UUID is the same ServiceId shape.
        let upper = normalize(json!({
            "messageRequestResponse": {
                "threadAci": THREAD_ACI.to_uppercase(),
                "messageRequestResponseType": 1,
            }
        }));
        assert!(matches!(
            upper.control,
            Some(ControlReceive::MessageRequestResponse { .. })
        ));

        // The gates, every one falling through to the plain skip arm:
        // UNKNOWN (0) and past-enum values, a bad UUID, a wrong-byte-length
        // group id, non-base64 group material, neither thread leg, and both
        // legs at once.
        for (why, sync) in [
            (
                "UNKNOWN (0) carries no actionable state",
                json!({
                    "messageRequestResponse": {
                        "threadAci": THREAD_ACI,
                        "messageRequestResponseType": 0,
                    }
                }),
            ),
            (
                "past-enum value",
                json!({
                    "messageRequestResponse": {
                        "threadAci": THREAD_ACI,
                        "messageRequestResponseType": 7,
                    }
                }),
            ),
            (
                "bad UUID shape",
                json!({
                    "messageRequestResponse": {
                        "threadAci": "not-a-service-id",
                        "messageRequestResponseType": 1,
                    }
                }),
            ),
            (
                "wrong decoded group length",
                json!({
                    "messageRequestResponse": {
                        "groupId": "ZmFrZS1ncm91cC0x",
                        "messageRequestResponseType": 1,
                    }
                }),
            ),
            (
                "non-base64 group material",
                json!({
                    "messageRequestResponse": {
                        "groupId": "not base64!!",
                        "messageRequestResponseType": 1,
                    }
                }),
            ),
            (
                "neither thread leg",
                json!({
                    "messageRequestResponse": {
                        "messageRequestResponseType": 1,
                    }
                }),
            ),
            (
                "both thread legs",
                json!({
                    "messageRequestResponse": {
                        "threadAci": THREAD_ACI,
                        "groupId": GROUP_B64,
                        "messageRequestResponseType": 1,
                    }
                }),
            ),
        ] {
            let face = normalize(sync);
            assert_eq!(face.content_kind, "syncMessage", "{why}");
            assert_eq!(face.direction, "skip", "{why}");
            assert!(face.control.is_none(), "{why}");
        }
    }
}
