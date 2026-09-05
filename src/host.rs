// SPDX-License-Identifier: AGPL-3.0-only

use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex, Weak};
use std::time::{Duration, Instant};

use futures_util::future::BoxFuture;
use futures_util::stream::{FuturesUnordered, SplitSink};
use futures_util::{FutureExt, SinkExt, StreamExt};
use rand::RngCore;
use serde::Serialize;
use serde_json::{Value, json};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{Mutex, OwnedMutexGuard, OwnedSemaphorePermit, Semaphore, broadcast, watch};
use tokio::time::{MissedTickBehavior, interval, timeout};
use tokio_util::codec::{Framed, LinesCodec, LinesCodecError};

use crate::auth::{BootstrapSecret, HandshakeParams, PendingChallenge};
use crate::ipc::LocalListener;
use crate::metrics;
use crate::protocol::{ApiError, HostEvent, HostRequest, HostResponse};
use crate::registry::{ProxyGroupRuntime, RegistryEvent, StartFailure, StopFailure};
use crate::service::{
    AccountDeleteLocalDataParams, ContactsListParams, ContactsSetLocalAliasParams,
    ContactsSyncParams, ConversationsListParams, GroupsGetParams, HostSideEvent, LinkSessionParams,
    LinkStartParams, MessageGetTextParams, MessagesGetAttachmentParams, MessagesListParams,
    MessagesRemoteDeleteParams, MessagesSendReactionParams, MessagesSendTextParams,
    PresenceSetTypingMessageParams, SendTarget,
};
use crate::store::MAX_PAGE_LIMIT;
use crate::{API_VERSION, DEFAULT_HOST_FRAME_LIMIT, PHASE2_CAPABILITIES};

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
const HOST_DISPATCH_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
/// How often the redacted in-process metrics (src/metrics.rs) are logged.
const METRICS_LOG_INTERVAL: Duration = Duration::from_secs(60);
/// Outer bound for runtime shutdown during host teardown. The engine shutdown
/// is already bounded internally (grace period, then a direct kill); this
/// covers everything around it (e.g. a store lock held by a wedged
/// persistence write) so a single stuck component cannot keep the process
/// alive. Must exceed the engine-internal shutdown budget.
const HOST_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(15);
const RECENT_REQUEST_IDS: usize = 128;
const MAX_PENDING_HOST_REQUESTS: usize = 128;
const MAX_PENDING_HOST_REQUESTS_PER_ACCOUNT: usize = 32;
const MAX_PENDING_HOST_BYTES: usize = 8 * 1024 * 1024;
const CONTROL_CONCURRENCY: usize = 1;
/// Per-group `link.finish` capacity (ADR 0001 R3): phone-approval waits for
/// different groups never block each other, while a second concurrent finish
/// inside one group is rejected instead of queueing.
const LINK_WAIT_CONCURRENCY: usize = 1;
const READ_CONCURRENCY: usize = 4;
const SEND_CONCURRENCY: usize = 2;

type HostWriter<S> = Arc<Mutex<SplitSink<Framed<S, LinesCodec>, String>>>;
type DispatchFuture = BoxFuture<'static, DispatchCompletion>;

struct DispatchCompletion {
    account_id: Option<String>,
    request_bytes: usize,
    write_result: Result<(), HostError>,
}

struct HostDispatchPermit {
    _lane: OwnedSemaphorePermit,
    _account: Option<OwnedMutexGuard<()>>,
    _deleting: Option<DeletingAccountGuard>,
}

/// Marks an account as having a delete in progress for the whole dispatch
/// (refcounted, so overlapping deletes of the same account stay marked until
/// the last one finishes). Never held across an await in the guard itself.
struct DeletingAccountGuard {
    accounts: Arc<StdMutex<HashMap<String, usize>>>,
    account_key: String,
}

impl Drop for DeletingAccountGuard {
    fn drop(&mut self) {
        let Ok(mut accounts) = self.accounts.lock() else {
            return;
        };
        if let Some(count) = accounts.get_mut(&self.account_key) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                accounts.remove(&self.account_key);
            }
        }
    }
}

struct HostDispatchLimits {
    control: Arc<Semaphore>,
    read: Arc<Semaphore>,
    send: Arc<Semaphore>,
    send_accounts: Mutex<HashMap<String, Weak<Mutex<()>>>>,
    /// One bounded lane per proxy group for `link.finish` (ADR 0001 R3).
    /// Same weak-handle discipline as `send_accounts`.
    link_lanes: Mutex<HashMap<String, Weak<Semaphore>>>,
    /// Accounts with a delete in progress (refcounted). New mutating work for
    /// these accounts is rejected instead of queueing behind the delete.
    /// std mutex: only ever locked for a lookup/insert, never across an await.
    deleting_accounts: Arc<StdMutex<HashMap<String, usize>>>,
}

#[derive(Default)]
struct HostPendingBudget {
    total: usize,
    bytes: usize,
    by_account: HashMap<String, usize>,
}

impl HostPendingBudget {
    fn try_admit(&mut self, account_id: Option<&str>, request_bytes: usize) -> bool {
        let account_pending = account_id
            .and_then(|account| self.by_account.get(account))
            .copied()
            .unwrap_or(0);
        if self.total >= MAX_PENDING_HOST_REQUESTS
            || account_pending >= MAX_PENDING_HOST_REQUESTS_PER_ACCOUNT
            || self.bytes.saturating_add(request_bytes) > MAX_PENDING_HOST_BYTES
        {
            return false;
        }
        self.total += 1;
        self.bytes += request_bytes;
        if let Some(account) = account_id {
            *self.by_account.entry(account.to_string()).or_default() += 1;
        }
        metrics::set_host_pending(self.total);
        true
    }

    fn complete(&mut self, account_id: Option<&str>, request_bytes: usize) {
        self.total = self.total.saturating_sub(1);
        self.bytes = self.bytes.saturating_sub(request_bytes);
        metrics::set_host_pending(self.total);
        let Some(account_id) = account_id else {
            return;
        };
        if let Some(count) = self.by_account.get_mut(account_id) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.by_account.remove(account_id);
            }
        }
    }
}

impl HostDispatchLimits {
    fn new() -> Self {
        Self {
            control: Arc::new(Semaphore::new(CONTROL_CONCURRENCY)),
            read: Arc::new(Semaphore::new(READ_CONCURRENCY)),
            send: Arc::new(Semaphore::new(SEND_CONCURRENCY)),
            send_accounts: Mutex::new(HashMap::new()),
            link_lanes: Mutex::new(HashMap::new()),
            deleting_accounts: Arc::new(StdMutex::new(HashMap::new())),
        }
    }

    /// Acquire the dispatch permit for one host request.
    ///
    /// Mutating, account-scoped methods (`messages.sendText`,
    /// `messages.remoteDelete`, `contacts.sync`, `accounts.deleteLocalData`)
    /// share one per-account mutex, which doubles as the delete drain barrier:
    ///
    /// - A delete first marks the account as deleting (new sends/syncs are
    ///   rejected with `ACCOUNT_NOT_FOUND` instead of queueing), then waits on
    ///   the account mutex until every already-admitted send/sync for that
    ///   account has fully finished — including its completion write to the
    ///   store and its response write to the host. Only then does the upstream
    ///   `deleteLocalAccountData` and the local row cascade run. A "sent
    ///   upstream but locally rowless" message is therefore impossible.
    /// - The deleting mark is checked before taking the mutex, so a send that
    ///   raced past the check can still queue behind the delete; it then fails
    ///   deterministically at send preparation because the account row is
    ///   gone. Either interleaving linearizes the send strictly before or
    ///   strictly after the delete.
    async fn acquire(
        &self,
        method: &str,
        account_id: Option<&str>,
        lane_key: Option<&str>,
    ) -> Result<HostDispatchPermit, ApiError> {
        if matches!(
            method,
            "messages.sendText"
                | "messages.remoteDelete"
                | "messages.sendReaction"
                | "contacts.sync"
                | "contacts.setLocalAlias"
                | "presence.setTypingMessage"
                | "accounts.deleteLocalData"
        ) {
            let account_key = account_id.unwrap_or("").to_string();
            let deleting = if method == "accounts.deleteLocalData" {
                let mut accounts = self.deleting_accounts.lock().unwrap();
                *accounts.entry(account_key.clone()).or_insert(0) += 1;
                Some(DeletingAccountGuard {
                    accounts: Arc::clone(&self.deleting_accounts),
                    account_key: account_key.clone(),
                })
            } else {
                if self
                    .deleting_accounts
                    .lock()
                    .unwrap()
                    .contains_key(&account_key)
                {
                    return Err(ApiError::new(
                        "ACCOUNT_NOT_FOUND",
                        "account is being deleted",
                        false,
                    ));
                }
                None
            };
            let account_lock = {
                let mut accounts = self.send_accounts.lock().await;
                accounts.retain(|_, lock| lock.strong_count() > 0);
                if let Some(lock) = accounts.get(&account_key).and_then(Weak::upgrade) {
                    lock
                } else {
                    let lock = Arc::new(Mutex::new(()));
                    accounts.insert(account_key, Arc::downgrade(&lock));
                    lock
                }
            };
            // Account order is acquired before global lane capacity so one busy
            // account cannot occupy every lane permit while waiting on itself.
            let account = account_lock.lock_owned().await;
            let lane = match method {
                "messages.sendText"
                | "messages.remoteDelete"
                | "messages.sendReaction"
                | "contacts.setLocalAlias"
                | "presence.setTypingMessage" => self.send.clone(),
                "contacts.sync" => self.read.clone(),
                _ => self.control.clone(),
            }
            .acquire_owned()
            .await
            .unwrap();
            return Ok(HostDispatchPermit {
                _lane: lane,
                _account: Some(account),
                _deleting: deleting,
            });
        }

        if method == "link.finish" {
            // Dispatch resolves the owning group first; a finish without a
            // resolvable session is answered before any lane is taken, so a
            // missing key here means the caller skipped that step.
            let Some(group_key) = lane_key else {
                return Err(ApiError::new(
                    "LINK_NOT_FOUND",
                    "link session was not found",
                    false,
                ));
            };
            let lane = {
                let mut lanes = self.link_lanes.lock().await;
                lanes.retain(|_, lane| lane.strong_count() > 0);
                if let Some(lane) = lanes.get(group_key).and_then(Weak::upgrade) {
                    lane.clone()
                } else {
                    let lane = Arc::new(Semaphore::new(LINK_WAIT_CONCURRENCY));
                    lanes.insert(group_key.to_string(), Arc::downgrade(&lane));
                    lane
                }
            };
            // Contention inside one group rejects immediately (the wire
            // contract answers the second concurrent finish with
            // LINK_IN_PROGRESS); another group's lane is untouched.
            let permit = lane.try_acquire_owned().map_err(|_| {
                ApiError::new(
                    "LINK_IN_PROGRESS",
                    "a link finish request is already active for this proxy group",
                    false,
                )
            })?;
            return Ok(HostDispatchPermit {
                _lane: permit,
                _account: None,
                _deleting: None,
            });
        }

        let lane = if matches!(
            method,
            "conversations.list"
                | "messages.list"
                | "messages.getText"
                | "messages.attachments.get"
                | "contacts.list"
                | "groups.get"
        ) {
            self.read.clone().acquire_owned().await.unwrap()
        } else {
            self.control.clone().acquire_owned().await.unwrap()
        };
        Ok(HostDispatchPermit {
            _lane: lane,
            _account: None,
            _deleting: None,
        })
    }
}

#[derive(Debug, Error)]
pub enum HostError {
    #[error("local IPC failed")]
    Io(#[from] io::Error),
    #[error("host frame is invalid")]
    InvalidFrame,
    /// The peer vanished mid-frame. Internal only: it ends the session the same
    /// way a clean EOF does and never reaches the exit status or the host.
    #[error("host connection is gone")]
    PeerGone,
    #[error("host authentication failed")]
    Authentication,
    #[error("signal-cli runtime could not be stopped")]
    RuntimeShutdown,
}

pub async fn serve(
    listener: LocalListener,
    secret: BootstrapSecret,
    runtime: Arc<ProxyGroupRuntime>,
) -> Result<(), HostError> {
    let secret = Arc::new(secret);
    spawn_metrics_log();
    loop {
        let stream = tokio::select! {
            accepted = listener.accept() => accepted?,
            signal = tokio::signal::ctrl_c() => {
                signal?;
                return Ok(());
            }
        };
        let authenticated = Arc::new(AtomicBool::new(false));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let connection = handle_connection_until_shutdown(
            stream,
            secret.clone(),
            runtime.clone(),
            shutdown_rx,
            authenticated.clone(),
        );
        tokio::pin!(connection);
        let mut interrupted = false;
        let connection_result = tokio::select! {
            result = &mut connection => result,
            signal = tokio::signal::ctrl_c() => {
                signal?;
                interrupted = true;
                let _ = shutdown_tx.send(true);
                connection.await
            }
        };
        if interrupted || authenticated.load(Ordering::Acquire) {
            match timeout(HOST_SHUTDOWN_TIMEOUT, runtime.shutdown()).await {
                Ok(result) => result.map_err(|_| HostError::RuntimeShutdown)?,
                // A stuck teardown must not keep the process alive: log and
                // exit with the connection outcome.
                Err(_) => tracing::warn!(
                    budget_ms = HOST_SHUTDOWN_TIMEOUT.as_millis() as u64,
                    "runtime shutdown exceeded the teardown budget"
                ),
            }
            return connection_result;
        }
        // A malformed or unauthenticated local probe must not consume the process. The first
        // successful authentication does: when that session ends, a fresh process and secret
        // are required.
    }
}

#[cfg(test)]
async fn handle_connection<S>(
    stream: S,
    secret: Arc<BootstrapSecret>,
    runtime: Arc<ProxyGroupRuntime>,
) -> Result<(), HostError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    handle_connection_until_shutdown(
        stream,
        secret,
        runtime,
        shutdown_rx,
        Arc::new(AtomicBool::new(false)),
    )
    .await
}

async fn handle_connection_until_shutdown<S>(
    stream: S,
    secret: Arc<BootstrapSecret>,
    runtime: Arc<ProxyGroupRuntime>,
    shutdown: watch::Receiver<bool>,
    authenticated: Arc<AtomicBool>,
) -> Result<(), HostError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    // A host that closed the socket abruptly ended its session; it did not
    // violate the protocol, and this process must still exit cleanly. Normalize
    // here rather than at any single failure point, because every read and write
    // in the session below can be the one that discovers the peer is gone.
    match run_host_session(stream, secret, runtime, shutdown, authenticated).await {
        Err(HostError::PeerGone) => Ok(()),
        other => other,
    }
}

async fn run_host_session<S>(
    stream: S,
    secret: Arc<BootstrapSecret>,
    runtime: Arc<ProxyGroupRuntime>,
    mut shutdown: watch::Receiver<bool>,
    authenticated: Arc<AtomicBool>,
) -> Result<(), HostError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let codec = LinesCodec::new_with_max_length(DEFAULT_HOST_FRAME_LIMIT);
    let mut framed = Framed::new(stream, codec);
    let challenge = PendingChallenge::generate();
    send_json(
        &mut framed,
        &HostEvent::new("runtime.challenge", challenge.public()),
    )
    .await?;

    let handshake_line = tokio::select! {
        line = timeout(HANDSHAKE_TIMEOUT, framed.next()) => {
            line
                .map_err(|_| HostError::Authentication)?
                .ok_or(HostError::Authentication)?
                .map_err(|_| HostError::InvalidFrame)?
        }
        _ = shutdown.changed() => return Ok(()),
    };
    let handshake: HostRequest =
        serde_json::from_str(&handshake_line).map_err(|_| HostError::InvalidFrame)?;
    if let Err(error) = handshake.validate_envelope() {
        send_json(
            &mut framed,
            &HostResponse::failure(handshake.request_id, error),
        )
        .await?;
        return Err(HostError::Authentication);
    }
    if handshake.method != "handshake" {
        send_json(
            &mut framed,
            &HostResponse::failure(
                handshake.request_id,
                ApiError::new("AUTHENTICATION_FAILED", "authentication failed", false),
            ),
        )
        .await?;
        return Err(HostError::Authentication);
    }
    let params: HandshakeParams = match serde_json::from_value(handshake.params) {
        Ok(params) => params,
        Err(_) => {
            send_json(
                &mut framed,
                &HostResponse::failure(
                    handshake.request_id,
                    ApiError::new("AUTHENTICATION_FAILED", "authentication failed", false),
                ),
            )
            .await?;
            return Err(HostError::Authentication);
        }
    };
    if challenge.verify(&secret, &params).is_err() {
        send_json(
            &mut framed,
            &HostResponse::failure(
                handshake.request_id,
                ApiError::new("AUTHENTICATION_FAILED", "authentication failed", false),
            ),
        )
        .await?;
        return Err(HostError::Authentication);
    }
    authenticated.store(true, Ordering::Release);

    let handshake_request_id = handshake.request_id;
    send_json(
        &mut framed,
        &HostResponse::success(
            handshake_request_id.clone(),
            json!({
                "sessionId": random_identifier(),
                "apiVersion": API_VERSION,
                "capabilities": PHASE2_CAPABILITIES,
            }),
        ),
    )
    .await?;

    let (sink, mut stream) = framed.split();
    let writer = Arc::new(Mutex::new(sink));
    let limits = Arc::new(HostDispatchLimits::new());
    let mut dispatches = FuturesUnordered::<DispatchFuture>::new();
    let mut pending = HostPendingBudget::default();
    let mut recent_ids = RecentRequestIds::default();
    recent_ids.insert(handshake_request_id);
    let mut registry_events = runtime.subscribe();
    let mut host_events = runtime.subscribe_host();
    let connection_result = loop {
        tokio::select! {
            line = stream.next() => {
                let Some(line) = line else {
                    break Ok(());
                };
                let line = match line {
                    Ok(line) => line,
                    Err(error) => break Err(codec_failure(&error)),
                };
                let request_bytes = line.len();
                let request: HostRequest = match serde_json::from_str(&line) {
                    Ok(request) => request,
                    Err(_) => break Err(HostError::InvalidFrame),
                };
                if let Err(error) = request.validate_envelope() {
                    let response = HostResponse::failure(request.request_id, error);
                    if let Err(error) = send_shared(&writer, &response).await {
                        break Err(error);
                    }
                    continue;
                }
                if !recent_ids.insert(request.request_id.clone()) {
                    let response = HostResponse::failure(
                        request.request_id,
                        ApiError::new("INVALID_REQUEST", "requestId was already used in this session", false),
                    );
                    if let Err(error) = send_shared(&writer, &response).await {
                        break Err(error);
                    }
                    continue;
                }

                let method = request.method.clone();
                let account_id = request_account_id(&request);
                if !pending.try_admit(account_id.as_deref(), request_bytes) {
                    tracing::warn!(
                        method_class = metrics::method_class(&method),
                        account_id = account_id.as_deref().unwrap_or("-"),
                        request_bytes,
                        "host request rejected: capacity exceeded"
                    );
                    let response = HostResponse::failure(
                        request.request_id,
                        ApiError::new(
                            "INTERNAL_ERROR",
                            "connector request capacity exceeded",
                            true,
                        ),
                    );
                    if let Err(error) = send_shared(&writer, &response).await {
                        break Err(error);
                    }
                    continue;
                }

                let task_account = account_id.clone();
                let task_runtime = runtime.clone();
                let task_writer = writer.clone();
                let task_limits = limits.clone();
                dispatches.push(async move {
                    // A rejected acquire (mutating work on an account whose
                    // delete is draining, a second concurrent finish in one
                    // group) never reaches dispatch; it is answered directly
                    // with the non-retryable error.
                    let started = Instant::now();
                    // `link.finish` routes through the lane of the group that
                    // owns the session (ADR 0001 R3); a session that no longer
                    // exists anywhere fails closed without occupying any lane.
                    // Unparseable finish params fall through to dispatch so its
                    // own parse produces the INVALID_REQUEST answer.
                    let link_params = if method == "link.finish" {
                        serde_json::from_value::<LinkSessionParams>(request.params.clone()).ok()
                    } else {
                        None
                    };
                    // The permit itself must stay bound for the whole
                    // dispatch: dropping it early (e.g. by reducing the
                    // acquire result to its error) would release the account
                    // mutex, the delete barrier and the lane before the work
                    // has run, letting same-account requests interleave.
                    let acquired: Result<HostDispatchPermit, ApiError> = async {
                        match &link_params {
                            Some(params) => {
                                let Some(group) = task_runtime
                                    .resolve_link_session(&params.link_session_id)
                                    .await
                                else {
                                    return Err(ApiError::new(
                                        "LINK_NOT_FOUND",
                                        "link session was not found",
                                        false,
                                    ));
                                };
                                task_limits
                                    .acquire(&method, task_account.as_deref(), Some(&group))
                                    .await
                            }
                            None => {
                                task_limits
                                    .acquire(&method, task_account.as_deref(), None)
                                    .await
                            }
                        }
                    }
                    .await;
                    let response = match acquired {
                        Ok(_permit) => dispatch(request, &task_runtime).await,
                        Err(error) => HostResponse::failure(request.request_id, error),
                    };
                    let elapsed = started.elapsed();
                    let method_class = metrics::method_class(&method);
                    let result_class = metrics::result_class(response.error.as_ref());
                    metrics::record_request(method_class, result_class, elapsed);
                    log_dispatch(
                        method_class,
                        result_class,
                        response.error.as_ref().map(|error| error.code),
                        task_account.as_deref(),
                        request_bytes,
                        elapsed,
                    );
                    let write_result = send_shared(&task_writer, &response).await;
                    DispatchCompletion {
                        account_id: task_account,
                        request_bytes,
                        write_result,
                    }
                }.boxed());
            }
            completion = dispatches.next(), if !dispatches.is_empty() => {
                if let Some(completion) = completion {
                    pending.complete(
                        completion.account_id.as_deref(),
                        completion.request_bytes,
                    );
                    if let Err(error) = completion.write_result {
                        break Err(error);
                    }
                }
            }
            _ = shutdown.changed() => {
                break Ok(());
            }
            event = registry_events.recv() => {
                match event {
                    Ok(event) => {
                        let wire: HostEvent<Value> = match event {
                            // The aggregate keeps the pre-Phase-4 shape exactly.
                            RegistryEvent::AggregateStateChanged(status) => {
                                HostEvent::new(
                                    "runtime.stateChanged",
                                    serde_json::to_value(status).unwrap_or(Value::Null),
                                )
                            }
                            RegistryEvent::GroupStateChanged { group_id, status } => {
                                let mut payload = serde_json::to_value(&status)
                                    .unwrap_or(Value::Null);
                                if let Some(object) = payload.as_object_mut() {
                                    object.insert("groupId".into(), json!(group_id));
                                }
                                HostEvent::new("proxyGroup.stateChanged", payload)
                            }
                            RegistryEvent::ResourcePressure {
                                state,
                                pid,
                                rss_bytes,
                                group_id,
                            } => HostEvent::new(
                                "runtime.resourcePressure",
                                json!({
                                    "state": state,
                                    "pid": pid,
                                    "rssBytes": rss_bytes,
                                    "groupId": group_id
                                }),
                            ),
                            RegistryEvent::ProtocolWarning { kind } => HostEvent::new(
                                "runtime.protocolWarning",
                                json!({ "kind": kind }),
                            ),
                            RegistryEvent::StorageChanged { state } => HostEvent::new(
                                "runtime.storageChanged",
                                json!({ "state": state }),
                            ),
                        };
                        if let Err(error) = send_shared(&writer, &wire).await {
                            break Err(error);
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        if let Err(error) = send_shared(
                            &writer,
                            &HostEvent::new(
                                "runtime.protocolWarning",
                                json!({ "kind": "eventBackpressure" }),
                            ),
                        ).await {
                            break Err(error);
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break Ok(()),
                }
            }
            event = host_events.recv() => {
                match event {
                    Ok(event) => {
                        if let Err(error) = send_host_event(&writer, event).await {
                            break Err(error);
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        if let Err(error) = send_shared(
                            &writer,
                            &HostEvent::new(
                                "runtime.protocolWarning",
                                json!({ "kind": "eventBackpressure" }),
                            ),
                        ).await {
                            break Err(error);
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break Ok(()),
                }
            }
        }
    };

    // Closing the authenticated host connection owns runtime shutdown. This
    // resolves dispatched mutating calls as unknown before task futures drop.
    // Teardown is bounded: each engine shutdown has its own timeout-plus-kill
    // path, and this outer budget covers the rest, so a stuck component
    // cannot keep the process alive after its host is gone.
    if timeout(HOST_SHUTDOWN_TIMEOUT, runtime.shutdown())
        .await
        .is_err()
    {
        tracing::warn!(
            budget_ms = HOST_SHUTDOWN_TIMEOUT.as_millis() as u64,
            "runtime shutdown exceeded the teardown budget"
        );
    }
    let _ = timeout(HOST_DISPATCH_DRAIN_TIMEOUT, async {
        while let Some(completion) = dispatches.next().await {
            pending.complete(completion.account_id.as_deref(), completion.request_bytes);
        }
    })
    .await;
    connection_result
}

/// Classify a codec failure: losing the peer is the end of a session, while a
/// frame we could not parse or that broke the line limit is a protocol error.
fn codec_failure(error: &LinesCodecError) -> HostError {
    let LinesCodecError::Io(error) = error else {
        return HostError::InvalidFrame;
    };
    match error.kind() {
        io::ErrorKind::ConnectionReset
        | io::ErrorKind::ConnectionAborted
        | io::ErrorKind::BrokenPipe
        | io::ErrorKind::UnexpectedEof
        | io::ErrorKind::NotConnected => HostError::PeerGone,
        _ => HostError::InvalidFrame,
    }
}

fn request_account_id(request: &HostRequest) -> Option<String> {
    request
        .params
        .get("accountId")
        .and_then(Value::as_str)
        .map(str::to_string)
}

/// Periodic redacted metrics dump (plan §8): one structured log line per
/// series. Process-lifetime task; nothing is exported over any socket.
fn spawn_metrics_log() {
    tokio::spawn(async move {
        let mut ticker = interval(METRICS_LOG_INTERVAL);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        // Skip the immediate first tick so startup stays quiet.
        ticker.tick().await;
        loop {
            ticker.tick().await;
            metrics::log_snapshot();
        }
    });
}

/// One completed host dispatch, classified and redacted (plan §8): method
/// category, opaque account id, request size, duration, and result class. A
/// successful read/control poll stays at debug level so the default output
/// shows mutating work and failures only.
fn log_dispatch(
    method_class: &'static str,
    result_class: &'static str,
    error_code: Option<&'static str>,
    account_id: Option<&str>,
    request_bytes: usize,
    duration: Duration,
) {
    let account_id = account_id.unwrap_or("-");
    let error_code = error_code.unwrap_or("-");
    let duration_ms = duration.as_millis() as u64;
    match result_class {
        "ok" if matches!(method_class, "read" | "control") => tracing::debug!(
            method_class,
            result_class,
            account_id,
            request_bytes,
            duration_ms,
            "host request completed"
        ),
        "ok" => tracing::info!(
            method_class,
            result_class,
            account_id,
            request_bytes,
            duration_ms,
            "host request completed"
        ),
        _ => tracing::warn!(
            method_class,
            result_class,
            error_code,
            account_id,
            request_bytes,
            duration_ms,
            "host request failed"
        ),
    }
}

async fn dispatch(request: HostRequest, runtime: &ProxyGroupRuntime) -> HostResponse {
    let request_id = request.request_id;
    match request.method.as_str() {
        "runtime.status" if empty_params(&request.params) => HostResponse::success(
            request_id,
            serde_json::to_value(runtime.status().await).unwrap_or(Value::Null),
        ),
        "runtime.start" if empty_params(&request.params) => match runtime.start().await {
            Ok(status) => HostResponse::success(
                request_id,
                serde_json::to_value(status).unwrap_or(Value::Null),
            ),
            Err(failure) => HostResponse::failure(request_id, map_start_failure(failure)),
        },
        "runtime.stop" if empty_params(&request.params) => match runtime.stop().await {
            Ok(status) => HostResponse::success(
                request_id,
                serde_json::to_value(status).unwrap_or(Value::Null),
            ),
            Err(failure) => HostResponse::failure(request_id, map_stop_failure(failure)),
        },
        "runtime.status" | "runtime.start" | "runtime.stop" => HostResponse::failure(
            request_id,
            ApiError::new(
                "INVALID_REQUEST",
                "this method requires empty params",
                false,
            ),
        ),
        "accounts.list" if empty_params(&request.params) => match runtime.list_accounts().await {
            Ok(accounts) => HostResponse::success(
                request_id,
                serde_json::to_value(accounts).unwrap_or(Value::Null),
            ),
            Err(error) => HostResponse::failure(request_id, error.into_api()),
        },
        "accounts.list" => HostResponse::failure(
            request_id,
            ApiError::new(
                "INVALID_REQUEST",
                "this method requires empty params",
                false,
            ),
        ),
        "accounts.deleteLocalData" => {
            match serde_json::from_value::<AccountDeleteLocalDataParams>(request.params) {
                Ok(params) => match runtime
                    .delete_local_account(params.account_id, params.operation_id)
                    .await
                {
                    Ok(result) => HostResponse::success(request_id, result),
                    Err(error) => HostResponse::failure(request_id, error.into_api()),
                },
                Err(_) => HostResponse::failure(
                    request_id,
                    ApiError::new(
                        "INVALID_REQUEST",
                        "invalid accounts.deleteLocalData params",
                        false,
                    ),
                ),
            }
        }
        "link.start" => match serde_json::from_value::<LinkStartParams>(request.params) {
            Ok(params) => {
                match runtime
                    .start_link(params.device_name, params.proxy_group)
                    .await
                {
                    Ok(result) => HostResponse::success(request_id, result),
                    Err(error) => HostResponse::failure(request_id, error.into_api()),
                }
            }
            Err(_) => HostResponse::failure(
                request_id,
                ApiError::new("INVALID_REQUEST", "invalid link.start params", false),
            ),
        },
        "link.finish" => match serde_json::from_value::<LinkSessionParams>(request.params) {
            Ok(params) => match runtime.finish_link(params.link_session_id).await {
                Ok(account) => HostResponse::success(
                    request_id,
                    serde_json::to_value(account).unwrap_or(Value::Null),
                ),
                Err(error) => HostResponse::failure(request_id, error.into_api()),
            },
            Err(_) => HostResponse::failure(
                request_id,
                ApiError::new("INVALID_REQUEST", "invalid link.finish params", false),
            ),
        },
        "link.cancel" => match serde_json::from_value::<LinkSessionParams>(request.params) {
            Ok(params) => match runtime.cancel_link(params.link_session_id).await {
                Ok(result) => HostResponse::success(request_id, result),
                Err(error) => HostResponse::failure(request_id, error.into_api()),
            },
            Err(_) => HostResponse::failure(
                request_id,
                ApiError::new("INVALID_REQUEST", "invalid link.cancel params", false),
            ),
        },
        "conversations.list" => {
            match serde_json::from_value::<ConversationsListParams>(request.params) {
                Ok(params) if (1..=MAX_PAGE_LIMIT).contains(&params.limit) => {
                    match runtime
                        .list_conversations(params.account_id, params.limit, params.cursor)
                        .await
                    {
                        Ok(page) => HostResponse::success(
                            request_id,
                            serde_json::to_value(page).unwrap_or(Value::Null),
                        ),
                        Err(error) => HostResponse::failure(request_id, error.into_api()),
                    }
                }
                Ok(_) => HostResponse::failure(
                    request_id,
                    ApiError::new("INVALID_REQUEST", "limit must be between 1 and 200", false),
                ),
                Err(_) => HostResponse::failure(
                    request_id,
                    ApiError::new(
                        "INVALID_REQUEST",
                        "invalid conversations.list params",
                        false,
                    ),
                ),
            }
        }
        "messages.list" => match serde_json::from_value::<MessagesListParams>(request.params) {
            Ok(params) if (1..=MAX_PAGE_LIMIT).contains(&params.limit) => {
                match runtime
                    .list_messages(
                        params.account_id,
                        params.conversation_id,
                        params.limit,
                        params.before,
                    )
                    .await
                {
                    Ok(page) => HostResponse::success(
                        request_id,
                        serde_json::to_value(page).unwrap_or(Value::Null),
                    ),
                    Err(error) => HostResponse::failure(request_id, error.into_api()),
                }
            }
            Ok(_) => HostResponse::failure(
                request_id,
                ApiError::new("INVALID_REQUEST", "limit must be between 1 and 200", false),
            ),
            Err(_) => HostResponse::failure(
                request_id,
                ApiError::new("INVALID_REQUEST", "invalid messages.list params", false),
            ),
        },
        "messages.getText" => {
            match serde_json::from_value::<MessageGetTextParams>(request.params) {
                Ok(params) => match runtime
                    .get_message_text(params.account_id, params.conversation_id, params.message_id)
                    .await
                {
                    Ok(message) => HostResponse::success(
                        request_id,
                        serde_json::to_value(message).unwrap_or(Value::Null),
                    ),
                    Err(error) => HostResponse::failure(request_id, error.into_api()),
                },
                Err(_) => HostResponse::failure(
                    request_id,
                    ApiError::new("INVALID_REQUEST", "invalid messages.getText params", false),
                ),
            }
        }
        "messages.sendText" => {
            match serde_json::from_value::<MessagesSendTextParams>(request.params) {
                Ok(params) => {
                    // Exactly one addressing form: an existing conversationId,
                    // or a peer target (kind + peerKey, optional peerTitle).
                    let target = match (
                        params.conversation_id,
                        params.kind,
                        params.peer_key,
                        params.peer_title,
                    ) {
                        (Some(conversation_id), None, None, None) => {
                            Ok(SendTarget::Conversation(conversation_id))
                        }
                        (None, Some(kind), Some(peer_key), peer_title) => Ok(SendTarget::Peer {
                            kind,
                            peer_key,
                            peer_title,
                        }),
                        _ => Err(ApiError::new(
                            "INVALID_REQUEST",
                            "exactly one of conversationId or kind+peerKey must be provided",
                            false,
                        )),
                    };
                    match target {
                        Ok(target) => match runtime
                            .send_text(
                                params.account_id,
                                target,
                                params.text,
                                params.client_request_id,
                                params.quote_message_id,
                            )
                            .await
                        {
                            Ok(message) => HostResponse::success(
                                request_id,
                                serde_json::to_value(message).unwrap_or(Value::Null),
                            ),
                            Err(error) => HostResponse::failure(request_id, error.into_api()),
                        },
                        Err(error) => HostResponse::failure(request_id, error),
                    }
                }
                Err(_) => HostResponse::failure(
                    request_id,
                    ApiError::new("INVALID_REQUEST", "invalid messages.sendText params", false),
                ),
            }
        }
        "messages.remoteDelete" => {
            match serde_json::from_value::<MessagesRemoteDeleteParams>(request.params) {
                Ok(params) => match runtime.remote_delete(params).await {
                    Ok(status) => HostResponse::success(request_id, json!({ "status": status })),
                    Err(error) => HostResponse::failure(request_id, error.into_api()),
                },
                Err(_) => HostResponse::failure(
                    request_id,
                    ApiError::new(
                        "INVALID_REQUEST",
                        "invalid messages.remoteDelete params",
                        false,
                    ),
                ),
            }
        }
        "messages.sendReaction" => {
            match serde_json::from_value::<MessagesSendReactionParams>(request.params) {
                Ok(params) => match runtime.send_reaction(params).await {
                    Ok(status) => HostResponse::success(request_id, json!({ "status": status })),
                    Err(error) => HostResponse::failure(request_id, error.into_api()),
                },
                Err(_) => HostResponse::failure(
                    request_id,
                    ApiError::new(
                        "INVALID_REQUEST",
                        "invalid messages.sendReaction params",
                        false,
                    ),
                ),
            }
        }
        "messages.attachments.get" => {
            match serde_json::from_value::<MessagesGetAttachmentParams>(request.params) {
                Ok(params) => match runtime.get_attachment(params).await {
                    Ok(payload) => HostResponse::success(
                        request_id,
                        serde_json::to_value(payload).unwrap_or(Value::Null),
                    ),
                    Err(error) => HostResponse::failure(request_id, error.into_api()),
                },
                Err(_) => HostResponse::failure(
                    request_id,
                    ApiError::new(
                        "INVALID_REQUEST",
                        "invalid messages.attachments.get params",
                        false,
                    ),
                ),
            }
        }
        "contacts.sync" => match serde_json::from_value::<ContactsSyncParams>(request.params) {
            Ok(params) => match runtime.sync_contacts(&params.account_id).await {
                Ok(outcome) => HostResponse::success(
                    request_id,
                    serde_json::to_value(outcome).unwrap_or(Value::Null),
                ),
                Err(error) => HostResponse::failure(request_id, error.into_api()),
            },
            Err(_) => HostResponse::failure(
                request_id,
                ApiError::new("INVALID_REQUEST", "invalid contacts.sync params", false),
            ),
        },
        "contacts.list" => match serde_json::from_value::<ContactsListParams>(request.params) {
            Ok(params) if (1..=MAX_PAGE_LIMIT).contains(&params.limit) => {
                match runtime
                    .list_contacts(params.account_id, params.query, params.limit, params.cursor)
                    .await
                {
                    Ok(page) => HostResponse::success(
                        request_id,
                        serde_json::to_value(page).unwrap_or(Value::Null),
                    ),
                    Err(error) => HostResponse::failure(request_id, error.into_api()),
                }
            }
            Ok(_) => HostResponse::failure(
                request_id,
                ApiError::new("INVALID_REQUEST", "limit must be between 1 and 200", false),
            ),
            Err(_) => HostResponse::failure(
                request_id,
                ApiError::new("INVALID_REQUEST", "invalid contacts.list params", false),
            ),
        },
        "groups.get" => match serde_json::from_value::<GroupsGetParams>(request.params) {
            Ok(params) => match runtime.get_group(params).await {
                Ok(details) => HostResponse::success(
                    request_id,
                    serde_json::to_value(details).unwrap_or(Value::Null),
                ),
                Err(error) => HostResponse::failure(request_id, error.into_api()),
            },
            Err(_) => HostResponse::failure(
                request_id,
                ApiError::new("INVALID_REQUEST", "invalid groups.get params", false),
            ),
        },
        "contacts.setLocalAlias" => {
            match serde_json::from_value::<ContactsSetLocalAliasParams>(request.params) {
                Ok(params) => match runtime.set_local_alias(params).await {
                    Ok(status) => HostResponse::success(request_id, json!({ "status": status })),
                    Err(error) => HostResponse::failure(request_id, error.into_api()),
                },
                Err(_) => HostResponse::failure(
                    request_id,
                    ApiError::new(
                        "INVALID_REQUEST",
                        "invalid contacts.setLocalAlias params",
                        false,
                    ),
                ),
            }
        }
        "presence.setTypingMessage" => {
            match serde_json::from_value::<PresenceSetTypingMessageParams>(request.params) {
                Ok(params) => match runtime.set_typing_message(params).await {
                    Ok(status) => HostResponse::success(request_id, json!({ "status": status })),
                    Err(error) => HostResponse::failure(request_id, error.into_api()),
                },
                Err(_) => HostResponse::failure(
                    request_id,
                    ApiError::new(
                        "INVALID_REQUEST",
                        "invalid presence.setTypingMessage params",
                        false,
                    ),
                ),
            }
        }
        _ => HostResponse::failure(
            request_id,
            ApiError::new("METHOD_NOT_ALLOWED", "method is not allowed", false),
        ),
    }
}

fn empty_params(params: &Value) -> bool {
    params.as_object().is_some_and(serde_json::Map::is_empty)
}

fn map_start_failure(failure: StartFailure) -> ApiError {
    match failure {
        StartFailure::AlreadyRunning => ApiError::new(
            "RUNTIME_ALREADY_RUNNING",
            "signal-cli runtime is already running",
            false,
        ),
        StartFailure::StartFailed => ApiError::new(
            "RUNTIME_START_FAILED",
            "signal-cli runtime could not be started",
            true,
        ),
    }
}

fn map_stop_failure(failure: StopFailure) -> ApiError {
    match failure {
        StopFailure::NotRunning => ApiError::new(
            "RUNTIME_NOT_RUNNING",
            "signal-cli runtime is not running",
            false,
        ),
        StopFailure::StopFailed => ApiError::new(
            "RUNTIME_STOP_FAILED",
            "signal-cli runtime could not be stopped",
            true,
        ),
    }
}

async fn send_host_event<S>(writer: &HostWriter<S>, event: HostSideEvent) -> Result<(), HostError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    match event {
        HostSideEvent::StorageChanged { state } => {
            send_shared(
                writer,
                &HostEvent::new("runtime.storageChanged", json!({ "state": state })),
            )
            .await
        }
        HostSideEvent::AccountChanged(account) => {
            send_shared(writer, &HostEvent::new("account.changed", account)).await
        }
        HostSideEvent::ConversationChanged(conversation) => {
            send_shared(
                writer,
                &HostEvent::new("conversation.changed", conversation),
            )
            .await
        }
        HostSideEvent::MessageUpserted(message) => {
            send_shared(writer, &HostEvent::new("message.upserted", message)).await
        }
        HostSideEvent::MessageStatusChanged {
            account_id,
            message_id,
            status,
        } => {
            send_shared(
                writer,
                &HostEvent::new(
                    "message.statusChanged",
                    json!({ "accountId": account_id, "messageId": message_id, "status": status }),
                ),
            )
            .await
        }
    }
}

async fn send_shared<S, T>(writer: &HostWriter<S>, value: &T) -> Result<(), HostError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    T: Serialize,
{
    let encoded = serde_json::to_string(value).map_err(|_| HostError::InvalidFrame)?;
    writer
        .lock()
        .await
        .send(encoded)
        .await
        .map_err(|error| codec_failure(&error))
}

async fn send_json<S, T>(framed: &mut Framed<S, LinesCodec>, value: &T) -> Result<(), HostError>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: Serialize,
{
    let encoded = serde_json::to_string(value).map_err(|_| HostError::InvalidFrame)?;
    framed
        .send(encoded)
        .await
        .map_err(|error| codec_failure(&error))
}

fn random_identifier() -> String {
    let mut value = [0_u8; 16];
    rand::rng().fill_bytes(&mut value);
    hex::encode(value)
}

#[derive(Default)]
struct RecentRequestIds {
    set: HashSet<String>,
    order: VecDeque<String>,
}

impl RecentRequestIds {
    fn insert(&mut self, id: String) -> bool {
        if self.set.contains(&id) {
            return false;
        }
        if self.order.len() == RECENT_REQUEST_IDS {
            if let Some(expired) = self.order.pop_front() {
                self.set.remove(&expired);
            }
        }
        self.set.insert(id.clone());
        self.order.push_back(id);
        true
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use futures_util::{SinkExt, StreamExt};
    use hmac::{Hmac, Mac};
    use serde_json::Value;
    use sha2::Sha256;
    use tempfile::TempDir;
    use tokio::io::duplex;

    use super::*;
    use crate::auth::BootstrapSecret;
    use crate::engine::SignalCliConfig;
    use crate::store::{Store, StoreKey};
    use crate::supervisor::RuntimeSupervisor;

    fn test_supervisor() -> Arc<RuntimeSupervisor> {
        let temp = TempDir::new().unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        let store = Store::open(temp.path(), Some(StoreKey::from_bytes([0x5A; 32]))).unwrap();
        // Leak TempDir for unit test lifetime; path stays valid for process.
        std::mem::forget(temp);
        Arc::new(RuntimeSupervisor::new(
            SignalCliConfig::new(
                PathBuf::from("unused-signal-cli"),
                PathBuf::from("/tmp/unused-signal-data"),
            ),
            Arc::new(store),
            crate::DEFAULT_PROXY_GROUP_ID.to_string(),
        ))
    }

    /// A single-group runtime assembled the same way the launcher assembles
    /// multi-group plans; field-for-field identical on the wire.
    fn test_runtime() -> Arc<ProxyGroupRuntime> {
        ProxyGroupRuntime::new(vec![crate::registry::ProxyGroupSlot::new(
            crate::DEFAULT_PROXY_GROUP_ID.to_string(),
            test_supervisor(),
        )])
    }

    #[test]
    fn a_vanished_host_ends_the_session_instead_of_failing_the_process() {
        // The host closing its socket abruptly is how a session normally ends;
        // reporting it as a bad frame makes the process exit non-zero and tells
        // the user the protocol broke.
        for kind in [
            io::ErrorKind::ConnectionReset,
            io::ErrorKind::ConnectionAborted,
            io::ErrorKind::BrokenPipe,
            io::ErrorKind::UnexpectedEof,
            io::ErrorKind::NotConnected,
        ] {
            assert!(matches!(
                codec_failure(&LinesCodecError::Io(io::Error::from(kind))),
                HostError::PeerGone,
            ));
        }
        // A frame we could not read is still a protocol error.
        assert!(matches!(
            codec_failure(&LinesCodecError::MaxLineLengthExceeded),
            HostError::InvalidFrame,
        ));
        assert!(matches!(
            codec_failure(&LinesCodecError::Io(io::Error::from(
                io::ErrorKind::InvalidData
            ))),
            HostError::InvalidFrame,
        ));
    }

    #[test]
    fn request_id_window_is_bounded_and_detects_replay() {
        let mut ids = RecentRequestIds::default();
        assert!(ids.insert("same".into()));
        assert!(!ids.insert("same".into()));
        for index in 0..RECENT_REQUEST_IDS {
            assert!(ids.insert(format!("id-{index}")));
        }
        assert!(ids.set.len() <= RECENT_REQUEST_IDS);
        assert!(ids.insert("same".into()));
    }

    #[test]
    fn pending_budget_bounds_global_account_and_bytes() {
        let mut per_account = HostPendingBudget::default();
        for _ in 0..MAX_PENDING_HOST_REQUESTS_PER_ACCOUNT {
            assert!(per_account.try_admit(Some("account-a"), 1));
        }
        assert!(!per_account.try_admit(Some("account-a"), 1));
        assert!(per_account.try_admit(Some("account-b"), 1));
        per_account.complete(Some("account-a"), 1);
        assert!(per_account.try_admit(Some("account-a"), 1));

        let mut global = HostPendingBudget::default();
        for _ in 0..MAX_PENDING_HOST_REQUESTS {
            assert!(global.try_admit(None, 1));
        }
        assert!(!global.try_admit(None, 1));

        let mut bytes = HostPendingBudget::default();
        assert!(bytes.try_admit(None, MAX_PENDING_HOST_BYTES));
        assert!(!bytes.try_admit(None, 1));
    }

    #[tokio::test]
    async fn phone_approval_wait_does_not_block_link_control() {
        let limits = Arc::new(HostDispatchLimits::new());
        let finish = limits
            .acquire("link.finish", None, Some("default"))
            .await
            .unwrap();

        let cancel = timeout(
            Duration::from_millis(50),
            limits.acquire("link.cancel", None, None),
        )
        .await
        .expect("link.cancel must use the independent control lane")
        .unwrap();

        // A second concurrent finish inside the SAME group is rejected with
        // the wire-contract code instead of queueing (ADR 0001 R3).
        let second_finish = limits
            .acquire("link.finish", None, Some("default"))
            .await
            .err()
            .expect("a second finish in one group must be rejected");
        assert_eq!(second_finish.code, "LINK_IN_PROGRESS");
        assert!(!second_finish.retryable);

        // Another group's lane is untouched by the first group's wait.
        timeout(
            Duration::from_millis(50),
            limits.acquire("link.finish", None, Some("team-b")),
        )
        .await
        .expect("a different group's finish lane must stay free")
        .unwrap();

        drop(cancel);
        drop(finish);
        // Once the first finish drains, its group's lane is free again.
        limits
            .acquire("link.finish", None, Some("default"))
            .await
            .unwrap();
    }

    /// The account delete drain barrier: a delete marks the account, then waits
    /// for the in-flight send holding the per-account mutex; sends/syncs that
    /// arrive during the delete are rejected instead of queueing; once the
    /// delete dispatch ends, the mark is gone. Linearization point is the
    /// per-account mutex: a send either completes fully before the delete's
    /// upstream call, or it never reaches the upstream at all.
    #[tokio::test]
    async fn account_delete_drains_in_flight_send_and_rejects_new_mutations() {
        let limits = Arc::new(HostDispatchLimits::new());
        let send = limits
            .acquire("messages.sendText", Some("account-a"), None)
            .await
            .unwrap();

        // Spawn the delete: it marks the account immediately, then parks on
        // the account mutex while the send is still in flight.
        let delete_task = {
            let limits = Arc::clone(&limits);
            tokio::spawn(async move {
                limits
                    .acquire("accounts.deleteLocalData", Some("account-a"), None)
                    .await
            })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            !delete_task.is_finished(),
            "delete must wait for the in-flight send to drain"
        );
        assert!(
            limits
                .deleting_accounts
                .lock()
                .unwrap()
                .contains_key("account-a"),
            "delete marks the account before waiting for the drain"
        );

        // New mutating work for the deleting account is rejected, not queued.
        let rejected_send = limits
            .acquire("messages.sendText", Some("account-a"), None)
            .await
            .err()
            .expect("acquire must reject mutating work on a deleting account");
        assert_eq!(rejected_send.code, "ACCOUNT_NOT_FOUND");
        assert!(!rejected_send.retryable);
        let rejected_sync = limits
            .acquire("contacts.sync", Some("account-a"), None)
            .await
            .err()
            .expect("acquire must reject mutating work on a deleting account");
        assert_eq!(rejected_sync.code, "ACCOUNT_NOT_FOUND");

        // Other accounts and read-only methods are unaffected.
        limits
            .acquire("messages.sendText", Some("account-b"), None)
            .await
            .unwrap();
        limits
            .acquire("messages.list", Some("account-a"), None)
            .await
            .unwrap();

        // New mutating work covers remoteDelete too: rejected while the
        // delete drains, same-account serialized against sendText.
        let rejected_delete = limits
            .acquire("messages.remoteDelete", Some("account-a"), None)
            .await
            .err()
            .expect("acquire must reject mutating work on a deleting account");
        assert_eq!(rejected_delete.code, "ACCOUNT_NOT_FOUND");
        assert!(!rejected_delete.retryable);

        // Once the send drains, the delete proceeds; while it runs the mark
        // stays, and once the dispatch permit drops the mark is cleared so a
        // later send is admitted again (it then fails at preparation if the
        // account row is really gone).
        drop(send);
        let delete = timeout(Duration::from_secs(1), delete_task)
            .await
            .expect("delete must proceed once the send drained")
            .unwrap()
            .unwrap();
        let still_rejected = limits
            .acquire("messages.sendText", Some("account-a"), None)
            .await
            .err()
            .expect("acquire must reject mutating work on a deleting account");
        assert_eq!(still_rejected.code, "ACCOUNT_NOT_FOUND");
        drop(delete);
        assert!(
            limits.deleting_accounts.lock().unwrap().is_empty(),
            "a finished delete clears the mark"
        );
        limits
            .acquire("messages.sendText", Some("account-a"), None)
            .await
            .unwrap();
    }

    /// Overlapping deletes of the same account serialize on the account mutex,
    /// a delete whose wait is cancelled (timeout/dropped future) does not leave
    /// a stale mark, and an active delete keeps rejecting sends until it ends.
    #[tokio::test]
    async fn overlapping_deletes_keep_the_mark_until_the_last_one_finishes() {
        let limits = Arc::new(HostDispatchLimits::new());
        let first = limits
            .acquire("accounts.deleteLocalData", Some("account-a"), None)
            .await
            .unwrap();
        // A second delete marks too, then waits on the account mutex; when its
        // wait is cancelled, its mark is rolled back with the dropped future.
        let second_pending = timeout(
            Duration::from_millis(20),
            limits.acquire("accounts.deleteLocalData", Some("account-a"), None),
        )
        .await;
        assert!(second_pending.is_err());
        drop(first);
        assert!(
            limits.deleting_accounts.lock().unwrap().is_empty(),
            "a cancelled delete wait must not leave a stale mark"
        );

        let second = limits
            .acquire("accounts.deleteLocalData", Some("account-a"), None)
            .await
            .unwrap();
        let rejected = limits
            .acquire("messages.sendText", Some("account-a"), None)
            .await
            .err()
            .expect("acquire must reject mutating work on a deleting account");
        assert_eq!(rejected.code, "ACCOUNT_NOT_FOUND");
        drop(second);
        limits
            .acquire("messages.sendText", Some("account-a"), None)
            .await
            .unwrap();
    }

    /// remoteDelete and sendReaction are mutating, account-scoped methods:
    /// they share the send lane and the per-account mutex with sendText, so
    /// two same-account mutations can never interleave
    /// (docs/remote-delete-l2-plan.md §3.4).
    #[tokio::test]
    async fn remote_delete_and_send_text_serialize_per_account() {
        let limits = Arc::new(HostDispatchLimits::new());
        let send = limits
            .acquire("messages.sendText", Some("account-a"), None)
            .await
            .unwrap();

        // The second same-account mutation parks on the account mutex instead
        // of running concurrently (and not on lane capacity: the send lane
        // still has a free permit).
        let pending_delete = timeout(
            Duration::from_millis(50),
            limits.acquire("messages.remoteDelete", Some("account-a"), None),
        )
        .await;
        assert!(
            pending_delete.is_err(),
            "remoteDelete must wait for the in-flight same-account send"
        );

        // A different account's remoteDelete is unaffected.
        timeout(
            Duration::from_millis(50),
            limits.acquire("messages.remoteDelete", Some("account-b"), None),
        )
        .await
        .expect("another account's remoteDelete must not serialize behind account-a")
        .unwrap();

        // sendReaction joins the same mutating set: same-account serialized,
        // other-account unaffected.
        let pending_reaction = timeout(
            Duration::from_millis(50),
            limits.acquire("messages.sendReaction", Some("account-a"), None),
        )
        .await;
        assert!(
            pending_reaction.is_err(),
            "sendReaction must wait for the in-flight same-account send"
        );
        timeout(
            Duration::from_millis(50),
            limits.acquire("messages.sendReaction", Some("account-c"), None),
        )
        .await
        .expect("another account's sendReaction must not serialize behind account-a")
        .unwrap();

        // contacts.setLocalAlias joins the same mutating set (contract
        // revision 1.10): same-account serialized against the send, and it
        // rides the send lane rather than the control lane.
        let pending_alias = timeout(
            Duration::from_millis(50),
            limits.acquire("contacts.setLocalAlias", Some("account-a"), None),
        )
        .await;
        assert!(
            pending_alias.is_err(),
            "setLocalAlias must wait for the in-flight same-account send"
        );
        timeout(
            Duration::from_millis(50),
            limits.acquire("contacts.setLocalAlias", Some("account-c"), None),
        )
        .await
        .expect("another account's setLocalAlias must not serialize behind account-a")
        .unwrap();

        drop(send);
        timeout(
            Duration::from_secs(1),
            limits.acquire("messages.remoteDelete", Some("account-a"), None),
        )
        .await
        .expect("remoteDelete must proceed once the send drained")
        .unwrap();
    }

    #[tokio::test]
    async fn authenticated_session_can_query_status_and_rejects_replayed_id() {
        let secret_bytes = [7_u8; 32];
        let secret = Arc::new(BootstrapSecret::for_test(secret_bytes));
        let runtime = test_runtime();
        let (server_stream, client_stream) = duplex(64 * 1024);
        let server = tokio::spawn(handle_connection(server_stream, secret, runtime));
        let mut client = Framed::new(client_stream, LinesCodec::new());

        let challenge: Value =
            serde_json::from_str(&client.next().await.unwrap().unwrap()).unwrap();
        let server_nonce = challenge["data"]["serverNonce"].as_str().unwrap();
        let client_nonce = hex::encode([9_u8; 32]);
        let proof = client_proof(&secret_bytes, server_nonce, &client_nonce);
        client
            .send(
                json!({
                    "apiVersion": API_VERSION,
                    "requestId": "handshake-1",
                    "method": "handshake",
                    "params": { "clientNonce": client_nonce, "proof": proof }
                })
                .to_string(),
            )
            .await
            .unwrap();
        let handshake: Value =
            serde_json::from_str(&client.next().await.unwrap().unwrap()).unwrap();
        assert_eq!(handshake["requestId"], "handshake-1");
        assert_eq!(handshake["result"]["apiVersion"], API_VERSION);
        assert!(
            handshake["result"]["capabilities"]
                .as_array()
                .unwrap()
                .iter()
                .any(|value| value == "messages.sendText")
        );

        client
            .send(
                json!({
                    "apiVersion": API_VERSION,
                    "requestId": "handshake-1",
                    "method": "runtime.status",
                    "params": {}
                })
                .to_string(),
            )
            .await
            .unwrap();
        let handshake_replay: Value =
            serde_json::from_str(&client.next().await.unwrap().unwrap()).unwrap();
        assert_eq!(handshake_replay["error"]["code"], "INVALID_REQUEST");

        let status_request = json!({
            "apiVersion": API_VERSION,
            "requestId": "status-1",
            "method": "runtime.status",
            "params": {}
        })
        .to_string();
        client.send(status_request.clone()).await.unwrap();
        let status: Value = serde_json::from_str(&client.next().await.unwrap().unwrap()).unwrap();
        assert_eq!(status["result"]["state"], "stopped");

        client.send(status_request).await.unwrap();
        let replay: Value = serde_json::from_str(&client.next().await.unwrap().unwrap()).unwrap();
        assert_eq!(replay["error"]["code"], "INVALID_REQUEST");

        drop(client);
        assert!(server.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn a_host_that_leaves_right_after_authenticating_still_ends_cleanly() {
        let secret_bytes = [7_u8; 32];
        let secret = Arc::new(BootstrapSecret::for_test(secret_bytes));
        let runtime = test_runtime();
        let (server_stream, client_stream) = duplex(64 * 1024);
        let server = tokio::spawn(handle_connection(server_stream, secret, runtime));
        let mut client = Framed::new(client_stream, LinesCodec::new());
        let challenge: Value =
            serde_json::from_str(&client.next().await.unwrap().unwrap()).unwrap();
        let server_nonce = challenge["data"]["serverNonce"].as_str().unwrap();
        let client_nonce = hex::encode([9_u8; 32]);
        let proof = client_proof(&secret_bytes, server_nonce, &client_nonce);
        client
            .send(
                json!({
                    "apiVersion": API_VERSION,
                    "requestId": "handshake-1",
                    "method": "handshake",
                    "params": { "clientNonce": client_nonce, "proof": proof }
                })
                .to_string(),
            )
            .await
            .unwrap();

        // Leave without collecting the handshake reply, so the write that
        // reports success is the one that finds the peer gone. This is a session
        // that ended, not a protocol failure, and the process must exit cleanly.
        drop(client);

        assert!(
            server.await.unwrap().is_ok(),
            "a host that vanished must not fail the process"
        );
    }

    #[tokio::test]
    async fn failed_authentication_returns_generic_error_and_closes() {
        let secret = Arc::new(BootstrapSecret::for_test([7_u8; 32]));
        let runtime = test_runtime();
        let (server_stream, client_stream) = duplex(64 * 1024);
        let server = tokio::spawn(handle_connection(server_stream, secret, runtime));
        let mut client = Framed::new(client_stream, LinesCodec::new());
        let _challenge = client.next().await.unwrap().unwrap();
        client
            .send(
                json!({
                    "apiVersion": API_VERSION,
                    "requestId": "handshake-1",
                    "method": "handshake",
                    "params": {
                        "clientNonce": hex::encode([9_u8; 32])
                    }
                })
                .to_string(),
            )
            .await
            .unwrap();
        let response: Value = serde_json::from_str(&client.next().await.unwrap().unwrap()).unwrap();
        assert_eq!(response["error"]["code"], "AUTHENTICATION_FAILED");
        assert!(matches!(
            server.await.unwrap(),
            Err(HostError::Authentication)
        ));
    }

    fn client_proof(secret: &[u8; 32], server_nonce: &str, client_nonce: &str) -> String {
        let mut mac = Hmac::<Sha256>::new_from_slice(secret).unwrap();
        mac.update(b"kt-signal-connector-v1\0");
        mac.update(server_nonce.as_bytes());
        mac.update(b"\0");
        mac.update(client_nonce.as_bytes());
        mac.update(b"\0");
        mac.update(API_VERSION.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }
}
