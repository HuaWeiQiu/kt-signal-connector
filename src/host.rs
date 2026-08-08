// SPDX-License-Identifier: AGPL-3.0-only

use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use futures_util::future::BoxFuture;
use futures_util::stream::{FuturesUnordered, SplitSink};
use futures_util::{FutureExt, SinkExt, StreamExt};
use rand::RngCore;
use serde::Serialize;
use serde_json::{Value, json};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{Mutex, OwnedMutexGuard, OwnedSemaphorePermit, Semaphore, broadcast, watch};
use tokio::time::timeout;
use tokio_util::codec::{Framed, LinesCodec};

use crate::auth::{BootstrapSecret, HandshakeParams, PendingChallenge};
use crate::engine::{EngineError, EngineEvent};
use crate::ipc::LocalListener;
use crate::protocol::{ApiError, HostEvent, HostRequest, HostResponse};
use crate::service::{
    AccountDeleteLocalDataParams, ConversationsListParams, HostSideEvent, LinkSessionParams,
    LinkStartParams, MessageGetTextParams, MessagesListParams, MessagesSendTextParams,
};
use crate::store::MAX_PAGE_LIMIT;
use crate::supervisor::RuntimeSupervisor;
use crate::{API_VERSION, DEFAULT_HOST_FRAME_LIMIT, PHASE2_CAPABILITIES};

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
const HOST_DISPATCH_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
const RECENT_REQUEST_IDS: usize = 128;
const MAX_PENDING_HOST_REQUESTS: usize = 128;
const MAX_PENDING_HOST_REQUESTS_PER_ACCOUNT: usize = 32;
const MAX_PENDING_HOST_BYTES: usize = 8 * 1024 * 1024;
const CONTROL_CONCURRENCY: usize = 1;
const LINK_WAIT_CONCURRENCY: usize = 1;
const READ_CONCURRENCY: usize = 4;
const SEND_CONCURRENCY: usize = 2;

type HostWriter<S> = Arc<Mutex<SplitSink<Framed<S, LinesCodec>, String>>>;
type DispatchFuture = BoxFuture<'static, DispatchCompletion>;

struct DispatchCompletion {
    method: String,
    account_id: Option<String>,
    request_bytes: usize,
    write_result: Result<(), HostError>,
}

struct HostDispatchPermit {
    _lane: OwnedSemaphorePermit,
    _account: Option<OwnedMutexGuard<()>>,
}

struct HostDispatchLimits {
    control: Arc<Semaphore>,
    link_wait: Arc<Semaphore>,
    read: Arc<Semaphore>,
    send: Arc<Semaphore>,
    send_accounts: Mutex<HashMap<String, Weak<Mutex<()>>>>,
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
        true
    }

    fn complete(&mut self, account_id: Option<&str>, request_bytes: usize) {
        self.total = self.total.saturating_sub(1);
        self.bytes = self.bytes.saturating_sub(request_bytes);
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
            link_wait: Arc::new(Semaphore::new(LINK_WAIT_CONCURRENCY)),
            read: Arc::new(Semaphore::new(READ_CONCURRENCY)),
            send: Arc::new(Semaphore::new(SEND_CONCURRENCY)),
            send_accounts: Mutex::new(HashMap::new()),
        }
    }

    async fn acquire(&self, method: &str, account_id: Option<&str>) -> HostDispatchPermit {
        if method == "messages.sendText" {
            let account_key = account_id.unwrap_or("").to_string();
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
            // Account order is acquired before global send capacity so one busy
            // account cannot occupy every send permit while waiting on itself.
            let account = account_lock.lock_owned().await;
            let lane = self.send.clone().acquire_owned().await.unwrap();
            return HostDispatchPermit {
                _lane: lane,
                _account: Some(account),
            };
        }

        let lane = if method == "link.finish" {
            self.link_wait.clone().acquire_owned().await.unwrap()
        } else if matches!(
            method,
            "conversations.list" | "messages.list" | "messages.getText"
        ) {
            self.read.clone().acquire_owned().await.unwrap()
        } else {
            self.control.clone().acquire_owned().await.unwrap()
        };
        HostDispatchPermit {
            _lane: lane,
            _account: None,
        }
    }
}

#[derive(Debug, Error)]
pub enum HostError {
    #[error("local IPC failed")]
    Io(#[from] io::Error),
    #[error("host frame is invalid")]
    InvalidFrame,
    #[error("host authentication failed")]
    Authentication,
    #[error("signal-cli runtime could not be stopped")]
    RuntimeShutdown,
}

pub async fn serve(
    listener: LocalListener,
    secret: BootstrapSecret,
    supervisor: Arc<RuntimeSupervisor>,
) -> Result<(), HostError> {
    let secret = Arc::new(secret);
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
            supervisor.clone(),
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
            supervisor
                .shutdown()
                .await
                .map_err(|_| HostError::RuntimeShutdown)?;
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
    supervisor: Arc<RuntimeSupervisor>,
) -> Result<(), HostError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    handle_connection_until_shutdown(
        stream,
        secret,
        supervisor,
        shutdown_rx,
        Arc::new(AtomicBool::new(false)),
    )
    .await
}

async fn handle_connection_until_shutdown<S>(
    stream: S,
    secret: Arc<BootstrapSecret>,
    supervisor: Arc<RuntimeSupervisor>,
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
    let mut link_finish_pending = false;
    let mut recent_ids = RecentRequestIds::default();
    recent_ids.insert(handshake_request_id);
    let mut engine_events = supervisor.subscribe_engine();
    let mut host_events = supervisor.subscribe_host();
    let connection_result = loop {
        tokio::select! {
            line = stream.next() => {
                let Some(line) = line else {
                    break Ok(());
                };
                let line = match line {
                    Ok(line) => line,
                    Err(_) => break Err(HostError::InvalidFrame),
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
                if method == "link.finish" && link_finish_pending {
                    let response = HostResponse::failure(
                        request.request_id,
                        ApiError::new(
                            "LINK_IN_PROGRESS",
                            "a link finish request is already active",
                            false,
                        ),
                    );
                    if let Err(error) = send_shared(&writer, &response).await {
                        break Err(error);
                    }
                    continue;
                }
                let account_id = request_account_id(&request);
                if !pending.try_admit(account_id.as_deref(), request_bytes) {
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
                if method == "link.finish" {
                    link_finish_pending = true;
                }

                let task_account = account_id.clone();
                let task_supervisor = supervisor.clone();
                let task_writer = writer.clone();
                let task_limits = limits.clone();
                dispatches.push(async move {
                    let _permit = task_limits.acquire(&method, task_account.as_deref()).await;
                    let response = dispatch(request, &task_supervisor).await;
                    let write_result = send_shared(&task_writer, &response).await;
                    DispatchCompletion {
                        method,
                        account_id: task_account,
                        request_bytes,
                        write_result,
                    }
                }.boxed());
            }
            completion = dispatches.next(), if !dispatches.is_empty() => {
                if let Some(completion) = completion {
                    if completion.method == "link.finish" {
                        link_finish_pending = false;
                    }
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
            event = engine_events.recv() => {
                match event {
                    Ok(EngineEvent::StateChanged(status)) => {
                        if let Err(error) = send_shared(
                            &writer,
                            &HostEvent::new("runtime.stateChanged", status),
                        ).await {
                            break Err(error);
                        }
                    }
                    Ok(EngineEvent::ProtocolWarning { kind }) => {
                        if let Err(error) = send_shared(
                            &writer,
                            &HostEvent::new(
                                "runtime.protocolWarning",
                                json!({ "kind": kind }),
                            ),
                        ).await {
                            break Err(error);
                        }
                    }
                    Ok(EngineEvent::ResourcePressure { state, pid, rss_bytes }) => {
                        if let Err(error) = send_shared(
                            &writer,
                            &HostEvent::new(
                                "runtime.resourcePressure",
                                json!({ "state": state, "pid": pid, "rssBytes": rss_bytes }),
                            ),
                        ).await {
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
    let _ = supervisor.shutdown().await;
    let _ = timeout(HOST_DISPATCH_DRAIN_TIMEOUT, async {
        while let Some(completion) = dispatches.next().await {
            pending.complete(completion.account_id.as_deref(), completion.request_bytes);
        }
    })
    .await;
    connection_result
}

fn request_account_id(request: &HostRequest) -> Option<String> {
    request
        .params
        .get("accountId")
        .and_then(Value::as_str)
        .map(str::to_string)
}

async fn dispatch(request: HostRequest, supervisor: &RuntimeSupervisor) -> HostResponse {
    let request_id = request.request_id;
    match request.method.as_str() {
        "runtime.status" if empty_params(&request.params) => HostResponse::success(
            request_id,
            serde_json::to_value(supervisor.status().await).unwrap_or(Value::Null),
        ),
        "runtime.start" if empty_params(&request.params) => match supervisor.start().await {
            Ok(status) => HostResponse::success(
                request_id,
                serde_json::to_value(status).unwrap_or(Value::Null),
            ),
            Err(error) => HostResponse::failure(request_id, map_start_error(error)),
        },
        "runtime.stop" if empty_params(&request.params) => match supervisor.stop().await {
            Ok(status) => HostResponse::success(
                request_id,
                serde_json::to_value(status).unwrap_or(Value::Null),
            ),
            Err(error) => HostResponse::failure(request_id, map_stop_error(error)),
        },
        "runtime.status" | "runtime.start" | "runtime.stop" => HostResponse::failure(
            request_id,
            ApiError::new(
                "INVALID_REQUEST",
                "this method requires empty params",
                false,
            ),
        ),
        "accounts.list" if empty_params(&request.params) => {
            match supervisor.list_accounts().await {
                Ok(accounts) => HostResponse::success(
                    request_id,
                    serde_json::to_value(accounts).unwrap_or(Value::Null),
                ),
                Err(error) => HostResponse::failure(request_id, error.into_api()),
            }
        }
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
                Ok(params) => match supervisor
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
            Ok(params) => match supervisor.start_link(params.device_name).await {
                Ok(result) => HostResponse::success(request_id, result),
                Err(error) => HostResponse::failure(request_id, error.into_api()),
            },
            Err(_) => HostResponse::failure(
                request_id,
                ApiError::new("INVALID_REQUEST", "invalid link.start params", false),
            ),
        },
        "link.finish" => match serde_json::from_value::<LinkSessionParams>(request.params) {
            Ok(params) => match supervisor.finish_link(params.link_session_id).await {
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
            Ok(params) => match supervisor.cancel_link(params.link_session_id).await {
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
                    match supervisor
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
                match supervisor
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
                Ok(params) => match supervisor
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
                Ok(params) => match supervisor
                    .send_text(
                        params.account_id,
                        params.conversation_id,
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
                Err(_) => HostResponse::failure(
                    request_id,
                    ApiError::new("INVALID_REQUEST", "invalid messages.sendText params", false),
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

fn map_start_error(error: EngineError) -> ApiError {
    match error {
        EngineError::Backpressure => ApiError::new(
            "RUNTIME_ALREADY_RUNNING",
            "signal-cli runtime is already running",
            false,
        ),
        _ => ApiError::new(
            "RUNTIME_START_FAILED",
            "signal-cli runtime could not be started",
            true,
        ),
    }
}

fn map_stop_error(error: EngineError) -> ApiError {
    match error {
        EngineError::NotRunning => ApiError::new(
            "RUNTIME_NOT_RUNNING",
            "signal-cli runtime is not running",
            false,
        ),
        _ => ApiError::new(
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
        .map_err(|_| HostError::InvalidFrame)
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
        .map_err(|_| HostError::InvalidFrame)
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
    use crate::store::Store;
    use crate::supervisor::RuntimeSupervisor;

    fn test_supervisor() -> Arc<RuntimeSupervisor> {
        let temp = TempDir::new().unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        let store = Store::open(temp.path()).unwrap();
        // Leak TempDir for unit test lifetime; path stays valid for process.
        std::mem::forget(temp);
        Arc::new(RuntimeSupervisor::new(
            SignalCliConfig::new(
                PathBuf::from("unused-signal-cli"),
                PathBuf::from("/tmp/unused-signal-data"),
            ),
            store,
        ))
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
        let finish = limits.acquire("link.finish", None).await;

        let cancel = timeout(
            Duration::from_millis(50),
            limits.acquire("link.cancel", None),
        )
        .await
        .expect("link.cancel must use the independent control lane");

        let second_finish = timeout(
            Duration::from_millis(10),
            limits.acquire("link.finish", None),
        )
        .await;
        assert!(
            second_finish.is_err(),
            "link.finish capacity must stay bounded at one"
        );

        drop(cancel);
        drop(finish);
    }

    #[tokio::test]
    async fn authenticated_session_can_query_status_and_rejects_replayed_id() {
        let secret_bytes = [7_u8; 32];
        let secret = Arc::new(BootstrapSecret::for_test(secret_bytes));
        let supervisor = test_supervisor();
        let (server_stream, client_stream) = duplex(64 * 1024);
        let server = tokio::spawn(handle_connection(server_stream, secret, supervisor));
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
    async fn failed_authentication_returns_generic_error_and_closes() {
        let secret = Arc::new(BootstrapSecret::for_test([7_u8; 32]));
        let supervisor = test_supervisor();
        let (server_stream, client_stream) = duplex(64 * 1024);
        let server = tokio::spawn(handle_connection(server_stream, secret, supervisor));
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
