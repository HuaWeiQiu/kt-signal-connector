// SPDX-License-Identifier: AGPL-3.0-only

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};
use tokio::sync::{Mutex, broadcast, mpsc};
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior, interval, sleep};

use crate::engine::{
    CallClass, EngineError, EngineEvent, EngineHandle, EngineState, EngineStatus, QueuedReceive,
    ReceiveIngress, SignalCliConfig, event_channel, receive_channel,
};
use crate::protocol::ApiError;
use crate::service::{
    AttachmentPayload, AttachmentSendTarget, ConnectorService, ContactsSyncOutcome, GroupDetails,
    HostSideEvent, PeerTarget, PreparedSend, SendTarget, ServiceError, account_limit_error,
    validate_account_delete_operation_id, validate_attachment_payload,
};
use crate::store::{
    AccountDeletePlan, AccountSummary, ContactSummary, ConversationSummary, MessageRecord, Page,
    Store, SyncedContact,
};

// Match link QR lifetime so a slow phone confirmation can still complete.
const LINK_FINISH_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const RECEIVE_STORE_RETRY_MIN: Duration = Duration::from_millis(100);
const RECEIVE_STORE_RETRY_MAX: Duration = Duration::from_secs(5);
const WATCHDOG_PING_TIMEOUT: Duration = Duration::from_secs(15);
const WATCHDOG_MAX_PING_FAILURES: u32 = 2;
/// Maximum deferred (pending) restarts per failure episode before giving up.
const WATCHDOG_MAX_PENDING_RETRIES: u32 = 3;
/// Rows one retention transaction may delete, keeping each one short.
const RETENTION_BATCH_MESSAGES: u32 = 2_000;
/// Breathing room between retention batches so receives and host requests get
/// the store lock while a large history is being pruned.
const RETENTION_BATCH_PAUSE: Duration = Duration::from_millis(50);

/// What raised a restart request. Drives whether ping recovery may clear a
/// pending restart: the REST ping says nothing about the receive WebSocket,
/// so only an executed restart clears a stderr-sourced pending.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RestartSource {
    Stderr,
    Ping,
}

impl std::fmt::Display for RestartSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RestartSource::Stderr => write!(f, "stderr"),
            RestartSource::Ping => write!(f, "ping"),
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct PendingRestart {
    source: RestartSource,
    reason: &'static str,
}

/// One failure episode: restarts performed since the first trigger, and the
/// last trigger time. A trigger-free gap longer than one throttle window
/// resets the budget.
#[derive(Default)]
struct WatchdogEpisode {
    restarts: u32,
    last_trigger: Option<Instant>,
}

/// One proxy group's supervised runtime: its own engine slot, watchdog,
/// receive pipeline and link state, over the shared per-profile store
/// (ADR 0001 R4/R5). The group id is the opaque wire identity; the proxy
/// endpoint stays inside the launcher-provided config.
pub struct RuntimeSupervisor {
    config: SignalCliConfig,
    /// Opaque group id (launcher-defined, ADR 0001 R1). Appears on the wire
    /// and in logs; the group's proxy endpoint never does.
    pub(crate) group_id: String,
    engine: Mutex<Option<EngineHandle>>,
    service: Arc<Mutex<ConnectorService>>,
    active_link_finish: Mutex<Option<String>>,
    events: broadcast::Sender<EngineEvent>,
    host_events: broadcast::Sender<HostSideEvent>,
    receive_ingress: ReceiveIngress,
    receive_worker: JoinHandle<()>,
    /// Receive-liveness watchdog; never held across an await.
    watchdog: StdMutex<Option<JoinHandle<()>>>,
    /// One-shot history retention pass; never held across an await.
    retention: StdMutex<Option<JoinHandle<()>>>,
    /// unix ms of last listContacts title enrich (throttle hot list path)
    last_title_enrich_ms: AtomicU64,
}

impl RuntimeSupervisor {
    pub fn new(config: SignalCliConfig, store: Arc<Store>, group_id: String) -> Self {
        let (events, _) = event_channel();
        let (host_events, _) = broadcast::channel(1024);
        let service = Arc::new(Mutex::new(ConnectorService::new(store)));
        let (receive_ingress, receive_rx) = receive_channel();
        let receive_worker = tokio::spawn(receive_persistence_loop(
            receive_rx,
            service.clone(),
            host_events.clone(),
            group_id.clone(),
        ));
        Self {
            config,
            group_id,
            engine: Mutex::new(None),
            service,
            active_link_finish: Mutex::new(None),
            events,
            host_events,
            receive_ingress,
            receive_worker,
            watchdog: StdMutex::new(None),
            retention: StdMutex::new(None),
            last_title_enrich_ms: AtomicU64::new(0),
        }
    }

    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Shared service handle. The registry uses it for cross-group routing
    /// lookups and account counts over the single shared store.
    pub(crate) fn service(&self) -> &Arc<Mutex<ConnectorService>> {
        &self.service
    }

    /// Spawn the receive-liveness watchdog (idempotent). Detects a silently dead
    /// receive path — signal-cli stays alive but its server WebSocket is gone —
    /// via stderr error lines and periodic read-only pings, then restarts the engine.
    pub fn spawn_watchdog(self: &Arc<Self>) {
        let Ok(mut slot) = self.watchdog.lock() else {
            return;
        };
        if slot.is_some() {
            return;
        }
        let supervisor = Arc::clone(self);
        *slot = Some(tokio::spawn(supervisor.watchdog_loop()));
    }

    /// Apply history retention once per process start (idempotent).
    ///
    /// It runs in the background rather than on the startup path because the
    /// host only waits a few seconds for the handshake, and it takes the store
    /// lock one bounded batch at a time because inbound receives need that same
    /// lock. It does not repeat on a timer: nothing here is time-critical, and a
    /// long-lived process simply defers the rest to the next start.
    pub fn spawn_history_retention(self: &Arc<Self>) {
        let Ok(mut slot) = self.retention.lock() else {
            return;
        };
        if slot.is_some() {
            return;
        }
        let service = Arc::clone(&self.service);
        *slot = Some(tokio::spawn(async move {
            let now = crate::link::now_ms();
            let mut removed = 0_u64;
            loop {
                let outcome = service
                    .lock()
                    .await
                    .store_ref()
                    .prune_history(now, RETENTION_BATCH_MESSAGES);
                match outcome {
                    Ok(outcome) => {
                        removed += outcome.messages_deleted;
                        if outcome.messages_deleted < u64::from(RETENTION_BATCH_MESSAGES) {
                            break;
                        }
                        sleep(RETENTION_BATCH_PAUSE).await;
                    }
                    // Storage trouble surfaces on the paths the host is waiting
                    // on; retention leaves the rest for the next start.
                    Err(error) => {
                        tracing::warn!(
                            error_class = error.class(),
                            "history retention stopped: store unavailable"
                        );
                        break;
                    }
                }
            }
            if removed > 0 {
                tracing::info!(removed, "history retention removed expired messages");
            }
            // Phase 3: the one-time plaintext migration backup has its own
            // 7-day retention budget, pruned through this same once-per-start
            // batch pass.
            match service
                .lock()
                .await
                .store_ref()
                .prune_expired_plaintext_backup(now)
            {
                Ok(true) => tracing::info!("expired plaintext store backup removed"),
                Ok(false) => {}
                Err(error) => tracing::warn!(
                    error_class = error.class(),
                    "plaintext store backup cleanup failed"
                ),
            }
        }));
    }

    pub fn subscribe_engine(&self) -> broadcast::Receiver<EngineEvent> {
        self.events.subscribe()
    }

    pub fn subscribe_host(&self) -> broadcast::Receiver<HostSideEvent> {
        self.host_events.subscribe()
    }

    pub async fn status(&self) -> EngineStatus {
        self.engine
            .lock()
            .await
            .as_ref()
            .map(EngineHandle::status)
            .unwrap_or(EngineStatus {
                state: EngineState::Stopped,
                pid: None,
                rss_bytes: None,
                resource_pressure: false,
            })
    }

    pub async fn start(&self) -> Result<EngineStatus, EngineError> {
        let mut slot = self.engine.lock().await;
        if let Some(engine) = slot.as_ref() {
            if !engine.is_terminal() {
                return Err(EngineError::Backpressure);
            }
        }
        let engine = EngineHandle::start(
            self.config.clone(),
            self.events.clone(),
            self.receive_ingress.clone(),
        )
        .await?;
        let status = engine.status();
        *slot = Some(engine);
        Ok(status)
    }

    pub async fn stop(&self) -> Result<EngineStatus, EngineError> {
        let engine = self
            .engine
            .lock()
            .await
            .take()
            .ok_or(EngineError::NotRunning)?;
        self.service.lock().await.clear_link();
        *self.active_link_finish.lock().await = None;
        engine.shutdown().await?;
        Ok(EngineStatus {
            state: EngineState::Stopped,
            pid: None,
            rss_bytes: None,
            resource_pressure: false,
        })
    }

    pub async fn shutdown(&self) -> Result<(), EngineError> {
        let engine = self.engine.lock().await.take();
        self.service.lock().await.clear_link();
        *self.active_link_finish.lock().await = None;
        if let Some(engine) = engine {
            engine.shutdown().await?;
        }
        Ok(())
    }

    async fn running_engine(&self) -> Result<EngineHandle, EngineError> {
        self.engine
            .lock()
            .await
            .as_ref()
            .filter(|engine| !engine.is_terminal())
            .cloned()
            .ok_or(EngineError::NotRunning)
    }

    pub async fn list_accounts(&self) -> Result<Vec<AccountSummary>, ServiceError> {
        // Prefer live signal-cli numbers of THIS group's engine. On engine error fall
        // back to this group's store rows so a flaky listAccounts after link does not
        // hard-fail the host. When the engine is healthy and returns an empty list, do
        // NOT surface store-only "ghost" ready accounts from a partial/failed link.
        let engine = match self.running_engine().await {
            Ok(engine) => engine,
            Err(_) => {
                return self
                    .service
                    .lock()
                    .await
                    .list_accounts_in_group(&self.group_id);
            }
        };
        let numbers = match engine
            .call("listAccounts", json!({}), CallClass::ReadOnly)
            .await
        {
            Ok(result) => result
                .as_array()
                .map(|items| {
                    items
                        .iter()
                        .filter_map(|item| {
                            item.get("number")
                                .and_then(Value::as_str)
                                .map(str::to_string)
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default(),
            Err(_) => {
                return self
                    .service
                    .lock()
                    .await
                    .list_accounts_in_group(&self.group_id);
            }
        };
        if numbers.is_empty() {
            return Ok(Vec::new());
        }
        // Keep listAccounts fast: do not call listContacts here (blocks the single
        // signal-cli queue and starves conversations.list / startLink).
        // Profile names are filled on finish_link.
        let accounts = self
            .service
            .lock()
            .await
            .sync_accounts_from_numbers(&numbers, &self.group_id)?;
        let _ = engine;
        Ok(accounts)
    }

    /// Resolve a human-readable profile label for the local linked account.
    /// Never logs the phone number.
    async fn fetch_self_display_name(
        &self,
        engine: &crate::engine::EngineHandle,
        number: &str,
    ) -> Option<String> {
        if let Ok(result) = engine
            .call(
                "listContacts",
                json!({
                    "account": number,
                    "recipient": [number],
                }),
                CallClass::ReadOnly,
            )
            .await
        {
            if let Some(name) = display_name_from_contacts(&result, number) {
                return Some(name);
            }
        }
        if let Ok(result) = engine
            .call(
                "listContacts",
                json!({
                    "account": number,
                    "allRecipients": true,
                }),
                CallClass::ReadOnly,
            )
            .await
        {
            if let Some(name) = display_name_from_contacts(&result, number) {
                return Some(name);
            }
        }
        None
    }

    pub async fn start_link(&self, device_name: String) -> Result<Value, ServiceError> {
        self.service.lock().await.ensure_link_available()?;
        // Per-engine account ceiling (optimization-plan §6.4 M3.3): refuse
        // before the engine mints a QR, so no one scans a code that can never
        // finish. A store failure propagates as its own error class.
        if self
            .service
            .lock()
            .await
            .group_at_account_ceiling(&self.group_id)?
        {
            return Err(account_limit_error(&self.group_id));
        }
        let engine = self.running_engine().await?;
        let result = engine
            .call("startLink", json!({}), CallClass::ReadOnly)
            .await?;
        let device_link_uri = result
            .get("deviceLinkUri")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                ServiceError::Api(ApiError::new(
                    "UPSTREAM_PROTOCOL_ERROR",
                    "startLink response missing deviceLinkUri",
                    true,
                ))
            })?
            .to_string();
        let mut result = self
            .service
            .lock()
            .await
            .begin_link(device_name, device_link_uri)?;
        // The wire result echoes the selected group (implementation-plan §4.4);
        // this supervisor IS the selected group.
        result["proxyGroup"] = json!(self.group_id);
        tracing::info!("link session started");
        Ok(result)
    }

    pub async fn finish_link(
        &self,
        link_session_id: String,
    ) -> Result<AccountSummary, ServiceError> {
        let engine = self.running_engine().await?;
        {
            let mut active = self.active_link_finish.lock().await;
            if active.is_some() {
                return Err(ServiceError::Api(ApiError::new(
                    "LINK_IN_PROGRESS",
                    "a link finish request is already active",
                    false,
                )));
            }
            *active = Some(link_session_id.clone());
        }
        let credentials = self
            .service
            .lock()
            .await
            .peek_link_for_finish(&link_session_id);
        let (device_name, device_link_uri) = match credentials {
            Ok(credentials) => credentials,
            Err(error) => {
                let mut active = self.active_link_finish.lock().await;
                if active.as_deref() == Some(link_session_id.as_str()) {
                    *active = None;
                }
                return Err(error);
            }
        };

        // Per-engine account ceiling (M3.3): refuse before dispatching the
        // mutating finishLink, so the phone is never asked to approve a link
        // the connector would then have to reject at commit time. Session
        // errors (LINK_NOT_FOUND/LINK_EXPIRED) keep precedence.
        if self
            .service
            .lock()
            .await
            .group_at_account_ceiling(&self.group_id)?
        {
            let mut active = self.active_link_finish.lock().await;
            if active.as_deref() == Some(link_session_id.as_str()) {
                *active = None;
            }
            return Err(account_limit_error(&self.group_id));
        }

        let upstream = engine
            .call_with_timeout(
                "finishLink",
                json!({
                    "deviceLinkUri": device_link_uri,
                    "deviceName": device_name,
                }),
                CallClass::Mutating,
                LINK_FINISH_TIMEOUT,
            )
            .await;
        let still_current = {
            let mut active = self.active_link_finish.lock().await;
            if active.as_deref() == Some(link_session_id.as_str()) {
                *active = None;
                true
            } else {
                false
            }
        };
        if !still_current {
            return Err(ServiceError::Api(ApiError::new(
                "LINK_CANCELLED",
                "link session was cancelled",
                false,
            )));
        }

        let result = match upstream {
            Ok(value) => value,
            Err(EngineError::Timeout) => {
                return Err(ServiceError::Api(ApiError::new(
                    "UPSTREAM_TIMEOUT",
                    "finishLink timed out waiting for phone approval; retry after scanning",
                    true,
                )));
            }
            // finishLink is Mutating, so this means the call was in flight when
            // the engine died: the phone may already have linked the device.
            // Retrying would claim a second device slot, so the host has to
            // reconcile against the account list instead.
            Err(EngineError::UnknownOutcome) => {
                return Err(ServiceError::Api(ApiError::new(
                    "LINK_OUTCOME_UNKNOWN",
                    "finishLink outcome is unknown; check linked devices before retrying",
                    false,
                )));
            }
            Err(error) => return Err(ServiceError::Engine(error)),
        };
        let number = result
            .get("number")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                ServiceError::Api(ApiError::new(
                    "UPSTREAM_PROTOCOL_ERROR",
                    "finishLink response missing number",
                    true,
                ))
            })?;
        let mut account = {
            let mut service = self.service.lock().await;
            service.complete_link_session(&link_session_id, number, &self.group_id)?
        };
        // Refresh profile display name right after link (best-effort).
        if let Ok(engine) = self.running_engine().await {
            if let Some(name) = self.fetch_self_display_name(&engine, number).await {
                if let Ok(updated) = self
                    .service
                    .lock()
                    .await
                    .set_account_display_name(&account.id, Some(name.as_str()))
                {
                    account = updated;
                }
            }
        }
        // Best-effort initial contacts/groups sync right after link: signal-cli
        // has already pulled them from the primary device, so this only reads
        // them into the local cache. A failure must not fail the link flow.
        if let Err(error) = self.sync_contacts(&account.id).await {
            tracing::warn!(
                error_class = error.class(),
                account_id = %account.id,
                "initial contacts sync after link failed"
            );
        }
        let _ = self
            .host_events
            .send(HostSideEvent::AccountChanged(account.clone()));
        tracing::info!(account_id = %account.id, "link finished");
        Ok(account)
    }

    pub async fn cancel_link(&self, link_session_id: String) -> Result<Value, ServiceError> {
        let cancelled_in_flight = {
            let mut active = self.active_link_finish.lock().await;
            if active.as_deref() == Some(link_session_id.as_str()) {
                *active = None;
                true
            } else {
                false
            }
        };
        let result = self.service.lock().await.cancel_link(&link_session_id);
        if cancelled_in_flight {
            self.restart_engine_after_link_cancel().await?;
        }
        result
    }

    async fn restart_engine_after_link_cancel(&self) -> Result<(), EngineError> {
        self.restart_engine().await
    }

    /// Shared engine restart: shut the current engine down and start a fresh one.
    /// `EngineHandle::shutdown` is internally bounded (total timeout, then a
    /// direct child kill), so a watchdog restart cannot hang here even when the
    /// old engine's actor is parked on receive backpressure.
    async fn restart_engine(&self) -> Result<(), EngineError> {
        let mut slot = self.engine.lock().await;
        if let Some(engine) = slot.take() {
            engine.shutdown().await?;
        }
        let engine = EngineHandle::start(
            self.config.clone(),
            self.events.clone(),
            self.receive_ingress.clone(),
        )
        .await?;
        *slot = Some(engine);
        Ok(())
    }

    async fn watchdog_loop(self: Arc<Self>) {
        let mut ticker = interval(self.config.watchdog_interval.max(Duration::from_millis(1)));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        // stderr subscription follows the current engine instance (tracked by pid).
        let mut stderr_rx: Option<broadcast::Receiver<String>> = None;
        let mut stderr_pid: Option<u32> = None;
        let mut ping_failures = 0u32;
        let mut last_restart: Option<Instant> = None;
        // A restart request that arrived inside the throttle window. It must
        // not be dropped: a dead receive WebSocket produces no further stderr
        // lines, and the REST ping (getUserStatus) does not cover the receive
        // channel at all — a known structural blind spot. The pending retry is
        // the only backstop for "can send but cannot receive".
        let mut pending: Option<PendingRestart> = None;
        // Bounded retry budget per failure episode: the first ("initial")
        // restart plus at most WATCHDOG_MAX_PENDING_RETRIES further restarts.
        // Once the budget is spent the watchdog gives up until the engine
        // stays trigger-free for longer than one throttle window, which
        // resets the episode. This caps restart churn when the network is
        // genuinely dead and triggers keep arriving.
        let mut episode = WatchdogEpisode::default();
        loop {
            let engine = self.engine.lock().await.clone();
            let current_pid = engine.as_ref().and_then(|e| e.status().pid);
            if current_pid != stderr_pid {
                stderr_rx = engine
                    .as_ref()
                    .filter(|e| !e.is_terminal())
                    .map(EngineHandle::subscribe_stderr);
                stderr_pid = current_pid;
            }
            tokio::select! {
                line = recv_stderr_line(stderr_rx.as_mut()) => {
                    match line {
                        Some(line) => {
                            if is_receive_fatal_stderr(&line)
                                && self
                                    .watchdog_request_restart(
                                        &mut last_restart,
                                        &mut pending,
                                        &mut episode,
                                        RestartSource::Stderr,
                                        "receive WebSocket error on stderr",
                                    )
                                    .await
                            {
                                ping_failures = 0;
                                stderr_rx = None;
                                stderr_pid = None;
                            }
                        }
                        None => {
                            stderr_rx = None;
                            stderr_pid = None;
                        }
                    }
                }
                _ = ticker.tick() => {
                    // Episode reset: no trigger for longer than one throttle
                    // window means the engine is considered healthy again.
                    if pending.is_none()
                        && let Some(last_trigger) = episode.last_trigger
                        && last_trigger.elapsed() > self.config.watchdog_min_restart_interval
                    {
                        episode = WatchdogEpisode::default();
                    }
                    // A deferred restart fires as soon as the throttle window
                    // has passed. While a link flow is active it stays pending:
                    // restarting would invalidate an in-flight deviceLinkUri.
                    if let Some(deferred) = pending
                        && !self.restart_throttled(&last_restart)
                        && !self.link_flow_active().await
                    {
                        if episode.restarts > WATCHDOG_MAX_PENDING_RETRIES {
                            tracing::warn!(
                                reason = deferred.reason,
                                "watchdog giving up on pending restart: episode retry budget exhausted"
                            );
                            pending = None;
                        } else {
                            tracing::warn!(
                                attempt = episode.restarts,
                                reason = deferred.reason,
                                "watchdog restarting signal-cli engine (pending retry)"
                            );
                            if self.restart_engine().await.is_ok() {
                                crate::metrics::record_watchdog_restart();
                                last_restart = Some(Instant::now());
                                episode.restarts += 1;
                                pending = None;
                                ping_failures = 0;
                                stderr_rx = None;
                                stderr_pid = None;
                            }
                        }
                    }
                    let Some(engine) = engine.filter(|e| !e.is_terminal()) else {
                        continue;
                    };
                    let Some(number) = self.watchdog_ping_target().await else {
                        continue;
                    };
                    let ping = engine
                        .call_with_timeout(
                            "getUserStatus",
                            json!({
                                "account": number,
                                "recipient": [number],
                            }),
                            CallClass::ReadOnly,
                            WATCHDOG_PING_TIMEOUT,
                        )
                        .await;
                    match ping {
                        Ok(_) => {
                            ping_failures = 0;
                            // Ping recovery clears only a ping-sourced pending
                            // restart. A stderr-sourced one survives: REST
                            // liveness does not prove the receive WebSocket is
                            // alive (the blind spot above).
                            if pending.is_some_and(|p| p.source == RestartSource::Ping) {
                                tracing::info!(
                                    "watchdog pending restart cleared: receive liveness ping recovered"
                                );
                                pending = None;
                            }
                        }
                        Err(_) => {
                            ping_failures += 1;
                            if ping_failures >= WATCHDOG_MAX_PING_FAILURES
                                && self
                                    .watchdog_request_restart(
                                        &mut last_restart,
                                        &mut pending,
                                        &mut episode,
                                        RestartSource::Ping,
                                        "receive liveness ping failed repeatedly",
                                    )
                                    .await
                            {
                                ping_failures = 0;
                                stderr_rx = None;
                                stderr_pid = None;
                            }
                        }
                    }
                }
            }
        }
    }

    fn restart_throttled(&self, last_restart: &Option<Instant>) -> bool {
        last_restart.is_some_and(|last| last.elapsed() < self.config.watchdog_min_restart_interval)
    }

    async fn link_flow_active(&self) -> bool {
        self.active_link_finish.lock().await.is_some()
            || self.service.lock().await.has_pending_link()
    }

    /// Ping target account, or None when pinging/restarting is unsafe: engine not
    /// usable, a finishLink is in flight, a link session is pending (restarting
    /// would invalidate its deviceLinkUri), or no account exists yet.
    async fn watchdog_ping_target(&self) -> Option<String> {
        if self.active_link_finish.lock().await.is_some() {
            return None;
        }
        let service = self.service.lock().await;
        if service.has_pending_link() {
            return None;
        }
        // Only an account of this group's own engine is a valid probe target.
        service
            .any_signal_account_number_in_group(&self.group_id)
            .ok()
            .flatten()
    }

    /// Handle a watchdog restart trigger. Executes immediately unless a link
    /// flow is active (suppressed) or the throttle window is still open — in
    /// which case the request is recorded as pending instead of being dropped.
    /// Restarts within one failure episode are bounded: the initial restart
    /// plus at most WATCHDOG_MAX_PENDING_RETRIES retries; beyond that the
    /// trigger is logged as give-up. Any executed restart also satisfies an
    /// outstanding pending request. Returns true only when a restart happened
    /// now. Logs carry no phone numbers.
    async fn watchdog_request_restart(
        &self,
        last_restart: &mut Option<Instant>,
        pending: &mut Option<PendingRestart>,
        episode: &mut WatchdogEpisode,
        source: RestartSource,
        reason: &'static str,
    ) -> bool {
        // A quiet gap longer than one throttle window starts a fresh episode
        // with a full retry budget.
        if let Some(last_trigger) = episode.last_trigger
            && last_trigger.elapsed() > self.config.watchdog_min_restart_interval
        {
            episode.restarts = 0;
        }
        episode.last_trigger = Some(Instant::now());

        if self.link_flow_active().await {
            tracing::warn!(reason, "watchdog restart suppressed: link in progress");
            return false;
        }
        if episode.restarts > WATCHDOG_MAX_PENDING_RETRIES {
            tracing::warn!(reason, "watchdog giving up: episode retry budget exhausted");
            return false;
        }
        if self.restart_throttled(last_restart) {
            // stderr outranks ping: only an executed restart clears a
            // stderr-sourced pending, while a ping-sourced one may also be
            // cleared by ping recovery.
            match pending {
                Some(p) if p.source == RestartSource::Stderr => {}
                _ => *pending = Some(PendingRestart { source, reason }),
            }
            tracing::warn!(
                reason,
                source = %source,
                "watchdog restart throttled; recorded as pending"
            );
            return false;
        }
        let label = if episode.restarts == 0 {
            "initial".to_string()
        } else {
            format!("pending-retry {}", episode.restarts)
        };
        tracing::warn!(%label, reason, "watchdog restarting signal-cli engine");
        match self.restart_engine().await {
            Ok(()) => {
                crate::metrics::record_watchdog_restart();
                *last_restart = Some(Instant::now());
                episode.restarts += 1;
                *pending = None;
                true
            }
            Err(_) => false,
        }
    }

    /// Clear one local Signal account (desktop exit). Not remote primary unregister.
    /// Local rows are removed only after signal-cli confirms success. Unknown outcomes
    /// are surfaced without an automatic retry or local state loss.
    pub async fn delete_local_account(
        &self,
        account_id: String,
        operation_id: Option<String>,
    ) -> Result<Value, ServiceError> {
        if let Some(operation_id) = operation_id.as_deref() {
            validate_account_delete_operation_id(operation_id)?;
        }
        let (number, reconcile_first) = match operation_id.as_deref() {
            Some(operation_id) => match self
                .service
                .lock()
                .await
                .prepare_account_delete(&account_id, operation_id)?
            {
                AccountDeletePlan::Completed => return Ok(json!({})),
                AccountDeletePlan::Dispatch {
                    signal_account,
                    reconcile_first,
                } => (signal_account, reconcile_first),
            },
            None => match self
                .service
                .lock()
                .await
                .account_signal_number_optional(&account_id)?
            {
                Some(number) => (number, false),
                None => return Ok(json!({})),
            },
        };
        let engine = self.running_engine().await?;
        if reconcile_first && !signal_account_present(&engine, &number).await? {
            self.service
                .lock()
                .await
                .complete_account_delete(&account_id, operation_id.as_deref())?;
            return Ok(json!({}));
        }
        match engine
            .call_with_timeout(
                "deleteLocalAccountData",
                json!({
                    "account": number,
                    "ignoreRegistered": true,
                }),
                CallClass::Mutating,
                Duration::from_secs(60),
            )
            .await
        {
            Ok(_) => {}
            Err(EngineError::UnknownOutcome) => {
                if let Some(operation_id) = operation_id.as_deref() {
                    let _ = self
                        .service
                        .lock()
                        .await
                        .mark_account_delete_unknown(operation_id);
                }
                return Err(ServiceError::Api(ApiError::new(
                    "ACCOUNT_DELETE_OUTCOME_UNKNOWN",
                    "local account deletion has an unknown outcome",
                    false,
                )));
            }
            Err(error) => return Err(ServiceError::Engine(error)),
        }
        self.service
            .lock()
            .await
            .complete_account_delete(&account_id, operation_id.as_deref())?;
        Ok(json!({}))
    }

    pub async fn list_conversations(
        &self,
        account_id: String,
        limit: u32,
        cursor: Option<String>,
    ) -> Result<Page<ConversationSummary>, ServiceError> {
        // Best-effort title enrich only when masks remain — and at most every 60s
        // so poll/UI refresh does not hammer listContacts on the single JVM queue.
        let _ = self.enrich_conversation_titles_throttled(&account_id).await;
        self.service
            .lock()
            .await
            .list_conversations(&account_id, limit, cursor.as_deref())
    }

    /// Resolve human titles for direct chats still showing mask_address(peer).
    async fn enrich_conversation_titles_throttled(
        &self,
        account_id: &str,
    ) -> Result<(), ServiceError> {
        let peers = {
            let service = self.service.lock().await;
            service
                .store_ref()
                .list_direct_peers_needing_title(account_id)?
        };
        if peers.is_empty() {
            return Ok(());
        }
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let last = self.last_title_enrich_ms.load(Ordering::Relaxed);
        if now_ms.saturating_sub(last) < 60_000 {
            return Ok(());
        }
        self.last_title_enrich_ms.store(now_ms, Ordering::Relaxed);
        let engine = {
            let guard = self.engine.lock().await;
            guard
                .clone()
                .ok_or(ServiceError::Engine(EngineError::NotRunning))?
        };
        let number = self
            .service
            .lock()
            .await
            .account_signal_number(account_id)?;
        let result = engine
            .call(
                "listContacts",
                json!({
                    "account": number,
                    "allRecipients": true,
                }),
                CallClass::ReadOnly,
            )
            .await
            .map_err(ServiceError::Engine)?;
        let items = result.as_array().cloned().unwrap_or_default();
        let service = self.service.lock().await;
        for (peer_key, _title) in peers {
            if let Some(name) = find_contact_display_name(&items, &peer_key) {
                let _ = service
                    .store_ref()
                    .set_conversation_title_for_peer(account_id, "direct", &peer_key, &name);
            }
        }
        Ok(())
    }

    pub async fn list_messages(
        &self,
        account_id: String,
        conversation_id: String,
        limit: u32,
        before: Option<String>,
    ) -> Result<Page<MessageRecord>, ServiceError> {
        self.service.lock().await.list_messages(
            &account_id,
            &conversation_id,
            limit,
            before.as_deref(),
        )
    }

    pub async fn get_message_text(
        &self,
        account_id: String,
        conversation_id: String,
        message_id: String,
    ) -> Result<crate::service::MessageText, ServiceError> {
        self.service
            .lock()
            .await
            .get_message_text(&account_id, &conversation_id, &message_id)
    }

    pub async fn send_text(
        &self,
        account_id: String,
        target: SendTarget,
        text: String,
        client_request_id: String,
        quote_message_id: Option<String>,
    ) -> Result<MessageRecord, ServiceError> {
        let engine = self.running_engine().await?;
        let prepared = {
            let service = self.service.lock().await;
            match &target {
                SendTarget::Conversation(conversation_id) => service.prepare_send_text(
                    &account_id,
                    conversation_id,
                    &text,
                    &client_request_id,
                    quote_message_id.as_deref(),
                )?,
                SendTarget::Peer {
                    kind,
                    peer_key,
                    peer_title,
                } => service.prepare_send_text_to_peer(
                    &account_id,
                    &PeerTarget {
                        kind,
                        peer_key,
                        peer_title: peer_title.as_deref(),
                    },
                    &text,
                    &client_request_id,
                    quote_message_id.as_deref(),
                )?,
            }
        };
        self.dispatch_prepared(&engine, prepared).await
    }

    async fn dispatch_prepared(
        &self,
        engine: &EngineHandle,
        prepared: PreparedSend,
    ) -> Result<MessageRecord, ServiceError> {
        match prepared {
            PreparedSend::Existing(message) => Ok(message),
            PreparedSend::Dispatch {
                pending_id,
                account_id,
                conversation_id,
                params,
                pending_sent_at,
            } => match engine.call("send", params, CallClass::Mutating).await {
                Ok(result) => {
                    let sent_at = result
                        .get("timestamp")
                        .and_then(Value::as_u64)
                        .unwrap_or(pending_sent_at);
                    let (message, events) = self.service.lock().await.complete_send_success(
                        &pending_id,
                        &account_id,
                        &conversation_id,
                        sent_at,
                    )?;
                    for event in events {
                        let _ = self.host_events.send(event);
                    }
                    Ok(message)
                }
                Err(EngineError::UnknownOutcome) => {
                    let events = self
                        .service
                        .lock()
                        .await
                        .complete_send_unknown(&pending_id)?;
                    for event in events {
                        let _ = self.host_events.send(event);
                    }
                    Err(ServiceError::Api(ApiError::new(
                        "SEND_OUTCOME_UNKNOWN",
                        "mutating request has an unknown outcome",
                        false,
                    )))
                }
                Err(error) => {
                    let events = self
                        .service
                        .lock()
                        .await
                        .complete_send_failed(&pending_id)?;
                    for event in events {
                        let _ = self.host_events.send(event);
                    }
                    Err(ServiceError::Engine(error))
                }
            },
        }
    }

    /// Send one attachment (optionally with a caption) — implementation-plan
    /// §4.12. Identical settlement path to `send_text`: local prepare under
    /// the service lock, upstream mutating `send` call, pending row settled
    /// on confirmation / unknown outcome / failure with the same event
    /// emission and no-auto-retry discipline.
    #[allow(clippy::too_many_arguments)]
    pub async fn send_attachment(
        &self,
        account_id: String,
        target: SendTarget,
        client_request_id: String,
        data_base64: String,
        size_bytes: u64,
        filename: Option<String>,
        content_type: Option<String>,
        text: Option<String>,
        quote_message_id: Option<String>,
    ) -> Result<MessageRecord, ServiceError> {
        let engine = self.running_engine().await?;
        let prepared = {
            let service = self.service.lock().await;
            let attachment_target = match &target {
                SendTarget::Conversation(conversation_id) => {
                    AttachmentSendTarget::Conversation(conversation_id)
                }
                SendTarget::Peer {
                    kind,
                    peer_key,
                    peer_title,
                } => AttachmentSendTarget::Peer(PeerTarget {
                    kind,
                    peer_key,
                    peer_title: peer_title.as_deref(),
                }),
            };
            service.prepare_send_attachment(
                &account_id,
                &attachment_target,
                &client_request_id,
                &data_base64,
                size_bytes,
                filename.as_deref(),
                content_type.as_deref(),
                text.as_deref(),
                quote_message_id.as_deref(),
            )?
        };
        self.dispatch_prepared(&engine, prepared).await
    }

    /// Best-effort "Delete for everyone" for one own sent message
    /// (docs/remote-delete-l2-plan.md): local prepare under the service lock,
    /// upstream `remoteDelete` call without it — the same shape as
    /// `send_text`. No local row is created or changed and no event is
    /// emitted; an indeterminate mutating outcome maps to the explicit
    /// `{"status":"unknown"}` response, never to an automatic retry.
    pub async fn remote_delete(
        &self,
        account_id: String,
        conversation_id: String,
        message_id: String,
    ) -> Result<&'static str, ServiceError> {
        let engine = self.running_engine().await?;
        let prepared = {
            let service = self.service.lock().await;
            service.prepare_remote_delete(&account_id, &conversation_id, &message_id)?
        };
        match engine
            .call("remoteDelete", prepared.params, CallClass::Mutating)
            .await
        {
            Ok(_) => Ok("deleted"),
            Err(EngineError::UnknownOutcome) => Ok("unknown"),
            Err(error) => Err(error.into()),
        }
    }

    /// Best-effort reaction add/remove for one message with a resolvable
    /// protocol identity (contract revision 1.7): local prepare under the
    /// service lock, upstream `sendReaction` call without it — the same shape
    /// as `send_text` and `remote_delete`. No local row is created or changed
    /// and no event is emitted; an indeterminate mutating outcome maps to the
    /// explicit `{"status":"unknown"}` response, never to an automatic retry.
    pub async fn send_reaction(
        &self,
        account_id: String,
        conversation_id: String,
        message_id: String,
        emoji: String,
        remove: bool,
    ) -> Result<&'static str, ServiceError> {
        let engine = self.running_engine().await?;
        let prepared = {
            let service = self.service.lock().await;
            service.prepare_send_reaction(
                &account_id,
                &conversation_id,
                &message_id,
                &emoji,
                remove,
            )?
        };
        match engine
            .call("sendReaction", prepared.params, CallClass::Mutating)
            .await
        {
            Ok(_) => Ok("sent"),
            Err(EngineError::UnknownOutcome) => Ok("unknown"),
            Err(error) => Err(error.into()),
        }
    }

    /// Media PoC (contract revision 1.8): read one already-downloaded
    /// attachment as base64 through the upstream `getAttachment` local read —
    /// no download, no mutation, no local write, no event. The declared
    /// `sizeBytes` is re-verified against the returned payload before it
    /// reaches the host (implementation-plan §4.7); a mismatch answers
    /// INVALID_REQUEST instead of shipping an unbudgeted payload.
    pub async fn get_attachment(
        &self,
        account_id: String,
        conversation_id: String,
        message_id: String,
        attachment_id: String,
        size_bytes: u64,
    ) -> Result<AttachmentPayload, ServiceError> {
        let engine = self.running_engine().await?;
        let prepared = {
            let service = self.service.lock().await;
            service.prepare_get_attachment(
                &account_id,
                &conversation_id,
                &message_id,
                &attachment_id,
                size_bytes,
            )?
        };
        let result = engine
            .call("getAttachment", prepared.params, CallClass::ReadOnly)
            .await?;
        // The upstream jsonRpc response is {"data": "<base64>"}
        // (JsonAttachmentData); anything else is an upstream protocol error.
        let data = result
            .get("data")
            .and_then(Value::as_str)
            .ok_or(EngineError::Protocol)?;
        let payload = AttachmentPayload {
            attachment_id: prepared.attachment_id,
            data: data.to_string(),
        };
        validate_attachment_payload(&payload.data, size_bytes)?;
        Ok(payload)
    }

    /// Read-only view of the contacts cache; never triggers an upstream call.
    pub async fn list_contacts(
        &self,
        account_id: String,
        query: Option<String>,
        limit: u32,
        cursor: Option<String>,
    ) -> Result<Page<ContactSummary>, ServiceError> {
        self.service.lock().await.list_contacts(
            &account_id,
            query.as_deref(),
            limit,
            cursor.as_deref(),
        )
    }

    /// contacts.setLocalAlias (contract revision 1.10, implementation-plan
    /// §4.9): rename one contact via the mutating upstream `updateContact`.
    /// The alias lives in upstream account data — no local row is written and
    /// no event is emitted; the next contacts.sync brings the new name into
    /// the cache. An indeterminate mutating outcome answers "unknown" and is
    /// never retried automatically.
    pub async fn set_local_alias(
        &self,
        account_id: String,
        peer_key: String,
        alias: String,
    ) -> Result<&'static str, ServiceError> {
        let engine = self.running_engine().await?;
        let params = {
            let service = self.service.lock().await;
            service.prepare_set_local_alias(&account_id, &peer_key, &alias)?
        };
        match engine
            .call("updateContact", params, CallClass::Mutating)
            .await
        {
            Ok(_) => Ok("updated"),
            Err(EngineError::UnknownOutcome) => Ok("unknown"),
            Err(error) => Err(error.into()),
        }
    }

    /// presence.setTypingMessage (contract revision 1.11, implementation-plan
    /// §4.10): fire a typing indicator via the mutating upstream `sendTyping`.
    /// The indicator is ephemeral upstream state — no local row is written and
    /// no event is emitted. An indeterminate mutating outcome answers
    /// "unknown" and is never retried automatically.
    pub async fn set_typing_message(
        &self,
        account_id: String,
        conversation_id: String,
        stop: bool,
    ) -> Result<&'static str, ServiceError> {
        let engine = self.running_engine().await?;
        let params = {
            let service = self.service.lock().await;
            service.prepare_set_typing_message(&account_id, &conversation_id, stop)?
        };
        match engine.call("sendTyping", params, CallClass::Mutating).await {
            Ok(_) => Ok("sent"),
            Err(EngineError::UnknownOutcome) => Ok("unknown"),
            Err(error) => Err(error.into()),
        }
    }

    /// Read-only projection of one cached group row (§4.8); served from the
    /// contacts cache with no upstream call, so it answers even while the
    /// engine is stopped.
    pub async fn get_group(
        &self,
        account_id: String,
        group_key: String,
    ) -> Result<GroupDetails, ServiceError> {
        self.service.lock().await.get_group(&account_id, &group_key)
    }

    /// Pull the contacts/groups signal-cli already synced from the primary
    /// device into the local cache. Both calls are ReadOnly on the single JVM
    /// queue, so they cannot starve or corrupt mutating sends. A successful
    /// sync less than 60s old short-circuits to the cached counts.
    pub async fn sync_contacts(
        &self,
        account_id: &str,
    ) -> Result<ContactsSyncOutcome, ServiceError> {
        let now_ms = crate::link::now_ms();
        {
            let service = self.service.lock().await;
            if let Some(synced_at) = service.contacts_synced_at(account_id)? {
                if now_ms.saturating_sub(synced_at) < 60_000 {
                    let (contact_count, group_count) = service.count_contacts(account_id)?;
                    return Ok(ContactsSyncOutcome {
                        contact_count,
                        group_count,
                        synced_at,
                    });
                }
            }
        }
        let number = self
            .service
            .lock()
            .await
            .account_signal_number(account_id)?;
        let engine = self.running_engine().await?;
        // Contacts registered on the account only (no allRecipients walk).
        let contacts_result = engine
            .call(
                "listContacts",
                json!({ "account": number }),
                CallClass::ReadOnly,
            )
            .await
            .map_err(ServiceError::Engine)?;
        let groups_result = engine
            .call(
                "listGroups",
                json!({ "account": number }),
                CallClass::ReadOnly,
            )
            .await
            .map_err(ServiceError::Engine)?;

        let mut synced: Vec<(String, String, String, Option<String>)> = Vec::new();
        let mut contact_count = 0_u64;
        let mut group_count = 0_u64;
        for item in contacts_result.as_array().cloned().unwrap_or_default() {
            let peer_key = ["number", "uuid", "numberUuid"]
                .iter()
                .find_map(|field| item.get(field).and_then(Value::as_str))
                .map(str::trim)
                .filter(|value| !value.is_empty());
            let Some(peer_key) = peer_key else {
                continue;
            };
            let title = compose_contact_display_name(&item)
                .unwrap_or_else(|| crate::ids::mask_address(peer_key));
            synced.push(("contact".to_string(), peer_key.to_string(), title, None));
            contact_count += 1;
        }
        for item in groups_result.as_array().cloned().unwrap_or_default() {
            // Only groups the linked account is still a member of.
            if item.get("isMember").and_then(Value::as_bool) != Some(true) {
                continue;
            }
            let Some(peer_key) = item
                .get("id")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
            else {
                continue;
            };
            let title = item
                .get("name")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(|value| value.chars().take(64).collect::<String>())
                .unwrap_or_else(|| "group".to_string());
            let extra = item
                .get("members")
                .and_then(Value::as_array)
                .map(|members| json!({ "memberCount": members.len() }).to_string());
            synced.push(("group".to_string(), peer_key.to_string(), title, extra));
            group_count += 1;
        }

        let entries: Vec<SyncedContact<'_>> = synced
            .iter()
            .map(|(kind, peer_key, title, extra)| SyncedContact {
                kind: kind.as_str(),
                peer_key: peer_key.as_str(),
                title: title.as_str(),
                extra: extra.as_deref(),
            })
            .collect();
        // One transaction for the whole batch plus the sync marker: the store
        // lock is held once instead of per contact, and a partial sync is
        // never visible (a failed entry rolls back the rows and the marker).
        self.service
            .lock()
            .await
            .upsert_synced_contacts(account_id, &entries, now_ms)?;
        Ok(ContactsSyncOutcome {
            contact_count,
            group_count,
            synced_at: now_ms,
        })
    }
}

impl Drop for RuntimeSupervisor {
    fn drop(&mut self) {
        self.receive_worker.abort();
        if let Ok(mut slot) = self.watchdog.lock()
            && let Some(handle) = slot.take()
        {
            handle.abort();
        }
        if let Ok(mut slot) = self.retention.lock()
            && let Some(handle) = slot.take()
        {
            handle.abort();
        }
    }
}

/// Receive one stderr line, skipping lag gaps; None means the stream closed.
/// Pends forever when there is no subscription, so select! can ignore the branch.
async fn recv_stderr_line(rx: Option<&mut broadcast::Receiver<String>>) -> Option<String> {
    match rx {
        Some(rx) => loop {
            match rx.recv().await {
                Ok(line) => return Some(line),
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        },
        None => std::future::pending().await,
    }
}

/// Heuristic over signal-cli stderr logs: true when the line indicates the
/// receive WebSocket to the Signal server is dead (the silent-failure signature
/// this watchdog exists for, since signal-cli does not reconnect on its own).
/// Matches "websocketioexception" directly, or "websocket" together with a
/// failure keyword; normal INFO logs must not match.
fn is_receive_fatal_stderr(line: &str) -> bool {
    let lower = line.to_lowercase();
    lower.contains("websocketioexception")
        || (lower.contains("websocket")
            && ["error", "closed", "disconnected", "failed", "exception"]
                .iter()
                .any(|needle| lower.contains(needle)))
}

async fn receive_persistence_loop(
    mut receives: mpsc::Receiver<QueuedReceive>,
    service: Arc<Mutex<ConnectorService>>,
    host_events: broadcast::Sender<HostSideEvent>,
    owner_group: String,
) {
    let mut storage_unavailable = false;
    while let Some(queued) = receives.recv().await {
        crate::metrics::receive_queue_drained();
        let mut retry_delay = RECEIVE_STORE_RETRY_MIN;
        loop {
            let result = service
                .lock()
                .await
                .ingest_receive(queued.receive().clone(), &owner_group);
            match result {
                Ok(events) => {
                    for event in events {
                        let _ = host_events.send(event);
                    }
                    if storage_unavailable {
                        storage_unavailable = false;
                        tracing::info!(state = "recovered", "receive persistence recovered");
                        let _ =
                            host_events.send(HostSideEvent::StorageChanged { state: "recovered" });
                    }
                    break;
                }
                Err(error) => {
                    if !storage_unavailable {
                        storage_unavailable = true;
                        // Classification only: the store error may carry a
                        // source chain now, and none of it is logged.
                        tracing::warn!(
                            state = "unavailable",
                            error_class = error.class(),
                            "receive persistence failed; retrying"
                        );
                        let _ = host_events.send(HostSideEvent::StorageChanged {
                            state: "unavailable",
                        });
                    }
                    sleep(retry_delay).await;
                    retry_delay = retry_delay.saturating_mul(2).min(RECEIVE_STORE_RETRY_MAX);
                }
            }
        }
    }
}

async fn signal_account_present(
    engine: &EngineHandle,
    signal_account: &str,
) -> Result<bool, ServiceError> {
    for _ in 0..2 {
        let result = engine
            .call("listAccounts", json!({}), CallClass::ReadOnly)
            .await?;
        let accounts = result
            .as_array()
            .ok_or(ServiceError::Engine(EngineError::Protocol))?;
        if accounts
            .iter()
            .any(|account| account.get("number").and_then(Value::as_str) == Some(signal_account))
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn find_contact_display_name(items: &[Value], peer_key: &str) -> Option<String> {
    let item = items.iter().find(|entry| {
        let number = entry.get("number").and_then(Value::as_str);
        let uuid = entry.get("uuid").and_then(Value::as_str);
        let number_uuid = entry.get("numberUuid").and_then(Value::as_str);
        number == Some(peer_key) || uuid == Some(peer_key) || number_uuid == Some(peer_key)
    })?;
    compose_contact_display_name(item)
}

fn display_name_from_contacts(result: &Value, number: &str) -> Option<String> {
    let items = result.as_array()?;
    let item = items
        .iter()
        .find(|entry| {
            entry
                .get("number")
                .and_then(Value::as_str)
                .is_some_and(|n| n == number)
        })
        .or_else(|| items.first())?;
    compose_contact_display_name(item)
}

/// Build a UI label from signal-cli listContacts JSON without logging numbers.
fn compose_contact_display_name(item: &Value) -> Option<String> {
    let profile = item.get("profile");
    let given = profile
        .and_then(|p| p.get("givenName"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let family = profile
        .and_then(|p| p.get("familyName"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let profile_name = match (given, family) {
        (Some(g), Some(f)) => Some(format!("{g} {f}")),
        (Some(g), None) => Some(g.to_string()),
        (None, Some(f)) => Some(f.to_string()),
        _ => None,
    };
    let name = item
        .get("name")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    let nick = item
        .get("nickName")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    let username = item
        .get("username")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    profile_name
        .or(name)
        .or(nick)
        .or(username)
        .map(|s| s.chars().take(64).collect())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::Duration;

    use rusqlite::Connection;
    use serde_json::json;
    use std::sync::Arc;
    use tempfile::TempDir;

    use tokio::time::{sleep, timeout};

    use super::{RuntimeSupervisor, compose_contact_display_name, is_receive_fatal_stderr};
    use crate::DEFAULT_PROXY_GROUP_ID;
    use crate::engine::{NormalizedReceive, SignalCliConfig};
    use crate::service::HostSideEvent;
    use crate::store::{MessageRecord, Store, StoreKey};

    /// Every test store is encrypted (Phase 3); secondary observer connections
    /// to the same file must present the same key.
    const TEST_KEY_BYTES: [u8; 32] = [0x5A; 32];

    fn test_store(dir: &std::path::Path) -> Store {
        Store::open(dir, Some(StoreKey::from_bytes(TEST_KEY_BYTES))).unwrap()
    }

    fn keyed_observer(store: &Store) -> Connection {
        let conn = Connection::open(store.path()).unwrap();
        conn.execute_batch(&format!(
            "PRAGMA key = \"x'{}'\"",
            hex::encode(TEST_KEY_BYTES)
        ))
        .unwrap();
        conn
    }

    #[test]
    fn prefers_profile_given_and_family_name() {
        let item = json!({
            "number": "+15555550100",
            "name": "Contact",
            "username": "user.01",
            "profile": { "givenName": "Ada", "familyName": "Lovelace" }
        });
        assert_eq!(
            compose_contact_display_name(&item).as_deref(),
            Some("Ada Lovelace")
        );
    }

    #[tokio::test]
    async fn history_retention_runs_once_per_start_in_the_background() {
        let temp = TempDir::new().unwrap();
        let store = test_store(temp.path());
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let conversation = store
            .ensure_conversation(&account.id, "direct", "+15555550101", "contact")
            .unwrap();
        let expired_at = crate::link::now_ms() - 400 * 24 * 60 * 60 * 1_000;
        let mut expired = MessageRecord {
            id: "expired".into(),
            account_id: account.id.clone(),
            conversation_id: conversation.id.clone(),
            direction: "incoming",
            sender_id: "peer".into(),
            sent_at: expired_at,
            received_at: Some(expired_at),
            text: Some("body".into()),
            text_bytes: Some(4),
            text_truncated: false,
            text_retrievable: true,
            status: "delivered",
            client_request_id: None,
            quote_message_id: None,
        };
        store
            .insert_message(&expired, None, Some("body"), true)
            .unwrap();
        let observer = keyed_observer(&store);
        let backdate = |id: &str| {
            observer
                .execute(
                    "UPDATE messages SET stored_at=?2 WHERE id=?1",
                    rusqlite::params![id, expired_at as i64],
                )
                .unwrap();
        };
        backdate("expired");
        let stored = || {
            observer
                .query_row("SELECT COUNT(*) FROM messages", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap()
        };
        let supervisor = Arc::new(RuntimeSupervisor::new(
            SignalCliConfig::new(
                temp.path().join("unused-signal-cli"),
                temp.path().join("unused-signal-data"),
            ),
            Arc::new(store),
            DEFAULT_PROXY_GROUP_ID.to_string(),
        ));

        supervisor.spawn_history_retention();
        timeout(Duration::from_secs(2), async {
            while stored() != 0 {
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("retention should drop expired history");

        // Asking again is a no-op for the life of the process.
        expired.id = "expired-later".into();
        supervisor
            .service
            .lock()
            .await
            .store_ref()
            .insert_message(&expired, None, Some("body"), true)
            .unwrap();
        backdate("expired-later");
        supervisor.spawn_history_retention();
        sleep(Duration::from_millis(200)).await;
        assert_eq!(stored(), 1);
    }

    #[tokio::test]
    async fn failed_receive_persistence_is_retained_and_recovers() {
        let temp = TempDir::new().unwrap();
        let store = test_store(temp.path());
        store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let recovery = keyed_observer(&store);
        recovery
            .execute_batch(
                "CREATE TRIGGER fail_receive_insert
                 BEFORE INSERT ON messages
                 BEGIN SELECT RAISE(FAIL, 'controlled test failure'); END;",
            )
            .unwrap();
        let supervisor = RuntimeSupervisor::new(
            SignalCliConfig::new(
                temp.path().join("unused-signal-cli"),
                temp.path().join("unused-signal-data"),
            ),
            Arc::new(store),
            DEFAULT_PROXY_GROUP_ID.to_string(),
        );
        let mut host_events = supervisor.subscribe_host();
        supervisor
            .receive_ingress
            .enqueue(NormalizedReceive {
                timestamp: Some(42),
                content_kind: "dataMessage",
                direction: "incoming",
                account_present: true,
                account: Some("+15555550100".into()),
                source: Some("+15555550101".into()),
                peer_name: Some("Peer".into()),
                group_id: None,
                text: Some("persist me".into()),
                text_bytes: Some(10),
                text_truncated: false,
            })
            .await
            .unwrap();

        let unavailable = timeout(Duration::from_secs(1), host_events.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            unavailable,
            HostSideEvent::StorageChanged {
                state: "unavailable"
            }
        ));
        recovery
            .execute_batch("DROP TRIGGER fail_receive_insert;")
            .unwrap();

        let (saw_message, saw_recovered) = timeout(Duration::from_secs(2), async {
            let mut saw_message = false;
            loop {
                match host_events.recv().await.unwrap() {
                    HostSideEvent::MessageUpserted(message) => {
                        saw_message = message.text.as_deref() == Some("persist me");
                    }
                    HostSideEvent::StorageChanged { state: "recovered" } => {
                        break (saw_message, true);
                    }
                    _ => {}
                }
            }
        })
        .await
        .unwrap();
        assert!(saw_message);
        assert!(saw_recovered);
    }

    #[test]
    fn falls_back_to_username() {
        let item = json!({
            "number": "+15555550100",
            "username": "signal.user",
            "profile": {}
        });
        assert_eq!(
            compose_contact_display_name(&item).as_deref(),
            Some("signal.user")
        );
    }

    #[test]
    fn fatal_stderr_matches_receive_websocket_failures() {
        assert!(is_receive_fatal_stderr(
            "WARN WebSocketConnection - WebSocket connection closed unexpectedly"
        ));
        assert!(is_receive_fatal_stderr(
            "ERROR o.a.s.manager.internal.ReceiveConfig - websocket error while receiving"
        ));
        assert!(is_receive_fatal_stderr(
            "org.asamk.signal.manager.WebSocketIOException: Connection reset"
        ));
        assert!(is_receive_fatal_stderr(
            "websocket disconnected from server"
        ));
        assert!(is_receive_fatal_stderr(
            "Failed to connect websocket, retrying"
        ));
        assert!(is_receive_fatal_stderr(
            "Exception in websocket reader thread"
        ));
    }

    #[test]
    fn fatal_stderr_ignores_normal_log_lines() {
        assert!(!is_receive_fatal_stderr(
            "INFO App - Starting signal-cli daemon"
        ));
        assert!(!is_receive_fatal_stderr(
            "INFO WebSocketConnection - Connected successfully"
        ));
        assert!(!is_receive_fatal_stderr(
            "INFO ManagerImpl - Checking for new messages"
        ));
        assert!(!is_receive_fatal_stderr(
            "WARN Scheduler - task failed to start" // no websocket context
        ));
        assert!(!is_receive_fatal_stderr(""));
    }

    /// The engine runs the real fake-signal-cli fixture (the same one the
    /// engine-integration suite drives): its `remoteDelete` handler exits with
    /// the mutating call in flight, so the upstream result is lost mid-call.
    /// The supervisor must surface that as the explicit `unknown` response —
    /// not a retryable error — and never re-issue the call.
    #[tokio::test]
    async fn remote_delete_with_a_lost_upstream_result_answers_unknown() {
        let temp = TempDir::new().unwrap();
        let store =
            Arc::new(Store::open(temp.path(), Some(StoreKey::from_bytes(TEST_KEY_BYTES))).unwrap());
        let mut config = SignalCliConfig::new(
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fake-signal-cli.py"),
            temp.path().join("signal-data"),
        );
        config.request_timeout = Duration::from_secs(3);
        config.shutdown_grace = Duration::from_millis(100);
        let supervisor = Arc::new(RuntimeSupervisor::new(
            config,
            store,
            DEFAULT_PROXY_GROUP_ID.to_string(),
        ));
        supervisor.start().await.unwrap();
        let account = supervisor
            .service
            .lock()
            .await
            .sync_accounts_from_numbers(&["+15555550100".into()], DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let conversation = supervisor
            .service
            .lock()
            .await
            .store_ref()
            .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
            .unwrap();
        let pending_id = match supervisor
            .service
            .lock()
            .await
            .prepare_send_text(
                &account.id,
                &conversation.id,
                "vanish",
                "req-rd-crash",
                None,
            )
            .unwrap()
        {
            crate::service::PreparedSend::Dispatch { pending_id, .. } => pending_id,
            crate::service::PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        };
        supervisor
            .service
            .lock()
            .await
            .complete_send_success(&pending_id, &account.id, &conversation.id, 421)
            .unwrap();

        assert_eq!(
            supervisor
                .remote_delete(
                    account.id.clone(),
                    conversation.id.clone(),
                    pending_id.clone()
                )
                .await
                .unwrap(),
            "unknown",
            "a lost mutating result must answer unknown, never retry"
        );

        // The local row is untouched and no statusChanged event was emitted:
        // remoteDelete changes nothing locally (plan §3.6).
        let row = supervisor
            .service
            .lock()
            .await
            .store_ref()
            .message_by_id(&account.id, &conversation.id, &pending_id)
            .unwrap()
            .unwrap();
        assert_eq!(row.status, "sent");
        assert_eq!(row.sent_at, 421);
        supervisor.shutdown().await.unwrap();
    }

    /// The engine runs the real fake-signal-cli fixture: its `sendReaction`
    /// handler exits with the mutating call in flight (targetTimestamp 423),
    /// so the upstream result is lost mid-call. The supervisor must surface
    /// that as the explicit `unknown` response — not a retryable error — and
    /// never re-issue the call. The local row is untouched: reactions change
    /// nothing locally (contract revision 1.7).
    #[tokio::test]
    async fn send_reaction_with_a_lost_upstream_result_answers_unknown() {
        let temp = TempDir::new().unwrap();
        let store =
            Arc::new(Store::open(temp.path(), Some(StoreKey::from_bytes(TEST_KEY_BYTES))).unwrap());
        let mut config = SignalCliConfig::new(
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fake-signal-cli.py"),
            temp.path().join("signal-data"),
        );
        config.request_timeout = Duration::from_secs(3);
        config.shutdown_grace = Duration::from_millis(100);
        let supervisor = Arc::new(RuntimeSupervisor::new(
            config,
            store,
            DEFAULT_PROXY_GROUP_ID.to_string(),
        ));
        supervisor.start().await.unwrap();
        let account = supervisor
            .service
            .lock()
            .await
            .sync_accounts_from_numbers(&["+15555550100".into()], DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let conversation = supervisor
            .service
            .lock()
            .await
            .store_ref()
            .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
            .unwrap();
        let pending_id = match supervisor
            .service
            .lock()
            .await
            .prepare_send_text(
                &account.id,
                &conversation.id,
                "react to me",
                "req-sr-crash",
                None,
            )
            .unwrap()
        {
            crate::service::PreparedSend::Dispatch { pending_id, .. } => pending_id,
            crate::service::PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        };
        supervisor
            .service
            .lock()
            .await
            .complete_send_success(&pending_id, &account.id, &conversation.id, 423)
            .unwrap();

        assert_eq!(
            supervisor
                .send_reaction(
                    account.id.clone(),
                    conversation.id.clone(),
                    pending_id.clone(),
                    "👍".to_string(),
                    false
                )
                .await
                .unwrap(),
            "unknown",
            "a lost mutating result must answer unknown, never retry"
        );

        // The local row is untouched and no statusChanged event was emitted:
        // sendReaction changes nothing locally (contract revision 1.7).
        let row = supervisor
            .service
            .lock()
            .await
            .store_ref()
            .message_by_id(&account.id, &conversation.id, &pending_id)
            .unwrap()
            .unwrap();
        assert_eq!(row.status, "sent");
        assert_eq!(row.sent_at, 423);
        supervisor.shutdown().await.unwrap();
    }

    /// The fixture's `updateContact` handler exits with the mutating call in
    /// flight (alias "[fixture-crash-alias]"), so the upstream result is lost
    /// mid-call. setLocalAlias must answer the explicit `unknown` — never a
    /// retryable error, never an automatic retry — and write nothing locally
    /// (contract revision 1.10).
    #[tokio::test]
    async fn set_local_alias_with_a_lost_upstream_result_answers_unknown() {
        let temp = TempDir::new().unwrap();
        let store =
            Arc::new(Store::open(temp.path(), Some(StoreKey::from_bytes(TEST_KEY_BYTES))).unwrap());
        let mut config = SignalCliConfig::new(
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fake-signal-cli.py"),
            temp.path().join("signal-data"),
        );
        config.request_timeout = Duration::from_secs(3);
        config.shutdown_grace = Duration::from_millis(100);
        let supervisor = Arc::new(RuntimeSupervisor::new(
            config,
            store,
            DEFAULT_PROXY_GROUP_ID.to_string(),
        ));
        supervisor.start().await.unwrap();
        let account = supervisor
            .service
            .lock()
            .await
            .sync_accounts_from_numbers(&["+15555550100".into()], DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let conversation = supervisor
            .service
            .lock()
            .await
            .store_ref()
            .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
            .unwrap();

        assert_eq!(
            supervisor
                .set_local_alias(
                    account.id.clone(),
                    conversation.peer_key.clone(),
                    "[fixture-crash-alias]".to_string()
                )
                .await
                .unwrap(),
            "unknown",
            "a lost mutating result must answer unknown, never retry"
        );

        // The direct conversation row is untouched by the rename: the alias
        // lives in upstream account data and returns via contacts.sync.
        let row = supervisor
            .service
            .lock()
            .await
            .store_ref()
            .conversation_by_id(&account.id, &conversation.id)
            .unwrap()
            .unwrap();
        assert_eq!(row.peer_key, "+15555550101");
        supervisor.shutdown().await.unwrap();
    }

    /// The fixture's `sendTyping` handler exits with the mutating call in
    /// flight when the recipient is the magic peer "+15555550999", so the
    /// upstream result is lost mid-call. setTypingMessage must answer the
    /// explicit `unknown` — never a retryable error, never an automatic
    /// retry — and write nothing locally (contract revision 1.11).
    #[tokio::test]
    async fn set_typing_message_with_a_lost_upstream_result_answers_unknown() {
        let temp = TempDir::new().unwrap();
        let store =
            Arc::new(Store::open(temp.path(), Some(StoreKey::from_bytes(TEST_KEY_BYTES))).unwrap());
        let mut config = SignalCliConfig::new(
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fake-signal-cli.py"),
            temp.path().join("signal-data"),
        );
        config.request_timeout = Duration::from_secs(3);
        config.shutdown_grace = Duration::from_millis(100);
        let supervisor = Arc::new(RuntimeSupervisor::new(
            config,
            store,
            DEFAULT_PROXY_GROUP_ID.to_string(),
        ));
        supervisor.start().await.unwrap();
        let account = supervisor
            .service
            .lock()
            .await
            .sync_accounts_from_numbers(&["+15555550100".into()], DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let conversation = supervisor
            .service
            .lock()
            .await
            .store_ref()
            .ensure_conversation(&account.id, "direct", "+15555550999", "Crash Peer")
            .unwrap();

        assert_eq!(
            supervisor
                .set_typing_message(account.id.clone(), conversation.id.clone(), false)
                .await
                .unwrap(),
            "unknown",
            "a lost mutating result must answer unknown, never retry"
        );

        // No local row changes: the indicator is ephemeral upstream state.
        let row = supervisor
            .service
            .lock()
            .await
            .store_ref()
            .conversation_by_id(&account.id, &conversation.id)
            .unwrap()
            .unwrap();
        assert_eq!(row.peer_key, "+15555550999");
        supervisor.shutdown().await.unwrap();
    }
}
