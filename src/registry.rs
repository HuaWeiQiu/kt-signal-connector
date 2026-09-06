// SPDX-License-Identifier: AGPL-3.0-only

//! Multi-group runtime facade (ADR 0001 R6-R9): one connector process, one
//! supervised signal-cli engine per launcher-defined proxy group. Sibling
//! connector processes on the same host are legal under the ADR 0002
//! namespace rules — disjoint endpoints and data directories (enforced by
//! the startup occupancy lock in `crate::datalock`) plus a per-instance
//! state-dir store; nothing inside this facade assumes it is the only
//! connector on the machine.
//!
//! The facade owns every [`RuntimeSupervisor`] in launcher configuration order
//! and is the only surface the host protocol talks to. Its responsibilities,
//! pinned by implementation-plan §4.4:
//!
//! - `runtime.*` results carry the pre-Phase-4 top-level aggregate plus the
//!   additive `proxyGroups[]` array. The aggregation rule is fixed: `running`
//!   if any group runs, else `faulted`, else `exited`, else `stopped`;
//!   `rssBytes` sums live samples; pressure ORs across groups; the top-level
//!   `pid` exists only when exactly one group exists — so a single-group
//!   deployment is field-for-field identical to Phase 3.
//! - Account-addressed methods route by the account's stored binding.
//! - Link flows are per group (`LINK_IN_PROGRESS` and restarts never cross
//!   group boundaries); unknown groups fail closed with
//!   `PROXY_GROUP_NOT_FOUND`.
//! - Per-group engine transitions fan out as `proxyGroup.stateChanged` while
//!   `runtime.stateChanged` keeps carrying the aggregate shape unchanged.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex as StdMutex};

use serde::Serialize;
use serde_json::{Value, json};
use tokio::sync::{Mutex, broadcast};
use tokio::task::JoinHandle;

use crate::DEFAULT_PROXY_GROUP_ID;
use crate::engine::{
    EngineError, EngineEvent, EngineState, EngineStatus, SignalCliConfig, SignalCliMode,
};
use crate::groups::{MAX_PROXY_GROUPS, ProxyGroupPlan};
use crate::protocol::ApiError;
use crate::service::{ContactsSyncOutcome, HostSideEvent, MessageText, SendTarget, ServiceError};
use crate::store::{
    AccountSummary, ContactSummary, ConversationSummary, MessageRecord, Page, Store, StoreError,
    StoreKey,
};
use crate::supervisor::RuntimeSupervisor;

/// One group slot: opaque id plus its full supervision stack.
pub struct ProxyGroupSlot {
    id: String,
    supervisor: Arc<RuntimeSupervisor>,
}

impl ProxyGroupSlot {
    /// Assemble one slot from an already-built supervision stack. Production
    /// plans come exclusively from [`open_group_runtime`]; this constructor
    /// exists for unit tests that build single-group runtimes by hand.
    #[cfg(test)]
    pub(crate) fn new(id: String, supervisor: Arc<RuntimeSupervisor>) -> Self {
        Self { id, supervisor }
    }
}

/// Events merged from every group's supervision stack, already tagged and
/// aggregated for the wire.
#[derive(Clone, Debug)]
pub enum RegistryEvent {
    /// Top-level aggregate for `runtime.stateChanged` (shape unchanged).
    AggregateStateChanged(AggregateEngineStatus),
    /// Per-group detail for `proxyGroup.stateChanged`.
    GroupStateChanged {
        group_id: String,
        status: EngineStatus,
    },
    ProtocolWarning {
        kind: &'static str,
    },
    ResourcePressure {
        state: crate::resource::ResourcePressureState,
        pid: u32,
        rss_bytes: u64,
        group_id: String,
    },
    StorageChanged {
        state: &'static str,
    },
}

/// Pre-Phase-4 top-level runtime shape (`runtime.stateChanged` keeps exactly
/// this, and `runtime.status` flattens it before adding `proxyGroups`).
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AggregateEngineStatus {
    pub state: EngineState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rss_bytes: Option<u64>,
    pub resource_pressure: bool,
}

impl From<EngineStatus> for AggregateEngineStatus {
    fn from(status: EngineStatus) -> Self {
        Self {
            state: status.state,
            pid: status.pid,
            rss_bytes: status.rss_bytes,
            resource_pressure: status.resource_pressure,
        }
    }
}

/// One `proxyGroups[]` entry (implementation-plan §4.4).
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProxyGroupStatus {
    pub group_id: String,
    pub state: EngineState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rss_bytes: Option<u64>,
    pub resource_pressure: bool,
    /// Accounts bound to the group in the shared store.
    pub account_count: u64,
}

/// Result of `runtime.status` / `runtime.start` / `runtime.stop`: the pinned
/// top-level aggregate plus the additive group array.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeStatusResult {
    #[serde(flatten)]
    pub aggregate: AggregateEngineStatus,
    pub proxy_groups: Vec<ProxyGroupStatus>,
}

/// Why `runtime.start` failed when no group engine started.
#[derive(Debug)]
pub enum StartFailure {
    AlreadyRunning,
    StartFailed,
}

/// Why `runtime.stop` failed.
#[derive(Debug)]
pub enum StopFailure {
    NotRunning,
    StopFailed,
}

pub struct ProxyGroupRuntime {
    groups: Vec<ProxyGroupSlot>,
    events: broadcast::Sender<RegistryEvent>,
    host_events: broadcast::Sender<HostSideEvent>,
    /// Forwarder tasks merging every group's streams into this runtime;
    /// aborted on drop. Never held across an await.
    forwarders: StdMutex<Vec<JoinHandle<()>>>,
}

impl ProxyGroupRuntime {
    /// Assemble the runtime from launcher-planned groups and start the event
    /// forwarders. Callers must have spawned watchdogs/retention already.
    pub(crate) fn new(groups: Vec<ProxyGroupSlot>) -> Arc<Self> {
        assert!(
            !groups.is_empty(),
            "the launch plan always contains at least the default group"
        );
        assert!(
            groups.len() <= MAX_PROXY_GROUPS,
            "the launch plan enforces the group ceiling"
        );
        let (events, _) = broadcast::channel(1024);
        let (host_events, _) = broadcast::channel(1024);
        let runtime = Arc::new(Self {
            groups,
            events,
            host_events,
            forwarders: StdMutex::new(Vec::new()),
        });
        runtime.spawn_forwarders();
        runtime
    }

    pub fn group_ids(&self) -> Vec<&str> {
        self.groups.iter().map(|slot| slot.id.as_str()).collect()
    }

    pub fn subscribe(&self) -> broadcast::Receiver<RegistryEvent> {
        self.events.subscribe()
    }

    pub fn subscribe_host(&self) -> broadcast::Receiver<HostSideEvent> {
        self.host_events.subscribe()
    }

    fn spawn_forwarders(self: &Arc<Self>) {
        let weak = Arc::downgrade(self);
        let mut handles = Vec::new();
        for slot in &self.groups {
            let group_id = slot.id.clone();
            let mut engine_rx = slot.supervisor.subscribe_engine();
            handles.push(tokio::spawn({
                let weak = weak.clone();
                async move {
                    loop {
                        match engine_rx.recv().await {
                            Ok(event) => {
                                let Some(runtime) = weak.upgrade() else {
                                    break;
                                };
                                runtime.on_group_engine_event(&group_id, event).await;
                            }
                            // A lagging forwarder drops transitions; the next
                            // transition or a poll re-syncs the aggregate.
                            Err(broadcast::error::RecvError::Lagged(_)) => continue,
                            Err(broadcast::error::RecvError::Closed) => break,
                        }
                    }
                }
            }));
            let mut host_rx = slot.supervisor.subscribe_host();
            handles.push(tokio::spawn({
                let host_tx = self.host_events.clone();
                async move {
                    loop {
                        match host_rx.recv().await {
                            // Host-side events (account/conversation/message/
                            // storage) are already wire-shaped and group-free
                            // or carry their own attribution.
                            Ok(event) => {
                                let _ = host_tx.send(event);
                            }
                            Err(broadcast::error::RecvError::Lagged(_)) => continue,
                            Err(broadcast::error::RecvError::Closed) => break,
                        }
                    }
                }
            }));
        }
        *self.forwarders.lock().expect("forwarder list mutex") = handles;
    }

    async fn on_group_engine_event(&self, group_id: &str, event: EngineEvent) {
        match event {
            EngineEvent::StateChanged(status) => {
                let _ = self.events.send(RegistryEvent::GroupStateChanged {
                    group_id: group_id.to_string(),
                    status: status.clone(),
                });
                let aggregate = self.aggregate_status().await;
                let _ = self
                    .events
                    .send(RegistryEvent::AggregateStateChanged(aggregate));
            }
            EngineEvent::ResourcePressure {
                state,
                pid,
                rss_bytes,
            } => {
                let _ = self.events.send(RegistryEvent::ResourcePressure {
                    state,
                    pid,
                    rss_bytes,
                    group_id: group_id.to_string(),
                });
            }
            EngineEvent::ProtocolWarning { kind } => {
                let _ = self.events.send(RegistryEvent::ProtocolWarning { kind });
            }
            EngineEvent::StorageChanged { state } => {
                let _ = self.events.send(RegistryEvent::StorageChanged { state });
            }
        }
    }

    /// Current per-group statuses in launcher order, with store-backed
    /// account counts.
    async fn group_statuses(&self) -> Vec<ProxyGroupStatus> {
        let router = self.groups[0].supervisor.service();
        let mut statuses = Vec::with_capacity(self.groups.len());
        for slot in &self.groups {
            let status = slot.supervisor.status().await;
            let account_count = router.lock().await.count_accounts_in_group_lossy(&slot.id);
            statuses.push(ProxyGroupStatus {
                group_id: slot.id.clone(),
                state: status.state,
                pid: status.pid,
                rss_bytes: status.rss_bytes,
                resource_pressure: status.resource_pressure,
                account_count,
            });
        }
        statuses
    }

    /// The pinned aggregation rule (implementation-plan §4.4).
    fn aggregate_from(parts: &[ProxyGroupStatus]) -> AggregateEngineStatus {
        let running = parts.iter().any(|part| part.state == EngineState::Running);
        let faulted = parts.iter().any(|part| part.state == EngineState::Faulted);
        let exited = parts.iter().any(|part| part.state == EngineState::Exited);
        let state = if running {
            EngineState::Running
        } else if faulted {
            EngineState::Faulted
        } else if exited {
            EngineState::Exited
        } else {
            EngineState::Stopped
        };
        AggregateEngineStatus {
            state,
            // Present only when exactly one group exists, regardless of state.
            pid: if parts.len() == 1 { parts[0].pid } else { None },
            rss_bytes: parts
                .iter()
                .filter_map(|part| part.rss_bytes)
                .reduce(u64::saturating_add),
            resource_pressure: parts.iter().any(|part| part.resource_pressure),
        }
    }

    async fn aggregate_status(&self) -> AggregateEngineStatus {
        Self::aggregate_from(&self.group_statuses().await)
    }

    pub async fn status(&self) -> RuntimeStatusResult {
        let proxy_groups = self.group_statuses().await;
        RuntimeStatusResult {
            aggregate: Self::aggregate_from(&proxy_groups),
            proxy_groups,
        }
    }

    /// Start every group's engine (R7). Success reports the per-group states;
    /// RUNTIME_START_FAILED only when no group engine started.
    pub async fn start(&self) -> Result<RuntimeStatusResult, StartFailure> {
        let mut any_running = false;
        let mut already_running = false;
        for slot in &self.groups {
            match slot.supervisor.start().await {
                Ok(status) => {
                    if status.state == EngineState::Running {
                        any_running = true;
                    }
                }
                Err(EngineError::Backpressure) => already_running = true,
                Err(_) => {}
            }
        }
        if !any_running && !already_running {
            return Err(StartFailure::StartFailed);
        }
        Ok(self.status().await)
    }

    /// Stop every group's engine (R7).
    pub async fn stop(&self) -> Result<RuntimeStatusResult, StopFailure> {
        let mut any_stopped = false;
        let mut any_failed = false;
        for slot in &self.groups {
            match slot.supervisor.stop().await {
                Ok(_) => any_stopped = true,
                Err(EngineError::NotRunning) => {}
                Err(_) => any_failed = true,
            }
        }
        if any_failed {
            return Err(StopFailure::StopFailed);
        }
        if !any_stopped {
            return Err(StopFailure::NotRunning);
        }
        Ok(self.status().await)
    }

    /// Bounded teardown of every group; used by host session end and signals.
    pub async fn shutdown(&self) -> Result<(), EngineError> {
        let mut first_error = None;
        for slot in &self.groups {
            // Shut every remaining group down even when one fails.
            if let Err(error) = slot.supervisor.shutdown().await
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// Union of every group's live accounts (implementation-plan §4.4:
    /// `accounts.list` items gain an always-present `proxyGroup`).
    pub async fn list_accounts(&self) -> Result<Vec<AccountSummary>, ServiceError> {
        let mut accounts = Vec::new();
        for slot in &self.groups {
            accounts.extend(slot.supervisor.list_accounts().await?);
        }
        Ok(accounts)
    }

    /// Resolve the optional `link.start` proxyGroup to its slot. Absent means
    /// `default`; anything outside the launch plan fails closed (R1/R9).
    fn slot_for_link(&self, proxy_group: Option<&str>) -> Result<&ProxyGroupSlot, ApiError> {
        let requested = proxy_group.unwrap_or(DEFAULT_PROXY_GROUP_ID);
        self.groups
            .iter()
            .find(|slot| slot.id == requested)
            .ok_or_else(|| {
                ApiError::new(
                    "PROXY_GROUP_NOT_FOUND",
                    format!("proxy group '{requested}' is not configured in this connector"),
                    false,
                )
            })
    }

    pub async fn start_link(
        &self,
        device_name: String,
        proxy_group: Option<String>,
    ) -> Result<Value, ServiceError> {
        let slot = self
            .slot_for_link(proxy_group.as_deref())
            .map_err(ServiceError::Api)?;
        slot.supervisor.start_link(device_name).await
    }

    /// Which group owns a link session (expired sessions included, so the
    /// dispatched finish still answers the distinct LINK_EXPIRED). None means
    /// no live session anywhere.
    pub async fn resolve_link_session(&self, link_session_id: &str) -> Option<String> {
        self.slot_for_session(link_session_id)
            .await
            .ok()
            .map(|slot| slot.id.clone())
    }

    async fn slot_for_session(
        &self,
        link_session_id: &str,
    ) -> Result<&ProxyGroupSlot, ServiceError> {
        for slot in &self.groups {
            if slot
                .supervisor
                .service()
                .lock()
                .await
                .has_link_session(link_session_id)
            {
                return Ok(slot);
            }
        }
        Err(ServiceError::Api(ApiError::new(
            "LINK_NOT_FOUND",
            "link session was not found",
            false,
        )))
    }

    pub async fn finish_link(
        &self,
        link_session_id: String,
    ) -> Result<AccountSummary, ServiceError> {
        let slot = self.slot_for_session(&link_session_id).await?;
        slot.supervisor.finish_link(link_session_id).await
    }

    pub async fn cancel_link(&self, link_session_id: String) -> Result<Value, ServiceError> {
        let slot = self.slot_for_session(&link_session_id).await?;
        slot.supervisor.cancel_link(link_session_id).await
    }

    /// Route by the account's immutable binding. A binding whose group is not
    /// in this launch plan means the connector was started without that
    /// group's configuration: the account stays intact but unreachable
    /// (dormant), reported without retry advice.
    async fn slot_for_account(&self, account_id: &str) -> Result<&ProxyGroupSlot, ServiceError> {
        let group = self
            .router_service()
            .lock()
            .await
            .account_proxy_group(account_id)?;
        self.groups
            .iter()
            .find(|slot| slot.id == group)
            .ok_or_else(|| {
                ServiceError::Api(ApiError::new(
                    "CAPABILITY_UNAVAILABLE",
                    format!("account proxy group '{group}' is not configured in this connector"),
                    false,
                ))
            })
    }

    /// Any supervisor's service works for routing lookups: all share one store.
    fn router_service(&self) -> &Arc<Mutex<crate::service::ConnectorService>> {
        self.groups[0].supervisor.service()
    }

    pub async fn delete_local_account(
        &self,
        account_id: String,
        operation_id: Option<String>,
    ) -> Result<Value, ServiceError> {
        // Delete idempotency lives in the shared operation ledger, not in the
        // routing row: once the account rows are gone, a replayed operationId
        // (or a v1-compatible delete of an absent account) must still resolve
        // through `prepare_account_delete` instead of answering
        // ACCOUNT_NOT_FOUND from the failed route lookup. Absent-account plans
        // complete inside the store and never touch an engine, so serving them
        // through any supervisor — all share one store — is group-agnostic.
        match self.slot_for_account(&account_id).await {
            Ok(slot) => {
                slot.supervisor
                    .delete_local_account(account_id, operation_id)
                    .await
            }
            Err(ServiceError::Store(StoreError::AccountNotFound)) => {
                self.groups[0]
                    .supervisor
                    .delete_local_account(account_id, operation_id)
                    .await
            }
            Err(error) => Err(error),
        }
    }

    pub async fn list_conversations(
        &self,
        account_id: String,
        limit: u32,
        cursor: Option<String>,
    ) -> Result<Page<ConversationSummary>, ServiceError> {
        let slot = self.slot_for_account(&account_id).await?;
        slot.supervisor
            .list_conversations(account_id, limit, cursor)
            .await
    }

    pub async fn list_messages(
        &self,
        account_id: String,
        conversation_id: String,
        limit: u32,
        before: Option<String>,
    ) -> Result<Page<MessageRecord>, ServiceError> {
        let slot = self.slot_for_account(&account_id).await?;
        slot.supervisor
            .list_messages(account_id, conversation_id, limit, before)
            .await
    }

    pub async fn get_message_text(
        &self,
        account_id: String,
        conversation_id: String,
        message_id: String,
    ) -> Result<MessageText, ServiceError> {
        let slot = self.slot_for_account(&account_id).await?;
        slot.supervisor
            .get_message_text(account_id, conversation_id, message_id)
            .await
    }

    pub async fn send_text(
        &self,
        account_id: String,
        target: SendTarget,
        text: String,
        client_request_id: String,
        quote_message_id: Option<String>,
    ) -> Result<MessageRecord, ServiceError> {
        let slot = self.slot_for_account(&account_id).await?;
        slot.supervisor
            .send_text(
                account_id,
                target,
                text,
                client_request_id,
                quote_message_id,
            )
            .await
    }

    pub async fn remote_delete(
        &self,
        params: crate::service::MessagesRemoteDeleteParams,
    ) -> Result<&'static str, ServiceError> {
        let slot = self.slot_for_account(&params.account_id).await?;
        slot.supervisor
            .remote_delete(params.account_id, params.conversation_id, params.message_id)
            .await
    }

    pub async fn send_reaction(
        &self,
        params: crate::service::MessagesSendReactionParams,
    ) -> Result<&'static str, ServiceError> {
        let slot = self.slot_for_account(&params.account_id).await?;
        slot.supervisor
            .send_reaction(
                params.account_id,
                params.conversation_id,
                params.message_id,
                params.emoji,
                params.remove,
            )
            .await
    }

    pub async fn get_attachment(
        &self,
        params: crate::service::MessagesGetAttachmentParams,
    ) -> Result<crate::service::AttachmentPayload, ServiceError> {
        let slot = self.slot_for_account(&params.account_id).await?;
        slot.supervisor
            .get_attachment(
                params.account_id,
                params.conversation_id,
                params.message_id,
                params.attachment_id,
                params.size_bytes,
            )
            .await
    }

    pub async fn sync_contacts(
        &self,
        account_id: &str,
    ) -> Result<ContactsSyncOutcome, ServiceError> {
        let slot = self.slot_for_account(account_id).await?;
        slot.supervisor.sync_contacts(account_id).await
    }

    pub async fn list_contacts(
        &self,
        account_id: String,
        query: Option<String>,
        limit: u32,
        cursor: Option<String>,
    ) -> Result<Page<ContactSummary>, ServiceError> {
        let slot = self.slot_for_account(&account_id).await?;
        slot.supervisor
            .list_contacts(account_id, query, limit, cursor)
            .await
    }

    pub async fn get_group(
        &self,
        params: crate::service::GroupsGetParams,
    ) -> Result<crate::service::GroupDetails, ServiceError> {
        let slot = self.slot_for_account(&params.account_id).await?;
        slot.supervisor
            .get_group(params.account_id, params.group_key)
            .await
    }

    pub async fn set_local_alias(
        &self,
        params: crate::service::ContactsSetLocalAliasParams,
    ) -> Result<&'static str, ServiceError> {
        let slot = self.slot_for_account(&params.account_id).await?;
        slot.supervisor
            .set_local_alias(params.account_id, params.peer_key, params.alias)
            .await
    }

    pub async fn set_typing_message(
        &self,
        params: crate::service::PresenceSetTypingMessageParams,
    ) -> Result<&'static str, ServiceError> {
        let slot = self.slot_for_account(&params.account_id).await?;
        slot.supervisor
            .set_typing_message(
                params.account_id,
                params.conversation_id,
                params.stop.unwrap_or(false),
            )
            .await
    }
}

impl Drop for ProxyGroupRuntime {
    fn drop(&mut self) {
        if let Ok(mut forwarders) = self.forwarders.lock() {
            for handle in forwarders.drain(..) {
                handle.abort();
            }
        }
    }
}

/// Open the shared per-profile store once and assemble one supervised engine
/// stack per planned group (ADR 0001 R4/R5). History retention is process-wide
/// and therefore spawned once; watchdogs are per group.
pub fn open_group_runtime(
    plan: ProxyGroupPlan,
    signal_cli: PathBuf,
    state_dir: &Path,
    java_home: Option<PathBuf>,
    store_key: Option<StoreKey>,
    signal_cli_mode: SignalCliMode,
) -> Result<Arc<ProxyGroupRuntime>, crate::store::StoreError> {
    let store = Arc::new(Store::open(state_dir, store_key)?);
    let mut slots = Vec::with_capacity(plan.groups.len());
    for entry in &plan.groups {
        let mut config = SignalCliConfig::new(signal_cli.clone(), entry.data_dir.clone());
        config.java_home = java_home.clone();
        config.proxy = entry.proxy.clone();
        config.mode = signal_cli_mode;
        let supervisor = Arc::new(RuntimeSupervisor::new(
            config,
            store.clone(),
            entry.id.clone(),
        ));
        supervisor.spawn_watchdog();
        slots.push(ProxyGroupSlot {
            id: entry.id.clone(),
            supervisor,
        });
    }
    slots[0].supervisor.spawn_history_retention();
    let runtime = ProxyGroupRuntime::new(slots);
    tracing::info!(
        groups = json!(runtime.group_ids()).to_string(),
        "connector runtime assembled"
    );
    Ok(runtime)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tempfile::TempDir;

    use super::*;
    use crate::store::StoreKey;

    /// One supervisor per group over a single shared store, mirroring
    /// `open_group_runtime` without spawning real engines.
    fn two_group_runtime() -> Arc<ProxyGroupRuntime> {
        let temp = TempDir::new().unwrap();
        let store =
            Arc::new(Store::open(temp.path(), Some(StoreKey::from_bytes([0x5A; 32]))).unwrap());
        let slot = |id: &str| {
            ProxyGroupSlot::new(
                id.to_string(),
                Arc::new(RuntimeSupervisor::new(
                    SignalCliConfig::new(
                        PathBuf::from("unused-signal-cli"),
                        PathBuf::from("/tmp/unused-signal-data"),
                    ),
                    store.clone(),
                    id.to_string(),
                )),
            )
        };
        let runtime = ProxyGroupRuntime::new(vec![slot("default"), slot("team-b")]);
        // Leak the TempDir for the test lifetime so the store path stays valid
        // (same discipline as the host unit tests).
        std::mem::forget(temp);
        runtime
    }

    /// Routing contract for expired link sessions (implementation-plan §6.1):
    /// ownership survives expiry — an expired-but-known session still resolves
    /// to its owning group so its finish answers LINK_EXPIRED there instead of
    /// degrading to LINK_NOT_FOUND from the router.
    #[tokio::test]
    async fn expired_link_sessions_still_route_to_their_owning_group() {
        let runtime = two_group_runtime();
        let owner = &runtime.groups[1];
        // One group owns one session at a time, so each session is asserted
        // while it is the group's current one.
        let expired = owner
            .supervisor
            .service()
            .lock()
            .await
            .plant_link_session_for_test("KT-Expired", Duration::ZERO);
        assert_eq!(
            runtime.resolve_link_session(&expired).await.as_deref(),
            Some("team-b"),
            "an expired session must keep its owning group"
        );

        let live = owner
            .supervisor
            .service()
            .lock()
            .await
            .plant_link_session_for_test("KT-Live", Duration::from_secs(300));
        assert_eq!(
            runtime.resolve_link_session(&live).await.as_deref(),
            Some("team-b")
        );
        assert_eq!(runtime.resolve_link_session("never-started").await, None);
    }
}
