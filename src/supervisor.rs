// SPDX-License-Identifier: AGPL-3.0-only

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};
use tokio::sync::{Mutex, broadcast, mpsc};
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior, interval, sleep};

use crate::engine::{
    CallClass, EngineError, EngineEvent, EngineHandle, EngineState, EngineStatus,
    NormalizedReceive, QueuedReceive, ReceiveIngress, SignalCliConfig, event_channel,
    receive_channel,
};
use crate::media::{MediaGovernor, MediaHandleTable};
use crate::protocol::ApiError;
use crate::service::{
    AccountDeviceCapabilities, AccountUsername, AccountsDeviceCapabilitiesParams,
    AccountsGetDevicesParams, AccountsGetUsernameParams, AccountsSetExpireTimerParams,
    AccountsSetProfileParams, AccountsSetUsernameParams, AccountsUnlinkDeviceParams,
    AccountsUpdateConfigurationParams, AttachmentSendTarget, AvatarDownload, BlockedConversations,
    CallsSendGroupCallUpdateParams, CallsSendSignalParams, ChatFolderSyncs, ChatFoldersListParams,
    ChatFoldersSetRecordsParams, ConnectorService, ContactsSyncOutcome, ConversationState,
    ConversationsGetAvatarParams, ConversationsGetBlockedParams, ConversationsGetPinnedParams,
    ConversationsGetStateParams, ConversationsSetBlockedParams, ConversationsSetExpireTimerParams,
    ConversationsSetExpireTimerResult, ConversationsSetPinnedParams, ConversationsSetStateParams,
    DeviceList, GROUP_MEMBER_LABEL_EMOJI_MAX_BYTES, GROUP_MEMBER_LABEL_SERVER_MAX_BYTES,
    GroupDetails, GroupInviteLink, GroupInviteLinkParse, GroupInviteLinkPassword,
    GroupInviteLinkPreview, GroupsApproveJoinRequestsParams, GroupsBuildInviteLinkParams,
    GroupsCancelJoinRequestParams, GroupsCreateParams, GroupsCreateResult,
    GroupsInviteLinkPasswordParams, GroupsJoinViaLinkParams, GroupsMigrateParams,
    GroupsMigrateResult, GroupsParseInviteLinkParams, GroupsPreviewViaLinkParams, GroupsQuitParams,
    GroupsRevokeInvitesParams, GroupsTerminateParams, GroupsUpdateParams,
    HistoryAttachmentDownloadView, HostSideEvent, IdentitiesGetParams, IdentitiesVerifyParams,
    IdentityRead, IdentityVerifyOutcome, MAX_AVATAR_DATA_URI_CHARS, MAX_GROUP_MEMBER_ID_CHARS,
    MAX_GROUP_MEMBERS, MAX_STORY_RECIPIENTS, MediaChunkView, MediaCloseView, MediaIngest,
    MediaOpenView, MessageRequestOutcome, MessageRequestParams, MessagesDeleteForMeParams,
    MessagesReportSpamParams, MessagesSendContactParams, MessagesSendPollCreateParams,
    MessagesSendPollTerminateParams, MessagesSendPollVoteParams, MessagesSendStickerParams,
    MessagesSendStoryParams, PeerTarget, PinnedConversations, PreparedHistoryDownload,
    PreparedSend, ReportSpamFailure, ReportSpamResult, SendTarget, SendTextMentionParams,
    SendTextPreviewParams, SendTextStyleParams, ServiceError, StickerManifestEntry,
    StickerPackGetSyncsParams, StickerPackImage, StickerPackImageParams, StickerPackManifest,
    StickerPackManifestParams, StickerPackSetSyncParams, StickerPackSyncs, StoriesDisabled,
    StoriesGetDisabledParams, StoriesMarkViewedParams, StoriesSendReactionParams,
    StoriesSetDisabledParams, StorySendRecipientResult, StorySendResult, StorySendSuccess,
    UsernameLookupEntry, UsernameLookupParams, UsernameLookupResult,
    account_device_capabilities_from_engine, account_limit_error, blocked_conversations_result,
    chat_folder_syncs_result, conversation_state_from_engine, device_list_from_engine,
    group_create_result_from_engine, group_invite_join_status_from_engine,
    group_invite_link_parse_from_engine, group_invite_link_password_from_engine,
    group_invite_link_preview_from_engine, group_migrate_result_from_engine, identity_from_engine,
    pinned_conversations_result, sticker_pack_syncs_result, store_adjacent_media,
    store_get_group_with_requesting, store_get_message_text, store_list_contacts,
    store_list_conversations, store_list_messages, store_search_messages,
    validate_account_delete_operation_id,
};
use crate::store::{
    AccountDeletePlan, AccountSummary, ContactSummary, ConversationSummary, EXPIRE_SWEEP_BATCH,
    MessageRecord, Page, Store, StoreError, SyncedContact,
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
/// Settle time before a group's first expire sweep after start (§4.41): the
/// handshake must not share its few-second budget with a store pass.
const EXPIRE_SWEEP_STARTUP_SETTLE: Duration = Duration::from_secs(5);
/// Capacity of the auto delivery-receipt queue (contract revision 1.34):
/// bounded, lossy by design — a full queue drops the job, never the receive.
const DELIVERY_RECEIPT_QUEUE_CAP: usize = 512;

/// One queued auto delivery receipt (contract revision 1.34, §4.29): after an
/// incoming dataMessage row lands, the connector best-effort tells the engine
/// to send the official delivery receipt to its author. Fully silent: failures
/// change nothing, and there is no retry.
#[derive(Clone, Debug)]
struct DeliveryReceiptJob {
    account: String,
    recipient: String,
    timestamp: u64,
}

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
    /// Shared store handle for the store-only paths (retention, reads) that
    /// run on the blocking pool without the service lock.
    store: Arc<Store>,
    /// Media ingest backing (ADR 0002) when the launcher passed
    /// `--media-ingest`; drives the retention trigger and exists iff the
    /// service's own backing exists. Never held; `MediaGovernor` is a plain
    /// data-dir handle.
    media: Option<MediaIngest>,
    active_link_finish: Mutex<Option<String>>,
    events: broadcast::Sender<EngineEvent>,
    host_events: broadcast::Sender<HostSideEvent>,
    receive_ingress: ReceiveIngress,
    receive_worker: JoinHandle<()>,
    /// Receive-liveness watchdog; never held across an await.
    watchdog: StdMutex<Option<JoinHandle<()>>>,
    /// Engine-event watcher marking device-unlinked accounts (contract 1.21);
    /// never held across an await.
    account_unlink_worker: JoinHandle<()>,
    /// One-shot history retention pass; never held across an await.
    retention: StdMutex<Option<JoinHandle<()>>>,
    /// One-shot media governor pass (ADR 0002: once per process start);
    /// never held across an await.
    media_governor_pass: StdMutex<Option<JoinHandle<()>>>,
    /// One-shot contacts/groups re-sync pass (once per process start);
    /// never held across an await.
    contacts_resync: StdMutex<Option<JoinHandle<()>>>,
    /// Link-time history import single flight (contract revision 1.39,
    /// §4.38): at most one import per group at a time — claimed by the
    /// `link.finish` watcher or the startup sweep, released when the import
    /// (plus its retention convergence) finishes. Shared with the spawned
    /// tasks, which outlive any borrow of `self`.
    history_import_running: Arc<AtomicBool>,
    /// Which account holds the import slot right now (contract 1.53): the
    /// `processing` stage of `history.importStatus` must name the polled
    /// account, not just "the group is busy" — a crashed run's stale
    /// `running` row on a second account must not borrow another account's
    /// live pass as its progress. Set/cleared inside the same claim/release
    /// window as the single-flight flag.
    history_import_holder: Arc<StdMutex<Option<String>>>,
    /// Disappearing-message sweep single flight (contract revision 1.42,
    /// §4.41): at most one sweep pass per group at a time, shared with the
    /// periodic task.
    expire_sweep_running: Arc<AtomicBool>,
    /// The periodic expire sweeper; never held across an await.
    expire_sweeper: StdMutex<Option<JoinHandle<()>>>,
    /// Receiver of the bounded auto delivery-receipt queue; `Some` until
    /// `spawn_delivery_receipt_worker` takes it. The queue itself is created
    /// in `new` (the persistence loop holds the sender), but the worker can
    /// only be spawned once the supervisor sits behind its `Arc`.
    delivery_receipt_rx: StdMutex<Option<mpsc::Receiver<DeliveryReceiptJob>>>,
    /// The delivery-receipt worker; never held across an await.
    delivery_receipt_worker: StdMutex<Option<JoinHandle<()>>>,
    /// unix ms of last listContacts title enrich (throttle hot list path)
    last_title_enrich_ms: AtomicU64,
    /// The §4.46 backup file three-chain: `Some` when the launcher armed the
    /// backup-export capability. Process-wide like the media table (the
    /// registry hands every group the same `Arc` — `readChunk`/`closeHandle`
    /// carry no accountId, so handle resolution must not depend on routing).
    backup_handles: Option<Arc<crate::backup_export::BackupFileHandleTable>>,
    /// Per-account skip counters from the most recent successful artifact
    /// build; published after `build_artifact` writes the artifact, merged
    /// into `backups.exportStatus` answers. Content-free numbers only.
    backup_counters: StdMutex<HashMap<String, crate::backup_export::SkipCounters>>,
}

impl RuntimeSupervisor {
    pub fn new(
        config: SignalCliConfig,
        store: Arc<Store>,
        group_id: String,
        media_handles: Option<Arc<MediaHandleTable>>,
    ) -> Self {
        Self::with_backup_handles(config, store, group_id, media_handles, None)
    }

    /// Launcher entry carrying the §4.46 backup-handle table: `Some` arms the
    /// backup-export capability (every group receives the same process-wide
    /// table — see `backup_handles`). `new` is the media-only compatibility
    /// form the tests use.
    pub fn with_backup_handles(
        config: SignalCliConfig,
        store: Arc<Store>,
        group_id: String,
        media_handles: Option<Arc<MediaHandleTable>>,
        backup_handles: Option<Arc<crate::backup_export::BackupFileHandleTable>>,
    ) -> Self {
        let (events, _) = event_channel();
        let (host_events, _) = broadcast::channel(1024);
        // Media ingest is armed only when the launcher passed both the
        // `--media-ingest` flag and the shared handle table; anything else
        // keeps the service on the pre-PoC capability-unavailable shape.
        let media = if config.media_ingest {
            media_handles.map(|handles| MediaIngest::new(config.data_dir.clone(), handles))
        } else {
            None
        };
        if config.media_ingest && media.is_none() {
            tracing::warn!(
                "media ingest flag is set without a handle table; media methods stay unavailable"
            );
        }
        let service = Arc::new(Mutex::new(ConnectorService::with_media(
            Arc::clone(&store),
            media.clone(),
        )));
        let (receive_ingress, receive_rx) = receive_channel();
        // Auto delivery receipts (contract revision 1.34): the persistence
        // loop queues one job per landed incoming dataMessage; the worker
        // spawned later by the registry drains it. Bounded and lossy — a full
        // queue drops the receipt job, never the receive.
        let (delivery_receipt_tx, delivery_receipt_rx) =
            mpsc::channel::<DeliveryReceiptJob>(DELIVERY_RECEIPT_QUEUE_CAP);
        let receive_worker = tokio::spawn(receive_persistence_loop(
            receive_rx,
            service.clone(),
            host_events.clone(),
            group_id.clone(),
            media.as_ref().map(|media| media.governor().clone()),
            delivery_receipt_tx,
        ));
        // Contract 1.21: authorization failures observed on this group's
        // engine mark the dead account in the shared store and surface it on
        // the host lane (account.changed).
        let account_unlink_worker = tokio::spawn(account_unlink_watch_loop(
            events.subscribe(),
            service.clone(),
            host_events.clone(),
        ));
        Self {
            config,
            group_id,
            engine: Mutex::new(None),
            service,
            store,
            media,
            active_link_finish: Mutex::new(None),
            events,
            host_events,
            receive_ingress,
            receive_worker,
            account_unlink_worker,
            watchdog: StdMutex::new(None),
            retention: StdMutex::new(None),
            media_governor_pass: StdMutex::new(None),
            contacts_resync: StdMutex::new(None),
            history_import_running: Arc::new(AtomicBool::new(false)),
            history_import_holder: Arc::new(StdMutex::new(None)),
            expire_sweep_running: Arc::new(AtomicBool::new(false)),
            expire_sweeper: StdMutex::new(None),
            delivery_receipt_rx: StdMutex::new(Some(delivery_receipt_rx)),
            delivery_receipt_worker: StdMutex::new(None),
            last_title_enrich_ms: AtomicU64::new(0),
            backup_handles,
            backup_counters: StdMutex::new(HashMap::new()),
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

    /// Whether the media-download capability is genuinely armed for this
    /// group: the media table exists — the launcher passed `--media-ingest`
    /// AND the shared handle table arrived — which is the exact gate every
    /// media method answers before anything else. The registry reads it for
    /// the handshake `attachment-download` advertisement (contract revision
    /// 1.98): an unarmed group must not advertise a tag whose faces answer
    /// `CAPABILITY_UNAVAILABLE` for every call.
    pub(crate) fn media_ingest_armed(&self) -> bool {
        self.media.is_some()
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
        let store = Arc::clone(&self.store);
        *slot = Some(tokio::spawn(async move {
            let now = crate::link::now_ms();
            let mut removed = 0_u64;
            loop {
                // Each batch runs on the blocking pool: rusqlite is
                // synchronous and must never occupy a tokio worker thread.
                let outcome = tokio::task::spawn_blocking({
                    let store = Arc::clone(&store);
                    move || store.prune_history(now, RETENTION_BATCH_MESSAGES)
                })
                .await
                .unwrap_or(Err(StoreError::Unavailable(None)));
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
            match tokio::task::spawn_blocking(move || store.prune_expired_plaintext_backup(now))
                .await
                .unwrap_or(Err(StoreError::Unavailable(None)))
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

    /// Spawn the disappearing-message sweeper (contract 1.42, §4.41;
    /// idempotent). One periodic task per group: after a short startup settle
    /// it runs one full drain (bounded batches back to back), then sweeps on
    /// the configured cadence. Store work runs on the blocking pool; each
    /// pass is single-flight (`expire_sweep_running`) so a long drain never
    /// overlaps the next tick. A conversation with no timer state costs
    /// nothing: the partial index makes the "nothing due" probe a point query.
    pub fn spawn_expire_sweeper(self: &Arc<Self>) {
        let Ok(mut slot) = self.expire_sweeper.lock() else {
            return;
        };
        if slot.is_some() {
            return;
        }
        let supervisor = Arc::clone(self);
        *slot = Some(tokio::spawn(async move {
            sleep(EXPIRE_SWEEP_STARTUP_SETTLE).await;
            loop {
                supervisor.run_expire_sweep_pass().await;
                sleep(supervisor.config.expire_sweep_interval).await;
            }
        }));
    }

    /// One sweep drain: bounded delete batches back to back until a batch
    /// comes back under the cap (or the store fails), emitting one batched
    /// `message.expired` event per touched conversation. Failures leave the
    /// rest for the next tick and never take the sweeper down.
    async fn run_expire_sweep_pass(&self) {
        if self.expire_sweep_running.swap(true, Ordering::Relaxed) {
            return;
        }
        let result = self.expire_sweep_batches().await;
        self.expire_sweep_running.store(false, Ordering::Relaxed);
        if let Err(error) = result {
            tracing::warn!(
                error_class = error.class(),
                "expire sweep stopped: store unavailable"
            );
        }
    }

    async fn expire_sweep_batches(&self) -> Result<(), StoreError> {
        loop {
            let groups = tokio::task::spawn_blocking({
                let store = Arc::clone(&self.store);
                move || store.sweep_expired_messages(crate::link::now_ms(), EXPIRE_SWEEP_BATCH)
            })
            .await
            .unwrap_or(Err(StoreError::Unavailable(None)))?;
            // The cap bounds deleted rows, not conversations: a full batch is
            // total ids across the returned groups (one group may hold the
            // whole batch), so a backed-up drain keeps going immediately.
            let deleted: usize = groups.iter().map(|group| group.message_ids.len()).sum();
            let full_batch = deleted >= EXPIRE_SWEEP_BATCH as usize;
            for group in groups {
                let _ = self.host_events.send(HostSideEvent::MessageExpired {
                    account_id: group.account_id,
                    conversation_id: group.conversation_id,
                    message_ids: group.message_ids,
                });
            }
            if !full_batch {
                return Ok(());
            }
            sleep(RETENTION_BATCH_PAUSE).await;
        }
    }

    /// One media governor pass per process start (ADR 0002 trigger
    /// discipline; idempotent). Media retention is per engine data directory,
    /// so — unlike the store-wide history retention — every proxy group's
    /// supervisor runs its own pass, and the receive pipeline repeats it
    /// after every [`crate::media::GOVERNOR_INBOUND_MESSAGE_INTERVAL`]
    /// inbound messages.
    pub fn spawn_media_governor(self: &Arc<Self>) {
        let Some(governor) = self.media.as_ref().map(|media| media.governor().clone()) else {
            return;
        };
        let Ok(mut slot) = self.media_governor_pass.lock() else {
            return;
        };
        if slot.is_some() {
            return;
        }
        *slot = Some(run_media_governor_pass(governor));
    }

    /// Take the queued auto delivery-receipt receiver and spawn its one
    /// worker (contract revision 1.34, idempotent). Spawned by the registry —
    /// not in `new`, which runs before the supervisor sits behind its
    /// `Arc<Self>` — and the worker re-resolves `running_engine()` per job
    /// because a pre-cloned engine handle would go stale across watchdog
    /// restarts. Fully silent: every failure (runtime down, engine error,
    /// unknown outcome) is dropped; nothing is retried.
    pub fn spawn_delivery_receipt_worker(self: &Arc<Self>) {
        let Some(receiver) = self
            .delivery_receipt_rx
            .lock()
            .ok()
            .and_then(|mut slot| slot.take())
        else {
            return;
        };
        let supervisor = Arc::clone(self);
        let worker = tokio::spawn(async move {
            let mut receiver = receiver;
            while let Some(job) = receiver.recv().await {
                let Ok(engine) = supervisor.running_engine().await else {
                    continue;
                };
                let _ = engine
                    .call(
                        "sendDeliveryReceipt",
                        json!({
                            "account": job.account,
                            "recipient": job.recipient,
                            "timestamps": [job.timestamp],
                        }),
                        CallClass::Mutating,
                    )
                    .await;
            }
        });
        if let Ok(mut slot) = self.delivery_receipt_worker.lock() {
            *slot = Some(worker);
        }
    }

    /// One best-effort contacts/groups re-sync per linked account of this
    /// group once an engine has landed (boot start or watchdog recovery).
    ///
    /// The sync is what materializes the contract-1.14 conversation
    /// skeletons, so an account linked before that revision — or whose
    /// inline link-time sync failed — would otherwise surface conversations
    /// only message by message. It runs in the background (the host
    /// handshake path never waits on the engine) and the 60-second cache inside
    /// the sync folds repeat starts into read-only no-ops. One pass per
    /// process start, same discipline as history retention.
    fn spawn_contacts_resync(&self) {
        let Ok(engine_slot) = self.engine.try_lock() else {
            return;
        };
        let Some(engine) = engine_slot
            .as_ref()
            .filter(|engine| !engine.is_terminal())
            .cloned()
        else {
            return;
        };
        drop(engine_slot);
        self.spawn_contacts_resync_with(engine);
    }

    fn spawn_contacts_resync_with(&self, engine: EngineHandle) {
        let Ok(mut slot) = self.contacts_resync.lock() else {
            return;
        };
        if slot.is_some() {
            return;
        }
        let service = Arc::clone(&self.service);
        let group_id = self.group_id.clone();
        *slot = Some(tokio::spawn(async move {
            // Let the fresh engine settle and any link-time inline sync land
            // first; the 60-second cache then folds the overlap into a no-op.
            sleep(Duration::from_secs(2)).await;
            let accounts = match service.lock().await.list_accounts_in_group(&group_id) {
                Ok(accounts) => accounts,
                Err(error) => {
                    tracing::warn!(
                        error_class = error.class(),
                        "startup contacts resync account listing failed"
                    );
                    return;
                }
            };
            for account in accounts {
                if let Err(error) = sync_contacts_with(&service, &engine, &account.id).await {
                    tracing::warn!(
                        error_class = error.class(),
                        account_id = %account.id,
                        "startup contacts resync failed"
                    );
                }
            }
        }));
    }

    // -------------------------------------------------------------------------
    // Link-time history import (contract revision 1.39, §4.38): the kt-engine
    // backup5 archive face. The engine downloads and decrypts in the
    // background after `link.finish`; the connector watches for the file
    // (plus a once-per-start sweep for restarts-in-between) and imports one
    // account at a time under the bounded-batch discipline. The status face
    // is store-only and never touches the engine.
    // -------------------------------------------------------------------------

    /// Once-per-start sweep: import any group account whose archive exists
    /// but has no completed row — the connector may have restarted between
    /// link and import, or a previous attempt failed. Settles first so the
    /// fresh engine's own startup work lands, then imports one account at a
    /// time (the group's single slot).
    fn spawn_history_import_pass(&self) {
        let store = Arc::clone(&self.store);
        let group_id = self.group_id.clone();
        let data_dir = self.config.data_dir.clone();
        let running = Arc::clone(&self.history_import_running);
        let holder = Arc::clone(&self.history_import_holder);
        tokio::spawn(async move {
            // Let the fresh engine settle and any link-time inline sync land
            // before the first import pass claims store batches.
            sleep(Duration::from_secs(5)).await;
            let accounts = match tokio::task::spawn_blocking({
                let store = Arc::clone(&store);
                let group_id = group_id.clone();
                move || store.list_accounts_in_group(&group_id)
            })
            .await
            {
                Ok(Ok(accounts)) => accounts,
                _ => {
                    tracing::warn!("history import sweep account listing failed");
                    return;
                }
            };
            for account in accounts {
                let probe = tokio::task::spawn_blocking({
                    let store = Arc::clone(&store);
                    let data_dir = data_dir.clone();
                    let account_id = account.id.clone();
                    move || probe_history_import(&store, &data_dir, &account_id)
                })
                .await
                .unwrap_or(HistoryImportProbe::Done);
                let HistoryImportProbe::Ready(job) = probe else {
                    continue;
                };
                // A busy slot skips this account: the next start (or the link
                // watcher's tick for freshly linked accounts) retries.
                let _ = claim_and_run_history_import(
                    Arc::clone(&running),
                    Arc::clone(&holder),
                    Arc::clone(&store),
                    job,
                )
                .await;
            }
        });
    }

    /// After a `link.finish`, watch for the archive the engine is
    /// downloading in the background (§4.38): poll every few seconds for at
    /// most [`crate::history_import::LINK_WAIT_MS`], import when the file
    /// appears. A busy group slot retries on the next tick. Best-effort: the
    /// import never fails the link flow.
    fn spawn_history_import_watch(&self, account_id: String) {
        let store = Arc::clone(&self.store);
        let data_dir = self.config.data_dir.clone();
        let running = Arc::clone(&self.history_import_running);
        let holder = Arc::clone(&self.history_import_holder);
        tokio::spawn(async move {
            let deadline =
                Instant::now() + Duration::from_millis(crate::history_import::LINK_WAIT_MS);
            loop {
                if Instant::now() >= deadline {
                    tracing::info!(
                        account_id = %account_id,
                        "history archive did not appear within the link-time window"
                    );
                    return;
                }
                sleep(Duration::from_secs(5)).await;
                let probe = tokio::task::spawn_blocking({
                    let store = Arc::clone(&store);
                    let data_dir = data_dir.clone();
                    let account_id = account_id.clone();
                    move || probe_history_import(&store, &data_dir, &account_id)
                })
                .await
                .unwrap_or(HistoryImportProbe::Done);
                match probe {
                    HistoryImportProbe::Waiting => continue,
                    HistoryImportProbe::Done => return,
                    HistoryImportProbe::Ready(job) => {
                        if claim_and_run_history_import(
                            Arc::clone(&running),
                            Arc::clone(&holder),
                            Arc::clone(&store),
                            job,
                        )
                        .await
                        {
                            return;
                        }
                        // Slot busy: the next tick retries.
                    }
                }
            }
        });
    }

    /// Whether the group's import slot is held by THIS account's run (the
    /// `processing` stage must not borrow another account's live pass — the
    /// group slot is single-flight but a crashed run's stale `running` row on
    /// a second account is not a live pass, contract 1.53).
    fn history_import_pass_active_for(&self, account_id: &str) -> bool {
        self.history_import_running.load(Ordering::Acquire)
            && self
                .history_import_holder
                .lock()
                .map(|holder| holder.as_deref() == Some(account_id))
                .unwrap_or(false)
    }

    /// history.importStatus (contract revision 1.39, §4.38): the honest
    /// minimal projection the desktop polls — store-first, never the network.
    /// Since contract 1.53 a `running` row also consults the engine's
    /// transfer-download snapshot (one bounded read-only call) so the
    /// Download stage can report byte progress; every other state is
    /// store/disk-only exactly as before.
    pub async fn history_import_status(&self, account_id: String) -> Result<Value, ServiceError> {
        let store = Arc::clone(&self.store);
        let data_dir = self.config.data_dir.clone();
        let row_account_id = account_id.clone();
        let (mut status, signal_account) = blocking_service(move || {
            let status = history_import_status_projection(&store, &data_dir, &row_account_id)?;
            let signal_account = if status["state"] == "running" {
                store
                    .account_by_id(&row_account_id)
                    .ok()
                    .flatten()
                    .map(|account| account.signal_account)
            } else {
                None
            };
            Ok((status, signal_account))
        })
        .await?;
        // Download-stage progress: only while the row says `running`, and
        // only if this group's engine is answering. A down engine reads as no
        // snapshot — the projection stays honest without the bytes rather
        // than guessing a state the store does not hold. When the engine
        // answers "no download in flight" but this group's own NDJSON import
        // pass holds the slot, the stage is `processing` (the official
        // Process stage renders copy, not numbers). Neither signal present:
        // the pre-1.53 shape, no progress invented.
        if status["state"] == "running" {
            let mut downloading = None;
            if let Some(signal_account) = signal_account
                && let Ok(engine) = self.running_engine().await
                && let Ok(snapshot) = engine
                    .call(
                        "transfer.downloadStatus",
                        json!({ "account": signal_account }),
                        CallClass::ReadOnly,
                    )
                    .await
                && let Some(progress) = import_progress_from_engine_snapshot(&snapshot)
            {
                downloading = Some(progress);
            }
            let stage = compose_import_progress(
                downloading,
                self.history_import_pass_active_for(&account_id),
            );
            if let (Some(progress), Some(object)) = (stage, status.as_object_mut()) {
                object.insert("progress".to_owned(), progress);
            }
        }
        Ok(status)
    }

    /// history.importRetry (contract revision 1.53, §4.38): clear the
    /// account's `failed` import row and re-arm the orchestration — the
    /// archive on disk returns to `pending` and the sweep (next engine start)
    /// or the link watcher (fresh spawn) picks it up. Mutating face: the
    /// account-scoped write-lane mutex and delete drain barrier serialize it
    /// against every other account mutation (host `acquire`); the store work
    /// runs on the blocking pool. A non-`failed` row is NOT_RETRYABLE; no
    /// archive on the engine face is NO_ARCHIVE.
    pub async fn history_import_retry(&self, account_id: String) -> Result<Value, ServiceError> {
        let store = Arc::clone(&self.store);
        let data_dir = self.config.data_dir.clone();
        blocking_service(move || history_import_retry_projection(&store, &data_dir, &account_id))
            .await
    }

    /// history.attachmentDownload (contract revision 1.57, §4.38 step 3):
    /// walk one stored transit pointer face end to end — stream the CDN body
    /// to a temp file under hard byte bounds, gate the ciphertext layer
    /// (terminal HMAC + whole-body SHA-256 `digest`), decrypt
    /// (AES-256-CBC / PKCS7 / log-pad trim), gate the plaintext hash, and
    /// atomically land the plaintext where `messages.attachments.open`
    /// resolves it. Mutating, send lane behind the reaction-class write-lane
    /// mutex and delete barrier (like importRetry). Every download/verify
    /// failure is terminal — no auto-retry (§4.38 honest degradation; a retry
    /// is the caller's fresh call). Local-IO failures keep the established
    /// retryable UPSTREAM_ERROR face.
    pub async fn history_attachment_download(
        &self,
        params: crate::service::HistoryAttachmentDownloadParams,
    ) -> Result<Value, ServiceError> {
        self.run_attachment_download_chain(|service| {
            service.prepare_history_attachment_download(
                &params.account_id,
                &params.conversation_id,
                &params.message_id,
                &params.attachment_id,
            )
        })
        .await
    }

    /// attachments.download (contract revision 1.61, §4.54): the §4.54
    /// addressing face onto the same chain as
    /// [`Self::history_attachment_download`] — the prepare half resolves the
    /// descriptor positionally and walks the live/import locator ladder, the
    /// network and crypto halves are the 1.57 machine unchanged. Mutating,
    /// send lane behind the reaction-class write-lane mutex and delete
    /// barrier; every download/verify failure terminal; idempotent on an
    /// already-present file.
    pub async fn attachment_download(
        &self,
        params: crate::service::AttachmentDownloadParams,
    ) -> Result<Value, ServiceError> {
        self.run_attachment_download_chain(|service| {
            service.prepare_attachment_download(
                &params.account_id,
                &params.conversation_id,
                &params.message_id,
                params.attachment_index,
            )
        })
        .await
    }

    /// stories.downloadAttachment (contract revision 1.86, §4.71): the
    /// story-anchored face onto the same chain as
    /// [`Self::attachment_download`] — the prepare half gates the
    /// caller-supplied pointer's wire shape (the §4.54 ingest caps mirrored)
    /// and stages the landing, the network and crypto halves are the 1.57
    /// machine unchanged with the `TransitFace::build` crypto-shape decode
    /// still ahead of any network traffic. Mutating, send lane behind the
    /// reaction-class write-lane mutex and delete barrier; every
    /// download/verify failure terminal; idempotent on an already-present
    /// file.
    pub async fn story_attachment_download(
        &self,
        params: crate::service::StoryAttachmentDownloadParams,
    ) -> Result<Value, ServiceError> {
        self.run_attachment_download_chain(|service| {
            service.prepare_story_attachment_download(&params)
        })
        .await
    }

    /// stories.openAttachment (contract revision 1.86, §4.71): the handle
    /// mint over an already-landed story attachment — the
    /// [`Self::open_media`] machine without the row addressing (stories
    /// land in no conversation row). Read lane, no engine touch.
    pub async fn story_open_attachment(
        &self,
        params: crate::service::StoryOpenAttachmentParams,
    ) -> Result<MediaOpenView, ServiceError> {
        self.service
            .lock()
            .await
            .open_story_attachment_handle(&params.account_id, &params.attachment_id)
    }

    /// The shared download chain: the deterministic prepare half runs inside
    /// the service lock, the streaming network half runs on the supervisor
    /// (§4.38 step 3 machine verbatim).
    async fn run_attachment_download_chain(
        &self,
        prepare: impl FnOnce(
            &crate::service::ConnectorService,
        ) -> Result<crate::service::PreparedHistoryDownload, ServiceError>,
    ) -> Result<Value, ServiceError> {
        // The staged, locally-verified inputs: capability gate, id ladder,
        // descriptor index, pointer face, destination paths. Everything
        // deterministic answers here, before any network traffic.
        let prepared = prepare(&*self.service.lock().await)?;
        let (locator, descriptor_size, target, temp_path, plaintext_path) = match prepared {
            // Idempotent dedup: the file already exists (an earlier call or
            // the engine's own live download). The measured size is the
            // honest answer; the CDN is never re-asked.
            PreparedHistoryDownload::AlreadyPresent { size_bytes } => {
                return Ok(
                    serde_json::to_value(HistoryAttachmentDownloadView { size_bytes })
                        .unwrap_or(Value::Null),
                );
            }
            PreparedHistoryDownload::Ready {
                locator,
                descriptor_size,
                target,
                temp_path,
                plaintext_path,
            } => (locator, descriptor_size, target, temp_path, plaintext_path),
        };
        // Fail-closed pointer decode (official gate parity): the face's
        // key/cdn/integrity/size fields, the URL assembly, and the body
        // bounds — before the sink opens. Cheap, but the locator strings
        // (bounded) move into a scoped thread anyway so nothing borrows
        // across the await below.
        let cdn_base_override = self.config.history_cdn_base.clone();
        let face = std::thread::scope(|scope| {
            scope
                .spawn(move || {
                    crate::history_attachment::TransitFace::build(
                        &locator,
                        descriptor_size,
                        cdn_base_override.as_deref(),
                    )
                })
                .join()
                .unwrap_or(Err(crate::history_attachment::DownloadFailure::Io))
        })
        .map_err(map_download_failure)?;
        let body_max = face.body_max_bytes();
        let url = face.url.clone();

        // Stream the body. The service lock is released for the network half
        // (the media chain answers stay serialized at dispatch; the temp
        // names are unique so two chains cannot collide). The launcher's
        // SOCKS route from the process environment (contract revision 1.98,
        // §4.38 step 3) rides this client too: the CDN GET shares the exact
        // route the engine's own transport takes.
        let env_proxy = std::env::var(crate::doctor::SOCKS_PROXY_ENV).ok();
        let client = attachment_download_client(env_proxy.as_deref())?;
        let response = client.get(&url).send().await.map_err(|_| {
            map_download_failure(crate::history_attachment::DownloadFailure::Network)
        })?;
        if !response.status().is_success() {
            return Err(map_download_failure(
                crate::history_attachment::DownloadFailure::Http(response.status().as_u16()),
            ));
        }
        let mut sink = crate::history_attachment::BodySink::create(&temp_path, body_max)
            .map_err(map_download_failure)?;
        sink.set_mac_key(&face.mac_key);
        let mut stream = response.bytes_stream();
        while let Some(chunk) = futures_util::StreamExt::next(&mut stream).await {
            let chunk = chunk.map_err(|_| {
                map_download_failure(crate::history_attachment::DownloadFailure::Network)
            })?;
            sink.push(&chunk).map_err(|failure| {
                cleanup_temp(&temp_path);
                map_download_failure(failure)
            })?;
        }
        let finished = sink.finish().map_err(|failure| {
            cleanup_temp(&temp_path);
            map_download_failure(failure)
        })?;
        // Ciphertext gates: terminal HMAC, then the whole-body digest face.
        finished.verify_ciphertext(&face).map_err(|failure| {
            cleanup_temp(&temp_path);
            map_download_failure(failure)
        })?;
        // The decrypt pass is blocking file work — off the async workers.
        let decrypt = {
            let temp_path = temp_path.clone();
            let plaintext_path = plaintext_path.clone();
            let face = face.clone();
            tokio::task::spawn_blocking(move || {
                crate::history_attachment::decrypt_to_file(&temp_path, &plaintext_path, &face)
            })
            .await
            .unwrap_or(Err(crate::history_attachment::DownloadFailure::Io))
        };
        let size_bytes = decrypt.map_err(|failure| {
            cleanup_temp(&temp_path);
            cleanup_temp(&plaintext_path);
            map_download_failure(failure)
        })?;
        // Land the plaintext atomically; the ciphertext temp never outlives
        // the chain, win or lose.
        std::fs::remove_file(&temp_path).map_err(|_| ServiceError::Io)?;
        std::fs::rename(&plaintext_path, &target).map_err(|_| {
            cleanup_temp(&plaintext_path);
            ServiceError::Io
        })?;
        Ok(
            serde_json::to_value(HistoryAttachmentDownloadView { size_bytes })
                .unwrap_or(Value::Null),
        )
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

    /// The read-only per-group snapshot the `runtime.doctor` face (§4.79)
    /// consumes: the launcher-config identity plus the supervised engine
    /// status and a live engine handle when one exists. Read-only by
    /// contract — it never starts, stops, or writes anything; the doctor's
    /// engine method probes ride the handle's read-only call path.
    pub(crate) async fn doctor_face(&self) -> crate::doctor::DoctorGroupFace {
        // The credentials probe target first — the same any-account-of-this-
        // group rule the watchdog ping uses, taken before the engine mutex so
        // the store read never holds it across SQLite I/O. The number rides
        // only into the engine call params — it never enters the report.
        let probe_account = self
            .service
            .lock()
            .await
            .any_signal_account_number_in_group(&self.group_id)
            .ok()
            .flatten();
        let slot = self.engine.lock().await;
        let live = slot.as_ref().filter(|engine| !engine.is_terminal());
        crate::doctor::DoctorGroupFace {
            data_dir: self.config.data_dir.clone(),
            status: slot
                .as_ref()
                .map(EngineHandle::status)
                .unwrap_or(EngineStatus {
                    state: EngineState::Stopped,
                    pid: None,
                    rss_bytes: None,
                    resource_pressure: false,
                }),
            engine: live.cloned(),
            probe_account,
        }
    }

    pub async fn start(&self) -> Result<EngineStatus, EngineError> {
        let mut slot = self.engine.lock().await;
        if let Some(engine) = slot.as_ref()
            && !engine.is_terminal()
        {
            return Err(EngineError::Backpressure);
        }
        let engine = EngineHandle::start(
            self.config.clone(),
            self.events.clone(),
            self.receive_ingress.clone(),
        )
        .await?;
        let status = engine.status();
        *slot = Some(engine);
        drop(slot);
        if status.state == EngineState::Running {
            self.spawn_contacts_resync();
            self.spawn_history_import_pass();
        }
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
            && let Some(name) = display_name_from_contacts(&result, number)
        {
            return Some(name);
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
            && let Some(name) = display_name_from_contacts(&result, number)
        {
            return Some(name);
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
        if let Ok(engine) = self.running_engine().await
            && let Some(name) = self.fetch_self_display_name(&engine, number).await
            && let Ok(updated) = self
                .service
                .lock()
                .await
                .set_account_display_name(&account.id, Some(name.as_str()))
        {
            account = updated;
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
        // §4.38: the engine starts the backup5 archive download in the
        // background when finishLink returns; watch for the file and import.
        // Best-effort — a watcher failure never fails the link flow.
        self.spawn_history_import_watch(account.id.clone());
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
        let fresh = engine.clone();
        *slot = Some(engine);
        drop(slot);
        self.spawn_contacts_resync_with(fresh);
        self.spawn_history_import_pass();
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
                        Err(EngineError::Unauthorized) => {
                            // Contract 1.21: the ping target's device was
                            // unlinked by the account holder. The credential
                            // is dead and no restart can resurrect it; the
                            // account-unauthorized event has already marked
                            // the account. Counting this as a ping failure
                            // would only churn bounded restarts forever.
                            ping_failures = 0;
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
            Some(operation_id) => {
                // Bind the plan before matching: the lock guard borrowed by the
                // prepare call would otherwise live for the whole match, and the
                // Completed arm re-locks below.
                let plan = self
                    .service
                    .lock()
                    .await
                    .prepare_account_delete(&account_id, operation_id)?;
                match plan {
                    // Idempotent replay of an already-completed delete: the
                    // account session is long gone, so any straggler media
                    // handles from before are released here too.
                    AccountDeletePlan::Completed => {
                        self.service
                            .lock()
                            .await
                            .clear_media_handles_for_account(&account_id);
                        if let Some(handles) = self.backup_handles.as_ref() {
                            handles.clear_account(&account_id);
                        }
                        return Ok(json!({}));
                    }
                    AccountDeletePlan::Dispatch {
                        signal_account,
                        reconcile_first,
                    } => (signal_account, reconcile_first),
                }
            }
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
            {
                let mut service = self.service.lock().await;
                service.complete_account_delete(&account_id, operation_id.as_deref())?;
                service.clear_media_handles_for_account(&account_id);
            }
            if let Some(handles) = self.backup_handles.as_ref() {
                handles.clear_account(&account_id);
            }
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
        {
            let mut service = self.service.lock().await;
            service.complete_account_delete(&account_id, operation_id.as_deref())?;
            // ADR 0002 gate 3: a chunk-stream handle cannot outlive the
            // account session it was opened from — media and §4.46 backup
            // file streams alike.
            service.clear_media_handles_for_account(&account_id);
        }
        if let Some(handles) = self.backup_handles.as_ref() {
            handles.clear_account(&account_id);
        }
        Ok(json!({}))
    }

    pub async fn list_conversations(
        &self,
        account_id: String,
        limit: u32,
        cursor: Option<String>,
    ) -> Result<Page<ConversationSummary>, ServiceError> {
        // Best-effort title enrich only when masks remain or a fresh direct
        // row still owes its §4.58 factor probe — and at most every 60s so
        // poll/UI refresh does not hammer listContacts on the single engine
        // queue.
        let _ = self.enrich_conversation_faces_throttled(&account_id).await;
        // Store-only read: the service lock guards in-memory state only.
        let store = Arc::clone(&self.store);
        blocking_service(move || {
            store_list_conversations(&store, &account_id, limit, cursor.as_deref())
        })
        .await
    }

    /// Resolve human titles for direct chats still showing mask_address(peer)
    /// and, on the same one listContacts round trip, stamp the §4.58
    /// message-request factor trio. Throttled to at most one engine call per
    /// 60s; the call fires only when a mask remains or a fresh direct row has
    /// never been probed, so a fully-enriched account costs no engine traffic.
    async fn enrich_conversation_faces_throttled(
        &self,
        account_id: &str,
    ) -> Result<(), ServiceError> {
        let (peers, mrr_missing) = tokio::task::spawn_blocking({
            let store = Arc::clone(&self.store);
            let account_id = account_id.to_string();
            move || {
                let peers = store.list_direct_peers_needing_title(&account_id)?;
                // The §4.58 probe only widens the trigger: a page with masks
                // to resolve fires anyway, so the count is read only when the
                // title fast path would otherwise skip the engine call. The
                // counter itself only owes rows whose derivation can flip on
                // a stamp (data window without outgoing rows), so the probe
                // never wakes for rows the window weight already settles.
                let mrr_missing = if peers.is_empty() {
                    store.count_direct_conversations_without_mrr_factors(&account_id)?
                } else {
                    0
                };
                Ok((peers, mrr_missing))
            }
        })
        .await
        .unwrap_or(Err(StoreError::Unavailable(None)))?;
        if peers.is_empty() && mrr_missing == 0 {
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
        // The row updates are store-only; they run batched on the blocking
        // pool without the service lock.
        tokio::task::spawn_blocking({
            let store = Arc::clone(&self.store);
            let account_id = account_id.to_string();
            move || {
                for (peer_key, _title) in peers {
                    if let Some(name) = find_contact_display_name(&items, &peer_key) {
                        let _ = store.set_conversation_title_for_peer(
                            &account_id,
                            "direct",
                            &peer_key,
                            &name,
                        );
                    }
                }
                // Contract 1.65 (§4.58): the factor stamp rides the same
                // answer. Every engine contact with a direct conversation row
                // gets exactly the flags the engine exposed; an item without
                // any of the three (the kt-engine read face today) stamps the
                // probed-absent `{}` so the probe never re-fires for that
                // row. Per-item failures are warn-silent by the same
                // discipline as the title pass — one bad stamp never fails
                // the list that triggered the enrich.
                for item in &items {
                    let Some(peer_key) = contact_item_peer_key(item) else {
                        continue;
                    };
                    let factors = mrr_factors_stamp(item);
                    let _ = store.set_conversation_mrr_factors_for_peer(
                        &account_id,
                        peer_key,
                        &factors,
                    );
                }
            }
        })
        .await
        .map_err(|_| StoreError::Unavailable(None))?;
        Ok(())
    }

    pub async fn list_messages(
        &self,
        account_id: String,
        conversation_id: String,
        limit: u32,
        before: Option<String>,
    ) -> Result<Page<MessageRecord>, ServiceError> {
        // Store-only read: the service lock guards in-memory state only.
        let store = Arc::clone(&self.store);
        blocking_service(move || {
            store_list_messages(
                &store,
                &account_id,
                &conversation_id,
                limit,
                before.as_deref(),
            )
        })
        .await
    }

    pub async fn list_adjacent_media(
        &self,
        account_id: String,
        conversation_id: String,
        anchor_message_id: String,
        direction: crate::store::AdjacentMediaDirection,
        limit: u32,
    ) -> Result<Vec<MessageRecord>, ServiceError> {
        // Store-only read: the service lock guards in-memory state only.
        let store = Arc::clone(&self.store);
        blocking_service(move || {
            store_adjacent_media(
                &store,
                &account_id,
                &conversation_id,
                &anchor_message_id,
                direction,
                limit,
            )
        })
        .await
    }

    pub async fn search_messages(
        &self,
        account_id: String,
        query: String,
        limit: u32,
        cursor: Option<String>,
    ) -> Result<Page<MessageRecord>, ServiceError> {
        // Store-only read: the service lock guards in-memory state only.
        let store = Arc::clone(&self.store);
        blocking_service(move || {
            store_search_messages(&store, &account_id, &query, limit, cursor.as_deref())
        })
        .await
    }

    pub async fn get_message_text(
        &self,
        account_id: String,
        conversation_id: String,
        message_id: String,
    ) -> Result<crate::service::MessageText, ServiceError> {
        // Store-only read: the service lock guards in-memory state only.
        let store = Arc::clone(&self.store);
        blocking_service(move || {
            store_get_message_text(&store, &account_id, &conversation_id, &message_id)
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn send_text(
        &self,
        account_id: String,
        target: SendTarget,
        text: String,
        client_request_id: String,
        quote_message_id: Option<String>,
        previews: Option<Vec<SendTextPreviewParams>>,
        mentions: Option<Vec<SendTextMentionParams>>,
        text_styles: Option<Vec<SendTextStyleParams>>,
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
                    previews,
                    mentions,
                    text_styles,
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
                    previews,
                    mentions,
                    text_styles,
                )?,
            }
        };
        self.dispatch_prepared(&engine, prepared).await
    }

    /// Retry one definitively-failed outgoing text in place (contract 1.31,
    /// implementation-plan §4.26). Same settlement path as `send_text` — the
    /// pending row rearmed by `prepare_retry_text` keeps its original id, so
    /// success / unknown / failure all complete the SAME row the desktop
    /// bubble renders and no superseded duplicate row can appear.
    pub async fn retry_text(
        &self,
        account_id: String,
        conversation_id: String,
        client_request_id: String,
    ) -> Result<MessageRecord, ServiceError> {
        let engine = self.running_engine().await?;
        let prepared = {
            let service = self.service.lock().await;
            service.prepare_retry_text(&account_id, &conversation_id, &client_request_id)?
        };
        self.dispatch_prepared(&engine, prepared).await
    }

    async fn dispatch_prepared(
        &self,
        engine: &EngineHandle,
        prepared: PreparedSend,
    ) -> Result<MessageRecord, ServiceError> {
        match prepared {
            PreparedSend::Existing(message) => Ok(*message),
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

    /// messages.sendStory (contract revision 1.76, §4.67): one bounded story
    /// text out through the engine's `sendStory` face (engine compat §3
    /// send batch). Stories live outside every conversation — no local row,
    /// no `clientRequestId`, no pending settlement: prepare under the
    /// service lock, one mutating call, and the answer is the upstream
    /// truth projected (`story_from_engine`). An indeterminate mutating
    /// outcome answers SEND_OUTCOME_UNKNOWN and is never retried (the §5
    /// send discipline, rowless variant — there is no pending row to
    /// settle, and a story is idempotent upstream by its own timestamp).
    pub async fn send_story(
        &self,
        params: MessagesSendStoryParams,
    ) -> Result<StorySendResult, ServiceError> {
        let engine = self.running_engine().await?;
        let frame = {
            let service = self.service.lock().await;
            service.prepare_send_story(&params)?
        };
        match engine.call("sendStory", frame, CallClass::Mutating).await {
            Ok(result) => story_from_engine(&result),
            Err(EngineError::UnknownOutcome) => Err(ServiceError::Api(ApiError::new(
                "SEND_OUTCOME_UNKNOWN",
                "mutating request has an unknown outcome",
                false,
            ))),
            Err(error) => Err(error.into()),
        }
    }

    /// stories.setDisabled (contract revision 1.76, §4.67): the account's
    /// stories-disabled toggle — the receive-gate mirror of the official
    /// `getStoriesBlocked` leg. Local store only (no engine frame exists):
    /// validation + account pin under the
    /// service lock, then the meta-KV write on the blocking pool. The
    /// result is the stored truth, not an echo of the request.
    pub async fn set_stories_disabled(
        &self,
        params: StoriesSetDisabledParams,
    ) -> Result<StoriesDisabled, ServiceError> {
        {
            let service = self.service.lock().await;
            service.prepare_stories_set_disabled(&params)?;
        }
        let store = Arc::clone(&self.store);
        let account_id = params.account_id;
        let disabled = params.disabled;
        blocking_service(move || {
            store.set_stories_disabled(&account_id, disabled)?;
            Ok(StoriesDisabled { disabled })
        })
        .await
    }

    /// stories.getDisabled (contract revision 1.76, §4.67): the same local
    /// toggle, read side — an account with no stored state answers the
    /// official default (stories enabled).
    pub async fn get_stories_disabled(
        &self,
        params: StoriesGetDisabledParams,
    ) -> Result<StoriesDisabled, ServiceError> {
        {
            let service = self.service.lock().await;
            service.prepare_stories_get_disabled(&params)?;
        }
        let store = Arc::clone(&self.store);
        let account_id = params.account_id;
        blocking_service(move || {
            Ok(StoriesDisabled {
                disabled: store.stories_disabled(&account_id)?,
            })
        })
        .await
    }

    /// stories.markViewed (contract revision 1.85, §4.70): the story-anchored
    /// viewed receipt — the `messages.markViewed` receipt class verbatim
    /// (best-effort, `{status: sent|unknown}`, any upstream failure
    /// degrades and nothing retries) with the §4.29 row selection replaced
    /// by the story anchor frame. One mutating `sendViewedReceipt` call
    /// upstream (the author is a single recipient, one timestamp per frame —
    /// there is no fan-out: a story receipt names exactly one story).
    pub async fn send_story_viewed(
        &self,
        params: StoriesMarkViewedParams,
    ) -> Result<&'static str, ServiceError> {
        let engine = self.running_engine().await?;
        let frame = {
            let service = self.service.lock().await;
            service.prepare_story_mark_viewed(&params)?
        };
        match engine
            .call("sendViewedReceipt", frame, CallClass::Mutating)
            .await
        {
            Ok(_) => Ok("sent"),
            // Receipt-class degradation: the pin-family discipline — a
            // definite upstream error, transport trouble, or an unknown
            // outcome only degrades the answer; nothing is ever retried
            // (AGENTS.md).
            Err(_) => Ok("unknown"),
        }
    }

    /// stories.sendReaction (contract 1.94, §4.77): the story-anchored
    /// reaction send — the `messages.sendReaction` send class verbatim
    /// (confirmed `sent`, an indeterminate outcome `unknown`, a definite
    /// error propagates, nothing retries) with the story anchor frame in
    /// place of the row-addressed one. No local write and no event: a story
    /// reaction has no row to pill on (stories are rowless, §4.67).
    pub async fn send_story_reaction(
        &self,
        params: StoriesSendReactionParams,
    ) -> Result<&'static str, ServiceError> {
        let engine = self.running_engine().await?;
        let frame = {
            let service = self.service.lock().await;
            service.prepare_story_send_reaction(&params)?
        };
        match engine
            .call("sendReaction", frame, CallClass::Mutating)
            .await
        {
            Ok(_) => Ok("sent"),
            Err(EngineError::UnknownOutcome) => Ok("unknown"),
            Err(error) => Err(error.into()),
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
        voice_note: bool,
        view_once: bool,
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
                voice_note,
                view_once,
            )?
        };
        self.dispatch_prepared(&engine, prepared).await
    }

    /// Send one message carrying several attachments (contract revision
    /// 1.66, §4.59). Identical settlement path to `send_attachment`: local
    /// prepare under the service lock, one upstream mutating `send` call
    /// whose array-typed `attachments` carries every entry, pending row
    /// settled with the same no-auto-retry discipline.
    pub async fn send_attachments_multi(
        &self,
        account_id: String,
        target: SendTarget,
        client_request_id: String,
        entries: Vec<crate::service::MessagesSendAttachmentEntry>,
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
            service.prepare_send_attachment_multi(
                &account_id,
                &attachment_target,
                &client_request_id,
                &entries,
                text.as_deref(),
                quote_message_id.as_deref(),
            )?
        };
        self.dispatch_prepared(&engine, prepared).await
    }

    /// messages.sendViewOnceOpen (contract revision 1.38, §4.36): the bare
    /// engine `sendViewOnceOpen` passthrough under receipt-class discipline —
    /// account resolution still fails deterministically, but every upstream
    /// failure (engine down, method absent, unknown outcome) degrades to
    /// `{status: "unknown"}` and nothing is ever retried (AGENTS.md).
    pub async fn send_view_once_open(
        &self,
        params: crate::service::MessagesSendViewOnceOpenParams,
    ) -> Result<&'static str, ServiceError> {
        let prepared = {
            let service = self.service.lock().await;
            service.prepare_send_view_once_open(&params)?
        };
        let engine = match self.running_engine().await {
            Ok(engine) => engine,
            Err(_) => return Ok("unknown"),
        };
        match engine
            .call("sendViewOnceOpen", prepared, CallClass::Mutating)
            .await
        {
            Ok(_) => Ok("sent"),
            Err(_) => Ok("unknown"),
        }
    }

    /// messages.markViewOnceOpened (contract revision 1.38, §4.37): the
    /// desktop's single view-once trigger point. The burn already ran inside
    /// prepare (it never depends on upstream); this method emits the one
    /// `message.viewOnceOpened` event the transition earned, then runs the
    /// official upstream pair — `sendViewedReceipt` to the author, then
    /// `sendViewOnceOpen` — in order under receipt-class discipline: any
    /// failure degrades the answer to `unknown`, and nothing is ever retried.
    /// A replay (already burned) skips the fan-out and answers a trivial
    /// `sent` no-op.
    pub async fn mark_view_once_opened(
        &self,
        params: crate::service::MessagesMarkViewOnceOpenedParams,
    ) -> Result<&'static str, ServiceError> {
        let prepared = {
            let service = self.service.lock().await;
            service.prepare_mark_view_once_opened(&params)?
        };
        for event in prepared.events {
            let _ = self.host_events.send(event);
        }
        if prepared.receipt_recipient.is_none() && prepared.open_sender_aci.is_none() {
            return Ok("sent");
        }
        let engine = match self.running_engine().await {
            Ok(engine) => engine,
            Err(_) => return Ok("unknown"),
        };
        let mut all_confirmed = true;
        if let Some(recipient) = &prepared.receipt_recipient {
            let params = json!({
                "account": prepared.account_signal,
                "recipient": recipient,
                "timestamps": [prepared.timestamp],
            });
            match engine
                .call("sendViewedReceipt", params, CallClass::Mutating)
                .await
            {
                Ok(_) => {}
                Err(_) => all_confirmed = false,
            }
        }
        if let Some(sender_aci) = &prepared.open_sender_aci {
            let params = json!({
                "account": prepared.account_signal,
                "senderAci": sender_aci,
                "timestamp": prepared.timestamp,
            });
            match engine
                .call("sendViewOnceOpen", params, CallClass::Mutating)
                .await
            {
                Ok(_) => {}
                Err(_) => all_confirmed = false,
            }
        }
        Ok(if all_confirmed { "sent" } else { "unknown" })
    }

    /// messages.sendSticker (contract revision 1.35, §4.31; quote face 1.37,
    /// §4.35): the §4.12 attachment shape end to end — local prepare under
    /// the service lock (addressing resolved like `messages.attachments.send`,
    /// exactly one form; the optional quote resolved through the shared
    /// ladder before the pending row), upstream `send` with the `sticker`
    /// object via the shared `dispatch_prepared` settlement (success completes
    /// the pending row with the upstream timestamp; an unknown mutating
    /// outcome answers SEND_OUTCOME_UNKNOWN and is never retried).
    pub async fn send_sticker(
        &self,
        params: MessagesSendStickerParams,
    ) -> Result<MessageRecord, ServiceError> {
        let engine = self.running_engine().await?;
        let prepared = {
            let service = self.service.lock().await;
            let MessagesSendStickerParams {
                account_id,
                conversation_id,
                kind,
                peer_key,
                peer_title,
                client_request_id,
                pack_id,
                pack_key,
                sticker_id,
                emoji,
                image,
                quote_message_id,
            } = params;
            let target = match (&conversation_id, &kind, &peer_key, &peer_title) {
                (Some(conversation_id), None, None, None) => {
                    AttachmentSendTarget::Conversation(conversation_id)
                }
                (None, Some(kind), Some(peer_key), peer_title) => {
                    AttachmentSendTarget::Peer(PeerTarget {
                        kind,
                        peer_key,
                        peer_title: peer_title.as_deref(),
                    })
                }
                _ => {
                    return Err(ServiceError::Api(ApiError::new(
                        "INVALID_REQUEST",
                        "exactly one of conversationId or kind+peerKey must be provided",
                        false,
                    )));
                }
            };
            service.prepare_send_sticker(
                &account_id,
                &target,
                &client_request_id,
                &pack_id,
                &pack_key,
                sticker_id,
                emoji.as_deref(),
                &image,
                quote_message_id.as_deref(),
            )?
        };
        self.dispatch_prepared(&engine, prepared).await
    }

    /// stickerPacks.getManifest (contract revision 1.36, §4.32): bounds
    /// validation under the service lock, then the read-only engine call —
    /// anonymous CDN work engine-side, no account context, nothing
    /// persisted. A timeout answers the ordinary retryable UPSTREAM_TIMEOUT
    /// (a read provably took no effect), and structured engine errors
    /// (STICKER_*) surface through the §4.32 mapping.
    pub async fn get_sticker_pack_manifest(
        &self,
        params: StickerPackManifestParams,
    ) -> Result<StickerPackManifest, ServiceError> {
        let engine = self.running_engine().await?;
        {
            let service = self.service.lock().await;
            service.prepare_sticker_pack_manifest(&params)?;
        }
        let result = engine
            .call(
                "getStickerPackManifest",
                json!({
                    "packId": params.pack_id,
                    "packKey": params.pack_key,
                }),
                CallClass::ReadOnly,
            )
            .await?;
        manifest_from_engine(&result)
    }

    /// stickerPacks.getImage (contract revision 1.36, §4.32): the same
    /// shape as the manifest face with the decrypted sticker bytes as the
    /// result — base64 pass-through, `size` cross-checked against the
    /// engine's declared byte count so a mismatching result fails as a
    /// protocol error instead of shipping a truncated image.
    pub async fn get_sticker_image(
        &self,
        params: StickerPackImageParams,
    ) -> Result<StickerPackImage, ServiceError> {
        let engine = self.running_engine().await?;
        {
            let service = self.service.lock().await;
            service.prepare_sticker_pack_image(&params)?;
        }
        let result = engine
            .call(
                "getStickerImage",
                json!({
                    "packId": params.pack_id,
                    "packKey": params.pack_key,
                    "stickerId": params.sticker_id,
                }),
                CallClass::ReadOnly,
            )
            .await?;
        image_from_engine(&result)
    }

    /// conversations.getPinned (contract revision 1.36, §4.33): account
    /// resolution then the read-only engine call; the pinned list projects
    /// straight from the engine result with the KT entry cap (§4.33).
    pub async fn get_pinned_conversations(
        &self,
        params: ConversationsGetPinnedParams,
    ) -> Result<PinnedConversations, ServiceError> {
        let engine = self.running_engine().await?;
        let upstream = {
            let service = self.service.lock().await;
            service.prepare_conversations_get_pinned(&params.account_id)?
        };
        let result = engine
            .call("getPinnedConversations", upstream, CallClass::ReadOnly)
            .await?;
        Ok(pinned_conversations_result(
            result
                .get("pinned")
                .and_then(Value::as_array)
                .ok_or(ServiceError::Engine(EngineError::Protocol))?,
        ))
    }

    /// conversations.setPinned (contract revision 1.36, §4.33): bounds +
    /// resolution under the service lock, then the mutating engine call —
    /// reaction-class discipline: an unknown outcome answers
    /// SEND_OUTCOME_UNKNOWN and is never retried; the success result is the
    /// post-write cloud state read back, not a local echo.
    pub async fn set_conversation_pinned(
        &self,
        params: ConversationsSetPinnedParams,
    ) -> Result<PinnedConversations, ServiceError> {
        let engine = self.running_engine().await?;
        let upstream = {
            let service = self.service.lock().await;
            service.prepare_conversations_set_pinned(
                &params.account_id,
                &params.conversation_id,
                &params.kind,
                params.pinned,
            )?
        };
        match engine
            .call("setConversationPinned", upstream, CallClass::Mutating)
            .await
        {
            Ok(result) => Ok(pinned_conversations_result(
                result
                    .get("pinned")
                    .and_then(Value::as_array)
                    .ok_or(ServiceError::Engine(EngineError::Protocol))?,
            )),
            Err(EngineError::UnknownOutcome) => Err(ServiceError::Api(ApiError::new(
                "SEND_OUTCOME_UNKNOWN",
                "mutating request has an unknown outcome",
                false,
            ))),
            Err(error) => Err(error.into()),
        }
    }

    /// conversations.setExpireTimer (contract revision 1.42, §4.41): bounds +
    /// resolution + the mode-shaped upstream payload under the service lock,
    /// then the mutating engine call — reaction-class discipline, an unknown
    /// outcome answers SEND_OUTCOME_UNKNOWN and is never retried. On a
    /// definite success the conversation's local mirror row updates and the
    /// refreshed summary rides conversation.changed, so every window sees the
    /// new timer without a re-read; the upstream timer state stays the source
    /// of truth. The direct route is the engine's `setExpirationTimer`; a
    /// group rides `updateGroup` — same-value no-ops are upstream semantics,
    /// the passthrough never pre-checks.
    pub async fn set_conversation_expire_timer(
        &self,
        params: ConversationsSetExpireTimerParams,
    ) -> Result<ConversationsSetExpireTimerResult, ServiceError> {
        let engine = self.running_engine().await?;
        let (method, upstream, seconds) = {
            let service = self.service.lock().await;
            service.prepare_conversations_set_expire_timer(
                &params.account_id,
                &params.conversation_id,
                params.expire_seconds,
            )?
        };
        match engine.call(method, upstream, CallClass::Mutating).await {
            Ok(_) => {
                // Local mirror best-effort: the upstream write is the state;
                // a mirror failure only costs the next event its freshness.
                if self
                    .store
                    .set_conversation_expire_timer(
                        &params.account_id,
                        &params.conversation_id,
                        seconds,
                    )
                    .unwrap_or(false)
                    && let Some(conversation) = self
                        .store
                        .conversation_summary(&params.account_id, &params.conversation_id)?
                {
                    let _ = self
                        .host_events
                        .send(HostSideEvent::ConversationChanged(conversation));
                }
                Ok(ConversationsSetExpireTimerResult {
                    expire_timer_seconds: seconds,
                })
            }
            Err(EngineError::UnknownOutcome) => Err(ServiceError::Api(ApiError::new(
                "SEND_OUTCOME_UNKNOWN",
                "mutating request has an unknown outcome",
                false,
            ))),
            Err(error) => Err(error.into()),
        }
    }

    /// groups.update (contract revision 1.45, §4.44, capability
    /// group-management; the engine write face wired by contract 1.115,
    /// §4.96): full local validation + resolution under the service lock,
    /// then the mutating `updateGroup` call — reaction-class discipline with
    /// the timer face's unknown-outcome honesty: an indeterminate answer is
    /// the SEND_OUTCOME_UNKNOWN error and is never retried. The engine's
    /// deny faces (unknown account/group, non-member, server user errors)
    /// are deterministic refusals: UPSTREAM_ERROR with retryable=false — a
    /// bare replay fails the same way. Zero local writes: the group row
    /// converges through the receive projection when the change's own
    /// group-update envelope lands.
    pub async fn update_group(
        &self,
        params: GroupsUpdateParams,
    ) -> Result<&'static str, ServiceError> {
        let engine = self.running_engine().await?;
        let upstream = {
            let service = self.service.lock().await;
            service.prepare_groups_update(&params)?
        };
        match engine
            .call("updateGroup", upstream, CallClass::Mutating)
            .await
        {
            Ok(_) => Ok("sent"),
            Err(EngineError::UnknownOutcome) => Err(ServiceError::Api(ApiError::new(
                "SEND_OUTCOME_UNKNOWN",
                "mutating request has an unknown outcome",
                false,
            ))),
            Err(EngineError::Upstream) => Err(ServiceError::Api(ApiError::new(
                "UPSTREAM_ERROR",
                "signal-cli returned an error",
                false,
            ))),
            Err(error) => Err(error.into()),
        }
    }

    /// groups.quit (contract revision 1.45, §4.44, capability
    /// group-management; the engine write face wired by contract 1.115,
    /// §4.96): resolution under the service lock, then the mutating
    /// `leaveGroup` call — the engine's method name for the upstream quit
    /// write — under the same unknown-outcome discipline as groups.update,
    /// with the same deterministic deny face (a non-member or last-admin
    /// quit is UPSTREAM_ERROR retryable=false). The local conversation row
    /// is kept; non-member state is upstream convergence.
    pub async fn quit_group(&self, params: GroupsQuitParams) -> Result<&'static str, ServiceError> {
        let engine = self.running_engine().await?;
        let upstream = {
            let service = self.service.lock().await;
            service.prepare_groups_quit(&params)?
        };
        match engine
            .call("leaveGroup", upstream, CallClass::Mutating)
            .await
        {
            Ok(_) => Ok("sent"),
            Err(EngineError::UnknownOutcome) => Err(ServiceError::Api(ApiError::new(
                "SEND_OUTCOME_UNKNOWN",
                "mutating request has an unknown outcome",
                false,
            ))),
            Err(EngineError::Upstream) => Err(ServiceError::Api(ApiError::new(
                "UPSTREAM_ERROR",
                "signal-cli returned an error",
                false,
            ))),
            Err(error) => Err(error.into()),
        }
    }

    /// groups.terminate (contract revision 1.52, §4.50, capability
    /// group-terminate): resolution under the service lock, then the
    /// mutating `terminateGroup` call — the same unknown-outcome discipline
    /// as groups.update/groups.quit. Zero local writes: the terminated state
    /// converges through the receive projection when the change's own
    /// group-update envelope lands.
    pub async fn terminate_group(
        &self,
        params: GroupsTerminateParams,
    ) -> Result<&'static str, ServiceError> {
        let engine = self.running_engine().await?;
        let upstream = {
            let service = self.service.lock().await;
            service.prepare_groups_terminate(&params)?
        };
        match engine
            .call("terminateGroup", upstream, CallClass::Mutating)
            .await
        {
            Ok(_) => Ok("sent"),
            Err(EngineError::UnknownOutcome) => Err(ServiceError::Api(ApiError::new(
                "SEND_OUTCOME_UNKNOWN",
                "mutating request has an unknown outcome",
                false,
            ))),
            Err(error) => Err(error.into()),
        }
    }

    /// groups.approveJoinRequests (contract revision 1.58, §4.53, capability
    /// group-join-approve): the safety fold, then the mutating upstream
    /// write. The pinned upstream weaves approval into the updateGroup
    /// member semantics (`GroupHelper.updateGroupV2`: `members` entries
    /// that are requesting members ride `approveJoinRequestMembers`,
    /// `removeMembers` entries that are requesting members ride
    /// `refuseJoinRequestMembers` — every other entry is an add/remove
    /// member change), so the connector folds the caller's candidates
    /// against the engine's own requesting-members read
    /// (`listGroups {account, groupId}` → JsonGroup.requestingMembers)
    /// before anything goes upstream: only candidates the engine itself
    /// reports as requesting can ever ride the member arrays, so the
    /// "add a stranger" / "remove a member" mis-target faces of the shared
    /// arrays fail closed here instead of upstream. A fully-stale candidate
    /// set (the fold empties it) answers
    /// `JOIN_REQUEST_NO_LONGER_PENDING` (retryable) — the request was
    /// resolved elsewhere (another admin acted, the candidate withdrew); a
    /// read face that cannot answer (engine refuses the group read) fails
    /// closed with that refusal. The write is the groups.terminate
    /// discipline verbatim: one mutating `updateGroup` call, a definite
    /// success answers `{"status": "sent"}`, an indeterminate outcome
    /// answers SEND_OUTCOME_UNKNOWN and is never retried. Zero local
    /// writes: the roster converges through contacts.sync.
    pub async fn approve_join_requests(
        &self,
        params: GroupsApproveJoinRequestsParams,
    ) -> Result<&'static str, ServiceError> {
        let engine = self.running_engine().await?;
        let (group_id, members, approve) = {
            let service = self.service.lock().await;
            service.prepare_groups_approve_join_requests(&params)?
        };
        let number = self
            .service
            .lock()
            .await
            .account_signal_number(&params.account_id)?;
        // The engine's own read of who is still requesting membership (the
        // §4.50-verified listGroups read source; a single-group read keeps
        // the fold O(requesting) and the traffic bounded).
        let listed = engine
            .call(
                "listGroups",
                json!({ "account": number, "groupId": [group_id.clone()] }),
                CallClass::ReadOnly,
            )
            .await
            .map_err(ServiceError::Engine)?;
        let requesting: std::collections::HashSet<String> = requesting_member_identities(&listed);
        // The fold: the caller's candidates survive only while the engine
        // still reports them requesting. A requesting entry carries either
        // or both of its number/uuid identities (the JsonGroupMemberAddress
        // NON_NULL omissions), so both ride the same identity set and the
        // caller may name the candidate by either (the connector-wide
        // first-wins address ladder). Nothing else can ride the arrays.
        let folded: Vec<String> = fold_join_request_candidates(members, &requesting);
        if folded.is_empty() {
            return Err(ServiceError::Api(ApiError::new(
                "JOIN_REQUEST_NO_LONGER_PENDING",
                "no candidate is still requesting membership (already approved, refused, or withdrawn)",
                true,
            )));
        }
        // The upstream write: the requesting-survivor set rides exactly the
        // one member family its approve bit names, every other change key
        // absent — the upstream reads absence as "no change" (§4.44).
        let mut upstream = json!({
            "account": number,
            "groupId": group_id,
        });
        if let Some(object) = upstream.as_object_mut() {
            object.insert(
                if approve { "members" } else { "removeMembers" }.to_string(),
                json!(folded),
            );
        }
        match engine
            .call("updateGroup", upstream, CallClass::Mutating)
            .await
        {
            Ok(_) => Ok("sent"),
            Err(EngineError::UnknownOutcome) => Err(ServiceError::Api(ApiError::new(
                "SEND_OUTCOME_UNKNOWN",
                "mutating request has an unknown outcome",
                false,
            ))),
            Err(error) => Err(error.into()),
        }
    }

    /// groups.revokeInvites (contract revision 1.64, §4.57, capability
    /// group-invite-revoke): the safety fold, then the mutating upstream
    /// write. The pinned upstream weaves invite revocation into the
    /// updateGroup removeMembers semantics (`GroupHelper.updateGroupV2`:
    /// `removeMembers` entries that are pending members ride
    /// `revokeInvitedMembers` — :760-764 — while entries that are full
    /// members ride the member removal and entries that are requesting
    /// members ride `refuseJoinRequestMembers`; the three-way discrimination
    /// is upstream's), so the connector folds the caller's candidates
    /// against the engine's own pending-members read
    /// (`listGroups {account, groupId}` → JsonGroup.pendingMembers) before
    /// anything goes upstream: only candidates the engine itself reports as
    /// pending invitees can ever ride the `removeMembers` array, so the
    /// "remove a member" / "refuse a request" mis-target faces of the
    /// shared array fail closed here instead of upstream. A fully-stale
    /// candidate set (the fold empties it) answers
    /// `INVITE_NO_LONGER_PENDING` (retryable) — the invite was resolved
    /// elsewhere (another admin revoked it, the invitee joined); a read
    /// face that cannot answer (engine refuses the group read) fails closed
    /// with that refusal. The write is the groups.terminate discipline
    /// verbatim: one mutating `updateGroup` call, a definite success
    /// answers `{"status": "sent"}`, an indeterminate outcome answers
    /// SEND_OUTCOME_UNKNOWN and is never retried. Zero local writes: the
    /// roster converges through contacts.sync.
    pub async fn revoke_invites(
        &self,
        params: GroupsRevokeInvitesParams,
    ) -> Result<&'static str, ServiceError> {
        let engine = self.running_engine().await?;
        let (group_id, members) = {
            let service = self.service.lock().await;
            service.prepare_groups_revoke_invites(&params)?
        };
        let number = self
            .service
            .lock()
            .await
            .account_signal_number(&params.account_id)?;
        // The engine's own read of who is still a pending invitee (the same
        // listGroups read source the groups.get pendingMembers face
        // projects; a single-group read keeps the fold O(pending) and the
        // traffic bounded).
        let listed = engine
            .call(
                "listGroups",
                json!({ "account": number, "groupId": [group_id.clone()] }),
                CallClass::ReadOnly,
            )
            .await
            .map_err(ServiceError::Engine)?;
        let pending: std::collections::HashSet<String> = pending_member_identities(&listed);
        // The fold: the caller's candidates survive only while the engine
        // still reports them pending invitees. A pending entry carries
        // either or both of its number/uuid identities (the
        // JsonGroupMemberAddress NON_NULL omissions), so both ride the same
        // identity set and the caller may name the candidate by either (the
        // connector-wide first-wins address ladder). Nothing else can ride
        // the array — a full member or a requesting candidate in the
        // candidate set drops here, never into the upstream write.
        let folded: Vec<String> = fold_invite_candidates(members, &pending);
        if folded.is_empty() {
            return Err(ServiceError::Api(ApiError::new(
                "INVITE_NO_LONGER_PENDING",
                "no candidate is still a pending invitee (already revoked, joined, or withdrawn)",
                true,
            )));
        }
        // The upstream write: the pending-survivor set rides exactly the
        // `removeMembers` family, every other change key absent — the
        // upstream reads absence as "no change" (§4.44).
        let upstream = json!({
            "account": number,
            "groupId": group_id,
            "removeMembers": folded,
        });
        match engine
            .call("updateGroup", upstream, CallClass::Mutating)
            .await
        {
            Ok(_) => Ok("sent"),
            Err(EngineError::UnknownOutcome) => Err(ServiceError::Api(ApiError::new(
                "SEND_OUTCOME_UNKNOWN",
                "mutating request has an unknown outcome",
                false,
            ))),
            Err(error) => Err(error.into()),
        }
    }

    /// messages.reportSpam (contract revision 1.49, §4.48, capability
    /// report-spam): resolution and row reads under the service lock, then
    /// the mutating `reportSpam` call — the groups face's unknown-outcome
    /// discipline (a lost answer is the SEND_OUTCOME_UNKNOWN error and is
    /// never retried). The confirmed result projects the engine's
    /// `{reported, failures}` back onto the caller's message ids; rows
    /// without a serverGuid never reached upstream and answer as failures.
    pub async fn report_spam(
        &self,
        params: MessagesReportSpamParams,
    ) -> Result<ReportSpamResult, ServiceError> {
        let engine = self.running_engine().await?;
        let prepared = {
            let service = self.service.lock().await;
            service.prepare_report_spam(&params)?
        };
        match engine
            .call("reportSpam", prepared.upstream, CallClass::Mutating)
            .await
        {
            Ok(result) => {
                let reported = result
                    .get("reported")
                    .and_then(Value::as_u64)
                    .ok_or(ServiceError::Engine(EngineError::Protocol))?;
                let answered = result
                    .get("failures")
                    .and_then(Value::as_array)
                    .ok_or(ServiceError::Engine(EngineError::Protocol))?;
                // The engine answers failures by serverGuid with an optional
                // status (absent on transport failures — never invented as 0).
                let answered_by_guid = answered
                    .iter()
                    .filter_map(|entry| {
                        let guid = entry.get("serverGuid")?.as_str()?;
                        Some((guid, entry.get("status").and_then(Value::as_u64)))
                    })
                    .collect::<Vec<_>>();
                let mut failures = prepared
                    .pairs
                    .iter()
                    .filter_map(|(message_id, guid)| {
                        answered_by_guid
                            .iter()
                            .find(|(answered_guid, _)| answered_guid == guid)
                            .map(|(_, code)| ReportSpamFailure {
                                message_id: message_id.clone(),
                                code: *code,
                            })
                    })
                    .collect::<Vec<_>>();
                // Rows without a persisted serverGuid never reached upstream
                // (sent rows, history imports, unknown ids) — honest per-id
                // failures with no code, not a dropped or failed request.
                for message_id in &params.message_ids {
                    if !prepared.pairs.iter().any(|(id, _)| id == message_id) {
                        failures.push(ReportSpamFailure {
                            message_id: message_id.clone(),
                            code: None,
                        });
                    }
                }
                Ok(ReportSpamResult { reported, failures })
            }
            Err(EngineError::UnknownOutcome) => Err(ServiceError::Api(ApiError::new(
                "SEND_OUTCOME_UNKNOWN",
                "mutating request has an unknown outcome",
                false,
            ))),
            Err(error) => Err(error.into()),
        }
    }

    /// stickerPacks.getSyncs (contract revision 1.37, §4.34): account
    /// resolution then the read-only engine call; the record list projects
    /// straight from the engine result with the KT entry cap (§4.34).
    pub async fn get_sticker_pack_syncs(
        &self,
        params: StickerPackGetSyncsParams,
    ) -> Result<StickerPackSyncs, ServiceError> {
        let engine = self.running_engine().await?;
        let upstream = {
            let service = self.service.lock().await;
            service.prepare_sticker_pack_get_syncs(&params.account_id)?
        };
        let result = engine
            .call("getStickerPackSyncs", upstream, CallClass::ReadOnly)
            .await?;
        Ok(sticker_pack_syncs_result(
            result
                .get("packs")
                .and_then(Value::as_array)
                .ok_or(ServiceError::Engine(EngineError::Protocol))?,
        ))
    }

    /// stickerPacks.setSync (contract revision 1.37, §4.34): bounds under the
    /// service lock, then the mutating engine call — reaction-class
    /// discipline: an unknown outcome answers SEND_OUTCOME_UNKNOWN and is
    /// never retried; the success result is the post-write cloud state read
    /// back, not a local echo.
    pub async fn set_sticker_pack_sync(
        &self,
        params: StickerPackSetSyncParams,
    ) -> Result<StickerPackSyncs, ServiceError> {
        let engine = self.running_engine().await?;
        let upstream = {
            let service = self.service.lock().await;
            service.prepare_sticker_pack_set_sync(
                &params.account_id,
                &params.pack_id,
                params.pack_key.as_deref(),
                params.installed,
                params.position,
            )?
        };
        match engine
            .call("setStickerPackSync", upstream, CallClass::Mutating)
            .await
        {
            Ok(result) => Ok(sticker_pack_syncs_result(
                result
                    .get("packs")
                    .and_then(Value::as_array)
                    .ok_or(ServiceError::Engine(EngineError::Protocol))?,
            )),
            Err(EngineError::UnknownOutcome) => Err(ServiceError::Api(ApiError::new(
                "SEND_OUTCOME_UNKNOWN",
                "mutating request has an unknown outcome",
                false,
            ))),
            Err(error) => Err(error.into()),
        }
    }

    /// chatFolders.list (contract revision 1.95, §4.78): bounds and account
    /// resolution under the service lock, then the read-only engine call;
    /// the folder list projects straight from the engine result with the KT
    /// entry cap, record bodies verbatim.
    pub async fn chat_folders_list(
        &self,
        params: ChatFoldersListParams,
    ) -> Result<ChatFolderSyncs, ServiceError> {
        let upstream = {
            let service = self.service.lock().await;
            service.prepare_chat_folders_list(&params.account_id)?
        };
        let engine = self.running_engine().await?;
        let result = engine
            .call("chatFolders.list", upstream, CallClass::ReadOnly)
            .await?;
        Ok(chat_folder_syncs_result(
            result
                .get("folders")
                .and_then(Value::as_array)
                .ok_or(ServiceError::Engine(EngineError::Protocol))?,
        ))
    }

    /// chatFolders.setRecords (contract revision 1.95, §4.78): bounds under
    /// the service lock, then the mutating engine call — reaction-class
    /// discipline: an unknown outcome answers SEND_OUTCOME_UNKNOWN and is
    /// never retried; the success result is the post-write cloud state read
    /// back, not a local echo.
    pub async fn chat_folders_set_records(
        &self,
        params: ChatFoldersSetRecordsParams,
    ) -> Result<ChatFolderSyncs, ServiceError> {
        let upstream = {
            let service = self.service.lock().await;
            service.prepare_chat_folders_set_records(
                &params.account_id,
                params.records.as_deref().unwrap_or(&[]),
                params.remove_ids.as_deref().unwrap_or(&[]),
            )?
        };
        let engine = self.running_engine().await?;
        match engine
            .call("chatFolders.setRecords", upstream, CallClass::Mutating)
            .await
        {
            Ok(result) => Ok(chat_folder_syncs_result(
                result
                    .get("folders")
                    .and_then(Value::as_array)
                    .ok_or(ServiceError::Engine(EngineError::Protocol))?,
            )),
            Err(EngineError::UnknownOutcome) => Err(ServiceError::Api(ApiError::new(
                "SEND_OUTCOME_UNKNOWN",
                "mutating request has an unknown outcome",
                false,
            ))),
            Err(error) => Err(error.into()),
        }
    }

    /// accounts.updateConfiguration (contract revision 1.46, §4.45,
    /// capability account-settings): bounds under the service lock, then the
    /// mutating engine call — reaction-class discipline: an unknown outcome
    /// answers SEND_OUTCOME_UNKNOWN and is never retried. The result is the
    /// definite-success ack, not state: the settings live upstream and the
    /// desktop re-reads them through its own channel.
    pub async fn update_account_configuration(
        &self,
        params: AccountsUpdateConfigurationParams,
    ) -> Result<&'static str, ServiceError> {
        let engine = self.running_engine().await?;
        let upstream = {
            let service = self.service.lock().await;
            service.prepare_accounts_update_configuration(&params)?
        };
        match engine
            .call("setAccountConfiguration", upstream, CallClass::Mutating)
            .await
        {
            Ok(_) => Ok("saved"),
            Err(EngineError::UnknownOutcome) => Err(ServiceError::Api(ApiError::new(
                "SEND_OUTCOME_UNKNOWN",
                "mutating request has an unknown outcome",
                false,
            ))),
            Err(error) => Err(error.into()),
        }
    }

    /// accounts.setProfile (contract revision 1.46, §4.45, capability
    /// account-settings): bounds + the avatar tri-state under the service
    /// lock, then the mutating engine call — the reaction-class discipline
    /// verbatim. Zero local writes: the profile lives upstream.
    pub async fn set_profile(
        &self,
        params: AccountsSetProfileParams,
    ) -> Result<&'static str, ServiceError> {
        let engine = self.running_engine().await?;
        let upstream = {
            let service = self.service.lock().await;
            service.prepare_accounts_set_profile(&params)?
        };
        match engine
            .call("setProfile", upstream, CallClass::Mutating)
            .await
        {
            Ok(_) => Ok("saved"),
            Err(EngineError::UnknownOutcome) => Err(ServiceError::Api(ApiError::new(
                "SEND_OUTCOME_UNKNOWN",
                "mutating request has an unknown outcome",
                false,
            ))),
            Err(error) => Err(error.into()),
        }
    }

    /// accounts.setExpireTimer (contract revision 1.46, §4.45, capability
    /// account-settings): account resolution under the service lock, then the
    /// mutating engine call — the reaction-class discipline verbatim. Zero
    /// local writes: the universal timer lives upstream, other devices
    /// converge through storage sync.
    pub async fn set_account_expire_timer(
        &self,
        params: AccountsSetExpireTimerParams,
    ) -> Result<&'static str, ServiceError> {
        let engine = self.running_engine().await?;
        let upstream = {
            let service = self.service.lock().await;
            service.prepare_accounts_set_expire_timer(&params.account_id, params.expire_seconds)?
        };
        match engine
            .call("setUniversalExpireTimer", upstream, CallClass::Mutating)
            .await
        {
            Ok(_) => Ok("saved"),
            Err(EngineError::UnknownOutcome) => Err(ServiceError::Api(ApiError::new(
                "SEND_OUTCOME_UNKNOWN",
                "mutating request has an unknown outcome",
                false,
            ))),
            Err(error) => Err(error.into()),
        }
    }

    /// conversations.getBlocked (contract revision 1.46, §4.45, capability
    /// blocked-conversations): account resolution then the read-only engine
    /// call; the blocked list projects straight from the engine result with
    /// the KT entry cap (§4.45).
    pub async fn get_blocked_conversations(
        &self,
        params: ConversationsGetBlockedParams,
    ) -> Result<BlockedConversations, ServiceError> {
        let engine = self.running_engine().await?;
        let upstream = {
            let service = self.service.lock().await;
            service.prepare_conversations_get_blocked(&params.account_id)?
        };
        let result = engine
            .call("getBlockedConversations", upstream, CallClass::ReadOnly)
            .await?;
        Ok(blocked_conversations_result(
            result
                .get("blocked")
                .and_then(Value::as_array)
                .ok_or(ServiceError::Engine(EngineError::Protocol))?,
        ))
    }

    /// accounts.deviceCapabilities (contract revision 1.100, §4.81): account
    /// resolution then the read-only engine call — the idempotent device-face
    /// registration (the official registerCapabilities endpoint, PUT — the
    /// engine folds every failure into the plain -32603, the connector adds
    /// no matcher of its own) — and the projection that strips the engine's
    /// bare `account` identity before it crosses the connector.
    pub async fn get_device_capabilities(
        &self,
        params: AccountsDeviceCapabilitiesParams,
    ) -> Result<AccountDeviceCapabilities, ServiceError> {
        let engine = self.running_engine().await?;
        let upstream = {
            let service = self.service.lock().await;
            service.prepare_accounts_device_capabilities(&params.account_id)?
        };
        let result = engine
            .call("engine.deviceCapabilities", upstream, CallClass::ReadOnly)
            .await?;
        account_device_capabilities_from_engine(&result)
    }

    /// accounts.getDevices (contract revision 1.122, §4.103, capability
    /// device-management): account resolution then the read-only engine call —
    /// the GET v1/devices projection with the engine-side device-name
    /// decryption — and the bounded projection (the 32-entry cap, the
    /// malformed-entry drops, the id-0 fail-closed refusal). Nothing persists,
    /// nothing logs.
    pub async fn get_devices(
        &self,
        params: AccountsGetDevicesParams,
    ) -> Result<DeviceList, ServiceError> {
        let engine = self.running_engine().await?;
        let upstream = {
            let service = self.service.lock().await;
            service.prepare_accounts_get_devices(&params.account_id)?
        };
        let result = engine
            .call("getDevices", upstream, CallClass::ReadOnly)
            .await?;
        device_list_from_engine(&result)
    }

    /// accounts.unlinkDevice (contract revision 1.122, §4.103, capability
    /// device-management): the server-side unlink — the engine DELETE
    /// v1/devices/{id}, the official unlink endpoint; the server owns the
    /// authorization (a primary unlinks any linked device, a linked device
    /// unlinks only itself, and the over-reach/primary-delete refusals surface
    /// as DEVICES_FAILED with the engine's `data.status?`). A mutating call
    /// whose outcome can be lost in transport: `EngineError::UnknownOutcome`
    /// folds to SEND_OUTCOME_UNKNOWN (retryable=false — the AGENTS.md red
    /// line, never auto-retried) through the global into_api face.
    pub async fn unlink_device(
        &self,
        params: AccountsUnlinkDeviceParams,
    ) -> Result<&'static str, ServiceError> {
        let engine = self.running_engine().await?;
        let upstream = {
            let service = self.service.lock().await;
            service.prepare_accounts_unlink_device(&params.account_id, params.device_id)?
        };
        engine
            .call("unlinkDevice", upstream, CallClass::Mutating)
            .await?;
        Ok("unlinked")
    }

    /// groups.create (contract revision 1.126, §4.107, capability
    /// group-management): the new-group upstream leg — the engine
    /// `createGroup` face (the official createGroupV2): the engine derives
    /// the new group master key and answers {groupId, revision}. A mutating
    /// upstream write: the indeterminate outcome folds to
    /// SEND_OUTCOME_UNKNOWN (retryable=false) through the global into_api
    /// face and is never retried — a create whose delivery is unknown may
    /// already have created the group server-side. Zero local writes: the
    /// new group converges through contacts.sync like every group row.
    pub async fn create_group(
        &self,
        params: GroupsCreateParams,
    ) -> Result<GroupsCreateResult, ServiceError> {
        let engine = self.running_engine().await?;
        let upstream = {
            let service = self.service.lock().await;
            service.prepare_groups_create(&params)?
        };
        let result = engine
            .call("createGroup", upstream, CallClass::Mutating)
            .await?;
        group_create_result_from_engine(&result)
    }

    /// groups.migrate (contract revision 1.123, §4.104, capability
    /// group-management): the GV1→GV2 migration upstream leg — the engine
    /// `migrateGroup` face (the official initiateMigrationToGroupV2): the
    /// engine derives the new GV2 master key and answers {groupId, revision},
    /// with the already-migrated fallback probe presenting as the same honest
    /// answer. A mutating upstream write: the indeterminate outcome folds to
    /// SEND_OUTCOME_UNKNOWN (retryable=false) through the global into_api
    /// face and is never retried — a migration whose delivery is unknown may
    /// already have created the GV2 group server-side. Zero local writes.
    pub async fn migrate_group(
        &self,
        params: GroupsMigrateParams,
    ) -> Result<GroupsMigrateResult, ServiceError> {
        let engine = self.running_engine().await?;
        let upstream = {
            let service = self.service.lock().await;
            service.prepare_groups_migrate(&params)?
        };
        let result = engine
            .call("migrateGroup", upstream, CallClass::Mutating)
            .await?;
        group_migrate_result_from_engine(&result)
    }

    /// messages.sendContact (contract revision 1.124, §4.105, capability
    /// send-contact): the shape gate + resolution under the service lock,
    /// then the mutating engine `sendContact` call — the reaction-class
    /// discipline verbatim (same-account mutex, delete barrier, an unknown
    /// outcome answers SEND_OUTCOME_UNKNOWN and is never retried — a card
    /// whose delivery is unknown may already sit in the recipients'
    /// timelines). The conversation kind decides the leg (direct recipient /
    /// group groupId, the calls.sendSignal addressing precedent). The
    /// result is the engine's send timestamp, echoed as `{sentAt}` — the
    /// send ack, not message state (the connector keeps no send rows for
    /// the face).
    pub async fn send_contact(
        &self,
        params: MessagesSendContactParams,
    ) -> Result<Value, ServiceError> {
        let engine = self.running_engine().await?;
        let upstream = {
            let service = self.service.lock().await;
            service.prepare_messages_send_contact(&params)?
        };
        match engine
            .call("sendContact", upstream, CallClass::Mutating)
            .await
        {
            Ok(result) => {
                let sent_at = result
                    .get("timestamp")
                    .and_then(Value::as_u64)
                    .ok_or(ServiceError::Engine(EngineError::Protocol))?;
                Ok(json!({ "sentAt": sent_at }))
            }
            Err(EngineError::UnknownOutcome) => Err(ServiceError::Api(ApiError::new(
                "SEND_OUTCOME_UNKNOWN",
                "mutating request has an unknown outcome",
                false,
            ))),
            Err(error) => Err(error.into()),
        }
    }

    /// messages.sendPollCreate (contract revision 1.125, §4.106, capability
    /// polls): the shape gate + resolution under the service lock, then the
    /// mutating engine `sendPollCreate` call — the remoteDelete answer
    /// discipline (a definite success answers the explicit `sent`, an
    /// indeterminate outcome the explicit `unknown`, a definite error
    /// propagates; nothing retries). The upstream result body is empty —
    /// the poll's identity is the send timestamp the receive projection
    /// converges.
    pub async fn send_poll_create(
        &self,
        params: MessagesSendPollCreateParams,
    ) -> Result<&'static str, ServiceError> {
        let engine = self.running_engine().await?;
        let upstream = {
            let service = self.service.lock().await;
            service.prepare_messages_send_poll_create(&params)?
        };
        match engine
            .call("sendPollCreate", upstream, CallClass::Mutating)
            .await
        {
            Ok(_) => Ok("sent"),
            Err(EngineError::UnknownOutcome) => Ok("unknown"),
            Err(error) => Err(error.into()),
        }
    }

    /// messages.sendPollVote (contract revision 1.125, §4.106): the
    /// `sendPollCreate` discipline verbatim against the engine's
    /// group-only `sendPollVote` face (the anchor + selection gates ride
    /// the prepare; an empty optionIndexes array is the legal clear-vote
    /// and answers the same `sent`).
    pub async fn send_poll_vote(
        &self,
        params: MessagesSendPollVoteParams,
    ) -> Result<&'static str, ServiceError> {
        let engine = self.running_engine().await?;
        let upstream = {
            let service = self.service.lock().await;
            service.prepare_messages_send_poll_vote(&params)?
        };
        match engine
            .call("sendPollVote", upstream, CallClass::Mutating)
            .await
        {
            Ok(_) => Ok("sent"),
            Err(EngineError::UnknownOutcome) => Ok("unknown"),
            Err(error) => Err(error.into()),
        }
    }

    /// messages.sendPollTerminate (contract revision 1.125, §4.106): the
    /// `sendPollCreate` discipline verbatim against the engine's
    /// group-only `sendPollTerminate` face.
    pub async fn send_poll_terminate(
        &self,
        params: MessagesSendPollTerminateParams,
    ) -> Result<&'static str, ServiceError> {
        let engine = self.running_engine().await?;
        let upstream = {
            let service = self.service.lock().await;
            service.prepare_messages_send_poll_terminate(&params)?
        };
        match engine
            .call("sendPollTerminate", upstream, CallClass::Mutating)
            .await
        {
            Ok(_) => Ok("sent"),
            Err(EngineError::UnknownOutcome) => Ok("unknown"),
            Err(error) => Err(error.into()),
        }
    }

    /// conversations.getAvatar (contract revision 1.48, §4.47, capability
    /// avatar-fetch): resolution under the service lock, then the read-only
    /// engine call — a download touches no account state. The result
    /// projects through the avatar face's bound and byte-count cross-check
    /// (`avatar_from_engine`); nothing is persisted and nothing is logged.
    pub async fn get_avatar(
        &self,
        params: ConversationsGetAvatarParams,
    ) -> Result<AvatarDownload, ServiceError> {
        let engine = self.running_engine().await?;
        let upstream = {
            let service = self.service.lock().await;
            service.prepare_conversations_get_avatar(&params.account_id, &params.conversation_id)?
        };
        let result = engine
            .call("getAvatar", upstream, CallClass::ReadOnly)
            .await?;
        avatar_from_engine(&result)
    }

    /// accounts.setUsername (contract revision 1.59, §4.55; the kt-engine
    /// write face opened with contract 1.75, §4.66): the two-hop
    /// orchestration — `usernameReserve` then `usernameConfirm`, each a
    /// separate mutating call under the groups face's unknown-outcome
    /// discipline (an indeterminate hop stops the chain: the reservation
    /// itself carries no user-visible effect server-side, and a torn
    /// two-hop converges through getUsername / the engine's storage sync).
    /// The confirmed result projects the write-back `{username,
    /// usernameLink}` on the set path — the adopted candidate wins (a bare
    /// nickname's discriminator is the server's pick) — and `{username:
    /// null}` on the delete path (the upstream writes no result body
    /// there). Structured engine codes (USERNAME_TAKEN /
    /// USERNAME_RESERVATION_GONE / USERNAME_RATE_LIMITED /
    /// USERNAME_SERVER_FAILED, and the STORAGE_* honest partial-success
    /// family when the server confirmed but the storage write failed) map
    /// through the connector's allowlist (`into_api`).
    pub async fn set_username(
        &self,
        params: AccountsSetUsernameParams,
    ) -> Result<AccountUsername, ServiceError> {
        let engine = self.running_engine().await?;
        let (upstream, account_key) = {
            let service = self.service.lock().await;
            let upstream = service.prepare_accounts_set_username(&params)?;
            let account_key = upstream["account"].clone();
            (upstream, account_key)
        };
        // Delete hop: usernameDelete {account} → {} (idempotent — the
        // engine deletes server-side and clears the storage record).
        if params.delete_username {
            return match engine
                .call("usernameDelete", upstream, CallClass::Mutating)
                .await
            {
                Ok(_) => Ok(AccountUsername {
                    username: None,
                    username_link: None,
                }),
                Err(EngineError::UnknownOutcome) => Err(ServiceError::Api(ApiError::new(
                    "SEND_OUTCOME_UNKNOWN",
                    "mutating request has an unknown outcome",
                    false,
                ))),
                Err(error) => Err(error.into()),
            };
        }
        // Set path, two hops under the same unknown-outcome discipline:
        // reserve (candidate + hash, no user-visible server effect)
        // adopts the candidate, confirm (proof + link) lands it.
        let reserved = match engine
            .call("usernameReserve", upstream, CallClass::Mutating)
            .await
        {
            Ok(result) => result,
            Err(EngineError::UnknownOutcome) => {
                return Err(ServiceError::Api(ApiError::new(
                    "SEND_OUTCOME_UNKNOWN",
                    "mutating request has an unknown outcome",
                    false,
                )));
            }
            Err(error) => return Err(error.into()),
        };
        let confirm = json!({
            "account": account_key,
            "username": reserved
                .get("username")
                .and_then(Value::as_str)
                .ok_or(ServiceError::Engine(EngineError::Protocol))?,
            "usernameHash": reserved
                .get("usernameHash")
                .and_then(Value::as_str)
                .ok_or(ServiceError::Engine(EngineError::Protocol))?,
        });
        match engine
            .call("usernameConfirm", confirm, CallClass::Mutating)
            .await
        {
            Ok(result) => {
                let username = result
                    .get("username")
                    .and_then(Value::as_str)
                    .ok_or(ServiceError::Engine(EngineError::Protocol))?
                    .to_string();
                Ok(AccountUsername {
                    username: Some(username),
                    username_link: result
                        .get("usernameLink")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                })
            }
            Err(EngineError::UnknownOutcome) => Err(ServiceError::Api(ApiError::new(
                "SEND_OUTCOME_UNKNOWN",
                "mutating request has an unknown outcome",
                false,
            ))),
            Err(error) => Err(error.into()),
        }
    }

    /// accounts.getUsername (contract revision 1.59, §4.55; the kt-engine
    /// read face opened with contract 1.74, §4.65): a read-only engine
    /// `getUsername` call projects the `{username?, usernameLink?}` storage
    /// truth (both keys absent = unset — the honest never-set answer, not an
    /// error).
    pub async fn get_username(
        &self,
        params: AccountsGetUsernameParams,
    ) -> Result<AccountUsername, ServiceError> {
        let upstream = {
            let service = self.service.lock().await;
            service.prepare_accounts_get_username(&params.account_id)?
        };
        let engine = self.running_engine().await?;
        match engine
            .call("getUsername", upstream, CallClass::ReadOnly)
            .await
        {
            Ok(result) => Ok(AccountUsername {
                username: result
                    .get("username")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                username_link: result
                    .get("usernameLink")
                    .and_then(Value::as_str)
                    .map(str::to_string),
            }),
            Err(error) => Err(error.into()),
        }
    }

    /// username.lookup (contract revision 1.59, §4.55; the kt-engine face
    /// opened with contract 1.74, §4.65): resolution under the service lock,
    /// then the read-only `usernameLookup` engine call (same server
    /// lookups). The upstream answer
    /// is a top-level array; the connector projects it entry-for-entry — a
    /// missing field is a protocol face, not a silently-dropped item.
    pub async fn lookup_usernames(
        &self,
        params: UsernameLookupParams,
    ) -> Result<UsernameLookupResult, ServiceError> {
        let engine = self.running_engine().await?;
        let upstream = {
            let service = self.service.lock().await;
            service.prepare_username_lookup(&params)?
        };
        let result = engine
            .call("usernameLookup", upstream, CallClass::ReadOnly)
            .await?;
        let entries = result
            .as_array()
            .ok_or(ServiceError::Engine(EngineError::Protocol))?;
        Ok(UsernameLookupResult {
            results: entries
                .iter()
                .map(|entry| {
                    Ok(UsernameLookupEntry {
                        recipient: entry
                            .get("recipient")
                            .and_then(Value::as_str)
                            .ok_or(ServiceError::Engine(EngineError::Protocol))?
                            .to_string(),
                        number: entry
                            .get("number")
                            .and_then(Value::as_str)
                            .map(str::to_string),
                        username: entry
                            .get("username")
                            .and_then(Value::as_str)
                            .map(str::to_string),
                        uuid: entry
                            .get("uuid")
                            .and_then(Value::as_str)
                            .map(str::to_string),
                        is_registered: entry
                            .get("isRegistered")
                            .and_then(Value::as_bool)
                            .unwrap_or(false),
                    })
                })
                .collect::<Result<Vec<_>, ServiceError>>()?,
        })
    }

    /// identities.get (contract revision 1.68, §4.61, capability
    /// safety-number): resolution under the service lock, then the
    /// read-only pinned `listIdentities` call narrowed to the resolved peer
    /// reference. The upstream answer is the JSON array of `JsonIdentity`
    /// records; the projection is the §4.61 ladder (empty → `identity` null,
    /// malformed record → protocol defect).
    pub async fn get_identity(
        &self,
        params: IdentitiesGetParams,
    ) -> Result<IdentityRead, ServiceError> {
        let engine = self.running_engine().await?;
        let upstream = {
            let service = self.service.lock().await;
            service.prepare_identities_get(&params.account_id, &params.conversation_id)?
        };
        let result = engine
            .call("listIdentities", upstream, CallClass::ReadOnly)
            .await?;
        identity_from_engine(&result)
    }

    /// identities.verify (contract revision 1.68, §4.61, capability
    /// safety-number): resolution under the service lock, then the mutating
    /// mutating `trust {verifiedSafetyNumber}` call — the reaction-class
    /// discipline verbatim (an indeterminate outcome answers
    /// SEND_OUTCOME_UNKNOWN and is never retried: the trust write may
    /// already have landed). The engine answers an empty result object
    /// on success (the command writes no JSON output), so the result is the
    /// plain `{status: "verified"}` confirmation; the upstream mismatch
    /// UserError folds into the plain UPSTREAM_ERROR like every
    /// account-domain method.
    pub async fn verify_identity(
        &self,
        params: IdentitiesVerifyParams,
    ) -> Result<IdentityVerifyOutcome, ServiceError> {
        let engine = self.running_engine().await?;
        let upstream = {
            let service = self.service.lock().await;
            service.prepare_identities_verify(
                &params.account_id,
                &params.conversation_id,
                &params.safety_number,
            )?
        };
        match engine.call("trust", upstream, CallClass::Mutating).await {
            Ok(_) => Ok(IdentityVerifyOutcome { status: "verified" }),
            Err(EngineError::UnknownOutcome) => Err(ServiceError::Api(ApiError::new(
                "SEND_OUTCOME_UNKNOWN",
                "mutating request has an unknown outcome",
                false,
            ))),
            Err(error) => Err(error.into()),
        }
    }

    /// conversations.setBlocked (contract revision 1.48, §4.47, capability
    /// blocked-write): bounds + resolution under the service lock, then the
    /// mutating engine call — reaction-class discipline: an unknown outcome
    /// answers SEND_OUTCOME_UNKNOWN and is never retried; the success result
    /// is the post-write cloud blocked list read back, not a local echo.
    pub async fn set_conversation_blocked(
        &self,
        params: ConversationsSetBlockedParams,
    ) -> Result<BlockedConversations, ServiceError> {
        let engine = self.running_engine().await?;
        let upstream = {
            let service = self.service.lock().await;
            service.prepare_conversations_set_blocked(
                &params.account_id,
                &params.conversation_id,
                &params.kind,
                params.blocked,
            )?
        };
        match engine
            .call("setConversationBlocked", upstream, CallClass::Mutating)
            .await
        {
            Ok(result) => Ok(blocked_conversations_result(
                result
                    .get("blocked")
                    .and_then(Value::as_array)
                    .ok_or(ServiceError::Engine(EngineError::Protocol))?,
            )),
            Err(EngineError::UnknownOutcome) => Err(ServiceError::Api(ApiError::new(
                "SEND_OUTCOME_UNKNOWN",
                "mutating request has an unknown outcome",
                false,
            ))),
            Err(error) => Err(error.into()),
        }
    }

    /// conversations.getState (contract revision 1.116, §4.97, capability
    /// conversation-state): bounds + resolution under the service lock, then
    /// the read-only engine call — the three cloud bits read back, all keys
    /// always present (0/false = unset bit). The connector persists nothing:
    /// the cloud record is the whole state.
    pub async fn get_conversation_state(
        &self,
        params: ConversationsGetStateParams,
    ) -> Result<ConversationState, ServiceError> {
        let engine = self.running_engine().await?;
        let upstream = {
            let service = self.service.lock().await;
            service.prepare_conversations_get_state(
                &params.account_id,
                &params.conversation_id,
                &params.kind,
            )?
        };
        match engine
            .call("getConversationState", upstream, CallClass::ReadOnly)
            .await
        {
            Ok(result) => conversation_state_from_engine(&result),
            // The bare -32603 face (an account the engine no longer holds, a
            // malformed parameter the typed ladder cannot produce) is a
            // deterministic refusal — retryable=false, the 1.115 discipline.
            Err(EngineError::Upstream) => Err(ServiceError::Api(ApiError::new(
                "UPSTREAM_ERROR",
                "signal-cli returned an error",
                false,
            ))),
            Err(error) => Err(error.into()),
        }
    }

    /// conversations.setState (contract revision 1.116, §4.97, capability
    /// conversation-state): bounds + resolution under the service lock, then
    /// the mutating engine call — the reaction-class discipline verbatim (an
    /// indeterminate outcome answers SEND_OUTCOME_UNKNOWN and is never
    /// retried: the storage write may already have landed). Only the flags
    /// the caller sent travel (the engine's partial-write grammar); the
    /// success result is the post-write projection read back, not a local
    /// echo. Zero local writes: the cloud record is the whole state.
    pub async fn set_conversation_state(
        &self,
        params: ConversationsSetStateParams,
    ) -> Result<ConversationState, ServiceError> {
        let engine = self.running_engine().await?;
        let upstream = {
            let service = self.service.lock().await;
            service.prepare_conversations_set_state(
                &params.account_id,
                &params.conversation_id,
                &params.kind,
                params.muted_until_timestamp,
                params.archived,
                params.marked_unread,
            )?
        };
        match engine
            .call("setConversationState", upstream, CallClass::Mutating)
            .await
        {
            Ok(result) => conversation_state_from_engine(&result),
            Err(EngineError::UnknownOutcome) => Err(ServiceError::Api(ApiError::new(
                "SEND_OUTCOME_UNKNOWN",
                "mutating request has an unknown outcome",
                false,
            ))),
            // The bare -32603 face rejects the parameters before any write
            // lands — a deterministic refusal, retryable=false (the 1.115
            // groups discipline).
            Err(EngineError::Upstream) => Err(ServiceError::Api(ApiError::new(
                "UPSTREAM_ERROR",
                "signal-cli returned an error",
                false,
            ))),
            Err(error) => Err(error.into()),
        }
    }

    /// groups.generateInviteLinkPassword (contract revision 1.117, §4.98,
    /// capability group-invite-links): the account-less material face —
    /// 16 CSPRNG bytes as standard base64, the official
    /// GROUP_INVITE_LINK_PASSWORD_LENGTH. No dispatch record engine-side
    /// (the material faces never log), and the bytes are single-pass:
    /// they ride this answer and are never logged or persisted
    /// connector-side.
    pub async fn generate_group_invite_link_password(
        &self,
        _params: GroupsInviteLinkPasswordParams,
    ) -> Result<GroupInviteLinkPassword, ServiceError> {
        let engine = self.running_engine().await?;
        let result = engine
            .call("generateInviteLinkPassword", json!({}), CallClass::ReadOnly)
            .await?;
        group_invite_link_password_from_engine(&result)
    }

    /// groups.buildInviteLink (contract revision 1.117, §4.98, capability
    /// group-invite-links): group resolution under the service lock, then
    /// the read-only `buildInviteLink {account, groupId, password?}` — the
    /// explicit-password arm is pure construction, the default arm reads
    /// the server group state (a link the group has not enabled answers
    /// GROUP_INVITE_LINK_UNAVAILABLE). Read lane, non-mutating; the link
    /// and password bytes are single-pass material.
    pub async fn build_group_invite_link(
        &self,
        params: GroupsBuildInviteLinkParams,
    ) -> Result<GroupInviteLink, ServiceError> {
        let engine = self.running_engine().await?;
        let upstream = {
            let service = self.service.lock().await;
            service.prepare_groups_build_invite_link(
                &params.account_id,
                &params.conversation_id,
                params.password.as_deref(),
            )?
        };
        match engine
            .call("buildInviteLink", upstream, CallClass::ReadOnly)
            .await
        {
            Ok(result) => {
                let invite_link = result
                    .get("inviteLink")
                    .and_then(Value::as_str)
                    .ok_or(ServiceError::Engine(EngineError::Protocol))?;
                Ok(GroupInviteLink {
                    invite_link: invite_link.to_string(),
                })
            }
            // The bare -32603 face (an account the engine no longer holds,
            // a malformed parameter the typed ladder cannot produce) is a
            // deterministic refusal — retryable=false, the 1.115
            // discipline; the structured link families
            // (GROUP_INVITE_LINK_UNAVAILABLE, STORAGE_UNAVAILABLE) map
            // through the allowlist channels.
            Err(EngineError::Upstream) => Err(ServiceError::Api(ApiError::new(
                "UPSTREAM_ERROR",
                "signal-cli returned an error",
                false,
            ))),
            Err(error) => Err(error.into()),
        }
    }

    /// groups.parseInviteLink (contract revision 1.117, §4.98, capability
    /// group-invite-links): the account-less pure-parse face — the engine
    /// decodes the official `https://signal.group/#<websafe>` shape (the
    /// three URL forms and the bare invite code) to its master key +
    /// password bytes; a malformed link is the engine's -32603 face, the
    /// deterministic UPSTREAM_ERROR refusal. The decoded bytes are
    /// single-pass material.
    pub async fn parse_group_invite_link(
        &self,
        params: GroupsParseInviteLinkParams,
    ) -> Result<GroupInviteLinkParse, ServiceError> {
        let engine = self.running_engine().await?;
        let upstream = {
            let service = self.service.lock().await;
            service.prepare_groups_parse_invite_link(&params.invite_link)?
        };
        match engine
            .call("parseInviteLink", upstream, CallClass::ReadOnly)
            .await
        {
            Ok(result) => group_invite_link_parse_from_engine(&result),
            // The bare -32603 parse gate (a malformed link the engine's
            // tolerance ladder refuses before anything goes upstream) is a
            // deterministic refusal — retryable=false, a replay fails the
            // same way.
            Err(EngineError::Upstream) => Err(ServiceError::Api(ApiError::new(
                "UPSTREAM_ERROR",
                "signal-cli returned an error",
                false,
            ))),
            Err(error) => Err(error.into()),
        }
    }

    /// groups.previewViaLink (contract revision 1.117, §4.98, capability
    /// group-invite-links): bounds + account resolution under the service
    /// lock, then the read-only `previewGroupViaLink {account, inviteLink}`
    /// — the engine's pre-join projection (the link's group without
    /// membership). Read lane; a server refusal answers
    /// GROUP_INVITE_LINK_PREVIEW_FAILED with data.status? verbatim.
    pub async fn preview_group_via_link(
        &self,
        params: GroupsPreviewViaLinkParams,
    ) -> Result<GroupInviteLinkPreview, ServiceError> {
        let engine = self.running_engine().await?;
        let upstream = {
            let service = self.service.lock().await;
            service.prepare_groups_link_addressed_face(&params.account_id, &params.invite_link)?
        };
        match engine
            .call("previewGroupViaLink", upstream, CallClass::ReadOnly)
            .await
        {
            Ok(result) => group_invite_link_preview_from_engine(&result),
            // The bare -32603 face (a malformed link, an account the
            // engine no longer holds) is a deterministic refusal —
            // retryable=false; the structured faces
            // (GROUP_INVITE_LINK_PREVIEW_FAILED, STORAGE_UNAVAILABLE) map
            // through the allowlist channels.
            Err(EngineError::Upstream) => Err(ServiceError::Api(ApiError::new(
                "UPSTREAM_ERROR",
                "signal-cli returned an error",
                false,
            ))),
            Err(error) => Err(error.into()),
        }
    }

    /// groups.joinViaLink (contract revision 1.117, §4.98, capability
    /// group-invite-links): bounds + account resolution under the service
    /// lock, then the mutating `joinGroupViaLink {account, inviteLink}` —
    /// the reaction-class discipline with the timer face's unknown-outcome
    /// honesty (an indeterminate answer is SEND_OUTCOME_UNKNOWN and is
    /// never retried; the join may already have landed). The status verb
    /// distinguishes the definite member add (`joined`) from the approval
    /// gate's request (`requested`); a server rejection answers
    /// GROUP_UPDATE_FAILED with data.status? verbatim (the 1.115 channel).
    /// Zero local writes: the roster converges through contacts.sync.
    pub async fn join_group_via_link(
        &self,
        params: GroupsJoinViaLinkParams,
    ) -> Result<&'static str, ServiceError> {
        let engine = self.running_engine().await?;
        let upstream = {
            let service = self.service.lock().await;
            service.prepare_groups_link_addressed_face(&params.account_id, &params.invite_link)?
        };
        match engine
            .call("joinGroupViaLink", upstream, CallClass::Mutating)
            .await
        {
            Ok(result) => group_invite_join_status_from_engine(&result),
            Err(EngineError::UnknownOutcome) => Err(ServiceError::Api(ApiError::new(
                "SEND_OUTCOME_UNKNOWN",
                "mutating request has an unknown outcome",
                false,
            ))),
            // The bare -32603 face (a malformed link the engine's parse
            // gate refuses before anything goes upstream) is a
            // deterministic refusal — retryable=false.
            Err(EngineError::Upstream) => Err(ServiceError::Api(ApiError::new(
                "UPSTREAM_ERROR",
                "signal-cli returned an error",
                false,
            ))),
            Err(error) => Err(error.into()),
        }
    }

    /// groups.cancelJoinRequest (contract revision 1.117, §4.98, capability
    /// group-invite-links): the join write's withdrawal twin — the same
    /// link-addressed ladder, the same reaction-class discipline, the
    /// same `{"status": "cancelled"}` definite success. Zero local
    /// writes: the roster converges through contacts.sync.
    pub async fn cancel_join_request(
        &self,
        params: GroupsCancelJoinRequestParams,
    ) -> Result<&'static str, ServiceError> {
        let engine = self.running_engine().await?;
        let upstream = {
            let service = self.service.lock().await;
            service.prepare_groups_link_addressed_face(&params.account_id, &params.invite_link)?
        };
        match engine
            .call("cancelJoinRequest", upstream, CallClass::Mutating)
            .await
        {
            Ok(_) => Ok("cancelled"),
            Err(EngineError::UnknownOutcome) => Err(ServiceError::Api(ApiError::new(
                "SEND_OUTCOME_UNKNOWN",
                "mutating request has an unknown outcome",
                false,
            ))),
            Err(EngineError::Upstream) => Err(ServiceError::Api(ApiError::new(
                "UPSTREAM_ERROR",
                "signal-cli returned an error",
                false,
            ))),
            Err(error) => Err(error.into()),
        }
    }

    /// conversations.acceptRequest (contract revision 1.63, §4.56, capability
    /// message-requests): bounds + resolution under the service lock, then
    /// the mutating upstream `sendMessageRequestResponse {type: accept}` —
    /// the pinned native face. Reaction-class discipline: an unknown outcome
    /// answers SEND_OUTCOME_UNKNOWN and is never retried. Zero local writes:
    /// the request state converges through the §4.56 receive projection.
    pub async fn accept_message_request(
        &self,
        params: MessageRequestParams,
    ) -> Result<MessageRequestOutcome, ServiceError> {
        self.message_request_request(params, "accept", "accepted")
            .await
    }

    /// conversations.declineRequest (contract revision 1.63, §4.56,
    /// capability message-requests): the delete variant — the same bare
    /// upstream face with `type: delete`, the official decline (delete-the-
    /// conversation) sync semantics. The pinned JSON-RPC face answers an
    /// empty result object on success and folds every per-recipient failure
    /// (ResolveHelper/SyncHelper, incl. the profile-sharing-off write's
    /// UnregisteredRecipientException path) into a USER_ERROR -1 with no
    /// structured marker, so no per-failure ladder is attempted here — the
    /// connector maps the error class honestly (UPSTREAM_ERROR) and never
    /// composes a second upstream write onto one host method (the official
    /// block-and-delete twin stays the desktop's explicit
    /// conversations.setBlocked composition). Unknown outcomes are terminal.
    pub async fn decline_message_request(
        &self,
        params: MessageRequestParams,
    ) -> Result<MessageRequestOutcome, ServiceError> {
        self.message_request_request(params, "delete", "declined")
            .await
    }

    /// The shared §4.56 body: prepare under the service lock, stamp the
    /// upstream `type`, one mutating engine call, unknown-outcome mapping.
    async fn message_request_request(
        &self,
        params: MessageRequestParams,
        request_type: &str,
        status: &'static str,
    ) -> Result<MessageRequestOutcome, ServiceError> {
        let engine = self.running_engine().await?;
        let mut upstream = {
            let service = self.service.lock().await;
            service.prepare_conversations_message_request(
                &params.account_id,
                &params.conversation_id,
            )?
        };
        if let Some(object) = upstream.as_object_mut() {
            object.insert("type".to_string(), json!(request_type));
        }
        match engine
            .call("sendMessageRequestResponse", upstream, CallClass::Mutating)
            .await
        {
            Ok(_) => Ok(MessageRequestOutcome { status }),
            Err(EngineError::UnknownOutcome) => Err(ServiceError::Api(ApiError::new(
                "SEND_OUTCOME_UNKNOWN",
                "mutating request has an unknown outcome",
                false,
            ))),
            Err(error) => Err(error.into()),
        }
    }

    /// calls.sendSignal (contract revision 1.53, §4.49, capability
    /// call-signaling): the shape gate + resolution under the service lock,
    /// then the mutating engine `sendCallMessage` call — the reaction-class
    /// discipline verbatim (same-account mutex, delete barrier, an unknown
    /// outcome answers SEND_OUTCOME_UNKNOWN and is never retried). The
    /// signaling envelope travels byte-for-byte: the connector neither
    /// parses nor logs the payload. Legacy busy/hangup frames without a
    /// payload route the engine's `legacy` bare-frame face (contract 1.53);
    /// `urgency` rides through when present. The result is the engine's
    /// envelope timestamp, echoed as `{sentAt}` — the send ack, not call
    /// state (the connector keeps none).
    pub async fn send_call_signal(
        &self,
        params: CallsSendSignalParams,
    ) -> Result<Value, ServiceError> {
        let engine = self.running_engine().await?;
        let upstream = {
            let service = self.service.lock().await;
            service.prepare_calls_send_signal(
                &params.account_id,
                &params.conversation_id,
                &params.signal_type,
                params.call_id,
                params.payload.as_deref(),
                params.urgency.as_deref(),
            )?
        };
        match engine
            .call("sendCallMessage", upstream, CallClass::Mutating)
            .await
        {
            Ok(result) => {
                let sent_at = result
                    .get("timestamp")
                    .and_then(Value::as_u64)
                    .ok_or(ServiceError::Engine(EngineError::Protocol))?;
                Ok(json!({ "sentAt": sent_at }))
            }
            Err(EngineError::UnknownOutcome) => Err(ServiceError::Api(ApiError::new(
                "SEND_OUTCOME_UNKNOWN",
                "mutating request has an unknown outcome",
                false,
            ))),
            Err(error) => Err(error.into()),
        }
    }

    /// calls.getState (contract revision 1.50, §4.48, capability
    /// call-signaling): a store-only read — no engine call, no network, no
    /// mutation (the `history.importStatus` class). The connector's call
    /// registry is the honest empty state: the connector is the signaling
    /// pipe between the engine's transport and the desktop + calling
    /// sidecar's RingRTC state machine, not a state owner.
    pub async fn call_state(&self, account_id: String) -> Result<Value, ServiceError> {
        let service = self.service.lock().await;
        let state = service.prepare_calls_get_state(&account_id)?;
        Ok(serde_json::to_value(state).unwrap_or(Value::Null))
    }

    /// calls.sendGroupCallUpdate (contract revision 1.127, §4.108,
    /// capability call-signaling): the group-call era update — the engine
    /// `sendGroupCallUpdate` face (the official DataMessage.groupCallUpdate
    /// group fan-out). A mutating upstream write: the indeterminate outcome
    /// folds to SEND_OUTCOME_UNKNOWN (retryable=false) and is never retried
    /// — an era update whose delivery is unknown may already sit in the
    /// group's call state. The definite success answers the explicit
    /// `{"status":"sent"}` — the engine's own body is empty, the update's
    /// identity is the era string itself.
    pub async fn send_group_call_update(
        &self,
        params: CallsSendGroupCallUpdateParams,
    ) -> Result<&'static str, ServiceError> {
        let engine = self.running_engine().await?;
        let upstream = {
            let service = self.service.lock().await;
            service.prepare_calls_send_group_call_update(&params)?
        };
        match engine
            .call("sendGroupCallUpdate", upstream, CallClass::Mutating)
            .await
        {
            Ok(_) => Ok("sent"),
            Err(EngineError::UnknownOutcome) => Err(ServiceError::Api(ApiError::new(
                "SEND_OUTCOME_UNKNOWN",
                "mutating request has an unknown outcome",
                false,
            ))),
            Err(error) => Err(error.into()),
        }
    }

    /// calls.linkPresentation (contract revision 1.88, §4.73, capability
    /// call-signaling widened in place): pure engine material passthrough —
    /// the shape gate + account resolution ride the service prepare under
    /// the service lock, then the engine's KT-extension method answers the
    /// presentation bytes, forwarded verbatim. The connector parses nothing,
    /// logs nothing, caches nothing: the credential bytes cross this process
    /// exactly once, engine → host (the engine compat §5 zero-persistence
    /// discipline mirrored at the pipe). Read-only engine class: a timeout
    /// answers UPSTREAM_TIMEOUT (retryable), never the mutating unknown
    /// outcome. The faces are KT-extension engine methods (the §4.69
    /// ladder).
    pub async fn calls_link_presentation(
        &self,
        account_id: &str,
        root_key: &str,
    ) -> Result<Value, ServiceError> {
        let params = {
            let service = self.service.lock().await;
            service.prepare_calls_link_presentation(account_id, root_key)?
        };
        let engine = self.running_engine().await?;
        engine
            .call("calls.linkPresentation", params, CallClass::ReadOnly)
            .await
            .map_err(ServiceError::from)
    }

    /// calls.createLinkAuth (contract revision 1.88, §4.73): the create-auth
    /// two-shot passthrough — the same discipline as `calls_link_presentation`
    /// with the roomId gate added. The engine runs both shots (the
    /// create-auth POST → redeem → present + public params) inside the one
    /// RPC; the server-side POST is credential issuance with no user-visible
    /// state change, so the face stays read-only.
    pub async fn calls_create_link_auth(
        &self,
        account_id: &str,
        root_key: &str,
        room_id: &str,
    ) -> Result<Value, ServiceError> {
        let params = {
            let service = self.service.lock().await;
            service.prepare_calls_create_link_auth(account_id, root_key, room_id)?
        };
        let engine = self.running_engine().await?;
        engine
            .call("calls.createLinkAuth", params, CallClass::ReadOnly)
            .await
            .map_err(ServiceError::from)
    }

    /// calls.groupProof (contract revision 1.88, §4.73): the membership
    /// proof passthrough — `calls_link_presentation`'s discipline with the
    /// 32-byte group master key gate (the engine's decode_group_id shape).
    pub async fn calls_group_proof(
        &self,
        account_id: &str,
        group_id: &str,
    ) -> Result<Value, ServiceError> {
        let params = {
            let service = self.service.lock().await;
            service.prepare_calls_group_proof(account_id, group_id)?
        };
        let engine = self.running_engine().await?;
        engine
            .call("calls.groupProof", params, CallClass::ReadOnly)
            .await
            .map_err(ServiceError::from)
    }

    /// calls.groupMembers (contract revision 1.88, §4.73): the member
    /// cipher-text material passthrough — the `calls_group_proof` discipline
    /// verbatim against the engine's groupMembers face; the ACI-keyed
    /// material never touches a connector log or store.
    pub async fn calls_group_members(
        &self,
        account_id: &str,
        group_id: &str,
    ) -> Result<Value, ServiceError> {
        let params = {
            let service = self.service.lock().await;
            service.prepare_calls_group_members(account_id, group_id)?
        };
        let engine = self.running_engine().await?;
        engine
            .call("calls.groupMembers", params, CallClass::ReadOnly)
            .await
            .map_err(ServiceError::from)
    }

    /// calls.endorsementsPublicKey (contract revision 1.88, §4.73): the
    /// constant-derivation passthrough — no account, no material input, no
    /// network; the engine derives from its pinned legacy ServerPublicParams
    /// and the bytes pass through (a KT-extension engine method).
    pub async fn calls_endorsements_public_key(&self) -> Result<Value, ServiceError> {
        let params = {
            let service = self.service.lock().await;
            service.prepare_calls_endorsements_public_key()?
        };
        let engine = self.running_engine().await?;
        engine
            .call("calls.endorsementsPublicKey", params, CallClass::ReadOnly)
            .await
            .map_err(ServiceError::from)
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
            Ok(_) => {
                // Confirmed upstream: persist the own reaction and push the
                // conversation refresh so pills survive the app's optimistic
                // window (the same event the receive path uses for reactions).
                let summary = {
                    let service = self.service.lock().await;
                    service.complete_send_reaction(
                        &prepared.account_id,
                        &prepared.conversation_id,
                        &prepared.emoji,
                        prepared.remove,
                        prepared.target_sent_at,
                    )?
                };
                if let Some(summary) = summary {
                    let _ = self
                        .host_events
                        .send(HostSideEvent::ConversationChanged(summary));
                }
                Ok("sent")
            }
            Err(EngineError::UnknownOutcome) => Ok("unknown"),
            Err(error) => Err(error.into()),
        }
    }

    /// Best-effort pin for one message with a resolvable protocol identity
    /// (contract revision 1.33): the reaction shape end to end — local
    /// prepare under the service lock, upstream `sendPinMessage` call without
    /// it. On confirmation the derived per-conversation pinned state is
    /// written (a newer pin replaces the older one; the expiry is connector
    /// clock computed) and the conversation refresh rides the host lane, so a
    /// reloaded window renders the pin bar without replaying events. An
    /// indeterminate mutating outcome answers `{"status":"unknown"}` and is
    /// never retried automatically.
    pub async fn send_pin_message(
        &self,
        account_id: String,
        conversation_id: String,
        message_id: String,
        pin_duration_seconds: Option<u32>,
    ) -> Result<&'static str, ServiceError> {
        let engine = self.running_engine().await?;
        let prepared = {
            let service = self.service.lock().await;
            service.prepare_send_pin_message(
                &account_id,
                &conversation_id,
                &message_id,
                pin_duration_seconds,
            )?
        };
        match engine
            .call("sendPinMessage", prepared.params, CallClass::Mutating)
            .await
        {
            Ok(_) => {
                let summary = {
                    let service = self.service.lock().await;
                    service.complete_send_pin_message(
                        &prepared.account_id,
                        &prepared.conversation_id,
                        &prepared.target_author,
                        prepared.target_sent_at,
                        prepared.pin_duration_seconds,
                    )?
                };
                if let Some(summary) = summary {
                    let _ = self
                        .host_events
                        .send(HostSideEvent::ConversationChanged(summary));
                }
                Ok("sent")
            }
            Err(EngineError::UnknownOutcome) => Ok("unknown"),
            Err(error) => Err(error.into()),
        }
    }

    /// Best-effort unpin (contract revision 1.33): the pin shape without the
    /// duration. On confirmation the local pinned state clears only when it
    /// still points at the (author, timestamp) this unpin names, and the
    /// conversation refresh rides the host lane.
    pub async fn send_unpin_message(
        &self,
        account_id: String,
        conversation_id: String,
        message_id: String,
    ) -> Result<&'static str, ServiceError> {
        let engine = self.running_engine().await?;
        let prepared = {
            let service = self.service.lock().await;
            service.prepare_send_unpin_message(&account_id, &conversation_id, &message_id)?
        };
        match engine
            .call("sendUnpinMessage", prepared.params, CallClass::Mutating)
            .await
        {
            Ok(_) => {
                let summary = {
                    let service = self.service.lock().await;
                    service.complete_send_unpin_message(
                        &prepared.account_id,
                        &prepared.conversation_id,
                        &prepared.target_author,
                        prepared.target_sent_at,
                    )?
                };
                if let Some(summary) = summary {
                    let _ = self
                        .host_events
                        .send(HostSideEvent::ConversationChanged(summary));
                }
                Ok("sent")
            }
            Err(EngineError::UnknownOutcome) => Ok("unknown"),
            Err(error) => Err(error.into()),
        }
    }

    /// Best-effort group admin delete (contract revision 1.33): the pin
    /// addressing, group conversations only, with no admin-eligibility
    /// pre-check — the server is the authority and its rejection surfaces as
    /// the upstream error. The local row is left unchanged on success (the
    /// remoteDelete boundary, §4.5): the authoritative tombstone converges
    /// through the row-marker projection when the echo arrives.
    pub async fn send_admin_delete(
        &self,
        account_id: String,
        conversation_id: String,
        message_id: String,
    ) -> Result<&'static str, ServiceError> {
        let engine = self.running_engine().await?;
        let prepared = {
            let service = self.service.lock().await;
            service.prepare_send_admin_delete(&account_id, &conversation_id, &message_id)?
        };
        match engine
            .call("sendAdminDelete", prepared.params, CallClass::Mutating)
            .await
        {
            Ok(_) => Ok("sent"),
            Err(EngineError::UnknownOutcome) => Ok("unknown"),
            Err(error) => Err(error.into()),
        }
    }

    /// Outbound read receipt (contract revision 1.34, §4.29): the pin-family
    /// shape with a fan-out instead of a single call — one engine
    /// `sendReadReceipt` per author group, results aggregated. A receipt-call
    /// failure never fails the request: any failure (including an unknown
    /// outcome) only degrades the answer to `{"status":"unknown"}`, and
    /// nothing is ever retried automatically.
    pub async fn send_read_receipt(
        &self,
        account_id: String,
        conversation_id: String,
        message_ids: Option<Vec<String>>,
    ) -> Result<&'static str, ServiceError> {
        self.send_receipt(account_id, conversation_id, message_ids, "sendReadReceipt")
            .await
    }

    /// Outbound viewed receipt (contract revision 1.34): identical fan-out,
    /// upstream `sendViewedReceipt`.
    pub async fn send_viewed_receipt(
        &self,
        account_id: String,
        conversation_id: String,
        message_ids: Option<Vec<String>>,
    ) -> Result<&'static str, ServiceError> {
        self.send_receipt(
            account_id,
            conversation_id,
            message_ids,
            "sendViewedReceipt",
        )
        .await
    }

    /// Shared mark-read/mark-viewed body: prepare (selection + author
    /// grouping) under the service lock, one mutating engine call per author
    /// group without it. markViewed first-stamps the selected rows' local
    /// `viewed_at` (§4.77) before any fan-out — the stamp is the request's
    /// own local effect, not a receipt consequence, so it lands even when
    /// the group list is empty and survives an upstream failure. Zero groups
    /// is the trivial `sent` no-op; the per-author params are exactly the
    /// engine contract `{account, recipient, timestamps}` — recipient is the
    /// single author, timestamps ascend (the engine chunks 100 per
    /// envelope).
    async fn send_receipt(
        &self,
        account_id: String,
        conversation_id: String,
        message_ids: Option<Vec<String>>,
        method: &'static str,
    ) -> Result<&'static str, ServiceError> {
        let engine = self.running_engine().await?;
        let prepared = {
            let service = self.service.lock().await;
            let prepared = match method {
                "sendReadReceipt" => {
                    service.prepare_mark_read(&account_id, &conversation_id, message_ids)?
                }
                _ => service.prepare_mark_viewed(&account_id, &conversation_id, message_ids)?,
            };
            if method == "sendViewedReceipt" {
                service.complete_mark_viewed(&prepared)?;
            }
            prepared
        };
        if prepared.groups.is_empty() {
            return Ok("sent");
        }
        let mut all_confirmed = true;
        for group in &prepared.groups {
            let params = json!({
                "account": prepared.account_signal,
                "recipient": group.recipient,
                "timestamps": group.timestamps,
            });
            match engine.call(method, params, CallClass::Mutating).await {
                Ok(_) => {}
                // Any failure — definite upstream error, transport trouble,
                // or an unknown outcome — degrades to `unknown` and is never
                // propagated: a receipt is best-effort, and an unknown
                // outcome must not be retried (AGENTS.md).
                Err(_) => all_confirmed = false,
            }
        }
        Ok(if all_confirmed { "sent" } else { "unknown" })
    }

    /// Outbound read SELF-SYNC (contract revision 1.108, §4.89): the same
    /// selection and receipt-class answer as `send_read_receipt`, but the
    /// upstream face is the engine's `markRead` — SyncMessage.read to the
    /// account's own devices — not the peer-facing `sendReadReceipt`. The
    /// two channels are independent protocol faces (the official
    /// markConversationRead.preload.ts:252 note); the sync leg is what makes
    /// the account's OTHER devices show the read state, and it is not gated
    /// by the account's readReceipts configuration flag. The prepared group
    /// (direct conversations only, one author) maps to the engine's entry
    /// list `{account, reads: [{sender, timestamp}...]}` — one engine call,
    /// the engine chunking 100 entries per envelope internally.
    pub async fn send_sync_read_receipt(
        &self,
        account_id: String,
        conversation_id: String,
        message_ids: Option<Vec<String>>,
    ) -> Result<&'static str, ServiceError> {
        self.send_sync_receipt(account_id, conversation_id, message_ids, "markRead")
            .await
    }

    /// Outbound viewed SELF-SYNC (contract revision 1.108, §4.89): identical
    /// shape, engine `markViewed` (`{account, viewed: [...]}`), with the
    /// same local `viewed_at` first-stamp discipline as the peer-faced
    /// markViewed.
    pub async fn send_sync_viewed_receipt(
        &self,
        account_id: String,
        conversation_id: String,
        message_ids: Option<Vec<String>>,
    ) -> Result<&'static str, ServiceError> {
        self.send_sync_receipt(account_id, conversation_id, message_ids, "markViewed")
            .await
    }

    /// messages.deleteForMe (contract revision 1.118, §4.99, capability
    /// delete-for-me): the conversation-level delete-for-me upstream leg —
    /// the engine `deleteForMeConversation` face (the official
    /// deleteConversation sync, the full-clear arm and the local-only arm
    /// verbatim). Receipt-class like the sync read/viewed pair: any engine
    /// call failure degrades to `{status: "unknown"}` and is never retried
    /// — the sync message may already ride the self-send lane. Zero local
    /// writes (the §4.89 discipline): the §4.83 receive mirror stays the
    /// inbound channel; this face is the outbound leg only.
    pub async fn delete_for_me(
        &self,
        params: MessagesDeleteForMeParams,
    ) -> Result<&'static str, ServiceError> {
        let engine = self.running_engine().await?;
        let upstream = {
            let service = self.service.lock().await;
            service.prepare_messages_delete_for_me(&params)?
        };
        match engine
            .call("deleteForMeConversation", upstream, CallClass::Mutating)
            .await
        {
            Ok(_) => Ok("sent"),
            // Same receipt-class degradation as the peer-faced fan-out: any
            // failure — including the engine's pre-flight refusal — answers
            // `unknown` and is never retried.
            Err(_) => Ok("unknown"),
        }
    }

    /// Shared sync read/viewed body: the receipt-class twin of
    /// [`Self::send_receipt`] on the self-sync engine faces. Prepare and the
    /// `viewed_at` first-stamp are byte-identical (the local stamp is the
    /// request's own effect and survives any upstream failure); only the
    /// engine method and params shape differ — `{account, reads|viewed:
    /// [{sender, timestamp}...]}` carries the author inside each entry, so
    /// the whole group rides ONE call (the direct-conversation selection has
    /// exactly one resolvable author). The same aggregation, degradation,
    /// and never-retry discipline applies.
    async fn send_sync_receipt(
        &self,
        account_id: String,
        conversation_id: String,
        message_ids: Option<Vec<String>>,
        method: &'static str,
    ) -> Result<&'static str, ServiceError> {
        let engine = self.running_engine().await?;
        let prepared = {
            let service = self.service.lock().await;
            let prepared = match method {
                "markRead" => {
                    service.prepare_sync_read(&account_id, &conversation_id, message_ids)?
                }
                _ => service.prepare_sync_viewed(&account_id, &conversation_id, message_ids)?,
            };
            if method == "markViewed" {
                service.complete_mark_viewed(&prepared)?;
            }
            prepared
        };
        if prepared.groups.is_empty() {
            return Ok("sent");
        }
        let mut all_confirmed = true;
        for group in &prepared.groups {
            let list_key = if method == "markRead" {
                "reads"
            } else {
                "viewed"
            };
            let entries: Vec<Value> = group
                .timestamps
                .iter()
                .map(|timestamp| json!({ "sender": group.recipient, "timestamp": timestamp }))
                .collect();
            let params = json!({
                "account": prepared.account_signal,
                list_key: entries,
            });
            match engine.call(method, params, CallClass::Mutating).await {
                Ok(_) => {}
                // Same receipt-class discipline as the peer-faced fan-out:
                // any failure degrades to `unknown` and is never retried.
                Err(_) => all_confirmed = false,
            }
        }
        Ok(if all_confirmed { "sent" } else { "unknown" })
    }

    /// messages.edit (contract revision 1.15): retarget one previously sent
    /// message's body via the engine's native `editMessage` wire method
    /// (contract 1.105 migration — the signal-cli-era `send` + `editTimestamp`
    /// call dispatched a brand-new message on the kt-engine, which has no
    /// `editTimestamp` parameter). Local prepare (row resolution, text bounds)
    /// under the service lock, upstream mutating call without it; on confirmed
    /// success the local row is rewritten and the updated record returned
    /// (`Some(record)`); an indeterminate outcome maps to `Ok(None)` with the
    /// local row unchanged — never an automatic retry.
    pub async fn edit_message(
        &self,
        account_id: String,
        conversation_id: String,
        message_id: String,
        text: String,
    ) -> Result<Option<MessageRecord>, ServiceError> {
        let engine = self.running_engine().await?;
        let prepared = {
            let service = self.service.lock().await;
            service.prepare_edit(&account_id, &conversation_id, &message_id, &text)?
        };
        match engine
            .call("editMessage", prepared.params, CallClass::Mutating)
            .await
        {
            Ok(_) => {
                let service = self.service.lock().await;
                Ok(service.complete_edit_success(
                    &prepared.account_id,
                    &prepared.conversation_id,
                    &prepared.message_id,
                    &prepared.text,
                )?)
            }
            Err(EngineError::UnknownOutcome) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    /// Media ingest (ADR 0002, contract revision 1.17): open a chunk stream
    /// for one already-downloaded attachment. Pure local resolution — the
    /// engine is not consulted, the upstream whole-file base64
    /// `getAttachment` read is never used, and a not-downloaded or
    /// governor-deleted file answers UPSTREAM_ERROR exactly as before.
    pub async fn open_media(
        &self,
        params: crate::service::MessagesAttachmentsOpenParams,
    ) -> Result<MediaOpenView, ServiceError> {
        self.service.lock().await.open_media_handle(
            &params.account_id,
            &params.conversation_id,
            &params.message_id,
            &params.attachment_id,
        )
    }

    /// Media ingest: one sequential chunk from a live handle. The handle
    /// table is process-wide, so this answers identically no matter which
    /// group's supervisor the registry routed it through.
    pub async fn read_media_chunk(
        &self,
        media_handle: String,
        offset: u64,
    ) -> Result<MediaChunkView, ServiceError> {
        self.service
            .lock()
            .await
            .read_media_chunk(&media_handle, offset)
    }

    /// Media ingest: explicit early release; idempotent, TTL is the backstop.
    pub async fn close_media_handle(
        &self,
        media_handle: String,
    ) -> Result<MediaCloseView, ServiceError> {
        self.service.lock().await.close_media_handle(&media_handle)
    }

    // ---------------------------------------------------------------------
    // Android local-backup export (contract revision 1.47, §4.46,
    // capability backup-export).
    // ---------------------------------------------------------------------

    fn backup_handles(
        &self,
    ) -> Result<Arc<crate::backup_export::BackupFileHandleTable>, ServiceError> {
        self.backup_handles.clone().ok_or_else(|| {
            ServiceError::Api(ApiError::new(
                "CAPABILITY_UNAVAILABLE",
                "backup export is not enabled for this connector",
                false,
            ))
        })
    }

    /// backups.export: build (or rebuild) the transitional artifact locally,
    /// run the connector's snapshot cleanup, then start the engine's export
    /// task. The artifact build and the snapshot walk are pure filesystem +
    /// store work and run on the blocking pool; the engine call is Mutating,
    /// so an indeterminate outcome answers SEND_OUTCOME_UNKNOWN — the task
    /// may already be writing a snapshot, and restarting it automatically
    /// could race the running one. On any pre-engine failure the artifact is
    /// either absent (never built) or removed by `build_artifact` itself, so
    /// the engine never sees a stale one.
    pub async fn export_local_backup(&self, account_id: &str) -> Result<Value, ServiceError> {
        self.backup_handles()?;
        let signal_account = {
            let service = self.service.lock().await;
            service.prepare_backups_account(account_id)?
        };
        let store = Arc::clone(&self.store);
        let data_dir = self.config.data_dir.clone();
        let account = account_id.to_string();
        let counters = tokio::task::spawn_blocking({
            let signal_account = signal_account.clone();
            move || {
                crate::backup_export::build_artifact(&store, &data_dir, &account, &signal_account)
            }
        })
        .await
        .map_err(|_| ServiceError::Store(StoreError::Unavailable(None)))?
        .map_err(ServiceError::from)?;
        let account_dir =
            crate::history_import::engine_account_dir(&self.config.data_dir, &signal_account)
                .ok_or(ServiceError::from(
                    crate::backup_export::BuildError::NoEngineDirectory,
                ))?;
        crate::backup_export::prepare_snapshots_for_export(&account_dir);
        self.backup_counters
            .lock()
            .expect("backup counters mutex")
            .insert(account_id.to_string(), counters);
        let engine = self.running_engine().await?;
        match engine
            .call(
                "backup.export",
                json!({ "account": signal_account }),
                CallClass::Mutating,
            )
            .await
        {
            Ok(value) => {
                if value.get("status").and_then(Value::as_str) == Some("started") {
                    Ok(json!({ "status": "started" }))
                } else {
                    Err(ServiceError::Api(ApiError::new(
                        "UPSTREAM_PROTOCOL_ERROR",
                        "backup.export answered without a started status",
                        true,
                    )))
                }
            }
            Err(EngineError::UnknownOutcome) => Err(ServiceError::Api(ApiError::new(
                "SEND_OUTCOME_UNKNOWN",
                "backup.export outcome is unknown; poll backups.exportStatus",
                false,
            ))),
            Err(error) => Err(error.into()),
        }
    }

    /// backups.exportStatus: poll the engine read-only and project the
    /// answer — the upstream `account` echo is stripped (the raw number never
    /// crosses the host boundary, §4.46), `accountId` rides in its place,
    /// and the connector's own skip counters merge in after a successful
    /// build. Read-only, so an unknown outcome degrades honestly to the same
    /// shape as an idle engine rather than inventing a state.
    pub async fn export_backup_status(&self, account_id: &str) -> Result<Value, ServiceError> {
        self.backup_handles()?;
        let signal_account = {
            let service = self.service.lock().await;
            service.prepare_backups_account(account_id)?
        };
        let upstream = match self.running_engine().await {
            Ok(engine) => Some(
                engine
                    .call(
                        "backup.exportStatus",
                        json!({ "account": signal_account }),
                        CallClass::ReadOnly,
                    )
                    .await,
            ),
            // Engine down (or the read raced a crash): the snapshots live on
            // disk — report the store-side projection rather than failing the
            // poll. Read-only, so this is honest, not a guess.
            Err(_) => None,
        };
        match upstream {
            Some(Ok(result)) => {
                let counters = self
                    .backup_counters
                    .lock()
                    .expect("backup counters mutex")
                    .get(account_id)
                    .copied();
                Ok(export_status_projection(
                    account_id,
                    &signal_account,
                    result,
                    counters,
                ))
            }
            // Read-only, so an indeterminate outcome degrades to the same
            // disk-truth projection as an engine that is down (§4.46: never
            // invent a task state).
            Some(Err(EngineError::UnknownOutcome)) | None => {
                let data_dir = self.config.data_dir.clone();
                let account = account_id.to_string();
                blocking_service(move || {
                    export_status_idle_projection(&data_dir, &account, &signal_account)
                })
                .await
            }
            Some(Err(error)) => Err(error.into()),
        }
    }

    /// backups.exportFile.open: mint a chunk-stream handle for one fixed file
    /// (`metadata`/`main`) of the latest snapshot. Pure local resolution —
    /// the engine is not consulted; the snapshot directory is the addressable
    /// unit, its name is the newest one on disk (the same rule the engine's
    /// exportStatus list applies).
    pub async fn export_file_open(
        &self,
        account_id: &str,
        file: &str,
    ) -> Result<Value, ServiceError> {
        let handles = self.backup_handles()?;
        let signal_account = {
            let service = self.service.lock().await;
            service.prepare_backups_account(account_id)?
        };
        let store = Arc::clone(&self.store);
        let data_dir = self.config.data_dir.clone();
        let account = account_id.to_string();
        let file = file.to_string();
        blocking_service(move || {
            let snapshot = export_latest_snapshot(&store, &data_dir, &account, &signal_account)?;
            let path = match file.as_str() {
                "metadata" => snapshot.join("metadata"),
                "main" => snapshot.join("main"),
                _ => {
                    return Err(ServiceError::Api(ApiError::new(
                        "INVALID_REQUEST",
                        "file must be \"metadata\" or \"main\"",
                        false,
                    )));
                }
            };
            let size_bytes = std::fs::metadata(&path)
                .map(|metadata| metadata.len())
                .map_err(|_| {
                    ServiceError::Api(ApiError::new(
                        "BACKUP_SNAPSHOT_MISSING",
                        "the snapshot file is not on disk",
                        false,
                    ))
                })?;
            let file_handle = handles
                .open(&account, path, size_bytes)
                .map_err(map_backup_handle_error)?;
            Ok(json!({
                "fileHandle": file_handle,
                "sizeBytes": size_bytes,
                "chunkBytes": crate::backup_export::CHUNK_BYTES,
            }))
        })
        .await
    }

    /// backups.exportFile.readChunk: one sequential chunk from a live handle.
    /// The handle table is process-wide, so this answers identically no
    /// matter which group's supervisor the registry routed it through.
    pub async fn export_file_read_chunk(
        &self,
        file_handle: &str,
        offset: u64,
    ) -> Result<Value, ServiceError> {
        let handles = self.backup_handles()?;
        crate::service::validate_backup_handle(file_handle)?;
        let chunk = handles
            .read_chunk(file_handle, offset)
            .map_err(map_backup_handle_error)?;
        Ok(json!({
            "dataBase64": crate::backup_export::encode_chunk(&chunk.data),
            "sizeBytes": chunk.data.len() as u64,
            "eof": chunk.eof,
        }))
    }

    /// backups.exportFile.closeHandle: explicit early release; idempotent,
    /// TTL is the backstop.
    pub async fn export_file_close_handle(&self, file_handle: &str) -> Result<Value, ServiceError> {
        self.backup_handles()?;
        crate::service::validate_backup_handle(file_handle)?;
        let handles = self.backup_handles()?;
        Ok(json!({
            "released": handles.close(file_handle),
        }))
    }

    /// backups.importSnapshot (contract revision 1.78, §4.68): forward the
    /// caller-chosen official snapshot directory to the engine's import task,
    /// then arm the consumption watcher. The account resolution rides the
    /// §4.38 faces (capability `history-import` — no new bit); the Mutating
    /// engine call answers SEND_OUTCOME_UNKNOWN on
    /// an indeterminate outcome and never retries — the watcher still spawns so
    /// an import that actually started is observed to its terminal state.
    pub async fn import_local_snapshot(
        self: &Arc<Self>,
        account_id: &str,
        snapshot_dir: &str,
    ) -> Result<Value, ServiceError> {
        let params = {
            let service = self.service.lock().await;
            service.prepare_import_snapshot(account_id, snapshot_dir)?
        };
        let engine = self.running_engine().await?;
        match engine
            .call("backup.importSnapshot", params, CallClass::Mutating)
            .await
        {
            Ok(value) => {
                if value.get("status").and_then(Value::as_str) == Some("started") {
                    self.spawn_import_snapshot_watcher(account_id);
                    Ok(json!({ "status": "started" }))
                } else {
                    Err(ServiceError::Api(ApiError::new(
                        "UPSTREAM_PROTOCOL_ERROR",
                        "backup.importSnapshot answered without a started status",
                        true,
                    )))
                }
            }
            Err(EngineError::UnknownOutcome) => {
                self.spawn_import_snapshot_watcher(account_id);
                Err(ServiceError::Api(ApiError::new(
                    "SEND_OUTCOME_UNKNOWN",
                    "backup.importSnapshot outcome is unknown; poll backups.importSnapshotStatus",
                    false,
                )))
            }
            Err(error) => Err(error.into()),
        }
    }

    /// backups.importSnapshotStatus (contract revision 1.78, §4.68): poll the
    /// engine read-only and project the answer — the upstream `account` echo
    /// is stripped and `accountId` rides in its place (the §4.46 precedent).
    /// Read-only, so an engine that is down or an indeterminate read degrades
    /// to the disk-truth idle projection rather than inventing a task state.
    /// The face has no store fallback, its whole truth is the engine's import
    /// task.
    pub async fn import_snapshot_status(&self, account_id: &str) -> Result<Value, ServiceError> {
        let signal_account = {
            let service = self.service.lock().await;
            service.prepare_backups_account(account_id)?
        };
        let upstream = match self.running_engine().await {
            Ok(engine) => Some(
                engine
                    .call(
                        "backup.importSnapshotStatus",
                        json!({ "account": signal_account }),
                        CallClass::ReadOnly,
                    )
                    .await,
            ),
            // Engine down (or the read raced a crash): the produced NDJSON
            // lives on disk — report the disk-truth projection rather than
            // failing the poll. Read-only, so this is honest, not a guess.
            Err(_) => None,
        };
        match upstream {
            Some(Ok(result)) => Ok(import_snapshot_status_from_engine(account_id, result)),
            Some(Err(EngineError::UnknownOutcome)) | None => {
                let data_dir = self.config.data_dir.clone();
                let account = account_id.to_string();
                blocking_service(move || {
                    Ok(import_snapshot_status_idle_projection(
                        &data_dir,
                        &account,
                        &signal_account,
                    ))
                })
                .await
            }
            Some(Err(error)) => Err(error.into()),
        }
    }

    /// backups.restoreFromCloud (contract revision 1.83, §4.69): forward the
    /// cloud-restore start to the engine — the account resolution rides the
    /// §4.68 ladder (capability `history-import` — no new bit), and the
    /// engine runs the whole chain
    /// (credentials 404-bootstrap → zkgroup → chat gRPC → CDN → decrypt →
    /// NDJSON) in its own background. The Mutating engine call answers
    /// SEND_OUTCOME_UNKNOWN on an indeterminate outcome and never retries —
    /// the terminal state is observed through `backups.cloudStatus` (the
    /// engine keeps the failed phase until the next start overwrites). On a
    /// definite start — and on an unknown outcome, which may actually have
    /// started — the consumption watcher spawns (contract revision 1.84,
    /// [`Self::spawn_cloud_restore_watcher`]): the produced
    /// `history-import-cloud.ndjson` is driven through the §4.38 import pass
    /// and rendered through `history.importStatus`.
    pub async fn restore_from_cloud(
        self: &Arc<Self>,
        account_id: &str,
    ) -> Result<Value, ServiceError> {
        let params = {
            let service = self.service.lock().await;
            service.prepare_cloud_restore_account(account_id)?
        };
        let engine = self.running_engine().await?;
        match engine
            .call("backups.restoreFromCloud", params, CallClass::Mutating)
            .await
        {
            Ok(value) => {
                if value.get("status").and_then(Value::as_str) == Some("started") {
                    self.spawn_cloud_restore_watcher(account_id);
                    Ok(json!({ "status": "started" }))
                } else {
                    Err(ServiceError::Api(ApiError::new(
                        "UPSTREAM_PROTOCOL_ERROR",
                        "backups.restoreFromCloud answered without a started status",
                        true,
                    )))
                }
            }
            Err(EngineError::UnknownOutcome) => {
                // The start may actually have taken: the watcher still spawns
                // so a restore that is running is observed to its terminal
                // state (the §4.68 unknown-outcome posture).
                self.spawn_cloud_restore_watcher(account_id);
                Err(ServiceError::Api(ApiError::new(
                    "SEND_OUTCOME_UNKNOWN",
                    "backups.restoreFromCloud outcome is unknown; poll backups.cloudStatus",
                    false,
                )))
            }
            Err(error) => Err(error.into()),
        }
    }

    /// backups.cloudStatus (contract revision 1.83, §4.69): poll the engine's
    /// cloud-restore progress snapshot read-only and mirror it. The snapshot
    /// is engine-process memory (the transfer.downloadStatus discipline), so
    /// an engine that is down or an indeterminate read answers
    /// `{present: false}` — "not observable" is exactly the post-restart
    /// truth, not an invented state. The face has no store fallback, its
    /// whole truth is the engine's restore task.
    pub async fn cloud_backup_status(&self, account_id: &str) -> Result<Value, ServiceError> {
        let signal_account = {
            let service = self.service.lock().await;
            service.prepare_backups_account(account_id)?
        };
        let upstream = match self.running_engine().await {
            Ok(engine) => Some(
                engine
                    .call(
                        "backups.cloudStatus",
                        json!({ "account": signal_account }),
                        CallClass::ReadOnly,
                    )
                    .await,
            ),
            Err(_) => None,
        };
        match upstream {
            Some(Ok(result)) => cloud_status_from_engine(&result),
            // Read-only: an indeterminate read degrades to the same
            // not-observable answer as a down engine (never a guessed state).
            Some(Err(EngineError::UnknownOutcome)) | None => Ok(json!({ "present": false })),
            Some(Err(error)) => Err(error.into()),
        }
    }

    /// backups.uploadToCloud (contract revision 1.87, §4.72, engine
    /// cloud-backup-contract §6.2/§6.3): the outbound half of cloud backup.
    /// The connector's own chain stage comes first — (re)build the §10.2
    /// transitional artifact (`backup-export-input.ndjson`) in the engine's
    /// per-account data directory with the §4.46 machine verbatim
    /// (write-once, rebuilt before every attempt; a failed build removes
    /// the partial file, so the engine never sees a stale one; the
    /// engine's start gate answers BACKUP_ARTIFACT_MISSING only when this
    /// build never landed) — then the start forwards to the engine, whose
    /// whole chain (assembly thread → credentials with the 404 bootstrap
    /// self-heal → zkgroup → form gRPC → cdn3 TUS single POST →
    /// refresh_backup) runs in its own background. The Mutating engine
    /// call answers SEND_OUTCOME_UNKNOWN on an indeterminate outcome and
    /// never retries — the terminal state is observed through
    /// `backups.uploadStatus` (the engine keeps the failed phase until the
    /// next start overwrites; a retry is the caller's fresh
    /// backups.uploadToCloud — a new form, a new artifact). The
    /// eight-family structured error ladder maps through
    /// `cloud_upload_face_error`: the engine's `CloudUploadError::rpc_code`
    /// single mapping source, with the two per-face divergences (the
    /// transport family's upload text, BACKUP_IO_FAILED retryable=false +
    /// detail) reasserted over the global mapping. Deliberately no
    /// consumption watcher (the upload produces nothing local — a success
    /// leaves no trace), no snapshot caretaker and no skip-counter merge
    /// (both are the export face's own: the upload creates no snapshot and
    /// has no status face to merge counters into).
    pub async fn upload_to_cloud(&self, account_id: &str) -> Result<Value, ServiceError> {
        let signal_account = {
            let service = self.service.lock().await;
            service.prepare_cloud_upload_account(account_id)?
        };
        let store = Arc::clone(&self.store);
        let data_dir = self.config.data_dir.clone();
        let account = account_id.to_string();
        tokio::task::spawn_blocking({
            let signal_account = signal_account.clone();
            move || {
                crate::backup_export::build_artifact(&store, &data_dir, &account, &signal_account)
            }
        })
        .await
        .map_err(|_| ServiceError::Store(StoreError::Unavailable(None)))?
        .map_err(ServiceError::from)?;
        let engine = self.running_engine().await?;
        match engine
            .call(
                "backups.uploadToCloud",
                json!({ "account": signal_account }),
                CallClass::Mutating,
            )
            .await
        {
            Ok(value) => {
                if value.get("status").and_then(Value::as_str) == Some("started") {
                    Ok(json!({ "status": "started" }))
                } else {
                    Err(ServiceError::Api(ApiError::new(
                        "UPSTREAM_PROTOCOL_ERROR",
                        "backups.uploadToCloud answered without a started status",
                        true,
                    )))
                }
            }
            Err(EngineError::UnknownOutcome) => Err(ServiceError::Api(ApiError::new(
                "SEND_OUTCOME_UNKNOWN",
                "backups.uploadToCloud outcome is unknown; poll backups.uploadStatus",
                false,
            ))),
            Err(EngineError::UpstreamCode { code, data }) => Err(ServiceError::Api(
                crate::service::cloud_upload_face_error(code, data),
            )),
            Err(error) => Err(error.into()),
        }
    }

    /// backups.uploadStatus (contract revision 1.87, §4.72, engine
    /// cloud-backup-contract §6.2): poll the engine's cloud-upload progress
    /// snapshot read-only and mirror it — the `cloud_backup_status`
    /// discipline verbatim. The snapshot is engine-process memory (a
    /// process-wide single slot), so an engine that is down or an
    /// indeterminate read answers `{present: false}` — "not observable" is
    /// exactly the post-restart truth, not an invented state. The mode
    /// gate sits first (the §4.69 status-face precedent): the face has no
    /// store fallback, its whole truth is the engine's upload task.
    pub async fn cloud_upload_status(&self, account_id: &str) -> Result<Value, ServiceError> {
        let signal_account = {
            let service = self.service.lock().await;
            service.prepare_backups_account(account_id)?
        };
        let upstream = match self.running_engine().await {
            Ok(engine) => Some(
                engine
                    .call(
                        "backups.uploadStatus",
                        json!({ "account": signal_account }),
                        CallClass::ReadOnly,
                    )
                    .await,
            ),
            Err(_) => None,
        };
        match upstream {
            Some(Ok(result)) => upload_status_from_engine(&result),
            // Read-only: an indeterminate read degrades to the same
            // not-observable answer as a down engine (never a guessed state).
            Some(Err(EngineError::UnknownOutcome)) | None => Ok(json!({ "present": false })),
            Some(Err(error)) => Err(error.into()),
        }
    }

    /// The §4.68 consumption watcher: poll the engine's import status (~3 s,
    /// 30-minute ceiling) and on a terminal state drive the §4.38 import pass
    /// directly — `completed` constructs the job immediately (bypassing the
    /// link-time probe's completed/attempts gates on purpose: this is an
    /// explicit user request, not a link-time sweep) under the group's
    /// single-flight slot; `failed` or the expired-idle escape lands an honest
    /// failed ledger row with the engine class. `history.importRetry` then
    /// answers NO_ARCHIVE for that row — retry is re-invoking
    /// backups.importSnapshot, the official Retry semantics.
    fn spawn_import_snapshot_watcher(self: &Arc<Self>, account_id: &str) {
        let supervisor = Arc::clone(self);
        let account = account_id.to_string();
        tokio::spawn(async move {
            let started = Instant::now();
            // `completed` observed but the group slot busy: keep polling while
            // the engine still reports it (its task registry is process
            // memory) so a link-time pass holding the slot only delays the
            // claim, never fakes a failure.
            let mut completed_seen = false;
            loop {
                if started.elapsed() >= IMPORT_SNAPSHOT_CEILING {
                    if !completed_seen {
                        land_import_engine_failure(&supervisor.store, &account);
                    }
                    tracing::warn!(
                        account_id = %account,
                        "snapshot import watcher hit its ceiling"
                    );
                    return;
                }
                sleep(IMPORT_SNAPSHOT_POLL).await;
                // A deleted account has no ledger row to land and nothing
                // left to observe — exit quietly.
                let signal_account = match supervisor
                    .service
                    .lock()
                    .await
                    .prepare_backups_account(&account)
                {
                    Ok(number) => number,
                    Err(_) => return,
                };
                let engine = match supervisor.running_engine().await {
                    Ok(engine) => engine,
                    // The task registry is engine-process memory: a down
                    // engine keeps polling within the ceiling in case the
                    // process restarts around it.
                    Err(_) => continue,
                };
                let status = engine
                    .call(
                        "backup.importSnapshotStatus",
                        json!({ "account": signal_account }),
                        CallClass::ReadOnly,
                    )
                    .await;
                match status {
                    Ok(status) => match status.get("state").and_then(Value::as_str) {
                        Some("running") => continue,
                        Some("completed") => {
                            completed_seen = true;
                            let Some(archive) = crate::history_import::archive_path(
                                &supervisor.config.data_dir,
                                &signal_account,
                            ) else {
                                land_import_engine_failure(&supervisor.store, &account);
                                return;
                            };
                            let claimed = claim_and_run_history_import(
                                Arc::clone(&supervisor.history_import_running),
                                Arc::clone(&supervisor.history_import_holder),
                                Arc::clone(&supervisor.store),
                                HistoryImportJob {
                                    account_id: account.clone(),
                                    signal_account,
                                    archive,
                                },
                            )
                            .await;
                            if claimed {
                                return;
                            }
                            // Slot busy: the next tick retries while the
                            // engine still reports completed.
                        }
                        Some("failed") => {
                            land_import_engine_failure(&supervisor.store, &account);
                            return;
                        }
                        // Expired-idle escape: the engine restarted and its
                        // task registry lost the run — the import is gone.
                        _ => {
                            land_import_engine_failure(&supervisor.store, &account);
                            return;
                        }
                    },
                    // Read-only: every error flavor reads as "not yet
                    // terminal" — keep polling within the ceiling.
                    Err(_) => continue,
                }
            }
        });
    }

    /// The §4.69 consumption watcher (contract revision 1.84): follow a
    /// started cloud restore and drive the produced
    /// `history-import-cloud.ndjson` through the §4.38 import pass — the
    /// §4.68 watcher pattern on the cloud artifact. The engine's progress
    /// snapshot cannot certify success (`{present: false}` is the same
    /// answer a never-started or restarted engine gives — a success leaves
    /// no trace), so the completion face is the artifact's disk freshness
    /// face: the `(mtime, len)` identity captured at spawn, and only a face
    /// that CHANGES during the watch is the engine's fresh write — a stale
    /// archive from a previous restore is never imported. `cloudStatus`
    /// rides the loop for the failure face (`phase: "failed"`, the
    /// engine's terminal snapshot), with the §4.68 cadence bounding the
    /// watch: ~3 s polls, a 60-minute ceiling (the watched chain includes
    /// the whole CDN download). The artifact-face check precedes the status
    /// check each tick — the engine writes the file strictly before an
    /// export failure maps onto the failed phase, so a landed artifact is
    /// consumed before a late failure observation could clobber a completed
    /// ledger row.
    fn spawn_cloud_restore_watcher(self: &Arc<Self>, account_id: &str) {
        let supervisor = Arc::clone(self);
        let account = account_id.to_string();
        tokio::spawn(async move {
            let started = Instant::now();
            // The freshness baseline, captured before the first sleep: the
            // restore chain cannot have landed its artifact in the moments
            // between the started answer and here, so this face is exactly
            // the stale world the fresh write must differ from. An
            // unresolvable account or a missing registry entry exits
            // quietly — no ledger row was ever claimed, no face promised an
            // import.
            let Ok(signal_account) = supervisor
                .service
                .lock()
                .await
                .prepare_backups_account(&account)
            else {
                return;
            };
            let Some(archive) = crate::history_import::cloud_archive_path(
                &supervisor.config.data_dir,
                &signal_account,
            ) else {
                tracing::warn!(
                    account_id = %account,
                    "cloud restore watcher found no engine registry entry"
                );
                return;
            };
            let baseline = cloud_archive_face(&archive);
            // A fresh face observed while the group slot is busy: keep
            // polling while it persists (the §4.68 completed-seen posture),
            // so a link-time pass holding the slot only delays the claim,
            // never fakes a failure — and never lands one at the ceiling
            // either.
            let mut fresh_seen = false;
            loop {
                if started.elapsed() >= CLOUD_RESTORE_CEILING {
                    // Last-chance disk check: a slow restore whose artifact
                    // landed between ticks still consumes.
                    let landed = if cloud_archive_face(&archive) != baseline
                        && crate::history_import::archive_ready(&archive)
                    {
                        claim_and_run_history_import(
                            Arc::clone(&supervisor.history_import_running),
                            Arc::clone(&supervisor.history_import_holder),
                            Arc::clone(&supervisor.store),
                            HistoryImportJob {
                                account_id: account.clone(),
                                signal_account: signal_account.clone(),
                                archive: archive.clone(),
                            },
                        )
                        .await
                    } else {
                        false
                    };
                    if !landed && !fresh_seen {
                        land_import_engine_failure(&supervisor.store, &account);
                    }
                    tracing::warn!(
                        account_id = %account,
                        "cloud restore watcher hit its ceiling"
                    );
                    return;
                }
                sleep(CLOUD_RESTORE_POLL).await;
                // A deleted account has no ledger row to land and nothing
                // left to observe — exit quietly (the §4.68 posture).
                if supervisor
                    .service
                    .lock()
                    .await
                    .prepare_backups_account(&account)
                    .is_err()
                {
                    return;
                }
                // ① The artifact face first: a face off the baseline is the
                // engine's definite success signal.
                if cloud_archive_face(&archive) != baseline {
                    if !crate::history_import::archive_ready(&archive) {
                        // Present but empty: not a produced archive (the
                        // §4.38 ready discipline). Keep watching.
                        continue;
                    }
                    fresh_seen = true;
                    let claimed = claim_and_run_history_import(
                        Arc::clone(&supervisor.history_import_running),
                        Arc::clone(&supervisor.history_import_holder),
                        Arc::clone(&supervisor.store),
                        HistoryImportJob {
                            account_id: account.clone(),
                            signal_account: signal_account.clone(),
                            archive: archive.clone(),
                        },
                    )
                    .await;
                    if claimed {
                        return;
                    }
                    // Slot busy: the next tick retries while the fresh face
                    // persists.
                    continue;
                }
                // ② The failure face: a failed phase is the engine's
                // terminal snapshot (it persists until the next start) —
                // land the honest failed ledger row and stop; retry is
                // re-invoking backups.restoreFromCloud, so
                // `history.importRetry` answers NO_ARCHIVE (the §4.68
                // official-Retry semantics). Every other answer —
                // downloading, present:false, a down engine, an unknown
                // read — is "not yet terminal": keep polling within the
                // ceiling.
                let engine = match supervisor.running_engine().await {
                    Ok(engine) => engine,
                    Err(_) => continue,
                };
                let status = engine
                    .call(
                        "backups.cloudStatus",
                        json!({ "account": signal_account.clone() }),
                        CallClass::ReadOnly,
                    )
                    .await;
                if let Ok(status) = status
                    && status.get("phase").and_then(Value::as_str) == Some("failed")
                {
                    land_import_engine_failure(&supervisor.store, &account);
                    return;
                }
            }
        });
    }

    /// Read-only view of the contacts cache; never triggers an upstream call.
    pub async fn list_contacts(
        &self,
        account_id: String,
        query: Option<String>,
        limit: u32,
        cursor: Option<String>,
    ) -> Result<Page<ContactSummary>, ServiceError> {
        // Store-only read: the service lock guards in-memory state only.
        let store = Arc::clone(&self.store);
        blocking_service(move || {
            store_list_contacts(
                &store,
                &account_id,
                query.as_deref(),
                limit,
                cursor.as_deref(),
            )
        })
        .await
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

    /// Read-only projection of one group (§4.8): the cached contacts row plus
    /// — contract revision 1.62, §4.53 appendix — the group's live
    /// requesting-members list. The requesting list is deliberately NOT a
    /// sync-captured cache field: the §4.53 fold treats the engine's own
    /// single-group `listGroups` read as the approval truth source (the
    /// candidate set is intersected against it at write time), and the read
    /// face consumes the same source so the list the desktop renders and the
    /// set the approval write folds against can never disagree — a sync
    /// snapshot would be a second, older truth that the write's own
    /// `JOIN_REQUEST_NO_LONGER_PENDING` stale-ladder exists to paper over.
    /// Freshness follows the read call itself (as fresh as opening the group
    /// info page, the official render rhythm), which also lets the desktop's
    /// post-action refresh observe the converged list without waiting for a
    /// contacts.sync (the sync path cannot carry this face: its 60s
    /// short-circuit would keep answering the pre-approval roster). The read
    /// degrades honestly: an engine that is not running, or a `listGroups`
    /// face without the requesting array, answers the cached row without the
    /// key (the desktop renders no approval section) — the cache face the
    /// method has always promised stays intact.
    pub async fn get_group(
        &self,
        account_id: String,
        group_key: String,
    ) -> Result<GroupDetails, ServiceError> {
        // The live halves first (ReadOnly, the §4.53/§4.57 folds' own read):
        // failure degrades to no live keys instead of failing the cached
        // read.
        let requesting = self
            .requesting_members_for_group(&account_id, &group_key)
            .await
            .unwrap_or_default();
        let pending = self
            .pending_members_for_group(&account_id, &group_key)
            .await
            .unwrap_or_default();
        // Store-only read: the service lock guards in-memory state only.
        let store = Arc::clone(&self.store);
        blocking_service(move || {
            store_get_group_with_requesting(
                &store,
                &account_id,
                &group_key,
                requesting.as_deref(),
                pending.as_deref(),
            )
        })
        .await
    }

    /// The engine's live requesting-members array for one group (contract
    /// revision 1.62, §4.53 appendix): the same single-group `listGroups`
    /// read the §4.53 fold runs, projected to the raw
    /// `JsonGroup.requestingMembers` entries. `Ok(None)` = the engine
    /// answered without the face (every entry drops, the key stays absent);
    /// `Err` = the engine is not running or refused the read (the caller
    /// degrades the same way). The read does not widen the §4.8 addressing:
    /// the caller still gates on the cached kind='group' row before this
    /// face is worth anything, and `store_get_group_with_requesting` keeps
    /// answering `GROUP_NOT_FOUND` for an unsynced group even when the live
    /// read succeeded.
    async fn requesting_members_for_group(
        &self,
        account_id: &str,
        group_key: &str,
    ) -> Result<Option<Vec<Value>>, ServiceError> {
        let engine = match self.running_engine().await {
            Ok(engine) => engine,
            Err(_) => return Ok(None),
        };
        let number = self
            .service
            .lock()
            .await
            .account_signal_number(account_id)?;
        let listed = engine
            .call(
                "listGroups",
                json!({ "account": number, "groupId": [group_key] }),
                CallClass::ReadOnly,
            )
            .await
            .map_err(ServiceError::Engine)?;
        Ok(listed
            .as_array()
            .and_then(|groups| groups.first())
            .and_then(|group| group.get("requestingMembers"))
            .and_then(Value::as_array)
            .cloned())
    }

    /// The engine's live pending-members array for one group (contract
    /// revision 1.64, §4.57): the same single-group `listGroups` read the
    /// §4.57 revoke fold runs, projected to the raw
    /// `JsonGroup.pendingMembers` entries. `Ok(None)` = the engine answered
    /// without the face (every entry drops, the key stays absent); `Err` =
    /// the engine is not running or refused the read (the caller degrades
    /// the same way). The read does not widen the §4.8 addressing: the
    /// caller still gates on the cached kind='group' row before this face is
    /// worth anything, and `store_get_group_with_requesting` keeps answering
    /// `GROUP_NOT_FOUND` for an unsynced group even when the live read
    /// succeeded.
    async fn pending_members_for_group(
        &self,
        account_id: &str,
        group_key: &str,
    ) -> Result<Option<Vec<Value>>, ServiceError> {
        let engine = match self.running_engine().await {
            Ok(engine) => engine,
            Err(_) => return Ok(None),
        };
        let number = self
            .service
            .lock()
            .await
            .account_signal_number(account_id)?;
        let listed = engine
            .call(
                "listGroups",
                json!({ "account": number, "groupId": [group_key] }),
                CallClass::ReadOnly,
            )
            .await
            .map_err(ServiceError::Engine)?;
        Ok(listed
            .as_array()
            .and_then(|groups| groups.first())
            .and_then(|group| group.get("pendingMembers"))
            .and_then(Value::as_array)
            .cloned())
    }

    /// Pull the contacts/groups signal-cli already synced from the primary
    /// device into the local cache. Both calls are ReadOnly on the single engine
    /// queue, so they cannot starve or corrupt mutating sends. A successful
    /// sync less than 60s old short-circuits to the cached counts.
    pub async fn sync_contacts(
        &self,
        account_id: &str,
    ) -> Result<ContactsSyncOutcome, ServiceError> {
        let now_ms = crate::link::now_ms();
        {
            let service = self.service.lock().await;
            if let Some(synced_at) = service.contacts_synced_at(account_id)?
                && now_ms.saturating_sub(synced_at) < 60_000
            {
                let (contact_count, group_count) = service.count_contacts(account_id)?;
                return Ok(ContactsSyncOutcome {
                    contact_count,
                    group_count,
                    synced_at,
                });
            }
        }
        let engine = self.running_engine().await?;
        sync_contacts_with(&self.service, &engine, account_id).await
    }
}

/// Shared body of [`RuntimeSupervisor::sync_contacts`] plus the startup
/// re-sync: the read-only engine listing and the single all-or-nothing upsert
/// transaction. Free-standing so the spawned startup pass can run against the
/// engine handle it captured at spawn time.
async fn sync_contacts_with(
    service: &Arc<Mutex<ConnectorService>>,
    engine: &EngineHandle,
    account_id: &str,
) -> Result<ContactsSyncOutcome, ServiceError> {
    let number = service.lock().await.account_signal_number(account_id)?;
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
        // Contract 1.43 (§4.42): the linked account's own entry (the
        // signal-cli "note to self" contact) flows through the same skeleton
        // path as every other contact. Its composed title is the account's
        // own profile name — raw material the desktop replaces with its
        // localized Note to Self label off the summary's `isSelf` marker; the
        // connector ships no copy of its own.
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
            .map(|members| group_extra_json(members, &item));
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
    let now_ms = crate::link::now_ms();
    service
        .lock()
        .await
        .upsert_synced_contacts(account_id, &entries, now_ms)?;
    Ok(ContactsSyncOutcome {
        contact_count,
        group_count,
        synced_at: now_ms,
    })
}

/// The requesting-members identity set the §4.53 fold reads: the first group
/// entry's `requestingMembers` array (the pinned JsonGroupMemberAddress
/// shape — each entry carries `number` and/or `uuid`, the NON_NULL
/// omissions a real single-identity candidate has), flattened into the
/// set of address strings. Anything else (an engine without the face, a
/// foreign shape) reads as an empty set — the fold then empties and the
/// write never happens, the honest fail-closed direction.
fn requesting_member_identities(listed: &Value) -> std::collections::HashSet<String> {
    listed
        .as_array()
        .and_then(|groups| groups.first())
        .and_then(|group| group.get("requestingMembers"))
        .and_then(Value::as_array)
        .map(|entries| {
            entries
                .iter()
                .filter_map(|entry| entry.as_object())
                .flat_map(|object| {
                    ["number", "uuid"].iter().filter_map(|field| {
                        object
                            .get(*field)
                            .and_then(Value::as_str)
                            .filter(|value| !value.is_empty())
                            .map(str::to_owned)
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// The §4.53 fold: the caller's candidates survive only while the engine
/// still reports them requesting (either identity the set carries). The
/// return keeps the caller's order; empty means no candidate is still
/// requesting — the write never goes upstream.
fn fold_join_request_candidates(
    members: Vec<String>,
    requesting: &std::collections::HashSet<String>,
) -> Vec<String> {
    members
        .into_iter()
        .filter(|member| requesting.contains(member))
        .collect()
}

/// The pending-invitees identity set the §4.57 fold reads: the first group
/// entry's `pendingMembers` array (the pinned JsonGroupMemberAddress shape —
/// each entry carries `number` and/or `uuid`, the NON_NULL omissions a real
/// single-identity invitee has), flattened into the set of address strings.
/// Anything else (an engine without the face, a foreign shape) reads as an
/// empty set — the fold then empties and the write never happens, the
/// honest fail-closed direction.
fn pending_member_identities(listed: &Value) -> std::collections::HashSet<String> {
    listed
        .as_array()
        .and_then(|groups| groups.first())
        .and_then(|group| group.get("pendingMembers"))
        .and_then(Value::as_array)
        .map(|entries| {
            entries
                .iter()
                .filter_map(|entry| entry.as_object())
                .flat_map(|object| {
                    ["number", "uuid"].iter().filter_map(|field| {
                        object
                            .get(*field)
                            .and_then(Value::as_str)
                            .filter(|value| !value.is_empty())
                            .map(str::to_owned)
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// The §4.57 fold: the caller's candidates survive only while the engine
/// still reports them pending invitees (either identity the set carries).
/// The return keeps the caller's order; empty means no candidate is still
/// pending — the write never goes upstream.
fn fold_invite_candidates(
    members: Vec<String>,
    pending: &std::collections::HashSet<String>,
) -> Vec<String> {
    members
        .into_iter()
        .filter(|member| pending.contains(member))
        .collect()
}

/// The cached group row's `extra` JSON (contract revision 1.40, §4.39; the
/// terminated flag added in 1.53, §4.50 appendix; the member label pair added
/// in 1.67, §4.60): the member count the 1.9 sync already recorded, now beside
/// the bounded roster the §4.39 projection reads. Member entries keep the
/// connector-wide first-wins address ladder (`number` before `uuid`); a
/// separate `uuid` key
/// is stored only when the upstream supplied one that differs from `id`.
/// `admin: true` is recorded when the pinned upstream marks the member — its
/// per-member `isAdmin` flag or the group-level `admins` address set
/// (`ListGroupsCommand$JsonGroupMember` / `$JsonGroup`, verified in the
/// 0.14.8 distribution jar); engines without an admin face simply never set
/// it. `labelEmoji` / `label` (contract 1.67, §4.60) are the pinned upstream's
/// per-member label fields (`JsonGroupMember.labelEmoji` / `.label`, both
/// `@JsonInclude(NON_NULL)` — absent keys when the member carries none) passed
/// through verbatim under the upstream's own server budgets: an emoji over 64
/// bytes or a string over 512 bytes (the pinned
/// `GroupsV2Operations` server-side caps; the write face budgets them at
/// `SERVER_EMOJI_BYTE_LIMIT` / `SERVER_STRING_BYTE_LIMIT`) drops the key
/// instead of truncating — a clipped label would render a lie. Engines
/// without a label face simply never set either key. Entries without any
/// address and entries past [`MAX_GROUP_MEMBERS`]
/// drop — a hostile roster can never fail the sync. `isTerminated: true` is
/// recorded when the upstream `JsonGroup` carries it (the §4.50 verified read
/// source; false and absent collapse to absent, the same informative-keys
/// discipline as `admin` — the read projection answers the default false).
fn group_extra_json(members: &[Value], group: &Value) -> String {
    let admin_ids: std::collections::BTreeSet<String> = group
        .get("admins")
        .and_then(Value::as_array)
        .map(|admins| {
            admins
                .iter()
                .take(MAX_GROUP_MEMBERS)
                .filter_map(|entry| entry.as_object().and_then(member_address_id))
                .collect()
        })
        .unwrap_or_default();
    let roster: Vec<Value> = members
        .iter()
        .take(MAX_GROUP_MEMBERS)
        .filter_map(|entry| {
            let object = entry.as_object()?;
            let id = member_address_id(object)?;
            let uuid = ["uuid"]
                .iter()
                .find_map(|key| object.get(*key).and_then(Value::as_str))
                .map(str::trim)
                .filter(|value| !value.is_empty() && *value != id)
                .map(|value| {
                    value
                        .chars()
                        .take(MAX_GROUP_MEMBER_ID_CHARS)
                        .collect::<String>()
                });
            let admin = object.get("isAdmin").and_then(Value::as_bool) == Some(true)
                || admin_ids.contains(&id);
            let label_emoji = bounded_upstream_label(
                object.get("labelEmoji").and_then(Value::as_str),
                GROUP_MEMBER_LABEL_EMOJI_MAX_BYTES,
            );
            let label = bounded_upstream_label(
                object.get("label").and_then(Value::as_str),
                GROUP_MEMBER_LABEL_SERVER_MAX_BYTES,
            );
            let mut projected = serde_json::Map::with_capacity(5);
            projected.insert("id".to_string(), json!(id));
            if let Some(uuid) = uuid {
                projected.insert("uuid".to_string(), json!(uuid));
            }
            if admin {
                projected.insert("admin".to_string(), json!(true));
            }
            if let Some(label_emoji) = label_emoji {
                projected.insert("labelEmoji".to_string(), json!(label_emoji));
            }
            if let Some(label) = label {
                projected.insert("label".to_string(), json!(label));
            }
            Some(Value::Object(projected))
        })
        .collect();
    let mut extra = serde_json::Map::with_capacity(3);
    extra.insert("memberCount".to_string(), json!(members.len()));
    extra.insert("members".to_string(), Value::Array(roster));
    if group.get("isTerminated").and_then(Value::as_bool) == Some(true) {
        extra.insert("isTerminated".to_string(), json!(true));
    }
    Value::Object(extra).to_string()
}

/// One upstream roster address with the connector-wide first-wins ladder:
/// `number` before `uuid`, trimmed, non-empty, bounded to the roster id cap.
fn member_address_id(object: &serde_json::Map<String, Value>) -> Option<String> {
    ["number", "uuid"]
        .iter()
        .find_map(|key| object.get(*key).and_then(Value::as_str))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| {
            value
                .chars()
                .take(MAX_GROUP_MEMBER_ID_CHARS)
                .collect::<String>()
        })
}

/// One upstream label field under its server budget (contract 1.67, §4.60):
/// trimmed, non-empty, and only when it fits the budget whole — the key drops
/// otherwise (a truncated label would render a lie, so over-budget values are
/// treated as malformed and dropped individually, the §4.39 hostile-roster
/// discipline).
fn bounded_upstream_label(value: Option<&str>, max_bytes: usize) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty() && value.len() <= max_bytes)
        .map(str::to_owned)
}

impl Drop for RuntimeSupervisor {
    fn drop(&mut self) {
        self.receive_worker.abort();
        self.account_unlink_worker.abort();
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
        if let Ok(mut slot) = self.media_governor_pass.lock()
            && let Some(handle) = slot.take()
        {
            handle.abort();
        }
        if let Ok(mut slot) = self.contacts_resync.lock()
            && let Some(handle) = slot.take()
        {
            handle.abort();
        }
        if let Ok(mut slot) = self.delivery_receipt_worker.lock()
            && let Some(handle) = slot.take()
        {
            handle.abort();
        }
    }
}

/// Run one store-backed call on the blocking pool: rusqlite is synchronous
/// and must never occupy a tokio worker thread. A blocking task that died
/// without a result is a storage failure for the caller; the JoinError detail
/// is never logged.
async fn blocking_service<T>(
    call: impl FnOnce() -> Result<T, ServiceError> + Send + 'static,
) -> Result<T, ServiceError>
where
    T: Send + 'static,
{
    match tokio::task::spawn_blocking(call).await {
        Ok(result) => result,
        Err(_) => Err(ServiceError::Store(StoreError::Unavailable(None))),
    }
}

/// Map one download-chain failure onto the wire codes. Content failures are
/// terminal (a corrupt or expired transit object does not improve on a
/// re-fetch — §4.38 no-auto-retry); only local filesystem work keeps the
/// advisory retryable flag. Log discipline: the failure variants are
/// content-free already — nothing here logs a URL, path, id, or key.
fn map_download_failure(failure: crate::history_attachment::DownloadFailure) -> ServiceError {
    use crate::history_attachment::DownloadFailure as Failure;
    let (code, text, retryable) = match failure {
        Failure::InvalidLocator(reason) => ("INVALID_REQUEST", reason, false),
        Failure::Http(_) => (
            "UPSTREAM_ERROR",
            "the transit CDN refused the attachment request",
            false,
        ),
        Failure::Network => ("UPSTREAM_ERROR", "the transit CDN download failed", false),
        Failure::BodyTooLarge { .. } => (
            "UPSTREAM_ERROR",
            "the attachment body exceeded its declared bounds",
            false,
        ),
        Failure::BadMac => (
            "UPSTREAM_ERROR",
            "the attachment ciphertext failed its integrity check",
            false,
        ),
        Failure::DigestMismatch => (
            "UPSTREAM_ERROR",
            "the attachment ciphertext failed its integrity check",
            false,
        ),
        Failure::BadPadding | Failure::PlaintextTooShort { .. } => (
            "UPSTREAM_ERROR",
            "the attachment could not be decrypted",
            false,
        ),
        Failure::PlaintextHashMismatch => (
            "UPSTREAM_ERROR",
            "the attachment failed its plaintext integrity check",
            false,
        ),
        Failure::Io => ("UPSTREAM_ERROR", "local attachment storage failed", true),
    };
    ServiceError::Api(ApiError::new(code, text, retryable))
}

/// The §4.38 step-3 CDN download client: the same connect/total budgets and
/// no-redirect posture the chain has always used, plus the launcher's SOCKS
/// route when the caller passes the `KT_SIGNAL_SOCKS_PROXY` environment
/// value (injected, so both branches stay testable without mutating process
/// state). The value parses through the doctor's tolerant parse — the
/// canonical `host:port`, a `socks5(h)://` prefix normalized away — and
/// rides the client as `socks5h://` (remote DNS), the same `ALL_PROXY`
/// mapping the engine child gets, so a walled or DNS-poisoned network still
/// reaches the transit CDN over the route the engine's own transport takes.
/// A missing or malformed value builds the direct client: byte-for-byte the
/// behavior before the route existed.
fn attachment_download_client(proxy: Option<&str>) -> Result<reqwest::Client, ServiceError> {
    let mut builder = reqwest::Client::builder()
        .connect_timeout(crate::history_attachment::CONNECT_TIMEOUT)
        .timeout(crate::history_attachment::TOTAL_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none());
    if let Some(proxy) = proxy.and_then(crate::doctor::parse_socks_proxy_env) {
        let socks_url = format!("socks5h://{}:{}", proxy.host, proxy.port);
        builder = builder.proxy(reqwest::Proxy::all(&socks_url).map_err(|_| ServiceError::Io)?);
    }
    builder.build().map_err(|_| ServiceError::Io)
}

/// Best-effort temp cleanup; the download dir stays governed, so a leftover
/// temp only wastes its own bytes.
fn cleanup_temp(path: &std::path::Path) {
    let _ = std::fs::remove_file(path);
}

// ---------------------------------------------------------------------------
// Link-time history import plumbing (contract revision 1.39, §4.38).
// ---------------------------------------------------------------------------

/// One importable account: the identity the blocking import needs to open
/// the engine's archive through the registry.
#[derive(Clone)]
struct HistoryImportJob {
    account_id: String,
    signal_account: String,
    archive: PathBuf,
}

/// What the blocking eligibility probe decided for one account.
enum HistoryImportProbe {
    /// Archive present, no completed row, attempts inside the budget.
    Ready(HistoryImportJob),
    /// No archive on the engine face yet — the link watcher keeps polling.
    Waiting,
    /// Nothing to do: account gone, completed row, or attempt budget spent.
    Done,
}

fn probe_history_import(
    store: &Arc<Store>,
    data_dir: &Path,
    account_id: &str,
) -> HistoryImportProbe {
    let Ok(Some(account)) = store.account_by_id(account_id) else {
        return HistoryImportProbe::Done;
    };
    let Some(archive) = crate::history_import::archive_path(data_dir, &account.signal_account)
    else {
        // No registry entry for this number: the engine face cannot name an
        // archive path, so there is nothing to watch for.
        return HistoryImportProbe::Done;
    };
    if !crate::history_import::archive_ready(&archive) {
        return HistoryImportProbe::Waiting;
    }
    match store.history_import_row(account_id) {
        // Store trouble must not spin the watcher: treat as nothing-to-do.
        Err(_) => HistoryImportProbe::Done,
        Ok(Some(row)) if row.state == "completed" => HistoryImportProbe::Done,
        Ok(Some(row)) if row.attempts >= crate::history_import::MAX_ATTEMPTS => {
            HistoryImportProbe::Done
        }
        _ => HistoryImportProbe::Ready(HistoryImportJob {
            account_id: account_id.to_string(),
            signal_account: account.signal_account,
            archive,
        }),
    }
}

/// Claim the group's single import slot, run the import on the blocking pool,
/// and converge retention after a completed run (§4.38). `false` means
/// another import holds the slot; the caller retries later. The holder name
/// rides the claim window so `history.importStatus` can attribute the
/// `processing` stage to the polled account (contract 1.53).
async fn claim_and_run_history_import(
    running: Arc<AtomicBool>,
    holder: Arc<StdMutex<Option<String>>>,
    store: Arc<Store>,
    job: HistoryImportJob,
) -> bool {
    if running
        .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
        .is_err()
    {
        return false;
    }
    if let Ok(mut slot) = holder.lock() {
        *slot = Some(job.account_id.clone());
    }
    run_history_import(&store, &job).await;
    if let Ok(mut slot) = holder.lock() {
        *slot = None;
    }
    running.store(false, Ordering::Release);
    true
}

async fn run_history_import(store: &Arc<Store>, job: &HistoryImportJob) {
    let outcome = tokio::task::spawn_blocking({
        let store = Arc::clone(store);
        let job = job.clone();
        move || {
            crate::history_import::run_import(
                &store,
                &job.account_id,
                &job.signal_account,
                &job.archive,
            )
        }
    })
    .await;
    match outcome {
        Ok(Ok(counters)) => {
            tracing::info!(
                account_id = %job.account_id,
                imported = counters.imported,
                skipped_lines = counters.skipped_lines,
                skipped_chats = counters.skipped_chats,
                skipped_messages = counters.skipped_messages,
                "history import completed"
            );
            converge_history_retention(Arc::clone(store)).await;
        }
        Ok(Err(failure)) => {
            tracing::warn!(
                account_id = %job.account_id,
                error_class = failure.class(),
                "history import failed"
            );
        }
        Err(_) => {
            // The blocking task died without landing a terminal state; record
            // an honest failure so the face never shows a phantom `running`.
            let _ = store.complete_history_import(
                &job.account_id,
                "failed",
                &crate::store::ImportCounters::default(),
                Some("internal"),
                crate::link::now_ms(),
            );
            tracing::warn!(account_id = %job.account_id, "history import task died");
        }
    }
}

// ---------------------------------------------------------------------------
// Official-desktop snapshot import plumbing (contract revision 1.78, §4.68).
// ---------------------------------------------------------------------------

/// The §4.68 consumption-watcher cadence: ~3 s polls, a 30-minute ceiling.
const IMPORT_SNAPSHOT_POLL: Duration = Duration::from_secs(3);
const IMPORT_SNAPSHOT_CEILING: Duration = Duration::from_secs(30 * 60);

/// The §4.69 consumption-watcher cadence (contract revision 1.84): the
/// §4.68 watcher's ~3 s polls. The ceiling rides wider than the §4.68
/// import ceiling because the watched chain is the engine's whole restore —
/// credentials, the CDN download of the entire backup, decrypt, export.
const CLOUD_RESTORE_POLL: Duration = Duration::from_secs(3);
const CLOUD_RESTORE_CEILING: Duration = Duration::from_secs(60 * 60);

/// The cloud archive's freshness face: `(modified, len)`, `None` when the
/// file is absent. Equality with the face captured at watcher spawn means
/// "the artifact did not change during the watch" — only a changed face is
/// the engine's fresh write (the write is whole and strictly post-success,
/// engine `src/service/cloud_backup.rs`), so a stale archive from a
/// previous restore is never imported a second time.
fn cloud_archive_face(path: &Path) -> Option<(SystemTime, u64)> {
    let metadata = std::fs::metadata(path).ok()?;
    Some((metadata.modified().ok()?, metadata.len()))
}

/// Land the watcher's honest failure row: an engine-side task failure, the
/// expired-idle escape, or the ceiling. `begin` upserts the row (an account
/// may never have had a link-time import), `complete` stamps the failed
/// terminal state with the engine class so `history.importStatus` renders it
/// and `history.importRetry` answers NO_ARCHIVE — retry is re-invoking
/// backups.importSnapshot, the official Retry semantics.
fn land_import_engine_failure(store: &Arc<Store>, account_id: &str) {
    let _ = store.begin_history_import(account_id, crate::link::now_ms());
    let _ = store.complete_history_import(
        account_id,
        "failed",
        &crate::store::ImportCounters::default(),
        Some("engine"),
        crate::link::now_ms(),
    );
}

/// The §4.68 status projection: the engine's answer verbatim minus the raw
/// `account` echo, `accountId` riding in its place (the §4.46 precedent).
fn import_snapshot_status_from_engine(account_id: &str, mut result: Value) -> Value {
    if let Some(object) = result.as_object_mut() {
        object.remove("account");
        object.insert("accountId".to_owned(), json!(account_id));
    }
    result
}

/// The disk-truth degradation (the §4.46 posture): an engine that is down or
/// an indeterminate read never invents a task state — idle plus the NDJSON
/// presence the produced archive actually shows.
fn import_snapshot_status_idle_projection(
    data_dir: &Path,
    account_id: &str,
    signal_account: &str,
) -> Value {
    let present = crate::history_import::archive_path(data_dir, signal_account)
        .map(|archive| crate::history_import::archive_ready(&archive))
        .unwrap_or(false);
    json!({ "accountId": account_id, "state": "idle", "ndjsonPresent": present })
}

/// The §4.69 cloudStatus projection: the engine's progress snapshot mirrored
/// through a shape gate (engine output is not trusted input — the
/// transfer.downloadStatus gate posture). `present:false` passes as the bare
/// absent face; a present snapshot must carry the two-value phase
/// (`downloading`/`failed`) and a non-negative integer `bytesRead`, with
/// `bytesTotal`/`errorCode` optional and typed. The answer is rebuilt
/// key-by-key so no unplanned key rides and a `bytesTotal` absence stays an
/// honest absence (never a zero). A malformed present-snapshot face fails
/// UPSTREAM_PROTOCOL_ERROR instead of degrading to `present:false` — a read
/// that delivered junk knows nothing, and `present:false` would claim it does.
fn cloud_status_from_engine(result: &Value) -> Result<Value, ServiceError> {
    if result.get("present") == Some(&Value::Bool(false)) {
        return Ok(json!({ "present": false }));
    }
    let phase = result.get("phase").and_then(Value::as_str);
    let bytes_read = result.get("bytesRead").and_then(Value::as_u64);
    let bytes_total = result.get("bytesTotal").and_then(Value::as_u64);
    let error_code = result.get("errorCode").and_then(Value::as_str);
    let shaped = result.get("present") == Some(&Value::Bool(true))
        && matches!(phase, Some("downloading") | Some("failed"))
        && bytes_read.is_some()
        && result.get("bytesTotal").is_some() == bytes_total.is_some()
        && result.get("errorCode").is_some() == error_code.is_some();
    if !shaped {
        return Err(ServiceError::Api(ApiError::new(
            "UPSTREAM_PROTOCOL_ERROR",
            "backups.cloudStatus answered a malformed snapshot",
            true,
        )));
    }
    let mut payload = serde_json::Map::new();
    payload.insert("present".to_owned(), json!(true));
    payload.insert("phase".to_owned(), json!(phase));
    payload.insert("bytesRead".to_owned(), json!(bytes_read));
    if let Some(total) = bytes_total {
        payload.insert("bytesTotal".to_owned(), json!(total));
    }
    if let Some(code) = error_code {
        payload.insert("errorCode".to_owned(), json!(code));
    }
    Ok(Value::Object(payload))
}

/// The §4.72 uploadStatus projection (contract 1.87, engine
/// cloud-backup-contract §6.2): the engine's progress snapshot mirrored
/// through a shape gate — the `cloud_status_from_engine` posture verbatim
/// (engine output is not trusted input), with the upload three-value phase
/// set: `exporting` (the assembly stage — byte counts carry no semantics
/// there, the engine keeps bytesRead at 0 and the total absent),
/// `uploading` (the uplink stage — block counts valid, the total is the
/// assembled artifact's byte count), `failed` (the terminal failure —
/// errorCode carries the same code table as the start face's error ladder,
/// the snapshot persists until the next start overwrites it).
/// `present:false` passes as the bare absent face (never started /
/// succeeded — a success leaves no trace — / the engine restarted); a
/// `bytesTotal` absence stays an honest absence (never a zero); the answer
/// is rebuilt key-by-key so no unplanned key rides; a malformed
/// present-snapshot face fails UPSTREAM_PROTOCOL_ERROR instead of
/// degrading to `present:false` — a read that delivered junk knows
/// nothing.
fn upload_status_from_engine(result: &Value) -> Result<Value, ServiceError> {
    if result.get("present") == Some(&Value::Bool(false)) {
        return Ok(json!({ "present": false }));
    }
    let phase = result.get("phase").and_then(Value::as_str);
    let bytes_read = result.get("bytesRead").and_then(Value::as_u64);
    let bytes_total = result.get("bytesTotal").and_then(Value::as_u64);
    let error_code = result.get("errorCode").and_then(Value::as_str);
    let shaped = result.get("present") == Some(&Value::Bool(true))
        && matches!(
            phase,
            Some("exporting") | Some("uploading") | Some("failed")
        )
        && bytes_read.is_some()
        && result.get("bytesTotal").is_some() == bytes_total.is_some()
        && result.get("errorCode").is_some() == error_code.is_some();
    if !shaped {
        return Err(ServiceError::Api(ApiError::new(
            "UPSTREAM_PROTOCOL_ERROR",
            "backups.uploadStatus answered a malformed snapshot",
            true,
        )));
    }
    let mut payload = serde_json::Map::new();
    payload.insert("present".to_owned(), json!(true));
    payload.insert("phase".to_owned(), json!(phase));
    payload.insert("bytesRead".to_owned(), json!(bytes_read));
    if let Some(total) = bytes_total {
        payload.insert("bytesTotal".to_owned(), json!(total));
    }
    if let Some(code) = error_code {
        payload.insert("errorCode".to_owned(), json!(code));
    }
    Ok(Value::Object(payload))
}

/// §4.38: one bounded retention pass after a completed import, so the store
/// converges to the per-conversation cap within the same session. Same batch
/// discipline as the once-per-start retention pass.
async fn converge_history_retention(store: Arc<Store>) {
    let now = crate::link::now_ms();
    loop {
        let outcome = tokio::task::spawn_blocking({
            let store = Arc::clone(&store);
            move || store.prune_history(now, RETENTION_BATCH_MESSAGES)
        })
        .await
        .unwrap_or(Err(StoreError::Unavailable(None)));
        match outcome {
            Ok(outcome) => {
                if outcome.messages_deleted < u64::from(RETENTION_BATCH_MESSAGES) {
                    return;
                }
                sleep(RETENTION_BATCH_PAUSE).await;
            }
            Err(_) => return,
        }
    }
}

/// The `history.importStatus` projection (§4.38). Priority order is the
/// contract's: a persisted ledger row (running/completed/failed) always
/// answers; otherwise the archive presence or the link-time wait window
/// decides between `pending` and `unavailable` — the engine's failure modes
/// are indistinguishable from "the phone had nothing to transfer" and are
/// reported as such, never guessed.
fn history_import_status_projection(
    store: &Arc<Store>,
    data_dir: &Path,
    account_id: &str,
) -> Result<Value, ServiceError> {
    let account = store
        .account_by_id(account_id)?
        .ok_or(ServiceError::Store(StoreError::AccountNotFound))?;
    if let Some(row) = store.history_import_row(account_id)? {
        return serde_json::to_value(row)
            .map_err(|_| ServiceError::Store(StoreError::Unavailable(None)));
    }
    let on_disk = crate::history_import::archive_path(data_dir, &account.signal_account)
        .map(|path| crate::history_import::archive_ready(&path))
        .unwrap_or(false);
    if on_disk {
        // Archive present, no run has claimed it yet: the sweep or the link
        // watcher picks it up within seconds.
        return Ok(json!({ "accountId": account_id, "state": "pending" }));
    }
    let linked_at = store
        .account_summary(account_id)?
        .and_then(|summary| summary.linked_at);
    let inside_window = linked_at.is_some_and(|linked_at| {
        crate::link::now_ms().saturating_sub(linked_at) < crate::history_import::LINK_WAIT_MS
    });
    if inside_window {
        return Ok(json!({ "accountId": account_id, "state": "pending" }));
    }
    Ok(json!({
        "accountId": account_id,
        "state": "unavailable",
        "reason": "no-archive",
    }))
}

/// The contract 1.53 `progress` object the desktop gets while
/// `state === "running"`: `downloading` (byte progress from the engine's
/// transfer snapshot) or `processing` (the connector's own NDJSON import pass
/// — the official Process stage renders copy, not numbers, so no byte fields
/// ride this shape). `None` means the caller's projection stays without
/// `progress` — the honest "nothing observable" answer, never a guess.
///
/// Byte fields ride together or not at all (the contract's two brothers):
/// the engine only reports both when it read a content-length, so a snapshot
/// with a total presents `bytesTransferred` + `bytesTotal`, and one without
/// presents `stage` only.
///
/// Shape gate (`transfer.downloadStatus` is engine output, not trusted
/// input): `present` must be a true boolean and `bytesRead` a non-negative
/// integer within u64; `bytesTotal`, when present, the same; when both ride,
/// `bytesRead <= bytesTotal` must hold. A malformed snapshot drops the
/// `progress` object entirely — the state projection is never sacrificed for
/// a malformed ornament ("better to omit than to get it wrong").
fn import_progress_from_engine_snapshot(snapshot: &Value) -> Option<Value> {
    if snapshot.get("present") != Some(&Value::Bool(true)) {
        return None;
    }
    let bytes_read = snapshot.get("bytesRead").and_then(Value::as_u64);
    let bytes_total = snapshot.get("bytesTotal").and_then(Value::as_u64);
    // A present-but-malformed byte field kills the object (better to omit
    // than to get it wrong); transferred > total is unrenderable the same way.
    if snapshot.get("bytesRead").is_some() && bytes_read.is_none() {
        return None;
    }
    if snapshot.get("bytesTotal").is_some() && bytes_total.is_none() {
        return None;
    }
    if let (Some(read), Some(total)) = (bytes_read, bytes_total)
        && read > total
    {
        return None;
    }
    let mut progress = serde_json::Map::new();
    progress.insert("stage".to_owned(), json!("downloading"));
    if let (Some(read), Some(total)) = (bytes_read, bytes_total) {
        progress.insert("bytesTransferred".to_owned(), json!(read));
        progress.insert("bytesTotal".to_owned(), json!(total));
    }
    Some(Value::Object(progress))
}

/// The `running`-state progress composition (contract 1.53): the engine's
/// download snapshot wins (stage downloading), else an active connector
/// import pass answers stage processing, else the pre-1.53 shape — no
/// progress invented.
fn compose_import_progress(downloading: Option<Value>, import_pass_active: bool) -> Option<Value> {
    if downloading.is_some() {
        return downloading;
    }
    if import_pass_active {
        return Some(json!({ "stage": "processing" }));
    }
    None
}

/// The `history.importRetry` service (contract revision 1.53, §4.38): the
/// state ladder answers before anything else — a non-`failed` row (or a
/// `running` row's absence of a re-armable state) is NOT_RETRYABLE; a failed
/// row (or a failed-engine-download face with no row at all) re-arms only
/// when the archive is still on the engine face, else NO_ARCHIVE. Success is
/// the re-armed answer `{armed: true, state: "pending"}`.
fn history_import_retry_projection(
    store: &Arc<Store>,
    data_dir: &Path,
    account_id: &str,
) -> Result<Value, ServiceError> {
    let account = store
        .account_by_id(account_id)?
        .ok_or(ServiceError::Store(StoreError::AccountNotFound))?;
    let on_disk = crate::history_import::archive_path(data_dir, &account.signal_account)
        .map(|path| crate::history_import::archive_ready(&path))
        .unwrap_or(false);
    let row = store.history_import_row(account_id)?;
    match row.as_ref().map(|row| row.state.as_str()) {
        // A failed row with its archive still on disk: clear and re-arm.
        Some("failed") if on_disk => {
            store.clear_failed_history_import(account_id)?;
            Ok(json!({ "armed": true, "state": "pending" }))
        }
        // A failed row whose archive is gone: nothing to re-import.
        Some("failed") => Err(ServiceError::Api(ApiError::new(
            "NO_ARCHIVE",
            "the transfer archive is no longer on the engine face",
            false,
        ))),
        // Completed/running rows are not retryable by definition (completed
        // is the consumption marker; running belongs to the live pass).
        Some(_) => Err(ServiceError::Api(ApiError::new(
            "NOT_RETRYABLE",
            "the import is not in a failed state",
            false,
        ))),
        // No row: the archive face decides. An archive that never completed a
        // run (the engine's best-effort download failed, or the connector
        // died mid-run leaving no row) re-arms when the file is there;
        // otherwise there is nothing to retry — the honest NO_ARCHIVE.
        None if on_disk => Ok(json!({ "armed": true, "state": "pending" })),
        None => Err(ServiceError::Api(ApiError::new(
            "NO_ARCHIVE",
            "no transfer archive has been produced for this account",
            false,
        ))),
    }
}

// ---------------------------------------------------------------------------
// Android local-backup export plumbing (contract revision 1.47, §4.46).
// ---------------------------------------------------------------------------

/// Map the backup handle table's content-free failures onto the wire codes —
/// the media chain's ladder: caller mistakes are `INVALID_REQUEST` (one
/// indistinguishable unknown-handle answer so handle space cannot be probed),
/// a mid-stream I/O failure is the transient `UPSTREAM_ERROR`.
fn map_backup_handle_error(error: crate::backup_export::BackupHandleError) -> ServiceError {
    let (code, message, retryable) = match error {
        crate::backup_export::BackupHandleError::NotFound => (
            "INVALID_REQUEST",
            "fileHandle is unknown, expired, or already closed",
            false,
        ),
        crate::backup_export::BackupHandleError::NotSequential { .. } => (
            "INVALID_REQUEST",
            "offset must match the bytes already read on this handle",
            false,
        ),
        crate::backup_export::BackupHandleError::Exhausted => (
            "INVALID_REQUEST",
            "the account already holds the maximum number of open backup file handles",
            false,
        ),
        crate::backup_export::BackupHandleError::Io => {
            ("UPSTREAM_ERROR", "snapshot file read failed", true)
        }
    };
    ServiceError::Api(ApiError::new(code, message, retryable))
}

/// Resolve the account and its latest snapshot directory, or answer the
/// no-snapshot code. Store work only — the caller runs it on the blocking
/// pool.
fn export_latest_snapshot(
    store: &Arc<Store>,
    data_dir: &Path,
    account_id: &str,
    signal_account: &str,
) -> Result<PathBuf, ServiceError> {
    let account = store
        .account_by_id(account_id)?
        .ok_or(ServiceError::Store(StoreError::AccountNotFound))?;
    if account.signal_account != signal_account {
        return Err(ServiceError::Store(StoreError::AccountNotFound));
    }
    let account_dir = crate::history_import::engine_account_dir(data_dir, signal_account).ok_or(
        ServiceError::from(crate::backup_export::BuildError::NoEngineDirectory),
    )?;
    crate::backup_export::latest_snapshot(&account_dir)
        .map(|entry| {
            account_dir
                .join(crate::backup_export::SNAPSHOT_ROOT)
                .join(entry.name)
        })
        .ok_or_else(|| {
            ServiceError::Api(ApiError::new(
                "BACKUP_SNAPSHOT_MISSING",
                "the account has no local-backup snapshot yet",
                false,
            ))
        })
}

/// The engine-down / unknown-outcome `backups.exportStatus` answer: the disk
/// truth (snapshot list) plus `idle` — the task registry is engine-process
/// memory, so without an engine there is provably no running task (§10.4).
fn export_status_idle_projection(
    data_dir: &Path,
    account_id: &str,
    signal_account: &str,
) -> Result<Value, ServiceError> {
    let account_dir = crate::history_import::engine_account_dir(data_dir, signal_account);
    let snapshots = account_dir
        .as_ref()
        .map(|dir| crate::backup_export::list_snapshots(dir))
        .unwrap_or_default();
    let snapshots = snapshots
        .iter()
        .map(|entry| {
            json!({
                "name": entry.name,
                "mainBytes": entry.main_bytes,
            })
        })
        .collect::<Vec<_>>();
    let mut result = json!({
        "accountId": account_id,
        "state": "idle",
        "artifactPresent": false,
        "snapshots": snapshots,
    });
    if let Some(dir) = &account_dir
        && let Some(entry) = crate::backup_export::latest_snapshot(dir)
    {
        result["snapshot"] = json!({
            "name": entry.name,
            "mainBytes": entry.main_bytes,
        });
    }
    Ok(result)
}

/// The engine-answering `backups.exportStatus` projection (§4.46): the engine
/// payload mirrors honestly, minus the raw `account` echo (the number never
/// crosses the host boundary) — `accountId` rides in its place, and the
/// connector's own skip counters merge in when a build has run.
fn export_status_projection(
    account_id: &str,
    _signal_account: &str,
    mut result: Value,
    counters: Option<crate::backup_export::SkipCounters>,
) -> Value {
    let object = result.as_object_mut().expect("engine result object");
    object.remove("account");
    object.insert(
        "accountId".to_string(),
        Value::String(account_id.to_string()),
    );
    if let Some(counters) = counters {
        counters.merge_into(&mut result);
    }
    result
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

/// Run one media governor pass on a blocking task (ADR 0002): pure
/// filesystem work — the store lock is never taken, so a deletion sweep can
/// never delay receives or host requests. Counts only in the log; never
/// paths or ids.
fn run_media_governor_pass(governor: MediaGovernor) -> JoinHandle<()> {
    tokio::spawn(async move {
        let stats = tokio::task::spawn_blocking(move || governor.run_pass())
            .await
            .unwrap_or_default();
        if stats.deleted > 0 {
            tracing::info!(
                deleted = stats.deleted,
                bytes_freed = stats.bytes_deleted,
                "media governor freed attachment storage"
            );
        }
    })
}

/// Contract 1.21: authorization failures observed on this group's engine mark
/// the dead account in the shared store and surface it on the host lane
/// (account.changed). The engine classifies the raw upstream error here —
/// this loop never sees error content, only the attributed decision.
async fn account_unlink_watch_loop(
    mut engine_events: broadcast::Receiver<EngineEvent>,
    service: Arc<Mutex<ConnectorService>>,
    host_events: broadcast::Sender<HostSideEvent>,
) {
    loop {
        match engine_events.recv().await {
            Ok(EngineEvent::AccountUnauthorized { account }) => {
                let marked = {
                    let guard = service.lock().await;
                    guard.mark_account_device_unlinked(&account)
                };
                match marked {
                    Ok(events) => {
                        for event in events {
                            let _ = host_events.send(event);
                        }
                    }
                    Err(error) => {
                        // The state re-syncs on the next failure or list;
                        // never take the watcher down over a store hiccup.
                        tracing::warn!(error_class = error.class(), "device-unlinked mark failed");
                    }
                }
            }
            Ok(_) => {}
            Err(broadcast::error::RecvError::Lagged(_)) => continue,
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }
}

async fn receive_persistence_loop(
    mut receives: mpsc::Receiver<QueuedReceive>,
    service: Arc<Mutex<ConnectorService>>,
    host_events: broadcast::Sender<HostSideEvent>,
    owner_group: String,
    media_governor: Option<MediaGovernor>,
    delivery_receipts: mpsc::Sender<DeliveryReceiptJob>,
) {
    let mut storage_unavailable = false;
    // ADR 0002 trigger discipline: after every GOVERNOR_INBOUND_MESSAGE_INTERVAL
    // persisted inbound messages, one retention pass. A plain counter in this
    // single task — no shared state, no resident timer thread.
    let mut inbound_since_governor_pass = 0_u64;
    while let Some(queued) = receives.recv().await {
        crate::metrics::receive_queue_drained();
        let mut retry_delay = RECEIVE_STORE_RETRY_MIN;
        loop {
            // The ingest is a synchronous SQLite batch serialized under the
            // service lock as before, but it runs on the blocking pool so a
            // slow store never occupies a tokio worker thread.
            let result = blocking_service({
                let receive = queued.receive().clone();
                let service = Arc::clone(&service);
                let owner_group = owner_group.clone();
                move || {
                    service
                        .blocking_lock()
                        .ingest_receive(receive, &owner_group)
                }
            })
            .await;
            match result {
                Ok(events) => {
                    for event in &events {
                        let _ = host_events.send(event.clone());
                    }
                    if storage_unavailable {
                        storage_unavailable = false;
                        tracing::info!(state = "recovered", "receive persistence recovered");
                        let _ =
                            host_events.send(HostSideEvent::StorageChanged { state: "recovered" });
                    }
                    if let Some(governor) = media_governor.as_ref()
                        && queued.receive().direction == "incoming"
                    {
                        inbound_since_governor_pass += 1;
                        if inbound_since_governor_pass
                            >= crate::media::GOVERNOR_INBOUND_MESSAGE_INTERVAL
                        {
                            inbound_since_governor_pass = 0;
                            run_media_governor_pass(governor.clone());
                        }
                    }
                    // Auto delivery receipt (contract revision 1.34): only a
                    // real landing proves eligibility — the MessageUpserted
                    // event in `events` means the row was inserted now (not a
                    // dedupe replay), and only incoming dataMessage envelopes
                    // carry an author the receipt can address. Bounded and
                    // lossy: a full queue drops the job silently.
                    if let Some(job) = delivery_receipt_job(queued.receive(), &events)
                        && let Err(_) = delivery_receipts.try_send(job)
                    {
                        // Queue full: drop the receipt, never block the
                        // receive pipeline.
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

/// Eligibility for one auto delivery receipt (contract revision 1.34): the
/// envelope must be an incoming dataMessage carrying account, author, and
/// timestamp, and the ingest pass must have produced a `MessageUpserted`
/// event for an incoming row — proof the row landed now, not that the
/// envelope deduped against an already-stored message.
fn delivery_receipt_job(
    receive: &NormalizedReceive,
    events: &[HostSideEvent],
) -> Option<DeliveryReceiptJob> {
    if receive.direction != "incoming" || receive.content_kind != "dataMessage" {
        return None;
    }
    let landed = events.iter().any(|event| {
        matches!(
            event,
            HostSideEvent::MessageUpserted(record) if record.direction == "incoming"
        )
    });
    if !landed {
        return None;
    }
    Some(DeliveryReceiptJob {
        account: receive.account.clone()?,
        recipient: receive.source.clone()?,
        timestamp: receive.timestamp?,
    })
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

/// The connector-wide contact address ladder (number before uuid, the
/// `numberUuid` projection tail included) — the same match
/// [`find_contact_display_name`] runs, exposed for the §4.58 factor stamp so
/// both consumers of one listContacts answer can never disagree about which
/// identity a row is addressed by. First present non-empty identity wins.
fn contact_item_peer_key(item: &Value) -> Option<&str> {
    ["number", "uuid", "numberUuid"].iter().find_map(|key| {
        item.get(*key)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
    })
}

/// Contract 1.65 (§4.58): serialize exactly the factor flags one engine
/// contact item exposed. The upstream names are the pinned signal-cli
/// `JsonContact` fields (`isHidden` / `profileSharing` / `isBlocked`); the
/// stored keys drop the `is` prefix for hidden/blocked to match the official
/// desktop's factor vocabulary (`hidden` / `profileSharing`). An item without
/// any of the three stamps the empty object — the probed-absent marker an
/// engine without the trio (the kt-engine read face today) legitimately
/// leaves, so the probe never re-fires for that row.
fn mrr_factors_stamp(item: &Value) -> String {
    let mut object = serde_json::Map::new();
    for (stored, upstream) in [
        ("hidden", "isHidden"),
        ("profileSharing", "profileSharing"),
        ("blocked", "isBlocked"),
    ] {
        if let Some(flag) = item.get(upstream).and_then(Value::as_bool) {
            object.insert(stored.to_string(), Value::Bool(flag));
        }
    }
    Value::Object(object).to_string()
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

/// Project the engine's `getStickerPackManifest` result (contract 1.36,
/// §4.32): bounded title/author (KT 256-char bound, the official decoder
/// applies none), the optional cover, and the sticker list truncated at the
/// 1024-entry cap — the official decoder's truncation semantics, mirrored
/// rather than rejected. A result missing a required field is a protocol
/// error, not a partial manifest.
fn manifest_from_engine(result: &Value) -> Result<StickerPackManifest, ServiceError> {
    let title = bounded_manifest_text(
        result,
        "title",
        crate::service::MAX_STICKER_MANIFEST_TITLE_CHARS,
    )?;
    let author = bounded_manifest_text(
        result,
        "author",
        crate::service::MAX_STICKER_MANIFEST_AUTHOR_CHARS,
    )?;
    let cover = result.get("cover").map(manifest_entry_from_engine);
    let stickers_json = result
        .get("stickers")
        .and_then(Value::as_array)
        .ok_or(ServiceError::Engine(EngineError::Protocol))?;
    let stickers: Vec<StickerManifestEntry> = stickers_json
        .iter()
        .take(crate::service::MAX_STICKER_MANIFEST_ENTRIES)
        .map(manifest_entry_from_engine)
        .collect();
    let sticker_count = result
        .get("stickerCount")
        .and_then(Value::as_u64)
        .ok_or(ServiceError::Engine(EngineError::Protocol))?;
    if sticker_count > u32::MAX as u64 {
        return Err(ServiceError::Engine(EngineError::Protocol));
    }
    Ok(StickerPackManifest {
        title,
        author,
        cover,
        stickers,
        sticker_count: sticker_count as u32,
    })
}

/// One manifest entry (cover or sticker): id required, emoji/contentType
/// optional and absent-on-absent so the wire matches the official optionality.
fn manifest_entry_from_engine(entry: &Value) -> StickerManifestEntry {
    let id = entry.get("id").and_then(Value::as_u64).unwrap_or(0) as u32;
    let emoji = entry
        .get("emoji")
        .and_then(Value::as_str)
        .map(str::to_string);
    let content_type = entry
        .get("contentType")
        .and_then(Value::as_str)
        .map(str::to_string);
    StickerManifestEntry {
        id,
        emoji,
        content_type,
    }
}

fn bounded_manifest_text(
    result: &Value,
    key: &str,
    max_chars: usize,
) -> Result<String, ServiceError> {
    let text = result
        .get(key)
        .and_then(Value::as_str)
        .ok_or(ServiceError::Engine(EngineError::Protocol))?;
    if text.chars().count() > max_chars {
        return Err(ServiceError::Engine(EngineError::Protocol));
    }
    Ok(text.to_string())
}

/// Project the engine's `getStickerImage` result (contract 1.36, §4.32):
/// base64/size/contentType pass through; the encoded length is checked
/// against the 400 KB budget (300 KB CDN ceiling + base64 expansion), and a
/// `size` that disagrees with the decoded byte count is a protocol error.
fn image_from_engine(result: &Value) -> Result<StickerPackImage, ServiceError> {
    let data_base64 = result
        .get("dataBase64")
        .and_then(Value::as_str)
        .ok_or(ServiceError::Engine(EngineError::Protocol))?;
    if data_base64.len() > crate::service::MAX_STICKER_IMAGE_BASE64_CHARS {
        return Err(ServiceError::Engine(EngineError::Protocol));
    }
    let content_type = result
        .get("contentType")
        .and_then(Value::as_str)
        .ok_or(ServiceError::Engine(EngineError::Protocol))?
        .to_string();
    let size = result
        .get("size")
        .and_then(Value::as_u64)
        .ok_or(ServiceError::Engine(EngineError::Protocol))?;
    let mut decoded = Vec::new();
    let decodes = crate::service::base64_decode_to_vec(data_base64, &mut decoded);
    if size > u32::MAX as u64 || decodes.is_err() || decoded.len() != size as usize {
        return Err(ServiceError::Engine(EngineError::Protocol));
    }
    Ok(StickerPackImage {
        data_base64: data_base64.to_string(),
        content_type,
        size: size as u32,
    })
}

/// Project the engine's `getAvatar` result (contract 1.48, §4.47): the data
/// URI passes through with the result-face bound — the engine caps a
/// download at [`MAX_AVATAR_DATA_URI_CHARS`] chars, the padded encoding of
/// the engine's 10 MiB decoded ceiling plus the URI prefix margin — and
/// `byteSize` must equal the decoded byte count, or the mismatching result
/// is a protocol error instead of shipping a truncated image. The decoded
/// bytes are never logged and never persisted.
fn avatar_from_engine(result: &Value) -> Result<AvatarDownload, ServiceError> {
    let data_uri = result
        .get("dataUri")
        .and_then(Value::as_str)
        .ok_or(ServiceError::Engine(EngineError::Protocol))?;
    if data_uri.len() > MAX_AVATAR_DATA_URI_CHARS {
        return Err(ServiceError::Engine(EngineError::Protocol));
    }
    let byte_size = result
        .get("size")
        .and_then(Value::as_u64)
        .ok_or(ServiceError::Engine(EngineError::Protocol))?;
    let payload = data_uri
        .split_once(";base64,")
        .map(|(_, payload)| payload)
        .ok_or(ServiceError::Engine(EngineError::Protocol))?;
    let mut decoded = Vec::new();
    let decodes = crate::service::base64_decode_to_vec(payload, &mut decoded);
    if decodes.is_err() || decoded.len() as u64 != byte_size {
        return Err(ServiceError::Engine(EngineError::Protocol));
    }
    Ok(AvatarDownload {
        data_uri: data_uri.to_string(),
        byte_size,
    })
}

/// Test-only re-export so the service-layer projection test can pin the
/// avatar result face (the same pattern as the engine error allowlist).
#[cfg(test)]
pub(crate) fn avatar_from_engine_for_tests(result: &Value) -> Result<AvatarDownload, ServiceError> {
    avatar_from_engine(result)
}

/// The `sendStory` engine answer projected (contract revision 1.76, §4.67):
/// the story `timestamp` plus the per-recipient `results` list, projected
/// entry for entry. The engine face is pinned — a missing or wrong-typed
/// required key is a protocol defect, answered loudly rather than smoothed
/// into a row-less send truth the desktop cannot reconcile; an entry count
/// past the request cap is the same drift (a silently dropped entry would
/// misreport one recipient's send state).
fn story_from_engine(result: &Value) -> Result<StorySendResult, ServiceError> {
    let timestamp = result
        .get("timestamp")
        .and_then(Value::as_u64)
        .ok_or(ServiceError::Engine(EngineError::Protocol))?;
    let entries = result
        .get("results")
        .and_then(Value::as_array)
        .ok_or(ServiceError::Engine(EngineError::Protocol))?;
    if entries.len() > MAX_STORY_RECIPIENTS {
        return Err(ServiceError::Engine(EngineError::Protocol));
    }
    let mut results = Vec::with_capacity(entries.len());
    for entry in entries {
        results.push(StorySendRecipientResult {
            recipient_number: entry
                .get("recipientNumber")
                .and_then(Value::as_str)
                .ok_or(ServiceError::Engine(EngineError::Protocol))?
                .to_string(),
            success: StorySendSuccess {
                timestamp: entry
                    .get("success")
                    .and_then(|success| success.get("timestamp"))
                    .and_then(Value::as_u64)
                    .ok_or(ServiceError::Engine(EngineError::Protocol))?,
            },
        });
    }
    Ok(StorySendResult { timestamp, results })
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

    use super::{
        RuntimeSupervisor, attachment_download_client, cloud_archive_face,
        cloud_status_from_engine, compose_contact_display_name, compose_import_progress,
        group_extra_json, is_receive_fatal_stderr, upload_status_from_engine,
    };
    use crate::DEFAULT_PROXY_GROUP_ID;
    use crate::engine::{NormalizedReceive, SignalCliConfig};
    use crate::service::HostSideEvent;
    use crate::store::{MessageRecord, Store, StoreKey};
    use crate::supervisor::EngineError;

    /// Contract 1.53: the running-state progress composition — download
    /// wins, else THIS account's active import pass renders stage
    /// processing, else the pre-1.53 shape.
    #[test]
    fn import_progress_composition_prefers_download_then_processing() {
        let downloading = json!({"stage": "downloading", "bytesTransferred": 1, "bytesTotal": 2});
        assert_eq!(
            compose_import_progress(Some(downloading.clone()), true),
            Some(downloading.clone())
        );
        assert_eq!(
            compose_import_progress(Some(downloading), false),
            Some(json!({"stage": "downloading", "bytesTransferred": 1, "bytesTotal": 2}))
        );
        assert_eq!(
            compose_import_progress(None, true),
            Some(json!({ "stage": "processing" }))
        );
        // Neither signal (including: the group slot is busy with another
        // account's pass — the boolean is already account-attributed by the
        // caller): the honest pre-1.53 shape, no progress invented.
        assert_eq!(compose_import_progress(None, false), None);
    }

    /// §4.69 cloudStatus shape gate (contract 1.83): well-formed snapshots
    /// mirror key-for-key with an absent `bytesTotal` staying an honest
    /// absence, and every malformed present-face fails UPSTREAM_PROTOCOL_ERROR
    /// rather than degrading to `present:false` (a read that delivered junk
    /// knows nothing — claiming absence would be the fake-0 lie in state form).
    #[test]
    fn cloud_status_projection_gates_the_engine_shape() {
        let absent = cloud_status_from_engine(&json!({ "present": false })).unwrap();
        assert_eq!(absent, json!({ "present": false }));

        let downloading = cloud_status_from_engine(&json!({
            "present": true,
            "phase": "downloading",
            "bytesRead": 40,
            "bytesTotal": 100
        }))
        .unwrap();
        assert_eq!(
            downloading,
            json!({ "present": true, "phase": "downloading", "bytesRead": 40, "bytesTotal": 100 })
        );

        // The honest-uncertainty form: no total is no key, never a zero.
        let totalless = cloud_status_from_engine(&json!({
            "present": true,
            "phase": "downloading",
            "bytesRead": 40
        }))
        .unwrap();
        assert_eq!(
            totalless,
            json!({ "present": true, "phase": "downloading", "bytesRead": 40 })
        );
        assert!(totalless.get("bytesTotal").is_none());

        let failed = cloud_status_from_engine(&json!({
            "present": true,
            "phase": "failed",
            "bytesRead": 40,
            "errorCode": "BACKUP_DOWNLOAD_FAILED"
        }))
        .unwrap();
        assert_eq!(failed["phase"], "failed");
        assert_eq!(failed["errorCode"], "BACKUP_DOWNLOAD_FAILED");

        for junk in [
            json!({}),
            json!({ "present": "yes" }),
            json!({ "present": true }),
            json!({ "present": true, "phase": "downloading" }),
            json!({ "present": true, "phase": "teleporting", "bytesRead": 1 }),
            json!({ "present": true, "phase": "downloading", "bytesRead": "40" }),
            json!({
                "present": true,
                "phase": "downloading",
                "bytesRead": 40,
                "bytesTotal": "100"
            }),
            json!({
                "present": true,
                "phase": "failed",
                "bytesRead": 40,
                "errorCode": 7
            }),
        ] {
            let error = cloud_status_from_engine(&junk)
                .expect_err("malformed snapshots must fail the gate")
                .into_api();
            assert_eq!(error.code, "UPSTREAM_PROTOCOL_ERROR", "{junk}");
            assert!(error.retryable, "{junk}");
        }
    }

    /// §4.72 uploadStatus shape gate (contract 1.87, engine
    /// cloud-backup-contract §6.2): well-formed snapshots mirror key-for-key
    /// across the three-phase set (exporting with its zero-count face,
    /// uploading with and without a total — an absent `bytesTotal` staying
    /// an honest absence —, failed with the structured errorCode), and
    /// every malformed present-face fails UPSTREAM_PROTOCOL_ERROR rather
    /// than degrading to `present:false` (a read that delivered junk knows
    /// nothing — claiming absence would be the fake-0 lie in state form).
    #[test]
    fn upload_status_projection_gates_the_engine_shape() {
        let absent = upload_status_from_engine(&json!({ "present": false })).unwrap();
        assert_eq!(absent, json!({ "present": false }));

        let exporting = upload_status_from_engine(&json!({
            "present": true,
            "phase": "exporting",
            "bytesRead": 0
        }))
        .unwrap();
        assert_eq!(
            exporting,
            json!({ "present": true, "phase": "exporting", "bytesRead": 0 })
        );
        assert!(
            exporting.get("bytesTotal").is_none(),
            "the assembly stage carries no total: {exporting}"
        );

        let uploading = upload_status_from_engine(&json!({
            "present": true,
            "phase": "uploading",
            "bytesRead": 131072,
            "bytesTotal": 1048576
        }))
        .unwrap();
        assert_eq!(
            uploading,
            json!({
                "present": true,
                "phase": "uploading",
                "bytesRead": 131072,
                "bytesTotal": 1048576
            })
        );

        // The honest-uncertainty form: no total is no key, never a zero.
        let totalless = upload_status_from_engine(&json!({
            "present": true,
            "phase": "uploading",
            "bytesRead": 131072
        }))
        .unwrap();
        assert_eq!(
            totalless,
            json!({ "present": true, "phase": "uploading", "bytesRead": 131072 })
        );
        assert!(totalless.get("bytesTotal").is_none());

        let failed = upload_status_from_engine(&json!({
            "present": true,
            "phase": "failed",
            "bytesRead": 131072,
            "errorCode": "BACKUP_UPLOAD_FAILED"
        }))
        .unwrap();
        assert_eq!(failed["phase"], "failed");
        assert_eq!(failed["errorCode"], "BACKUP_UPLOAD_FAILED");

        for junk in [
            json!({}),
            json!({ "present": "yes" }),
            json!({ "present": true }),
            json!({ "present": true, "phase": "uploading" }),
            // The restore phase enum does not pass the upload gate (and
            // vice versa): the two faces keep their own phase sets.
            json!({ "present": true, "phase": "downloading", "bytesRead": 1 }),
            json!({ "present": true, "phase": "teleporting", "bytesRead": 1 }),
            json!({ "present": true, "phase": "uploading", "bytesRead": "40" }),
            json!({
                "present": true,
                "phase": "uploading",
                "bytesRead": 40,
                "bytesTotal": "100"
            }),
            json!({
                "present": true,
                "phase": "failed",
                "bytesRead": 40,
                "errorCode": 7
            }),
        ] {
            let error = upload_status_from_engine(&junk)
                .expect_err("malformed snapshots must fail the gate")
                .into_api();
            assert_eq!(error.code, "UPSTREAM_PROTOCOL_ERROR", "{junk}");
            assert!(error.retryable, "{junk}");
        }
    }

    /// The cloud-archive freshness face (contract revision 1.84): absent is
    /// `None`, present is the `(mtime, len)` identity, and a rewrite with
    /// different bytes presents a different face — the watcher's stale-file
    /// guard.
    #[test]
    fn cloud_archive_face_tracks_presence_and_rewrite() {
        let temp = TempDir::new().unwrap();
        let archive = temp.path().join("history-import-cloud.ndjson");
        assert_eq!(cloud_archive_face(&archive), None, "absent is None");

        std::fs::write(&archive, b"first").unwrap();
        let first = cloud_archive_face(&archive).expect("present file has a face");
        assert_eq!(first.1, 5, "the face carries the byte length");
        assert_eq!(
            cloud_archive_face(&archive),
            Some(first),
            "stable when untouched"
        );

        // A fresh engine write (different bytes) must present a different
        // face — this is the only definite success signal, because the
        // engine's snapshot clears on success and `{present: false}` is the
        // same answer a never-started or restarted engine gives.
        std::fs::write(&archive, b"second write").unwrap();
        let second = cloud_archive_face(&archive).expect("rewritten file has a face");
        assert_ne!(first, second, "a rewrite changes the face");
    }

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

    /// Contract 1.65 (§4.58): the factor stamp serializes exactly the flags
    /// the engine item exposed — upstream `JsonContact` names minus the `is`
    /// prefix — and an item without any of the three stamps the honest
    /// probed-absent `{}` so the probe never re-fires for the row.
    #[test]
    fn mrr_factor_stamp_carries_exactly_the_exposed_flags() {
        use super::{contact_item_peer_key, mrr_factors_stamp};
        let signal_cli = json!({
            "number": "+15555550101",
            "isBlocked": false,
            "isHidden": true,
            "profileSharing": false
        });
        assert_eq!(
            mrr_factors_stamp(&signal_cli),
            r#"{"blocked":false,"hidden":true,"profileSharing":false}"#
        );
        assert_eq!(contact_item_peer_key(&signal_cli), Some("+15555550101"));

        let kt_engine = json!({ "number": "+15555550102", "name": "Peer" });
        assert_eq!(mrr_factors_stamp(&kt_engine), "{}");
        assert_eq!(contact_item_peer_key(&kt_engine), Some("+15555550102"));

        // The address ladder: number wins before uuid, the numberUuid tail
        // included; an identity-less item contributes nothing.
        let uuid_only = json!({
            "uuid": "abcd1234-0000-0000-0000-000000000000",
            "numberUuid": "abcd1234-0000-0000-0000-000000000001"
        });
        assert_eq!(
            contact_item_peer_key(&uuid_only),
            Some("abcd1234-0000-0000-0000-000000000000")
        );
        assert_eq!(contact_item_peer_key(&json!({"name": "x"})), None);
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
            sender_name: None,
            mentions_self: false,
            sent_at: expired_at,
            received_at: Some(expired_at),
            text: Some("body".into()),
            text_bytes: Some(4),
            text_truncated: false,
            text_retrievable: true,
            status: "delivered",
            client_request_id: None,
            quote_message_id: None,
            quote_snapshot: None,
            attachments: Vec::new(),
            rich: None,
            edited_at: None,
            reactions: Vec::new(),
            edits: Vec::new(),
            delivered_at: None,
            read_at: None,
            viewed_at: None,
            admin_deleted: false,
            sticker: None,
            expire_seconds: None,
            expire_start_at: None,
            server_guid: None,
            server_timestamp: None,
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
            None,
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
            None,
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
                source_uuid: None,
                peer_name: Some("Peer".into()),
                group_id: None,
                text: Some("persist me".into()),
                text_bytes: Some(10),
                text_truncated: false,
                quote: None,
                attachments: Vec::new(),
                rich: None,
                sticker: None,
                expire_seconds: None,
                expire_start_at: None,
                control: None,
                server_guid: None,
                server_timestamp: None,
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

    /// The §4.53 requesting-members identity read: both identities of a
    /// candidate (number and uuid) land in the set, an address-less or
    /// malformed entry drops, a missing/foreign listGroups answer reads as
    /// empty (the fold's fail-closed direction), and the fold keeps only
    /// candidates the engine still reports requesting, in caller order.
    #[test]
    fn join_request_fold_keeps_only_requesting_candidates() {
        use super::{fold_join_request_candidates, requesting_member_identities};

        let listed = json!([
            {
                "id": "ZmFrZS1ncm91cC0x",
                "name": "Fixture Group",
                "isMember": true,
                "requestingMembers": [
                    {"number": "+15555550101"},
                    {
                        "number": "+15555550102",
                        "uuid": "0b7fca57-1234-4d0e-9b0f-4f6c1f8a2e10"
                    },
                    {"uuid": "0b7fca57-abcd-4d0e-9b0f-4f6c1f8a2e10"},
                    {},
                    {"number": 12345},
                    "not-an-object"
                ]
            }
        ]);
        let requesting = requesting_member_identities(&listed);
        assert!(requesting.contains("+15555550101"));
        assert!(requesting.contains("+15555550102"));
        assert!(requesting.contains("0b7fca57-1234-4d0e-9b0f-4f6c1f8a2e10"));
        assert!(requesting.contains("0b7fca57-abcd-4d0e-9b0f-4f6c1f8a2e10"));
        assert_eq!(requesting.len(), 4, "address-less/malformed entries drop");

        // The fold: requesting candidates survive in caller order; a stale
        // or mistaken candidate (an existing member, a stranger) drops.
        let folded = fold_join_request_candidates(
            vec![
                "+15555550102".into(),
                "+15555550999".into(),
                "+15555550101".into(),
            ],
            &requesting,
        );
        assert_eq!(
            folded,
            vec!["+15555550102".to_string(), "+15555550101".to_string()]
        );

        // The fail-closed direction: an engine answer without the face (or
        // a foreign shape) reads as empty and the fold empties — nothing
        // rides the member arrays.
        assert!(requesting_member_identities(&json!([])).is_empty());
        assert!(requesting_member_identities(&json!({"error": 1})).is_empty());
        assert!(requesting_member_identities(&json!([{"members": []}])).is_empty());
        assert!(
            fold_join_request_candidates(
                vec!["+15555550101".into()],
                &requesting_member_identities(&json!([]))
            )
            .is_empty()
        );
    }

    /// The §4.57 pending-invitees identity read: both identities of an
    /// invitee (number and uuid) land in the set, an address-less or
    /// malformed entry drops, a missing/foreign listGroups answer reads as
    /// empty (the fold's fail-closed direction), and the fold keeps only
    /// candidates the engine still reports pending, in caller order.
    #[test]
    fn invite_fold_keeps_only_pending_candidates() {
        use super::{fold_invite_candidates, pending_member_identities};

        let listed = json!([
            {
                "id": "ZmFrZS1ncm91cC0x",
                "name": "Fixture Group",
                "isMember": true,
                "pendingMembers": [
                    {"number": "+15555550101"},
                    {
                        "number": "+15555550102",
                        "uuid": "0b7fca57-1234-4d0e-9b0f-4f6c1f8a2e10"
                    },
                    {"uuid": "0b7fca57-abcd-4d0e-9b0f-4f6c1f8a2e10"},
                    {},
                    {"number": 12345},
                    "not-an-object"
                ]
            }
        ]);
        let pending = pending_member_identities(&listed);
        assert!(pending.contains("+15555550101"));
        assert!(pending.contains("+15555550102"));
        assert!(pending.contains("0b7fca57-1234-4d0e-9b0f-4f6c1f8a2e10"));
        assert!(pending.contains("0b7fca57-abcd-4d0e-9b0f-4f6c1f8a2e10"));
        assert_eq!(pending.len(), 4, "address-less/malformed entries drop");

        // The fold: pending invitees survive in caller order; a stale or
        // mistaken candidate (an existing member, a stranger) drops — never
        // rides the removeMembers array.
        let folded = fold_invite_candidates(
            vec![
                "+15555550102".into(),
                "+15555550999".into(),
                "+15555550101".into(),
            ],
            &pending,
        );
        assert_eq!(
            folded,
            vec!["+15555550102".to_string(), "+15555550101".to_string()]
        );

        // The fail-closed direction: an engine answer without the face (or
        // a foreign shape) reads as empty and the fold empties — nothing
        // rides the removeMembers array.
        assert!(pending_member_identities(&json!([])).is_empty());
        assert!(pending_member_identities(&json!({"error": 1})).is_empty());
        assert!(pending_member_identities(&json!([{"members": []}])).is_empty());
        assert!(
            fold_invite_candidates(
                vec!["+15555550101".into()],
                &pending_member_identities(&json!([]))
            )
            .is_empty()
        );
    }

    /// The §4.39 roster capture (contract revision 1.40): the historical
    /// signal-cli member/admin shapes mark admins through both channels, the
    /// kt-signal-engine shape marks none, address-less entries drop
    /// individually, and the roster cap holds while memberCount keeps the
    /// upstream length.
    #[test]
    fn group_extra_json_captures_the_bounded_roster() {
        use super::group_extra_json;
        // Pinned 0.14.8 shape: `members` carry {number, uuid, isAdmin}, the
        // group carries a separate `admins` address set. Alice is marked by
        // her per-member flag, Bob only through the admins set (his uuid),
        // and the address-less entry drops without failing the batch.
        let group = json!({
            "id": "ZmFrZS1ncm91cC0x",
            "name": "Fixture Group",
            "isMember": true,
            "admins": [{"uuid": "0b7fca57-6c3d-4a9e-b3e8-2f14c5d6a7b9"}],
        });
        let members = json!([
            {"number": "+15555550100"},
            {"number": "+15555550101", "uuid": "0b7fca57-1234-4d0e-9b0f-4f6c1f8a2e10", "isAdmin": true},
            {"uuid": "0b7fca57-6c3d-4a9e-b3e8-2f14c5d6a7b9"},
            {},
            "not-an-object",
        ]);
        let extra: serde_json::Value =
            serde_json::from_str(&group_extra_json(members.as_array().unwrap(), &group)).unwrap();
        assert_eq!(extra["memberCount"], 5);
        let roster = extra["members"].as_array().unwrap();
        assert_eq!(
            roster,
            &[
                json!({"id": "+15555550100"}),
                json!({
                    "id": "+15555550101",
                    "uuid": "0b7fca57-1234-4d0e-9b0f-4f6c1f8a2e10",
                    "admin": true,
                }),
                json!({"id": "0b7fca57-6c3d-4a9e-b3e8-2f14c5d6a7b9", "admin": true}),
            ]
        );

        // Engine shape (kt-signal-engine listGroups): bare number
        // entries, no uuid key, no admin face — the projection difference
        // §4.39 records.
        let engine_group = json!({"id": "aWQ", "name": "g", "isMember": true});
        let engine_members = json!([{"number": "+15555550100"}, {"number": "+15555550101"}]);
        let extra: serde_json::Value = serde_json::from_str(&group_extra_json(
            engine_members.as_array().unwrap(),
            &engine_group,
        ))
        .unwrap();
        assert_eq!(
            extra["members"],
            json!([{"id": "+15555550100"}, {"id": "+15555550101"}])
        );

        // The roster cap is the official group-size ceiling; memberCount
        // keeps the upstream length even when the roster truncates.
        let flood: Vec<serde_json::Value> = (0..crate::service::MAX_GROUP_MEMBERS + 10)
            .map(|index| json!({"number": format!("+1555555{index:04}")}))
            .collect();
        let extra: serde_json::Value = serde_json::from_str(&group_extra_json(
            &flood,
            &json!({"id": "aWQ", "isMember": true}),
        ))
        .unwrap();
        assert_eq!(extra["memberCount"], crate::service::MAX_GROUP_MEMBERS + 10);
        assert_eq!(
            extra["members"].as_array().unwrap().len(),
            crate::service::MAX_GROUP_MEMBERS
        );

        // Contract 1.53 (§4.50 appendix): the upstream isTerminated flag is
        // captured only when true — false and absent collapse to absent (the
        // informative-keys discipline), engines without the flag never set
        // it.
        let plain = serde_json::from_str::<serde_json::Value>(&group_extra_json(
            members.as_array().unwrap(),
            &json!({"id": "aWQ", "isMember": true, "isTerminated": false}),
        ))
        .unwrap();
        assert!(plain.get("isTerminated").is_none());
        let terminated = serde_json::from_str::<serde_json::Value>(&group_extra_json(
            members.as_array().unwrap(),
            &json!({"id": "aWQ", "isMember": true, "isTerminated": true}),
        ))
        .unwrap();
        assert_eq!(terminated["isTerminated"], serde_json::json!(true));
    }

    #[test]
    fn group_extra_json_captures_member_labels_within_server_budgets() {
        // Contract 1.67 (§4.60): the pinned listGroups member label pair is
        // captured verbatim; label-less members, engines without the face,
        // and over-budget values (dropped, never truncated) follow the
        // informative-keys discipline.
        let group = json!({"id": "aWQ", "isMember": true});
        let members = json!([
            {
                "number": "+15555550100",
                "labelEmoji": "🛠️",
                "label": "Builders",
            },
            {"number": "+15555550101", "label": "  Design  "},
            {"number": "+15555550102", "labelEmoji": "", "label": "   "},
            {"number": "+15555550103", "label": "x".repeat(512)},
            {"number": "+15555550104", "label": "x".repeat(513)},
            {"number": "+15555550105", "labelEmoji": "x".repeat(64)},
            {"number": "+15555550106", "labelEmoji": "x".repeat(65)},
        ]);
        let extra: serde_json::Value =
            serde_json::from_str(&group_extra_json(members.as_array().unwrap(), &group)).unwrap();
        let roster = extra["members"].as_array().unwrap();
        assert_eq!(
            roster[0],
            json!({"id": "+15555550100", "labelEmoji": "🛠️", "label": "Builders"})
        );
        // Trimmed but otherwise verbatim.
        assert_eq!(roster[1]["label"], json!("Design"));
        assert!(roster[1].get("labelEmoji").is_none());
        // Empty/whitespace-only values drop (the upstream emits NON_NULL but
        // an empty string is not a label).
        assert_eq!(roster[2], json!({"id": "+15555550102"}));
        // Exactly at the 512-byte server string budget passes whole.
        assert_eq!(roster[3]["label"], json!("x".repeat(512)));
        // One byte over drops the key instead of truncating.
        assert_eq!(roster[4], json!({"id": "+15555550104"}));
        // The 64-byte emoji budget: a 64-byte value passes whole, 65 drops.
        // The budget is bytes, not graphemes.
        assert_eq!(roster[5]["labelEmoji"], json!("x".repeat(64)));
        assert_eq!(roster[6], json!({"id": "+15555550106"}));
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
            store.clone(),
            DEFAULT_PROXY_GROUP_ID.to_string(),
            None,
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
                None,
                None,
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
            None,
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
                None,
                None,
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

    /// 上行确认路径（fixture 正常应答，targetTimestamp≠423/351）：确认后
    /// complete 落库 own 反应（contract 1.27 actor 口径：account 本体、无名），
    /// 并向 host lane 推 conversation.changed——app 的乐观药丸由此获得权威
    /// 状态兜底，不再随窗口过期消失。
    #[tokio::test]
    async fn send_reaction_confirmed_persists_own_pill_and_pushes_conversation_changed() {
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
            None,
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
                "react confirmed",
                "req-sr-ok",
                None,
                None,
                None,
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
            .complete_send_success(&pending_id, &account.id, &conversation.id, 424)
            .unwrap();
        let mut host_lane = supervisor.subscribe_host();

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
            "sent"
        );

        let row = supervisor
            .service
            .lock()
            .await
            .store_ref()
            .message_by_id(&account.id, &conversation.id, &pending_id)
            .unwrap()
            .unwrap();
        assert_eq!(row.reactions.len(), 1);
        assert_eq!(row.reactions[0].emoji, "👍");
        assert!(row.reactions[0].mine);
        assert!(row.reactions[0].actors.iter().all(|actor| actor.is_self));
        let event = tokio::time::timeout(Duration::from_secs(2), host_lane.recv())
            .await
            .expect("conversation.changed within timeout")
            .unwrap();
        assert!(matches!(event, HostSideEvent::ConversationChanged(_)));
        supervisor.shutdown().await.unwrap();
    }

    /// The fixture's `sendPinMessage` handler exits with the mutating call in
    /// flight (target timestamp 424), so the upstream result is lost mid-call.
    /// sendPinMessage must answer the explicit `unknown` — never a retryable
    /// error, never an automatic retry — and write no pinned state locally
    /// (the reaction precedent, contract revision 1.33).
    #[tokio::test]
    async fn send_pin_message_with_a_lost_upstream_result_answers_unknown() {
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
            None,
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
                "pin me",
                "req-pin-crash",
                None,
                None,
                None,
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
            .complete_send_success(&pending_id, &account.id, &conversation.id, 424)
            .unwrap();

        assert_eq!(
            supervisor
                .send_pin_message(
                    account.id.clone(),
                    conversation.id.clone(),
                    pending_id.clone(),
                    Some(3600)
                )
                .await
                .unwrap(),
            "unknown",
            "a lost mutating result must answer unknown, never retry"
        );

        // No pinned state was written: the local conversation stays pin-free.
        assert!(
            supervisor
                .service
                .lock()
                .await
                .store_ref()
                .conversation_summary(&account.id, &conversation.id)
                .unwrap()
                .unwrap()
                .pinned_message
                .is_none()
        );
        supervisor.shutdown().await.unwrap();
    }

    /// 上行确认路径（fixture 正常应答，targetTimestamp≠424/352）：确认后
    /// pinned 状态落库（timed pin 带 expiry）并向 host lane 推
    /// conversation.changed；随后的 unpin 清除状态并再推一次——重载窗口
    /// 由此免重放即可渲染/摘除置顶条（contract revision 1.33）。
    #[tokio::test]
    async fn send_pin_message_confirmed_persists_pin_and_unpin_clears() {
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
            None,
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
                "pin confirmed",
                "req-pin-ok",
                None,
                None,
                None,
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
            .complete_send_success(&pending_id, &account.id, &conversation.id, 426)
            .unwrap();
        let mut host_lane = supervisor.subscribe_host();

        assert_eq!(
            supervisor
                .send_pin_message(
                    account.id.clone(),
                    conversation.id.clone(),
                    pending_id.clone(),
                    Some(3600)
                )
                .await
                .unwrap(),
            "sent"
        );
        let summary = supervisor
            .service
            .lock()
            .await
            .store_ref()
            .conversation_summary(&account.id, &conversation.id)
            .unwrap()
            .unwrap();
        let pinned = summary.pinned_message.expect("the confirmed pin persists");
        assert_eq!(pinned.message_id, pending_id);
        assert_eq!(pinned.target_author, "+15555550100");
        assert_eq!(pinned.target_sent_timestamp, 426);
        assert!(pinned.expires_at.is_some(), "a timed pin carries expiry");
        let event = tokio::time::timeout(Duration::from_secs(2), host_lane.recv())
            .await
            .expect("conversation.changed within timeout")
            .unwrap();
        assert!(matches!(event, HostSideEvent::ConversationChanged(_)));

        assert_eq!(
            supervisor
                .send_unpin_message(account.id.clone(), conversation.id.clone(), pending_id)
                .await
                .unwrap(),
            "sent"
        );
        assert!(
            supervisor
                .service
                .lock()
                .await
                .store_ref()
                .conversation_summary(&account.id, &conversation.id)
                .unwrap()
                .unwrap()
                .pinned_message
                .is_none()
        );
        let event = tokio::time::timeout(Duration::from_secs(2), host_lane.recv())
            .await
            .expect("unpin conversation.changed within timeout")
            .unwrap();
        assert!(matches!(event, HostSideEvent::ConversationChanged(_)));
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
            None,
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
            None,
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

    // ---- Contract 1.42 (§4.41): expire sweep + timer route ----

    fn due_record(
        id: &str,
        account_id: &str,
        conversation_id: &str,
        deadline_ms: u64,
    ) -> MessageRecord {
        MessageRecord {
            id: id.into(),
            account_id: account_id.into(),
            conversation_id: conversation_id.into(),
            direction: "incoming",
            sender_id: "peer".into(),
            sender_name: None,
            mentions_self: false,
            sent_at: deadline_ms,
            received_at: Some(deadline_ms),
            text: Some("vanishing".into()),
            text_bytes: Some(9),
            text_truncated: false,
            text_retrievable: true,
            status: "delivered",
            client_request_id: None,
            quote_message_id: None,
            quote_snapshot: None,
            attachments: Vec::new(),
            rich: None,
            edited_at: None,
            reactions: Vec::new(),
            edits: Vec::new(),
            delivered_at: None,
            read_at: None,
            viewed_at: None,
            admin_deleted: false,
            sticker: None,
            expire_seconds: Some(60),
            expire_start_at: Some(deadline_ms.saturating_sub(60_000)),
            server_guid: None,
            server_timestamp: None,
        }
    }

    /// One drain pass deletes a full batch's worth of due rows and keeps
    /// draining immediately when the batch came back full (the cap bounds
    /// rows, not conversations), emitting one batched `message.expired` event
    /// per touched conversation; the follow-up pass is quiet.
    #[tokio::test]
    async fn expire_sweep_drains_full_batches_and_emits_grouped_events() {
        let temp = TempDir::new().unwrap();
        let store = test_store(temp.path());
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let conversation_a = store
            .ensure_conversation(&account.id, "direct", "+15555550101", "contact")
            .unwrap();
        let conversation_b = store
            .ensure_conversation(&account.id, "direct", "+15555550102", "contact")
            .unwrap();
        // One full batch plus one, interleaved across two conversations: the
        // first pass deletes exactly the cap, the second drains the rest, and
        // both conversations must have been named by some event.
        let total = super::EXPIRE_SWEEP_BATCH as usize + 1;
        for index in 0..total {
            let conversation = if index % 2 == 0 {
                &conversation_a
            } else {
                &conversation_b
            };
            let record = due_record(
                &format!("due-{index:04}"),
                &account.id,
                &conversation.id,
                10_000 + index as u64,
            );
            store
                .insert_message(&record, None, Some("vanishing"), true)
                .unwrap();
        }
        // A live row the timer has not touched yet survives the whole drain:
        // its countdown starts far beyond the real wall clock.
        let mut waiting = due_record("waiting", &account.id, &conversation_a.id, 1_000_000);
        waiting.expire_start_at = Some(crate::link::now_ms() + 100_000);
        store
            .insert_message(&waiting, None, Some("vanishing"), true)
            .unwrap();

        let supervisor = Arc::new(RuntimeSupervisor::new(
            SignalCliConfig::new(
                temp.path().join("unused-signal-cli"),
                temp.path().join("unused-signal-data"),
            ),
            Arc::new(store),
            DEFAULT_PROXY_GROUP_ID.to_string(),
            None,
        ));
        let mut host_events = supervisor.subscribe_host();
        supervisor.run_expire_sweep_pass().await;

        let mut expired_ids = std::collections::HashSet::new();
        let mut conversations_seen = std::collections::HashSet::new();
        while expired_ids.len() < total {
            let event = timeout(Duration::from_secs(5), host_events.recv())
                .await
                .expect("expired events within timeout")
                .unwrap();
            let HostSideEvent::MessageExpired {
                account_id,
                conversation_id,
                message_ids,
            } = event
            else {
                continue;
            };
            assert_eq!(account_id, account.id);
            assert!(!message_ids.is_empty(), "a group never ships empty");
            conversations_seen.insert(conversation_id);
            expired_ids.extend(message_ids);
        }
        assert_eq!(expired_ids.len(), total, "every due row swept exactly once");
        assert_eq!(conversations_seen.len(), 2, "both conversations reported");

        // The follow-up pass is quiet: the sweeper's event lane stays silent.
        supervisor.run_expire_sweep_pass().await;
        sleep(Duration::from_millis(150)).await;
        assert!(host_events.try_recv().is_err(), "no second-wave events");
        supervisor.shutdown().await.unwrap();
    }

    /// The direct route dispatches the engine's mutating
    /// `setExpirationTimer` (`expirationInSeconds`, single-string
    /// recipient), mirrors the conversation state on the definite success,
    /// and rides the refreshed summary out on conversation.changed.
    #[tokio::test]
    async fn set_expire_timer_routes_set_expiration_timer_and_mirrors_state() {
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
            None,
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
        let mut host_lane = supervisor.subscribe_host();

        let result = supervisor
            .set_conversation_expire_timer(crate::service::ConversationsSetExpireTimerParams {
                account_id: account.id.clone(),
                conversation_id: conversation.id.clone(),
                expire_seconds: 604_800,
            })
            .await
            .unwrap();
        assert_eq!(result.expire_timer_seconds, 604_800);

        let send_log = std::fs::read_to_string(
            temp.path()
                .join("signal-data")
                .join(".fixture-send-log.jsonl"),
        )
        .expect("fixture must log the dispatch");
        let dispatch: serde_json::Value = send_log
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .find(|params: &serde_json::Value| params["expirationInSeconds"] == 604_800)
            .expect("the timer call must reach setExpirationTimer");
        assert_eq!(dispatch["account"], "+15555550100");
        assert_eq!(dispatch["recipient"], "+15555550101");
        let summary = supervisor
            .service
            .lock()
            .await
            .store_ref()
            .conversation_summary(&account.id, &conversation.id)
            .unwrap()
            .unwrap();
        assert_eq!(summary.expire_timer_seconds, Some(604_800));
        let event = timeout(Duration::from_secs(2), host_lane.recv())
            .await
            .expect("conversation.changed within timeout")
            .unwrap();
        let HostSideEvent::ConversationChanged(changed) = event else {
            panic!("expected a conversation.changed event");
        };
        assert_eq!(changed.expire_timer_seconds, Some(604_800));
        supervisor.shutdown().await.unwrap();
    }

    /// Contract 1.74 (§4.65): the kt-engine username read faces route to the
    /// engine's `getUsername`/`usernameLookup` method names — getUsername
    /// projects the storage truth (set shape: both fields; the unset marker:
    /// both keys absent project the null face), usernameLookup shares the
    /// §4.55 array projection (no `number` key on the engine record). The
    /// dispatch log pins the engine method name and the resolved account.
    #[tokio::test]
    async fn username_read_faces_route_engine_methods() {
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
            None,
        ));
        supervisor.start().await.unwrap();
        let account = supervisor
            .service
            .lock()
            .await
            .sync_accounts_from_numbers(&["+15555550100".into()], DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);

        // getUsername: the set shape projects both fields.
        let read = supervisor
            .get_username(crate::service::AccountsGetUsernameParams {
                account_id: account.id.clone(),
            })
            .await
            .unwrap();
        assert_eq!(read.username.as_deref(), Some("fixture_user.01"));
        assert_eq!(
            read.username_link.as_deref(),
            Some("https://signal.me/username/fixture-link")
        );

        // The never-set shape (engine answers both keys absent): the null
        // face, not an error.
        let unset_marker = temp
            .path()
            .join("signal-data")
            .join(".fixture-username-unset");
        std::fs::write(&unset_marker, b"").unwrap();
        let unset = supervisor
            .get_username(crate::service::AccountsGetUsernameParams {
                account_id: account.id.clone(),
            })
            .await
            .unwrap();
        assert_eq!(unset.username, None);
        assert_eq!(unset.username_link, None);
        std::fs::remove_file(&unset_marker).unwrap();

        // usernameLookup: the engine array projects entry-for-entry.
        let lookup = supervisor
            .lookup_usernames(crate::service::UsernameLookupParams {
                account_id: account.id.clone(),
                usernames: vec!["kt_registered.01".into(), "kt_unregistered.02".into()],
            })
            .await
            .unwrap();
        assert_eq!(lookup.results.len(), 2);
        assert_eq!(lookup.results[0].recipient, "kt_registered.01");
        assert_eq!(
            lookup.results[0].username.as_deref(),
            Some("kt_registered.01")
        );
        assert!(lookup.results[0].uuid.is_some());
        assert!(
            lookup.results[0].number.is_none(),
            "engine records carry no number key"
        );
        assert!(lookup.results[0].is_registered);
        assert_eq!(lookup.results[1].recipient, "kt_unregistered.02");
        assert!(!lookup.results[1].is_registered);

        // The dispatch log carries the engine method names and the resolved
        // signal account (the raw account id never rides the frame).
        let send_log = std::fs::read_to_string(
            temp.path()
                .join("signal-data")
                .join(".fixture-send-log.jsonl"),
        )
        .expect("fixture must log the dispatches");
        let dispatches: Vec<serde_json::Value> = send_log
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        let get_dispatch = dispatches
            .iter()
            .find(|params| params.get("usernames").is_none() && params.get("account").is_some())
            .expect("a getUsername dispatch must land");
        assert_eq!(get_dispatch["account"], "+15555550100");
        let lookup_dispatch = dispatches
            .iter()
            .find(|params| params.get("usernames").is_some())
            .expect("a usernameLookup dispatch must land");
        assert_eq!(lookup_dispatch["account"], "+15555550100");
        supervisor.shutdown().await.unwrap();
    }

    /// Contract 1.75 (§4.66): the kt-engine write face orchestrates the two
    /// hops — reserve adopts the candidate (a bare nickname's discriminator
    /// is the engine's pick), confirm lands the link; the delete hop rides
    /// usernameDelete. The structured server codes map through the allowlist
    /// (TAKEN/RESERVATION_GONE deterministic, RATE_LIMITED retryable); the
    /// torn two-hop (confirm refuses on a gone reservation) surfaces the
    /// code verbatim; a crash with reserve in flight answers
    /// SEND_OUTCOME_UNKNOWN, is never retried (exactly one reserve dispatch,
    /// no confirm hop), and the next call lands on a fresh engine.
    #[tokio::test]
    async fn username_write_face_two_hop_delete_and_structured_codes() {
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
            None,
        ));
        supervisor.start().await.unwrap();
        let account = supervisor
            .service
            .lock()
            .await
            .sync_accounts_from_numbers(&["+15555550100".into()], DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let set_params =
            |username: Option<String>, delete: bool| crate::service::AccountsSetUsernameParams {
                account_id: account.id.clone(),
                username,
                delete_username: delete,
            };

        // Set path: the full username echoes through both hops and the
        // write-back projects {username, usernameLink}.
        let set = supervisor
            .set_username(set_params(Some("kt_test_agent.07".into()), false))
            .await
            .unwrap();
        assert_eq!(set.username.as_deref(), Some("kt_test_agent.07"));
        assert_eq!(
            set.username_link.as_deref(),
            Some("https://signal.me/username/fixture-link")
        );

        // Bare nickname: the adopted candidate wins (the engine picks the
        // discriminator).
        let bare = supervisor
            .set_username(set_params(Some("kt_agent_bare".into()), false))
            .await
            .unwrap();
        assert_eq!(bare.username.as_deref(), Some("kt_agent_bare.07"));

        // Delete hop: usernameDelete → the null face.
        let deleted = supervisor
            .set_username(set_params(None, true))
            .await
            .unwrap();
        assert_eq!(deleted.username, None);
        assert_eq!(deleted.username_link, None);

        // Structured codes through the allowlist.
        let taken = supervisor
            .set_username(set_params(Some("fixture_username_taken.01".into()), false))
            .await
            .unwrap_err();
        // The structured codes surface as engine UpstreamCode literals (the
        // allowlist classification); the host-face mapping (code + retryable)
        // is pinned by the service-layer totality/mapping tests.
        assert!(matches!(
            taken,
            crate::service::ServiceError::Engine(EngineError::UpstreamCode {
                code: "USERNAME_TAKEN",
                ..
            })
        ));
        let rate = supervisor
            .set_username(set_params(
                Some("fixture_username_ratelimited.01".into()),
                false,
            ))
            .await
            .unwrap_err();
        assert!(matches!(
            rate,
            crate::service::ServiceError::Engine(EngineError::UpstreamCode {
                code: "USERNAME_RATE_LIMITED",
                ..
            })
        ));
        // The torn two-hop: reserve succeeds, confirm refuses.
        let gone = supervisor
            .set_username(set_params(Some("fixture_username_gone.01".into()), false))
            .await
            .unwrap_err();
        assert!(matches!(
            gone,
            crate::service::ServiceError::Engine(EngineError::UpstreamCode {
                code: "USERNAME_RESERVATION_GONE",
                ..
            })
        ));

        // Crash with reserve in flight: SEND_OUTCOME_UNKNOWN, never retried;
        // an explicit restart lands a fresh engine and the next set works.
        let crash = supervisor
            .set_username(set_params(Some("fixture_crash_username.31".into()), false))
            .await
            .unwrap_err();
        assert!(matches!(crash, crate::service::ServiceError::Api(ref api)
                if api.code == "SEND_OUTCOME_UNKNOWN" && !api.retryable));
        supervisor.start().await.unwrap();
        let after = supervisor
            .set_username(set_params(Some("kt_after_crash.01".into()), false))
            .await
            .unwrap();
        assert_eq!(after.username.as_deref(), Some("kt_after_crash.01"));

        // The dispatch log pins the hop order and payloads: every set rode
        // reserve→confirm (the crash username has exactly one reserve, no
        // confirm), the delete rode usernameDelete, and the raw account id
        // never rides the frame.
        let send_log = std::fs::read_to_string(
            temp.path()
                .join("signal-data")
                .join(".fixture-send-log.jsonl"),
        )
        .expect("fixture must log the dispatches");
        let dispatches: Vec<serde_json::Value> = send_log
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        let reserves: Vec<&serde_json::Value> = dispatches
            .iter()
            .filter(|params| {
                params.get("usernameHash").is_none() && params.get("username").is_some()
            })
            .collect();
        let confirms: Vec<&serde_json::Value> = dispatches
            .iter()
            .filter(|params| params.get("usernameHash").is_some())
            .collect();
        // Every reserve that succeeded rode a confirm hop; the refusals
        // (taken/rate/crash) legitimately stop at reserve.
        assert_eq!(reserves.len(), confirms.len() + 3);
        for name in [
            "kt_test_agent.07",
            "kt_agent_bare.07",
            "fixture_username_gone.01",
            "kt_after_crash.01",
        ] {
            assert_eq!(
                confirms
                    .iter()
                    .filter(|params| params["username"] == name)
                    .count(),
                1,
                "exactly one confirm hop for {name}"
            );
        }
        let crash_reserves = reserves
            .iter()
            .filter(|params| params["username"] == "fixture_crash_username.31")
            .count();
        let crash_confirms = confirms
            .iter()
            .filter(|params| params["username"] == "fixture_crash_username.31")
            .count();
        assert_eq!(
            crash_reserves, 1,
            "an unknown reserve outcome is never retried"
        );
        assert_eq!(crash_confirms, 0, "a lost reserve never reaches confirm");
        assert!(
            dispatches
                .iter()
                .any(|params| params.get("deleteUsername").is_none()
                    && params.get("username").is_none()
                    && params.get("usernameHash").is_none()
                    && params.get("account").and_then(serde_json::Value::as_str)
                        == Some("+15555550100")),
            "the delete hop rides usernameDelete with the bare account"
        );
        assert!(
            dispatches
                .iter()
                .all(|params| params.get("accountId").is_none())
        );
        supervisor.shutdown().await.unwrap();
    }

    /// The fixture exits with the max-legal timer call in flight, so the
    /// upstream result is lost mid-call: setExpireTimer answers the explicit
    /// SEND_OUTCOME_UNKNOWN — never a retry, and the local mirror stays
    /// untouched (an unconfirmed upstream value is never projected).
    #[tokio::test]
    async fn set_expire_timer_with_a_lost_upstream_result_answers_unknown() {
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
            None,
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

        let outcome = supervisor
            .set_conversation_expire_timer(crate::service::ConversationsSetExpireTimerParams {
                account_id: account.id.clone(),
                conversation_id: conversation.id.clone(),
                expire_seconds: i32::MAX as u32,
            })
            .await
            .unwrap_err();
        assert!(
            matches!(outcome, crate::service::ServiceError::Api(ref api) if api.code == "SEND_OUTCOME_UNKNOWN"),
            "a lost mutating result must answer SEND_OUTCOME_UNKNOWN"
        );

        // No mirror write: the summary's timer state stays unknown.
        let summary = supervisor
            .service
            .lock()
            .await
            .store_ref()
            .conversation_summary(&account.id, &conversation.id)
            .unwrap()
            .unwrap();
        assert_eq!(summary.expire_timer_seconds, None);
        supervisor.shutdown().await.unwrap();
    }

    /// Contract 1.76 (§4.67): the kt-engine story send routes to the engine's
    /// `sendStory` face — the frame carries the resolved signal account and
    /// omits the default-keyed options (distributionListId absent = the
    /// MY_STORY zero-UUID semantic, textStyle DEFAULT = the key absent,
    /// allowsReplies always explicit); the crash sentinel answers
    /// SEND_OUTCOME_UNKNOWN and is never retried (rowless discipline — there
    /// is no pending row, the restart is explicit). The stories-disabled
    /// toggle round-trips through the store from the official default
    /// (enabled).
    #[tokio::test]
    async fn story_faces_route_engine_send_and_local_toggle() {
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
            None,
        ));
        supervisor.start().await.unwrap();
        let account = supervisor
            .service
            .lock()
            .await
            .sync_accounts_from_numbers(&["+15555550100".into()], DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);

        // The toggle round-trips from the official default (enabled).
        let before = supervisor
            .get_stories_disabled(crate::service::StoriesGetDisabledParams {
                account_id: account.id.clone(),
            })
            .await
            .unwrap();
        assert!(!before.disabled);
        let set = supervisor
            .set_stories_disabled(crate::service::StoriesSetDisabledParams {
                account_id: account.id.clone(),
                disabled: true,
            })
            .await
            .unwrap();
        assert!(set.disabled);
        let after = supervisor
            .get_stories_disabled(crate::service::StoriesGetDisabledParams {
                account_id: account.id.clone(),
            })
            .await
            .unwrap();
        assert!(after.disabled);
        supervisor
            .set_stories_disabled(crate::service::StoriesSetDisabledParams {
                account_id: account.id.clone(),
                disabled: false,
            })
            .await
            .unwrap();

        let story_params = |message: &str, full: bool| crate::service::MessagesSendStoryParams {
            account_id: account.id.clone(),
            message: message.to_string(),
            recipients: vec!["+15555550101".into()],
            distribution_list_id: full.then(|| "11111111-2222-3333-4444-555555555555".into()),
            allows_replies: full,
            text_style: full.then(|| "BOLD".to_string()),
            body_ranges: full.then(|| {
                vec![crate::service::SendTextStyleParams {
                    style: "BOLD".to_string(),
                    start: 0,
                    length: 6,
                }]
            }),
            preview_image: full.then(|| "data:image/png;base64,AAAA".to_string()),
            // Contract 1.91 (§4.74): the link-preview metadata rides the
            // full-option dispatch; the lean shape stays key-free.
            preview_url: full.then(|| "https://example.com/story".to_string()),
            preview_title: full.then(|| "Example".to_string()),
        };

        // The default-shape send projects the lean frame and the fake's
        // empty per-recipient list.
        let story = supervisor
            .send_story(story_params("fixture story body", false))
            .await
            .unwrap();
        assert!(story.timestamp > 0);
        assert!(story.results.is_empty(), "the fake answers the empty list");

        // The full-option send carries every optional key.
        let styled = supervisor
            .send_story(story_params("styled", true))
            .await
            .unwrap();
        assert!(styled.timestamp > 0);

        // Crash with the mutating call in flight: SEND_OUTCOME_UNKNOWN,
        // never retried; an explicit restart lands a fresh engine and the
        // next send works.
        let crash = supervisor
            .send_story(story_params("fixture_story_crash.31", false))
            .await
            .unwrap_err();
        assert!(matches!(crash, crate::service::ServiceError::Api(ref api)
                if api.code == "SEND_OUTCOME_UNKNOWN" && !api.retryable));
        supervisor.start().await.unwrap();
        let recovered = supervisor
            .send_story(story_params("after crash", false))
            .await
            .unwrap();
        assert!(recovered.timestamp > 0);

        // The dispatch log pins the frame shapes: the resolved signal
        // account rides every story frame, the default-keyed options stay
        // absent, the full-option frame carries them all, the crash sentinel
        // dispatched exactly once (no retry), and the raw account id never
        // rides any frame.
        let send_log = std::fs::read_to_string(
            temp.path()
                .join("signal-data")
                .join(".fixture-send-log.jsonl"),
        )
        .expect("fixture must log the dispatches");
        let dispatches: Vec<serde_json::Value> = send_log
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        let story_dispatches: Vec<&serde_json::Value> = dispatches
            .iter()
            .filter(|params| params.get("recipients").is_some())
            .collect();
        assert_eq!(story_dispatches.len(), 4, "three sends + the crash");
        for dispatch in &story_dispatches {
            assert_eq!(dispatch["account"], "+15555550100");
            assert!(
                dispatch.get("accountId").is_none(),
                "the store account id never rides the frame"
            );
        }
        let lean = story_dispatches
            .iter()
            .find(|params| params["message"] == "fixture story body")
            .expect("the default-shape dispatch must land");
        assert!(lean.get("distributionListId").is_none());
        assert!(lean.get("textStyle").is_none());
        assert!(lean.get("bodyRanges").is_none());
        assert!(lean.get("previewImage").is_none());
        // Contract 1.91 (§4.74): the metadata keys are absent-optional.
        assert!(lean.get("previewUrl").is_none());
        assert!(lean.get("previewTitle").is_none());
        assert_eq!(lean["allowsReplies"], false);
        let full = story_dispatches
            .iter()
            .find(|params| params["message"] == "styled")
            .expect("the full-option dispatch must land");
        assert_eq!(
            full["distributionListId"],
            "11111111-2222-3333-4444-555555555555"
        );
        assert_eq!(full["textStyle"], "BOLD");
        assert_eq!(full["allowsReplies"], true);
        assert_eq!(full["bodyRanges"][0]["style"], "BOLD");
        assert_eq!(full["bodyRanges"][0]["start"], 0);
        assert_eq!(full["bodyRanges"][0]["length"], 6);
        assert!(full.get("previewImage").is_some());
        // The metadata keys ride the engine frame verbatim (camelCase).
        assert_eq!(full["previewUrl"], "https://example.com/story");
        assert_eq!(full["previewTitle"], "Example");
        assert_eq!(
            story_dispatches
                .iter()
                .filter(|params| params["message"] == "fixture_story_crash.31")
                .count(),
            1,
            "an unknown sendStory outcome is never retried"
        );
        supervisor.shutdown().await.unwrap();
    }

    /// stories.markViewed (contract 1.85, §4.70): the frame is
    /// exactly the engine's `sendViewedReceipt` params — resolved signal
    /// account, the trimmed author as the single recipient, one timestamp —
    /// the receipt-class answer is `sent`, a crash in flight degrades to
    /// `unknown` without a retry, an explicit restart lands a fresh engine
    /// and the next receipt sends again, and the raw store account id never
    /// rides any frame.
    #[tokio::test]
    async fn stories_mark_viewed_routes_sendviewedreceipt() {
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
            None,
        ));
        supervisor.start().await.unwrap();
        let account = supervisor
            .service
            .lock()
            .await
            .sync_accounts_from_numbers(&["+15555550100".into()], DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);

        let viewed = |author: &str, sent_at: u64| crate::service::StoriesMarkViewedParams {
            account_id: account.id.clone(),
            story_author: author.to_string(),
            story_sent_at: sent_at,
        };

        let sent = supervisor
            .send_story_viewed(viewed("  +15555550101  ", 1727000000000))
            .await
            .unwrap();
        assert_eq!(sent, "sent");

        // Crash with the mutating receipt in flight: the receipt may or may
        // not have reached the server — `unknown`, never retried.
        let crash = supervisor
            .send_story_viewed(viewed("+15555550101", 425))
            .await
            .unwrap();
        assert_eq!(crash, "unknown");
        supervisor.start().await.unwrap();
        let recovered = supervisor
            .send_story_viewed(viewed("+15555550101", 1727000000001))
            .await
            .unwrap();
        assert_eq!(recovered, "sent");

        // The dispatch log pins the frame shapes: resolved signal account,
        // the trimmed author as the single recipient, exactly one timestamp
        // per receipt, the crash sentinel dispatched exactly once (no
        // retry), and the raw account id never rides any frame.
        let send_log = std::fs::read_to_string(
            temp.path()
                .join("signal-data")
                .join(".fixture-send-log.jsonl"),
        )
        .expect("fixture must log the dispatches");
        let dispatches: Vec<serde_json::Value> = send_log
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        let receipts: Vec<&serde_json::Value> = dispatches
            .iter()
            .filter(|params| params.get("recipient").is_some())
            .collect();
        assert_eq!(receipts.len(), 3, "one receipt + the crash + the recovery");
        for dispatch in &receipts {
            assert_eq!(dispatch["account"], "+15555550100");
            assert!(
                dispatch.get("accountId").is_none(),
                "the store account id never rides the frame"
            );
            assert_eq!(dispatch["timestamps"].as_array().unwrap().len(), 1);
        }
        assert_eq!(receipts[0]["recipient"], "+15555550101");
        assert_eq!(
            receipts[0]["timestamps"],
            serde_json::json!([1727000000000u64])
        );
        assert_eq!(receipts[1]["timestamps"], serde_json::json!([425]));
        assert_eq!(
            receipts[2]["timestamps"],
            serde_json::json!([1727000000001u64])
        );
        supervisor.shutdown().await.unwrap();
    }

    /// stories.sendReaction: the engine-gap answer is
    /// CAPABILITY_UNAVAILABLE (not retryable) after local validation — the
    /// §4.53 precedent — and no frame ever leaves the process.
    #[tokio::test]
    async fn stories_send_reaction_answers_capability_unavailable() {
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
            None,
        ));
        supervisor.start().await.unwrap();
        let account = supervisor
            .service
            .lock()
            .await
            .sync_accounts_from_numbers(&["+15555550100".into()], DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);

        let error = supervisor
            .send_story_reaction(crate::service::StoriesSendReactionParams {
                account_id: account.id.clone(),
                story_author: "+15555550101".into(),
                story_sent_at: 1727000000000,
                emoji: "👍".into(),
                remove: false,
                operation_id: None,
            })
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            crate::service::ServiceError::Api(ref api)
                if api.code == "CAPABILITY_UNAVAILABLE" && !api.retryable
        ));
        assert!(
            !temp
                .path()
                .join("signal-data")
                .join(".fixture-send-log.jsonl")
                .exists(),
            "no frame ever leaves the process"
        );
        supervisor.shutdown().await.unwrap();
    }

    /// messages.markViewed (contract 1.94, §4.77) end to end:
    /// the selected incoming rows take their local `viewed_at` stamp while
    /// the upstream receipt still confirms `sent`, a group conversation
    /// stamps with an empty fan-out, and messages.markRead never stamps.
    #[tokio::test]
    async fn mark_viewed_stamps_locally_and_mark_read_does_not() {
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
            store.clone(),
            DEFAULT_PROXY_GROUP_ID.to_string(),
            None,
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
        let group = supervisor
            .service
            .lock()
            .await
            .store_ref()
            .ensure_conversation(&account.id, "group", "ZmFrZS1ncm91cC0x", "Group")
            .unwrap();
        let seed = |id: &str, sent_at: u64, conversation_id: &str| {
            let record = crate::store::MessageRecord {
                id: id.into(),
                account_id: account.id.clone(),
                conversation_id: conversation_id.to_string(),
                direction: "incoming",
                sender_id: "peer-hash".into(),
                sender_name: None,
                mentions_self: false,
                sent_at,
                received_at: None,
                text: Some("voice note".into()),
                text_bytes: Some(10),
                text_truncated: false,
                text_retrievable: true,
                status: "delivered",
                client_request_id: None,
                quote_message_id: None,
                quote_snapshot: None,
                attachments: Vec::new(),
                rich: None,
                edited_at: None,
                reactions: Vec::new(),
                edits: Vec::new(),
                delivered_at: None,
                read_at: None,
                viewed_at: None,
                sticker: None,
                expire_seconds: None,
                expire_start_at: None,
                admin_deleted: false,
                server_guid: None,
                server_timestamp: None,
            };
            assert!(
                store
                    .insert_message(&record, None, Some("voice note"), true)
                    .unwrap()
            );
        };
        seed("m1", 10, &conversation.id);
        seed("m2", 20, &conversation.id);
        seed("m3", 30, &conversation.id);
        seed("g1", 50, &group.id);
        let viewed_at = |conversation_id: &str, id: &str| {
            store
                .message_by_id(&account.id, conversation_id, id)
                .unwrap()
                .unwrap()
                .viewed_at
        };

        // markRead first: the receipt fans out and confirms, and no row
        // takes any local stamp.
        let read_status = supervisor
            .send_read_receipt(
                account.id.clone(),
                conversation.id.clone(),
                Some(vec!["m1".into()]),
            )
            .await
            .unwrap();
        assert_eq!(read_status, "sent");
        assert_eq!(viewed_at(&conversation.id, "m1"), None);

        // markViewed with an explicit selection: the upstream receipt
        // confirms `sent` and the selected row is stamped locally.
        let viewed_status = supervisor
            .send_viewed_receipt(
                account.id.clone(),
                conversation.id.clone(),
                Some(vec!["m1".into()]),
            )
            .await
            .unwrap();
        assert_eq!(viewed_status, "sent");
        let first_stamp = viewed_at(&conversation.id, "m1");
        assert!(first_stamp.is_some(), "the selected row is stamped");
        assert_eq!(viewed_at(&conversation.id, "m2"), None);

        // The default selection covers the rest; the first stamp never
        // moves.
        let viewed_all = supervisor
            .send_viewed_receipt(account.id.clone(), conversation.id.clone(), None)
            .await
            .unwrap();
        assert_eq!(viewed_all, "sent");
        assert!(viewed_at(&conversation.id, "m2").is_some());
        assert!(viewed_at(&conversation.id, "m3").is_some());
        assert_eq!(viewed_at(&conversation.id, "m1"), first_stamp);

        // A group conversation's fan-out is empty — the answer is the
        // trivial `sent` — and the local stamp still lands.
        let group_status = supervisor
            .send_viewed_receipt(account.id.clone(), group.id.clone(), None)
            .await
            .unwrap();
        assert_eq!(group_status, "sent");
        assert!(viewed_at(&group.id, "g1").is_some());
        supervisor.shutdown().await.unwrap();
    }

    /// §4.38 step 3 (contract revision 1.98), direct branch: no environment
    /// value builds the same client the chain has always used — the budgets
    /// and the no-redirect posture, and no proxy layer.
    #[test]
    fn download_client_without_an_env_value_builds_the_direct_route() {
        assert!(attachment_download_client(None).is_ok());
    }

    /// The environment value parses through the doctor's tolerant parse, so
    /// a malformed value reads as unconfigured everywhere else too: the
    /// direct route, never a half-configured client.
    #[test]
    fn download_client_treats_a_malformed_env_value_as_unconfigured() {
        assert!(attachment_download_client(Some("not a proxy")).is_ok());
        assert!(attachment_download_client(Some("host-without-port")).is_ok());
    }

    /// The SOCKS branch, proven against the real transport: a loopback proxy
    /// port with no listener refuses the SOCKS handshake immediately, so the
    /// GET fails fast offline — a client that silently ignored the route
    /// would attempt the real DNS/TLS path instead (the doctor's
    /// `socks_refused` proof, mirrored for the CDN client). Both canonical
    /// `host:port` and the tolerated `socks5h://` prefix wire the route.
    #[tokio::test]
    async fn download_client_wires_the_socks_route_into_the_transport() {
        for value in ["127.0.0.1:1", "socks5h://127.0.0.1:1"] {
            let client = attachment_download_client(Some(value)).expect("client builds");
            let error = client
                .get("https://chat.signal.org/")
                .send()
                .await
                .expect_err("nothing listens on the loopback socks port");
            assert!(
                error.is_connect(),
                "the refusal must be the proxy hop, got: {error}"
            );
        }
    }
}
