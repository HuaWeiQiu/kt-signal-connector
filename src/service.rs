// SPDX-License-Identifier: AGPL-3.0-only

use std::path::PathBuf;
use std::sync::Arc;
#[cfg(test)]
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;
use unicode_segmentation::UnicodeSegmentation;

use crate::engine::{
    ControlReceive, EngineError, NormalizedAttachment, NormalizedReceive, truncate_utf8_bytes,
};
use crate::groups::MAX_ACCOUNTS_PER_ENGINE;
use crate::ids::{mask_address, stable_hash_id};
use crate::link::{ActiveLinkSession, LINK_SESSION_TTL, now_ms};
use crate::media::{MEDIA_CHUNK_BYTES, MediaGovernor, MediaHandleError, MediaHandleTable};
use crate::protocol::ApiError;
use crate::store::{
    AccountDeletePlan, AccountRow, AccountSummary, ContactSummary, ConversationRow,
    ConversationSummary, MessageRecord, Page, Store, StoreError, SyncedContact,
};

/// Bounds below are the code-side enforcement of the host-facing schema
/// (schemas/connector-api-v1.schema.json); tests/schema_consistency.rs diffs
/// each constant against its schema constraint, so the two cannot drift
/// apart silently.
pub const MAX_TEXT_BYTES: usize = 64 * 1024;
const MAX_INBOUND_TEXT_BYTES: usize = 128 * 1024;
const MAX_HOST_TEXT_PREVIEW_BYTES: usize = 4 * 1024;
pub const MAX_DEVICE_NAME_BYTES: usize = 64;
pub const MAX_EMOJI_BYTES: usize = 32;
/// contacts.setLocalAlias bound (contract revision 1.10): the alias is a
/// short display name, not a free-form profile field — 128 bytes matches the
/// peerKey/opaqueId bound and keeps the upstream `updateContact` payload
/// trivially small.
pub const MAX_ALIAS_BYTES: usize = 128;
/// Attachment size budget (contract revision 1.16, implementation-plan
/// §4.15): the official clients' 100 MiB ceiling. The engine's upstream line
/// limit and the host frame budget move with it (both 160 MiB, see lib.rs
/// DEFAULT_UPSTREAM_LINE_LIMIT / DEFAULT_HOST_FRAME_LIMIT) so one maximum
/// attachment's base64 always fits a single line/frame.
pub const MAX_ATTACHMENT_BYTES: usize = 100 * 1024 * 1024;
/// Encoded length of [`MAX_ATTACHMENT_BYTES`]: 4 * ceil(n / 3), the exact
/// padded-base64 length java.util.Base64 emits.
const MAX_ATTACHMENT_BASE64_CHARS: usize = 4 * MAX_ATTACHMENT_BYTES.div_ceil(3);
/// Attachment send bounds (contract revision 1.13, implementation-plan
/// §4.12): filename/contentType are caller-supplied display descriptors for
/// the upstream data URI, bounded like the opaqueId family; the decoded
/// payload must match the declared `sizeBytes` exactly (§4.7 discipline).
pub const MAX_ATTACHMENT_FILENAME_BYTES: usize = 128;
pub const MAX_ATTACHMENT_CONTENT_TYPE_BYTES: usize = 128;
/// An upstream attachment id (receive-time metadata field `id`), bounded to
/// the schema's `attachmentId` maxLength (schemas/connector-api-v1.schema.json
/// is the single source for this length); Signal ids stay far below it, the
/// bound only rejects absurd values early.
pub const MAX_ATTACHMENT_ID_BYTES: usize = 128;
/// Every opaque identifier the host frame carries (accountId, conversationId,
/// messageId, clientRequestId, operationId, peerKey, peerTitle, query,
/// groupKey — the schema's opaqueId family and its inline 128 peers).
pub const MAX_OPAQUE_ID_BYTES: usize = 128;
/// The receipt selection cap (contract revision 1.34, §4.29): both the
/// absent-`messageIds` scan and an explicit list are bounded at 512 incoming
/// rows — the schema declares the same bound as the early shape guard, this
/// constant is the authoritative code-side enforcement.
pub const MARK_RECEIPT_MESSAGE_IDS_LIMIT: usize = 512;
/// The outbound @mention cap (contract revision 1.34): the §4.20 receive
/// projection cap mirrored — entries beyond 64 are dropped, mirroring the
/// official bounded BodyRange list.
pub const MAX_SEND_MENTIONS: usize = 64;

#[derive(Debug, Error)]
pub enum ServiceError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Engine(#[from] EngineError),
    #[error("api error: {0}")]
    Api(ApiError),
}

impl ServiceError {
    /// Log-safe classification: variant category only, never any content the
    /// error may carry. Logs use this instead of the Display text.
    pub fn class(&self) -> &'static str {
        match self {
            ServiceError::Store(_) => "store",
            ServiceError::Engine(_) => "engine",
            ServiceError::Api(_) => "api",
        }
    }

    pub fn into_api(self) -> ApiError {
        match self {
            ServiceError::Api(error) => error,
            ServiceError::Store(StoreError::AccountNotFound) => {
                ApiError::new("ACCOUNT_NOT_FOUND", "account was not found", false)
            }
            ServiceError::Store(StoreError::ConversationNotFound) => ApiError::new(
                "CONVERSATION_NOT_FOUND",
                "conversation was not found",
                false,
            ),
            ServiceError::Store(StoreError::MessageNotFound) => {
                ApiError::new("MESSAGE_NOT_FOUND", "message was not found", false)
            }
            ServiceError::Store(StoreError::OperationConflict) => ApiError::new(
                "INVALID_REQUEST",
                "account delete operation conflicts with existing state",
                false,
            ),
            ServiceError::Store(StoreError::InvalidCursor) => {
                ApiError::new("INVALID_REQUEST", "pagination cursor is invalid", false)
            }
            ServiceError::Store(_) => {
                ApiError::new("INTERNAL_ERROR", "connector store failed", true)
            }
            ServiceError::Engine(EngineError::NotRunning) => ApiError::new(
                "RUNTIME_NOT_RUNNING",
                "signal-cli runtime is not running",
                false,
            ),
            ServiceError::Engine(EngineError::Timeout) => {
                ApiError::new("UPSTREAM_TIMEOUT", "signal-cli request timed out", true)
            }
            ServiceError::Engine(EngineError::UnknownOutcome) => ApiError::new(
                "SEND_OUTCOME_UNKNOWN",
                "mutating request has an unknown outcome",
                false,
            ),
            ServiceError::Engine(EngineError::Exited) => {
                ApiError::new("UPSTREAM_EXITED", "signal-cli exited", true)
            }
            ServiceError::Engine(EngineError::Protocol) => {
                ApiError::new("UPSTREAM_PROTOCOL_ERROR", "signal-cli protocol error", true)
            }
            ServiceError::Engine(EngineError::Upstream) => {
                ApiError::new("UPSTREAM_ERROR", "signal-cli returned an error", true)
            }
            ServiceError::Engine(EngineError::Unauthorized) => ApiError::new(
                "ACCOUNT_UNLINKED",
                "device was unlinked by the account holder",
                false,
            ),
            ServiceError::Engine(EngineError::Backpressure) => {
                ApiError::new("INTERNAL_ERROR", "signal-cli request queue is full", true)
            }
            ServiceError::Engine(EngineError::StartFailed) => ApiError::new(
                "RUNTIME_START_FAILED",
                "signal-cli runtime could not be started",
                true,
            ),
        }
    }
}

#[derive(Clone, Debug)]
// The reactions projection (contract 1.16) makes `MessageUpserted` the large
// variant. Events are transient and single-consumer on an in-process lane, so
// boxing every payload to appease the size lint would trade a real allocation
// per message event for nothing.
#[allow(clippy::large_enum_variant)]
pub enum HostSideEvent {
    StorageChanged {
        state: &'static str,
    },
    AccountChanged(AccountSummary),
    ConversationChanged(ConversationSummary),
    MessageUpserted(MessageRecord),
    MessageStatusChanged {
        account_id: String,
        message_id: String,
        status: &'static str,
        /// Contract 1.32: receipt stamps carried on the transition that wrote
        /// them (absent on transitions that did not stamp, so the host merge
        /// never clears an existing stamp with a no-op).
        delivered_at: Option<u64>,
        read_at: Option<u64>,
    },
    /// Ephemeral typing indicator (contract 1.15): never persisted, rate
    /// limited per account+peer before it reaches the host lane.
    ConversationTyping {
        account_id: String,
        conversation_id: String,
        action: String,
    },
}

/// Ephemeral typing notifications keep the event lane quiet: at most one
/// notification per account+conversation pair per second, bounded map, oldest
/// entry evicted (§4.13).
const TYPING_RATE_LIMIT_MS: u64 = 1_000;
const TYPING_RATE_MAP_CAPACITY: usize = 256;

/// The filesystem-and-handles half of media ingest (ADR 0002), injected into
/// one service per proxy group when the launcher passes `--media-ingest`.
/// `None` is the pre-1.17 shape: every media method answers
/// `CAPABILITY_UNAVAILABLE` before anything else happens. The handle table is
/// process-wide (a `readChunk` carries no accountId, so the table — not the
/// routing — is the single-origin authority) and is therefore handed in as a
/// shared `Arc` across all groups.
#[derive(Clone)]
pub struct MediaIngest {
    governor: MediaGovernor,
    handles: Arc<MediaHandleTable>,
}

impl MediaIngest {
    pub fn new(data_dir: PathBuf, handles: Arc<MediaHandleTable>) -> Self {
        Self {
            governor: MediaGovernor::new(data_dir),
            handles,
        }
    }

    /// The retention machine the supervisor drives (process start + every
    /// [`crate::media::GOVERNOR_INBOUND_MESSAGE_INTERVAL`] inbound messages).
    pub fn governor(&self) -> &MediaGovernor {
        &self.governor
    }

    /// The process-wide chunk-stream table.
    pub fn handles(&self) -> &MediaHandleTable {
        &self.handles
    }

    /// Resolve a sanitized attachment id to its on-disk file and size by
    /// probing the engine's candidate `attachments/` directories
    /// (signal-cli `PathConfig` layout — see
    /// [`MediaGovernor::attachments_dirs`]). `None` means the id names no
    /// downloaded file: not-downloaded and governor-deleted are
    /// deliberately indistinguishable (UPSTREAM_ERROR, as before the PoC).
    fn locate_attachment(&self, sanitized_id: &str) -> Option<(PathBuf, u64)> {
        for dir in self.governor.attachments_dirs() {
            let candidate = dir.join(sanitized_id);
            if let Ok(metadata) = std::fs::metadata(&candidate)
                && metadata.is_file()
            {
                return Some((candidate, metadata.len()));
            }
        }
        None
    }
}

/// The capability gate every media method shares (same shape as the
/// `messages.getText` gate): without the launcher opt-in the answer is
/// deterministic and precedes every other check.
fn media_capability_error() -> ServiceError {
    ServiceError::Api(ApiError::new(
        "CAPABILITY_UNAVAILABLE",
        "media ingest is not enabled for this connector",
        false,
    ))
}

/// Map the handle table's content-free failures onto the wire codes. Unknown,
/// expired, non-sequential, and over-ceiling are all caller mistakes
/// (INVALID_REQUEST); an I/O failure mid-stream is the upstream file
/// world (UPSTREAM_ERROR, same answer a missing attachment gives).
fn map_media_handle_error(error: MediaHandleError) -> ServiceError {
    let (code, message, retryable) = match error {
        MediaHandleError::NotFound => (
            "INVALID_REQUEST",
            "mediaHandle is unknown, expired, or already closed",
            false,
        ),
        // The wire answer states the rule, never the handle's internal read
        // position: state helps a probing caller, the rule helps a buggy one.
        MediaHandleError::NotSequential { .. } => (
            "INVALID_REQUEST",
            "offset must match the bytes already read on this handle",
            false,
        ),
        MediaHandleError::Exhausted => (
            "INVALID_REQUEST",
            "the account already holds the maximum number of open media handles",
            false,
        ),
        MediaHandleError::Io => ("UPSTREAM_ERROR", "attachment read failed", true),
    };
    ServiceError::Api(ApiError::new(code, message, retryable))
}

/// Whether an outgoing row's `sent_at` is the upstream Signal protocol
/// timestamp (real protocol identity) rather than a local clock value.
/// `complete_outgoing_send` overwrites `sent_at` with the send response's
/// upstream timestamp when the row settles at `sent`; later delivery/read
/// receipts only advance `status` — `update_message_status` preserves
/// `sent_at` (COALESCE) — so `sent`/`delivered`/`read` rows are all
/// addressable upstream. `pending`/`failed`/`unknown` rows hold a local
/// `now_ms()` value and are indistinguishable from a missing row
/// (remote-delete plan §3.2 precedent).
fn outgoing_status_is_addressable(status: &str) -> bool {
    matches!(status, "sent" | "delivered" | "read")
}

pub struct ConnectorService {
    store: Arc<Store>,
    link: Option<ActiveLinkSession>,
    /// Typing rate limiter: (account_id, conversation_id) → last emission.
    typing_last_emission: std::collections::HashMap<(String, String), u64>,
    /// Media ingest backing (ADR 0002); `None` keeps every media method on
    /// the pre-PoC `CAPABILITY_UNAVAILABLE` answer.
    media: Option<MediaIngest>,
}

impl ConnectorService {
    pub fn new(store: Arc<Store>) -> Self {
        Self::with_media(store, None)
    }

    /// Service with the media ingest backing enabled (`--media-ingest`).
    /// The handle table comes in as a shared `Arc` because it is
    /// process-wide: `readChunk`/`closeHandle` carry no accountId on the
    /// wire, so all groups must resolve handles from one table.
    pub fn with_media(store: Arc<Store>, media: Option<MediaIngest>) -> Self {
        Self {
            store,
            link: None,
            typing_last_emission: std::collections::HashMap::new(),
            media,
        }
    }

    pub fn store_ref(&self) -> &Store {
        &self.store
    }

    pub fn clear_link(&mut self) {
        self.link = None;
    }

    /// Sync live signal-cli numbers of ONE group's engine into the store. New
    /// numbers bind to that group (R3); known numbers keep their immutable
    /// binding. Returns the accounts visible in this group.
    pub fn sync_accounts_from_numbers(
        &self,
        numbers: &[String],
        proxy_group: &str,
    ) -> Result<Vec<AccountSummary>, ServiceError> {
        for number in numbers {
            let _ = self
                .store
                .upsert_account_from_signal(number, None, proxy_group)?;
        }
        Ok(self.store.list_accounts_in_group(proxy_group)?)
    }

    pub fn list_accounts_in_group(
        &self,
        proxy_group: &str,
    ) -> Result<Vec<AccountSummary>, ServiceError> {
        Ok(self.store.list_accounts_in_group(proxy_group)?)
    }

    /// Proxy group an account is bound to, or ACCOUNT_NOT_FOUND. The routing
    /// key for every account-addressed host method (implementation-plan §4.4).
    pub fn account_proxy_group(&self, account_id: &str) -> Result<String, ServiceError> {
        self.store
            .account_by_id(account_id)?
            .map(|row| row.proxy_group)
            .ok_or(ServiceError::Store(StoreError::AccountNotFound))
    }

    /// Best-effort `accountCount` for one runtime group entry: a store hiccup
    /// degrades the status field to 0 rather than failing the status report.
    pub fn count_accounts_in_group_lossy(&self, proxy_group: &str) -> u64 {
        self.store.count_accounts_in_group(proxy_group).unwrap_or(0)
    }

    pub fn account_signal_number(&self, account_id: &str) -> Result<String, ServiceError> {
        self.store
            .account_by_id(account_id)?
            .map(|row| row.signal_account)
            .ok_or(ServiceError::Store(StoreError::AccountNotFound))
    }

    pub fn account_signal_number_optional(
        &self,
        account_id: &str,
    ) -> Result<Option<String>, ServiceError> {
        Ok(self
            .store
            .account_by_id(account_id)?
            .map(|row| row.signal_account))
    }

    pub fn set_account_display_name(
        &self,
        account_id: &str,
        display_name: Option<&str>,
    ) -> Result<AccountSummary, ServiceError> {
        Ok(self
            .store
            .set_account_display_name(account_id, display_name)?)
    }

    pub fn prepare_account_delete(
        &mut self,
        account_id: &str,
        operation_id: &str,
    ) -> Result<AccountDeletePlan, ServiceError> {
        Ok(self
            .store
            .prepare_account_delete(account_id, operation_id, now_ms())?)
    }

    pub fn mark_account_delete_unknown(&self, operation_id: &str) -> Result<(), ServiceError> {
        Ok(self
            .store
            .mark_account_delete_unknown(operation_id, now_ms())?)
    }

    pub fn complete_account_delete(
        &mut self,
        account_id: &str,
        operation_id: Option<&str>,
    ) -> Result<bool, ServiceError> {
        Ok(self
            .store
            .complete_account_delete(account_id, operation_id, now_ms())?)
    }

    pub fn begin_link(
        &mut self,
        device_name: String,
        device_link_uri: String,
    ) -> Result<Value, ServiceError> {
        validate_device_name(&device_name)?;
        if let Some(existing) = self.link.as_ref() {
            if !existing.is_expired(now_ms()) {
                return Err(ServiceError::Api(ApiError::new(
                    "LINK_IN_PROGRESS",
                    "a link session is already active",
                    false,
                )));
            }
            self.link = None;
        }
        let session = ActiveLinkSession::new(device_name, device_link_uri, LINK_SESSION_TTL);
        let response = json!({
            "linkSessionId": session.session_id,
            "qrPayload": session.qr_payload(),
            "expiresAt": session.expires_at_ms,
        });
        self.link = Some(session);
        Ok(response)
    }

    pub fn ensure_link_available(&mut self) -> Result<(), ServiceError> {
        if let Some(existing) = self.link.as_ref() {
            if !existing.is_expired(now_ms()) {
                return Err(ServiceError::Api(ApiError::new(
                    "LINK_IN_PROGRESS",
                    "a link session is already active",
                    false,
                )));
            }
            self.link = None;
        }
        Ok(())
    }

    /// Clone link credentials without clearing the session, so a timed-out finishLink can be retried.
    pub fn peek_link_for_finish(
        &mut self,
        link_session_id: &str,
    ) -> Result<(String, String), ServiceError> {
        let session = self.require_active_link(link_session_id)?;
        Ok((
            session.device_name.clone(),
            session.qr_payload().to_string(),
        ))
    }

    pub fn complete_link_session(
        &mut self,
        link_session_id: &str,
        number: &str,
        proxy_group: &str,
    ) -> Result<AccountSummary, ServiceError> {
        let _ = self.require_active_link(link_session_id)?;
        // Per-engine account ceiling (optimization-plan §6.4 M3.3): refuse a
        // finish that would add a ninth account before the session is
        // consumed or any row is written. Re-linking a number already bound
        // to this group is an update, not an addition, and stays allowed.
        if self.link_would_exceed_ceiling(number, proxy_group)? {
            return Err(account_limit_error(proxy_group));
        }
        let _ = self.take_link_session(link_session_id)?;
        // The binding is fixed here, at link.finish success, and immutable for
        // the life of the account (ADR 0001 R3).
        Ok(self
            .store
            .upsert_account_from_signal(number, Some(now_ms()), proxy_group)?)
    }

    /// Whether this group already holds the per-engine account ceiling, for
    /// the link entries' early rejection (M3.3). Store failures propagate.
    pub fn group_at_account_ceiling(&self, proxy_group: &str) -> Result<bool, ServiceError> {
        Ok(self.store.count_accounts_in_group(proxy_group)? >= MAX_ACCOUNTS_PER_ENGINE as u64)
    }

    /// Store-level ceiling check for one finishing link (M3.3): true only
    /// when the group is at the ceiling AND this number would be a new
    /// account in it (a re-link of a number already bound here is an
    /// upsert that does not grow the count).
    fn link_would_exceed_ceiling(
        &self,
        number: &str,
        proxy_group: &str,
    ) -> Result<bool, ServiceError> {
        if self
            .store
            .account_by_signal(number)?
            .is_some_and(|row| row.proxy_group == proxy_group)
        {
            return Ok(false);
        }
        self.group_at_account_ceiling(proxy_group)
    }

    pub fn cancel_link(&mut self, link_session_id: &str) -> Result<Value, ServiceError> {
        let _ = self.take_link_session(link_session_id)?;
        Ok(json!({}))
    }

    /// True while a non-expired link session exists. Restarting signal-cli would
    /// invalidate its deviceLinkUri, so the watchdog must not restart meanwhile.
    pub fn has_pending_link(&self) -> bool {
        self.link
            .as_ref()
            .is_some_and(|session| !session.is_expired(now_ms()))
    }

    /// Whether this group's service owns the link session, expired or not:
    /// routing a finish/cancel to its owning group must preserve the distinct
    /// LINK_EXPIRED answer an expired session produces at dispatch time.
    pub fn has_link_session(&self, link_session_id: &str) -> bool {
        self.link
            .as_ref()
            .is_some_and(|session| session.session_id == link_session_id)
    }

    /// Test-only: plant a link session with an explicit TTL so registry and
    /// service tests can pin expired-session routing without waiting out the
    /// real five-minute [`LINK_SESSION_TTL`].
    #[cfg(test)]
    pub(crate) fn plant_link_session_for_test(
        &mut self,
        device_name: &str,
        ttl: Duration,
    ) -> String {
        let session =
            ActiveLinkSession::new(device_name.to_string(), "sgnl://link?test".into(), ttl);
        self.link = Some(session);
        self.link
            .as_ref()
            .expect("session planted")
            .session_id
            .clone()
    }

    /// Signal number of any account bound to this group; the watchdog ping target.
    pub fn any_signal_account_number_in_group(
        &self,
        proxy_group: &str,
    ) -> Result<Option<String>, ServiceError> {
        Ok(self.store.any_signal_account_number_in_group(proxy_group)?)
    }

    /// Signal number of any linked account, used as the watchdog ping target.
    pub fn any_signal_account_number(&self) -> Result<Option<String>, ServiceError> {
        Ok(self.store.any_signal_account_number()?)
    }

    pub fn list_conversations(
        &self,
        account_id: &str,
        limit: u32,
        cursor: Option<&str>,
    ) -> Result<Page<ConversationSummary>, ServiceError> {
        store_list_conversations(&self.store, account_id, limit, cursor)
    }

    /// Read-only view of the contacts cache; never touches the upstream engine.
    pub fn list_contacts(
        &self,
        account_id: &str,
        query: Option<&str>,
        limit: u32,
        cursor: Option<&str>,
    ) -> Result<Page<ContactSummary>, ServiceError> {
        store_list_contacts(&self.store, account_id, query, limit, cursor)
    }

    /// Read-only projection of one cached group row (contract revision 1.9,
    /// implementation-plan §4.8): served entirely from the contacts cache
    /// written by contacts.sync — the upstream is never called, so an unsynced
    /// or departed group answers a deterministic GROUP_NOT_FOUND instead of a
    /// listGroups round trip. `syncedAt` lets the host judge staleness itself;
    /// the connector adds no second cache layer.
    pub fn get_group(
        &self,
        account_id: &str,
        group_key: &str,
    ) -> Result<GroupDetails, ServiceError> {
        store_get_group(&self.store, account_id, group_key)
    }

    // --- Shared target resolution for the prepare_* family -----------------
    // (optimization-plan §5.2 B3): every prepare_* resolves the addressed rows
    // through this one ladder, in wire order — account, then conversation,
    // then message — so a bogus id answers its NOT_FOUND code before any
    // upstream call (implementation-plan §4.4). Per-function validation runs
    // first and stays in the caller.

    /// The addressed account row, or ACCOUNT_NOT_FOUND.
    fn resolve_account(&self, account_id: &str) -> Result<AccountRow, ServiceError> {
        Ok(self
            .store
            .account_by_id(account_id)?
            .ok_or(StoreError::AccountNotFound)?)
    }

    /// The addressed conversation row, or CONVERSATION_NOT_FOUND.
    fn resolve_conversation(
        &self,
        account_id: &str,
        conversation_id: &str,
    ) -> Result<ConversationRow, ServiceError> {
        Ok(self
            .store
            .conversation_by_id(account_id, conversation_id)?
            .ok_or(StoreError::ConversationNotFound)?)
    }

    /// The addressed message row, or MESSAGE_NOT_FOUND.
    fn resolve_message(
        &self,
        account_id: &str,
        conversation_id: &str,
        message_id: &str,
    ) -> Result<MessageRecord, ServiceError> {
        Ok(self
            .store
            .message_by_id(account_id, conversation_id, message_id)?
            .ok_or(StoreError::MessageNotFound)?)
    }

    /// Account → conversation → message in wire order, for the
    /// message-addressed trio (`messages.remoteDelete`, `messages.sendReaction`,
    /// `messages.attachments.open`).
    fn resolve_target(
        &self,
        account_id: &str,
        conversation_id: &str,
        message_id: &str,
    ) -> Result<(AccountRow, ConversationRow, MessageRecord), ServiceError> {
        let account = self.resolve_account(account_id)?;
        let conversation = self.resolve_conversation(account_id, conversation_id)?;
        let message = self.resolve_message(account_id, conversation_id, message_id)?;
        Ok((account, conversation, message))
    }

    /// contacts.setLocalAlias (contract revision 1.10, implementation-plan
    /// §4.9): resolve the rename locally, then build the upstream
    /// `updateContact` dispatch. The peer must already be known to the
    /// account — a cached `kind='contact'` contacts row or the peer of an
    /// existing direct conversation — so a bogus peerKey answers a
    /// deterministic INVALID_REQUEST before any upstream call.
    pub fn prepare_set_local_alias(
        &self,
        account_id: &str,
        peer_key: &str,
        alias: &str,
    ) -> Result<Value, ServiceError> {
        validate_opaque_id(account_id, "accountId")?;
        validate_opaque_id(peer_key, "peerKey")?;
        validate_alias(alias)?;
        let account = self.resolve_account(account_id)?;
        let known = self
            .store
            .contact_by_peer(account_id, "contact", peer_key)?
            .is_some()
            || self
                .store
                .conversation_by_peer(account_id, "direct", peer_key)?
                .is_some();
        if !known {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "peerKey must name a known contact or direct conversation peer",
                false,
            )));
        }
        // signal-cli JSON-RPC updateContact parameters (verified against the
        // pinned 0.14.7 distribution: UpdateContactCommand reads `recipient`
        // with ns.getString — a single string, not an array; the earlier
        // feasibility note's `recipient: [peerKey]` would ClassCastException
        // into an upstream INTERNAL_ERROR): `account` is consumed by the
        // multi-account dispatcher, `name` is the new alias.
        Ok(json!({
            "account": account.signal_account,
            "recipient": peer_key,
            "name": alias,
        }))
    }

    /// presence.setTypingMessage (contract revision 1.11, implementation-plan
    /// §4.10): resolve the conversation locally, then build the upstream
    /// `sendTyping` dispatch. Targeting is by conversationId only and nothing
    /// is checked against the message history — a typing indicator references
    /// no message. The `stop` boolean is always sent explicitly: with the key
    /// absent the upstream `getBoolean` answers null, and the connector does
    /// not depend on null handling outside the pinned contract.
    pub fn prepare_set_typing_message(
        &self,
        account_id: &str,
        conversation_id: &str,
        stop: bool,
    ) -> Result<Value, ServiceError> {
        validate_opaque_id(account_id, "accountId")?;
        validate_opaque_id(conversation_id, "conversationId")?;
        let account = self.resolve_account(account_id)?;
        let conversation = self.resolve_conversation(account_id, conversation_id)?;
        // signal-cli JSON-RPC sendTyping parameters (verified against the
        // pinned 0.14.7 distribution: SendTypingCommand dests `recipient`
        // (nargs=*, consumed with getList), `group-id` and `stop` (a boolean
        // dest read with getBoolean); JsonRpcNamespace maps dash-separated
        // dests to camelCase JSON keys). The recipient array follows the
        // sendReaction addressing precedent (getList wraps a scalar anyway).
        let mut params = json!({
            "account": account.signal_account,
            "stop": stop,
        });
        set_upstream_target(&mut params, &conversation);
        Ok(params)
    }

    /// Cache one full contacts sync atomically: all rows and the sync marker
    /// commit in a single transaction, so a failed batch leaves nothing behind.
    pub fn upsert_synced_contacts(
        &self,
        account_id: &str,
        entries: &[SyncedContact<'_>],
        synced_at: u64,
    ) -> Result<(), ServiceError> {
        Ok(self
            .store
            .upsert_synced_contacts(account_id, entries, synced_at)?)
    }

    pub fn contacts_synced_at(&self, account_id: &str) -> Result<Option<u64>, ServiceError> {
        Ok(self.store.contacts_synced_at(account_id)?)
    }

    pub fn count_contacts(&self, account_id: &str) -> Result<(u64, u64), ServiceError> {
        Ok(self.store.count_contacts(account_id)?)
    }

    pub fn list_messages(
        &self,
        account_id: &str,
        conversation_id: &str,
        limit: u32,
        before: Option<&str>,
    ) -> Result<Page<MessageRecord>, ServiceError> {
        store_list_messages(&self.store, account_id, conversation_id, limit, before)
    }

    pub fn search_messages(
        &self,
        account_id: &str,
        query: &str,
        limit: u32,
        cursor: Option<&str>,
    ) -> Result<Page<MessageRecord>, ServiceError> {
        store_search_messages(&self.store, account_id, query, limit, cursor)
    }

    pub fn get_message_text(
        &self,
        account_id: &str,
        conversation_id: &str,
        message_id: &str,
    ) -> Result<MessageText, ServiceError> {
        store_get_message_text(&self.store, account_id, conversation_id, message_id)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_send_text(
        &self,
        account_id: &str,
        conversation_id: &str,
        text: &str,
        client_request_id: &str,
        quote_message_id: Option<&str>,
        previews: Option<Vec<SendTextPreviewParams>>,
        mentions: Option<Vec<SendTextMentionParams>>,
    ) -> Result<PreparedSend, ServiceError> {
        let preview = previews_to_upstream(previews, text)?;
        validate_text(text)?;
        validate_opaque_id(client_request_id, "clientRequestId")?;
        if let Some(quote) = quote_message_id {
            validate_opaque_id(quote, "quoteMessageId")?;
        }
        if let Some(existing) = self
            .store
            .message_by_client_request(account_id, client_request_id)?
        {
            return Ok(PreparedSend::Existing(Box::new(existing)));
        }
        let account = self.resolve_account(account_id)?;
        let mentions = self.mentions_to_upstream(&account, mentions)?;
        let conversation = self.resolve_conversation(account_id, conversation_id)?;
        self.dispatch_send(
            &account,
            &conversation,
            text,
            client_request_id,
            quote_message_id,
            preview,
            mentions,
            None,
            Vec::new(),
        )
    }

    /// Prepare a send addressed by peer (kind + peer_key) instead of an
    /// existing conversation id. When no conversation for the peer exists yet,
    /// it is created here and carries the first outgoing message. Since
    /// contract revision 1.14 a contacts.sync already materializes skeleton
    /// conversations for synced peers (§6.5), so this path mostly attaches the
    /// first message to an existing skeleton; unknown peers still get their
    /// conversation from this send.
    #[allow(clippy::too_many_arguments)]
    pub fn prepare_send_text_to_peer(
        &self,
        account_id: &str,
        peer: &PeerTarget<'_>,
        text: &str,
        client_request_id: &str,
        quote_message_id: Option<&str>,
        previews: Option<Vec<SendTextPreviewParams>>,
        mentions: Option<Vec<SendTextMentionParams>>,
    ) -> Result<PreparedSend, ServiceError> {
        let preview = previews_to_upstream(previews, text)?;
        validate_text(text)?;
        validate_opaque_id(client_request_id, "clientRequestId")?;
        if let Some(quote) = quote_message_id {
            validate_opaque_id(quote, "quoteMessageId")?;
        }
        // contacts.list reports 'contact'; conversations use 'direct'. Both are
        // accepted for the same direct-chat target.
        let kind = match peer.kind {
            "direct" | "contact" => "direct",
            "group" => "group",
            _ => {
                return Err(ServiceError::Api(ApiError::new(
                    "INVALID_REQUEST",
                    "kind must be 'contact', 'direct', or 'group'",
                    false,
                )));
            }
        };
        validate_opaque_id(peer.peer_key, "peerKey")?;
        let peer_title = peer
            .peer_title
            .map(str::trim)
            .filter(|title| !title.is_empty())
            .map(|title| title.chars().take(64).collect::<String>());
        if let Some(existing) = self
            .store
            .message_by_client_request(account_id, client_request_id)?
        {
            return Ok(PreparedSend::Existing(Box::new(existing)));
        }
        let account = self.resolve_account(account_id)?;
        let mentions = self.mentions_to_upstream(&account, mentions)?;
        let title = peer_title.unwrap_or_else(|| {
            if kind == "group" {
                "group".to_string()
            } else {
                mask_address(peer.peer_key)
            }
        });
        let conversation =
            self.store
                .ensure_conversation(account_id, kind, peer.peer_key, &title)?;
        self.dispatch_send(
            &account,
            &conversation,
            text,
            client_request_id,
            quote_message_id,
            preview,
            mentions,
            None,
            Vec::new(),
        )
    }

    /// Validate and normalize the caller-supplied mentions (contract revision
    /// 1.34) into the exact shape the upstream `send` expects. Enforced here —
    /// not at the engine — because the engine answers a malformed or
    /// unresolvable mention entry with a late upstream internal error, which
    /// would surface as an unknown-outcome-shaped send failure instead of
    /// deterministic local behavior.
    ///
    /// Rules (spec §4.29): more than 64 entries are dropped (the §4.20
    /// receive cap mirrored); an empty/whitespace number fails the whole
    /// request closed INVALID_REQUEST; a UUID-shaped number passes through
    /// (the engine resolves ACIs directly); anything else must digit-suffix
    /// match the linked account's own number or the cached contacts — the
    /// engine `resolve_author_aci` rule — and is dropped when nothing matches
    /// (the official behavior for unresolvable mention addresses). Entries
    /// that survive keep their `number` verbatim: the engine re-resolves
    /// against its own store, which stays the single resolution authority.
    fn mentions_to_upstream(
        &self,
        account: &AccountRow,
        mentions: Option<Vec<SendTextMentionParams>>,
    ) -> Result<Option<Vec<UpstreamSendMention>>, ServiceError> {
        let Some(mentions) = mentions else {
            return Ok(None);
        };
        let mut upstream = Vec::with_capacity(mentions.len().min(MAX_SEND_MENTIONS));
        let mut contacts: Option<Vec<String>> = None;
        for entry in mentions.into_iter().take(MAX_SEND_MENTIONS) {
            if entry.number.trim().is_empty() {
                return Err(ServiceError::Api(ApiError::new(
                    "INVALID_REQUEST",
                    "mention number must not be empty",
                    false,
                )));
            }
            let keep = is_uuid_shaped(&entry.number) || {
                let digits = address_digits(&entry.number);
                !digits.is_empty()
                    && (address_digits(&account.signal_account) == digits
                        || contacts
                            .get_or_insert_with(|| {
                                self.store
                                    .contact_peer_keys(&account.id)
                                    .unwrap_or_default()
                            })
                            .iter()
                            .any(|peer_key| {
                                let candidate = address_digits(peer_key);
                                !candidate.is_empty() && candidate.ends_with(&digits)
                            }))
            };
            if keep {
                upstream.push(UpstreamSendMention {
                    number: entry.number,
                    start: entry.start,
                    length: entry.length,
                });
            }
        }
        Ok((!upstream.is_empty()).then_some(upstream))
    }

    /// Send one attachment (optionally with a caption) — implementation-plan
    /// §4.12. Validation runs entirely before the pending row exists, so a
    /// rejected request leaves nothing behind and the same clientRequestId
    /// stays a fresh request. The base64 payload is size-verified against the
    /// declared `sizeBytes` and re-encoded as an RFC 2397 data URI: upstream
    /// decodes it itself (AttachmentHelper, pinned 0.14.7), uploads via CDN,
    /// and owns any temp file lifetime — bytes never touch connector disk and
    /// no caller-controlled path reaches upstream. The pending row carries the
    /// metadata-only descriptor (same wire shape as inbound, contract 1.15) so
    /// the host renders the outgoing attachment from its first tick.
    #[allow(clippy::too_many_arguments)]
    pub fn prepare_send_attachment(
        &self,
        account_id: &str,
        target: &AttachmentSendTarget<'_>,
        client_request_id: &str,
        data_base64: &str,
        size_bytes: u64,
        filename: Option<&str>,
        content_type: Option<&str>,
        text: Option<&str>,
        quote_message_id: Option<&str>,
    ) -> Result<PreparedSend, ServiceError> {
        validate_attachment_send_payload(data_base64, size_bytes)?;
        validate_attachment_descriptor(filename, "filename")?;
        validate_attachment_content_type(content_type)?;
        let caption = text.unwrap_or_default();
        if !caption.is_empty() {
            validate_text(caption)?;
        }
        validate_opaque_id(client_request_id, "clientRequestId")?;
        if let Some(quote) = quote_message_id {
            validate_opaque_id(quote, "quoteMessageId")?;
        }
        if let Some(existing) = self
            .store
            .message_by_client_request(account_id, client_request_id)?
        {
            return Ok(PreparedSend::Existing(Box::new(existing)));
        }
        let account = self.resolve_account(account_id)?;
        let conversation = match target {
            AttachmentSendTarget::Conversation(conversation_id) => {
                self.resolve_conversation(account_id, conversation_id)?
            }
            AttachmentSendTarget::Peer(peer) => {
                // contacts.list reports 'contact'; conversations use
                // 'direct'. Both are accepted for the same direct-chat target.
                let kind = match peer.kind {
                    "direct" | "contact" => "direct",
                    "group" => "group",
                    _ => {
                        return Err(ServiceError::Api(ApiError::new(
                            "INVALID_REQUEST",
                            "kind must be 'contact', 'direct', or 'group'",
                            false,
                        )));
                    }
                };
                validate_opaque_id(peer.peer_key, "peerKey")?;
                let peer_title = peer
                    .peer_title
                    .map(str::trim)
                    .filter(|title| !title.is_empty())
                    .map(|title| title.chars().take(64).collect::<String>());
                let title = peer_title.unwrap_or_else(|| {
                    if kind == "group" {
                        "group".to_string()
                    } else {
                        mask_address(peer.peer_key)
                    }
                });
                self.store
                    .ensure_conversation(account_id, kind, peer.peer_key, &title)?
            }
        };
        let data_uri = build_attachment_data_uri(data_base64, filename, content_type)?;
        // The pending row carries the same metadata-only descriptor the
        // renderer projects (contract 1.15 wire shape), so the desktop bubble
        // and conversation preview exist from the first pending tick instead
        // of collapsing into an invisible empty row.
        let descriptor = NormalizedAttachment {
            id: stable_hash_id(&[account_id, client_request_id, "attachment", "0"]),
            content_type: content_type.map(str::to_string),
            filename: filename.map(str::to_string),
            size: Some(size_bytes),
            width: None,
            height: None,
            is_voice_note: false,
        };
        self.dispatch_send(
            &account,
            &conversation,
            caption,
            client_request_id,
            quote_message_id,
            None,
            None,
            Some(vec![json!(data_uri)]),
            vec![descriptor],
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn dispatch_send(
        &self,
        account: &AccountRow,
        conversation: &ConversationRow,
        text: &str,
        client_request_id: &str,
        quote_message_id: Option<&str>,
        link_preview: Option<UpstreamSendPreview>,
        mentions: Option<Vec<UpstreamSendMention>>,
        attachments: Option<Vec<Value>>,
        attachment_descriptors: Vec<NormalizedAttachment>,
    ) -> Result<PreparedSend, ServiceError> {
        let account_id = account.id.as_str();
        let conversation_id = conversation.id.as_str();
        // Quote resolution is part of send validation and runs before the
        // pending row exists: a rejected quote leaves nothing behind, so the
        // same clientRequestId stays a fresh (re-validated) request.
        let quote = quote_message_id
            .map(|id| self.resolve_quote(account, conversation, id))
            .transpose()?;
        let pending_id =
            stable_hash_id(&[account_id, conversation_id, "outgoing", client_request_id]);
        let pending = MessageRecord {
            id: pending_id.clone(),
            account_id: account_id.to_string(),
            conversation_id: conversation_id.to_string(),
            direction: "outgoing",
            sender_id: account_id.to_string(),
            sender_name: None,
            mentions_self: false,
            sent_at: now_ms(),
            received_at: None,
            text: Some(text.to_string()),
            text_bytes: Some(text.len() as u32),
            text_truncated: false,
            text_retrievable: true,
            status: "pending",
            client_request_id: Some(client_request_id.to_string()),
            quote_message_id: quote_message_id.map(str::to_string),
            quote_snapshot: None,
            attachments: attachment_descriptors,
            edited_at: None,
            rich: None,
            reactions: Vec::new(),
            edits: Vec::new(),
            delivered_at: None,
            read_at: None,
            admin_deleted: false,
        };
        // Preview mirrors the visible row: the caption when present, else the
        // attachment filename — never an empty string for an attachment send.
        let preview = (!text.is_empty()).then(|| text.to_string()).or_else(|| {
            pending
                .attachments
                .iter()
                .find_map(|a| a.filename.as_deref().filter(|f| !f.is_empty()))
                .map(str::to_string)
        });
        let inserted = self.store.insert_message(
            &pending,
            Some(client_request_id),
            preview.as_deref(),
            false,
        )?;
        if !inserted {
            if let Some(existing) = self
                .store
                .message_by_client_request(account_id, client_request_id)?
            {
                return Ok(PreparedSend::Existing(Box::new(existing)));
            }
        }

        let params = Self::upstream_text_send_params(
            &account.signal_account,
            text,
            link_preview.as_ref(),
            mentions.as_deref(),
            attachments,
            quote,
            conversation,
        );
        Ok(PreparedSend::Dispatch {
            pending_id,
            account_id: account_id.to_string(),
            conversation_id: conversation_id.to_string(),
            params,
            pending_sent_at: pending.sent_at,
        })
    }

    /// Upstream JSON-RPC send params shared by first dispatch (`dispatch_send`)
    /// and same-row retry (`prepare_retry_text`) — one owner for the wire key
    /// names so the two paths cannot drift.
    ///
    /// Outbound link preview (contract §5.10 v1.26), projected 1:1 onto the
    /// signal-cli JSON-RPC send keys (verified against the pinned
    /// distribution: previewUrl/previewTitle/previewDescription plus an
    /// optional previewImage carrying a path or RFC 2397 data URI — the
    /// connector passes data URIs only, mirroring `attachments`).
    ///
    /// Outbound @mentions (contract revision 1.34) project onto the engine
    /// `send` `mentions` entry keys verbatim — `number`/`start`/`length`;
    /// validation and address resolution happened in `mentions_to_upstream`,
    /// so every entry that gets here is upstream-safe.
    ///
    /// signal-cli jsonRpc send accepts `attachments` entries as file paths or
    /// RFC 2397 data URIs (SendCommand --attachment, pinned 0.14.7); the
    /// connector passes data URIs only — bytes stay in memory, no
    /// caller-controlled path ever reaches upstream.
    ///
    /// signal-cli JSON-RPC send quote parameters (verified against the pinned
    /// 0.14.7 distribution): quoteTimestamp is the quoted message's Signal
    /// timestamp, quoteAuthor its author's number — both required.
    fn upstream_text_send_params(
        signal_account: &str,
        text: &str,
        preview: Option<&UpstreamSendPreview>,
        mentions: Option<&[UpstreamSendMention]>,
        attachments: Option<Vec<Value>>,
        quote: Option<(u64, String)>,
        conversation: &ConversationRow,
    ) -> Value {
        let mut params = json!({
            "account": signal_account,
            "message": text,
        });
        if let Some(link_preview) = preview {
            params["previewUrl"] = json!(link_preview.url);
            params["previewTitle"] = json!(link_preview.title);
            if let Some(description) = &link_preview.description {
                params["previewDescription"] = json!(description);
            }
            if let Some(image) = &link_preview.image_data_uri {
                params["previewImage"] = json!(image);
            }
        }
        if let Some(mentions) = mentions {
            // The engine `send` mention entry keys, verbatim (number/start/
            // length); offsets are UTF-16 code units passed through — the
            // caller indexes its own body.
            params["mentions"] = json!(
                mentions
                    .iter()
                    .map(|mention| {
                        json!({
                            "number": mention.number,
                            "start": mention.start,
                            "length": mention.length,
                        })
                    })
                    .collect::<Vec<_>>()
            );
        }
        if let Some(attachments) = attachments {
            params["attachments"] = Value::Array(attachments);
        }
        set_upstream_target(&mut params, conversation);
        if let Some((quote_timestamp, quote_author)) = quote {
            params["quoteTimestamp"] = json!(quote_timestamp);
            params["quoteAuthor"] = json!(quote_author);
        }
        params
    }

    /// Retry one definitively-failed outgoing text in place (contract 1.31,
    /// §4.26): the row addressed by its original clientRequestId is rearmed
    /// `failed -> pending` under a store-level guard and re-dispatched with
    /// upstream params rebuilt from the persisted record — a retry never
    /// mints a new clientRequestId, so the history keeps exactly one row per
    /// logical message and settlement (any terminal outcome) completes the
    /// SAME row the desktop bubble already renders.
    pub fn prepare_retry_text(
        &self,
        account_id: &str,
        conversation_id: &str,
        client_request_id: &str,
    ) -> Result<PreparedSend, ServiceError> {
        validate_opaque_id(client_request_id, "clientRequestId")?;
        let account = self.resolve_account(account_id)?;
        let conversation = self.resolve_conversation(account_id, conversation_id)?;
        let row = self
            .store
            .message_by_client_request(account_id, client_request_id)?
            .ok_or(ServiceError::Store(StoreError::MessageNotFound))?;
        if row.conversation_id != conversation.id {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "clientRequestId does not belong to this conversation",
                false,
            )));
        }
        if row.direction != "outgoing" {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "only outgoing messages can be retried",
                false,
            )));
        }
        let text = row.text.clone().unwrap_or_default();
        if text.is_empty() {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "retry supports text messages only",
                false,
            )));
        }
        if row.status == "pending" {
            return Err(ServiceError::Api(ApiError::new(
                "RETRY_IN_FLIGHT",
                "retry already in flight",
                false,
            )));
        }
        if row.status != "failed" {
            // 'unknown' is never retryable: its wire outcome was never
            // settled, so a blind resend could double-deliver.
            return Err(ServiceError::Api(ApiError::new(
                "RETRY_NOT_ALLOWED",
                "only a definitively failed send can be retried",
                false,
            )));
        }
        // Store-level rearm is the authority: `WHERE status='failed'` makes
        // the transition atomic, so a status push that settled the row
        // between the guard above and this write cannot be overwritten.
        let (rearmed, changed) = self
            .store
            .rearm_failed_message(&row.id)?
            .ok_or(ServiceError::Store(StoreError::MessageNotFound))?;
        if !changed {
            return Err(ServiceError::Api(ApiError::new(
                "RETRY_IN_FLIGHT",
                "retry already in flight",
                false,
            )));
        }
        let quote = row
            .quote_message_id
            .as_deref()
            .map(|quote_id| self.resolve_quote(&account, &conversation, quote_id))
            .transpose()?;
        // Mentions are not persisted on the row, so a retry rebuilds the send
        // without them (documented deviation: a retried mention send goes out
        // un-highlighted; the text arrives unchanged).
        let params = Self::upstream_text_send_params(
            &account.signal_account,
            &text,
            None,
            None,
            None,
            quote,
            &conversation,
        );
        Ok(PreparedSend::Dispatch {
            pending_id: row.id,
            account_id: account.id,
            conversation_id: conversation.id,
            params,
            pending_sent_at: rearmed.sent_at,
        })
    }

    /// Resolve a local quoteMessageId to the upstream (quoteTimestamp,
    /// quoteAuthor) pair. A quote targets a message in the same conversation;
    /// an unknown target is MESSAGE_NOT_FOUND, and a target whose Signal
    /// author/timestamp is not established is a deterministic INVALID_REQUEST
    /// — never a silently dropped or mis-addressed quote upstream.
    fn resolve_quote(
        &self,
        account: &AccountRow,
        conversation: &ConversationRow,
        quote_message_id: &str,
    ) -> Result<(u64, String), ServiceError> {
        let quoted = self
            .store
            .message_by_id(&account.id, &conversation.id, quote_message_id)?
            .ok_or(StoreError::MessageNotFound)?;
        let author = match quoted.direction {
            // We authored it: the author is the linked account itself. Only an
            // addressable send carries the upstream timestamp Signal quotes
            // match on; pending/failed/unknown rows would misquote.
            "outgoing" if outgoing_status_is_addressable(quoted.status) => {
                account.signal_account.clone()
            }
            // In a direct chat the only other possible author is the peer.
            // Incoming rows store the envelope timestamp, which is the
            // protocol identity a quote references.
            "incoming" if conversation.kind == "direct" => conversation.peer_key.clone(),
            // Group messages do not persist the member address (only a local
            // sender hash), and system rows have no author at all.
            _ => {
                return Err(ServiceError::Api(ApiError::new(
                    "INVALID_REQUEST",
                    "quoted message author is not resolvable",
                    false,
                )));
            }
        };
        Ok((quoted.sent_at, author))
    }

    /// Pure local validation for `messages.edit` (contract revision 1.15,
    /// upstream `send` with `editTimestamp`): resolve the target row exactly
    /// like `prepare_remote_delete` — only an outgoing row in the terminal
    /// state `sent` carries the upstream protocol identity an edit must
    /// reference — and build the exact upstream jsonRpc params.
    pub fn prepare_edit(
        &self,
        account_id: &str,
        conversation_id: &str,
        message_id: &str,
        text: &str,
    ) -> Result<PreparedEdit, ServiceError> {
        validate_opaque_id(account_id, "accountId")?;
        validate_opaque_id(conversation_id, "conversationId")?;
        validate_opaque_id(message_id, "messageId")?;
        validate_text(text)?;
        let (account, conversation, message) =
            self.resolve_target(account_id, conversation_id, message_id)?;
        if message.direction != "outgoing" || !outgoing_status_is_addressable(message.status) {
            return Err(StoreError::MessageNotFound.into());
        }
        // signal-cli `send` with editTimestamp (verified against the jsonRpc
        // surface: SendCommand dest `edit-timestamp` maps to the camelCase
        // jsonRpc key) retargets the earlier message; addressing matches send.
        let mut params = json!({
            "account": account.signal_account,
            "message": text,
            "editTimestamp": message.sent_at,
        });
        set_upstream_target(&mut params, &conversation);
        Ok(PreparedEdit {
            account_id: account_id.to_string(),
            conversation_id: conversation_id.to_string(),
            message_id: message_id.to_string(),
            text: text.to_string(),
            params,
        })
    }

    /// Settlement of a confirmed upstream edit (contract 1.15): replace the
    /// row's body and stamp `edited_at`; status is untouched.
    pub fn complete_edit_success(
        &self,
        account_id: &str,
        conversation_id: &str,
        message_id: &str,
        text: &str,
    ) -> Result<Option<MessageRecord>, ServiceError> {
        let text_bytes = text.len().min(u32::MAX as usize) as u32;
        let mut record = self.store.apply_outgoing_edit(
            message_id,
            account_id,
            conversation_id,
            text,
            text_bytes,
        )?;
        if let Some(record) = record.as_mut() {
            self.store
                .attach_reactions(account_id, std::slice::from_mut(record))?;
            self.store
                .attach_edits(account_id, std::slice::from_mut(record))?;
        }
        Ok(record)
    }

    /// Pure local validation for `messages.remoteDelete` (docs/remote-delete-l2-plan.md §3.2):
    /// resolve the target row and build the exact upstream jsonRpc params. Only an
    /// addressable outgoing row carries a real Signal protocol identity
    /// — its `sent_at` was overwritten with the send response's upstream timestamp by
    /// `complete_outgoing_send` and is preserved across delivery/read receipts
    /// (`outgoing_status_is_addressable`), the same precedent as quote resolution
    /// (`resolve_quote`). Pending/failed/unknown rows hold a local clock value there,
    /// so they are indistinguishable from a missing row and answer `MESSAGE_NOT_FOUND`.
    /// Nothing is written locally and no event is emitted: the connector does not
    /// mutate the local `messages` row on a remote delete (plan §3.6).
    pub fn prepare_remote_delete(
        &self,
        account_id: &str,
        conversation_id: &str,
        message_id: &str,
    ) -> Result<PreparedRemoteDelete, ServiceError> {
        validate_opaque_id(account_id, "accountId")?;
        validate_opaque_id(conversation_id, "conversationId")?;
        validate_opaque_id(message_id, "messageId")?;
        let (account, conversation, message) =
            self.resolve_target(account_id, conversation_id, message_id)?;
        if message.direction != "outgoing" || !outgoing_status_is_addressable(message.status) {
            return Err(StoreError::MessageNotFound.into());
        }
        // signal-cli JSON-RPC remoteDelete parameters (verified against the pinned
        // 0.14.7 distribution: RemoteDeleteCommand dests `target-timestamp` /
        // `recipient` / `group-id` / `note-to-self`, and JsonRpcNamespace maps
        // dash-separated dests to camelCase JSON keys): `targetTimestamp` is the
        // deleted message's Signal timestamp, `recipient`/`groupId` address the
        // receiving side — the same addressing shape as `send`.
        let mut params = json!({
            "account": account.signal_account,
            "targetTimestamp": message.sent_at,
        });
        set_upstream_target(&mut params, &conversation);
        Ok(PreparedRemoteDelete {
            account_id: account_id.to_string(),
            conversation_id: conversation_id.to_string(),
            message_id: message_id.to_string(),
            params,
        })
    }

    /// Pure local validation for `messages.sendReaction`
    /// (docs/remote-delete-l2-plan.md §3.2 shape, upstream `sendReaction`):
    /// resolve the target row and build the exact upstream jsonRpc params.
    /// A reaction targets a message that already exists in the local history,
    /// so both directions are addressable — an own outgoing row qualifies in
    /// an addressable state (`sent`/`delivered`/`read`; its `sent_at` is the
    /// upstream Signal timestamp preserved across receipts, same precedent as
    /// `resolve_quote`/`prepare_remote_delete`),
    /// an incoming row carries the envelope timestamp. The target author
    /// follows the direction: the linked account's own number for outgoing
    /// rows, the conversation peer for incoming rows. Outgoing rows without an
    /// addressable state carry no protocol identity and answer
    /// `MESSAGE_NOT_FOUND`; group incoming rows persist only a local sender
    /// hash, not the member's number, so their author is not resolvable
    /// (deterministic `INVALID_REQUEST`). Nothing is written locally and no
    /// event is emitted.
    pub fn prepare_send_reaction(
        &self,
        account_id: &str,
        conversation_id: &str,
        message_id: &str,
        emoji: &str,
        remove: bool,
    ) -> Result<PreparedSendReaction, ServiceError> {
        validate_opaque_id(account_id, "accountId")?;
        validate_opaque_id(conversation_id, "conversationId")?;
        validate_opaque_id(message_id, "messageId")?;
        validate_emoji(emoji)?;
        let (account, conversation, message) =
            self.resolve_target(account_id, conversation_id, message_id)?;
        // signal-cli JSON-RPC sendReaction parameters (verified against the
        // pinned 0.14.7 distribution: SendReactionCommand dests `emoji`
        // (required, "should be a single unicode grapheme cluster"),
        // `target-author`, `target-timestamp` (required Long), `remove`
        // (storeTrue) and `recipient`/`group-id`/`username`/`note-to-self`;
        // JsonRpcNamespace.get falls back to
        // Util.dashSeparatedToCamelCaseString, so jsonRpc carries camelCase
        // keys): `targetTimestamp` is the reacted-to message's Signal
        // timestamp, `targetAuthor` its author's number, and
        // `recipient`/`groupId` address the receiving side — the same
        // addressing shape as `send`/`remoteDelete`.
        let target_author = match message.direction {
            // We authored it: the author is the linked account itself. Only an
            // addressable send carries the upstream timestamp Signal reactions
            // match on; pending/failed/unknown rows would mis-target.
            "outgoing" if outgoing_status_is_addressable(message.status) => {
                account.signal_account.clone()
            }
            // An outgoing row without an addressable state carries no
            // upstream protocol identity (its sent_at is a local clock value)
            // and is indistinguishable from a missing row (remoteDelete
            // precedent).
            "outgoing" => return Err(StoreError::MessageNotFound.into()),
            // In a direct chat the only other possible author is the peer.
            // Incoming rows store the envelope timestamp, which is the
            // protocol identity a reaction references (same precedent as
            // quote resolution).
            "incoming" if conversation.kind == "direct" => conversation.peer_key.clone(),
            // Group messages do not persist the member address (only a local
            // sender hash), and system rows have no author at all: reacting
            // cannot be addressed upstream, so it fails deterministically
            // instead of mis-targeting (quote-resolution precedent).
            _ => {
                return Err(ServiceError::Api(ApiError::new(
                    "INVALID_REQUEST",
                    "reaction target author is not resolvable",
                    false,
                )));
            }
        };
        let mut params = json!({
            "account": account.signal_account,
            "emoji": emoji,
            "remove": remove,
            "targetAuthor": target_author,
            "targetTimestamp": message.sent_at,
        });
        set_upstream_target(&mut params, &conversation);
        Ok(PreparedSendReaction {
            account_id: account_id.to_string(),
            conversation_id: conversation_id.to_string(),
            message_id: message_id.to_string(),
            params,
            emoji: emoji.to_string(),
            remove,
            target_sent_at: message.sent_at,
        })
    }

    /// sendReaction 上行确认后的本地落库（对齐 edit 的 prepare/complete 两段
    /// 式）：本机回应以 account.id 为 actor、无显示名——与收件侧多设备 echo
    /// 同口径（contract 1.27），pill 由聚合投影自然带出 mine=true。返回的会话
    /// 摘要供调用方推 conversation.changed，app 即时刷新，不等轮询。
    #[allow(clippy::too_many_arguments)]
    pub fn complete_send_reaction(
        &self,
        account_id: &str,
        conversation_id: &str,
        emoji: &str,
        remove: bool,
        target_sent_at: u64,
    ) -> Result<Option<ConversationSummary>, ServiceError> {
        let account = self
            .store
            .account_by_id(account_id)?
            .ok_or(StoreError::MessageNotFound)?;
        self.store.upsert_reaction_event(
            account_id,
            conversation_id,
            emoji,
            target_sent_at,
            &account.id,
            remove,
            None,
        )?;
        Ok(self
            .store
            .conversation_summary(account_id, conversation_id)?)
    }

    /// The §4.6 reaction addressing model, reused verbatim by the pin family
    /// (contract 1.33): the upstream `targetAuthor` follows the row
    /// direction — the linked account's own number for addressable outgoing
    /// rows, the conversation peer for incoming direct rows. A group incoming
    /// row persists only a local sender hash and a system row has no author
    /// at all, so neither resolves and the request fails with the
    /// deterministic INVALID_REQUEST instead of mis-addressing upstream.
    fn resolve_pin_target(
        &self,
        account_id: &str,
        conversation_id: &str,
        message_id: &str,
    ) -> Result<(AccountRow, ConversationRow, MessageRecord, String), ServiceError> {
        validate_opaque_id(account_id, "accountId")?;
        validate_opaque_id(conversation_id, "conversationId")?;
        validate_opaque_id(message_id, "messageId")?;
        let (account, conversation, message) =
            self.resolve_target(account_id, conversation_id, message_id)?;
        let target_author = match message.direction {
            // We authored it: the author is the linked account itself. Only
            // an addressable send carries the upstream timestamp the pin
            // family matches on; pending/failed/unknown rows would
            // mis-target.
            "outgoing" if outgoing_status_is_addressable(message.status) => {
                account.signal_account.clone()
            }
            // An outgoing row without an addressable state carries no
            // upstream protocol identity (its sent_at is a local clock value)
            // and is indistinguishable from a missing row (remoteDelete
            // precedent).
            "outgoing" => return Err(StoreError::MessageNotFound.into()),
            // In a direct chat the only other possible author is the peer.
            // Incoming rows store the envelope timestamp, which is the
            // protocol identity the pin family references.
            "incoming" if conversation.kind == "direct" => conversation.peer_key.clone(),
            // Group messages do not persist the member address, and system
            // rows have no author at all.
            _ => {
                return Err(ServiceError::Api(ApiError::new(
                    "INVALID_REQUEST",
                    "pin target author is not resolvable",
                    false,
                )));
            }
        };
        Ok((account, conversation, message, target_author))
    }

    /// messages.sendPinMessage (contract 1.33): resolve the target with the
    /// reaction addressing model and build the exact upstream jsonRpc
    /// params. Groups map to the engine `groupId` form, direct chats to the
    /// `recipient` array; `pinDurationSeconds` is passed through when the
    /// caller supplied one (absent = the official forever pin — the key is
    /// not sent).
    pub fn prepare_send_pin_message(
        &self,
        account_id: &str,
        conversation_id: &str,
        message_id: &str,
        pin_duration_seconds: Option<u32>,
    ) -> Result<PreparedPin, ServiceError> {
        let (account, conversation, message, target_author) =
            self.resolve_pin_target(account_id, conversation_id, message_id)?;
        let mut params = json!({
            "account": account.signal_account,
            "targetAuthor": target_author,
            "targetTimestamp": message.sent_at,
        });
        set_upstream_target(&mut params, &conversation);
        if let Some(seconds) = pin_duration_seconds {
            params["pinDurationSeconds"] = json!(seconds);
        }
        Ok(PreparedPin {
            account_id: account_id.to_string(),
            conversation_id: conversation_id.to_string(),
            message_id: message_id.to_string(),
            params,
            target_author,
            target_sent_at: message.sent_at,
            pin_duration_seconds,
        })
    }

    /// messages.sendUnpinMessage (contract 1.33): same addressing as the
    /// pin, no duration.
    pub fn prepare_send_unpin_message(
        &self,
        account_id: &str,
        conversation_id: &str,
        message_id: &str,
    ) -> Result<PreparedPin, ServiceError> {
        let (account, conversation, message, target_author) =
            self.resolve_pin_target(account_id, conversation_id, message_id)?;
        let mut params = json!({
            "account": account.signal_account,
            "targetAuthor": target_author,
            "targetTimestamp": message.sent_at,
        });
        set_upstream_target(&mut params, &conversation);
        Ok(PreparedPin {
            account_id: account_id.to_string(),
            conversation_id: conversation_id.to_string(),
            message_id: message_id.to_string(),
            params,
            target_author,
            target_sent_at: message.sent_at,
            pin_duration_seconds: None,
        })
    }

    /// messages.sendAdminDelete (contract 1.33): group conversations only —
    /// direct chats have no admin concept and answer INVALID_REQUEST before
    /// any upstream call. Admin eligibility against group state is not
    /// pre-checked: the server is the authority and rejects non-admin
    /// attempts, which surface as the upstream rejection (official behavior:
    /// send and show the failure).
    pub fn prepare_send_admin_delete(
        &self,
        account_id: &str,
        conversation_id: &str,
        message_id: &str,
    ) -> Result<PreparedPin, ServiceError> {
        let (account, conversation, message, target_author) =
            self.resolve_pin_target(account_id, conversation_id, message_id)?;
        if conversation.kind != "group" {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "admin delete applies to group conversations only",
                false,
            )));
        }
        let mut params = json!({
            "account": account.signal_account,
            "targetAuthor": target_author,
            "targetTimestamp": message.sent_at,
        });
        set_upstream_target(&mut params, &conversation);
        Ok(PreparedPin {
            account_id: account_id.to_string(),
            conversation_id: conversation_id.to_string(),
            message_id: message_id.to_string(),
            params,
            target_author,
            target_sent_at: message.sent_at,
            pin_duration_seconds: None,
        })
    }

    /// sendPinMessage 上行确认后的本地落库（reaction 先例的 complete 段）：
    /// 本机置顶以发送方口径写入每会话 pinned 状态（新 pin 替换旧 pin，
    /// 过期时间用 connector 时钟计算），并返回会话摘要供调用方推
    /// conversation.changed——重载窗口由此免重放即可渲染置顶条。
    pub fn complete_send_pin_message(
        &self,
        account_id: &str,
        conversation_id: &str,
        target_author: &str,
        target_sent_at: u64,
        pin_duration_seconds: Option<u32>,
    ) -> Result<Option<ConversationSummary>, ServiceError> {
        self.store.upsert_conversation_pin(
            account_id,
            conversation_id,
            target_author,
            target_sent_at,
            pin_duration_seconds,
        )?;
        Ok(self
            .store
            .conversation_summary(account_id, conversation_id)?)
    }

    /// sendUnpinMessage 上行确认后的本地落库：仅当本地 pinned 状态仍指向
    /// 被解 pin 的 (author, timestamp) 时清除；不匹配的 unpin 不动更新的
    /// pin。清除发生时返回会话摘要供调用方推 conversation.changed。
    pub fn complete_send_unpin_message(
        &self,
        account_id: &str,
        conversation_id: &str,
        target_author: &str,
        target_sent_at: u64,
    ) -> Result<Option<ConversationSummary>, ServiceError> {
        if !self.store.clear_conversation_pin(
            account_id,
            conversation_id,
            target_author,
            target_sent_at,
        )? {
            return Ok(None);
        }
        Ok(self
            .store
            .conversation_summary(account_id, conversation_id)?)
    }

    /// messages.markRead (contract revision 1.34, §4.29): select the
    /// receipt-eligible incoming rows of one conversation and group their
    /// upstream timestamps by author — one engine `sendReadReceipt` per
    /// author, the official per-author fan-out (the engine chunks 100 per
    /// envelope). No local row changes — the desktop owns its read state —
    /// so there is no complete step; an empty group list is the trivial
    /// no-op the supervisor answers `{"status":"sent"}` for.
    pub fn prepare_mark_read(
        &self,
        account_id: &str,
        conversation_id: &str,
        message_ids: Option<Vec<String>>,
    ) -> Result<PreparedReceipts, ServiceError> {
        self.prepare_receipt(account_id, conversation_id, message_ids)
    }

    /// messages.markViewed: identical addressing, selection, and grouping,
    /// upstream `sendViewedReceipt`.
    pub fn prepare_mark_viewed(
        &self,
        account_id: &str,
        conversation_id: &str,
        message_ids: Option<Vec<String>>,
    ) -> Result<PreparedReceipts, ServiceError> {
        self.prepare_receipt(account_id, conversation_id, message_ids)
    }

    /// Shared mark-read/mark-viewed body. Validation runs in the 1.33
    /// `resolve_pin_target` order — ids, account, conversation, then the
    /// bounded selection — so a bogus address answers its deterministic
    /// NOT_FOUND before any store scan. Author resolution follows the
    /// reaction addressing model read-side: a direct conversation's incoming
    /// rows are all the peer's; group rows persist only a local sender hash
    /// (§4.28), so their author is unknown and they are skipped from the
    /// fan-out — skipped, not an error (spec: rows whose author cannot be
    /// resolved are skipped).
    fn prepare_receipt(
        &self,
        account_id: &str,
        conversation_id: &str,
        message_ids: Option<Vec<String>>,
    ) -> Result<PreparedReceipts, ServiceError> {
        validate_opaque_id(account_id, "accountId")?;
        validate_opaque_id(conversation_id, "conversationId")?;
        let account = self.resolve_account(account_id)?;
        let conversation = self.resolve_conversation(account_id, conversation_id)?;
        let rows = match message_ids {
            Some(ids) => {
                for id in ids.iter().take(MARK_RECEIPT_MESSAGE_IDS_LIMIT) {
                    validate_opaque_id(id, "messageId")?;
                }
                // Explicit lists beyond the cap are truncated, not rejected:
                // a receipt marks what it can, and the bound keeps one
                // request's fan-out bounded (the schema guards the same 512
                // earlier).
                let bounded: Vec<String> = ids
                    .into_iter()
                    .take(MARK_RECEIPT_MESSAGE_IDS_LIMIT)
                    .collect();
                self.store
                    .incoming_rows_by_ids(account_id, conversation_id, &bounded)?
            }
            None => self.store.incoming_rows_for_receipts(
                account_id,
                conversation_id,
                MARK_RECEIPT_MESSAGE_IDS_LIMIT as u32,
            )?,
        };
        let groups = if conversation.kind == "direct" {
            // Rows arrive in receipt order (`sent_at ASC, id ASC`) from both
            // store paths, so per-author timestamps stay ascending; same-
            // timestamp rows collapse (duplicates are protocol-idempotent,
            // but one entry per upstream timestamp is what the official
            // desktop sends).
            let mut timestamps = rows
                .into_iter()
                .map(|row| row.sent_at)
                .collect::<Vec<u64>>();
            timestamps.dedup();
            if timestamps.is_empty() {
                Vec::new()
            } else {
                vec![ReceiptGroup {
                    recipient: conversation.peer_key.clone(),
                    timestamps,
                }]
            }
        } else {
            Vec::new()
        };
        Ok(PreparedReceipts {
            account_id: account.id,
            conversation_id: conversation.id,
            account_signal: account.signal_account,
            groups,
        })
    }

    /// messages.attachments.open (ADR 0002, contract revision 1.17): resolve
    /// the addressed rows, then locate the already-downloaded file in the
    /// engine's data directory and mint a short-lived chunk-stream handle for
    /// it. The addressing resolves before any file is touched, the id passes
    /// the sanitizeId-parity gate before any path join, and a missing file
    /// answers UPSTREAM_ERROR — not-downloaded and governor-deleted are
    /// deliberately indistinguishable (ADR 0002).
    pub fn open_media_handle(
        &self,
        account_id: &str,
        conversation_id: &str,
        message_id: &str,
        attachment_id: &str,
    ) -> Result<MediaOpenView, ServiceError> {
        let Some(media) = self.media.as_ref() else {
            return Err(media_capability_error());
        };
        validate_opaque_id(account_id, "accountId")?;
        validate_opaque_id(conversation_id, "conversationId")?;
        validate_opaque_id(message_id, "messageId")?;
        if attachment_id.is_empty() || attachment_id.len() > MAX_ATTACHMENT_ID_BYTES {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "attachmentId must contain between 1 and 128 bytes",
                false,
            )));
        }
        let sanitized_id = sanitize_attachment_id(attachment_id)?;
        let (_account, _conversation, _message) =
            self.resolve_target(account_id, conversation_id, message_id)?;
        let Some((path, size_bytes)) = media.locate_attachment(&sanitized_id) else {
            return Err(ServiceError::Api(ApiError::new(
                "UPSTREAM_ERROR",
                "attachment is not available in the signal-cli data directory",
                true,
            )));
        };
        let media_handle = media
            .handles
            .open(
                account_id,
                conversation_id,
                message_id,
                attachment_id,
                path,
                size_bytes,
            )
            .map_err(map_media_handle_error)?;
        Ok(MediaOpenView {
            media_handle,
            size_bytes,
            chunk_bytes: MEDIA_CHUNK_BYTES,
        })
    }

    /// messages.attachments.readChunk (ADR 0002): deliver exactly one bounded
    /// chunk of one live handle at a strictly sequential offset. The handle
    /// table validates origin, TTL, and offset; this layer base64-encodes the
    /// raw chunk (≤ 4 * ceil(262_144 / 3) = 349_528 characters, far inside
    /// the 160 MiB host frame budget) and reports this chunk's byte count
    /// plus the EOF flag that terminates the caller's loop.
    pub fn read_media_chunk(
        &self,
        media_handle: &str,
        offset: u64,
    ) -> Result<MediaChunkView, ServiceError> {
        let Some(media) = self.media.as_ref() else {
            return Err(media_capability_error());
        };
        validate_opaque_id(media_handle, "mediaHandle")?;
        let chunk = media
            .handles
            .read_chunk(media_handle, offset)
            .map_err(map_media_handle_error)?;
        Ok(MediaChunkView {
            data_base64: base64_encode(&chunk.data),
            size_bytes: chunk.data.len() as u64,
            eof: chunk.eof,
        })
    }

    /// messages.attachments.closeHandle (ADR 0002): explicit early release.
    /// Idempotent — a handle the TTL already reaped still closes cleanly
    /// (`released: false`) instead of turning the backstop into a race the
    /// caller must survive.
    pub fn close_media_handle(&self, media_handle: &str) -> Result<MediaCloseView, ServiceError> {
        let Some(media) = self.media.as_ref() else {
            return Err(media_capability_error());
        };
        validate_opaque_id(media_handle, "mediaHandle")?;
        Ok(MediaCloseView {
            released: media.handles.close(media_handle),
        })
    }

    /// ADR 0002 gate 3: handles cannot outlive their account session. The
    /// delete-local-data path calls this once the account rows are gone; the
    /// 300 s TTL backstops every other kind of abandonment.
    pub fn clear_media_handles_for_account(&self, account_id: &str) {
        if let Some(media) = self.media.as_ref() {
            media.handles.clear_account(account_id);
        }
    }

    pub fn complete_send_success(
        &self,
        pending_id: &str,
        account_id: &str,
        conversation_id: &str,
        sent_at: u64,
    ) -> Result<(MessageRecord, Vec<HostSideEvent>), ServiceError> {
        // A missing pending row at this point means the local state vanished
        // underneath a send that already completed upstream (e.g. the account's
        // rows were deleted in a race): the message may well be delivered, so
        // the outcome is final and must never be auto-retried. The schema has
        // no dedicated code for this; the closest existing one is
        // SEND_OUTCOME_UNKNOWN with retryable=false.
        let mut updated = self
            .store
            .complete_outgoing_send(pending_id, account_id, conversation_id, sent_at)?
            .ok_or_else(|| {
                ServiceError::Api(ApiError::new(
                    "SEND_OUTCOME_UNKNOWN",
                    "send completed upstream but the local pending record is gone",
                    false,
                ))
            })?;
        self.store
            .attach_reactions(account_id, std::slice::from_mut(&mut updated.0))?;
        let (updated, status_transitioned) = updated;
        let mut events = vec![HostSideEvent::MessageUpserted(project_message_for_host(
            updated.clone(),
        ))];
        if status_transitioned {
            events.push(status_changed_event(&updated));
        }
        if let Some(conversation) = self
            .store
            .conversation_summary(account_id, conversation_id)?
        {
            events.push(HostSideEvent::ConversationChanged(conversation));
        }
        if let Some(account) = self.store.account_summary(account_id)? {
            events.push(HostSideEvent::AccountChanged(account));
        }
        Ok((updated, events))
    }

    pub fn complete_send_unknown(
        &self,
        pending_id: &str,
    ) -> Result<Vec<HostSideEvent>, ServiceError> {
        Ok(status_change_events(
            self.store
                .update_message_status(pending_id, "unknown", None)?,
        ))
    }

    /// Contract 1.21: the engine classified an upstream authorization failure
    /// for `signal_account` — the account holder unlinked this device. Persist
    /// the dead state and surface it as an `account.changed` event so the
    /// host can auto-release stale session bindings. Unknown numbers (row
    /// already deleted) are a no-op.
    pub fn mark_account_device_unlinked(
        &self,
        signal_account: &str,
    ) -> Result<Vec<HostSideEvent>, ServiceError> {
        let Some(account) = self.store.mark_account_device_unlinked(signal_account)? else {
            return Ok(Vec::new());
        };
        Ok(vec![HostSideEvent::AccountChanged(account)])
    }

    pub fn complete_send_failed(
        &self,
        pending_id: &str,
    ) -> Result<Vec<HostSideEvent>, ServiceError> {
        Ok(status_change_events(
            self.store
                .update_message_status(pending_id, "failed", None)?,
        ))
    }

    /// Persist one normalized receive from ONE group's engine. The owning
    /// group scopes the single-account fallback: signal-cli may omit
    /// `account`, and with several engines the fallback must never cross
    /// group boundaries.
    pub fn ingest_receive(
        &mut self,
        receive: NormalizedReceive,
        owner_group: &str,
    ) -> Result<Vec<HostSideEvent>, ServiceError> {
        if receive.direction == "skip" {
            return Ok(Vec::new());
        }
        // Any control-carrying receive routes to the control plane — not just
        // direction=="control": a peer's envelope-level editMessage arrives as
        // direction "incoming" with control Edit and must edit the original
        // row, never insert a second message.
        if receive.control.is_some() {
            return self.ingest_control_receive(receive, owner_group);
        }
        if !matches!(receive.direction, "incoming" | "outgoing" | "system") {
            return Ok(Vec::new());
        }
        // Signal's timestamp is part of the protocol identity. Inventing one locally would
        // turn a replay into a second message after a restart or receive retry.
        let Some(sent_at) = receive.timestamp else {
            return Ok(Vec::new());
        };
        // signal-cli may omit `account` on single-account jsonRpc; fall back to the
        // sole account bound to the receiving engine's own group.
        let signal_account = match receive.account.as_deref().filter(|s| !s.is_empty()) {
            Some(account) => account.to_string(),
            None => {
                let accounts = self.store.list_accounts_in_group(owner_group)?;
                if accounts.len() != 1 {
                    return Ok(Vec::new());
                }
                match self.store.account_by_id(&accounts[0].id)? {
                    Some(row) => row.signal_account,
                    None => return Ok(Vec::new()),
                }
            }
        };
        let account = self
            .store
            .upsert_account_from_signal(&signal_account, None, owner_group)?;
        let (kind, peer_key, title) = if let Some(group_id) = receive.group_id.as_deref() {
            ("group", group_id.to_string(), "group".to_string())
        } else if let Some(peer) = receive.source.as_deref().filter(|s| !s.is_empty()) {
            // Prefer profile/contact label over masked peer id (e.g. 8fc***e2).
            let label = receive
                .peer_name
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .unwrap_or_else(|| mask_address(peer));
            ("direct", peer.to_string(), label)
        } else {
            return Ok(Vec::new());
        };
        let conversation = self
            .store
            .ensure_conversation(&account.id, kind, &peer_key, &title)?;
        let direction = receive.direction;
        let legacy_sender_id = stable_hash_id(&[&account.id, kind, &peer_key]);
        let sender_id = if direction == "outgoing" {
            account.id.clone()
        } else {
            stable_hash_id(&[
                &account.id,
                kind,
                receive.source.as_deref().unwrap_or(&peer_key),
            ])
        };
        if self
            .store
            .message_by_signal_identity(
                &account.id,
                &conversation.id,
                direction,
                sent_at,
                &sender_id,
                if direction == "outgoing" {
                    "self"
                } else {
                    &legacy_sender_id
                },
            )?
            .is_some()
        {
            return Ok(Vec::new());
        }
        let message_id = stable_hash_id(&[
            "signal-message-v2",
            &account.id,
            &conversation.id,
            direction,
            &sent_at.to_string(),
            &sender_id,
        ]);
        let text_bytes = receive.text_bytes.or_else(|| {
            receive
                .text
                .as_ref()
                .map(|text| text.len().min(u32::MAX as usize) as u32)
        });
        let text_complete = !receive.text_truncated
            && text_bytes.is_none_or(|bytes| bytes as usize <= MAX_INBOUND_TEXT_BYTES);
        let stored_text = receive.text.map(|text| {
            if text_complete {
                text
            } else {
                truncate_utf8_bytes(&text, MAX_HOST_TEXT_PREVIEW_BYTES)
            }
        });
        let status = match direction {
            "outgoing" => "sent",
            "system" => "system",
            _ => "delivered",
        };
        // Contract 1.29: a row @mentions the linked account when one of its
        // normalized mention authors resolves to the account's own number.
        // The match is number-based because the pinned upstream jsonRpc
        // surface exposes the account only by number (`listAccounts` returns
        // `{number}`; the account UUID is not queryable), so a mention author
        // carrying only a UUID cannot be attributed to self — recorded as the
        // revision's known boundary. Mention authors prefer the resolved
        // number (`number` first, `uuid` fallback), so the common group case
        // matches. Computed before the record consumes the rich payload.
        let mentions_self = direction == "incoming"
            && receive
                .rich
                .as_ref()
                .is_some_and(|rich| rich.mentions.iter().any(|m| m.author == signal_account));
        let message = MessageRecord {
            id: message_id,
            account_id: account.id.clone(),
            conversation_id: conversation.id.clone(),
            direction,
            sender_id,
            // Contract 1.27: the incoming author's display name, captured from
            // the envelope `sourceName` (already bounded by the engine).
            // Outgoing rows are the account itself; system rows have no author.
            sender_name: if direction == "incoming" {
                receive.peer_name.clone()
            } else {
                None
            },
            mentions_self,
            sent_at,
            received_at: Some(now_ms()),
            text: stored_text,
            text_bytes,
            text_truncated: !text_complete,
            text_retrievable: text_complete,
            status,
            client_request_id: None,
            quote_message_id: None,
            quote_snapshot: receive.quote,
            attachments: receive.attachments,
            rich: receive.rich,
            edited_at: None,
            reactions: Vec::new(),
            edits: Vec::new(),
            delivered_at: None,
            read_at: None,
            admin_deleted: false,
        };
        let preview = message
            .text
            .as_deref()
            .map(|text| text.chars().take(120).collect::<String>());
        let increment_unread = direction == "incoming";
        let inserted =
            self.store
                .insert_message(&message, None, preview.as_deref(), increment_unread)?;
        if !inserted {
            return Ok(Vec::new());
        }
        let mut events = vec![HostSideEvent::MessageUpserted(project_message_for_host(
            message,
        ))];
        if let Some(conversation) = self
            .store
            .conversation_summary(&account.id, &conversation.id)?
        {
            events.push(HostSideEvent::ConversationChanged(conversation));
        }
        if let Some(account) = self.store.account_summary(&account.id)? {
            events.push(HostSideEvent::AccountChanged(account));
        }
        Ok(events)
    }

    /// Persist-and-notify one control-plane receive (contract 1.15): reaction
    /// upserts, remote-delete marks, edits, and rate-limited typing. Control
    /// receives never create a conversation, never create a message row, and
    /// are dropped when they address nothing local.
    fn ingest_control_receive(
        &mut self,
        receive: NormalizedReceive,
        owner_group: &str,
    ) -> Result<Vec<HostSideEvent>, ServiceError> {
        let Some(control) = receive.control else {
            return Ok(Vec::new());
        };
        // Route to the account the same way messages do.
        let signal_account = match receive.account.as_deref().filter(|s| !s.is_empty()) {
            Some(account) => account.to_string(),
            None => {
                let accounts = self.store.list_accounts_in_group(owner_group)?;
                if accounts.len() != 1 {
                    return Ok(Vec::new());
                }
                match self.store.account_by_id(&accounts[0].id)? {
                    Some(row) => row.signal_account,
                    None => return Ok(Vec::new()),
                }
            }
        };
        let account = match self.store.account_by_signal(&signal_account).ok().flatten() {
            Some(account) => account,
            None => return Ok(Vec::new()),
        };
        // Resolve the conversation: group id, or peer (incoming source /
        // outgoing destination). Control receives address existing
        // conversations only — they never create one.
        let (kind, peer_key) = if let Some(group_id) = receive.group_id.as_deref() {
            ("group", group_id)
        } else if let Some(peer) = receive.source.as_deref().filter(|s| !s.is_empty()) {
            ("direct", peer)
        } else {
            return Ok(Vec::new());
        };
        let Ok(conversation) = self.store.conversation_by_peer(&account.id, kind, peer_key) else {
            return Ok(Vec::new());
        };
        let Some(conversation) = conversation else {
            return Ok(Vec::new());
        };
        match control {
            ControlReceive::Reaction {
                emoji,
                target_author,
                target_timestamp,
                remove,
            } => {
                // The actor is the envelope sender; the protocol's
                // targetAuthor cross-check is advisory (a group member
                // reacting to another member's message still keys on the
                // reacting actor). The display name rides the same envelope
                // (contract 1.27); own multi-device echoes are the account
                // itself and carry no name.
                let _ = target_author;
                let actor_id = match receive.direction {
                    "outgoing" => account.id.clone(),
                    _ => stable_hash_id(&[
                        &account.id,
                        kind,
                        receive.source.as_deref().unwrap_or(peer_key),
                    ]),
                };
                let actor_name = if receive.direction == "outgoing" {
                    None
                } else {
                    receive.peer_name.as_deref()
                };
                self.store.upsert_reaction_event(
                    &account.id,
                    &conversation.id,
                    &emoji,
                    target_timestamp,
                    &actor_id,
                    remove,
                    actor_name,
                )?;
                let mut events = Vec::new();
                if let Some(summary) = self
                    .store
                    .conversation_summary(&account.id, &conversation.id)?
                {
                    events.push(HostSideEvent::ConversationChanged(summary));
                }
                Ok(events)
            }
            ControlReceive::RemoteDelete { target_timestamp } => {
                match self.store.mark_remote_deleted(
                    &account.id,
                    &conversation.id,
                    target_timestamp,
                )? {
                    Some((record, true)) => Ok(vec![HostSideEvent::MessageStatusChanged {
                        account_id: record.account_id,
                        message_id: record.id,
                        status: record.status,
                        delivered_at: None,
                        read_at: None,
                    }]),
                    _ => Ok(Vec::new()),
                }
            }
            ControlReceive::Edit { target_timestamp } => {
                let Some(new_text) = receive.text else {
                    return Ok(Vec::new());
                };
                let new_bytes = new_text.len().min(u32::MAX as usize) as u32;
                match receive.direction {
                    // Peer edit: keyed on the upstream target timestamp.
                    "incoming" => {
                        let sender_id = stable_hash_id(&[
                            &account.id,
                            kind,
                            receive.source.as_deref().unwrap_or(peer_key),
                        ]);
                        match self.store.apply_inbound_edit(
                            &account.id,
                            &conversation.id,
                            target_timestamp,
                            &sender_id,
                            &legacy_sender_id(kind, peer_key, &account.id),
                            &new_text,
                            new_bytes,
                        )? {
                            Some(mut record) => {
                                self.store.attach_reactions(
                                    &account.id,
                                    std::slice::from_mut(&mut record),
                                )?;
                                self.store
                                    .attach_edits(&account.id, std::slice::from_mut(&mut record))?;
                                Ok(vec![HostSideEvent::MessageUpserted(record)])
                            }
                            None => Ok(Vec::new()),
                        }
                    }
                    // Our own multi-device edit of a phone-sent message: the
                    // sync mirror targets the row by its upstream timestamp
                    // through the same store path.
                    _ => {
                        match self.store.apply_outgoing_edit_by_signal(
                            &account.id,
                            &conversation.id,
                            target_timestamp,
                            &new_text,
                            new_bytes,
                        )? {
                            Some(mut record) => {
                                self.store.attach_reactions(
                                    &account.id,
                                    std::slice::from_mut(&mut record),
                                )?;
                                self.store
                                    .attach_edits(&account.id, std::slice::from_mut(&mut record))?;
                                Ok(vec![HostSideEvent::MessageUpserted(record)])
                            }
                            None => Ok(Vec::new()),
                        }
                    }
                }
            }
            ControlReceive::Typing { action } => {
                let key = (account.id.clone(), conversation.id.clone());
                let now = now_ms();
                if self.typing_last_emission.len() >= TYPING_RATE_MAP_CAPACITY
                    && !self.typing_last_emission.contains_key(&key)
                {
                    // Bounded map: drop the oldest entry (a linear scan of a
                    // 256-entry map is cheaper than tracking an order
                    // structure for a limiter that mostly sees fresh keys).
                    if let Some(oldest) = self
                        .typing_last_emission
                        .iter()
                        .min_by_key(|(_, at)| **at)
                        .map(|(key, _)| key.clone())
                    {
                        self.typing_last_emission.remove(&oldest);
                    }
                }
                let last = self.typing_last_emission.get(&key).copied();
                if last.is_some_and(|at| now.saturating_sub(at) < TYPING_RATE_LIMIT_MS) {
                    return Ok(Vec::new());
                }
                self.typing_last_emission.insert(key, now);
                Ok(vec![HostSideEvent::ConversationTyping {
                    account_id: account.id,
                    conversation_id: conversation.id,
                    action,
                }])
            }
            ControlReceive::Receipt {
                kind,
                timestamps,
                when,
            } => {
                // Contract 1.32: the receipt's arrival instant (envelope
                // timestamp; connector clock as fallback) stamps the rows the
                // receipt moves, and each status event carries the stamp it
                // just wrote.
                let when = when.unwrap_or_else(crate::link::now_ms);
                let moved = self.store.upgrade_outgoing_receipts(
                    &account.id,
                    &conversation.id,
                    &timestamps,
                    kind,
                    when,
                )?;
                Ok(moved
                    .into_iter()
                    .map(|record| HostSideEvent::MessageStatusChanged {
                        account_id: record.account_id,
                        message_id: record.id,
                        status: record.status,
                        delivered_at: record.delivered_at,
                        read_at: record.read_at,
                    })
                    .collect())
            }
            ControlReceive::PinMessage {
                target_author,
                target_timestamp,
                duration_seconds,
            } => {
                // Contract 1.33: the derived per-conversation pinned state —
                // a newer pin replaces the older one, the expiry is connector
                // clock computed, and the host refreshes the pin bar through
                // the summary that rides conversation.changed (the reaction
                // notification precedent).
                self.store.upsert_conversation_pin(
                    &account.id,
                    &conversation.id,
                    &target_author,
                    target_timestamp,
                    duration_seconds,
                )?;
                let mut events = Vec::new();
                if let Some(summary) = self
                    .store
                    .conversation_summary(&account.id, &conversation.id)?
                {
                    events.push(HostSideEvent::ConversationChanged(summary));
                }
                Ok(events)
            }
            ControlReceive::UnpinMessage {
                target_author,
                target_timestamp,
            } => {
                // Only the pinned (author, timestamp) the unpin names clears;
                // a stale or mismatched unpin leaves a newer pin alone and
                // stays silent (replays never re-notify).
                if !self.store.clear_conversation_pin(
                    &account.id,
                    &conversation.id,
                    &target_author,
                    target_timestamp,
                )? {
                    return Ok(Vec::new());
                }
                let mut events = Vec::new();
                if let Some(summary) = self
                    .store
                    .conversation_summary(&account.id, &conversation.id)?
                {
                    events.push(HostSideEvent::ConversationChanged(summary));
                }
                Ok(events)
            }
            ControlReceive::AdminDelete {
                target_author,
                target_timestamp,
            } => {
                // Contract 1.33: the official "deleted by admin" tombstone is
                // a row-level marker — the status ladder is untouched and the
                // body stays for the audit window (retention prunes the row
                // normally). The host re-renders the row through the
                // upserted record; if the removed message is also the pinned
                // one, the pin state clears and the summary refreshes.
                let mut events = Vec::new();
                if let Some((mut record, true)) = self.store.mark_admin_deleted(
                    &account.id,
                    &conversation.id,
                    target_timestamp,
                )? {
                    self.store
                        .attach_reactions(&account.id, std::slice::from_mut(&mut record))?;
                    self.store
                        .attach_edits(&account.id, std::slice::from_mut(&mut record))?;
                    events.push(HostSideEvent::MessageUpserted(project_message_for_host(
                        record,
                    )));
                }
                if self.store.clear_conversation_pin(
                    &account.id,
                    &conversation.id,
                    &target_author,
                    target_timestamp,
                )? {
                    if let Some(summary) = self
                        .store
                        .conversation_summary(&account.id, &conversation.id)?
                    {
                        events.push(HostSideEvent::ConversationChanged(summary));
                    }
                }
                Ok(events)
            }
        }
    }
}

// Store-backed read paths below: the single implementation behind the
// matching [`ConnectorService`] methods and the supervisor's `spawn_blocking`
// lane over the shared `Arc<Store>` (no service lock involved).

pub(crate) fn store_list_conversations(
    store: &Store,
    account_id: &str,
    limit: u32,
    cursor: Option<&str>,
) -> Result<Page<ConversationSummary>, ServiceError> {
    if store.account_by_id(account_id)?.is_none() {
        return Err(ServiceError::Store(StoreError::AccountNotFound));
    }
    Ok(store.list_conversations(account_id, limit, cursor)?)
}

pub(crate) fn store_list_contacts(
    store: &Store,
    account_id: &str,
    query: Option<&str>,
    limit: u32,
    cursor: Option<&str>,
) -> Result<Page<ContactSummary>, ServiceError> {
    if store.account_by_id(account_id)?.is_none() {
        return Err(ServiceError::Store(StoreError::AccountNotFound));
    }
    let query = query.map(str::trim).filter(|value| !value.is_empty());
    if let Some(query) = query {
        validate_opaque_id(query, "query")?;
    }
    Ok(store.list_contacts(account_id, query, limit, cursor)?)
}

pub(crate) fn store_get_group(
    store: &Store,
    account_id: &str,
    group_key: &str,
) -> Result<GroupDetails, ServiceError> {
    validate_opaque_id(account_id, "accountId")?;
    validate_opaque_id(group_key, "groupKey")?;
    if store.account_by_id(account_id)?.is_none() {
        return Err(ServiceError::Store(StoreError::AccountNotFound));
    }
    let Some((title, extra, synced_at)) = store.contact_by_peer(account_id, "group", group_key)?
    else {
        return Err(ServiceError::Api(ApiError::new(
            "GROUP_NOT_FOUND",
            "no cached group row for this accountId and groupKey",
            false,
        )));
    };
    let member_count = extra
        .as_deref()
        .and_then(|extra| serde_json::from_str::<Value>(extra).ok())
        .and_then(|extra| extra.get("memberCount").and_then(Value::as_u64));
    Ok(GroupDetails {
        peer_key: group_key.to_string(),
        title,
        member_count,
        synced_at,
    })
}

pub(crate) fn store_list_messages(
    store: &Store,
    account_id: &str,
    conversation_id: &str,
    limit: u32,
    before: Option<&str>,
) -> Result<Page<MessageRecord>, ServiceError> {
    if store.account_by_id(account_id)?.is_none() {
        return Err(ServiceError::Store(StoreError::AccountNotFound));
    }
    if store
        .conversation_by_id(account_id, conversation_id)?
        .is_none()
    {
        return Err(ServiceError::Store(StoreError::ConversationNotFound));
    }
    // Viewing the thread marks it read (local badge only; no Signal receipt RPC yet).
    let _ = store.clear_conversation_unread(account_id, conversation_id)?;
    let mut page = store.list_messages(account_id, conversation_id, limit, before)?;
    page.items = page
        .items
        .into_iter()
        .map(project_message_for_host)
        .collect();
    Ok(page)
}

pub(crate) fn store_search_messages(
    store: &Store,
    account_id: &str,
    query: &str,
    limit: u32,
    cursor: Option<&str>,
) -> Result<Page<MessageRecord>, ServiceError> {
    if store.account_by_id(account_id)?.is_none() {
        return Err(ServiceError::Store(StoreError::AccountNotFound));
    }
    // An empty (or whitespace-only) query would match every stored body;
    // the client treats that as "no query", so answer with an empty page.
    let query = query.trim();
    if query.is_empty() {
        return Ok(Page {
            items: Vec::new(),
            next_cursor: None,
        });
    }
    // Search terms are user input echoed into a LIKE pattern: bound them so
    // a paste cannot inflate the query (contract 1.30).
    if query.chars().count() > 128 {
        return Err(ServiceError::Api(ApiError::new(
            "INVALID_REQUEST",
            "query must be at most 128 characters",
            false,
        )));
    }
    let mut page = store.search_messages(account_id, query, limit, cursor)?;
    page.items = page
        .items
        .into_iter()
        .map(project_message_for_host)
        .collect();
    Ok(page)
}

pub(crate) fn store_get_message_text(
    store: &Store,
    account_id: &str,
    conversation_id: &str,
    message_id: &str,
) -> Result<MessageText, ServiceError> {
    validate_opaque_id(account_id, "accountId")?;
    validate_opaque_id(conversation_id, "conversationId")?;
    validate_opaque_id(message_id, "messageId")?;
    let message = store
        .message_by_id(account_id, conversation_id, message_id)?
        .ok_or(StoreError::MessageNotFound)?;
    if !message.text_retrievable {
        return Err(ServiceError::Api(ApiError::new(
            "CAPABILITY_UNAVAILABLE",
            "complete message text is unavailable",
            false,
        )));
    }
    let text = message.text.ok_or_else(|| {
        ServiceError::Api(ApiError::new(
            "CAPABILITY_UNAVAILABLE",
            "message has no text body",
            false,
        ))
    })?;
    Ok(MessageText {
        message_id: message.id,
        text_bytes: message.text_bytes.unwrap_or(text.len() as u32),
        text,
    })
}

/// The legacy sender identity a store generation may have written for this
/// peer (receive dedupe pairs the current and legacy hash).
fn legacy_sender_id(kind: &str, peer_key: &str, account_id: &str) -> String {
    stable_hash_id(&[account_id, kind, peer_key])
}

impl ConnectorService {
    fn require_active_link(
        &mut self,
        link_session_id: &str,
    ) -> Result<&ActiveLinkSession, ServiceError> {
        match self.link.as_ref() {
            Some(session) if session.session_id == link_session_id => {
                if session.is_expired(now_ms()) {
                    self.link = None;
                    return Err(ServiceError::Api(ApiError::new(
                        "LINK_EXPIRED",
                        "link session has expired",
                        false,
                    )));
                }
            }
            Some(_) | None => {
                return Err(ServiceError::Api(ApiError::new(
                    "LINK_NOT_FOUND",
                    "link session was not found",
                    false,
                )));
            }
        }
        Ok(self.link.as_ref().expect("link session present"))
    }

    fn take_link_session(
        &mut self,
        link_session_id: &str,
    ) -> Result<ActiveLinkSession, ServiceError> {
        let _ = self.require_active_link(link_session_id)?;
        Ok(self.link.take().expect("link session present"))
    }
}

#[derive(Debug)]
pub enum PreparedSend {
    Existing(Box<MessageRecord>),
    Dispatch {
        pending_id: String,
        account_id: String,
        conversation_id: String,
        params: Value,
        pending_sent_at: u64,
    },
}

/// Everything the supervisor needs to run one upstream `remoteDelete` call,
/// produced by the pure local `prepare_remote_delete` validation.
#[derive(Debug)]
pub struct PreparedRemoteDelete {
    pub account_id: String,
    pub conversation_id: String,
    pub message_id: String,
    pub params: Value,
}

/// Everything the supervisor needs to run one upstream `sendReaction` call,
/// produced by the pure local `prepare_send_reaction` validation.
#[derive(Debug)]
pub struct PreparedSendReaction {
    pub account_id: String,
    pub conversation_id: String,
    pub message_id: String,
    pub params: Value,
    /// Upstream-confirmed reaction delta, carried for the complete step
    /// (own reactions persist with the account itself as the actor).
    pub emoji: String,
    pub remove: bool,
    pub target_sent_at: u64,
}

/// Everything the supervisor needs to run one upstream pin-family call
/// (`sendPinMessage` / `sendUnpinMessage` / `sendAdminDelete`, contract
/// 1.33), produced by the pure local prepare validations. The target
/// identity rides along for the complete step (own pins and own unpins
/// converge the locally derived pinned state).
#[derive(Debug)]
pub struct PreparedPin {
    pub account_id: String,
    pub conversation_id: String,
    pub message_id: String,
    pub params: Value,
    pub target_author: String,
    pub target_sent_at: u64,
    pub pin_duration_seconds: Option<u32>,
}

/// One per-author receipt group (contract revision 1.34): the recipient the
/// engine addresses — the message author, never the whole group (official
/// receipts are always per-author direct sends) — plus that author's upstream
/// timestamps in receipt order.
#[derive(Debug)]
pub struct ReceiptGroup {
    pub recipient: String,
    pub timestamps: Vec<u64>,
}

/// The `messages.markRead` / `messages.markViewed` prepare result: the
/// resolved account plus the per-author fan-out. An empty group list is the
/// trivial no-op the supervisor answers `{"status":"sent"}` for; the
/// supervisor calls the engine once per group and never fails the request on
/// a receipt-call failure.
#[derive(Debug)]
pub struct PreparedReceipts {
    pub account_id: String,
    pub conversation_id: String,
    pub account_signal: String,
    pub groups: Vec<ReceiptGroup>,
}

/// `messages.attachments.open` result (ADR 0002, contract revision 1.17): an
/// unguessable 128-bit handle bound to the resolved
/// (account, conversation, message, attachment) tuple, the file size the
/// stream will deliver, and the raw chunk size every `readChunk` offsets
/// advance by. The filesystem path never appears anywhere on the wire.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MediaOpenView {
    pub media_handle: String,
    pub size_bytes: u64,
    pub chunk_bytes: u64,
}

/// `messages.attachments.readChunk` result (ADR 0002): exactly one chunk.
/// `size_bytes` counts this chunk's raw bytes; `dataBase64` inflates them by
/// the usual per-chunk base64 factor to at most
/// `4 * ceil(262_144 / 3)` = 349_528 characters — bounded, and far inside
/// the 160 MiB host frame budget. `eof` terminates the caller's read loop.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MediaChunkView {
    pub data_base64: String,
    pub size_bytes: u64,
    pub eof: bool,
}

/// `messages.attachments.closeHandle` result: `released` is false when the
/// handle was already gone (expired or closed earlier) — closing is
/// idempotent by design.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MediaCloseView {
    pub released: bool,
}

/// The groups.get response (contract revision 1.9): the cached kind='group'
/// contacts row projected for the host. `member_count` is absent when the
/// sync batch captured no member list; `synced_at` is the row's last sync
/// timestamp — the response is exactly as fresh as that sync, nothing more.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupDetails {
    pub peer_key: String,
    pub title: String,
    pub member_count: Option<u64>,
    pub synced_at: u64,
}

/// Target of an outgoing text send: either an existing conversation, or a peer
/// (kind + peer_key) for which a conversation is resolved/created on demand.
#[derive(Clone, Debug)]
pub enum SendTarget {
    Conversation(String),
    Peer {
        kind: String,
        peer_key: String,
        peer_title: Option<String>,
    },
}

/// Borrowed peer addressing for a send, validated by the service.
#[derive(Clone, Copy, Debug)]
pub struct PeerTarget<'a> {
    pub kind: &'a str,
    pub peer_key: &'a str,
    pub peer_title: Option<&'a str>,
}

/// Borrowed addressing for an attachment send (implementation-plan §4.12):
/// same two forms as [`SendTarget`] but borrowed, so validation happens in
/// the service before any row is written.
#[derive(Clone, Copy, Debug)]
pub enum AttachmentSendTarget<'a> {
    Conversation(&'a str),
    Peer(PeerTarget<'a>),
}

#[derive(Clone, Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ContactsSyncOutcome {
    pub contact_count: u64,
    pub group_count: u64,
    pub synced_at: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AccountDeleteLocalDataParams {
    pub account_id: String,
    pub operation_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LinkStartParams {
    pub device_name: String,
    /// Optional target proxy group (ADR 0001 R3). Absent means `default`; an
    /// unknown id fails with PROXY_GROUP_NOT_FOUND at the registry. Groups are
    /// selected here, never created or reconfigured over IPC (R1).
    pub proxy_group: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LinkSessionParams {
    pub link_session_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConversationsListParams {
    pub account_id: String,
    pub cursor: Option<String>,
    pub limit: u32,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MessagesListParams {
    pub account_id: String,
    pub conversation_id: String,
    pub before: Option<String>,
    pub limit: u32,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MessagesSearchParams {
    pub account_id: String,
    pub query: String,
    pub cursor: Option<String>,
    pub limit: u32,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageGetTextParams {
    pub account_id: String,
    pub conversation_id: String,
    pub message_id: String,
}

#[derive(Clone, Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageText {
    pub message_id: String,
    pub text: String,
    pub text_bytes: u32,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MessagesSendTextParams {
    pub account_id: String,
    pub conversation_id: Option<String>,
    pub kind: Option<String>,
    pub peer_key: Option<String>,
    pub peer_title: Option<String>,
    pub text: String,
    pub client_request_id: String,
    pub quote_message_id: Option<String>,
    pub previews: Option<Vec<SendTextPreviewParams>>,
    pub mentions: Option<Vec<SendTextMentionParams>>,
}

/// One outbound @mention entry (contract revision 1.34, §4.29): the shape
/// mirrors the §4.20 receive projection — `number` resolves contacts-first
/// (ACI/UUID passthrough), and `start`/`length` are UTF-16 code-unit offsets
/// the caller indexes against its own body (the official BodyRange
/// semantics). A malformed entry fails params deserialization with
/// INVALID_REQUEST before anything else runs.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SendTextMentionParams {
    pub number: String,
    pub start: u32,
    pub length: u32,
}

/// Retry one definitively-failed outgoing text (contract 1.31): addressed by
/// the ORIGINAL send's clientRequestId so both an in-session optimistic row
/// and a reloaded store row hit the same persisted record. No optional forms:
/// a retry has exactly one target.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MessagesRetryTextParams {
    pub account_id: String,
    pub conversation_id: String,
    pub client_request_id: String,
}

/// Outbound link preview (contract §5.10 v1.26). signal-cli's JSON-RPC send
/// carries exactly one preview per message (`previewUrl`/`previewTitle`/
/// `previewDescription`/`previewImage`, verified against the pinned
/// distribution), so only the first entry is used and extras are ignored.
/// The image travels as an RFC 2397 data URI — the same byte-in-memory
/// pattern as `messages.attachments.send`; no caller-controlled path ever
/// reaches upstream.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SendTextPreviewParams {
    pub url: String,
    pub title: String,
    pub description: Option<String>,
    pub image_data_uri: Option<String>,
}

/// Normalized, validated preview passed down to `dispatch_send` and projected
/// 1:1 onto the upstream JSON-RPC keys (`previewUrl`/`previewTitle`/
/// `previewDescription`/`previewImage`).
#[derive(Debug, Clone)]
pub struct UpstreamSendPreview {
    pub url: String,
    pub title: String,
    pub description: Option<String>,
    pub image_data_uri: Option<String>,
}

/// A validated mention passed down to `dispatch_send` and projected 1:1 onto
/// the upstream `mentions` entry keys (`number`/`start`/`length`). Only
/// entries the connector could plausibly resolve — a UUID-shaped ACI, the
/// linked account's own number, or a digit-suffix match of the cached
/// contacts — survive this far; everything else was dropped at validation
/// (the official behavior for unresolvable mention addresses).
#[derive(Debug, Clone)]
pub struct UpstreamSendMention {
    pub number: String,
    pub start: u32,
    pub length: u32,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MessagesSendAttachmentParams {
    pub account_id: String,
    pub conversation_id: Option<String>,
    pub kind: Option<String>,
    pub peer_key: Option<String>,
    pub peer_title: Option<String>,
    pub client_request_id: String,
    pub data_base64: String,
    pub size_bytes: u64,
    pub filename: Option<String>,
    pub content_type: Option<String>,
    pub text: Option<String>,
    pub quote_message_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MessagesEditParams {
    pub account_id: String,
    pub conversation_id: String,
    pub message_id: String,
    pub text: String,
    pub client_request_id: Option<String>,
}

#[derive(Debug)]
pub struct PreparedEdit {
    pub account_id: String,
    pub conversation_id: String,
    pub message_id: String,
    pub text: String,
    pub params: Value,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MessagesRemoteDeleteParams {
    pub account_id: String,
    pub conversation_id: String,
    pub message_id: String,
    pub operation_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MessagesSendReactionParams {
    pub account_id: String,
    pub conversation_id: String,
    pub message_id: String,
    pub emoji: String,
    #[serde(default)]
    pub remove: bool,
    pub operation_id: Option<String>,
}

/// messages.sendPinMessage params (contract 1.33): the §4.6 reaction
/// addressing triple verbatim plus the optional timed-pin duration. The u32
/// bound is the fail-closed range check — a negative, fractional, or
/// oversized value fails params deserialization with INVALID_REQUEST, and an
/// absent/null value is the official forever pin.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MessagesSendPinMessageParams {
    pub account_id: String,
    pub conversation_id: String,
    pub message_id: String,
    pub pin_duration_seconds: Option<u32>,
    pub operation_id: Option<String>,
}

/// messages.sendUnpinMessage params (contract 1.33): same addressing, no
/// duration — the official unpinMessage carries only the target identity.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MessagesSendUnpinMessageParams {
    pub account_id: String,
    pub conversation_id: String,
    pub message_id: String,
    pub operation_id: Option<String>,
}

/// messages.sendAdminDelete params (contract 1.33): same addressing, group
/// conversations only.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MessagesSendAdminDeleteParams {
    pub account_id: String,
    pub conversation_id: String,
    pub message_id: String,
    pub operation_id: Option<String>,
}

/// messages.markRead / messages.markViewed params (contract revision 1.34):
/// the conversation plus an optional bounded `messageIds` list — absent means
/// every incoming row of the conversation (ascending `sentAt`, bounded 512);
/// explicit ids beyond 512 are truncated, not rejected. No operationId: the
/// request never mutates local state and its outcome never triggers a retry,
/// so there is nothing to correlate.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MessagesMarkReadParams {
    pub account_id: String,
    pub conversation_id: String,
    pub message_ids: Option<Vec<String>>,
}

/// messages.markViewed params: identical shape to
/// [`MessagesMarkReadParams`].
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MessagesMarkViewedParams {
    pub account_id: String,
    pub conversation_id: String,
    pub message_ids: Option<Vec<String>>,
}

/// messages.attachments.open params (ADR 0002): the message-addressing triple
/// plus the attachment id from the message's received metadata. There is no
/// caller-declared sizeBytes: the file size is measured locally and reported
/// back, never trusted from the wire.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MessagesAttachmentsOpenParams {
    pub account_id: String,
    pub conversation_id: String,
    pub message_id: String,
    pub attachment_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MessagesAttachmentsReadChunkParams {
    pub media_handle: String,
    pub offset: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MessagesAttachmentsCloseHandleParams {
    pub media_handle: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContactsSyncParams {
    pub account_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ContactsListParams {
    pub account_id: String,
    pub query: Option<String>,
    pub cursor: Option<String>,
    pub limit: u32,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GroupsGetParams {
    pub account_id: String,
    pub group_key: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ContactsSetLocalAliasParams {
    pub account_id: String,
    pub peer_key: String,
    pub alias: String,
    pub operation_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PresenceSetTypingMessageParams {
    pub account_id: String,
    pub conversation_id: String,
    pub stop: Option<bool>,
    pub operation_id: Option<String>,
}

/// Upstream addressing for one resolved conversation (pinned 0.14.7 JSON-RPC
/// shape): a group conversation addresses `groupId`, every other kind the
/// `recipient` array — the same targeting `send`, `sendTyping`,
/// `remoteDelete`, and `sendReaction` share.
fn set_upstream_target(params: &mut Value, conversation: &ConversationRow) {
    if conversation.kind == "group" {
        params["groupId"] = json!(conversation.peer_key);
    } else {
        params["recipient"] = json!([conversation.peer_key]);
    }
}

fn validate_device_name(device_name: &str) -> Result<(), ServiceError> {
    if device_name.is_empty() || device_name.len() > MAX_DEVICE_NAME_BYTES {
        return Err(ServiceError::Api(ApiError::new(
            "INVALID_REQUEST",
            "deviceName must contain between 1 and 64 bytes",
            false,
        )));
    }
    Ok(())
}

/// Per-engine account ceiling rejection (optimization-plan §6.4 M3.3): the
/// message carries the group dimension and the ceiling; it never carries an
/// account number. Not retryable — freeing capacity needs an explicit
/// `accounts.deleteLocalData`.
pub fn account_limit_error(proxy_group: &str) -> ServiceError {
    ServiceError::Api(ApiError::new(
        "ACCOUNT_LIMIT_REACHED",
        format!(
            "proxy group '{proxy_group}' already holds {MAX_ACCOUNTS_PER_ENGINE} accounts, \
             the per-engine ceiling; delete an account before linking another"
        ),
        false,
    ))
}

/// contacts.setLocalAlias bound (§4.9): a non-empty, short display name.
fn validate_alias(alias: &str) -> Result<(), ServiceError> {
    if alias.is_empty() || alias.len() > MAX_ALIAS_BYTES {
        return Err(ServiceError::Api(ApiError::new(
            "INVALID_REQUEST",
            "alias must contain between 1 and 128 bytes",
            false,
        )));
    }
    Ok(())
}

pub(crate) fn validate_account_delete_operation_id(operation_id: &str) -> Result<(), ServiceError> {
    validate_opaque_id(operation_id, "operationId")
}

fn validate_text(text: &str) -> Result<(), ServiceError> {
    if text.is_empty() || text.len() > MAX_TEXT_BYTES {
        return Err(ServiceError::Api(ApiError::new(
            "INVALID_REQUEST",
            "text must contain between 1 and 65536 bytes",
            false,
        )));
    }
    Ok(())
}

/// Bounds for the outbound link preview (contract §5.10 v1.26). The URL cap
/// follows the de-facto upstream limit; title/description caps mirror the
/// official desktop renderer's truncation headroom; the image cap bounds the
/// data URI to ~1 MiB of binary (4/3 base64 inflation included).
const MAX_PREVIEW_URL_BYTES: usize = 2048;
const MAX_PREVIEW_TITLE_BYTES: usize = 1024;
const MAX_PREVIEW_DESCRIPTION_BYTES: usize = 4096;
const MAX_PREVIEW_IMAGE_DATA_URI_CHARS: usize = 1_572_864;

/// At most one preview per send upstream (signal-cli builds `List.of(one)`);
/// extras are ignored, `None`/empty list means no preview.
fn previews_to_upstream(
    previews: Option<Vec<SendTextPreviewParams>>,
    text: &str,
) -> Result<Option<UpstreamSendPreview>, ServiceError> {
    previews
        .and_then(|list| list.into_iter().next())
        .map(|preview| normalize_send_preview(preview, text))
        .transpose()
}

/// The ACI/UUID wire shape (contract revision 1.34): 36 chars, hyphens at the
/// canonical positions, hex elsewhere. Only this shape passes the mention
/// gate without a contacts lookup — the engine's ServiceId parse accepts
/// exactly it for bare-UUID entries.
fn is_uuid_shaped(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() != 36 {
        return false;
    }
    for (index, byte) in bytes.iter().enumerate() {
        match index {
            8 | 13 | 18 | 23 => {
                if *byte != b'-' {
                    return false;
                }
            }
            _ => {
                if !byte.is_ascii_hexdigit() {
                    return false;
                }
            }
        }
    }
    true
}

/// The ASCII digits of an address — the engine `resolve_author_aci` matching
/// key (numbers compare as digit-suffix, ignoring formatting).
fn address_digits(value: &str) -> String {
    value.chars().filter(char::is_ascii_digit).collect()
}

/// Validate and normalize the caller-supplied link preview into the exact
/// shape the upstream JSON-RPC `send` expects. Enforced here — not at the
/// engine — because signal-cli validates "the same url must also appear in
/// the message body" only via its CLI help text, and a late upstream failure
/// would surface as an unknown-outcome-shaped error instead of a
/// deterministic INVALID_REQUEST.
fn normalize_send_preview(
    preview: SendTextPreviewParams,
    text: &str,
) -> Result<UpstreamSendPreview, ServiceError> {
    let invalid = |why: &str| {
        Err(ServiceError::Api(ApiError::new(
            "INVALID_REQUEST",
            why,
            false,
        )))
    };
    let url = preview.url.trim();
    if url.is_empty() || url.len() > MAX_PREVIEW_URL_BYTES {
        return invalid("preview url must contain between 1 and 2048 bytes");
    }
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        return invalid("preview url must be an absolute http(s) url");
    }
    // signal-cli: "the same url must also appear in the message body".
    if !text.contains(url) {
        return invalid("preview url must appear in the message text");
    }
    let title = preview.title.trim();
    if title.is_empty() || title.len() > MAX_PREVIEW_TITLE_BYTES {
        return invalid("preview title must contain between 1 and 1024 bytes");
    }
    let description = preview
        .description
        .map(|d| d.trim().to_string())
        .filter(|d| !d.is_empty());
    if let Some(description) = &description {
        if description.len() > MAX_PREVIEW_DESCRIPTION_BYTES {
            return invalid("preview description must contain at most 4096 bytes");
        }
    }
    if let Some(image) = &preview.image_data_uri {
        if !image.starts_with("data:image/") {
            return invalid("preview image must be a data:image/ rfc2397 data uri");
        }
        if image.len() > MAX_PREVIEW_IMAGE_DATA_URI_CHARS {
            return invalid("preview image data uri exceeds 1.5M chars");
        }
    }
    Ok(UpstreamSendPreview {
        url: url.to_string(),
        title: title.to_string(),
        description,
        image_data_uri: preview.image_data_uri,
    })
}

/// The reaction must be exactly one unicode grapheme cluster of at most 32
/// UTF-8 bytes — the pinned signal-cli requirement ("should be a single
/// unicode grapheme cluster", SendReactionCommand --emoji). Grapheme clusters
/// keep multi-codepoint emoji (ZWJ sequences, skin-tone modifiers, flags)
/// valid while rejecting multi-emoji strings.
fn validate_emoji(emoji: &str) -> Result<(), ServiceError> {
    if emoji.is_empty() || emoji.len() > MAX_EMOJI_BYTES {
        return Err(ServiceError::Api(ApiError::new(
            "INVALID_REQUEST",
            "emoji must contain between 1 and 32 bytes",
            false,
        )));
    }
    if emoji.graphemes(true).count() != 1 {
        return Err(ServiceError::Api(ApiError::new(
            "INVALID_REQUEST",
            "emoji must be a single unicode grapheme cluster",
            false,
        )));
    }
    Ok(())
}

/// One message.statusChanged event for a record whose status just moved.
fn status_changed_event(record: &MessageRecord) -> HostSideEvent {
    HostSideEvent::MessageStatusChanged {
        account_id: record.account_id.clone(),
        message_id: record.id.clone(),
        status: record.status,
        delivered_at: record.delivered_at,
        read_at: record.read_at,
    }
}

/// Emit a status event only for a real transition: a replayed terminal write
/// (store reports `false`) or a vanished row produces nothing, so the host
/// never sees a duplicate or phantom status change.
fn status_change_events(updated: Option<(MessageRecord, bool)>) -> Vec<HostSideEvent> {
    match updated {
        Some((record, true)) => vec![status_changed_event(&record)],
        _ => Vec::new(),
    }
}

fn project_message_for_host(mut message: MessageRecord) -> MessageRecord {
    let Some(text) = message.text.as_deref() else {
        return message;
    };
    if text.len() <= MAX_HOST_TEXT_PREVIEW_BYTES {
        return message;
    }
    message.text = Some(truncate_utf8_bytes(text, MAX_HOST_TEXT_PREVIEW_BYTES));
    message.text_truncated = true;
    message
}

fn validate_opaque_id(value: &str, field: &str) -> Result<(), ServiceError> {
    if value.is_empty() || value.len() > MAX_OPAQUE_ID_BYTES {
        return Err(ServiceError::Api(ApiError::new(
            "INVALID_REQUEST",
            format!("{field} must contain between 1 and {MAX_OPAQUE_ID_BYTES} bytes"),
            false,
        )));
    }
    Ok(())
}

/// signal-cli `AttachmentStore.sanitizeId` parity (ADR 0002 security gate 3).
/// Upstream resolves an attachment id as
/// `new File(attachmentsPath, sanitizeId(id))` where `sanitizeId` replaces
/// every character outside `[A-Za-z0-9_.-]` with `_`; the connector applies
/// the identical transform before any path join so it can predict the exact
/// file the id names. On top of the transform — which alone would silently
/// launder hostile shapes into harmless-looking names — the shapes that only
/// make sense as traversal are refused outright: any id containing a path
/// separator (`/`, `\`) and, after sanitizing, the empty id, `.` and `..`.
/// Every refusal answers INVALID_REQUEST before the filesystem is touched.
fn sanitize_attachment_id(attachment_id: &str) -> Result<String, ServiceError> {
    if attachment_id.is_empty() || attachment_id.len() > MAX_ATTACHMENT_ID_BYTES {
        return Err(ServiceError::Api(ApiError::new(
            "INVALID_REQUEST",
            "attachmentId must contain between 1 and 128 bytes",
            false,
        )));
    }
    if attachment_id.contains('/') || attachment_id.contains('\\') {
        return Err(ServiceError::Api(ApiError::new(
            "INVALID_REQUEST",
            "attachmentId must be a bare attachment file name",
            false,
        )));
    }
    let sanitized: String = attachment_id
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '.' | '-' | '_') {
                character
            } else {
                '_'
            }
        })
        .collect();
    if sanitized.is_empty() || sanitized == "." || sanitized == ".." {
        return Err(ServiceError::Api(ApiError::new(
            "INVALID_REQUEST",
            "attachmentId does not name an attachment file",
            false,
        )));
    }
    Ok(sanitized)
}

/// The upstream base64 must decode to exactly the size the caller declared
/// (contract revision 1.8): anything else means the host budgeted for a
/// different payload. Encoding length is checked first so a hostile upstream
/// response cannot inflate memory before the byte budget is confirmed.
fn validate_attachment_payload(
    base64_data: &str,
    expected_size_bytes: u64,
) -> Result<(), ServiceError> {
    if base64_data.is_empty() {
        return Err(ServiceError::Api(ApiError::new(
            "INVALID_REQUEST",
            "attachment payload is empty",
            false,
        )));
    }
    if base64_data.len() > MAX_ATTACHMENT_BASE64_CHARS {
        return Err(ServiceError::Api(ApiError::new(
            "INVALID_REQUEST",
            "attachment exceeds the 104857600-byte limit",
            false,
        )));
    }
    let padding = base64_data.len() - base64_data.trim_end_matches('=').len();
    let decoded_len = (base64_data.len() / 4 * 3).saturating_sub(padding);
    if decoded_len as u64 != expected_size_bytes {
        return Err(ServiceError::Api(ApiError::new(
            "INVALID_REQUEST",
            "attachment size does not match the declared sizeBytes",
            false,
        )));
    }
    Ok(())
}

/// Validate an attachment send payload (contract revision 1.13,
/// implementation-plan §4.12): the same base64/size discipline as
/// [`validate_attachment_payload`] plus the standard-alphabet shape check —
/// the connector re-encodes the payload into a data URI itself, so a payload
/// that is not canonical standard base64 would corrupt the data URI.
fn validate_attachment_send_payload(
    base64_data: &str,
    expected_size_bytes: u64,
) -> Result<(), ServiceError> {
    validate_attachment_payload(base64_data, expected_size_bytes)?;
    if base64_data.len() % 4 != 0 {
        return Err(ServiceError::Api(ApiError::new(
            "INVALID_REQUEST",
            "attachment payload is not canonical base64",
            false,
        )));
    }
    let mut decoded = Vec::with_capacity(expected_size_bytes as usize);
    if base64_decode_to_vec(base64_data, &mut decoded).is_err()
        || decoded.len() as u64 != expected_size_bytes
    {
        return Err(ServiceError::Api(ApiError::new(
            "INVALID_REQUEST",
            "attachment payload is not valid base64 or does not match sizeBytes",
            false,
        )));
    }
    Ok(())
}

/// Encode standard padded base64 — the same canonical form
/// `build_attachment_data_uri` re-encodes to and `base64_decode_to_vec`
/// accepts. The chunked media delivery (ADR 0002) uses it per 256 KiB chunk:
/// 349_528 characters worst case, bounded and frame-safe.
fn base64_encode(data: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut encoded = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let byte1 = chunk.first().copied().unwrap_or_default() as u32;
        let byte2 = chunk.get(1).copied().unwrap_or_default() as u32;
        let byte3 = chunk.get(2).copied().unwrap_or_default() as u32;
        let triple = (byte1 << 16) | (byte2 << 8) | byte3;
        encoded.push(ALPHABET[(triple >> 18) as usize & 0x3F] as char);
        encoded.push(ALPHABET[(triple >> 12) as usize & 0x3F] as char);
        if chunk.len() > 1 {
            encoded.push(ALPHABET[(triple >> 6) as usize & 0x3F] as char);
        } else {
            encoded.push('=');
        }
        if chunk.len() > 2 {
            encoded.push(ALPHABET[(triple) as usize & 0x3F] as char);
        } else {
            encoded.push('=');
        }
    }
    encoded
}

/// Decode standard base64 (with padding) into `out`. Returns Err on any
/// non-canonical input; the implementation mirrors the alphabet upstream's
/// java.util.Base64 accepts.
fn base64_decode_to_vec(input: &str, out: &mut Vec<u8>) -> Result<(), ()> {
    const INVALID: u8 = 0xFF;
    fn value(byte: u8) -> u8 {
        match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => INVALID,
        }
    }
    let bytes = input.as_bytes();
    if bytes.len() % 4 != 0 {
        return Err(());
    }
    let padding = bytes.iter().rev().take_while(|&&b| b == b'=').count();
    if padding > 2 {
        return Err(());
    }
    let data_end = bytes.len() - padding;
    // Every remaining byte before padding must be a data character; '=' only
    // allowed in the final quad (already ensured by take_while from the end).
    out.clear();
    out.reserve(data_end / 4 * 3 + 3);
    let mut quad = [0u8; 4];
    let mut quad_len = 0;
    for &byte in &bytes[..data_end] {
        let v = value(byte);
        if v == INVALID {
            return Err(());
        }
        quad[quad_len] = v;
        quad_len += 1;
        if quad_len == 4 {
            out.push((quad[0] << 2) | (quad[1] >> 4));
            out.push((quad[1] << 4) | (quad[2] >> 2));
            out.push((quad[2] << 6) | quad[3]);
            quad_len = 0;
        }
    }
    // Final partial quad: 2 chars -> 1 byte, 3 chars -> 2 bytes; leftover
    // bits must be zero (canonical form).
    match quad_len {
        0 => {}
        2 => {
            if quad[1] & 0x0F != 0 {
                return Err(());
            }
            out.push((quad[0] << 2) | (quad[1] >> 4));
        }
        3 => {
            if quad[2] & 0x03 != 0 {
                return Err(());
            }
            out.push((quad[0] << 2) | (quad[1] >> 4));
            out.push((quad[1] << 4) | (quad[2] >> 2));
        }
        _ => return Err(()),
    }
    Ok(())
}

/// Validate the caller-supplied display filename/contentType bound
/// (1–128 bytes, no control characters). Path separators are rejected for
/// filename so a descriptor can never masquerade as a path fragment.
fn validate_attachment_descriptor(value: Option<&str>, field: &str) -> Result<(), ServiceError> {
    let Some(value) = value else {
        return Ok(());
    };
    if value.is_empty() || value.len() > MAX_ATTACHMENT_FILENAME_BYTES {
        return Err(ServiceError::Api(ApiError::new(
            "INVALID_REQUEST",
            format!("{field} must contain between 1 and 128 bytes"),
            false,
        )));
    }
    if value.chars().any(char::is_control) || value.contains('/') || value.contains('\\') {
        return Err(ServiceError::Api(ApiError::new(
            "INVALID_REQUEST",
            format!("{field} must not contain path separators or control characters"),
            false,
        )));
    }
    Ok(())
}

/// Validate an optional RFC 2045 media type (`type/subtype`, both non-empty
/// token shapes). The pinned upstream parses the data URI header loosely, but
/// a malformed content type upstream would surface as UPSTREAM_ERROR after a
/// pending row exists — reject it deterministically here instead.
fn validate_attachment_content_type(value: Option<&str>) -> Result<(), ServiceError> {
    let Some(value) = value else {
        return Ok(());
    };
    if value.is_empty() || value.len() > MAX_ATTACHMENT_CONTENT_TYPE_BYTES {
        return Err(ServiceError::Api(ApiError::new(
            "INVALID_REQUEST",
            "contentType must contain between 1 and 128 bytes",
            false,
        )));
    }
    let valid_token = |part: &str| {
        !part.is_empty()
            && part
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b))
    };
    let (main, sub) = value.split_once('/').ok_or_else(|| {
        ServiceError::Api(ApiError::new(
            "INVALID_REQUEST",
            "contentType must be 'type/subtype'",
            false,
        ))
    })?;
    if !valid_token(main) || !valid_token(sub) {
        return Err(ServiceError::Api(ApiError::new(
            "INVALID_REQUEST",
            "contentType contains invalid characters",
            false,
        )));
    }
    Ok(())
}

/// Build the RFC 2397 data URI the pinned upstream accepts
/// (AttachmentHelper: `data:<mime>;filename=<name>;base64,<payload>`). The
/// payload is re-encoded from the already-validated base64 — decoding first
/// and re-encoding keeps the URI byte-exact even if the host sent a base64
/// variant with different padding; the round-trip was verified in
/// [`validate_attachment_send_payload`].
fn build_attachment_data_uri(
    base64_data: &str,
    filename: Option<&str>,
    content_type: Option<&str>,
) -> Result<String, ServiceError> {
    let mut decoded = Vec::with_capacity(base64_data.len() / 4 * 3);
    base64_decode_to_vec(base64_data, &mut decoded).map_err(|_| {
        ServiceError::Api(ApiError::new(
            "INVALID_REQUEST",
            "attachment payload is not valid base64",
            false,
        ))
    })?;
    use std::fmt::Write as _;
    let mut uri = String::with_capacity(decoded.len() / 3 * 4 + 64);
    uri.push_str("data:");
    uri.push_str(content_type.unwrap_or("application/octet-stream"));
    if let Some(filename) = filename {
        uri.push_str(";filename=");
        // encodeURIComponent semantics for the filename parameter.
        for byte in filename.bytes() {
            let c = byte as char;
            if c.is_ascii_alphanumeric() || "-_.!~*'()".contains(c) {
                uri.push(c);
            } else {
                let _ = write!(uri, "%{byte:02X}");
            }
        }
    }
    uri.push_str(";base64,");
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    for chunk in decoded.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        uri.push(ALPHABET[(b[0] >> 2) as usize] as char);
        uri.push(ALPHABET[(((b[0] & 0x03) << 4) | (b[1] >> 4)) as usize] as char);
        if chunk.len() > 1 {
            uri.push(ALPHABET[(((b[1] & 0x0F) << 2) | (b[2] >> 6)) as usize] as char);
        } else {
            uri.push('=');
        }
        if chunk.len() > 2 {
            uri.push(ALPHABET[(b[2] & 0x3F) as usize] as char);
        } else {
            uri.push('=');
        }
    }
    Ok(uri)
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::engine::{NormalizedAttachment, NormalizedQuote, ReceiptKind};
    use crate::store::StoreKey;

    fn service() -> (TempDir, ConnectorService) {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(StoreKey::from_bytes([0x5A; 32]))).unwrap();
        (temp, ConnectorService::new(Arc::new(store)))
    }

    #[test]
    fn cancelled_link_cannot_complete_or_create_an_account() {
        let (_temp, mut service) = service();
        let started = service
            .begin_link("KT".into(), "sgnl://link?test".into())
            .unwrap();
        let link_session_id = started["linkSessionId"].as_str().unwrap().to_string();

        service.cancel_link(&link_session_id).unwrap();
        let late = service.complete_link_session(
            &link_session_id,
            "+15555550100",
            crate::DEFAULT_PROXY_GROUP_ID,
        );

        assert!(matches!(late, Err(ServiceError::Api(_))));
        assert!(
            service
                .list_accounts_in_group(crate::DEFAULT_PROXY_GROUP_ID)
                .unwrap()
                .is_empty()
        );
    }

    /// Per-engine account ceiling (optimization-plan §6.4 M3.3, decision D3):
    /// the eighth account in a group links; the ninth is refused with
    /// ACCOUNT_LIMIT_REACHED naming the group, and the refusal leaves the
    /// store untouched.
    #[test]
    fn eighth_account_links_but_a_ninth_is_refused_at_the_ceiling() {
        let (_temp, mut service) = service();
        let group = crate::DEFAULT_PROXY_GROUP_ID;
        let link_next = |service: &mut ConnectorService| {
            let number = format!(
                "+1556555{:04}",
                service.list_accounts_in_group(group).unwrap().len() + 1
            );
            let started = service
                .begin_link("KT".into(), "sgnl://link?test".into())
                .unwrap();
            let id = started["linkSessionId"].as_str().unwrap().to_string();
            service.complete_link_session(&id, &number, group)
        };

        for index in 1..=8 {
            let account =
                link_next(&mut service).unwrap_or_else(|e| panic!("link {index} failed: {e}"));
            assert_eq!(account.proxy_group, group);
        }
        assert_eq!(service.list_accounts_in_group(group).unwrap().len(), 8);
        assert!(service.group_at_account_ceiling(group).unwrap());

        let refused = link_next(&mut service).unwrap_err();
        let ServiceError::Api(api) = refused else {
            panic!("expected an api error at the ceiling");
        };
        assert_eq!(api.code, "ACCOUNT_LIMIT_REACHED");
        assert!(!api.retryable);
        assert!(
            api.message.contains(group),
            "message must name the group: {}",
            api.message
        );
        assert_eq!(service.list_accounts_in_group(group).unwrap().len(), 8);
    }

    /// The ceiling refuses additions, not updates: at a full group, finishing
    /// a link for a number already bound there completes as an upsert instead
    /// of an ACCOUNT_LIMIT_REACHED (store-level guard, M3.3).
    #[test]
    fn relinking_an_existing_number_at_the_ceiling_still_completes() {
        let (_temp, mut service) = service();
        let group = "team-a";
        for index in 1..=8 {
            let started = service
                .begin_link("KT".into(), "sgnl://link?test".into())
                .unwrap();
            let id = started["linkSessionId"].as_str().unwrap().to_string();
            service
                .complete_link_session(&id, &format!("+1556555{index:04}"), group)
                .unwrap();
        }

        let started = service
            .begin_link("KT".into(), "sgnl://link?test".into())
            .unwrap();
        let id = started["linkSessionId"].as_str().unwrap().to_string();
        let relinked = service
            .complete_link_session(&id, "+15565550008", group)
            .expect("relink of an existing number is an update, not an addition");
        assert_eq!(relinked.proxy_group, group);
        assert_eq!(service.list_accounts_in_group(group).unwrap().len(), 8);
    }

    #[test]
    fn active_link_is_rejected_before_requesting_another_upstream_uri() {
        let (_temp, mut service) = service();
        service
            .begin_link("KT".into(), "sgnl://link?test".into())
            .unwrap();

        assert!(matches!(
            service.ensure_link_available(),
            Err(ServiceError::Api(_))
        ));
    }

    /// An expired session answers the definite LINK_EXPIRED (retryable=false,
    /// same class as every other definite finish failure) and is consumed by
    /// that answer; only afterwards does the id read back as LINK_NOT_FOUND.
    #[test]
    fn expired_finish_answers_link_expired_once_then_reports_not_found() {
        let (_temp, mut service) = service();
        let link_session_id = service.plant_link_session_for_test("KT-Expired", Duration::ZERO);

        let error = service.peek_link_for_finish(&link_session_id).unwrap_err();
        let ServiceError::Api(api) = error else {
            panic!("expected an api error for the expired finish");
        };
        assert_eq!(api.code, "LINK_EXPIRED");
        assert!(!api.retryable);

        let again = service.peek_link_for_finish(&link_session_id).unwrap_err();
        let ServiceError::Api(api) = again else {
            panic!("expected an api error for the consumed session");
        };
        assert_eq!(api.code, "LINK_NOT_FOUND");

        // begin_link over an expired session must also succeed (expiry frees
        // the slot), keeping one deterministic terminal state per session.
        service
            .begin_link("KT-Again".into(), "sgnl://link?next".into())
            .expect("an expired session must not block a fresh link");
    }

    #[test]
    fn deleting_an_account_does_not_cancel_another_session_link() {
        let (_temp, mut service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let started = service
            .begin_link("KT-B".into(), "sgnl://link?second".into())
            .unwrap();
        let link_session_id = started["linkSessionId"].as_str().unwrap().to_string();

        assert!(
            service
                .store_ref()
                .delete_account_cascade(&account.id)
                .unwrap()
        );
        assert!(
            !service
                .store_ref()
                .delete_account_cascade(&account.id)
                .unwrap()
        );
        assert!(service.cancel_link(&link_session_id).is_ok());
    }

    /// Contract 1.21: marking an unlinked device persists `device_unlinked`
    /// and surfaces an account.changed event; unknown numbers are a no-op.
    #[test]
    fn mark_account_device_unlinked_persists_and_emits_account_changed() {
        let (_temp, service) = service();
        let group = crate::DEFAULT_PROXY_GROUP_ID;
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], group)
            .unwrap()
            .remove(0);

        let events = service
            .mark_account_device_unlinked("+15555550100")
            .unwrap();
        assert_eq!(events.len(), 1);
        let HostSideEvent::AccountChanged(changed) = &events[0] else {
            panic!("expected an account.changed event");
        };
        assert_eq!(changed.id, account.id);
        assert_eq!(changed.state, "device_unlinked");

        let listed = service
            .list_accounts_in_group(group)
            .unwrap()
            .into_iter()
            .find(|item| item.id == account.id)
            .unwrap();
        assert_eq!(listed.state, "device_unlinked");

        // A number with no account row is a silent no-op.
        assert!(
            service
                .mark_account_device_unlinked("+15555559999")
                .unwrap()
                .is_empty()
        );
    }

    /// The wire error for a rejected credential is ACCOUNT_UNLINKED
    /// (retryable=false) — distinct from the generic retryable upstream
    /// error, so hosts can distinguish "device was unlinked" from noise.
    #[test]
    fn unauthorized_engine_errors_map_to_account_unlinked() {
        let api = ServiceError::Engine(EngineError::Unauthorized).into_api();
        assert_eq!(api.code, "ACCOUNT_UNLINKED");
        assert!(!api.retryable);
    }

    #[test]
    fn text_limit_is_enforced_by_utf8_bytes() {
        assert!(validate_text(&"a".repeat(MAX_TEXT_BYTES)).is_ok());
        assert!(validate_text(&"a".repeat(MAX_TEXT_BYTES + 1)).is_err());

        assert!(validate_text(&"界".repeat(21_845)).is_ok());
        assert!(validate_text(&"界".repeat(21_846)).is_err());

        assert!(validate_text(&"😀".repeat(16_384)).is_ok());
        assert!(validate_text(&format!("{}a", "😀".repeat(16_384))).is_err());
        assert!(validate_text("").is_err());
    }

    #[test]
    fn inbound_long_text_is_projected_and_fetched_on_demand() {
        let (_temp, mut service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let full_text = "界".repeat(4_000);
        let events = service
            .ingest_receive(
                NormalizedReceive {
                    timestamp: Some(10),
                    content_kind: "dataMessage",
                    direction: "incoming",
                    account_present: true,
                    account: Some("+15555550100".into()),
                    source: Some("+15555550101".into()),
                    peer_name: Some("Peer".into()),
                    group_id: None,
                    text: Some(full_text.clone()),
                    text_bytes: None,
                    text_truncated: false,
                    quote: None,
                    attachments: Vec::new(),
                    rich: None,
                    control: None,
                },
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap();
        let projected = events
            .iter()
            .find_map(|event| match event {
                HostSideEvent::MessageUpserted(message) => Some(message),
                _ => None,
            })
            .unwrap();
        assert!(projected.text.as_ref().unwrap().len() <= MAX_HOST_TEXT_PREVIEW_BYTES);
        assert!(projected.text_truncated);
        assert!(projected.text_retrievable);

        let fetched = service
            .get_message_text(&account.id, &projected.conversation_id, &projected.id)
            .unwrap();
        assert_eq!(fetched.text, full_text);
        assert_eq!(fetched.text_bytes, 12_000);

        let other_account = service
            .sync_accounts_from_numbers(
                &["+15555550100".into(), "+15555550102".into()],
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap()
            .into_iter()
            .find(|candidate| candidate.id != account.id)
            .unwrap();
        assert!(matches!(
            service.get_message_text(&other_account.id, &projected.conversation_id, &projected.id,),
            Err(ServiceError::Store(StoreError::MessageNotFound))
        ));
        assert!(matches!(
            service.get_message_text(&"x".repeat(129), "conversation", "message"),
            Err(ServiceError::Api(error)) if error.code == "INVALID_REQUEST"
        ));
    }

    #[test]
    fn inbound_text_above_receive_ceiling_keeps_only_an_explicit_preview() {
        let (_temp, mut service) = service();
        service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let oversized = "a".repeat(MAX_INBOUND_TEXT_BYTES + 1);
        let events = service
            .ingest_receive(
                NormalizedReceive {
                    timestamp: Some(11),
                    content_kind: "dataMessage",
                    direction: "incoming",
                    account_present: true,
                    account: Some("+15555550100".into()),
                    source: Some("+15555550101".into()),
                    peer_name: None,
                    group_id: None,
                    text: Some(oversized),
                    text_bytes: None,
                    text_truncated: false,
                    quote: None,
                    attachments: Vec::new(),
                    rich: None,
                    control: None,
                },
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap();
        let projected = events
            .iter()
            .find_map(|event| match event {
                HostSideEvent::MessageUpserted(message) => Some(message),
                _ => None,
            })
            .unwrap();
        assert_eq!(
            projected.text.as_ref().unwrap().len(),
            MAX_HOST_TEXT_PREVIEW_BYTES
        );
        assert_eq!(
            projected.text_bytes,
            Some((MAX_INBOUND_TEXT_BYTES + 1) as u32)
        );
        assert!(projected.text_truncated);
        assert!(!projected.text_retrievable);
        assert!(matches!(
            service.get_message_text(
                &projected.account_id,
                &projected.conversation_id,
                &projected.id,
            ),
            Err(ServiceError::Api(error)) if error.code == "CAPABILITY_UNAVAILABLE"
        ));
    }

    #[test]
    fn send_text_projects_first_preview_onto_upstream_params() {
        let (_temp, service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let conversation = service
            .store_ref()
            .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
            .unwrap();
        let dispatch = match service
            .prepare_send_text(
                &account.id,
                &conversation.id,
                "look at https://example.com/a now",
                "req-preview-ok",
                None,
                Some(vec![
                    SendTextPreviewParams {
                        url: "https://example.com/a".into(),
                        title: "Example".into(),
                        description: Some("A page".into()),
                        image_data_uri: Some("data:image/jpeg;base64,QUJD".into()),
                    },
                    // extras are ignored upstream (List.of(one) on the pinned
                    // signal-cli) — the connector only forwards the first.
                    SendTextPreviewParams {
                        url: "https://example.com/b".into(),
                        title: "Second".into(),
                        description: None,
                        image_data_uri: None,
                    },
                ]),
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch { params, .. } => params,
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        };
        assert_eq!(dispatch["previewUrl"], json!("https://example.com/a"));
        assert_eq!(dispatch["previewTitle"], json!("Example"));
        assert_eq!(dispatch["previewDescription"], json!("A page"));
        assert_eq!(
            dispatch["previewImage"],
            json!("data:image/jpeg;base64,QUJD")
        );
    }

    #[test]
    fn send_text_preview_rejects_deterministic_shape_violations() {
        let (_temp, service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let conversation = service
            .store_ref()
            .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
            .unwrap();
        let preview = |url: &str, title: &str| SendTextPreviewParams {
            url: url.into(),
            title: title.into(),
            description: None,
            image_data_uri: None,
        };
        // signal-cli requires the preview url to appear in the message body —
        // enforced here so the failure is a deterministic INVALID_REQUEST
        // instead of a late upstream send error.
        let url_absent = service
            .prepare_send_text(
                &account.id,
                &conversation.id,
                "no link in here",
                "req-preview-absent",
                None,
                Some(vec![preview("https://example.com/a", "Example")]),
                None,
            )
            .unwrap_err();
        assert!(matches!(url_absent, ServiceError::Api(error) if error.code == "INVALID_REQUEST"));
        // A preview image is a data:image/ URI only — never a caller path.
        let path_image = service
            .prepare_send_text(
                &account.id,
                &conversation.id,
                "see https://example.com/a",
                "req-preview-path",
                None,
                Some(vec![SendTextPreviewParams {
                    url: "https://example.com/a".into(),
                    title: "Example".into(),
                    description: None,
                    image_data_uri: Some("/etc/passwd".into()),
                }]),
                None,
            )
            .unwrap_err();
        assert!(matches!(path_image, ServiceError::Api(error) if error.code == "INVALID_REQUEST"));
        // Title is mandatory upstream.
        let empty_title = service
            .prepare_send_text(
                &account.id,
                &conversation.id,
                "see https://example.com/a",
                "req-preview-title",
                None,
                Some(vec![preview("https://example.com/a", "   ")]),
                None,
            )
            .unwrap_err();
        assert!(matches!(empty_title, ServiceError::Api(error) if error.code == "INVALID_REQUEST"));
        // A rejected preview leaves no pending row behind.
        assert!(
            service
                .store_ref()
                .message_by_client_request(&account.id, "req-preview-absent")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn retry_text_rearms_the_same_failed_row_and_guards_states() {
        let (_temp, service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let conversation = service
            .store_ref()
            .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
            .unwrap();

        // First send: prepare inserts the pending row, then it fails for real.
        let pending_id = match service
            .prepare_send_text(
                &account.id,
                &conversation.id,
                "to retry",
                "retry-req-1",
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch { pending_id, .. } => pending_id,
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        };
        service.complete_send_failed(&pending_id).unwrap();

        // Retry re-dispatches the SAME row — no second row is inserted.
        let params = match service
            .prepare_retry_text(&account.id, &conversation.id, "retry-req-1")
            .unwrap()
        {
            PreparedSend::Dispatch {
                pending_id: retry_pending_id,
                params,
                ..
            } => {
                assert_eq!(retry_pending_id, pending_id);
                params
            }
            PreparedSend::Existing(_) => panic!("retry must re-dispatch the same row"),
        };
        assert_eq!(params["message"], "to retry");
        let row = service
            .store_ref()
            .message_by_client_request(&account.id, "retry-req-1")
            .unwrap()
            .unwrap();
        assert_eq!(row.id, pending_id);
        assert_eq!(row.status, "pending");

        // Settlement completes the SAME row; the history stays one row.
        let (sent, _) = service
            .complete_send_success(&pending_id, &account.id, &conversation.id, 777)
            .unwrap();
        assert_eq!(sent.status, "sent");
        let rows = service
            .list_messages(&account.id, &conversation.id, 10, None)
            .unwrap()
            .items;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, pending_id);

        // A settled row answers RETRY_NOT_ALLOWED.
        let sent_retry = service
            .prepare_retry_text(&account.id, &conversation.id, "retry-req-1")
            .unwrap_err();
        assert!(
            matches!(sent_retry, ServiceError::Api(ref error) if error.code == "RETRY_NOT_ALLOWED")
        );

        // An unknown-outcome row is never retryable (no automatic or blind
        // resend of an unsettled wire outcome).
        let unknown_pending = match service
            .prepare_send_text(
                &account.id,
                &conversation.id,
                "unknown outcome",
                "retry-req-2",
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch { pending_id, .. } => pending_id,
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        };
        service.complete_send_unknown(&unknown_pending).unwrap();
        let unknown_retry = service
            .prepare_retry_text(&account.id, &conversation.id, "retry-req-2")
            .unwrap_err();
        assert!(
            matches!(unknown_retry, ServiceError::Api(ref error) if error.code == "RETRY_NOT_ALLOWED")
        );

        // A row already in flight answers RETRY_IN_FLIGHT (double click).
        service.complete_send_failed(&unknown_pending).unwrap();
        service
            .prepare_retry_text(&account.id, &conversation.id, "retry-req-2")
            .unwrap();
        let inflight = service
            .prepare_retry_text(&account.id, &conversation.id, "retry-req-2")
            .unwrap_err();
        assert!(
            matches!(inflight, ServiceError::Api(ref error) if error.code == "RETRY_IN_FLIGHT")
        );

        // Unknown clientRequestId -> MESSAGE_NOT_FOUND; a row addressed from
        // another conversation answers INVALID_REQUEST.
        let missing = service
            .prepare_retry_text(&account.id, &conversation.id, "retry-req-absent")
            .unwrap_err();
        assert!(matches!(
            missing,
            ServiceError::Store(StoreError::MessageNotFound)
        ));
        let other = service
            .store_ref()
            .ensure_conversation(&account.id, "direct", "+15555550103", "Peer2")
            .unwrap();
        let cross = service
            .prepare_retry_text(&account.id, &other.id, "retry-req-1")
            .unwrap_err();
        assert!(matches!(cross, ServiceError::Api(ref error) if error.code == "INVALID_REQUEST"));
    }

    #[test]
    fn outgoing_sync_reconciles_by_signal_timestamp_and_client_request_id() {
        let (_temp, mut service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let conversation = service
            .store_ref()
            .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
            .unwrap();
        let pending_id = match service
            .prepare_send_text(
                &account.id,
                &conversation.id,
                "same text is not identity",
                "client-request-exact",
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch { pending_id, .. } => pending_id,
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        };
        let sync_receive = NormalizedReceive {
            timestamp: Some(99),
            content_kind: "syncMessage",
            direction: "outgoing",
            account_present: true,
            account: Some("+15555550100".into()),
            source: Some("+15555550101".into()),
            peer_name: None,
            group_id: None,
            text: Some("same text is not identity".into()),
            text_bytes: Some(25),
            text_truncated: false,
            quote: None,
            attachments: Vec::new(),
            rich: None,
            control: None,
        };
        service
            .ingest_receive(sync_receive.clone(), crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        assert_eq!(
            service
                .list_messages(&account.id, &conversation.id, 10, None)
                .unwrap()
                .items
                .len(),
            2
        );

        let (sent, _) = service
            .complete_send_success(&pending_id, &account.id, &conversation.id, 99)
            .unwrap();
        assert_eq!(
            sent.client_request_id.as_deref(),
            Some("client-request-exact")
        );
        let rows = service
            .list_messages(&account.id, &conversation.id, 10, None)
            .unwrap()
            .items;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, pending_id);

        assert!(
            service
                .ingest_receive(sync_receive, crate::DEFAULT_PROXY_GROUP_ID)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            service
                .list_messages(&account.id, &conversation.id, 10, None)
                .unwrap()
                .items
                .len(),
            1
        );
    }

    /// Regression: the pending row can be gone when an upstream send completes
    /// (the account's rows were deleted in a race). The message may already be
    /// delivered, so the completion must surface a final, non-retryable state —
    /// never INTERNAL_ERROR with retryable=true on a mutating operation.
    #[test]
    fn missing_pending_row_at_send_completion_is_final_not_retryable() {
        let (_temp, service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let conversation = service
            .store_ref()
            .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
            .unwrap();
        let pending_id = match service
            .prepare_send_text(
                &account.id,
                &conversation.id,
                "in flight",
                "req-race",
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch { pending_id, .. } => pending_id,
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        };

        // The deletion race: the account rows disappear while the upstream
        // send is still in flight.
        assert!(
            service
                .store_ref()
                .delete_account_cascade(&account.id)
                .unwrap()
        );

        let error = service
            .complete_send_success(&pending_id, &account.id, &conversation.id, 100)
            .unwrap_err();
        let ServiceError::Api(api) = error else {
            panic!("missing pending row must be an API error, got {error:?}");
        };
        assert_eq!(api.code, "SEND_OUTCOME_UNKNOWN");
        assert!(!api.retryable);

        // A pending id that never existed lands on the same non-retryable path.
        let error =
            match service.complete_send_success("absent", &account.id, &conversation.id, 100) {
                Err(ServiceError::Api(api)) => api,
                other => panic!("missing pending row must fail, got {other:?}"),
            };
        assert_eq!(error.code, "SEND_OUTCOME_UNKNOWN");
        assert!(!error.retryable);
    }

    #[test]
    fn signal_identity_keeps_group_senders_distinct_and_dedupes_replay() {
        let (_temp, mut service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let receive = |source: &str, text: &str| NormalizedReceive {
            timestamp: Some(200),
            content_kind: "dataMessage",
            direction: "incoming",
            account_present: true,
            account: Some("+15555550100".into()),
            source: Some(source.into()),
            peer_name: None,
            group_id: Some("group-one".into()),
            text: Some(text.into()),
            text_bytes: Some(text.len() as u32),
            text_truncated: false,
            quote: None,
            attachments: Vec::new(),
            rich: None,
            control: None,
        };

        let first = receive("peer-a", "first");
        assert!(
            !service
                .ingest_receive(first.clone(), crate::DEFAULT_PROXY_GROUP_ID)
                .unwrap()
                .is_empty()
        );
        assert!(
            !service
                .ingest_receive(receive("peer-b", "second"), crate::DEFAULT_PROXY_GROUP_ID)
                .unwrap()
                .is_empty()
        );
        assert!(
            service
                .ingest_receive(first, crate::DEFAULT_PROXY_GROUP_ID)
                .unwrap()
                .is_empty()
        );

        let conversation = service
            .list_conversations(&account.id, 10, None)
            .unwrap()
            .items
            .remove(0);
        assert_eq!(
            service
                .list_messages(&account.id, &conversation.id, 10, None)
                .unwrap()
                .items
                .len(),
            2
        );
    }

    /// Contract 1.27: an incoming message captures the envelope `sourceName`
    /// on the row (`senderName` on the wire record) and the conversation
    /// summary carries the last-message direction plus that author name —
    /// the metadata the desktop list needs for the official author-prefix
    /// preview. Outgoing rows stay name-less (the author is self).
    #[test]
    fn incoming_receive_captures_sender_name_into_summary_projection() {
        let (_temp, mut service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let receive = NormalizedReceive {
            timestamp: Some(200),
            content_kind: "dataMessage",
            direction: "incoming",
            account_present: true,
            account: Some("+15555550100".into()),
            source: Some("+15555550101".into()),
            peer_name: Some("林菲菲".into()),
            group_id: Some("group-one".into()),
            text: Some("你好".into()),
            text_bytes: Some(6),
            text_truncated: false,
            quote: None,
            attachments: Vec::new(),
            rich: None,
            control: None,
        };
        assert!(
            !service
                .ingest_receive(receive.clone(), crate::DEFAULT_PROXY_GROUP_ID)
                .unwrap()
                .is_empty()
        );

        let conversation = service
            .list_conversations(&account.id, 10, None)
            .unwrap()
            .items
            .remove(0);
        let summary = service
            .store
            .conversation_summary(&account.id, &conversation.id)
            .unwrap()
            .unwrap();
        assert_eq!(summary.last_message_direction, Some("incoming"));
        assert_eq!(summary.last_message_author_name.as_deref(), Some("林菲菲"));
        assert!(summary.last_message_reactions.is_empty());

        let row = service
            .list_messages(&account.id, &conversation.id, 10, None)
            .unwrap()
            .items
            .remove(0);
        assert_eq!(row.sender_name.as_deref(), Some("林菲菲"));
        let wire = serde_json::to_value(&row).unwrap();
        assert_eq!(wire["senderName"], "林菲菲");

        // The connector's own outgoing rows are the account itself: no name
        // is captured and the summary flips to outgoing with no author.
        let reply = NormalizedReceive {
            direction: "outgoing",
            timestamp: Some(300),
            content_kind: "syncMessage",
            source: Some("group-one".into()),
            text: Some("好的".into()),
            text_bytes: Some(6),
            ..receive
        };
        assert!(
            !service
                .ingest_receive(reply, crate::DEFAULT_PROXY_GROUP_ID)
                .unwrap()
                .is_empty()
        );
        let summary = service
            .store
            .conversation_summary(&account.id, &conversation.id)
            .unwrap()
            .unwrap();
        assert_eq!(summary.last_message_direction, Some("outgoing"));
        assert_eq!(summary.last_message_author_name, None);
    }

    #[test]
    fn receive_without_signal_timestamp_has_no_persistence_side_effects() {
        let (_temp, mut service) = service();
        let receive = NormalizedReceive {
            timestamp: None,
            content_kind: "dataMessage",
            direction: "incoming",
            account_present: true,
            account: Some("+15555550100".into()),
            source: Some("+15555550101".into()),
            peer_name: None,
            group_id: None,
            text: Some("unstable identity".into()),
            text_bytes: Some(17),
            text_truncated: false,
            quote: None,
            attachments: Vec::new(),
            rich: None,
            control: None,
        };

        assert!(
            service
                .ingest_receive(receive, crate::DEFAULT_PROXY_GROUP_ID)
                .unwrap()
                .is_empty()
        );
        assert!(
            service
                .list_accounts_in_group(crate::DEFAULT_PROXY_GROUP_ID)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn send_by_peer_creates_conversation_once_and_reuses_it() {
        let (_temp, service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);

        let first = match service
            .prepare_send_text_to_peer(
                &account.id,
                &PeerTarget {
                    kind: "contact",
                    peer_key: "+15555550109",
                    peer_title: Some("New Peer"),
                },
                "hello peer",
                "peer-req-1",
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch {
                conversation_id,
                params,
                ..
            } => {
                assert_eq!(params["recipient"], json!(["+15555550109"]));
                conversation_id
            }
            PreparedSend::Existing(_) => panic!("first peer send must dispatch"),
        };
        // The conversation appears with the peer title only alongside this send.
        let conversations = service.list_conversations(&account.id, 10, None).unwrap();
        assert_eq!(conversations.items.len(), 1);
        assert_eq!(conversations.items[0].id, first);
        assert_eq!(conversations.items[0].title, "New Peer");

        // A new client request to the same peer reuses the same conversation.
        let second = match service
            .prepare_send_text_to_peer(
                &account.id,
                &PeerTarget {
                    kind: "direct",
                    peer_key: "+15555550109",
                    peer_title: None,
                },
                "hello again",
                "peer-req-2",
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch {
                conversation_id, ..
            } => conversation_id,
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        };
        assert_eq!(second, first);
        assert_eq!(
            service
                .list_conversations(&account.id, 10, None)
                .unwrap()
                .items
                .len(),
            1
        );

        // A repeated client request is idempotent regardless of the target form.
        assert!(matches!(
            service
                .prepare_send_text_to_peer(
                    &account.id,
                    &PeerTarget {
                        kind: "contact",
                        peer_key: "+15555550109",
                        peer_title: None,
                    },
                    "hello peer",
                    "peer-req-1",
                    None,
                    None,
                    None
                )
                .unwrap(),
            PreparedSend::Existing(_)
        ));
        assert!(matches!(
            service
                .prepare_send_text(
                    &account.id,
                    &first,
                    "hello peer",
                    "peer-req-1",
                    None,
                    None,
                    None
                )
                .unwrap(),
            PreparedSend::Existing(_)
        ));
    }

    #[test]
    fn send_by_peer_validates_target_and_falls_back_to_masked_title() {
        let (_temp, service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);

        assert!(matches!(
            service.prepare_send_text_to_peer(
                &account.id,
                &PeerTarget {
                    kind: "channel",
                    peer_key: "+15555550109",
                    peer_title: None,
                },
                "text",
                "req-bad-kind",
                None,
             None, None),
            Err(ServiceError::Api(error)) if error.code == "INVALID_REQUEST"
        ));
        assert!(matches!(
            service.prepare_send_text_to_peer(
                &account.id,
                &PeerTarget {
                    kind: "contact",
                    peer_key: "",
                    peer_title: None,
                },
                "text",
                "req-empty-peer",
                None,
             None, None),
            Err(ServiceError::Api(error)) if error.code == "INVALID_REQUEST"
        ));
        assert!(matches!(
            service.prepare_send_text_to_peer(
                "absent-account",
                &PeerTarget {
                    kind: "contact",
                    peer_key: "+15555550109",
                    peer_title: None,
                },
                "text",
                "req-absent",
                None,
                None,
                None
            ),
            Err(ServiceError::Store(StoreError::AccountNotFound))
        ));

        match service
            .prepare_send_text_to_peer(
                &account.id,
                &PeerTarget {
                    kind: "group",
                    peer_key: "Z3JvdXAtaWQ=",
                    peer_title: None,
                },
                "group hello",
                "req-group",
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch { params, .. } => {
                assert_eq!(params["groupId"], json!("Z3JvdXAtaWQ="));
            }
            PreparedSend::Existing(_) => panic!("group peer send must dispatch"),
        }
        let group = service
            .store_ref()
            .conversation_by_peer(&account.id, "group", "Z3JvdXAtaWQ=")
            .unwrap()
            .unwrap();
        assert_eq!(
            service.store_ref().conversation_title(&group.id).unwrap(),
            Some("group".to_string())
        );

        match service
            .prepare_send_text_to_peer(
                &account.id,
                &PeerTarget {
                    kind: "direct",
                    peer_key: "+15555550110",
                    peer_title: None,
                },
                "masked hello",
                "req-masked",
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch {
                conversation_id, ..
            } => {
                let title = service
                    .store_ref()
                    .conversation_title(&conversation_id)
                    .unwrap()
                    .unwrap();
                assert!(title.contains("***"));
                assert!(!title.contains("555555"));
            }
            PreparedSend::Existing(_) => panic!("direct peer send must dispatch"),
        }
    }

    /// Phase 2 (docs/optimization-plan.md): quoteMessageId is delivered
    /// upstream. A quote of an incoming direct-chat message resolves to
    /// signal-cli's quoteTimestamp (the envelope timestamp, which is the
    /// protocol identity) and quoteAuthor (the peer number).
    #[test]
    fn quote_of_incoming_direct_message_becomes_upstream_quote_params() {
        let (_temp, mut service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let events = service
            .ingest_receive(
                NormalizedReceive {
                    timestamp: Some(777),
                    content_kind: "dataMessage",
                    direction: "incoming",
                    account_present: true,
                    account: Some("+15555550100".into()),
                    source: Some("+15555550101".into()),
                    peer_name: None,
                    group_id: None,
                    text: Some("quoted text".into()),
                    text_bytes: Some(11),
                    text_truncated: false,
                    quote: None,
                    attachments: Vec::new(),
                    rich: None,
                    control: None,
                },
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap();
        let quoted = events
            .iter()
            .find_map(|event| match event {
                HostSideEvent::MessageUpserted(message) => Some(message),
                _ => None,
            })
            .unwrap();

        match service
            .prepare_send_text(
                &account.id,
                &quoted.conversation_id,
                "reply with quote",
                "req-quote-1",
                Some(&quoted.id),
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch { params, .. } => {
                assert_eq!(params["quoteTimestamp"], json!(777));
                assert_eq!(params["quoteAuthor"], json!("+15555550101"));
                assert_eq!(params["recipient"], json!(["+15555550101"]));
                assert_eq!(params["account"], json!("+15555550100"));
            }
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        }

        // The local record keeps the opaque quoteMessageId for the renderer.
        let stored = service
            .store_ref()
            .message_by_client_request(&account.id, "req-quote-1")
            .unwrap()
            .unwrap();
        assert_eq!(stored.quote_message_id.as_deref(), Some(quoted.id.as_str()));
    }

    /// A quote of our own completed send resolves the author to the linked
    /// account and the timestamp to the upstream-assigned one.
    #[test]
    fn quote_of_own_sent_message_uses_account_number_and_upstream_timestamp() {
        let (_temp, service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let conversation = service
            .store_ref()
            .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
            .unwrap();
        let pending_id = match service
            .prepare_send_text(
                &account.id,
                &conversation.id,
                "original",
                "req-original",
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch { pending_id, .. } => pending_id,
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        };
        service
            .complete_send_success(&pending_id, &account.id, &conversation.id, 4242)
            .unwrap();

        match service
            .prepare_send_text(
                &account.id,
                &conversation.id,
                "quote own",
                "req-quote-own",
                Some(&pending_id),
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch { params, .. } => {
                assert_eq!(params["quoteTimestamp"], json!(4242));
                assert_eq!(params["quoteAuthor"], json!("+15555550100"));
            }
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        }
    }

    /// Deterministic rejections at validation time: unknown target, own
    /// not-yet-sent message, group message (member address is not persisted),
    /// and a message from another conversation. None of them may leave a
    /// pending row behind — the same clientRequestId must re-validate as new.
    #[test]
    fn unresolvable_quotes_are_rejected_before_any_pending_row_exists() {
        let (_temp, mut service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let conversation = service
            .store_ref()
            .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
            .unwrap();

        // Unknown quote target -> MESSAGE_NOT_FOUND, non-retryable.
        let api = service
            .prepare_send_text(
                &account.id,
                &conversation.id,
                "reply",
                "req-quote-missing",
                Some("no-such-message"),
                None,
                None,
            )
            .unwrap_err()
            .into_api();
        assert_eq!(api.code, "MESSAGE_NOT_FOUND");
        assert!(!api.retryable);

        // The rejection left no pending row: the same clientRequestId is a
        // fresh dispatch, not an idempotent replay of a stuck pending record.
        assert!(matches!(
            service
                .prepare_send_text(
                    &account.id,
                    &conversation.id,
                    "reply",
                    "req-quote-missing",
                    None,
                    None,
                    None
                )
                .unwrap(),
            PreparedSend::Dispatch { .. }
        ));

        // Quoting our own still-pending message would misquote upstream (its
        // sent_at is a local clock value until the send completes).
        let pending_id = match service
            .prepare_send_text(
                &account.id,
                &conversation.id,
                "in flight",
                "req-pending",
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch { pending_id, .. } => pending_id,
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        };
        assert!(matches!(
            service.prepare_send_text(
                &account.id,
                &conversation.id,
                "quote pending",
                "req-quote-pending",
                Some(&pending_id),
             None, None),
            Err(ServiceError::Api(error)) if error.code == "INVALID_REQUEST" && !error.retryable
        ));

        // A group incoming message's author address is not persisted (only a
        // local sender hash), so the quote cannot be constructed upstream.
        let group_events = service
            .ingest_receive(
                NormalizedReceive {
                    timestamp: Some(900),
                    content_kind: "dataMessage",
                    direction: "incoming",
                    account_present: true,
                    account: Some("+15555550100".into()),
                    source: Some("+15555550105".into()),
                    peer_name: None,
                    group_id: Some("group-one".into()),
                    text: Some("group text".into()),
                    text_bytes: Some(10),
                    text_truncated: false,
                    quote: None,
                    attachments: Vec::new(),
                    rich: None,
                    control: None,
                },
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap();
        let group_message = group_events
            .iter()
            .find_map(|event| match event {
                HostSideEvent::MessageUpserted(message) => Some(message),
                _ => None,
            })
            .unwrap();
        assert!(matches!(
            service.prepare_send_text(
                &account.id,
                &group_message.conversation_id,
                "quote group",
                "req-quote-group",
                Some(&group_message.id),
             None, None),
            Err(ServiceError::Api(error)) if error.code == "INVALID_REQUEST" && !error.retryable
        ));

        // A message from another conversation is not a valid quote target here.
        assert!(matches!(
            service.prepare_send_text(
                &account.id,
                &conversation.id,
                "cross quote",
                "req-quote-cross",
                Some(&group_message.id),
                None,
                None
            ),
            Err(ServiceError::Store(StoreError::MessageNotFound))
        ));
    }

    /// messages.remoteDelete resolves the local row to the upstream addressing:
    /// only an own outgoing `sent` row qualifies, its upstream-assigned
    /// `sent_at` becomes `targetTimestamp`, and the conversation's kind
    /// selects `recipient` (direct) vs `groupId` (group) — the same addressing
    /// shape as `send` (docs/remote-delete-l2-plan.md §3.2).
    #[test]
    fn remote_delete_maps_sent_outgoing_rows_to_upstream_params() {
        let (_temp, service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);

        // Direct conversation: recipient = [peer_key].
        let direct = service
            .store_ref()
            .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
            .unwrap();
        let direct_id = match service
            .prepare_send_text(
                &account.id,
                &direct.id,
                "delete me",
                "req-rd-1",
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch { pending_id, .. } => pending_id,
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        };
        service
            .complete_send_success(&direct_id, &account.id, &direct.id, 777)
            .unwrap();
        let prepared = service
            .prepare_remote_delete(&account.id, &direct.id, &direct_id)
            .unwrap();
        assert_eq!(prepared.params["account"], json!("+15555550100"));
        assert_eq!(prepared.params["targetTimestamp"], json!(777));
        assert_eq!(prepared.params["recipient"], json!(["+15555550101"]));
        assert!(prepared.params.get("groupId").is_none());
        assert_eq!(prepared.account_id, account.id);
        assert_eq!(prepared.conversation_id, direct.id);
        assert_eq!(prepared.message_id, direct_id);

        // Group conversation: groupId = peer_key, no recipient.
        service
            .sync_accounts_from_numbers(&["+15555550101".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let group = service
            .store_ref()
            .ensure_conversation(&account.id, "group", "Z3JvdXAtaWQ=", "group")
            .unwrap();
        let group_message_id = match service
            .prepare_send_text(
                &account.id,
                &group.id,
                "group delete",
                "req-rd-2",
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch { pending_id, .. } => pending_id,
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        };
        service
            .complete_send_success(&group_message_id, &account.id, &group.id, 888)
            .unwrap();
        let prepared = service
            .prepare_remote_delete(&account.id, &group.id, &group_message_id)
            .unwrap();
        assert_eq!(prepared.params["targetTimestamp"], json!(888));
        assert_eq!(prepared.params["groupId"], json!("Z3JvdXAtaWQ="));
        assert!(prepared.params.get("recipient").is_none());
    }

    /// The eligibility guard is deterministic and local: pending, failed and
    /// unknown rows (their sent_at is a local clock value, not a protocol
    /// identity) and incoming rows all answer MESSAGE_NOT_FOUND before any
    /// upstream call — the same rule as quote resolution.
    #[test]
    fn remote_delete_rejects_rows_without_a_protocol_identity() {
        let (_temp, mut service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let conversation = service
            .store_ref()
            .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
            .unwrap();
        let pending_id = match service
            .prepare_send_text(
                &account.id,
                &conversation.id,
                "in flight",
                "req-rd-p",
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch { pending_id, .. } => pending_id,
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        };
        for status in ["pending", "failed", "unknown"] {
            service
                .store_ref()
                .update_message_status(&pending_id, status, None)
                .unwrap();
            assert!(
                service
                    .prepare_remote_delete(&account.id, &conversation.id, &pending_id)
                    .is_err(),
                "a {status} row must not be deletable"
            );
        }

        // The incoming row of the same conversation is not deletable either.
        let events = service
            .ingest_receive(
                NormalizedReceive {
                    timestamp: Some(50),
                    content_kind: "dataMessage",
                    direction: "incoming",
                    account_present: true,
                    account: Some("+15555550100".into()),
                    source: Some("+15555550101".into()),
                    peer_name: None,
                    group_id: None,
                    text: Some("peer text".into()),
                    text_bytes: Some(9),
                    text_truncated: false,
                    quote: None,
                    attachments: Vec::new(),
                    rich: None,
                    control: None,
                },
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap();
        let incoming = events
            .iter()
            .find_map(|event| match event {
                HostSideEvent::MessageUpserted(message) => Some(message),
                _ => None,
            })
            .unwrap();
        let incoming_error = service
            .prepare_remote_delete(&account.id, &conversation.id, &incoming.id)
            .unwrap_err();
        assert_eq!(incoming_error.into_api().code, "MESSAGE_NOT_FOUND");

        // A never-existing row, conversation, or account answer their own
        // deterministic codes.
        assert_eq!(
            service
                .prepare_remote_delete(&account.id, &conversation.id, "absent-message")
                .unwrap_err()
                .into_api()
                .code,
            "MESSAGE_NOT_FOUND"
        );
        assert_eq!(
            service
                .prepare_remote_delete(&account.id, "absent-conversation", "anything")
                .unwrap_err()
                .into_api()
                .code,
            "CONVERSATION_NOT_FOUND"
        );
        assert_eq!(
            service
                .prepare_remote_delete("absent-account", "absent-conversation", "anything")
                .unwrap_err()
                .into_api()
                .code,
            "ACCOUNT_NOT_FOUND"
        );
    }

    /// messages.sendReaction resolves the local row to the upstream
    /// addressing: the target author follows the row direction — the linked
    /// account's own number for a `sent` outgoing row, the peer for an
    /// incoming direct row — and the conversation's kind selects `recipient`
    /// vs `groupId` (docs/remote-delete-l2-plan.md §3.2 shape, contract
    /// revision 1.7). remove passes through as an explicit boolean.
    #[test]
    fn send_reaction_maps_rows_to_the_direction_derived_target_author() {
        let (_temp, mut service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let direct = service
            .store_ref()
            .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
            .unwrap();

        // Outgoing `sent` row: targetAuthor is the linked account itself.
        let sent_id = match service
            .prepare_send_text(
                &account.id,
                &direct.id,
                "react to me",
                "req-sr-1",
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch { pending_id, .. } => pending_id,
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        };
        service
            .complete_send_success(&sent_id, &account.id, &direct.id, 777)
            .unwrap();
        let prepared = service
            .prepare_send_reaction(&account.id, &direct.id, &sent_id, "👍", false)
            .unwrap();
        assert_eq!(prepared.params["account"], json!("+15555550100"));
        assert_eq!(prepared.params["emoji"], json!("👍"));
        assert_eq!(prepared.params["remove"], json!(false));
        assert_eq!(prepared.params["targetAuthor"], json!("+15555550100"));
        assert_eq!(prepared.params["targetTimestamp"], json!(777));
        assert_eq!(prepared.params["recipient"], json!(["+15555550101"]));
        assert!(prepared.params.get("groupId").is_none());
        assert_eq!(prepared.account_id, account.id);
        assert_eq!(prepared.conversation_id, direct.id);
        assert_eq!(prepared.message_id, sent_id);

        // Incoming row (direct chat): targetAuthor is the peer and the
        // envelope timestamp is the protocol identity.
        let events = service
            .ingest_receive(
                NormalizedReceive {
                    timestamp: Some(50),
                    content_kind: "dataMessage",
                    direction: "incoming",
                    account_present: true,
                    account: Some("+15555550100".into()),
                    source: Some("+15555550101".into()),
                    peer_name: None,
                    group_id: None,
                    text: Some("peer text".into()),
                    text_bytes: Some(9),
                    text_truncated: false,
                    quote: None,
                    attachments: Vec::new(),
                    rich: None,
                    control: None,
                },
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap();
        let incoming = events
            .iter()
            .find_map(|event| match event {
                HostSideEvent::MessageUpserted(message) => Some(message),
                _ => None,
            })
            .unwrap();
        let prepared = service
            .prepare_send_reaction(&account.id, &direct.id, &incoming.id, "🎉", true)
            .unwrap();
        assert_eq!(prepared.params["targetAuthor"], json!("+15555550101"));
        assert_eq!(prepared.params["targetTimestamp"], json!(50));
        assert_eq!(prepared.params["remove"], json!(true));

        // Group conversation: groupId addressing, no recipient.
        let group = service
            .store_ref()
            .ensure_conversation(&account.id, "group", "Z3JvdXAtaWQ=", "group")
            .unwrap();
        let group_message_id = match service
            .prepare_send_text(
                &account.id,
                &group.id,
                "group react",
                "req-sr-2",
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch { pending_id, .. } => pending_id,
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        };
        service
            .complete_send_success(&group_message_id, &account.id, &group.id, 888)
            .unwrap();
        let prepared = service
            .prepare_send_reaction(&account.id, &group.id, &group_message_id, "👍", false)
            .unwrap();
        assert_eq!(prepared.params["targetAuthor"], json!("+15555550100"));
        assert_eq!(prepared.params["targetTimestamp"], json!(888));
        assert_eq!(prepared.params["groupId"], json!("Z3JvdXAtaWQ="));
        assert!(prepared.params.get("recipient").is_none());
    }

    /// The eligibility guard is deterministic and local: rows without a
    /// resolvable protocol identity (missing, pending/failed/unknown, system)
    /// answer MESSAGE_NOT_FOUND, and a group incoming row's author address is
    /// not persisted at all — a deterministic INVALID_REQUEST, never a
    /// mis-addressed upstream reaction (quote-resolution precedent).
    #[test]
    fn send_reaction_rejects_rows_without_a_resolvable_target_author() {
        let (_temp, mut service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let conversation = service
            .store_ref()
            .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
            .unwrap();
        let pending_id = match service
            .prepare_send_text(
                &account.id,
                &conversation.id,
                "in flight",
                "req-sr-p",
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch { pending_id, .. } => pending_id,
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        };
        for status in ["pending", "failed", "unknown"] {
            service
                .store_ref()
                .update_message_status(&pending_id, status, None)
                .unwrap();
            let error = service
                .prepare_send_reaction(&account.id, &conversation.id, &pending_id, "👍", false)
                .unwrap_err();
            assert_eq!(
                error.into_api().code,
                "MESSAGE_NOT_FOUND",
                "a {status} row must not be reactable"
            );
        }

        // A group incoming message's author address is not persisted (only a
        // local sender hash), so the reaction cannot be addressed upstream.
        service
            .sync_accounts_from_numbers(&["+15555550101".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let group = service
            .store_ref()
            .ensure_conversation(&account.id, "group", "group-one", "group")
            .unwrap();
        let group_events = service
            .ingest_receive(
                NormalizedReceive {
                    timestamp: Some(900),
                    content_kind: "dataMessage",
                    direction: "incoming",
                    account_present: true,
                    account: Some("+15555550100".into()),
                    source: Some("+15555550105".into()),
                    peer_name: None,
                    group_id: Some("group-one".into()),
                    text: Some("group text".into()),
                    text_bytes: Some(10),
                    text_truncated: false,
                    quote: None,
                    attachments: Vec::new(),
                    rich: None,
                    control: None,
                },
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap();
        let group_message = group_events
            .iter()
            .find_map(|event| match event {
                HostSideEvent::MessageUpserted(message) => Some(message),
                _ => None,
            })
            .unwrap();
        assert_eq!(
            service
                .prepare_send_reaction(&account.id, &group.id, &group_message.id, "👍", false)
                .unwrap_err()
                .into_api()
                .code,
            "INVALID_REQUEST"
        );

        // A never-existing row, conversation, or account answer their own
        // deterministic codes.
        assert_eq!(
            service
                .prepare_send_reaction(&account.id, &conversation.id, "absent-message", "👍", false)
                .unwrap_err()
                .into_api()
                .code,
            "MESSAGE_NOT_FOUND"
        );
        assert_eq!(
            service
                .prepare_send_reaction(&account.id, "absent-conversation", "anything", "👍", false)
                .unwrap_err()
                .into_api()
                .code,
            "CONVERSATION_NOT_FOUND"
        );
        assert_eq!(
            service
                .prepare_send_reaction(
                    "absent-account",
                    "absent-conversation",
                    "anything",
                    "👍",
                    false
                )
                .unwrap_err()
                .into_api()
                .code,
            "ACCOUNT_NOT_FOUND"
        );
    }

    /// Receipt-advanced outgoing rows stay addressable: delivery/read
    /// receipts only advance `status` while `update_message_status` preserves
    /// the upstream `sent_at`, so a delivered or read row reacts with the
    /// same protocol identity it had at `sent` (official behavior — reacting
    /// to an already-delivered message is the common case).
    #[test]
    fn send_reaction_accepts_receipt_advanced_outgoing_rows() {
        let (_temp, service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let conversation = service
            .store_ref()
            .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
            .unwrap();
        let message_id = match service
            .prepare_send_text(
                &account.id,
                &conversation.id,
                "delivered then read",
                "req-sr-advance",
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch { pending_id, .. } => pending_id,
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        };
        service
            .complete_send_success(&message_id, &account.id, &conversation.id, 777)
            .unwrap();
        for status in ["sent", "delivered", "read"] {
            service
                .store_ref()
                .update_message_status(&message_id, status, None)
                .unwrap();
            let prepared = service
                .prepare_send_reaction(&account.id, &conversation.id, &message_id, "👍", false)
                .unwrap();
            assert_eq!(prepared.params["targetAuthor"], json!("+15555550100"));
            assert_eq!(prepared.params["targetTimestamp"], json!(777));
        }
    }

    /// 上行确认后的 complete 落库：own 回应以 account 为 actor 持久化，pill
    /// 带 mine=true、actor 标 self；remove=true 撤回后 pill 消失。app 的乐观
    /// 窗口结束后读侧仍一致（supervisor 侧另推 conversation.changed）。
    #[test]
    fn complete_send_reaction_persists_own_pill_and_removal() {
        let (_temp, service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let conversation = service
            .store_ref()
            .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
            .unwrap();
        let message_id = match service
            .prepare_send_text(
                &account.id,
                &conversation.id,
                "reaction target",
                "req-sr-complete",
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch { pending_id, .. } => pending_id,
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        };
        service
            .complete_send_success(&message_id, &account.id, &conversation.id, 888)
            .unwrap();
        let prepared = service
            .prepare_send_reaction(&account.id, &conversation.id, &message_id, "👍", false)
            .unwrap();
        assert!(
            service
                .complete_send_reaction(
                    &prepared.account_id,
                    &prepared.conversation_id,
                    &prepared.emoji,
                    prepared.remove,
                    prepared.target_sent_at,
                )
                .unwrap()
                .is_some()
        );
        let reactions = service
            .store_ref()
            .message_by_id(&account.id, &conversation.id, &message_id)
            .unwrap()
            .unwrap()
            .reactions;
        assert_eq!(reactions.len(), 1);
        assert_eq!(reactions[0].emoji, "👍");
        assert!(reactions[0].mine);
        assert!(reactions[0].actors.iter().all(|actor| actor.is_self));

        let prepared = service
            .prepare_send_reaction(&account.id, &conversation.id, &message_id, "👍", true)
            .unwrap();
        service
            .complete_send_reaction(
                &prepared.account_id,
                &prepared.conversation_id,
                &prepared.emoji,
                prepared.remove,
                prepared.target_sent_at,
            )
            .unwrap();
        let reactions = service
            .store_ref()
            .message_by_id(&account.id, &conversation.id, &message_id)
            .unwrap()
            .unwrap()
            .reactions;
        assert!(reactions.is_empty());
    }

    /// One pin-family prepare callable under a uniform signature, so the
    /// guard loop drives all three methods over the same failing row.
    type PinPrepare<'a> =
        Box<dyn Fn(&ConnectorService, &str) -> Result<PreparedPin, ServiceError> + 'a>;

    /// Contract 1.33 pin family addressing: the §4.6 reaction model reused
    /// verbatim — an addressable outgoing row targets the linked account
    /// itself, a direct-chat incoming row targets the peer, groups map to
    /// `groupId` addressing, and `pinDurationSeconds` rides the upstream
    /// params only when the caller supplied one (absent = the official
    /// forever pin, the key is not sent).
    #[test]
    fn pin_family_maps_rows_with_the_reaction_addressing_model() {
        let (_temp, mut service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let direct = service
            .store_ref()
            .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
            .unwrap();
        let sent_id = match service
            .prepare_send_text(
                &account.id,
                &direct.id,
                "pin me",
                "req-pin-1",
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch { pending_id, .. } => pending_id,
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        };
        service
            .complete_send_success(&sent_id, &account.id, &direct.id, 777)
            .unwrap();

        // Pin with a duration: every upstream key carries the reaction
        // addressing plus the passthrough `pinDurationSeconds`.
        let prepared = service
            .prepare_send_pin_message(&account.id, &direct.id, &sent_id, Some(3600))
            .unwrap();
        assert_eq!(prepared.params["account"], json!("+15555550100"));
        assert_eq!(prepared.params["targetAuthor"], json!("+15555550100"));
        assert_eq!(prepared.params["targetTimestamp"], json!(777));
        assert_eq!(prepared.params["pinDurationSeconds"], json!(3600));
        assert_eq!(prepared.params["recipient"], json!(["+15555550101"]));
        assert!(prepared.params.get("groupId").is_none());
        assert_eq!(prepared.target_author, "+15555550100");
        assert_eq!(prepared.target_sent_at, 777);
        assert_eq!(prepared.pin_duration_seconds, Some(3600));

        // Pin without a duration: the key stays absent (forever pin).
        let prepared = service
            .prepare_send_pin_message(&account.id, &direct.id, &sent_id, None)
            .unwrap();
        assert!(prepared.params.get("pinDurationSeconds").is_none());
        assert_eq!(prepared.pin_duration_seconds, None);

        // Unpin: the same addressing, no duration field at all.
        let prepared = service
            .prepare_send_unpin_message(&account.id, &direct.id, &sent_id)
            .unwrap();
        assert_eq!(prepared.params["targetAuthor"], json!("+15555550100"));
        assert_eq!(prepared.params["targetTimestamp"], json!(777));
        assert!(prepared.params.get("pinDurationSeconds").is_none());
        assert_eq!(prepared.pin_duration_seconds, None);

        // Incoming direct row: the target author is the peer and the
        // envelope timestamp is the protocol identity.
        let events = service
            .ingest_receive(
                NormalizedReceive {
                    timestamp: Some(50),
                    content_kind: "dataMessage",
                    direction: "incoming",
                    account_present: true,
                    account: Some("+15555550100".into()),
                    source: Some("+15555550101".into()),
                    peer_name: None,
                    group_id: None,
                    text: Some("peer pin target".into()),
                    text_bytes: Some(15),
                    text_truncated: false,
                    quote: None,
                    attachments: Vec::new(),
                    rich: None,
                    control: None,
                },
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap();
        let incoming = events
            .iter()
            .find_map(|event| match event {
                HostSideEvent::MessageUpserted(message) => Some(message),
                _ => None,
            })
            .unwrap();
        let prepared = service
            .prepare_send_pin_message(&account.id, &direct.id, &incoming.id, None)
            .unwrap();
        assert_eq!(prepared.params["targetAuthor"], json!("+15555550101"));
        assert_eq!(prepared.params["targetTimestamp"], json!(50));

        // Group conversation: groupId addressing for the pin and the admin
        // delete, no recipient array.
        let group = service
            .store_ref()
            .ensure_conversation(&account.id, "group", "Z3JvdXAtaWQ=", "group")
            .unwrap();
        let group_message_id = match service
            .prepare_send_text(
                &account.id,
                &group.id,
                "group pin target",
                "req-pin-group",
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch { pending_id, .. } => pending_id,
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        };
        service
            .complete_send_success(&group_message_id, &account.id, &group.id, 888)
            .unwrap();
        let prepared = service
            .prepare_send_pin_message(&account.id, &group.id, &group_message_id, None)
            .unwrap();
        assert_eq!(prepared.params["groupId"], json!("Z3JvdXAtaWQ="));
        assert!(prepared.params.get("recipient").is_none());
        let prepared = service
            .prepare_send_admin_delete(&account.id, &group.id, &group_message_id)
            .unwrap();
        assert_eq!(prepared.params["groupId"], json!("Z3JvdXAtaWQ="));
        assert_eq!(prepared.params["targetAuthor"], json!("+15555550100"));
        assert_eq!(prepared.params["targetTimestamp"], json!(888));
        assert!(prepared.params.get("recipient").is_none());
        assert!(prepared.params.get("pinDurationSeconds").is_none());
    }

    /// The pin family eligibility guard is the reaction guard: rows without a
    /// resolvable protocol identity answer MESSAGE_NOT_FOUND, a group
    /// incoming row's author is not resolvable (INVALID_REQUEST), and a
    /// direct chat has no admin concept — sendAdminDelete fails closed
    /// INVALID_REQUEST before any upstream call.
    #[test]
    fn pin_family_rejects_unaddressable_and_out_of_scope_targets() {
        let (_temp, mut service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let conversation = service
            .store_ref()
            .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
            .unwrap();
        let pending_id = match service
            .prepare_send_text(
                &account.id,
                &conversation.id,
                "in flight",
                "req-pin-pending",
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch { pending_id, .. } => pending_id,
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        };
        for status in ["pending", "failed", "unknown"] {
            service
                .store_ref()
                .update_message_status(&pending_id, status, None)
                .unwrap();
            let prepares: Vec<PinPrepare> = vec![
                Box::new(|service, id| {
                    service.prepare_send_pin_message(&account.id, &conversation.id, id, None)
                }),
                Box::new(|service, id| {
                    service.prepare_send_unpin_message(&account.id, &conversation.id, id)
                }),
                Box::new(|service, id| {
                    service.prepare_send_admin_delete(&account.id, &conversation.id, id)
                }),
            ];
            for prepare in prepares {
                let error = prepare(&service, &pending_id).unwrap_err();
                assert_eq!(
                    error.into_api().code,
                    "MESSAGE_NOT_FOUND",
                    "a {status} row must not be pin-family addressable"
                );
            }
        }

        // A group incoming message's author address is not persisted, so no
        // pin-family method can be addressed upstream.
        let group = service
            .store_ref()
            .ensure_conversation(&account.id, "group", "group-one", "group")
            .unwrap();
        let group_events = service
            .ingest_receive(
                NormalizedReceive {
                    timestamp: Some(900),
                    content_kind: "dataMessage",
                    direction: "incoming",
                    account_present: true,
                    account: Some("+15555550100".into()),
                    source: Some("+15555550105".into()),
                    peer_name: None,
                    group_id: Some("group-one".into()),
                    text: Some("group text".into()),
                    text_bytes: Some(10),
                    text_truncated: false,
                    quote: None,
                    attachments: Vec::new(),
                    rich: None,
                    control: None,
                },
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap();
        let group_message = group_events
            .iter()
            .find_map(|event| match event {
                HostSideEvent::MessageUpserted(message) => Some(message),
                _ => None,
            })
            .unwrap();
        let prepares: Vec<PinPrepare> = vec![
            Box::new(|service, id| {
                service.prepare_send_pin_message(&account.id, &group.id, id, None)
            }),
            Box::new(|service, id| service.prepare_send_unpin_message(&account.id, &group.id, id)),
            Box::new(|service, id| service.prepare_send_admin_delete(&account.id, &group.id, id)),
        ];
        for prepare in prepares {
            assert_eq!(
                prepare(&service, &group_message.id)
                    .unwrap_err()
                    .into_api()
                    .code,
                "INVALID_REQUEST"
            );
        }

        // A direct chat has no admin: sendAdminDelete answers INVALID_REQUEST
        // even on a perfectly addressable row.
        let sent_id = match service
            .prepare_send_text(
                &account.id,
                &conversation.id,
                "direct row",
                "req-pin-direct",
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch { pending_id, .. } => pending_id,
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        };
        service
            .complete_send_success(&sent_id, &account.id, &conversation.id, 410)
            .unwrap();
        assert_eq!(
            service
                .prepare_send_admin_delete(&account.id, &conversation.id, &sent_id)
                .unwrap_err()
                .into_api()
                .code,
            "INVALID_REQUEST"
        );

        // Missing rows, conversations, and accounts answer their own
        // deterministic codes.
        assert_eq!(
            service
                .prepare_send_pin_message(&account.id, &conversation.id, "absent-message", None)
                .unwrap_err()
                .into_api()
                .code,
            "MESSAGE_NOT_FOUND"
        );
        assert_eq!(
            service
                .prepare_send_unpin_message(&account.id, "absent-conversation", "anything")
                .unwrap_err()
                .into_api()
                .code,
            "CONVERSATION_NOT_FOUND"
        );
        assert_eq!(
            service
                .prepare_send_admin_delete("absent-account", "absent-conversation", "anything")
                .unwrap_err()
                .into_api()
                .code,
            "ACCOUNT_NOT_FOUND"
        );
    }

    /// 上行确认后的 complete 落库：pin 写入每会话 pinned 状态（新 pin 替换
    /// 旧 pin，timed pin 带 connector 时钟 expiry，forever pin 无
    /// expiresAt），摘要以 camelCase `pinnedMessage` 投影；不匹配的 unpin
    /// 不动新 pin（返回 None），匹配的 unpin 清除并返回刷新摘要。
    #[test]
    fn complete_pin_unpin_write_replace_and_clear_the_pinned_state() {
        let (_temp, mut service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let conversation = service
            .store_ref()
            .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
            .unwrap();
        let own_row = match service
            .prepare_send_text(
                &account.id,
                &conversation.id,
                "pin target one",
                "req-pin-c1",
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch { pending_id, .. } => pending_id,
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        };
        service
            .complete_send_success(&own_row, &account.id, &conversation.id, 888)
            .unwrap();

        // Timed pin on the own row: the summary projects the pin bar with an
        // expiry computed from the duration.
        let summary = service
            .complete_send_pin_message(
                &account.id,
                &conversation.id,
                "+15555550100",
                888,
                Some(3600),
            )
            .unwrap()
            .unwrap();
        let wire = serde_json::to_value(&summary).unwrap();
        let pinned = &wire["pinnedMessage"];
        assert_eq!(pinned["messageId"], json!(own_row));
        assert_eq!(pinned["targetAuthor"], json!("+15555550100"));
        assert_eq!(pinned["targetSentTimestamp"], json!(888));
        assert!(pinned["pinnedAt"].is_u64());
        assert!(
            pinned["expiresAt"].is_u64(),
            "a timed pin carries expiresAt"
        );
        assert_eq!(
            summary
                .pinned_message
                .as_ref()
                .unwrap()
                .target_sent_timestamp,
            888
        );

        // A newer pin replaces the older one: pin the peer row (forever) and
        // the summary follows.
        let events = service
            .ingest_receive(
                NormalizedReceive {
                    timestamp: Some(50),
                    content_kind: "dataMessage",
                    direction: "incoming",
                    account_present: true,
                    account: Some("+15555550100".into()),
                    source: Some("+15555550101".into()),
                    peer_name: None,
                    group_id: None,
                    text: Some("peer pin target".into()),
                    text_bytes: Some(15),
                    text_truncated: false,
                    quote: None,
                    attachments: Vec::new(),
                    rich: None,
                    control: None,
                },
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap();
        let incoming_id = events
            .iter()
            .find_map(|event| match event {
                HostSideEvent::MessageUpserted(message) => Some(message.id.clone()),
                _ => None,
            })
            .unwrap();
        let summary = service
            .complete_send_pin_message(&account.id, &conversation.id, "+15555550101", 50, None)
            .unwrap()
            .unwrap();
        let pinned = summary.pinned_message.as_ref().unwrap();
        assert_eq!(pinned.message_id, incoming_id);
        assert_eq!(pinned.expires_at, None, "a forever pin carries no expiry");
        let wire = serde_json::to_value(&summary).unwrap();
        assert!(wire["pinnedMessage"].get("expiresAt").is_none());

        // A stale unpin naming the replaced (author, timestamp) stays
        // silent: complete answers None and the newer pin survives.
        assert!(
            service
                .complete_send_unpin_message(&account.id, &conversation.id, "+15555550100", 888)
                .unwrap()
                .is_none()
        );
        let pinned = service
            .store_ref()
            .conversation_summary(&account.id, &conversation.id)
            .unwrap()
            .unwrap()
            .pinned_message
            .unwrap();
        assert_eq!(pinned.target_sent_timestamp, 50);

        // The matching unpin clears the state and returns the refreshed
        // summary (pinnedMessage absent again).
        let summary = service
            .complete_send_unpin_message(&account.id, &conversation.id, "+15555550101", 50)
            .unwrap()
            .unwrap();
        assert!(summary.pinned_message.is_none());
    }

    /// 收件面（contract 1.33）：peer 的 pinMessage 更新每会话 pinned 状态并
    /// 推 conversation.changed（摘要带 pinnedMessage）；不匹配的 unpinMessage
    /// 静默；匹配的清除；adminDelete 落行级 adminDeleted 墓碑（status 不动、
    /// body 保留）——若被删行正是 pinned 行则一并清 pin。
    #[test]
    fn inbound_pin_family_updates_pinned_state_and_admin_delete_marks_rows() {
        let (_temp, mut service) = service();
        let (account, conversation) = linked_account_and_conversation(&mut service);
        let message_id = seed_outgoing_sent(&mut service, &account.id, &conversation.id, 400);

        // Peer pins our message with a timed duration: conversation.changed
        // carries the pinned summary.
        let events = service
            .ingest_receive(
                control_receive(
                    "+15555550100",
                    "+15555550101",
                    ControlReceive::PinMessage {
                        target_author: "+15555550100".into(),
                        target_timestamp: 400,
                        duration_seconds: Some(300),
                    },
                ),
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap();
        let changed = events
            .iter()
            .find_map(|event| match event {
                HostSideEvent::ConversationChanged(summary) => Some(summary),
                _ => None,
            })
            .expect("an inbound pin answers conversation.changed");
        let pinned = changed.pinned_message.as_ref().unwrap();
        assert_eq!(pinned.message_id, message_id);
        assert_eq!(pinned.target_author, "+15555550100");
        assert_eq!(pinned.target_sent_timestamp, 400);
        assert!(pinned.expires_at.is_some(), "the timed pin carries expiry");

        // An unpin naming another message stays silent (no event) and leaves
        // the pin alone.
        let events = service
            .ingest_receive(
                control_receive(
                    "+15555550100",
                    "+15555550101",
                    ControlReceive::UnpinMessage {
                        target_author: "+15555550100".into(),
                        target_timestamp: 999,
                    },
                ),
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap();
        assert!(events.is_empty(), "a mismatched unpin must be silent");
        assert!(
            service
                .store_ref()
                .conversation_summary(&account.id, &conversation.id)
                .unwrap()
                .unwrap()
                .pinned_message
                .is_some()
        );

        // The matching unpin clears the state and notifies once.
        let events = service
            .ingest_receive(
                control_receive(
                    "+15555550100",
                    "+15555550101",
                    ControlReceive::UnpinMessage {
                        target_author: "+15555550100".into(),
                        target_timestamp: 400,
                    },
                ),
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap();
        let changed = events
            .iter()
            .find_map(|event| match event {
                HostSideEvent::ConversationChanged(summary) => Some(summary),
                _ => None,
            })
            .expect("a matching unpin answers conversation.changed");
        assert!(changed.pinned_message.is_none());

        // Re-pin (forever), then a group admin deletes the pinned row: the
        // row flips its additive tombstone (status ladder untouched, body
        // kept) and the pinned state clears with it.
        service
            .ingest_receive(
                control_receive(
                    "+15555550100",
                    "+15555550101",
                    ControlReceive::PinMessage {
                        target_author: "+15555550100".into(),
                        target_timestamp: 400,
                        duration_seconds: None,
                    },
                ),
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap();
        let events = service
            .ingest_receive(
                control_receive(
                    "+15555550100",
                    "+15555550101",
                    ControlReceive::AdminDelete {
                        target_author: "+15555550100".into(),
                        target_timestamp: 400,
                    },
                ),
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap();
        let upserted = events
            .iter()
            .find_map(|event| match event {
                HostSideEvent::MessageUpserted(record) => Some(record),
                _ => None,
            })
            .expect("an admin delete answers message.upserted");
        assert_eq!(upserted.id, message_id);
        assert!(upserted.admin_deleted, "the tombstone flips");
        assert_eq!(upserted.status, "sent", "the status ladder is untouched");
        assert_eq!(upserted.text.as_deref(), Some("original"));
        let changed = events
            .iter()
            .find_map(|event| match event {
                HostSideEvent::ConversationChanged(summary) => Some(summary),
                _ => None,
            })
            .expect("clearing the pinned row refreshes the conversation");
        assert!(changed.pinned_message.is_none());

        // A replayed adminDelete flips nothing: no events, state unchanged.
        let events = service
            .ingest_receive(
                control_receive(
                    "+15555550100",
                    "+15555550101",
                    ControlReceive::AdminDelete {
                        target_author: "+15555550100".into(),
                        target_timestamp: 400,
                    },
                ),
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap();
        assert!(events.is_empty(), "a replayed admin delete must be silent");
    }

    /// The emoji guard enforces the pinned signal-cli contract locally:
    /// exactly one unicode grapheme cluster, 1-32 UTF-8 bytes. Multi-codepoint
    /// clusters (ZWJ family emoji, skin-tone modifiers) stay valid.
    #[test]
    fn emoji_validation_accepts_single_clusters_and_rejects_the_rest() {
        assert!(validate_emoji("👍").is_ok());
        assert!(validate_emoji("❤️").is_ok());
        assert!(validate_emoji("👍🏽").is_ok());
        assert!(validate_emoji("👨‍👩‍👧‍👦").is_ok());
        // One 32-byte ZWJ cluster: the byte bound, still a single grapheme.
        assert!(validate_emoji("👨‍👩‍👧‍👦‍👍").is_ok());

        for bad in [
            "",
            "ab",
            "👍👍",
            &"x".repeat(33),
            "👨‍👩‍👧‍👦‍👍🏽",             // 36 bytes in one cluster: over the bound
            &"👍".repeat(17), // 68 bytes: over the bound
        ] {
            assert!(
                validate_emoji(bad).is_err(),
                "emoji {bad:?} must be rejected"
            );
        }
    }

    /// message.statusChanged is produced on real transitions only: sent after
    /// completion, unknown/failed on terminal resolution — and never twice for
    /// a replayed write or a vanished row.
    #[test]
    fn status_changed_events_fire_once_per_real_transition() {
        let (_temp, service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let conversation = service
            .store_ref()
            .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
            .unwrap();
        let status_events = |events: &[HostSideEvent]| {
            events
                .iter()
                .filter_map(|event| match event {
                    HostSideEvent::MessageStatusChanged {
                        account_id,
                        message_id,
                        status,
                        ..
                    } => Some((account_id.clone(), message_id.clone(), *status)),
                    _ => None,
                })
                .collect::<Vec<_>>()
        };

        let pending_id = match service
            .prepare_send_text(
                &account.id,
                &conversation.id,
                "status flow",
                "req-status",
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch { pending_id, .. } => pending_id,
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        };

        let (_, events) = service
            .complete_send_success(&pending_id, &account.id, &conversation.id, 55)
            .unwrap();
        assert_eq!(
            status_events(&events),
            vec![(account.id.clone(), pending_id.clone(), "sent")]
        );

        // A replayed completion reports no transition -> no duplicate event.
        let (_, replayed) = service
            .complete_send_success(&pending_id, &account.id, &conversation.id, 55)
            .unwrap();
        assert!(status_events(&replayed).is_empty());

        // Terminal failure paths emit exactly once as well.
        let failed_id = match service
            .prepare_send_text(
                &account.id,
                &conversation.id,
                "will fail",
                "req-fail",
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch { pending_id, .. } => pending_id,
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        };
        let failed = service.complete_send_failed(&failed_id).unwrap();
        assert_eq!(
            status_events(&failed),
            vec![(account.id.clone(), failed_id.clone(), "failed")]
        );
        assert!(service.complete_send_failed(&failed_id).unwrap().is_empty());

        let unknown_id = match service
            .prepare_send_text(
                &account.id,
                &conversation.id,
                "will vanish",
                "req-unknown",
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch { pending_id, .. } => pending_id,
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        };
        let unknown = service.complete_send_unknown(&unknown_id).unwrap();
        assert_eq!(
            status_events(&unknown),
            vec![(account.id.clone(), unknown_id.clone(), "unknown")]
        );
        assert!(
            service
                .complete_send_unknown(&unknown_id)
                .unwrap()
                .is_empty()
        );

        // A row that never existed produces no phantom event.
        assert!(service.complete_send_unknown("absent").unwrap().is_empty());
    }

    /// The returned payload must be standard padded base64 decoding to
    /// exactly the declared size (contract revision 1.8): the encoded length
    /// is bounded before any allocation, and the padding arithmetic matches
    /// the sizes java.util.Base64 emits (implementation-plan §4.7).
    #[test]
    fn validate_attachment_payload_checks_declared_size_exactly() {
        // 3n and 3n+1 / 3n+2 byte payloads with their padded encodings.
        for (decoded, encoded) in [
            (0_u64, ""), // rejected: empty is never a valid attachment
            (24, "Zml4dHVyZSBhdHRhY2htZW50IGJ5dGVz"),
            (1, "YQ=="),
            (2, "YWI="),
            (3, "YWJj"),
            (
                // Largest 3-divisible size at the bound, encoded as unpadded
                // 4-char groups (holds for any MAX modulo 3).
                (MAX_ATTACHMENT_BYTES - MAX_ATTACHMENT_BYTES % 3) as u64,
                &"QUJD".repeat(MAX_ATTACHMENT_BYTES / 3),
            ),
        ] {
            let outcome = validate_attachment_payload(encoded, decoded);
            assert_eq!(
                outcome.is_ok(),
                decoded > 0,
                "decoded {decoded} via {encoded}"
            );
        }

        // A mismatch between the declared and the actual size is rejected.
        let error =
            validate_attachment_payload("Zml4dHVyZSBhdHRhY2htZW50IGJ5dGVz", 23).unwrap_err();
        assert_eq!(error.into_api().code, "INVALID_REQUEST");

        // An encoded length over the contract bound is rejected even before
        // the size arithmetic runs.
        let oversized = "A".repeat(MAX_ATTACHMENT_BASE64_CHARS + 4);
        let error = validate_attachment_payload(&oversized, 24).unwrap_err();
        assert_eq!(error.into_api().code, "INVALID_REQUEST");
    }

    /// Attachment send validation (contract revision 1.13,
    /// implementation-plan §4.12): canonical-base64 shape, declared-size
    /// equality, descriptor bounds, and the type/subtype content-type shape
    /// are all enforced deterministically before any row is written.
    #[test]
    fn attachment_send_payload_and_descriptors_are_validated_before_any_row() {
        let encoded = "Zml4dHVyZSBhdHRhY2htZW50IGJ5dGVz";
        assert!(validate_attachment_send_payload(encoded, 24).is_ok());

        // Non-canonical inputs: declared-size mismatch, whitespace, missing
        // padding (wrong length modulo), URL-safe alphabet, non-zero
        // leftover bits.
        assert!(validate_attachment_send_payload(encoded, 23).is_err());
        assert!(validate_attachment_send_payload("Zml4dHVyZSBhdHRhY2htZW50IGJ5dGVz ", 24).is_err());
        assert!(validate_attachment_send_payload("A", 1).is_err());
        assert!(validate_attachment_send_payload("YQ", 1).is_err());
        assert!(validate_attachment_send_payload("_w==", 1).is_err());
        assert!(validate_attachment_send_payload("YR==", 1).is_err());

        // Filename/contentType descriptor bounds.
        assert!(validate_attachment_descriptor(Some("report.pdf"), "filename").is_ok());
        assert!(validate_attachment_descriptor(None, "filename").is_ok());
        assert!(validate_attachment_descriptor(Some(""), "filename").is_err());
        assert!(validate_attachment_descriptor(Some("../etc/passwd"), "filename").is_err());
        assert!(validate_attachment_descriptor(Some("a\\b"), "filename").is_err());
        assert!(validate_attachment_descriptor(Some("a\nb"), "filename").is_err());
        assert!(validate_attachment_descriptor(Some(&"界".repeat(43)), "filename").is_err());

        assert!(validate_attachment_content_type(Some("image/png")).is_ok());
        assert!(validate_attachment_content_type(None).is_ok());
        assert!(validate_attachment_content_type(Some("image")).is_err());
        assert!(validate_attachment_content_type(Some("image/png;x=y")).is_err());
        assert!(validate_attachment_content_type(Some("imag e/png")).is_err());
    }

    /// The data URI is the only upstream-facing attachment form (§4.12): the
    /// validated base64 round-trips byte-exact, the filename is
    /// percent-encoded, and a missing contentType falls back to
    /// application/octet-stream.
    #[test]
    fn attachment_data_uri_round_trips_and_encodes_the_descriptor() {
        let payload = b"attachment bytes \xe2\x9c\x93";
        // Standard base64 of the payload, built locally to stay independent.
        let mut b64 = String::new();
        const ALPHABET: &[u8; 64] =
            b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        for chunk in payload.chunks(3) {
            let b = [
                chunk[0],
                *chunk.get(1).unwrap_or(&0),
                *chunk.get(2).unwrap_or(&0),
            ];
            b64.push(ALPHABET[(b[0] >> 2) as usize] as char);
            b64.push(ALPHABET[(((b[0] & 0x03) << 4) | (b[1] >> 4)) as usize] as char);
            if chunk.len() > 1 {
                b64.push(ALPHABET[(((b[1] & 0x0F) << 2) | (b[2] >> 6)) as usize] as char);
            }
            if chunk.len() > 2 {
                b64.push(ALPHABET[(b[2] & 0x3F) as usize] as char);
            }
        }
        let pad = (3 - payload.len() % 3) % 3;
        for _ in 0..pad {
            b64.push('=');
        }

        assert!(validate_attachment_send_payload(&b64, payload.len() as u64).is_ok());
        let uri =
            build_attachment_data_uri(&b64, Some("notes 下载.txt"), Some("text/plain")).unwrap();
        assert!(uri.starts_with("data:text/plain;filename=notes%20%E4%B8%8B%E8%BD%BD.txt;base64,"));
        let payload_part = uri.rsplit(";base64,").next().unwrap();
        let mut decoded = Vec::new();
        base64_decode_to_vec(payload_part, &mut decoded).unwrap();
        assert_eq!(decoded, payload);

        // Default content type when none given.
        let uri = build_attachment_data_uri(&b64, None, None).unwrap();
        assert!(uri.starts_with("data:application/octet-stream;base64,"));
    }

    /// prepare_send_attachment (§4.12): validation failures leave no pending
    /// row (the same clientRequestId stays fresh), the prepared upstream
    /// params carry the data URI under `attachments`, and an unknown
    /// clientRequestId replay returns the same existing row (idempotency).
    #[test]
    fn prepare_send_attachment_builds_data_uri_params_and_is_idempotent() {
        let (_temp, mut service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let _events = service
            .ingest_receive(
                NormalizedReceive {
                    timestamp: Some(10),
                    content_kind: "dataMessage",
                    direction: "incoming",
                    account_present: true,
                    account: Some("+15555550100".into()),
                    source: Some("+15555550101".into()),
                    peer_name: Some("Peer".into()),
                    group_id: None,
                    text: Some("hello".into()),
                    text_bytes: None,
                    text_truncated: false,
                    quote: None,
                    attachments: Vec::new(),
                    rich: None,
                    control: None,
                },
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap();
        let conversation_id = service
            .list_conversations(&account.id, 10, None)
            .unwrap()
            .items
            .remove(0)
            .id;

        let payload = b"attachment bytes";
        let b64 = "YXR0YWNobWVudCBieXRlcw=="; // base64 of "attachment bytes"
        assert_eq!(payload.len(), 16);

        // A rejected request (size mismatch) leaves no row behind: the same
        // clientRequestId is reusable afterwards.
        let error = service
            .prepare_send_attachment(
                &account.id,
                &AttachmentSendTarget::Conversation(&conversation_id),
                "req-attach-1",
                b64,
                15,
                Some("notes.txt"),
                Some("text/plain"),
                Some("see attachment"),
                None,
            )
            .unwrap_err();
        assert_eq!(error.into_api().code, "INVALID_REQUEST");
        assert!(
            service
                .store
                .message_by_client_request(&account.id, "req-attach-1")
                .unwrap()
                .is_none()
        );

        let prepared = service
            .prepare_send_attachment(
                &account.id,
                &AttachmentSendTarget::Conversation(&conversation_id),
                "req-attach-1",
                b64,
                16,
                Some("notes.txt"),
                Some("text/plain"),
                Some("see attachment"),
                None,
            )
            .unwrap();
        let PreparedSend::Dispatch {
            pending_id, params, ..
        } = &prepared
        else {
            panic!("expected a dispatch");
        };
        assert_eq!(params["message"], "see attachment");
        let attachments = params["attachments"].as_array().unwrap();
        assert_eq!(attachments.len(), 1);
        let uri = attachments[0].as_str().unwrap();
        assert_eq!(
            uri,
            "data:text/plain;filename=notes.txt;base64,YXR0YWNobWVudCBieXRlcw=="
        );
        // Addressing rides the shared upstream-target shape.
        assert!(params.get("recipient").is_some() || params.get("groupId").is_some());

        // The pending row exists once; replaying the same clientRequestId
        // returns the existing row instead of a second dispatch.
        let replay = service
            .prepare_send_attachment(
                &account.id,
                &AttachmentSendTarget::Conversation(&conversation_id),
                "req-attach-1",
                b64,
                16,
                Some("notes.txt"),
                Some("text/plain"),
                Some("see attachment"),
                None,
            )
            .unwrap();
        // The stored row carries the metadata-only descriptor so the desktop
        // renders the outgoing attachment from its first tick.
        let PreparedSend::Existing(row) = &replay else {
            panic!("expected the replayed existing row");
        };
        assert_eq!(row.attachments.len(), 1);
        assert_eq!(row.attachments[0].filename.as_deref(), Some("notes.txt"));
        assert_eq!(
            row.attachments[0].content_type.as_deref(),
            Some("text/plain")
        );
        assert_eq!(row.attachments[0].size, Some(16));
        assert!(!row.attachments[0].is_voice_note);
        assert_eq!(row.text.as_deref(), Some("see attachment"));
        let _ = pending_id;

        // The captioned attachment keeps a text ending — no noun (contract 1.22).
        let summary = service
            .store
            .conversation_summary(&account.id, &conversation_id)
            .unwrap()
            .unwrap();
        assert_eq!(summary.last_message_kind, None);

        // A caption-less attachment previews as the filename — never an
        // empty preview the conversation list renders as blank.
        let prepared = service
            .prepare_send_attachment(
                &account.id,
                &AttachmentSendTarget::Conversation(&conversation_id),
                "req-attach-3",
                b64,
                16,
                Some("report.pdf"),
                Some("application/pdf"),
                None,
                None,
            )
            .unwrap();
        assert!(matches!(prepared, PreparedSend::Dispatch { .. }));
        let summary = service
            .store
            .conversation_summary(&account.id, &conversation_id)
            .unwrap()
            .unwrap();
        assert_eq!(summary.last_message_preview.as_deref(), Some("report.pdf"));
        // Caption-less attachment ending: the generic file noun (contract 1.22).
        assert_eq!(summary.last_message_kind, Some("file"));

        // Unknown account and conversation answer their deterministic errors.
        let error = service
            .prepare_send_attachment(
                "no-such-account",
                &AttachmentSendTarget::Conversation(&conversation_id),
                "req-attach-2",
                b64,
                16,
                None,
                None,
                None,
                None,
            )
            .unwrap_err();
        assert_eq!(error.into_api().code, "ACCOUNT_NOT_FOUND");
    }

    /// groups.get (contract revision 1.9, implementation-plan §4.8): a pure
    /// cache projection. Synced groups answer with their stored title and
    /// memberCount; anything else — a contact peer key, an unsynced group, a
    /// missing account, a malformed key — answers its deterministic error
    /// without any upstream call.
    #[test]
    fn get_group_projects_the_cached_group_row() {
        let (_temp, service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        service
            .store_ref()
            .upsert_contact(
                &account.id,
                "group",
                "ZmFrZS1ncm91cC0x",
                "Fixture Group",
                Some("{\"memberCount\":2}"),
                1725300000,
            )
            .unwrap();
        service
            .store_ref()
            .upsert_contact(
                &account.id,
                "contact",
                "+15555550101",
                "Alice Contact",
                None,
                1725300000,
            )
            .unwrap();

        let group = service.get_group(&account.id, "ZmFrZS1ncm91cC0x").unwrap();
        assert_eq!(group.peer_key, "ZmFrZS1ncm91cC0x");
        assert_eq!(group.title, "Fixture Group");
        assert_eq!(group.member_count, Some(2));
        assert_eq!(group.synced_at, 1725300000);

        // A group row without a captured member list omits memberCount.
        service
            .store_ref()
            .upsert_contact(
                &account.id,
                "group",
                "Z3JvdXAtbm8tbWVtYmVycw",
                "No Members",
                None,
                7,
            )
            .unwrap();
        let bare = service
            .get_group(&account.id, "Z3JvdXAtbm8tbWVtYmVycw")
            .unwrap();
        assert_eq!(bare.title, "No Members");
        assert_eq!(bare.member_count, None);
        assert_eq!(bare.synced_at, 7);

        // A contact peer key is not a group row: deterministic GROUP_NOT_FOUND.
        let contact = service.get_group(&account.id, "+15555550101").unwrap_err();
        assert_eq!(contact.into_api().code, "GROUP_NOT_FOUND");

        // An unsynced group id is a GROUP_NOT_FOUND, never a listGroups call.
        let unsynced = service
            .get_group(&account.id, "bm90LWEtbWVtYmVy")
            .unwrap_err();
        assert_eq!(unsynced.into_api().code, "GROUP_NOT_FOUND");

        let absent_account = service
            .get_group("no-such-account", "ZmFrZS1ncm91cC0x")
            .unwrap_err();
        assert_eq!(absent_account.into_api().code, "ACCOUNT_NOT_FOUND");

        let malformed = service.get_group(&account.id, "").unwrap_err();
        assert_eq!(malformed.into_api().code, "INVALID_REQUEST");
    }

    /// contacts.setLocalAlias prepare (contract revision 1.10, §4.9): the
    /// upstream params are exactly `account` + a single-string `recipient` +
    /// `name`; the peer must already be known (cached contact row or direct
    /// conversation peer); a bogus peer, group key, or alias answers
    /// INVALID_REQUEST without any upstream call.
    #[test]
    fn prepare_set_local_alias_builds_upstream_params_and_resolves_the_peer() {
        let (_temp, service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        // Known via the direct conversation peer.
        let direct = service
            .store_ref()
            .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
            .unwrap();
        // Known via the cached contacts row.
        service
            .store_ref()
            .upsert_contact(&account.id, "contact", "+15555550102", "Bob", None, 9)
            .unwrap();

        let via_conversation = service
            .prepare_set_local_alias(&account.id, &direct.peer_key, "Alice K.")
            .unwrap();
        assert_eq!(
            via_conversation,
            json!({
                "account": "+15555550100",
                "recipient": "+15555550101",
                "name": "Alice K."
            })
        );

        let via_contact_row = service
            .prepare_set_local_alias(&account.id, "+15555550102", "Bob B.")
            .unwrap();
        assert_eq!(via_contact_row["recipient"], "+15555550102");
        assert_eq!(via_contact_row["name"], "Bob B.");

        // A group key is not a contact: rejected before any upstream call.
        let group_key = service
            .prepare_set_local_alias(&account.id, "ZmFrZS1ncm91cC0x", "G")
            .unwrap_err();
        assert_eq!(group_key.into_api().code, "INVALID_REQUEST");

        let unknown_peer = service
            .prepare_set_local_alias(&account.id, "+15555559999", "Nobody")
            .unwrap_err();
        assert_eq!(unknown_peer.into_api().code, "INVALID_REQUEST");

        let absent_account = service
            .prepare_set_local_alias("no-such-account", "+15555550101", "A")
            .unwrap_err();
        assert_eq!(absent_account.into_api().code, "ACCOUNT_NOT_FOUND");

        // Shape bounds: empty and oversized aliases.
        let empty = service
            .prepare_set_local_alias(&account.id, "+15555550101", "")
            .unwrap_err();
        assert_eq!(empty.into_api().code, "INVALID_REQUEST");
        let oversized = service
            .prepare_set_local_alias(&account.id, "+15555550101", &"x".repeat(129))
            .unwrap_err();
        assert_eq!(oversized.into_api().code, "INVALID_REQUEST");
    }

    /// presence.setTypingMessage prepare (contract revision 1.11, §4.10):
    /// the upstream params are exactly `account` + the explicit `stop`
    /// boolean + group/peer addressing; a missing conversation answers
    /// CONVERSATION_NOT_FOUND without any upstream call.
    #[test]
    fn prepare_set_typing_message_builds_upstream_params_from_the_conversation() {
        let (_temp, service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let direct = service
            .store_ref()
            .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
            .unwrap();
        let group = service
            .store_ref()
            .ensure_conversation(&account.id, "group", "ZmFrZS1ncm91cC0x", "G")
            .unwrap();

        let start = service
            .prepare_set_typing_message(&account.id, &direct.id, false)
            .unwrap();
        assert_eq!(
            start,
            json!({
                "account": "+15555550100",
                "stop": false,
                "recipient": ["+15555550101"]
            })
        );

        let stop = service
            .prepare_set_typing_message(&account.id, &direct.id, true)
            .unwrap();
        assert_eq!(
            stop,
            json!({
                "account": "+15555550100",
                "stop": true,
                "recipient": ["+15555550101"]
            })
        );

        let group_start = service
            .prepare_set_typing_message(&account.id, &group.id, false)
            .unwrap();
        assert_eq!(
            group_start,
            json!({
                "account": "+15555550100",
                "stop": false,
                "groupId": "ZmFrZS1ncm91cC0x"
            })
        );

        let missing = service
            .prepare_set_typing_message(&account.id, "no-such-conversation", false)
            .unwrap_err();
        assert_eq!(missing.into_api().code, "CONVERSATION_NOT_FOUND");

        let absent_account = service
            .prepare_set_typing_message("no-such-account", &direct.id, false)
            .unwrap_err();
        assert_eq!(absent_account.into_api().code, "ACCOUNT_NOT_FOUND");
    }

    /// Builds a control-carrying receive addressed to this account/peer.
    fn control_receive(account: &str, source: &str, control: ControlReceive) -> NormalizedReceive {
        NormalizedReceive {
            timestamp: Some(500),
            content_kind: "control",
            direction: "control",
            account_present: true,
            account: Some(account.into()),
            source: Some(source.into()),
            peer_name: None,
            group_id: None,
            text: None,
            text_bytes: None,
            text_truncated: false,
            quote: None,
            attachments: Vec::new(),
            rich: None,
            control: Some(control),
        }
    }

    /// Same as [`control_receive`] with the envelope display name a real
    /// signal-cli receive carries (contract 1.27 actor-name capture).
    fn control_receive_named(
        account: &str,
        source: &str,
        peer_name: &str,
        control: ControlReceive,
    ) -> NormalizedReceive {
        let mut receive = control_receive(account, source, control);
        receive.peer_name = Some(peer_name.into());
        receive
    }

    fn linked_account_and_conversation(
        service: &mut ConnectorService,
    ) -> (crate::store::AccountSummary, crate::store::ConversationRow) {
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let conversation = service
            .store_ref()
            .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
            .unwrap();
        (account, conversation)
    }

    fn seed_outgoing_sent(
        service: &mut ConnectorService,
        account_id: &str,
        conversation_id: &str,
        sent_at: u64,
    ) -> String {
        let prepared = match service
            .prepare_send_text(
                account_id,
                conversation_id,
                "original",
                "req-ctl",
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch { pending_id, .. } => pending_id,
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        };
        service
            .complete_send_success(&prepared, account_id, conversation_id, sent_at)
            .unwrap();
        prepared
    }

    /// Contract 1.34: `prepare_receipt` (markRead/markViewed shared body) —
    /// a direct conversation fans one group out to its peer with ascending
    /// deduplicated timestamps, explicit ids are filtered to incoming rows in
    /// receipt order, group conversations answer zero groups (local sender
    /// hashes cannot be resolved to an author), a 600-id list is truncated to
    /// the 512 cap instead of rejected, and an empty conversation is the
    /// trivial no-op.
    #[test]
    fn prepare_receipt_groups_direct_timestamps_and_skips_groups() {
        let (_temp, mut service) = service();
        let (account, conversation) = linked_account_and_conversation(&mut service);
        let seed = |id: &str, sent_at: u64, direction: &'static str, conv: &str| {
            let record = MessageRecord {
                id: id.into(),
                account_id: account.id.clone(),
                conversation_id: conv.to_string(),
                direction,
                sender_id: if direction == "incoming" {
                    "peer-hash".into()
                } else {
                    account.id.clone()
                },
                sender_name: None,
                mentions_self: false,
                sent_at,
                received_at: None,
                text: Some("body".into()),
                text_bytes: Some(4),
                text_truncated: false,
                text_retrievable: true,
                status: if direction == "incoming" {
                    "delivered"
                } else {
                    "sent"
                },
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
                admin_deleted: false,
            };
            assert!(
                service
                    .store_ref()
                    .insert_message(&record, None, Some("body"), true)
                    .unwrap()
            );
        };
        seed("m3", 30, "incoming", &conversation.id);
        seed("out-1", 40, "outgoing", &conversation.id);
        seed("m1", 10, "incoming", &conversation.id);
        seed("m2", 20, "incoming", &conversation.id);

        let absent = service
            .prepare_mark_read(&account.id, &conversation.id, None)
            .unwrap();
        assert_eq!(absent.account_id, account.id);
        assert_eq!(absent.conversation_id, conversation.id);
        assert_eq!(absent.account_signal, "+15555550100");
        assert_eq!(absent.groups.len(), 1);
        assert_eq!(absent.groups[0].recipient, "+15555550101");
        assert_eq!(absent.groups[0].timestamps, [10, 20, 30]);

        let explicit = service
            .prepare_mark_viewed(
                &account.id,
                &conversation.id,
                Some(vec![
                    "m3".into(),
                    "out-1".into(),
                    "m3".into(),
                    "missing".into(),
                ]),
            )
            .unwrap();
        assert_eq!(explicit.groups.len(), 1);
        assert_eq!(explicit.groups[0].timestamps, [30]);

        let group = service
            .store_ref()
            .ensure_conversation(&account.id, "group", "ZmFrZS1ncm91cC0x", "Group")
            .unwrap();
        seed("g1", 50, "incoming", &group.id);
        let group_receipts = service
            .prepare_mark_read(&account.id, &group.id, None)
            .unwrap();
        assert!(
            group_receipts.groups.is_empty(),
            "group rows carry no resolvable author"
        );

        let truncated = service
            .prepare_mark_read(
                &account.id,
                &conversation.id,
                Some((0..600).map(|index| format!("junk-{index}")).collect()),
            )
            .unwrap();
        assert!(truncated.groups.is_empty(), "truncation drops unknown ids");

        let empty_conversation = service
            .store_ref()
            .ensure_conversation(&account.id, "direct", "+15555550102", "Quiet")
            .unwrap();
        let empty = service
            .prepare_mark_viewed(&account.id, &empty_conversation.id, None)
            .unwrap();
        assert!(empty.groups.is_empty());
    }

    /// Contract 1.34 outbound mentions: more than 64 entries are capped, an
    /// empty number fails the whole request INVALID_REQUEST, a UUID-shaped
    /// number passes through without a lookup, the linked account's own
    /// number and digit-suffix contact matches survive, an unresolvable
    /// number drops its entry, and an all-dropped list leaves the upstream
    /// params without a `mentions` key at all.
    #[test]
    fn send_text_mentions_are_capped_resolved_and_dropped_locally() {
        let (_temp, mut service) = service();
        let (account, conversation) = linked_account_and_conversation(&mut service);
        service
            .store_ref()
            .upsert_synced_contacts(
                &account.id,
                &[SyncedContact {
                    kind: "contact",
                    peer_key: "+15555550101",
                    title: "Peer",
                    extra: None,
                }],
                1,
            )
            .unwrap();
        let uuid = |index: usize| format!("00000000-0000-4000-8000-{index:012x}");
        let mention = |number: &str, start: u32, length: u32| SendTextMentionParams {
            number: number.to_string(),
            start,
            length,
        };
        let dispatch = |prepared: PreparedSend| match prepared {
            PreparedSend::Dispatch { params, .. } => params,
            PreparedSend::Existing(_) => panic!("a fresh client request must dispatch"),
        };
        let send = |service: &mut ConnectorService,
                    client_request_id: &str,
                    mentions: Vec<SendTextMentionParams>| {
            service.prepare_send_text(
                &account.id,
                &conversation.id,
                "hi @all",
                client_request_id,
                None,
                None,
                Some(mentions),
            )
        };

        let capped = dispatch(
            send(
                &mut service,
                "req-cap",
                (0..70).map(|index| mention(&uuid(index), 0, 2)).collect(),
            )
            .unwrap(),
        );
        assert_eq!(capped["mentions"].as_array().unwrap().len(), 64);

        let error = match send(&mut service, "req-empty", vec![mention("  ", 0, 2)]).unwrap_err() {
            ServiceError::Api(api) => api,
            other => panic!("an empty mention number must fail closed, got {other:?}"),
        };
        assert_eq!(error.code, "INVALID_REQUEST");

        let resolved = dispatch(
            send(
                &mut service,
                "req-mixed",
                vec![
                    mention("+19990000001", 0, 2),
                    mention(&uuid(1), 3, 4),
                    mention("+15555550101", 7, 8),
                    mention("+15555550100", 9, 10),
                ],
            )
            .unwrap(),
        );
        let entries = resolved["mentions"].as_array().unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0]["number"], uuid(1));
        assert_eq!(entries[0]["start"], 3);
        assert_eq!(entries[0]["length"], 4);
        assert_eq!(entries[1]["number"], "+15555550101");
        assert_eq!(entries[2]["number"], "+15555550100");

        let all_dropped = dispatch(
            send(
                &mut service,
                "req-dropped",
                vec![mention("+19990000001", 0, 2), mention("+19990000002", 3, 4)],
            )
            .unwrap(),
        );
        assert!(all_dropped.get("mentions").is_none());
    }

    /// A peer reaction upserts a message_events row keyed on the reacting
    /// actor, answers conversation.changed (never message.upserted), and a
    /// repeat of the same reaction stays idempotent. The envelope display
    /// name rides along (contract 1.27) and surfaces on the pill's per-actor
    /// detail.
    #[test]
    fn inbound_reaction_persists_an_actor_keyed_event_and_notifies_once() {
        let (_temp, mut service) = service();
        let (account, conversation) = linked_account_and_conversation(&mut service);
        let message_id = seed_outgoing_sent(&mut service, &account.id, &conversation.id, 400);

        let receive = control_receive_named(
            "+15555550100",
            "+15555550101",
            "林菲菲",
            ControlReceive::Reaction {
                emoji: "👍".into(),
                target_author: "+15555550100".into(),
                target_timestamp: 400,
                remove: false,
            },
        );
        let events = service
            .ingest_receive(receive, crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        assert!(
            events
                .iter()
                .all(|event| matches!(event, HostSideEvent::ConversationChanged(_))),
            "reaction answers conversation.changed only: {events:?}"
        );

        let reactions = service
            .store_ref()
            .list_reaction_events(&account.id, &conversation.id, 10)
            .unwrap();
        assert_eq!(reactions.len(), 1);
        assert_eq!(reactions[0].emoji, "👍");
        assert_eq!(reactions[0].target_timestamp, 400);
        assert!(!reactions[0].removed);
        assert_ne!(reactions[0].actor_id, account.id, "actor is the peer");

        // The pill projection carries the actor with the captured name.
        let record = service
            .store_ref()
            .message_by_id(&account.id, &conversation.id, &message_id)
            .unwrap()
            .unwrap();
        assert_eq!(record.reactions.len(), 1);
        assert_eq!(record.reactions[0].actors.len(), 1);
        assert!(!record.reactions[0].actors[0].is_self);
        assert_eq!(
            record.reactions[0].actors[0].name.as_deref(),
            Some("林菲菲")
        );

        let repeat = service
            .ingest_receive(
                control_receive(
                    "+15555550100",
                    "+15555550101",
                    ControlReceive::Reaction {
                        emoji: "👍".into(),
                        target_author: "+15555550100".into(),
                        target_timestamp: 400,
                        remove: false,
                    },
                ),
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap();
        assert!(!repeat.is_empty(), "the summary still changes for the host");
        assert_eq!(
            service
                .store_ref()
                .list_reaction_events(&account.id, &conversation.id, 10)
                .unwrap()
                .len(),
            1,
            "a repeated reaction upserts in place"
        );
    }

    /// A reaction removal flips the stored row in place instead of adding a
    /// second event.
    #[test]
    fn inbound_reaction_removal_flips_the_existing_row() {
        let (_temp, mut service) = service();
        let (account, conversation) = linked_account_and_conversation(&mut service);
        seed_outgoing_sent(&mut service, &account.id, &conversation.id, 400);
        let reaction = |remove: bool| {
            control_receive(
                "+15555550100",
                "+15555550101",
                ControlReceive::Reaction {
                    emoji: "🎉".into(),
                    target_author: "+15555550100".into(),
                    target_timestamp: 400,
                    remove,
                },
            )
        };
        service
            .ingest_receive(reaction(false), crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        service
            .ingest_receive(reaction(true), crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap();

        let reactions = service
            .store_ref()
            .list_reaction_events(&account.id, &conversation.id, 10)
            .unwrap();
        assert_eq!(reactions.len(), 1);
        assert!(reactions[0].removed);
    }

    /// A reaction addressed to a conversation that does not exist locally is
    /// dropped without error and without a message_events row.
    #[test]
    fn inbound_reaction_without_a_local_conversation_is_dropped() {
        let (_temp, mut service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let events = service
            .ingest_receive(
                control_receive(
                    "+15555550100",
                    "+15555550999",
                    ControlReceive::Reaction {
                        emoji: "👍".into(),
                        target_author: "+15555550100".into(),
                        target_timestamp: 400,
                        remove: false,
                    },
                ),
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap();
        assert!(events.is_empty());
        assert_eq!(
            service
                .store_ref()
                .list_reaction_events(&account.id, "no-such-conversation", 10)
                .unwrap()
                .len(),
            0
        );
    }

    /// A peer remote delete marks the original incoming row
    /// `remote-deleted` and answers message.statusChanged — never a row
    /// insert.
    #[test]
    fn inbound_remote_delete_marks_the_row_and_answers_status_changed() {
        let (_temp, mut service) = service();
        let (account, conversation) = linked_account_and_conversation(&mut service);
        let incoming = NormalizedReceive {
            timestamp: Some(300),
            content_kind: "dataMessage",
            direction: "incoming",
            account_present: true,
            account: Some("+15555550100".into()),
            source: Some("+15555550101".into()),
            peer_name: None,
            group_id: None,
            text: Some("to be deleted".into()),
            text_bytes: Some(13),
            text_truncated: false,
            quote: None,
            attachments: Vec::new(),
            rich: None,
            control: None,
        };
        service
            .ingest_receive(incoming, crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap();

        let events = service
            .ingest_receive(
                control_receive(
                    "+15555550100",
                    "+15555550101",
                    ControlReceive::RemoteDelete {
                        target_timestamp: 300,
                    },
                ),
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap();
        assert_eq!(events.len(), 1);
        assert!(
            matches!(&events[0], HostSideEvent::MessageStatusChanged { status, .. } if *status == "remote-deleted")
        );

        let messages = service
            .list_messages(&account.id, &conversation.id, 10, None)
            .unwrap()
            .items;
        assert_eq!(messages.len(), 1, "no duplicate row is created");
        assert_eq!(messages[0].status, "remote-deleted");
        assert_eq!(messages[0].sent_at, 300);
    }

    /// A peer's envelope-level edit (direction "incoming" + control Edit)
    /// updates the original row in place: same id, new body, editedAt set —
    /// and never inserts a second message row.
    #[test]
    fn inbound_peer_edit_updates_the_row_in_place() {
        let (_temp, mut service) = service();
        let (account, conversation) = linked_account_and_conversation(&mut service);
        let original = NormalizedReceive {
            timestamp: Some(310),
            content_kind: "dataMessage",
            direction: "incoming",
            account_present: true,
            account: Some("+15555550100".into()),
            source: Some("+15555550101".into()),
            peer_name: None,
            group_id: None,
            text: Some("before edit".into()),
            text_bytes: Some(11),
            text_truncated: false,
            quote: None,
            attachments: Vec::new(),
            rich: None,
            control: None,
        };
        service
            .ingest_receive(original, crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap();

        let mut edit = control_receive(
            "+15555550100",
            "+15555550101",
            ControlReceive::Edit {
                target_timestamp: 310,
            },
        );
        edit.direction = "incoming";
        edit.content_kind = "editMessage";
        edit.text = Some("after edit".into());
        edit.text_bytes = Some(10);
        let events = service
            .ingest_receive(edit, crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        assert_eq!(events.len(), 1);
        let HostSideEvent::MessageUpserted(record) = &events[0] else {
            panic!("expected message.upserted, got {:?}", events[0]);
        };
        assert_eq!(record.text.as_deref(), Some("after edit"));
        assert!(record.edited_at.is_some());

        let messages = service
            .list_messages(&account.id, &conversation.id, 10, None)
            .unwrap()
            .items;
        assert_eq!(messages.len(), 1, "edit must not insert a second row");
        assert_eq!(messages[0].id, record.id);
        assert_eq!(messages[0].text.as_deref(), Some("after edit"));
        assert!(messages[0].edited_at.is_some());
    }

    /// Our own multi-device edit echo (direction "outgoing" + control Edit)
    /// retargets the phone-sent row through its upstream timestamp.
    #[test]
    fn sync_outgoing_edit_retargets_the_sent_row() {
        let (_temp, mut service) = service();
        let (account, conversation) = linked_account_and_conversation(&mut service);
        seed_outgoing_sent(&mut service, &account.id, &conversation.id, 420);

        let mut edit = control_receive(
            "+15555550100",
            "+15555550101",
            ControlReceive::Edit {
                target_timestamp: 420,
            },
        );
        edit.direction = "outgoing";
        edit.text = Some("edited on phone".into());
        edit.text_bytes = Some(15);
        let events = service
            .ingest_receive(edit, crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        assert_eq!(events.len(), 1);
        let HostSideEvent::MessageUpserted(record) = &events[0] else {
            panic!("expected message.upserted, got {:?}", events[0]);
        };
        assert_eq!(record.text.as_deref(), Some("edited on phone"));

        let row = service
            .store_ref()
            .message_by_id(&account.id, &conversation.id, &record.id)
            .unwrap()
            .unwrap();
        assert_eq!(row.sent_at, 420, "protocol identity is untouched");
        assert!(row.edited_at.is_some());
    }

    /// Typing START passes through as conversation.typing; an immediate
    /// second indicator for the same conversation is rate-limited away.
    #[test]
    fn typing_start_emits_once_and_an_immediate_repeat_is_rate_limited() {
        let (_temp, mut service) = service();
        let (account, conversation) = linked_account_and_conversation(&mut service);
        let start = || {
            control_receive(
                "+15555550100",
                "+15555550101",
                ControlReceive::Typing {
                    action: "START".into(),
                },
            )
        };
        let first = service
            .ingest_receive(start(), crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        assert_eq!(first.len(), 1);
        assert!(matches!(
            &first[0],
            HostSideEvent::ConversationTyping { action, .. } if action == "START"
        ));
        assert!(
            service
                .ingest_receive(start(), crate::DEFAULT_PROXY_GROUP_ID)
                .unwrap()
                .is_empty(),
            "a second indicator inside the window must be swallowed"
        );
        // STOP is control-plane state too, but the rate limiter is
        // conversation-scoped, not action-scoped — it is swallowed as well.
        assert!(
            service
                .ingest_receive(
                    control_receive(
                        "+15555550100",
                        "+15555550101",
                        ControlReceive::Typing {
                            action: "STOP".into()
                        },
                    ),
                    crate::DEFAULT_PROXY_GROUP_ID,
                )
                .unwrap()
                .is_empty()
        );
        let _ = (&account.id, &conversation.id);
    }

    /// Peer receipts upgrade earlier outgoing rows monotonically
    /// (sent → delivered → read): a lower tier never downgrades a higher
    /// one, incoming rows stay put, and only real transitions emit
    /// message.statusChanged.
    #[test]
    fn inbound_receipts_upgrade_outgoing_rows_monotonically() {
        let (_temp, mut service) = service();
        let (account, conversation) = linked_account_and_conversation(&mut service);
        seed_outgoing_sent(&mut service, &account.id, &conversation.id, 400);
        // An incoming row the receipts must never touch.
        service
            .ingest_receive(
                NormalizedReceive {
                    timestamp: Some(401),
                    content_kind: "dataMessage",
                    direction: "incoming",
                    account_present: true,
                    account: Some("+15555550100".into()),
                    source: Some("+15555550101".into()),
                    peer_name: None,
                    group_id: None,
                    text: Some("incoming stays".into()),
                    text_bytes: Some(14),
                    text_truncated: false,
                    quote: None,
                    attachments: Vec::new(),
                    rich: None,
                    control: None,
                },
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap();
        let messages_before = service
            .list_messages(&account.id, &conversation.id, 10, None)
            .unwrap()
            .items;
        let incoming_before = messages_before
            .iter()
            .find(|row| row.sent_at == 401)
            .map(|row| row.status)
            .unwrap();

        let receipt = |kind| {
            control_receive(
                "+15555550100",
                "+15555550101",
                ControlReceive::Receipt {
                    kind,
                    timestamps: vec![400],
                    when: Some(1234),
                },
            )
        };
        let delivered = service
            .ingest_receive(
                receipt(ReceiptKind::Delivered),
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap();
        assert_eq!(delivered.len(), 1);
        assert!(matches!(
            &delivered[0],
            HostSideEvent::MessageStatusChanged { status, .. } if *status == "delivered"
        ));

        let read = service
            .ingest_receive(receipt(ReceiptKind::Read), crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        assert_eq!(read.len(), 1);
        assert!(matches!(
            &read[0],
            HostSideEvent::MessageStatusChanged { status, .. } if *status == "read"
        ));

        // A late delivery receipt must not downgrade nor re-emit.
        assert!(
            service
                .ingest_receive(
                    receipt(ReceiptKind::Delivered),
                    crate::DEFAULT_PROXY_GROUP_ID,
                )
                .unwrap()
                .is_empty()
        );

        let messages = service
            .list_messages(&account.id, &conversation.id, 10, None)
            .unwrap()
            .items;
        let outgoing = messages.iter().find(|row| row.sent_at == 400).unwrap();
        assert_eq!(outgoing.status, "read");
        let incoming = messages.iter().find(|row| row.sent_at == 401).unwrap();
        assert_eq!(incoming.status, incoming_before, "incoming rows stay put");
    }

    /// Receipt envelopes carrying a `when` stamp the delivered/read timeline
    /// (contract 1.32): the first stamp wins, a replayed receipt neither
    /// re-stamps nor re-emits, and reloaded rows keep both stamps for the
    /// host projection.
    #[test]
    fn receipts_stamp_the_delivered_read_timeline_once() {
        let (_temp, mut service) = service();
        let (account, conversation) = linked_account_and_conversation(&mut service);
        seed_outgoing_sent(&mut service, &account.id, &conversation.id, 400);

        let receipt = |kind, when: u64| {
            control_receive(
                "+15555550100",
                "+15555550101",
                ControlReceive::Receipt {
                    kind,
                    timestamps: vec![400],
                    when: Some(when),
                },
            )
        };

        let delivered = service
            .ingest_receive(
                receipt(ReceiptKind::Delivered, 1234),
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap();
        assert_eq!(delivered.len(), 1);
        assert!(matches!(
            &delivered[0],
            HostSideEvent::MessageStatusChanged { status, delivered_at, read_at, .. }
                if *status == "delivered"
                    && *delivered_at == Some(1234)
                    && read_at.is_none()
        ));

        let read = service
            .ingest_receive(
                receipt(ReceiptKind::Read, 5678),
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap();
        assert_eq!(read.len(), 1);
        assert!(matches!(
            &read[0],
            HostSideEvent::MessageStatusChanged { status, delivered_at, read_at, .. }
                if *status == "read"
                    && *delivered_at == Some(1234)
                    && *read_at == Some(5678)
        ));

        // A replayed read neither re-stamps nor re-emits.
        assert!(
            service
                .ingest_receive(
                    receipt(ReceiptKind::Read, 9999),
                    crate::DEFAULT_PROXY_GROUP_ID,
                )
                .unwrap()
                .is_empty()
        );

        let outgoing = service
            .list_messages(&account.id, &conversation.id, 10, None)
            .unwrap()
            .items
            .into_iter()
            .find(|row| row.sent_at == 400)
            .unwrap();
        assert_eq!(outgoing.status, "read");
        assert_eq!(outgoing.delivered_at, Some(1234));
        assert_eq!(outgoing.read_at, Some(5678));
    }

    /// A receipt envelope without its own timestamp (contract 1.32) falls
    /// back to the connector clock: the row is stamped with a positive now
    /// and the event carries exactly the stamp it just wrote.
    #[test]
    fn receipt_without_envelope_timestamp_falls_back_to_connector_clock() {
        let (_temp, mut service) = service();
        let (account, conversation) = linked_account_and_conversation(&mut service);
        seed_outgoing_sent(&mut service, &account.id, &conversation.id, 400);

        let before = crate::link::now_ms();
        let delivered = service
            .ingest_receive(
                control_receive(
                    "+15555550100",
                    "+15555550101",
                    ControlReceive::Receipt {
                        kind: ReceiptKind::Delivered,
                        timestamps: vec![400],
                        when: None,
                    },
                ),
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap();
        let after = crate::link::now_ms();

        assert_eq!(delivered.len(), 1);
        let fallback = match &delivered[0] {
            HostSideEvent::MessageStatusChanged {
                status,
                delivered_at,
                read_at,
                ..
            } if *status == "delivered" && read_at.is_none() => {
                delivered_at.expect("fallback stamp must be present")
            }
            other => panic!("unexpected event: {other:?}"),
        };
        assert!(
            fallback >= before && fallback <= after,
            "fallback stamp {fallback} outside [{before}, {after}]"
        );

        let outgoing = service
            .list_messages(&account.id, &conversation.id, 10, None)
            .unwrap()
            .items
            .into_iter()
            .find(|row| row.sent_at == 400)
            .unwrap();
        assert_eq!(outgoing.status, "delivered");
        assert_eq!(outgoing.delivered_at, Some(fallback));
        assert_eq!(outgoing.read_at, None);
    }

    /// Peer edits snapshot the body they replace (contract 1.32): each
    /// upserted record rides the ascending prior-body history and reloaded
    /// rows keep it (the desktop renders newest-first on its own).
    #[test]
    fn edits_snapshot_the_replaced_body_into_the_row_projection() {
        let (_temp, mut service) = service();
        let (account, conversation) = linked_account_and_conversation(&mut service);
        let original = NormalizedReceive {
            timestamp: Some(310),
            content_kind: "dataMessage",
            direction: "incoming",
            account_present: true,
            account: Some("+15555550100".into()),
            source: Some("+15555550101".into()),
            peer_name: None,
            group_id: None,
            text: Some("v0".into()),
            text_bytes: Some(2),
            text_truncated: false,
            quote: None,
            attachments: Vec::new(),
            rich: None,
            control: None,
        };
        service
            .ingest_receive(original, crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap();

        let edit = |body: &str| {
            let mut receive = control_receive(
                "+15555550100",
                "+15555550101",
                ControlReceive::Edit {
                    target_timestamp: 310,
                },
            );
            receive.direction = "incoming";
            receive.content_kind = "editMessage";
            receive.text = Some(body.into());
            receive.text_bytes = Some(body.len() as u32);
            receive
        };

        let first = service
            .ingest_receive(edit("v1"), crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        assert_eq!(first.len(), 1);
        let HostSideEvent::MessageUpserted(record) = &first[0] else {
            panic!("expected message.upserted, got {:?}", first[0]);
        };
        assert_eq!(record.text.as_deref(), Some("v1"));
        let bodies: Vec<&str> = record
            .edits
            .iter()
            .map(|entry| entry.body.as_str())
            .collect();
        assert_eq!(bodies, ["v0"], "the replaced body rides the upserted row");

        let second = service
            .ingest_receive(edit("v2"), crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        assert_eq!(second.len(), 1);
        let HostSideEvent::MessageUpserted(record) = &second[0] else {
            panic!("expected message.upserted, got {:?}", second[0]);
        };
        let bodies: Vec<&str> = record
            .edits
            .iter()
            .map(|entry| entry.body.as_str())
            .collect();
        assert_eq!(
            bodies,
            ["v0", "v1"],
            "history grows ascending, oldest first"
        );

        let reloaded = service
            .list_messages(&account.id, &conversation.id, 10, None)
            .unwrap()
            .items
            .into_iter()
            .find(|row| row.sent_at == 310)
            .unwrap();
        assert_eq!(reloaded.text.as_deref(), Some("v2"));
        let bodies: Vec<&str> = reloaded
            .edits
            .iter()
            .map(|entry| entry.body.as_str())
            .collect();
        assert_eq!(bodies, ["v0", "v1"]);
    }

    /// An edit whose receive carries no new body is dropped silently.
    #[test]
    fn inbound_edit_without_a_body_is_dropped() {
        let (_temp, mut service) = service();
        let (account, conversation) = linked_account_and_conversation(&mut service);
        seed_outgoing_sent(&mut service, &account.id, &conversation.id, 420);
        let events = service
            .ingest_receive(
                control_receive(
                    "+15555550100",
                    "+15555550101",
                    ControlReceive::Edit {
                        target_timestamp: 420,
                    },
                ),
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap();
        assert!(events.is_empty());
        let row = service
            .store_ref()
            .conversation_summary(&account.id, &conversation.id)
            .unwrap()
            .unwrap();
        assert_eq!(row.id, conversation.id);
    }

    /// `messages.edit` preparation: an outgoing sent row resolves to
    /// upstream editTimestamp params; settlement rewrites the body and
    /// stamps editedAt.
    #[test]
    fn prepare_edit_targets_sent_row_and_settlement_rewrites_it() {
        let (_temp, mut service) = service();
        let (account, conversation) = linked_account_and_conversation(&mut service);
        let pending = seed_outgoing_sent(&mut service, &account.id, &conversation.id, 430);

        let prepared = service
            .prepare_edit(&account.id, &conversation.id, &pending, "edited body")
            .unwrap();
        assert_eq!(
            prepared.params["editTimestamp"], 430,
            "upstream edit targets the sent row's protocol timestamp"
        );
        assert_eq!(prepared.params["message"], "edited body");

        let record = service
            .complete_edit_success(&account.id, &conversation.id, &pending, "edited body")
            .unwrap()
            .expect("settlement of a sent row returns the updated record");
        assert_eq!(record.text.as_deref(), Some("edited body"));
        assert!(record.edited_at.is_some());
        assert_eq!(record.status, "sent");

        // An incoming row is not editable by us.
        let incoming = NormalizedReceive {
            timestamp: Some(440),
            content_kind: "dataMessage",
            direction: "incoming",
            account_present: true,
            account: Some("+15555550100".into()),
            source: Some("+15555550101".into()),
            peer_name: None,
            group_id: None,
            text: Some("peer says".into()),
            text_bytes: Some(9),
            text_truncated: false,
            quote: None,
            attachments: Vec::new(),
            rich: None,
            control: None,
        };
        service
            .ingest_receive(incoming, crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let incoming_id = service
            .list_messages(&account.id, &conversation.id, 10, None)
            .unwrap()
            .items
            .into_iter()
            .find(|row| row.direction == "incoming")
            .unwrap()
            .id;
        let error = service
            .prepare_edit(&account.id, &conversation.id, &incoming_id, "nope")
            .unwrap_err();
        assert_eq!(error.into_api().code, "MESSAGE_NOT_FOUND");
    }

    /// Inbound quote + attachment metadata ride the message row end to end:
    /// ingest persists them, list_messages returns them verbatim.
    #[test]
    fn inbound_quote_and_attachment_metadata_roundtrip_through_the_store() {
        let (_temp, mut service) = service();
        let (account, conversation) = linked_account_and_conversation(&mut service);
        let receive = NormalizedReceive {
            timestamp: Some(360),
            content_kind: "dataMessage",
            direction: "incoming",
            account_present: true,
            account: Some("+15555550100".into()),
            source: Some("+15555550101".into()),
            peer_name: None,
            group_id: None,
            text: Some("replying with a file".into()),
            text_bytes: Some(20),
            text_truncated: false,
            quote: Some(NormalizedQuote {
                id: 350,
                author: "+15555550100".into(),
                text: "the original".into(),
            }),
            attachments: vec![NormalizedAttachment {
                id: "att-1".into(),
                content_type: Some("image/png".into()),
                filename: Some("shot.png".into()),
                size: Some(2048),
                width: Some(64),
                height: Some(32),
                is_voice_note: false,
            }],
            rich: None,
            control: None,
        };
        service
            .ingest_receive(receive, crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap();

        let row = service
            .list_messages(&account.id, &conversation.id, 10, None)
            .unwrap()
            .items
            .into_iter()
            .next()
            .unwrap();
        let quote = row
            .quote_snapshot
            .expect("quote snapshot survives the roundtrip");
        assert_eq!(quote.id, 350);
        assert_eq!(quote.author, "+15555550100");
        assert_eq!(quote.text, "the original");
        assert_eq!(row.attachments.len(), 1);
        assert_eq!(row.attachments[0].id, "att-1");
        assert_eq!(row.attachments[0].filename.as_deref(), Some("shot.png"));
        assert_eq!(row.attachments[0].size, Some(2048));
    }

    // ---- Media ingest (ADR 0002) -------------------------------------

    /// A service with the media backing armed over an engine-shaped data
    /// directory (`<data>/attachments/`, the single-account probe layout).
    fn media_service() -> (TempDir, ConnectorService) {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(StoreKey::from_bytes([0x5A; 32]))).unwrap();
        let data_dir = temp.path().join("signal-data");
        std::fs::create_dir_all(data_dir.join("attachments")).unwrap();
        let handles = Arc::new(MediaHandleTable::new());
        (
            temp,
            ConnectorService::with_media(
                Arc::new(store),
                Some(MediaIngest::new(data_dir, handles)),
            ),
        )
    }

    /// Addressing rows for one account, mirroring the media-open test setup:
    /// account → direct conversation → one completed outgoing message.
    fn addressed_message(
        service: &ConnectorService,
        number: &str,
        peer: &str,
        request_id: &str,
    ) -> (String, String, String) {
        let account = service
            .store_ref()
            .upsert_account_from_signal(number, None, crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let direct = service
            .store_ref()
            .ensure_conversation(&account.id, "direct", peer, "Peer")
            .unwrap();
        let message_id = match service
            .prepare_send_text(
                &account.id,
                &direct.id,
                "has attachment",
                request_id,
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch { pending_id, .. } => pending_id,
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        };
        service
            .complete_send_success(&message_id, &account.id, &direct.id, 800)
            .unwrap();
        (account.id, direct.id, message_id)
    }

    /// Without the launcher opt-in (`ConnectorService::new`), all three
    /// media methods answer the shared capability gate before any other
    /// check — the pre-PoC contract (ADR 0002 acceptance gate 5).
    #[test]
    fn media_methods_are_capability_gated_without_the_opt_in() {
        let (_temp, service) = service();
        for error in [
            service
                .open_media_handle("a", "c", "m", "att-1")
                .unwrap_err(),
            service.read_media_chunk("handle", 0).unwrap_err(),
            service.close_media_handle("handle").unwrap_err(),
        ] {
            let api = error.into_api();
            assert_eq!(api.code, "CAPABILITY_UNAVAILABLE");
            assert!(!api.retryable);
        }
    }

    /// Security gate (ADR 0002 acceptance gate 3): traversal-shaped ids —
    /// `../x`, absolute paths, backslashes, and the sanitize survivors
    /// `.` / `..` — answer INVALID_REQUEST before the filesystem is touched.
    /// The decoy file makes the check real: a naive path join would resolve
    /// it (and answer a handle or UPSTREAM_ERROR), never INVALID_REQUEST.
    #[test]
    fn media_open_rejects_traversal_ids_before_touching_the_filesystem() {
        let (_temp, service) = media_service();
        let (account_id, conversation_id, message_id) =
            addressed_message(&service, "+15555550100", "+15555550101", "req-media-trav");
        // Exactly where a naive `attachments/` join of "../escape.dat" and
        // "../../escape.dat" would land.
        std::fs::write(
            _temp.path().join("signal-data").join("escape.dat"),
            b"escaped",
        )
        .unwrap();

        for attachment_id in [
            "../escape.dat",
            "..",
            ".",
            "/etc/passwd",
            "a\\b.dat",
            "C:\\temp\\att.dat",
            "sub/../../escape.dat",
        ] {
            let error = service
                .open_media_handle(&account_id, &conversation_id, &message_id, attachment_id)
                .unwrap_err()
                .into_api();
            assert_eq!(error.code, "INVALID_REQUEST", "{attachment_id}");
            assert!(!error.retryable, "{attachment_id}");
        }
    }

    /// The happy path end to end: a downloaded file resolves, `open` reports
    /// the local size and the chunk bound, the read loop delivers the exact
    /// bytes through sequential offsets, and `closeHandle` is idempotent.
    #[test]
    fn media_open_streams_a_downloaded_file_through_sequential_chunks() {
        let (_temp, service) = media_service();
        let (account_id, conversation_id, message_id) =
            addressed_message(&service, "+15555550100", "+15555550101", "req-media-open");
        let body: Vec<u8> = (0..8192_u32).map(|i| (i % 251) as u8).collect();
        let attachment_dir = _temp.path().join("signal-data").join("attachments");
        std::fs::write(attachment_dir.join("a1b2c3.dat"), &body).unwrap();

        let view = service
            .open_media_handle(&account_id, &conversation_id, &message_id, "a1b2c3.dat")
            .unwrap();
        assert_eq!(view.size_bytes, body.len() as u64);
        assert_eq!(view.chunk_bytes, 262_144);

        let mut assembled: Vec<u8> = Vec::with_capacity(body.len());
        let mut offset = 0_u64;
        let mut chunks = 0;
        loop {
            let chunk = service
                .read_media_chunk(&view.media_handle, offset)
                .unwrap();
            let mut decoded = Vec::with_capacity(chunk.size_bytes as usize);
            base64_decode_to_vec(&chunk.data_base64, &mut decoded)
                .expect("chunk base64 must be canonical");
            assert_eq!(decoded.len() as u64, chunk.size_bytes);
            assembled.extend_from_slice(&decoded);
            offset += chunk.size_bytes;
            chunks += 1;
            if chunk.eof {
                break;
            }
        }
        assert_eq!(assembled, body);
        assert_eq!(chunks, 1, "8 KiB fits one chunk; eof must be immediate");

        assert!(
            service
                .close_media_handle(&view.media_handle)
                .unwrap()
                .released
        );
        // Idempotent close, and a closed handle is unreadable.
        assert!(
            !service
                .close_media_handle(&view.media_handle)
                .unwrap()
                .released
        );
        let error = service
            .read_media_chunk(&view.media_handle, offset)
            .unwrap_err()
            .into_api();
        assert_eq!(error.code, "INVALID_REQUEST");
    }

    /// A missing file answers UPSTREAM_ERROR: not-downloaded and
    /// governor-deleted are indistinguishable (ADR 0002).
    #[test]
    fn media_open_answers_upstream_error_when_the_file_is_absent() {
        let (_temp, service) = media_service();
        let (account_id, conversation_id, message_id) = addressed_message(
            &service,
            "+15555550100",
            "+15555550101",
            "req-media-missing",
        );
        let error = service
            .open_media_handle(
                &account_id,
                &conversation_id,
                &message_id,
                "never-downloaded.dat",
            )
            .unwrap_err()
            .into_api();
        assert_eq!(error.code, "UPSTREAM_ERROR");
        assert!(error.retryable);
    }

    /// Non-sequential offsets fail closed at the service boundary with the
    /// shared INVALID_REQUEST answer.
    #[test]
    fn media_read_chunk_rejects_non_sequential_offsets() {
        let (_temp, service) = media_service();
        let (account_id, conversation_id, message_id) =
            addressed_message(&service, "+15555550100", "+15555550101", "req-media-seq");
        let attachment_dir = _temp.path().join("signal-data").join("attachments");
        std::fs::write(attachment_dir.join("seq.dat"), [0_u8; 4096]).unwrap();
        let view = service
            .open_media_handle(&account_id, &conversation_id, &message_id, "seq.dat")
            .unwrap();

        let error = service
            .read_media_chunk(&view.media_handle, 1)
            .unwrap_err()
            .into_api();
        assert_eq!(error.code, "INVALID_REQUEST");
        // The first read is still at offset 0 after the refusal.
        service
            .read_media_chunk(&view.media_handle, 0)
            .expect("a refused offset must not advance the stream");
    }

    /// Releasing an account's handles (the deleteLocalData hook) must not
    /// touch another account's live streams.
    #[test]
    fn media_handles_release_per_account_without_cross_account_effects() {
        let (_temp, service) = media_service();
        let (account_a, conversation_a, message_a) =
            addressed_message(&service, "+15555550100", "+15555550191", "req-media-a");
        let (account_b, conversation_b, message_b) =
            addressed_message(&service, "+15555550101", "+15555550192", "req-media-b");
        let attachment_dir = _temp.path().join("signal-data").join("attachments");
        std::fs::write(attachment_dir.join("a.dat"), [1_u8; 64]).unwrap();
        std::fs::write(attachment_dir.join("b.dat"), [2_u8; 64]).unwrap();
        let handle_a = service
            .open_media_handle(&account_a, &conversation_a, &message_a, "a.dat")
            .unwrap()
            .media_handle;
        let handle_b = service
            .open_media_handle(&account_b, &conversation_b, &message_b, "b.dat")
            .unwrap()
            .media_handle;

        service.clear_media_handles_for_account(&account_a);

        assert_eq!(
            service
                .read_media_chunk(&handle_a, 0)
                .unwrap_err()
                .into_api()
                .code,
            "INVALID_REQUEST"
        );
        // The other account's stream is untouched: single-origin by account.
        let chunk_b = service.read_media_chunk(&handle_b, 0).unwrap();
        assert_eq!(chunk_b.size_bytes, 64);
        assert!(chunk_b.eof);
    }

    /// The encoder emits the exact canonical form the upstream
    /// java.util.Base64 produces (RFC 4648 vectors), and every encoding
    /// round-trips through the decoder that guards attachment sends.
    #[test]
    fn base64_encode_matches_the_canonical_alphabet_and_round_trips() {
        for (raw, expected) in [
            (&b""[..], ""),
            (b"f".as_slice(), "Zg=="),
            (b"fo".as_slice(), "Zm8="),
            (b"foo".as_slice(), "Zm9v"),
            (b"foob".as_slice(), "Zm9vYg=="),
            (b"fooba".as_slice(), "Zm9vYmE="),
            (b"foobar".as_slice(), "Zm9vYmFy"),
        ] {
            assert_eq!(base64_encode(raw), expected, "{raw:?}");
        }
        // A full chunk's encoding is bounded exactly as the contract claims.
        assert_eq!(base64_encode(&[0_u8; 262_144]).len(), 349_528);
        // Random round-trip through the independent decoder.
        let mut value = [0_u8; 1000];
        use rand::RngCore;
        rand::rng().fill_bytes(&mut value);
        let mut decoded = Vec::new();
        base64_decode_to_vec(&base64_encode(&value), &mut decoded).unwrap();
        assert_eq!(decoded, value);
    }
}
