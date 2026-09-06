// SPDX-License-Identifier: AGPL-3.0-only

use std::sync::Arc;
#[cfg(test)]
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;
use unicode_segmentation::UnicodeSegmentation;

use crate::engine::{EngineError, NormalizedReceive};
use crate::groups::MAX_ACCOUNTS_PER_ENGINE;
use crate::ids::{mask_address, stable_hash_id};
use crate::link::{ActiveLinkSession, LINK_SESSION_TTL, now_ms};
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
/// Media PoC bound (contract revision 1.8, implementation-plan §4.7): 5 MiB
/// raw is the largest size whose standard base64 encoding stays a deliberate
/// margin under the engine's 8 MiB upstream stdout line limit — a longer
/// line faults the shared engine (oversized-output semantics). The task
/// allowed up to 10 MiB, but 10 MiB raw is ~13.7 MiB base64: incompatible.
pub const MAX_ATTACHMENT_BYTES: usize = 5 * 1024 * 1024;
/// Encoded length of [`MAX_ATTACHMENT_BYTES`]: 4 * ceil(n / 3), the exact
/// padded-base64 length java.util.Base64 emits.
const MAX_ATTACHMENT_BASE64_CHARS: usize = 4 * MAX_ATTACHMENT_BYTES.div_ceil(3);
/// An upstream attachment id (receive-time metadata field `id`), bounded to
/// the schema's `attachmentId` maxLength (schemas/connector-api-v1.schema.json
/// is the single source for this length); Signal ids stay far below it, the
/// bound only rejects absurd values early.
pub const MAX_ATTACHMENT_ID_BYTES: usize = 128;
/// Every opaque identifier the host frame carries (accountId, conversationId,
/// messageId, clientRequestId, operationId, peerKey, peerTitle, query,
/// groupKey — the schema's opaqueId family and its inline 128 peers).
pub const MAX_OPAQUE_ID_BYTES: usize = 128;

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
    },
}

pub struct ConnectorService {
    store: Arc<Store>,
    link: Option<ActiveLinkSession>,
}

impl ConnectorService {
    pub fn new(store: Arc<Store>) -> Self {
        Self { store, link: None }
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

    pub fn delete_account_local(&mut self, account_id: &str) -> Result<bool, ServiceError> {
        Ok(self.store.delete_account_cascade(account_id)?)
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
        if self.store.account_by_id(account_id)?.is_none() {
            return Err(ServiceError::Store(StoreError::AccountNotFound));
        }
        Ok(self.store.list_conversations(account_id, limit, cursor)?)
    }

    /// Read-only view of the contacts cache; never touches the upstream engine.
    pub fn list_contacts(
        &self,
        account_id: &str,
        query: Option<&str>,
        limit: u32,
        cursor: Option<&str>,
    ) -> Result<Page<ContactSummary>, ServiceError> {
        if self.store.account_by_id(account_id)?.is_none() {
            return Err(ServiceError::Store(StoreError::AccountNotFound));
        }
        let query = query.map(str::trim).filter(|value| !value.is_empty());
        if let Some(query) = query {
            validate_opaque_id(query, "query")?;
        }
        Ok(self.store.list_contacts(account_id, query, limit, cursor)?)
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
        validate_opaque_id(account_id, "accountId")?;
        validate_opaque_id(group_key, "groupKey")?;
        if self.store.account_by_id(account_id)?.is_none() {
            return Err(ServiceError::Store(StoreError::AccountNotFound));
        }
        let Some((title, extra, synced_at)) =
            self.store.contact_by_peer(account_id, "group", group_key)?
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
    /// `messages.attachments.get`).
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
        if self.store.account_by_id(account_id)?.is_none() {
            return Err(ServiceError::Store(StoreError::AccountNotFound));
        }
        if self
            .store
            .conversation_by_id(account_id, conversation_id)?
            .is_none()
        {
            return Err(ServiceError::Store(StoreError::ConversationNotFound));
        }
        // Viewing the thread marks it read (local badge only; no Signal receipt RPC yet).
        let _ = self
            .store
            .clear_conversation_unread(account_id, conversation_id)?;
        let mut page = self
            .store
            .list_messages(account_id, conversation_id, limit, before)?;
        page.items = page
            .items
            .into_iter()
            .map(project_message_for_host)
            .collect();
        Ok(page)
    }

    pub fn get_message_text(
        &self,
        account_id: &str,
        conversation_id: &str,
        message_id: &str,
    ) -> Result<MessageText, ServiceError> {
        validate_opaque_id(account_id, "accountId")?;
        validate_opaque_id(conversation_id, "conversationId")?;
        validate_opaque_id(message_id, "messageId")?;
        let message = self
            .store
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

    pub fn prepare_send_text(
        &self,
        account_id: &str,
        conversation_id: &str,
        text: &str,
        client_request_id: &str,
        quote_message_id: Option<&str>,
    ) -> Result<PreparedSend, ServiceError> {
        validate_text(text)?;
        validate_opaque_id(client_request_id, "clientRequestId")?;
        if let Some(quote) = quote_message_id {
            validate_opaque_id(quote, "quoteMessageId")?;
        }
        if let Some(existing) = self
            .store
            .message_by_client_request(account_id, client_request_id)?
        {
            return Ok(PreparedSend::Existing(existing));
        }
        let account = self.resolve_account(account_id)?;
        let conversation = self.resolve_conversation(account_id, conversation_id)?;
        self.dispatch_send(
            &account,
            &conversation,
            text,
            client_request_id,
            quote_message_id,
        )
    }

    /// Prepare a send addressed by peer (kind + peer_key) instead of an
    /// existing conversation id. Intentional design: when no conversation for
    /// the peer exists yet, it is created together with the first outgoing
    /// message, so the conversation only becomes visible/active once the first
    /// message is actually sent — no empty conversation skeletons are produced.
    pub fn prepare_send_text_to_peer(
        &self,
        account_id: &str,
        peer: &PeerTarget<'_>,
        text: &str,
        client_request_id: &str,
        quote_message_id: Option<&str>,
    ) -> Result<PreparedSend, ServiceError> {
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
            return Ok(PreparedSend::Existing(existing));
        }
        let account = self.resolve_account(account_id)?;
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
        )
    }

    fn dispatch_send(
        &self,
        account: &AccountRow,
        conversation: &ConversationRow,
        text: &str,
        client_request_id: &str,
        quote_message_id: Option<&str>,
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
            sent_at: now_ms(),
            received_at: None,
            text: Some(text.to_string()),
            text_bytes: Some(text.len() as u32),
            text_truncated: false,
            text_retrievable: true,
            status: "pending",
            client_request_id: Some(client_request_id.to_string()),
            quote_message_id: quote_message_id.map(str::to_string),
        };
        let inserted =
            self.store
                .insert_message(&pending, Some(client_request_id), Some(text), false)?;
        if !inserted {
            if let Some(existing) = self
                .store
                .message_by_client_request(account_id, client_request_id)?
            {
                return Ok(PreparedSend::Existing(existing));
            }
        }

        let mut params = json!({
            "account": account.signal_account,
            "message": text,
        });
        set_upstream_target(&mut params, conversation);
        // signal-cli JSON-RPC send quote parameters (verified against the
        // pinned 0.14.7 distribution): quoteTimestamp is the quoted message's
        // Signal timestamp, quoteAuthor its author's number — both required.
        if let Some((quote_timestamp, quote_author)) = quote {
            params["quoteTimestamp"] = json!(quote_timestamp);
            params["quoteAuthor"] = json!(quote_author);
        }
        Ok(PreparedSend::Dispatch {
            pending_id,
            account_id: account_id.to_string(),
            conversation_id: conversation_id.to_string(),
            params,
            pending_sent_at: pending.sent_at,
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
            // We authored it: the author is the linked account itself. Only a
            // completed send carries the upstream timestamp Signal quotes
            // match on; pending/failed/unknown rows would misquote.
            "outgoing" if quoted.status == "sent" => account.signal_account.clone(),
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

    /// Pure local validation for `messages.remoteDelete` (docs/remote-delete-l2-plan.md §3.2):
    /// resolve the target row and build the exact upstream jsonRpc params. Only an
    /// outgoing row in the terminal state `sent` carries a real Signal protocol identity
    /// — its `sent_at` was overwritten with the send response's upstream timestamp by
    /// `complete_outgoing_send`, the same precedent as quote resolution
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
        if message.direction != "outgoing" || message.status != "sent" {
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
    /// the terminal state `sent` (its `sent_at` is the upstream Signal
    /// timestamp, same precedent as `resolve_quote`/`prepare_remote_delete`),
    /// an incoming row carries the envelope timestamp. The target author
    /// follows the direction: the linked account's own number for outgoing
    /// rows, the conversation peer for incoming rows. Outgoing rows without a
    /// terminal `sent` state carry no protocol identity and answer
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
            // We authored it: the author is the linked account itself. Only a
            // completed send carries the upstream timestamp Signal reactions
            // match on; pending/failed/unknown rows would mis-target.
            "outgoing" if message.status == "sent" => account.signal_account.clone(),
            // An outgoing row without the terminal `sent` state carries no
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
        })
    }

    /// Pure local validation for `messages.attachments.get`
    /// (docs/implementation-plan.md §4.7, upstream `getAttachment`): resolve
    /// the addressed rows and build the exact upstream jsonRpc params. The
    /// connector persists no attachment metadata this revision, so the
    /// attachment id cannot be validated against the message row — the caller
    /// learns ids out of band (PoC boundary, §4.7). The row lookups keep the
    /// addressing honest: a bogus conversation or message answers
    /// CONVERSATION_NOT_FOUND / MESSAGE_NOT_FOUND before any upstream call.
    pub fn prepare_get_attachment(
        &self,
        account_id: &str,
        conversation_id: &str,
        message_id: &str,
        attachment_id: &str,
        size_bytes: u64,
    ) -> Result<PreparedGetAttachment, ServiceError> {
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
        if size_bytes == 0 || size_bytes as usize > MAX_ATTACHMENT_BYTES {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "sizeBytes must be between 1 and 5242880",
                false,
            )));
        }
        let (account, _conversation, _message) =
            self.resolve_target(account_id, conversation_id, message_id)?;
        // signal-cli JSON-RPC getAttachment parameters (verified against the
        // pinned 0.14.7 distribution: GetAttachmentCommand reads only `id` in
        // jsonRpc mode — the CLI-side recipient/group-id flags never reach the
        // local-command handler — and AttachmentStore.retrieveAttachment is a
        // pure local file read): `account` is consumed by the upstream
        // multi-account dispatcher, `id` addresses the already-downloaded file.
        Ok(PreparedGetAttachment {
            account_id: account_id.to_string(),
            conversation_id: conversation_id.to_string(),
            message_id: message_id.to_string(),
            attachment_id: attachment_id.to_string(),
            params: json!({
                "account": account.signal_account,
                "id": attachment_id,
            }),
        })
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
        let (updated, status_transitioned) = self
            .store
            .complete_outgoing_send(pending_id, account_id, conversation_id, sent_at)?
            .ok_or_else(|| {
                ServiceError::Api(ApiError::new(
                    "SEND_OUTCOME_UNKNOWN",
                    "send completed upstream but the local pending record is gone",
                    false,
                ))
            })?;
        let mut events = vec![HostSideEvent::MessageUpserted(project_message_for_host(
            updated.clone(),
        ))];
        if status_transitioned {
            events.push(status_changed_event(&updated));
        }
        if let Some(conversation) = self.store.conversation_summary(conversation_id)? {
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
        &self,
        receive: NormalizedReceive,
        owner_group: &str,
    ) -> Result<Vec<HostSideEvent>, ServiceError> {
        if receive.direction == "skip" {
            return Ok(Vec::new());
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
        let message = MessageRecord {
            id: message_id,
            account_id: account.id.clone(),
            conversation_id: conversation.id.clone(),
            direction,
            sender_id,
            sent_at,
            received_at: Some(now_ms()),
            text: stored_text,
            text_bytes,
            text_truncated: !text_complete,
            text_retrievable: text_complete,
            status,
            client_request_id: None,
            quote_message_id: None,
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
        if let Some(conversation) = self.store.conversation_summary(&conversation.id)? {
            events.push(HostSideEvent::ConversationChanged(conversation));
        }
        if let Some(account) = self.store.account_summary(&account.id)? {
            events.push(HostSideEvent::AccountChanged(account));
        }
        Ok(events)
    }

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
    Existing(MessageRecord),
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
}

/// Everything the supervisor needs to run one upstream `getAttachment` call,
/// produced by the pure local `prepare_get_attachment` validation.
#[derive(Debug)]
pub struct PreparedGetAttachment {
    pub account_id: String,
    pub conversation_id: String,
    pub message_id: String,
    pub attachment_id: String,
    pub params: Value,
}

/// The attachment payload the host receives (contract revision 1.8): the
/// upstream base64 passthrough, verified against the declared size. The
/// upstream jsonRpc response is exactly `{"data": "<base64>"}` (JsonAttachmentData);
/// `attachmentId` echoes the request so a caller can correlate parallel fetches.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AttachmentPayload {
    pub attachment_id: String,
    pub data: String,
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

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MessagesGetAttachmentParams {
    pub account_id: String,
    pub conversation_id: String,
    pub message_id: String,
    pub attachment_id: String,
    pub size_bytes: u64,
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

/// The upstream base64 must decode to exactly the size the caller declared
/// (contract revision 1.8): anything else means the host budgeted for a
/// different payload. Encoding length is checked first so a hostile upstream
/// response cannot inflate memory before the byte budget is confirmed.
pub fn validate_attachment_payload(
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
            "attachment exceeds the 5242880-byte PoC limit",
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

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
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

        assert!(service.delete_account_local(&account.id).unwrap());
        assert!(!service.delete_account_local(&account.id).unwrap());
        assert!(service.cancel_link(&link_session_id).is_ok());
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
        let (_temp, service) = service();
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
        let (_temp, service) = service();
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
    fn outgoing_sync_reconciles_by_signal_timestamp_and_client_request_id() {
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
                "same text is not identity",
                "client-request-exact",
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
            .prepare_send_text(&account.id, &conversation.id, "in flight", "req-race", None)
            .unwrap()
        {
            PreparedSend::Dispatch { pending_id, .. } => pending_id,
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        };

        // The deletion race: the account rows disappear while the upstream
        // send is still in flight.
        assert!(service.delete_account_local(&account.id).unwrap());

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
        let (_temp, service) = service();
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

    #[test]
    fn receive_without_signal_timestamp_has_no_persistence_side_effects() {
        let (_temp, service) = service();
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
                )
                .unwrap(),
            PreparedSend::Existing(_)
        ));
        assert!(matches!(
            service
                .prepare_send_text(&account.id, &first, "hello peer", "peer-req-1", None)
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
            ),
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
            ),
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
        let (_temp, service) = service();
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
        let (_temp, service) = service();
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
            ),
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
            ),
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
            .prepare_send_text(&account.id, &direct.id, "delete me", "req-rd-1", None)
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
            .prepare_send_text(&account.id, &group.id, "group delete", "req-rd-2", None)
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
            .prepare_send_text(&account.id, &conversation.id, "in flight", "req-rd-p", None)
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
        let (_temp, service) = service();
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
            .prepare_send_text(&account.id, &direct.id, "react to me", "req-sr-1", None)
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
            .prepare_send_text(&account.id, &group.id, "group react", "req-sr-2", None)
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
            .prepare_send_text(&account.id, &conversation.id, "in flight", "req-sr-p", None)
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
            .prepare_send_text(&account.id, &conversation.id, "will fail", "req-fail", None)
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

    /// messages.attachments.get prepare (contract revision 1.8): the upstream
    /// params are exactly `account` + `id`; the addressed conversation and
    /// message rows must exist (deterministic NOT_FOUND answers before any
    /// upstream call); the declared size and the attachment id shape are
    /// bounded before anything is dispatched (implementation-plan §4.7).
    #[test]
    fn prepare_get_attachment_builds_upstream_params_and_enforces_bounds() {
        let (_temp, service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let direct = service
            .store_ref()
            .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
            .unwrap();
        let sent_id = match service
            .prepare_send_text(&account.id, &direct.id, "has attachment", "req-att-1", None)
            .unwrap()
        {
            PreparedSend::Dispatch { pending_id, .. } => pending_id,
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        };
        service
            .complete_send_success(&sent_id, &account.id, &direct.id, 800)
            .unwrap();

        let prepared = service
            .prepare_get_attachment(&account.id, &direct.id, &sent_id, "att-1", 24)
            .unwrap();
        assert_eq!(prepared.params["account"], json!("+15555550100"));
        assert_eq!(prepared.params["id"], json!("att-1"));
        assert_eq!(prepared.attachment_id, "att-1");
        assert_eq!(prepared.conversation_id, direct.id);
        assert_eq!(prepared.message_id, sent_id);

        // Missing rows answer deterministically before any upstream call.
        for (conversation_id, message_id, expected) in [
            (
                "no-such-conversation",
                sent_id.as_str(),
                "CONVERSATION_NOT_FOUND",
            ),
            (direct.id.as_str(), "no-such-message", "MESSAGE_NOT_FOUND"),
        ] {
            let error = service
                .prepare_get_attachment(&account.id, conversation_id, message_id, "att-1", 24)
                .unwrap_err()
                .into_api();
            assert_eq!(error.code, expected);
        }
        let error = service
            .prepare_get_attachment("no-such-account", &direct.id, &sent_id, "att-1", 24)
            .unwrap_err()
            .into_api();
        assert_eq!(error.code, "ACCOUNT_NOT_FOUND");

        // Shape bounds: attachment id, declared size.
        for (attachment_id, size_bytes) in [
            ("", 24_u64),
            (&"a".repeat(257), 24),
            ("att-1", 0),
            ("att-1", (MAX_ATTACHMENT_BYTES + 1) as u64),
        ] {
            let error = service
                .prepare_get_attachment(
                    &account.id,
                    &direct.id,
                    &sent_id,
                    attachment_id,
                    size_bytes,
                )
                .unwrap_err()
                .into_api();
            assert_eq!(
                error.code, "INVALID_REQUEST",
                "{attachment_id} {size_bytes}"
            );
            assert!(!error.retryable);
        }
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
                (MAX_ATTACHMENT_BYTES - 2) as u64, // largest 3-divisible size at the bound
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
}
