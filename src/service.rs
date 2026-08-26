// SPDX-License-Identifier: AGPL-3.0-only

use std::sync::Arc;
#[cfg(test)]
use std::time::Duration;

use serde::Deserialize;
use serde_json::{Value, json};
use thiserror::Error;

use crate::engine::{EngineError, NormalizedReceive};
use crate::ids::{mask_address, stable_hash_id};
use crate::link::{ActiveLinkSession, LINK_SESSION_TTL, now_ms};
use crate::protocol::ApiError;
use crate::store::{
    AccountDeletePlan, AccountRow, AccountSummary, ContactSummary, ConversationRow,
    ConversationSummary, MessageRecord, Page, Store, StoreError, SyncedContact,
};

const MAX_TEXT_BYTES: usize = 64 * 1024;
const MAX_INBOUND_TEXT_BYTES: usize = 128 * 1024;
const MAX_HOST_TEXT_PREVIEW_BYTES: usize = 4 * 1024;
const MAX_DEVICE_NAME_BYTES: usize = 64;

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
        let _ = self.take_link_session(link_session_id)?;
        // The binding is fixed here, at link.finish success, and immutable for
        // the life of the account (ADR 0001 R3).
        Ok(self
            .store
            .upsert_account_from_signal(number, Some(now_ms()), proxy_group)?)
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
        let account = self
            .store
            .account_by_id(account_id)?
            .ok_or(StoreError::AccountNotFound)?;
        let conversation = self
            .store
            .conversation_by_id(account_id, conversation_id)?
            .ok_or(StoreError::ConversationNotFound)?;
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
        let account = self
            .store
            .account_by_id(account_id)?
            .ok_or(StoreError::AccountNotFound)?;
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
        if conversation.kind == "group" {
            params["groupId"] = json!(conversation.peer_key);
        } else {
            params["recipient"] = json!([conversation.peer_key]);
        }
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
#[serde(rename_all = "camelCase")]
pub struct ContactsSyncParams {
    pub account_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContactsListParams {
    pub account_id: String,
    pub query: Option<String>,
    pub cursor: Option<String>,
    pub limit: u32,
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
    if value.is_empty() || value.len() > 128 {
        return Err(ServiceError::Api(ApiError::new(
            "INVALID_REQUEST",
            format!("{field} must contain between 1 and 128 bytes"),
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
}
