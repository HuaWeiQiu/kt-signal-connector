// SPDX-License-Identifier: AGPL-3.0-only

use serde::Deserialize;
use serde_json::{Value, json};
use thiserror::Error;

use crate::engine::{EngineError, NormalizedReceive};
use crate::ids::{mask_address, stable_hash_id};
use crate::link::{ActiveLinkSession, LINK_SESSION_TTL, now_ms};
use crate::protocol::ApiError;
use crate::store::{
    AccountDeletePlan, AccountSummary, ConversationSummary, MessageRecord, Page, Store, StoreError,
};

const MAX_TEXT_BYTES: usize = 64 * 1024;
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
    AccountChanged(AccountSummary),
    ConversationChanged(ConversationSummary),
    MessageUpserted(MessageRecord),
    MessageStatusChanged {
        message_id: String,
        status: &'static str,
    },
}

pub struct ConnectorService {
    store: Store,
    link: Option<ActiveLinkSession>,
}

impl ConnectorService {
    pub fn new(store: Store) -> Self {
        Self { store, link: None }
    }

    pub fn store_ref(&self) -> &Store {
        &self.store
    }

    pub fn clear_link(&mut self) {
        self.link = None;
    }

    pub fn sync_accounts_from_numbers(
        &self,
        numbers: &[String],
    ) -> Result<Vec<AccountSummary>, ServiceError> {
        for number in numbers {
            let _ = self.store.upsert_account_from_signal(number, None)?;
        }
        Ok(self.store.list_accounts()?)
    }

    pub fn list_accounts(&self) -> Result<Vec<AccountSummary>, ServiceError> {
        Ok(self.store.list_accounts()?)
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
    ) -> Result<AccountSummary, ServiceError> {
        let _ = self.take_link_session(link_session_id)?;
        Ok(self
            .store
            .upsert_account_from_signal(number, Some(now_ms()))?)
    }

    pub fn cancel_link(&mut self, link_session_id: &str) -> Result<Value, ServiceError> {
        let _ = self.take_link_session(link_session_id)?;
        Ok(json!({}))
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
        Ok(self
            .store
            .list_messages(account_id, conversation_id, limit, before)?)
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

        let pending_id =
            stable_hash_id(&[account_id, conversation_id, "outgoing", client_request_id]);
        let pending = MessageRecord {
            id: pending_id.clone(),
            account_id: account_id.to_string(),
            conversation_id: conversation_id.to_string(),
            direction: "outgoing",
            sender_id: "self".into(),
            sent_at: now_ms(),
            received_at: None,
            text: Some(text.to_string()),
            attachments: Vec::new(),
            status: "pending",
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
        Ok(PreparedSend::Dispatch {
            pending_id,
            account_id: account_id.to_string(),
            conversation_id: conversation_id.to_string(),
            params,
            pending_sent_at: pending.sent_at,
        })
    }

    pub fn complete_send_success(
        &self,
        pending_id: &str,
        account_id: &str,
        conversation_id: &str,
        sent_at: u64,
    ) -> Result<(MessageRecord, Vec<HostSideEvent>), ServiceError> {
        let updated = self
            .store
            .update_message_status(pending_id, "sent", Some(sent_at))?
            .ok_or(StoreError::Unavailable)?;
        let mut events = vec![HostSideEvent::MessageUpserted(updated.clone())];
        if let Some(conversation) = self.store.conversation_summary(conversation_id)? {
            events.push(HostSideEvent::ConversationChanged(conversation));
        }
        if let Some(account) = self.store.account_summary(account_id)? {
            events.push(HostSideEvent::AccountChanged(account));
        }
        Ok((updated, events))
    }

    pub fn complete_send_unknown(&self, pending_id: &str) -> Result<(), ServiceError> {
        let _ = self
            .store
            .update_message_status(pending_id, "unknown", None)?;
        Ok(())
    }

    pub fn complete_send_failed(&self, pending_id: &str) -> Result<(), ServiceError> {
        let _ = self
            .store
            .update_message_status(pending_id, "failed", None)?;
        Ok(())
    }

    pub fn ingest_receive(
        &self,
        receive: NormalizedReceive,
    ) -> Result<Vec<HostSideEvent>, ServiceError> {
        if receive.direction == "skip" {
            return Ok(Vec::new());
        }
        if !matches!(receive.direction, "incoming" | "outgoing" | "system") {
            return Ok(Vec::new());
        }
        // signal-cli may omit `account` on single-account jsonRpc; fall back to sole store account.
        let signal_account = match receive.account.as_deref().filter(|s| !s.is_empty()) {
            Some(account) => account.to_string(),
            None => {
                let accounts = self.store.list_accounts()?;
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
            .upsert_account_from_signal(&signal_account, None)?;
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
        let sent_at = receive.timestamp.unwrap_or_else(now_ms);
        let direction = receive.direction;
        let sender_id = if direction == "outgoing" {
            account.id.clone()
        } else {
            stable_hash_id(&[&account.id, kind, &peer_key])
        };
        let message_id = stable_hash_id(&[
            &account.id,
            &conversation.id,
            direction,
            &sent_at.to_string(),
            &sender_id,
            receive.text.as_deref().unwrap_or(""),
        ]);
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
            text: receive.text.clone(),
            attachments: Vec::new(),
            status,
            quote_message_id: None,
        };
        let preview = match direction {
            "system" => receive
                .text
                .as_deref()
                .map(|text| text.chars().take(120).collect::<String>()),
            _ => receive
                .text
                .as_deref()
                .map(|text| text.chars().take(120).collect::<String>()),
        };
        let increment_unread = direction == "incoming";
        let inserted =
            self.store
                .insert_message(&message, None, preview.as_deref(), increment_unread)?;
        if !inserted {
            return Ok(Vec::new());
        }
        let mut events = vec![HostSideEvent::MessageUpserted(message)];
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

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AccountDeleteLocalDataParams {
    pub account_id: String,
    pub operation_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LinkStartParams {
    pub device_name: String,
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
pub struct MessagesSendTextParams {
    pub account_id: String,
    pub conversation_id: String,
    pub text: String,
    pub client_request_id: String,
    pub quote_message_id: Option<String>,
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

    fn service() -> (TempDir, ConnectorService) {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path()).unwrap();
        (temp, ConnectorService::new(store))
    }

    #[test]
    fn cancelled_link_cannot_complete_or_create_an_account() {
        let (_temp, mut service) = service();
        let started = service
            .begin_link("KT".into(), "sgnl://link?test".into())
            .unwrap();
        let link_session_id = started["linkSessionId"].as_str().unwrap().to_string();

        service.cancel_link(&link_session_id).unwrap();
        let late = service.complete_link_session(&link_session_id, "+15555550100");

        assert!(matches!(late, Err(ServiceError::Api(_))));
        assert!(service.list_accounts().unwrap().is_empty());
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

    #[test]
    fn deleting_an_account_does_not_cancel_another_session_link() {
        let (_temp, mut service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()])
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
}
