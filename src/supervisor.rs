// SPDX-License-Identifier: AGPL-3.0-only

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};
use tokio::sync::{Mutex, broadcast};

use crate::engine::{
    CallClass, EngineError, EngineEvent, EngineHandle, EngineState, EngineStatus,
    NormalizedReceive, SignalCliConfig, event_channel,
};
use crate::protocol::ApiError;
use crate::service::{ConnectorService, HostSideEvent, PreparedSend, ServiceError};
use crate::store::{AccountSummary, ConversationSummary, MessageRecord, Page, Store, StoreError};

// Match link QR lifetime so a slow phone confirmation can still complete.
const LINK_FINISH_TIMEOUT: Duration = Duration::from_secs(5 * 60);

pub struct RuntimeSupervisor {
    config: SignalCliConfig,
    engine: Mutex<Option<EngineHandle>>,
    service: Mutex<ConnectorService>,
    events: broadcast::Sender<EngineEvent>,
    host_events: broadcast::Sender<HostSideEvent>,
    /// unix ms of last listContacts title enrich (throttle hot list path)
    last_title_enrich_ms: AtomicU64,
}

impl RuntimeSupervisor {
    pub fn new(config: SignalCliConfig, store: Store) -> Self {
        let (events, _) = event_channel();
        let (host_events, _) = broadcast::channel(1024);
        Self {
            config,
            engine: Mutex::new(None),
            service: Mutex::new(ConnectorService::new(store)),
            events,
            host_events,
            last_title_enrich_ms: AtomicU64::new(0),
        }
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
            })
    }

    pub async fn start(&self) -> Result<EngineStatus, EngineError> {
        let mut slot = self.engine.lock().await;
        if let Some(engine) = slot.as_ref() {
            if !engine.is_terminal() {
                return Err(EngineError::Backpressure);
            }
        }
        let engine = EngineHandle::start(self.config.clone(), self.events.clone()).await?;
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
        engine.shutdown().await?;
        Ok(EngineStatus {
            state: EngineState::Stopped,
            pid: None,
        })
    }

    pub async fn shutdown(&self) -> Result<(), EngineError> {
        let engine = self.engine.lock().await.take();
        self.service.lock().await.clear_link();
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
        // Prefer live signal-cli numbers. On engine error fall back to store so a
        // flaky listAccounts after link does not hard-fail the host. When the
        // engine is healthy and returns an empty list, do NOT surface store-only
        // "ghost" ready accounts from a partial/failed link.
        let engine = match self.running_engine().await {
            Ok(engine) => engine,
            Err(_) => return self.service.lock().await.list_accounts(),
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
                return self.service.lock().await.list_accounts();
            }
        };
        if numbers.is_empty() {
            return Ok(Vec::new());
        }
        // Keep listAccounts fast: do not call listContacts here (blocks the single
        // signal-cli queue and starves conversations.list / startLink).
        // Profile names are filled on finish_link and via refresh_account_profiles.
        let accounts = self
            .service
            .lock()
            .await
            .sync_accounts_from_numbers(&numbers)?;
        let _ = engine;
        Ok(accounts)
    }

    /// Optional profile refresh (not on the hot listAccounts path).
    pub async fn refresh_account_profiles(&self) -> Result<Vec<AccountSummary>, ServiceError> {
        let engine = self.running_engine().await?;
        let accounts = self.service.lock().await.list_accounts()?;
        let mut enriched = Vec::with_capacity(accounts.len());
        for account in accounts {
            let number = match self.service.lock().await.account_signal_number(&account.id) {
                Ok(n) => n,
                Err(_) => {
                    enriched.push(account);
                    continue;
                }
            };
            let display = self.fetch_self_display_name(&engine, &number).await;
            if let Some(ref name) = display {
                match self
                    .service
                    .lock()
                    .await
                    .set_account_display_name(&account.id, Some(name.as_str()))
                {
                    Ok(updated) => enriched.push(updated),
                    Err(_) => enriched.push(account),
                }
            } else {
                enriched.push(account);
            }
        }
        Ok(enriched)
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
        self.service
            .lock()
            .await
            .begin_link(device_name, device_link_uri)
    }

    pub async fn finish_link(
        &self,
        link_session_id: String,
    ) -> Result<AccountSummary, ServiceError> {
        let engine = self.running_engine().await?;
        let (device_name, device_link_uri) = self
            .service
            .lock()
            .await
            .peek_link_for_finish(&link_session_id)?;
        let result = match engine
            .call_with_timeout(
                "finishLink",
                json!({
                    "deviceLinkUri": device_link_uri,
                    "deviceName": device_name,
                }),
                CallClass::Mutating,
                LINK_FINISH_TIMEOUT,
            )
            .await
        {
            Ok(value) => value,
            Err(EngineError::Timeout) | Err(EngineError::UnknownOutcome) => {
                return Err(ServiceError::Api(ApiError::new(
                    "UPSTREAM_TIMEOUT",
                    "finishLink timed out waiting for phone approval; retry after scanning",
                    true,
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
            service.clear_link_session(&link_session_id);
            service.complete_link(number)?
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
        let _ = self
            .host_events
            .send(HostSideEvent::AccountChanged(account.clone()));
        Ok(account)
    }

    pub async fn cancel_link(&self, link_session_id: String) -> Result<Value, ServiceError> {
        self.service.lock().await.cancel_link(&link_session_id)
    }

    /// Clear local Signal account data (desktop exit). Not remote primary unregister.
    /// Tries signal-cli deleteLocalAccountData, then always purges connector store rows.
    pub async fn delete_local_account(&self, account_id: String) -> Result<Value, ServiceError> {
        let number = self
            .service
            .lock()
            .await
            .account_signal_number(&account_id)?;
        if let Ok(engine) = self.running_engine().await {
            // Best-effort: ignore upstream errors so a half-broken local account can still exit.
            let _ = engine
                .call_with_timeout(
                    "deleteLocalAccountData",
                    json!({
                        "account": number,
                        "ignoreRegistered": true,
                    }),
                    CallClass::Mutating,
                    Duration::from_secs(60),
                )
                .await;
        }
        self.service.lock().await.delete_account_local(&account_id)?;
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
            guard.clone().ok_or(ServiceError::Engine(EngineError::NotRunning))?
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
                let _ = service.store_ref().set_conversation_title_for_peer(
                    account_id,
                    "direct",
                    &peer_key,
                    &name,
                );
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

    pub async fn send_text(
        &self,
        account_id: String,
        conversation_id: String,
        text: String,
        client_request_id: String,
        quote_message_id: Option<String>,
    ) -> Result<MessageRecord, ServiceError> {
        let engine = self.running_engine().await?;
        let prepared = self.service.lock().await.prepare_send_text(
            &account_id,
            &conversation_id,
            &text,
            &client_request_id,
            quote_message_id.as_deref(),
        )?;
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
                    self.service
                        .lock()
                        .await
                        .complete_send_unknown(&pending_id)?;
                    Err(ServiceError::Api(ApiError::new(
                        "SEND_OUTCOME_UNKNOWN",
                        "mutating request has an unknown outcome",
                        false,
                    )))
                }
                Err(error) => {
                    self.service
                        .lock()
                        .await
                        .complete_send_failed(&pending_id)?;
                    Err(ServiceError::Engine(error))
                }
            },
        }
    }

    pub async fn ingest_receive(&self, receive: NormalizedReceive) {
        let events = self.service.lock().await.ingest_receive(receive);
        if let Ok(events) = events {
            for event in events {
                let _ = self.host_events.send(event);
            }
        }
    }
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

pub fn open_supervisor(
    signal_cli: PathBuf,
    signal_data_dir: PathBuf,
    state_dir: PathBuf,
) -> Result<Arc<RuntimeSupervisor>, StoreError> {
    let store = Store::open(&state_dir)?;
    Ok(Arc::new(RuntimeSupervisor::new(
        SignalCliConfig::new(signal_cli, signal_data_dir),
        store,
    )))
}

#[cfg(test)]
mod tests {
    use super::compose_contact_display_name;
    use serde_json::json;

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
}
