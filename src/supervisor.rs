// SPDX-License-Identifier: AGPL-3.0-only

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};
use tokio::sync::{Mutex, broadcast};

use crate::engine::{
    CallClass, EngineError, EngineEvent, EngineHandle, EngineState, EngineStatus,
    NormalizedReceive, SignalCliConfig, event_channel,
};
use crate::protocol::ApiError;
use crate::service::{ConnectorService, HostSideEvent, PreparedSend, ServiceError};
use crate::store::{AccountSummary, ConversationSummary, MessageRecord, Page, Store, StoreError};

const LINK_FINISH_TIMEOUT: Duration = Duration::from_secs(120);

pub struct RuntimeSupervisor {
    config: SignalCliConfig,
    engine: Mutex<Option<EngineHandle>>,
    service: Mutex<ConnectorService>,
    events: broadcast::Sender<EngineEvent>,
    host_events: broadcast::Sender<HostSideEvent>,
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
        let numbers = if let Ok(engine) = self.running_engine().await {
            let result = engine
                .call("listAccounts", json!({}), CallClass::ReadOnly)
                .await?;
            result
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
                .unwrap_or_default()
        } else {
            Vec::new()
        };
        self.service
            .lock()
            .await
            .sync_accounts_from_numbers(&numbers)
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
            .take_link_for_finish(&link_session_id)?;
        let result = engine
            .call_with_timeout(
                "finishLink",
                json!({
                    "deviceLinkUri": device_link_uri,
                    "deviceName": device_name,
                }),
                CallClass::Mutating,
                LINK_FINISH_TIMEOUT,
            )
            .await?;
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
        let account = self.service.lock().await.complete_link(number)?;
        let _ = self
            .host_events
            .send(HostSideEvent::AccountChanged(account.clone()));
        Ok(account)
    }

    pub async fn cancel_link(&self, link_session_id: String) -> Result<Value, ServiceError> {
        self.service.lock().await.cancel_link(&link_session_id)
    }

    pub async fn list_conversations(
        &self,
        account_id: String,
        limit: u32,
        cursor: Option<String>,
    ) -> Result<Page<ConversationSummary>, ServiceError> {
        self.service
            .lock()
            .await
            .list_conversations(&account_id, limit, cursor.as_deref())
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
