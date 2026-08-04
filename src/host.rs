// SPDX-License-Identifier: AGPL-3.0-only

use std::collections::{HashSet, VecDeque};
use std::io;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use rand::RngCore;
use serde::Serialize;
use serde_json::{Value, json};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::broadcast;
use tokio::time::timeout;
use tokio_util::codec::{Framed, LinesCodec};

use crate::auth::{BootstrapSecret, HandshakeParams, PendingChallenge};
use crate::engine::{EngineError, EngineEvent};
use crate::ipc::LocalListener;
use crate::protocol::{ApiError, HostEvent, HostRequest, HostResponse};
use crate::supervisor::RuntimeSupervisor;
use crate::{API_VERSION, DEFAULT_HOST_FRAME_LIMIT};

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
const RECENT_REQUEST_IDS: usize = 128;

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
        let connection = handle_connection(stream, secret.clone(), supervisor.clone());
        let connection_result = tokio::select! {
            result = connection => Some(result),
            signal = tokio::signal::ctrl_c() => {
                signal?;
                None
            }
        };
        supervisor
            .shutdown()
            .await
            .map_err(|_| HostError::RuntimeShutdown)?;
        match connection_result {
            None => return Ok(()),
            Some(Ok(())) => {}
            Some(Err(error)) => {
                if !matches!(error, HostError::Authentication | HostError::InvalidFrame) {
                    return Err(error);
                }
            }
        }
    }
}

async fn handle_connection<S>(
    stream: S,
    secret: Arc<BootstrapSecret>,
    supervisor: Arc<RuntimeSupervisor>,
) -> Result<(), HostError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let codec = LinesCodec::new_with_max_length(DEFAULT_HOST_FRAME_LIMIT);
    let mut framed = Framed::new(stream, codec);
    let challenge = PendingChallenge::generate();
    send_json(
        &mut framed,
        &HostEvent::new("runtime.challenge", challenge.public()),
    )
    .await?;

    let handshake_line = timeout(HANDSHAKE_TIMEOUT, framed.next())
        .await
        .map_err(|_| HostError::Authentication)?
        .ok_or(HostError::Authentication)?
        .map_err(|_| HostError::InvalidFrame)?;
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

    let handshake_request_id = handshake.request_id;
    send_json(
        &mut framed,
        &HostResponse::success(
            handshake_request_id.clone(),
            json!({
                "sessionId": random_identifier(),
                "apiVersion": API_VERSION,
                "capabilities": ["runtime.status", "runtime.start", "runtime.stop"]
            }),
        ),
    )
    .await?;

    let mut recent_ids = RecentRequestIds::default();
    recent_ids.insert(handshake_request_id);
    let mut events = supervisor.subscribe();
    loop {
        tokio::select! {
            line = framed.next() => {
                let Some(line) = line else {
                    return Ok(());
                };
                let line = line.map_err(|_| HostError::InvalidFrame)?;
                let request: HostRequest = serde_json::from_str(&line).map_err(|_| HostError::InvalidFrame)?;
                let response = if let Err(error) = request.validate_envelope() {
                    HostResponse::failure(request.request_id, error)
                } else if !recent_ids.insert(request.request_id.clone()) {
                    HostResponse::failure(
                        request.request_id,
                        ApiError::new("INVALID_REQUEST", "requestId was already used in this session", false),
                    )
                } else {
                    dispatch(request, &supervisor).await
                };
                send_json(&mut framed, &response).await?;
            }
            event = events.recv() => {
                match event {
                    Ok(event) => send_engine_event(&mut framed, event).await?,
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        send_json(
                            &mut framed,
                            &HostEvent::new(
                                "runtime.protocolWarning",
                                json!({ "kind": "eventBackpressure" }),
                            ),
                        )
                        .await?;
                    }
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                }
            }
        }
    }
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
        "accounts.list" | "link.start" | "link.finish" | "link.cancel" | "conversations.list"
        | "messages.list" | "messages.sendText" => HostResponse::failure(
            request_id,
            ApiError::new(
                "CAPABILITY_UNAVAILABLE",
                "capability is not available in this connector phase",
                false,
            ),
        ),
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

async fn send_engine_event<S>(
    framed: &mut Framed<S, LinesCodec>,
    event: EngineEvent,
) -> Result<(), HostError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    match event {
        EngineEvent::StateChanged(status) => {
            send_json(framed, &HostEvent::new("runtime.stateChanged", status)).await
        }
        EngineEvent::Receive(receive) => {
            send_json(framed, &HostEvent::new("signal.receive", receive)).await
        }
        EngineEvent::ProtocolWarning { kind } => {
            send_json(
                framed,
                &HostEvent::new("runtime.protocolWarning", json!({ "kind": kind })),
            )
            .await
        }
    }
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
    use tokio::io::duplex;

    use super::*;
    use crate::auth::BootstrapSecret;
    use crate::engine::SignalCliConfig;

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

    #[tokio::test]
    async fn authenticated_session_can_query_status_and_rejects_replayed_id() {
        let secret_bytes = [7_u8; 32];
        let secret = Arc::new(BootstrapSecret::for_test(secret_bytes));
        let supervisor = Arc::new(RuntimeSupervisor::new(SignalCliConfig::new(
            PathBuf::from("unused-signal-cli"),
            PathBuf::from("unused-signal-data"),
        )));
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
        let supervisor = Arc::new(RuntimeSupervisor::new(SignalCliConfig::new(
            PathBuf::from("unused-signal-cli"),
            PathBuf::from("unused-signal-data"),
        )));
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
