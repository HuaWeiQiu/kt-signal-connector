// SPDX-License-Identifier: AGPL-3.0-only

use tokio::sync::{Mutex, broadcast};

use crate::engine::{
    EngineError, EngineEvent, EngineHandle, EngineState, EngineStatus, SignalCliConfig,
    event_channel,
};

pub struct RuntimeSupervisor {
    config: SignalCliConfig,
    engine: Mutex<Option<EngineHandle>>,
    events: broadcast::Sender<EngineEvent>,
}

impl RuntimeSupervisor {
    pub fn new(config: SignalCliConfig) -> Self {
        let (events, _) = event_channel();
        Self {
            config,
            engine: Mutex::new(None),
            events,
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<EngineEvent> {
        self.events.subscribe()
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
        engine.shutdown().await?;
        Ok(EngineStatus {
            state: EngineState::Stopped,
            pid: None,
        })
    }

    pub async fn shutdown(&self) -> Result<(), EngineError> {
        let engine = self.engine.lock().await.take();
        if let Some(engine) = engine {
            engine.shutdown().await?;
        }
        Ok(())
    }
}
