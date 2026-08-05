// SPDX-License-Identifier: AGPL-3.0-only

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use zeroize::Zeroize;

use crate::ids::random_id;

pub const LINK_SESSION_TTL: Duration = Duration::from_secs(5 * 60);

#[derive(Debug)]
pub struct ActiveLinkSession {
    pub session_id: String,
    pub device_name: String,
    device_link_uri: String,
    pub expires_at_ms: u64,
}

impl ActiveLinkSession {
    pub fn new(device_name: String, device_link_uri: String, ttl: Duration) -> Self {
        Self {
            session_id: random_id(),
            device_name,
            device_link_uri,
            expires_at_ms: now_ms().saturating_add(ttl.as_millis() as u64),
        }
    }

    pub fn is_expired(&self, now: u64) -> bool {
        now >= self.expires_at_ms
    }

    pub fn qr_payload(&self) -> &str {
        &self.device_link_uri
    }

    pub fn take_device_link_uri(&mut self) -> String {
        std::mem::take(&mut self.device_link_uri)
    }
}

impl Drop for ActiveLinkSession {
    fn drop(&mut self) {
        self.device_link_uri.zeroize();
        self.device_name.zeroize();
        self.session_id.zeroize();
    }
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_expires_and_zeroizes_uri_on_drop() {
        let mut session = ActiveLinkSession::new(
            "KT".into(),
            "sgnl://link?uuid=test".into(),
            Duration::from_millis(1),
        );
        assert!(!session.is_expired(0));
        assert!(session.is_expired(session.expires_at_ms));
        let uri = session.take_device_link_uri();
        assert_eq!(uri, "sgnl://link?uuid=test");
        drop(session);
    }
}
