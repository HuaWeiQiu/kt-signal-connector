// SPDX-License-Identifier: AGPL-3.0-only

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::API_VERSION;

pub const MAX_REQUEST_ID_BYTES: usize = 128;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HostRequest {
    pub api_version: String,
    pub request_id: String,
    pub method: String,
    #[serde(default)]
    pub params: Value,
}

impl HostRequest {
    pub fn validate_envelope(&self) -> Result<(), ApiError> {
        if self.api_version != API_VERSION {
            return Err(ApiError::new(
                "UNSUPPORTED_VERSION",
                "only connector API 1.0 is supported",
                false,
            ));
        }
        if self.request_id.is_empty() || self.request_id.len() > MAX_REQUEST_ID_BYTES {
            return Err(ApiError::new(
                "INVALID_REQUEST",
                "requestId must contain between 1 and 128 bytes",
                false,
            ));
        }
        if !self.params.is_object() {
            return Err(ApiError::new(
                "INVALID_REQUEST",
                "params must be an object",
                false,
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HostResponse {
    pub api_version: &'static str,
    pub request_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ApiError>,
}

impl HostResponse {
    pub fn success(request_id: String, result: Value) -> Self {
        Self {
            api_version: API_VERSION,
            request_id,
            result: Some(result),
            error: None,
        }
    }

    pub fn failure(request_id: String, error: ApiError) -> Self {
        Self {
            api_version: API_VERSION,
            request_id,
            result: None,
            error: Some(error),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiError {
    pub code: &'static str,
    pub message: String,
    pub retryable: bool,
}

impl ApiError {
    pub fn new(code: &'static str, message: impl Into<String>, retryable: bool) -> Self {
        Self {
            code,
            message: message.into(),
            retryable,
        }
    }
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HostEvent<T: Serialize> {
    pub api_version: &'static str,
    pub event: &'static str,
    pub data: T,
}

impl<T: Serialize> HostEvent<T> {
    pub fn new(event: &'static str, data: T) -> Self {
        Self {
            api_version: API_VERSION,
            event,
            data,
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn response_has_exactly_one_outcome() {
        let success = serde_json::to_value(HostResponse::success("a".into(), json!({}))).unwrap();
        assert!(success.get("result").is_some());
        assert!(success.get("error").is_none());

        let failure = serde_json::to_value(HostResponse::failure(
            "b".into(),
            ApiError::new("INVALID_REQUEST", "bad request", false),
        ))
        .unwrap();
        assert!(failure.get("result").is_none());
        assert!(failure.get("error").is_some());
    }

    #[test]
    fn envelope_rejects_bad_version_and_identifier() {
        let request = HostRequest {
            api_version: "2.0".into(),
            request_id: String::new(),
            method: "runtime.status".into(),
            params: json!({}),
        };
        assert_eq!(
            request.validate_envelope().unwrap_err().code,
            "UNSUPPORTED_VERSION"
        );
    }
}
