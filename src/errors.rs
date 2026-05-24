use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCode {
    NotRegistered,
    AlreadyRegistered,
    InvalidRole,
    SessionExpired,
    SessionFull,
    InvalidSession,
    Unauthorized,
    PeerDisconnected,
    PeerNotFound,
    MalformedMessage,
    InternalError,
    RateLimitExceeded,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppError {
    #[serde(rename = "type")]
    pub error_type: String,
    pub code: ErrorCode,
    pub message: String,
}

impl AppError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            error_type: "error".to_string(),
            code,
            message: message.into(),
        }
    }

    #[allow(dead_code)]
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| {
            r#"{"type":"error","code":"INTERNAL_ERROR","message":"Failed to serialize error"}"#.to_string()
        })
    }
}
