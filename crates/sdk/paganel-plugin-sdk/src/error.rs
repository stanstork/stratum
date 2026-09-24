use serde::{Deserialize, Serialize};
use std::fmt;

/// Result type returned by plugin entry points.
pub type PluginResult<T> = Result<T, PluginError>;

/// A plugin-level error. Serialized to JSON and returned to the host.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginError {
    pub code: PluginErrorCode,
    pub message: String,
    /// Whether the host should treat this as transient (retryable).
    #[serde(default)]
    pub transient: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginErrorCode {
    /// Required input field missing or wrong type.
    InvalidInput,
    /// Plugin rejected the row (filter only).
    FilterRejected,
    /// Plugin tried to use a capability that wasn't granted.
    CapabilityDenied,
    /// External call (HTTP, KV) failed.
    ExternalError,
    /// Plugin-specific logic error.
    Internal,
    /// Plugin panicked; message contains the panic string.
    Panic,
}

impl PluginError {
    pub fn invalid_input(msg: impl Into<String>) -> Self {
        Self {
            code: PluginErrorCode::InvalidInput,
            message: msg.into(),
            transient: false,
        }
    }

    pub fn internal(msg: impl Into<String>) -> Self {
        Self {
            code: PluginErrorCode::Internal,
            message: msg.into(),
            transient: false,
        }
    }

    pub fn external(msg: impl Into<String>) -> Self {
        Self {
            code: PluginErrorCode::ExternalError,
            message: msg.into(),
            transient: true,
        }
    }

    pub fn capability_denied(name: &str) -> Self {
        Self {
            code: PluginErrorCode::CapabilityDenied,
            message: format!("capability '{}' not granted", name),
            transient: false,
        }
    }

    pub fn panic(msg: String) -> Self {
        Self {
            code: PluginErrorCode::Panic,
            message: msg,
            transient: false,
        }
    }

    /// Mark an error as transient (retryable).
    pub fn with_transient(mut self) -> Self {
        self.transient = true;
        self
    }
}

impl fmt::Display for PluginError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{:?}] {}", self.code, self.message)
    }
}

impl std::error::Error for PluginError {}

impl From<serde_json::Error> for PluginError {
    fn from(e: serde_json::Error) -> Self {
        PluginError::internal(format!("json error: {}", e))
    }
}

impl From<std::num::ParseIntError> for PluginError {
    fn from(e: std::num::ParseIntError) -> Self {
        PluginError::invalid_input(format!("int parse error: {}", e))
    }
}

impl From<std::num::ParseFloatError> for PluginError {
    fn from(e: std::num::ParseFloatError) -> Self {
        PluginError::invalid_input(format!("float parse error: {}", e))
    }
}

#[doc(hidden)]
pub fn serialize_error(err: &PluginError) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "error": err.message,
        "code": err.code,
        "transient": err.transient,
    }))
    .unwrap_or_else(|_| b"{\"error\":\"error serialization failed\"}".to_vec())
}
