use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FilterDecision {
    Pass,
    Reject { reason: String },
}

impl FilterDecision {
    pub fn pass() -> Self {
        Self::Pass
    }
    pub fn reject(reason: impl Into<String>) -> Self {
        Self::Reject {
            reason: reason.into(),
        }
    }

    pub fn is_pass(&self) -> bool {
        matches!(self, Self::Pass)
    }

    /// Wire format expected by the host:
    ///   pass   -> `{"pass": true}`
    ///   reject -> `{"pass": false, "reason": "..."}`
    pub fn to_json_bytes(&self) -> Vec<u8> {
        let json = match self {
            Self::Pass => serde_json::json!({ "pass": true }),
            Self::Reject { reason } => serde_json::json!({
                "pass": false,
                "reason": reason,
            }),
        };
        serde_json::to_vec(&json).unwrap_or_default()
    }
}
