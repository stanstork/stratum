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
}
