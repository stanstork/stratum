use chrono::{DateTime, Utc};

/// Application lifecycle state
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum AppState {
    #[default]
    Initializing,
    Running,
    Paused,
    Completed,
    Failed(String),
}

impl AppState {
    pub fn is_running(&self) -> bool {
        matches!(self, Self::Running)
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Failed(_))
    }
}

/// Current view in the TUI
#[derive(Debug, Default, PartialEq, Eq)]
pub enum View {
    #[default]
    Overview,
    PipelineDetail,
    Errors,
    Help,
}

impl View {
    /// Cycle to the next view
    pub fn next(&self) -> Self {
        match self {
            Self::Overview => Self::PipelineDetail,
            Self::PipelineDetail => Self::Errors,
            Self::Errors => Self::Help,
            Self::Help => Self::Overview,
        }
    }
}

/// Error log entry
#[derive(Debug, Clone)]
pub struct ErrorEntry {
    pub timestamp: DateTime<Utc>,
    pub message: String,
    pub item_id: Option<String>,
}

impl ErrorEntry {
    pub fn new(message: String, item_id: Option<String>) -> Self {
        Self {
            timestamp: Utc::now(),
            message,
            item_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_app_state_is_running() {
        assert!(AppState::Running.is_running());
        assert!(!AppState::Paused.is_running());
        assert!(!AppState::Completed.is_running());
    }

    #[test]
    fn test_app_state_is_terminal() {
        assert!(AppState::Completed.is_terminal());
        assert!(AppState::Failed("error".to_string()).is_terminal());
        assert!(!AppState::Running.is_terminal());
        assert!(!AppState::Paused.is_terminal());
    }

    #[test]
    fn test_view_next() {
        assert_eq!(View::Overview.next(), View::PipelineDetail);
        assert_eq!(View::PipelineDetail.next(), View::Errors);
        assert_eq!(View::Errors.next(), View::Help);
        assert_eq!(View::Help.next(), View::Overview);
    }
}
