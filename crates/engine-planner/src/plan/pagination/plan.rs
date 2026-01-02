use crate::plan::pagination::{cursor::CursorColumn, strategy::PaginationStrategy};
use serde::Serialize;

#[derive(Serialize, Debug, Clone)]
pub struct PaginationPlan {
    pub strategy: PaginationStrategy,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor_column: Option<CursorColumn>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub tiebreaker: Option<CursorColumn>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub timezone: Option<String>,

    /// Whether the cursor column has an index (affects performance)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub column_indexed: Option<bool>,
}

impl Default for PaginationPlan {
    fn default() -> Self {
        Self {
            strategy: PaginationStrategy::Default,
            cursor_column: None,
            tiebreaker: None,
            timezone: None,
            column_indexed: None,
        }
    }
}
