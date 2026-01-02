use crate::plan::execution::types::RowCount;
use serde::Serialize;

#[derive(Serialize, Debug, Clone)]
pub struct JoinPlan {
    /// Alias (e.g., "users")
    pub alias: String,

    /// Source table being joined
    pub source_table: String,

    pub join_type: JoinType,
    pub conditions: Vec<JoinCondition>,

    /// Columns selected from this join
    pub columns_used: Vec<String>,

    /// Row count in joined table
    pub table_rows: RowCount,

    /// Estimated percentage of rows that match (0.0 to 1.0)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub match_rate: Option<f32>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

#[derive(Serialize, Debug, Clone)]
pub struct JoinCondition {
    pub left: JoinColumn,
    pub right: JoinColumn,
    pub expression: String,
    /// Whether the join columns have an index
    pub indexed: bool,
}

#[derive(Serialize, Debug, Clone)]
pub struct JoinColumn {
    pub table: String,
    pub column: String,
}

#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}
