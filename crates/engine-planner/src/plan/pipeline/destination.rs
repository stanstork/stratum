use crate::plan::{
    connection::plan::DatabaseDriver, execution::types::RowCount, pipeline::source::ColumnInfo,
};
use serde::Serialize;

#[derive(Serialize, Debug, Clone)]
pub struct DestinationPlan {
    pub connection: String,
    pub table: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,

    /// Fully qualified name
    pub fqn: String,
    pub driver: DatabaseDriver,

    /// Does target table exist?
    pub exists: bool,

    /// Current row count
    #[serde(skip_serializing_if = "RowCount::is_unknown")]
    pub current_rows: RowCount,

    /// Write mode
    pub mode: WriteMode,

    /// For upsert/merge: columns used to detect conflicts
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub conflict_keys: Vec<String>,

    /// Target columns
    pub columns: Vec<ColumnInfo>,

    /// Description of what will happen to existing data
    pub data_impact: DataImpact,
}

#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum WriteMode {
    Replace, // TRUNCATE + INSERT
    Append,  // INSERT only
    Upsert,  // INSERT ON CONFLICT UPDATE
    Merge,   // MERGE statement
}

impl WriteMode {
    pub fn description(&self) -> &'static str {
        match self {
            WriteMode::Replace => "Replace all data (truncate + insert)",
            WriteMode::Append => "Append new rows only",
            WriteMode::Upsert => "Insert or update existing rows",
            WriteMode::Merge => "Merge with conditional logic",
        }
    }

    pub fn is_destructive(&self) -> bool {
        matches!(self, WriteMode::Replace)
    }
}

#[derive(Serialize, Debug, Clone)]
pub struct DataImpact {
    pub action: DataImpactAction,
    pub description: String,
    pub is_destructive: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub affected_rows: Option<RowCount>,
}

#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum DataImpactAction {
    Truncate,
    Append,
    Upsert,
    Merge,
    Create,
}
