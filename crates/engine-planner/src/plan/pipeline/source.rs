use crate::plan::connection::plan::DatabaseDriver;
use chrono::{DateTime, Utc};
use connectors::sql::metadata::{column::ColumnMetadata, index::IndexMetadata};
use model::execution::row_count::RowCount;
use serde::Serialize;

#[derive(Serialize, Debug, Clone)]
pub struct SourcePlan {
    pub connection: String,
    pub table: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,

    /// Fully qualified name: schema.table
    pub fqn: String,
    pub driver: DatabaseDriver,

    /// Total rows in source table (before filters applied)
    pub total_rows: RowCount,

    /// Rows after WHERE filter applied (what will actually be migrated)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filtered_rows: Option<RowCount>,

    /// Columns in source table
    pub columns: Vec<ColumnInfo>,

    /// Primary key columns
    pub primary_key: Vec<String>,

    /// Relevant indexes
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub indexes: Vec<IndexInfo>,

    /// Size of the table in bytes
    pub size_bytes: u64,

    /// Last analyzed timestamp (unix epoch ms)
    pub last_analyzed: DateTime<Utc>,
}

impl SourcePlan {
    /// Returns the effective row count after filters are applied (if any)
    pub fn effective_row_count(&self) -> &RowCount {
        if let Some(r) = &self.filtered_rows {
            r
        } else {
            &self.total_rows
        }
    }
}

#[derive(Serialize, Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_length: Option<usize>,
    pub is_primary_key: bool,
    pub is_auto_increment: bool,
}

impl ColumnInfo {
    pub fn from_metadata(meta: &ColumnMetadata) -> Self {
        Self {
            name: meta.name.clone(),
            data_type: meta.data_type.clone(),
            nullable: meta.is_nullable,
            default: meta.default_value.clone(),
            max_length: meta.char_max_length,
            is_primary_key: meta.is_primary_key,
            is_auto_increment: meta.is_auto_increment,
        }
    }
}

#[derive(Serialize, Debug, Clone)]
pub struct IndexInfo {
    pub name: String,
    pub columns: Vec<String>,
    pub is_unique: bool,
    pub is_primary: bool,
}

impl IndexInfo {
    pub fn from_metadata(meta: &IndexMetadata) -> Self {
        let columns = meta.columns.iter().map(|c| c.name.clone()).collect();
        Self {
            name: meta.name.clone(),
            columns,
            is_unique: meta.is_unique,
            is_primary: meta.is_primary,
        }
    }
}
