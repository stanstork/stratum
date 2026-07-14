use crate::plan::{sample::preview::SampleDataPreview, transform::mapping::ColumnMapping};
use model::execution::row_count::RowCount;
use serde::Serialize;

/// One discovered table within a graph/cascade pipeline (`with references`).
#[derive(Serialize, Debug, Clone)]
pub struct CascadeTablePlan {
    /// Source table name (as discovered while walking the FK graph).
    pub source_table: String,

    /// Destination table name (after any `map { }` rename).
    pub dest_table: String,

    /// Rows to migrate for this table.
    pub row_count: RowCount,

    /// Number of columns carried to the destination.
    pub columns: usize,

    /// Primary key columns.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub primary_key: Vec<String>,

    /// Whether the destination table already exists (false = it will be created).
    pub dest_exists: bool,

    /// Column mappings for this table (drives the per-card conversion lines).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub mappings: Vec<ColumnMapping>,

    /// Transformed sample rows for this table (when `--sample` is set).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sample: Option<SampleDataPreview>,
}
