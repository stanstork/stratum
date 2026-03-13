use serde::{Deserialize, Serialize};

/// Configuration for graph-based schema and data migration.
/// Populated from the `with references { ... }` block in SMQL.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphReferences {
    pub data_mode: DataMode,
    pub depth: TraversalDepth,
    pub exclude: Vec<String>,
    /// When true, drop existing FK constraints before cascade data migration
    /// and re-add them afterwards.  Useful when a prior `schema_only` run
    /// already created the constraints and you now want to run a `cascade`
    /// migration into the existing tables.
    #[serde(default)]
    pub drop_constraints: bool,
}

/// Controls whether related tables have their data migrated alongside schema.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
pub enum DataMode {
    /// Only migrate schema (DDL) for related tables; data is not cascaded.
    #[default]
    SchemaOnly,
    /// Cascade data from related tables via the existing DbSourceReader infrastructure.
    Cascade,
}

/// Controls how deep FK traversal goes from the root table.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
pub enum TraversalDepth {
    /// Traverse all FK dependencies recursively.
    #[default]
    All,
    /// Stop after N levels of FK traversal.
    Limited(usize),
}
