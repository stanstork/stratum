use crate::sql::metadata::index::{IndexType, NullsOrder, SortOrder};

/// Definition for creating an index, used by the query generator.
/// All values are pre-resolved for the target dialect by the planner/TypeRegistry.
#[derive(Debug, Clone)]
pub struct IndexDef {
    pub name: String,
    pub table: String,
    pub columns: Vec<IndexColumnDef>,
    pub unique: bool,
    /// Index type, already converted to the target dialect by TypeRegistry.
    pub index_type: Option<IndexType>,
    pub condition: Option<String>,
}

/// A single column in an index definition.
#[derive(Debug, Clone)]
pub struct IndexColumnDef {
    pub name: String,
    pub sort_order: SortOrder,
    pub nulls_order: NullsOrder,
}
