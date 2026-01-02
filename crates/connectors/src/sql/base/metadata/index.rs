use crate::sql::base::row::DbRow;
use serde::Serialize;

const COL_INDEX_NAME: &str = "index_name";
const COL_IS_UNIQUE: &str = "is_unique";
const COL_IS_PRIMARY: &str = "is_primary";
const COL_INDEX_COLUMNS: &str = "column_name";
const COL_INDEX_TYPE: &str = "index_type";
const COL_INDEX_CONDITION: &str = "index_condition";

#[derive(Debug, Clone, Serialize)]
pub struct IndexMetadata {
    pub name: String,
    pub is_unique: bool,
    pub is_primary: bool,

    /// Columns in the index (in order)
    pub columns: Vec<String>,

    /// Index type (btree, hash, gin, gist, etc.)
    pub index_type: String,

    /// Partial index condition (if any)
    pub condition: Option<String>,
}

impl IndexMetadata {
    pub fn from_row(row: &DbRow) -> Self {
        Self {
            name: row.try_get_string(COL_INDEX_NAME).unwrap_or_default(),
            is_unique: row.try_get_bool(COL_IS_UNIQUE).unwrap_or(false),
            is_primary: row.try_get_bool(COL_IS_PRIMARY).unwrap_or(false),
            columns: row
                .try_get_string(COL_INDEX_COLUMNS)
                .map(|cols| cols.split(',').map(|s| s.trim().to_string()).collect())
                .unwrap_or_default(),
            index_type: row.try_get_string(COL_INDEX_TYPE).unwrap_or_default(),
            condition: row.try_get_string(COL_INDEX_CONDITION),
        }
    }
}
