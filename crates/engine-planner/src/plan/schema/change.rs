use crate::plan::schema::types::SchemaChangeType;
use serde::Serialize;

#[derive(Serialize, Debug, Clone)]
pub struct SchemaChange {
    pub change_type: SchemaChangeType,

    /// Entity affected (table name, column name, etc.)
    pub entity: String,
    pub description: String,

    /// SQL DDL statement to execute (if available)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ddl: Option<String>,

    /// Whether this change could break existing applications
    pub is_breaking: bool,

    /// Whether this change can be rolled back
    pub is_reversible: bool,
}
