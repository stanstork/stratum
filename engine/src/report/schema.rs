use crate::report::finding::Finding;
use serde::Serialize;

/// A report on proposed schema changes and detected issues.
#[derive(Serialize, Debug, Default, Clone)]
pub struct SchemaReview {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub source_findings: Vec<Finding>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub destination_findings: Vec<Finding>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub actions: Vec<SchemaAction>,
}

/// A report on schema validation against transformed data.
#[derive(Serialize, Debug, Default, Clone)]
pub struct SchemaValidationReport {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub findings: Vec<Finding>,
}

/// An actionable change to be made to the destination schema.
#[derive(Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SchemaAction {
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entity: Option<String>,
}

const ACTION_ADD_COLUMN: &str = "ACTION_ADD_COLUMN";
const ACTION_CREATE_ENUM: &str = "ACTION_CREATE_ENUM";
const ACTION_CREATE_TABLE: &str = "ACTION_CREATE_TABLE";
const ACTION_ADD_FOREIGN_KEY: &str = "ACTION_ADD_FOREIGN_KEY";

impl SchemaAction {
    fn new(code: &str, message: String, entity: Option<String>) -> Self {
        Self {
            code: code.to_string(),
            message,
            entity,
        }
    }

    pub fn add_column(entity: &str, column: &str) -> Self {
        Self::new(
            ACTION_ADD_COLUMN,
            format!(
                "A new column '{column}' will be added to the destination table '{entity}'."
            ),
            Some(format!("{entity}.{column}")),
        )
    }

    pub fn create_enum(enum_name: &str) -> Self {
        Self::new(
            ACTION_CREATE_ENUM,
            format!(
                "A new enum type '{enum_name}' will be created in the destination."
            ),
            Some(enum_name.to_string()),
        )
    }

    pub fn create_table(table: &str) -> Self {
        Self::new(
            ACTION_CREATE_TABLE,
            format!("A new table '{table}' will be created."),
            Some(table.to_string()),
        )
    }

    pub fn add_foreign_key(fk_name: &str) -> Self {
        Self::new(
            ACTION_ADD_FOREIGN_KEY,
            format!(
                "A foreign key constraint '{fk_name}' will be added to the destination."
            ),
            Some(fk_name.to_string()),
        )
    }
}
