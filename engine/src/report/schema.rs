use crate::report::finding::Finding;
use serde::Serialize;

const ACTION_ADD_COLUMN: &str = "ACTION_ADD_COLUMN";
const ACTION_CREATE_ENUM: &str = "ACTION_CREATE_ENUM";
const ACTION_CREATE_TABLE: &str = "ACTION_CREATE_TABLE";
const ACTION_ADD_FOREIGN_KEY: &str = "ACTION_ADD_FOREIGN_KEY";

#[derive(Serialize, Debug, Clone)]
pub struct SchemaAction {
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entity: Option<String>,
}

#[derive(Serialize, Debug, Default, Clone)]
pub struct SchemaReview {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub source_findings: Vec<Finding>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub destination_findings: Vec<Finding>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub actions: Vec<SchemaAction>, // actionable changes (e.g., "ADD COLUMN")
}

#[derive(Serialize, Debug, Default, Clone)]
pub struct SchemaValidationReport {
    /// Findings related to schema mismatches between transformed data and the destination.
    pub findings: Vec<Finding>,
}

impl SchemaAction {
    pub fn add_column(entity: &str, column: &str) -> Self {
        SchemaAction {
            code: ACTION_ADD_COLUMN.to_string(),
            message: format!(
                "A new column '{}' will be added to the destination table '{}'.",
                column, entity
            ),
            entity: Some(format!("{}.{}", entity, column)),
        }
    }

    pub fn create_enum(enum_name: &str) -> Self {
        SchemaAction {
            code: ACTION_CREATE_ENUM.to_string(),
            message: format!(
                "A new enum type '{}' will be created in the destination.",
                enum_name
            ),
            entity: Some(enum_name.to_string()),
        }
    }

    pub fn create_table(table: &str) -> Self {
        SchemaAction {
            code: ACTION_CREATE_TABLE.to_string(),
            message: format!("A new table '{}' will be created.", table),
            entity: Some(table.to_string()),
        }
    }

    pub fn add_foreign_key(fk_name: &str) -> Self {
        SchemaAction {
            code: ACTION_ADD_FOREIGN_KEY.to_string(),
            message: format!(
                "A foreign key constraint '{}' will be added to the destination.",
                fk_name
            ),
            entity: Some(fk_name.to_string()),
        }
    }
}
