use crate::traits::row_decoder::RowDecoder;
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct UniqueConstraintMetadata {
    pub constraint_name: String,
    pub table: String,
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CheckConstraintMetadata {
    pub constraint_name: String,
    pub table: String,
    pub definition: String,
}

const CONSTRAINT_NAME_COL: &str = "constraint_name";
const TABLE_NAME_COL: &str = "table_name";
const COLUMNS_COL: &str = "columns";
const DEFINITION_COL: &str = "definition";

impl UniqueConstraintMetadata {
    pub fn from_row<R: RowDecoder>(row: &R) -> Self {
        Self {
            constraint_name: row.get_string(CONSTRAINT_NAME_COL).unwrap_or_default(),
            table: row.get_string(TABLE_NAME_COL).unwrap_or_default(),
            columns: row
                .get_string(COLUMNS_COL)
                .map(|cols| cols.split(',').map(|s| s.trim().to_string()).collect())
                .unwrap_or_default(),
        }
    }
}

impl CheckConstraintMetadata {
    pub fn from_row<R: RowDecoder>(row: &R) -> Self {
        let raw_def = row.get_string(DEFINITION_COL).unwrap_or_default();
        // pg_get_constraintdef returns "CHECK (expr)" - strip the wrapper
        let definition = Self::strip_check_wrapper(&raw_def);

        Self {
            constraint_name: row.get_string(CONSTRAINT_NAME_COL).unwrap_or_default(),
            table: row.get_string(TABLE_NAME_COL).unwrap_or_default(),
            definition,
        }
    }

    /// Strip the "CHECK (...)" wrapper from PostgreSQL's pg_get_constraintdef output.
    /// If the input doesn't match the pattern, returns the raw string.
    fn strip_check_wrapper(raw: &str) -> String {
        let trimmed = raw.trim();
        if let Some(inner) = trimmed.strip_prefix("CHECK (")
            && let Some(expr) = inner.strip_suffix(')')
        {
            return expr.to_string();
        }
        trimmed.to_string()
    }
}
