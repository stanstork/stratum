use crate::traits::row_decoder::RowDecoder;
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct ForeignKeyMetadata {
    pub constraint_name: String,
    pub table: String,
    pub schema: String,
    pub columns: Vec<String>,
    pub referenced_table: String,
    pub referenced_schema: Option<String>,
    pub referenced_columns: Vec<String>,
    pub on_delete: ForeignKeyAction,
    pub on_update: ForeignKeyAction,
    pub nullable: bool,

    /// Deferrable constraint (POSTGRESQL only)
    pub deferrable: Option<bool>,

    /// Initially deferred (POSTGRESQL only)
    pub initially_deferred: Option<bool>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub enum ForeignKeyAction {
    NoAction,
    Restrict,
    Cascade,
    SetNull,
    SetDefault,
}

impl ForeignKeyAction {
    pub fn to_string(&self) -> Option<String> {
        match self {
            ForeignKeyAction::NoAction => None,
            ForeignKeyAction::Restrict => Some("RESTRICT".to_string()),
            ForeignKeyAction::Cascade => Some("CASCADE".to_string()),
            ForeignKeyAction::SetNull => Some("SET NULL".to_string()),
            ForeignKeyAction::SetDefault => Some("SET DEFAULT".to_string()),
        }
    }
}

// SQL string constants for FK actions
const FK_ACTION_CASCADE: &str = "CASCADE";
const FK_ACTION_SET_NULL: &str = "SET NULL";
const FK_ACTION_SET_DEFAULT: &str = "SET DEFAULT";
const FK_ACTION_RESTRICT: &str = "RESTRICT";
const FK_ACTION_NO_ACTION: &str = "NO ACTION";

// SQL column name constants
const CONSTRAINT_NAME_COL: &str = "constraint_name";
const TABLE_NAME_COL: &str = "table_name";
const TABLE_SCHEMA_COL: &str = "schema_name";
const COLUMN_NAMES_COL: &str = "columns";
const REFERENCED_TABLE_NAME_COL: &str = "referenced_table";
const REFERENCED_TABLE_SCHEMA_COL: &str = "referenced_schema";
const REFERENCED_COLUMN_NAMES_COL: &str = "referenced_columns";
const ON_DELETE_COL: &str = "on_delete";
const ON_UPDATE_COL: &str = "on_update";
const IS_NULLABLE_COL: &str = "is_nullable";
const IS_DEFERRABLE_COL: &str = "is_deferrable";
const INITIALLY_DEFERRED_COL: &str = "initially_deferred";

impl ForeignKeyMetadata {
    /// Parse ForeignKeyAction from SQL string
    pub fn parse_action(action: Option<&str>) -> ForeignKeyAction {
        match action.map(|s| s.to_uppercase()).as_deref() {
            Some(FK_ACTION_CASCADE) => ForeignKeyAction::Cascade,
            Some(FK_ACTION_SET_NULL) => ForeignKeyAction::SetNull,
            Some(FK_ACTION_SET_DEFAULT) => ForeignKeyAction::SetDefault,
            Some(FK_ACTION_RESTRICT) => ForeignKeyAction::Restrict,
            Some(FK_ACTION_NO_ACTION) | None => ForeignKeyAction::NoAction,
            _ => ForeignKeyAction::NoAction,
        }
    }

    pub fn from_row<R: RowDecoder>(row: &R) -> Self {
        Self {
            constraint_name: row.get_string(CONSTRAINT_NAME_COL).unwrap_or_default(),
            table: row.get_string(TABLE_NAME_COL).unwrap_or_default(),
            schema: row.get_string(TABLE_SCHEMA_COL).unwrap_or_default(),
            columns: row
                .get_string(COLUMN_NAMES_COL)
                .map(|cols| cols.split(',').map(|s| s.trim().to_string()).collect())
                .unwrap_or_default(),
            referenced_table: row
                .get_string(REFERENCED_TABLE_NAME_COL)
                .unwrap_or_default(),
            referenced_schema: row.get_string(REFERENCED_TABLE_SCHEMA_COL),
            referenced_columns: row
                .get_string(REFERENCED_COLUMN_NAMES_COL)
                .map(|cols| cols.split(',').map(|s| s.trim().to_string()).collect())
                .unwrap_or_default(),
            on_delete: Self::parse_action(row.get_string(ON_DELETE_COL).as_deref()),
            on_update: Self::parse_action(row.get_string(ON_UPDATE_COL).as_deref()),
            nullable: row
                .get_string(IS_NULLABLE_COL)
                .map(|s| s.eq_ignore_ascii_case("YES"))
                .unwrap_or(false),
            deferrable: row.get_bool(IS_DEFERRABLE_COL),
            initially_deferred: row.get_bool(INITIALLY_DEFERRED_COL),
        }
    }
}
