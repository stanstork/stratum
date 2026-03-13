use crate::sql::metadata::fk::ForeignKeyAction;

#[derive(Debug, Clone)]
pub struct ForeignKeyDef {
    pub constraint_name: Option<String>,
    pub columns: Vec<String>,
    pub referenced_table: String,
    pub referenced_columns: Vec<String>,
    pub on_delete: ForeignKeyAction,
    pub on_update: ForeignKeyAction,
}
