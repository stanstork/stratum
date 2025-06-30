use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct ForeignKeyMetadata {
    pub column: String,
    pub referenced_table: String,
    pub referenced_column: String,
    pub nullable: bool,
}
