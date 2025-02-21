#[derive(Debug, Clone)]
pub struct ForeignKeyMetadata {
    pub column: String,
    pub referenced_table: String,
    pub referenced_column: String,
}
