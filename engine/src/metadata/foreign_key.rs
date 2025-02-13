#[derive(Debug)]
pub struct ForeignKeyMetadata {
    pub column: String,
    pub foreign_table: String,
    pub foreign_column: String,
}
