use crate::query::builder::SelectColumn;

#[derive(Debug, Clone)]
pub struct FetchRowsRequest {
    pub table: String,
    pub alias: String,
    pub columns: Vec<SelectColumn>,
    pub joins: Vec<JoinClause>,
    pub limit: usize,
    pub offset: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct JoinClause {
    pub table: String,
    pub alias: String,
    pub join_type: String,  // e.g., "LEFT", "INNER"
    pub from_alias: String, // Alias of the table to join from
    pub from_col: String,   // Column in the current table
    pub to_col: String,     // Column in the joined table
}
