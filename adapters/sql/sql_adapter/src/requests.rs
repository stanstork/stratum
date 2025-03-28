use crate::query::select::SelectField;

#[derive(Debug, Clone)]
pub struct FetchRowsRequest {
    pub table: String,
    pub alias: Option<String>,
    pub columns: Vec<SelectField>,
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

impl FetchRowsRequest {
    pub fn new(
        table: String,
        alias: Option<String>,
        columns: Vec<SelectField>,
        joins: Vec<JoinClause>,
        limit: usize,
        offset: Option<usize>,
    ) -> Self {
        FetchRowsRequest {
            table,
            alias,
            columns,
            joins,
            limit,
            offset,
        }
    }
}
