use crate::{join::clause::JoinClause, query::select::SelectField};

#[derive(Debug, Clone)]
pub struct FetchRowsRequest {
    pub source_table: String,
    pub target_table: Option<String>,
    pub alias: Option<String>,
    pub columns: Vec<SelectField>,
    pub joins: Vec<JoinClause>,
    pub limit: usize,
    pub offset: Option<usize>,
}

impl FetchRowsRequest {
    pub fn new(
        source_table: String,
        target_table: Option<String>,
        alias: Option<String>,
        columns: Vec<SelectField>,
        joins: Vec<JoinClause>,
        limit: usize,
        offset: Option<usize>,
    ) -> Self {
        FetchRowsRequest {
            source_table,
            target_table,
            alias,
            columns,
            joins,
            limit,
            offset,
        }
    }
}
