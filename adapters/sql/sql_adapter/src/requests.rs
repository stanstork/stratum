use crate::{join::clause::JoinClause, query::select::SelectField};

#[derive(Debug, Clone)]
pub struct FetchRowsRequest {
    pub table: String,
    pub alias: Option<String>,
    pub columns: Vec<SelectField>,
    pub joins: Vec<JoinClause>,
    pub limit: usize,
    pub offset: Option<usize>,
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
