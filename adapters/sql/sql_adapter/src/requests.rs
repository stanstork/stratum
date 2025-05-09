use crate::{filter::SqlFilter, join::clause::JoinClause, query::select::SelectField};

#[derive(Debug, Clone)]
pub struct FetchRowsRequest {
    pub table: String,
    pub alias: Option<String>,
    pub columns: Vec<SelectField>,
    pub joins: Vec<JoinClause>,
    pub filter: Option<SqlFilter>,
    pub limit: usize,
    pub offset: Option<usize>,
}

pub struct FetchRowsRequestBuilder {
    table: String,
    alias: Option<String>,
    columns: Vec<SelectField>,
    joins: Vec<JoinClause>,
    filter: Option<SqlFilter>,
    limit: usize,
    offset: Option<usize>,
}

impl FetchRowsRequestBuilder {
    pub fn new(table: String) -> Self {
        FetchRowsRequestBuilder {
            table,
            alias: None,
            columns: Vec::new(),
            joins: Vec::new(),
            filter: None,
            limit: 0,
            offset: None,
        }
    }

    pub fn alias(mut self, alias: String) -> Self {
        self.alias = Some(alias);
        self
    }

    pub fn columns(mut self, columns: Vec<SelectField>) -> Self {
        self.columns = columns;
        self
    }

    pub fn joins(mut self, joins: Vec<JoinClause>) -> Self {
        self.joins = joins;
        self
    }

    pub fn filter(mut self, filter: Option<SqlFilter>) -> Self {
        self.filter = filter;
        self
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }

    pub fn offset(mut self, offset: Option<usize>) -> Self {
        self.offset = offset;
        self
    }

    pub fn build(self) -> FetchRowsRequest {
        FetchRowsRequest {
            table: self.table,
            alias: self.alias,
            columns: self.columns,
            joins: self.joins,
            filter: self.filter,
            limit: self.limit,
            offset: self.offset,
        }
    }
}
