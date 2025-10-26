use crate::{filter::SqlFilter, join::clause::JoinClause, query::select::SelectField};
use query_builder::offsets::{Cursor, OffsetStrategy, PkOffset};

pub struct FetchRowsRequest {
    pub table: String,
    pub alias: Option<String>,
    pub columns: Vec<SelectField>,
    pub joins: Vec<JoinClause>,
    pub filter: Option<SqlFilter>,
    pub limit: usize,
    pub cursor: Cursor,
    pub start: Box<dyn OffsetStrategy>,
}

pub struct FetchRowsRequestBuilder {
    table: String,
    alias: Option<String>,
    columns: Vec<SelectField>,
    joins: Vec<JoinClause>,
    filter: Option<SqlFilter>,
    limit: usize,
    cursor: Cursor,
    start: Box<dyn OffsetStrategy>,
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
            cursor: Cursor::None,
            start: Box::new(PkOffset { pk: "".to_string() }),
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

    pub fn cursor(mut self, cursor: Cursor) -> Self {
        self.cursor = cursor;
        self
    }

    pub fn start(mut self, start: &dyn OffsetStrategy) -> Self {
        self.start = start.clone_box();
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
            cursor: self.cursor,
            start: self.start,
        }
    }
}
