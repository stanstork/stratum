use common::value::Value;
use query_builder::{
    build::select::SelectBuilder,
    dialect::Dialect,
    render::{Render, Renderer},
    table_ref, value,
};

use crate::{
    add_joins, add_where, ident, join_on_expr, requests::FetchRowsRequest, sql_filter_expr,
};

pub struct QueryGenerator<'a> {
    dialect: &'a dyn Dialect,
}

impl<'a> QueryGenerator<'a> {
    pub fn new(dialect: &'a dyn Dialect) -> Self {
        Self { dialect }
    }

    /// Generates a SQL SELECT statement and its parameters.
    pub fn select(&self, request: &FetchRowsRequest) -> (String, Vec<Value>) {
        let alias = request.alias.as_deref().unwrap_or(&request.table);
        let table = table_ref!(&request.table);

        let columns = request
            .columns
            .iter()
            .map(|c| ident!(c)) // Assumes `ident!` macro exists
            .collect::<Vec<_>>();

        // Start building the query
        let mut select = SelectBuilder::new()
            .select(columns)
            .from(table, Some(alias));

        // Apply joins and where clause
        select = add_joins!(select, &request.joins);
        select = add_where!(select, &request.filter);

        // Build the final AST
        let select_ast = select
            .limit(value!(Value::Int(request.limit as i64)))
            .offset(value!(Value::Int(request.offset.unwrap_or(0) as i64)))
            .build();

        let mut renderer = Renderer::new(self.dialect);
        select_ast.render(&mut renderer);
        renderer.finish()
    }
}
