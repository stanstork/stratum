use crate::{
    add_joins, add_where, ident, join_on_expr, metadata::table::TableMetadata,
    query::column::ColumnDef, requests::FetchRowsRequest, sql_filter_expr,
};
use common::{row_data::RowData, value::Value};
use query_builder::{
    ast::expr::Expr,
    build::{alter_table::AlterTableBuilder, insert::InsertBuilder, select::SelectBuilder},
    dialect::Dialect,
    render::{Render, Renderer},
    table_ref, value,
};
use std::collections::HashMap;

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
            .map(|c| ident!(c))
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

    pub fn insert_batch(&self, meta: &TableMetadata, rows: Vec<RowData>) -> (String, Vec<Value>) {
        if rows.is_empty() {
            return (String::new(), Vec::new());
        }

        // Sorting ensures the column order is always consistent
        let mut sorted_columns: Vec<_> = meta.columns.values().collect();
        sorted_columns.sort_by_key(|col| col.ordinal);
        let col_names = sorted_columns
            .iter()
            .map(|col| col.name.clone())
            .collect::<Vec<_>>();

        let mut builder = InsertBuilder::new(table_ref!(meta.name))
            .columns(&col_names.iter().map(|s| s.as_str()).collect::<Vec<_>>());

        for row in rows {
            // Create a HashMap for efficient, case-insensitive lookup of values by column name
            let field_map: HashMap<String, Value> = row
                .field_values
                .into_iter()
                .filter_map(|rc| rc.value.map(|v| (rc.name.to_lowercase(), v)))
                .collect();

            // Map the ordered column names to their corresponding values for the current row
            let ordered_values: Vec<Expr> = col_names
                .iter()
                .map(|col_name| {
                    let value = field_map
                        .get(&col_name.to_lowercase())
                        .cloned()
                        .unwrap_or(Value::Null); // Use NULL if a value isn't present for a column.
                    Expr::Value(value)
                })
                .collect();

            // Add the row of values to the builder.
            builder = builder.values(ordered_values);
        }

        let insert_ast = builder.build();

        let mut renderer = Renderer::new(self.dialect);
        insert_ast.render(&mut renderer);
        renderer.finish()
    }

    pub fn toggle_triggers(&self, table: &str, enable: bool) -> (String, Vec<Value>) {
        let builder = AlterTableBuilder::new(table_ref!(table)).toggle_triggers(!enable);
        let query_ast = builder.build();

        let mut renderer = Renderer::new(self.dialect);
        query_ast.render(&mut renderer);
        renderer.finish()
    }

    pub fn add_column(&self, table: &str, column: ColumnDef) -> (String, Vec<Value>) {
        let builder = AlterTableBuilder::new(table_ref!(table));
        let query_ast = builder
            .add_column(&column.name, column.data_type)
            .add()
            .build();

        let mut renderer = Renderer::new(self.dialect);
        query_ast.render(&mut renderer);
        renderer.finish()
    }
}
