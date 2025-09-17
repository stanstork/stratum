use crate::{
    add_joins, add_where, ident, join_on_expr,
    metadata::{column::ColumnMetadata, table::TableMetadata},
    query::{column::ColumnDef, fk::ForeignKeyDef},
    requests::FetchRowsRequest,
    sql_filter_expr,
};
use common::{row_data::RowData, types::DataType, value::Value};
use query_builder::{
    ast::{common::TypeName, expr::Expr},
    build::{
        alter_table::AlterTableBuilder, create_enum::CreateEnumBuilder,
        create_table::CreateTableBuilder, insert::InsertBuilder, select::SelectBuilder,
    },
    dialect::{self, Dialect},
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

        self.render_ast(select_ast)
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

        // println!("Rows: {:?}", rows);
        // println!("Sorted Columns: {:?}", sorted_columns);

        for row in rows {
            // Create a HashMap for efficient, case-insensitive lookup of values by column name
            let field_map: HashMap<String, Value> = row
                .field_values
                .into_iter()
                .filter_map(|rc| rc.value.map(|v| (rc.name.to_lowercase(), v)))
                .collect();

            // Map the ordered column names to their corresponding values for the current row
            let ordered_values: Vec<Expr> = sorted_columns
                .iter()
                .map(|col_meta| {
                    let value = field_map
                        .get(&col_meta.name.to_lowercase())
                        .cloned()
                        .unwrap_or(Value::Null);
                    map_value_to_expr(value, col_meta)
                })
                .collect();

            // Add the row of values to the builder.
            builder = builder.values(ordered_values);
        }

        let insert_ast = builder.build();

        self.render_ast(insert_ast)
    }

    pub fn toggle_triggers(&self, table: &str, enable: bool) -> (String, Vec<Value>) {
        let builder = AlterTableBuilder::new(table_ref!(table)).toggle_triggers(!enable);
        let query_ast = builder.build();

        self.render_ast(query_ast)
    }

    pub fn add_column(&self, table: &str, column: ColumnDef) -> (String, Vec<Value>) {
        let builder = AlterTableBuilder::new(table_ref!(table));
        let query_ast = builder
            .add_column(&column.name, column.data_type)
            .add()
            .build();

        self.render_ast(query_ast)
    }

    pub fn create_table(
        &self,
        table: &str,
        columns: &[ColumnDef],
        ignore_constraints: bool,
    ) -> (String, Vec<Value>) {
        // Find all primary key columns upfront
        let primary_keys: Vec<String> = if ignore_constraints {
            vec![]
        } else {
            columns
                .iter()
                .filter(|c| c.is_primary_key)
                .map(|c| c.name.clone())
                .collect()
        };

        let builder_with_cols = columns.iter().fold(
            CreateTableBuilder::new(table_ref!(table)),
            |builder, col| {
                let mut col_builder =
                    builder.column(&col.name, col.data_type.clone(), col.char_max_length);

                // Only add PRIMARY KEY to the column definition if it's the *only* primary key.
                if primary_keys.len() == 1 && primary_keys[0] == col.name.as_str() {
                    col_builder = col_builder.primary_key();
                }
                if col.is_nullable {
                    col_builder = col_builder.nullable();
                }
                if let Some(default_val) = &col.default {
                    col_builder = col_builder.default_value(Expr::Value(default_val.clone()));
                }

                col_builder.add() // .add() returns the CreateTableBuilder for the next fold iteration
            },
        );

        // Add the composite primary key constraint at the table level if necessary
        let final_builder = if primary_keys.len() > 1 {
            builder_with_cols.primary_key(primary_keys)
        } else {
            builder_with_cols
        };

        let create_ast = final_builder.build();

        self.render_ast(create_ast)
    }

    pub fn add_foreign_key(
        &self,
        table: &str,
        foreign_key: &ForeignKeyDef,
    ) -> (String, Vec<Value>) {
        let query_ast = AlterTableBuilder::new(table_ref!(table))
            .add_foreign_key(
                &[&foreign_key.column],
                table_ref!(&foreign_key.referenced_table),
                &[&foreign_key.referenced_column],
            )
            .build();

        self.render_ast(query_ast)
    }

    pub fn create_enum(&self, name: &str, values: &Vec<String>) -> (String, Vec<Value>) {
        let builder = CreateEnumBuilder::new(
            TypeName {
                schema: None,
                name: name.to_string(),
            },
            values,
        );
        self.render_ast(builder.build())
    }

    pub fn key_existence(
        &self,
        table_name: &str,
        key_columns: &[String],
        keys_batch: usize,
    ) -> String {
        self.dialect
            .build_key_existence_query(table_name, key_columns, keys_batch)
    }

    fn render_ast(&self, ast: impl Render) -> (String, Vec<Value>) {
        let mut renderer = Renderer::new(self.dialect);
        ast.render(&mut renderer);
        renderer.finish()
    }
}

/// Maps a `Value` to a query `Expr` based on column metadata.
///
/// This function contains all the specific logic for handling different data types,
/// like casting enums or parsing string representations of arrays.
fn map_value_to_expr(value: Value, col_meta: &ColumnMetadata) -> Expr {
    // If the value is NULL, generate a CAST to ensure the database knows the correct type.
    // This avoids the "expression is of type ..." error for bytea and other columns.
    if let Value::Null = value {
        let dialect = dialect::Postgres; // Postgres is only supported for now

        return Expr::Cast {
            expr: Box::new(Expr::Literal("NULL".to_string())), // Generate the literal NULL keyword
            data_type: dialect.render_data_type(&col_meta.data_type, col_meta.char_max_length), // Render the SQL type name
        };
    }

    match col_meta.data_type {
        // For array types, check if the value is already an array or if it's a
        // string that needs to be parsed.
        DataType::Array => {
            let string_array = match value {
                Value::String(s) => s.split(',').map(|item| item.trim().to_string()).collect(),
                Value::StringArray(arr) => arr,
                _ => vec![], // Default to an empty array if the type is unexpected
            };
            Expr::Value(Value::StringArray(string_array))
        }

        // For custom types like enums, wrap the value in a CAST expression.
        DataType::Custom(_) => match value {
            Value::Enum(n, v) => Expr::Cast {
                expr: Box::new(Expr::Value(Value::String(v))),
                data_type: n, // Use the enum name as the SQL type
            },
            _ => Expr::Value(value),
        },

        // For all other standard data types, just use the value directly.
        _ => Expr::Value(value),
    }
}
