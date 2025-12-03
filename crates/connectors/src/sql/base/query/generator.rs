use crate::sql::base::{
    metadata::{column::ColumnMetadata, table::TableMetadata},
    query::{coercion::coerce_value, column::ColumnDef, fk::ForeignKeyDef},
    requests::FetchRowsRequest,
};
use crate::{add_joins, add_where, ident, join_on_expr, sql_filter_expr};
use model::{
    core::{data_type::DataType, value::Value},
    records::row::RowData,
};
use planner::query::{
    ast::{
        common::TypeName,
        copy::{CopyDirection, CopyEndpoint},
        expr::{BinaryOp, BinaryOperator, Expr, Ident},
        insert::{ConflictAction, ConflictAssignment, Insert, OnConflict},
        merge::MergeAssignment,
    },
    builder::{
        alter_table::AlterTableBuilder, copy::CopyBuilder, create_enum::CreateEnumBuilder,
        create_table::CreateTableBuilder, drop_table::DropTableBuilder, insert::InsertBuilder,
        merge::MergeBuilder, select::SelectBuilder,
    },
    dialect::{self, Dialect},
    renderer::{Render, Renderer},
};
use planner::{table_ref, value};
use std::collections::{HashMap, HashSet};

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
            .paginate(request.strategy.clone(), &request.cursor, request.limit)
            .build();

        self.render_ast(select_ast)
    }

    pub fn insert_batch(&self, meta: &TableMetadata, rows: &[RowData]) -> (String, Vec<Value>) {
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

        for row in rows.iter() {
            // Create a HashMap for efficient, case-insensitive lookup of values by column name
            let field_map: HashMap<String, Value> = row
                .field_values
                .clone()
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

            builder = builder.values(ordered_values);
        }

        self.render_ast(builder.build())
    }

    pub fn copy_from_stdin(&self, table: &str, columns: &[ColumnMetadata]) -> String {
        let column_names = columns
            .iter()
            .map(|col| col.name.as_str())
            .collect::<Vec<_>>();

        let copy_ast = CopyBuilder::new(table_ref!(table))
            .columns(&column_names)
            .direction(CopyDirection::From)
            .endpoint(CopyEndpoint::Stdin)
            .option("FORMAT", Some("csv, NULL '\\N'"))
            .build();

        let (sql, _) = self.render_ast(copy_ast);
        sql
    }

    pub fn merge_from_staging(
        &self,
        meta: &TableMetadata,
        staging: &str,
        columns: &[ColumnMetadata],
    ) -> (String, Vec<Value>) {
        let target_ref = table_ref!(meta.name);
        let staging_ref = table_ref!(staging);
        let target_alias = "t";
        let staging_alias = "s";

        let pk_set = primary_key_set(meta);

        let mut builder = MergeBuilder::new(target_ref, staging_ref)
            .target_alias(target_alias)
            .source_alias(staging_alias)
            .on(build_pk_match_expr(meta, target_alias, staging_alias));

        let assignments: Vec<MergeAssignment> = columns
            .iter()
            .filter(|col| !pk_set.contains(&col.name.to_lowercase()))
            .map(|col| MergeAssignment {
                column: col.name.clone(),
                value: aliased_ident(staging_alias, &col.name),
            })
            .collect();

        builder = if assignments.is_empty() {
            builder.when_matched_do_nothing()
        } else {
            builder.when_matched_update(assignments)
        };

        let insert_columns = columns.iter().map(|c| c.name.clone()).collect();
        let insert_values = columns
            .iter()
            .map(|c| aliased_ident(staging_alias, &c.name))
            .collect();

        builder = builder.when_not_matched_insert(insert_columns, insert_values);

        self.render_ast(builder.build())
    }

    pub fn upsert_from_staging(
        &self,
        meta: &TableMetadata,
        staging_table: &str,
        columns: &[ColumnMetadata],
    ) -> (String, Vec<Value>) {
        let pk_set = primary_key_set(meta);
        let staging_alias = "s";
        let select_columns = columns
            .iter()
            .map(|col| aliased_ident(staging_alias, &col.name))
            .collect();

        let select_ast = SelectBuilder::new()
            .select(select_columns)
            .from(table_ref!(staging_table), Some(staging_alias))
            .build();

        let conflict_clause = if meta.primary_keys.is_empty() {
            None
        } else {
            Some(OnConflict {
                columns: meta.primary_keys.clone(),
                action: self.build_conflict_action(columns, &pk_set),
            })
        };

        let insert_ast = Insert {
            table: table_ref!(meta.name),
            columns: columns.iter().map(|c| c.name.clone()).collect(),
            values: vec![],
            select: Some(select_ast),
            on_conflict: conflict_clause,
        };

        self.render_ast(insert_ast)
    }

    pub fn toggle_triggers(&self, table: &str, enable: bool) -> (String, Vec<Value>) {
        let builder = AlterTableBuilder::new(table_ref!(table)).toggle_triggers(enable);
        self.render_ast(builder.build())
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
        temp: bool,
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

        let initial_builder = if temp {
            CreateTableBuilder::new(table_ref!(table)).temporary()
        } else {
            CreateTableBuilder::new(table_ref!(table))
        };

        let builder_with_cols = columns.iter().fold(initial_builder, |builder, col| {
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
        });

        // Add the composite primary key constraint at the table level if necessary
        let final_builder = if primary_keys.len() > 1 {
            builder_with_cols.primary_key(primary_keys)
        } else {
            builder_with_cols
        };

        self.render_ast(final_builder.build())
    }

    pub fn drop_table(&self, table: &str, if_exists: bool) -> (String, Vec<Value>) {
        let mut builder = DropTableBuilder::new(table_ref!(table));
        if if_exists {
            builder = builder.if_exists();
        }
        self.render_ast(builder.build())
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

    pub fn create_enum(&self, name: &str, values: &[String]) -> (String, Vec<Value>) {
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

    fn build_conflict_action(
        &self,
        columns: &[ColumnMetadata],
        pk_set: &HashSet<String>,
    ) -> ConflictAction {
        let assignments: Vec<ConflictAssignment> = columns
            .iter()
            .filter(|col| !pk_set.contains(&col.name.to_lowercase()))
            .map(|col| ConflictAssignment {
                column: col.name.clone(),
                value: self.excluded_column_expr(&col.name),
            })
            .collect();

        if assignments.is_empty() {
            ConflictAction::DoNothing
        } else {
            ConflictAction::DoUpdate { assignments }
        }
    }

    fn excluded_column_expr(&self, column: &str) -> Expr {
        Expr::Literal(format!(
            "EXCLUDED.{}",
            self.dialect.quote_identifier(column)
        ))
    }
}

// TODO: Split functionality by dialect
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

    let coerced_value = coerce_value(value, col_meta);

    match col_meta.data_type {
        // For array types, check if the value is already an array or if it's a
        // string that needs to be parsed.
        DataType::Array(_) | DataType::Set => {
            let string_array = match coerced_value {
                Value::String(s) => s.split(',').map(|item| item.trim().to_string()).collect(),
                Value::StringArray(arr) => arr,
                _ => vec![], // Default to an empty array if the type is unexpected
            };
            Expr::Value(Value::StringArray(string_array))
        }

        // For custom types or enums, wrap the value in a CAST expression to ensure the parameter
        // is typed correctly (e.g., `$1::rating`).
        DataType::Custom(_) | DataType::Enum => {
            let type_name = match col_meta.data_type {
                DataType::Custom(ref name) => name.clone(),
                DataType::Enum => col_meta.name.clone(),
                _ => unreachable!(),
            };

            let base_expr = match coerced_value {
                Value::Enum(_, v) => Expr::Value(Value::String(v)),
                Value::String(s) => Expr::Value(Value::String(s)),
                other => Expr::Value(other),
            };

            Expr::Cast {
                data_type: type_name,
                expr: Box::new(Expr::Cast {
                    expr: Box::new(base_expr),
                    data_type: "TEXT".into(),
                }),
            }
        }

        // For all other standard data types, just use the value directly.
        _ => Expr::Value(coerced_value),
    }
}

fn primary_key_set(meta: &TableMetadata) -> HashSet<String> {
    meta.primary_keys
        .iter()
        .map(|pk| pk.to_lowercase())
        .collect()
}

fn aliased_ident(alias: &str, column: &str) -> Expr {
    Expr::Identifier(Ident {
        qualifier: Some(alias.to_string()),
        name: column.to_string(),
    })
}

fn build_pk_match_expr(meta: &TableMetadata, target_alias: &str, source_alias: &str) -> Expr {
    let mut pk_iter = meta.primary_keys.iter().map(|pk| {
        Expr::BinaryOp(Box::new(BinaryOp {
            left: aliased_ident(target_alias, pk),
            op: BinaryOperator::Eq,
            right: aliased_ident(source_alias, pk),
        }))
    });

    let first = match pk_iter.next() {
        Some(expr) => expr,
        None => Expr::Literal("TRUE".to_string()),
    };

    pk_iter.fold(first, |acc, expr| {
        Expr::BinaryOp(Box::new(BinaryOp {
            left: acc,
            op: BinaryOperator::And,
            right: expr,
        }))
    })
}
