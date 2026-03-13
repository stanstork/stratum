//! Provides a fluent builder for constructing `CreateTable` ASTs.

use crate::ast::{
    common::TableRef,
    create_table::{ColumnDef, CreateTable, TableConstraint},
    expr::Expr,
};
use model::core::types::Type;

#[derive(Debug, Clone)]
pub struct CreateTableBuilder {
    ast: CreateTable,
}

impl CreateTableBuilder {
    pub fn new(table: TableRef) -> Self {
        Self {
            ast: CreateTable {
                table,
                ..Default::default()
            },
        }
    }

    pub fn if_not_exists(mut self) -> Self {
        self.ast.if_not_exists = true;
        self
    }

    pub fn temporary(mut self) -> Self {
        self.ast.temp = true;
        self
    }

    pub fn column(self, name: &str, data_type: Type, max_length: Option<usize>) -> ColumnBuilder {
        ColumnBuilder::new(self, name, data_type, max_length)
    }

    pub fn primary_key(mut self, columns: Vec<String>) -> Self {
        self.ast.constraints.push(TableConstraint::PrimaryKey {
            columns: columns.to_vec(),
        });
        self
    }

    pub fn build(self) -> CreateTable {
        self.ast
    }
}

pub struct ColumnBuilder {
    table_builder: CreateTableBuilder,
    column: ColumnDef,
}

impl ColumnBuilder {
    pub fn new(
        table_builder: CreateTableBuilder,
        name: &str,
        data_type: Type,
        max_length: Option<usize>,
    ) -> Self {
        Self {
            table_builder,
            column: ColumnDef {
                name: name.to_string(),
                data_type,
                is_nullable: false, // Columns are NOT NULL by default
                is_primary_key: false,
                default_value: None,
                max_length,
                generated_expression: None,
                is_stored: false,
            },
        }
    }

    pub fn nullable(mut self) -> Self {
        self.column.is_nullable = true;
        self
    }

    pub fn primary_key(mut self) -> Self {
        self.column.is_primary_key = true;
        self
    }

    pub fn default_value(mut self, default_value: Expr) -> Self {
        self.column.default_value = Some(default_value);
        self
    }

    pub fn generated(mut self, expression: &str, is_stored: bool) -> Self {
        self.column.generated_expression = Some(expression.to_string());
        self.column.is_stored = is_stored;
        self
    }

    pub fn add(mut self) -> CreateTableBuilder {
        self.table_builder.ast.columns.push(self.column);
        self.table_builder
    }
}

#[cfg(test)]
mod tests {
    use model::core::{
        types::{IntSize, Type},
        value::Value,
    };

    use crate::{
        ast::{common::TableRef, expr::Expr},
        builder::create_table::CreateTableBuilder,
    };

    fn table(name: &str) -> TableRef {
        TableRef {
            schema: None,
            name: name.to_string(),
        }
    }

    #[test]
    fn test_build_create_table() {
        let builder = CreateTableBuilder::new(table("users"));

        let ast = builder
            .if_not_exists()
            .column(
                "id",
                Type::Int {
                    bits: IntSize::I32,
                    unsigned: true,
                    auto_increment: false,
                },
                None,
            )
            .primary_key()
            .add()
            .column(
                "username",
                Type::Varchar {
                    length: Some(255),
                    charset: None,
                },
                Some(255),
            )
            .add()
            .column("is_active", Type::Boolean, None)
            .default_value(Expr::Value(Value::Boolean(true)))
            .add()
            .build();

        assert!(ast.if_not_exists);
        assert_eq!(ast.table.name, "users");
        assert_eq!(ast.columns.len(), 3);
        assert!(ast.columns[0].is_primary_key);
        assert_eq!(
            ast.columns[1].data_type,
            Type::Varchar {
                length: Some(255),
                charset: None,
            }
        );
        assert!(!ast.columns[2].is_nullable); // Should be NOT NULL by default
        assert!(ast.columns[2].default_value.is_some());
    }
}
