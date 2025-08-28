//! Provides a fluent builder for constructing `CreateTable` ASTs.

use crate::ast::{
    common::TableRef,
    create_table::{ColumnDef, CreateTable, DataType, TableConstraint},
    expr::Expr,
};

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

    pub fn column(self, name: &str, data_type: DataType) -> ColumnBuilder {
        ColumnBuilder::new(self, name, data_type)
    }

    pub fn primary_key(mut self, columns: Vec<String>) -> Self {
        self.ast.constraints.push(TableConstraint::PrimaryKey {
            columns: columns.iter().map(|s| s.to_string()).collect(),
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
    pub fn new(table_builder: CreateTableBuilder, name: &str, data_type: DataType) -> Self {
        Self {
            table_builder,
            column: ColumnDef {
                name: name.to_string(),
                data_type,
                is_nullable: false, // Columns are NOT NULL by default
                is_primary_key: false,
                default_value: None,
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

    pub fn add(mut self) -> CreateTableBuilder {
        self.table_builder.ast.columns.push(self.column);
        self.table_builder
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ast::{common::TableRef, create_table::DataType, expr::Expr},
        build::create_table::CreateTableBuilder,
    };
    use common::value::Value::Boolean;

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
            .column("id", DataType::Serial)
            .primary_key()
            .add()
            .column("username", DataType::Varchar(255))
            .add()
            .column("is_active", DataType::Boolean)
            .default_value(Expr::Value(Boolean(true)))
            .add()
            .build();

        assert!(ast.if_not_exists);
        assert_eq!(ast.table.name, "users");
        assert_eq!(ast.columns.len(), 3);
        assert!(ast.columns[0].is_primary_key);
        assert_eq!(ast.columns[1].data_type, DataType::Varchar(255));
        assert!(!ast.columns[2].is_nullable); // Should be NOT NULL by default
        assert!(ast.columns[2].default_value.is_some());
    }
}
