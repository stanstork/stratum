//! Provides a fluent builder for constructing `AlterTable` ASTs.

use crate::ast::{
    alter_table::{AlterTable, AlterTableOperation},
    common::TableRef,
    create_table::{ColumnDef, TableConstraint},
    expr::Expr,
};
use common::types::DataType;

#[derive(Debug, Clone)]
pub struct AlterTableBuilder {
    ast: AlterTable,
}

impl AlterTableBuilder {
    pub fn new(table: TableRef) -> Self {
        Self {
            ast: AlterTable {
                table,
                ..Default::default()
            },
        }
    }

    pub fn add_column(self, name: &str, data_type: DataType) -> AddColumnBuilder {
        AddColumnBuilder::new(self, name, data_type)
    }

    pub fn add_foreign_key(
        mut self,
        columns: &[&str],
        references: TableRef,
        referenced_columns: &[&str],
    ) -> Self {
        self.ast.operations.push(AlterTableOperation::AddConstraint(
            TableConstraint::ForeignKey {
                columns: columns.iter().map(|s| s.to_string()).collect(),
                references,
                referenced_columns: referenced_columns.iter().map(|s| s.to_string()).collect(),
            },
        ));
        self
    }

    pub fn toggle_triggers(mut self, enabled: bool) -> Self {
        self.ast
            .operations
            .push(AlterTableOperation::ToggleTriggers { enabled });
        self
    }

    pub fn build(self) -> AlterTable {
        self.ast
    }
}

pub struct AddColumnBuilder {
    table_builder: AlterTableBuilder,
    column: ColumnDef,
}

impl AddColumnBuilder {
    pub fn new(table_builder: AlterTableBuilder, name: &str, data_type: DataType) -> Self {
        Self {
            table_builder,
            column: ColumnDef {
                name: name.to_string(),
                data_type,
                is_nullable: false,
                is_primary_key: false, // Cannot add PK via ADD COLUMN
                default_value: None,
            },
        }
    }

    pub fn nullable(mut self) -> Self {
        self.column.is_nullable = true;
        self
    }

    pub fn default(mut self, value: Expr) -> Self {
        self.column.default_value = Some(value);
        self
    }

    pub fn add(mut self) -> AlterTableBuilder {
        self.table_builder
            .ast
            .operations
            .push(AlterTableOperation::AddColumn(self.column));
        self.table_builder
    }
}

#[cfg(test)]
mod tests {
    use common::types::DataType;

    use crate::{
        ast::{alter_table::AlterTableOperation, common::TableRef},
        build::alter_table::AlterTableBuilder,
    };

    fn table(name: &str) -> TableRef {
        TableRef {
            schema: None,
            name: name.to_string(),
        }
    }

    #[test]
    fn test_build_alter_table_with_toggle_triggers() {
        let builder = AlterTableBuilder::new(table("posts"));

        let ast = builder
            .add_column("category_id", DataType::Int)
            .add()
            .toggle_triggers(false) // Disable triggers
            .build();

        assert_eq!(ast.table.name, "posts");
        assert_eq!(ast.operations.len(), 2);

        assert!(matches!(
            &ast.operations[0],
            AlterTableOperation::AddColumn(_)
        ));
        assert!(matches!(
            &ast.operations[1],
            AlterTableOperation::ToggleTriggers { enabled: false }
        ));
    }
}
