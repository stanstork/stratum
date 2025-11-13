use crate::query::ast::{common::TableRef, drop_table::DropTable};

#[derive(Debug, Clone)]
pub struct DropTableBuilder {
    ast: DropTable,
}

impl DropTableBuilder {
    pub fn new(table: TableRef) -> Self {
        Self {
            ast: DropTable {
                table,
                if_exists: false,
            },
        }
    }

    pub fn if_exists(mut self) -> Self {
        self.ast.if_exists = true;
        self
    }

    pub fn build(self) -> DropTable {
        self.ast
    }
}

#[cfg(test)]
mod tests {
    use crate::query::{ast::common::TableRef, builder::drop_table::DropTableBuilder};

    #[test]
    fn test_drop_table_builder() {
        let ast = DropTableBuilder::new(TableRef {
            schema: None,
            name: "users".to_string(),
        })
        .if_exists()
        .build();

        assert!(ast.if_exists);
        assert_eq!(ast.table.name, "users");
    }
}
