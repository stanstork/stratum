use crate::ast::{common::TableRef, truncate_table::TruncateTable};

#[derive(Debug, Clone)]
pub struct TruncateTableBuilder {
    ast: TruncateTable,
}

impl TruncateTableBuilder {
    pub fn new(table: TableRef) -> Self {
        Self {
            ast: TruncateTable { table },
        }
    }

    pub fn build(self) -> TruncateTable {
        self.ast
    }
}
