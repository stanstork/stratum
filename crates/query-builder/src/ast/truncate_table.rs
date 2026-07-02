use crate::ast::common::TableRef;

/// `TRUNCATE TABLE <table>`
#[derive(Debug, Clone)]
pub struct TruncateTable {
    pub table: TableRef,
}
