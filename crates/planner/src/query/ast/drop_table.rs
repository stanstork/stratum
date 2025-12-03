use crate::query::ast::common::TableRef;

#[derive(Debug, Clone)]
pub struct DropTable {
    pub table: TableRef,
    pub if_exists: bool,
}
