use crate::metadata::table::TableMetadata;

#[derive(Debug, Clone)]
pub struct Join {
    pub source_metadata: TableMetadata,
    pub join_clause: JoinClause,
}

impl Join {
    pub fn new(source_metadata: TableMetadata, join_clause: JoinClause) -> Self {
        Self {
            source_metadata,
            join_clause,
        }
    }
}

#[derive(Debug, Clone)]
pub struct JoinClause {
    pub left: JoinedTable,
    pub right: JoinedTable,
    pub join_type: JoinType,
    pub conditions: Vec<JoinCondition>,
}

#[derive(Debug, Clone)]
pub struct JoinedTable {
    pub table: String,
    pub alias: String,
}

#[derive(Debug, Clone)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

#[derive(Debug, Clone)]
pub struct JoinCondition {
    pub left: JoinColumn,
    pub right: JoinColumn,
}

#[derive(Debug, Clone)]
pub struct JoinColumn {
    pub alias: String,
    pub column: String,
}
