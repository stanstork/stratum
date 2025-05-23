use common::mapping::EntityMapping;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct JoinClause {
    pub left: JoinedTable,
    pub right: JoinedTable,
    pub join_type: JoinType,
    pub conditions: Vec<JoinCondition>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct JoinedTable {
    pub table: String,
    pub alias: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct JoinCondition {
    pub left: JoinColumn,
    pub right: JoinColumn,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct JoinColumn {
    pub alias: String,
    pub column: String,
}

impl JoinClause {
    pub fn apply_mapping(&mut self, mapping: &EntityMapping) {
        self.right.table = mapping.entity_name_map.reverse_resolve(&self.right.table);
        self.right.alias = self.right.table.clone();
    }
}
