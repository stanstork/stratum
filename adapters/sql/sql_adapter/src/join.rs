use smql::statements::load::Load;

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

impl JoinClause {
    pub fn from_load(load: &Load) -> Self {
        let left_alias = load.name.clone();
        let left_table = load.source.clone();
        let right_table = load.join.clone();
        let right_alias = right_table.clone(); // no alias support yet

        let conditions = load
            .mappings
            .iter()
            .map(|(left_col, right_col)| JoinCondition {
                left: JoinColumn {
                    alias: left_alias.clone(),
                    column: left_col.clone(),
                },
                right: JoinColumn {
                    alias: right_alias.clone(),
                    column: right_col.clone(),
                },
            })
            .collect();

        JoinClause {
            left: JoinedTable {
                table: left_table,
                alias: left_alias,
            },
            right: JoinedTable {
                table: right_table,
                alias: right_alias,
            },
            join_type: JoinType::Inner, // default, can be customized later
            conditions,
        }
    }
}
