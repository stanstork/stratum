use smql::statements::{connection::DataFormat, load::Load};
use sql_adapter::join::{JoinClause, JoinColumn, JoinCondition, JoinType, JoinedTable};

#[derive(Debug, Clone)]
pub enum LoadSource {
    TableJoin(JoinClause),
    File { path: String, format: String },
}

impl LoadSource {
    pub fn from_load(source_format: DataFormat, value: Load) -> Self {
        match source_format {
            DataFormat::MySql | DataFormat::Postgres => {
                let join_clause = Self::join_from_load(&value);
                LoadSource::TableJoin(join_clause)
            }
            _ => panic!("Unsupported data format"),
        }
    }

    fn join_from_load(load: &Load) -> JoinClause {
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
