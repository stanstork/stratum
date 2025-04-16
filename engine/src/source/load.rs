use smql::statements::{connection::DataFormat, load::Load};
use sql_adapter::{
    adapter::SqlAdapter,
    join::{Join, JoinClause, JoinColumn, JoinCondition, JoinType, JoinedTable},
};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum LoadSource {
    TableJoin(Join),
    File { path: String, format: String },
}

impl LoadSource {
    pub async fn from_load(
        adapter: Arc<(dyn SqlAdapter + Send + Sync)>,
        source_format: DataFormat,
        value: Load,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        match source_format {
            DataFormat::MySql | DataFormat::Postgres => {
                let join_clause = Self::join_from_load(&value);
                let source_metadata = adapter.fetch_metadata(&value.source).await?;
                Ok(LoadSource::TableJoin(Join::new(
                    source_metadata,
                    join_clause,
                )))
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
