use common::{computed::ComputedField, mapping::NameMap};
use smql::statements::{connection::DataFormat, expr::Expression, load::Load};
use sql_adapter::{
    adapter::SqlAdapter,
    join::{Join, JoinClause, JoinColumn, JoinCondition, JoinType, JoinedTable},
};
use std::{collections::HashMap, sync::Arc};

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
        computed: &HashMap<String, Vec<ComputedField>>,
        entity_name_map: NameMap,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        match source_format {
            DataFormat::MySql | DataFormat::Postgres => {
                let join_clause = Self::join_from_load(&value, computed, entity_name_map);
                let source_metadata = adapter.fetch_metadata(&value.source).await?;
                Ok(LoadSource::TableJoin(Join::new(
                    source_metadata,
                    join_clause,
                )))
            }
            _ => panic!("Unsupported data format"),
        }
    }

    fn join_from_load(
        load: &Load,
        computed: &HashMap<String, Vec<ComputedField>>,
        entity_name_map: NameMap,
    ) -> JoinClause {
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

        let computed_fields = computed
            .values()
            .flat_map(|fields| fields.clone())
            .collect::<Vec<_>>();

        let fields = computed_fields
            .iter()
            .map(|f| match &f.expression {
                Expression::Lookup { table, key, field } => {
                    if table.eq_ignore_ascii_case(&left_alias) {
                        Some(key.clone())
                    } else {
                        None
                    }
                }
                _ => None,
            })
            .filter(Option::is_some)
            .map(Option::unwrap)
            .collect::<Vec<_>>();

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
            fields,
        }
    }
}
