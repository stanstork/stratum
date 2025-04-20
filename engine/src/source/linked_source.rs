use crate::adapter::Adapter;
use common::{computed::ComputedField, mapping::EntityMappingContext};
use smql::statements::{connection::DataFormat, expr::Expression, load::Load};
use sql_adapter::join::{
    clause::{JoinClause, JoinColumn, JoinCondition, JoinType, JoinedTable},
    source::JoinSource,
};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub enum LinkedSource {
    Table(JoinSource),
    File { path: String, format: String },
}

impl LinkedSource {
    pub async fn new(
        adapter: &Adapter,
        format: &DataFormat,
        mapping_context: &EntityMappingContext,
        load: &Load,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        match *format {
            DataFormat::MySql | DataFormat::Postgres => {
                let join_clause = Self::build_join_clause(load);
                let projection =
                    Self::extract_projection(load, &mapping_context.field_mappings.computed_fields);
                let source_metadata = adapter.get_adapter().fetch_metadata(&load.source).await?;

                Ok(LinkedSource::Table(JoinSource::new(
                    source_metadata,
                    join_clause,
                    projection,
                    mapping_context.clone(),
                )))
            }
            unsupported => Err(format!("Unsupported data format: {:?}", unsupported).into()),
        }
    }

    fn build_join_clause(load: &Load) -> JoinClause {
        let left_alias = &load.name;
        let left_table = &load.source;
        let right_table = &load.join;
        let right_alias = right_table; // aliasing not yet supported

        let conditions: Vec<JoinCondition> = load
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
                table: left_table.clone(),
                alias: left_alias.clone(),
            },
            right: JoinedTable {
                table: right_table.clone(),
                alias: right_alias.clone(),
            },
            join_type: JoinType::Inner, // default for now
            conditions,
        }
    }

    fn extract_projection(
        load: &Load,
        computed: &HashMap<String, Vec<ComputedField>>,
    ) -> Vec<String> {
        let fields = computed
            .values()
            .flat_map(|fields| fields.clone())
            .collect::<Vec<_>>();

        fields
            .iter()
            .filter_map(|f| match &f.expression {
                Expression::Lookup { table, key, .. } if table.eq_ignore_ascii_case(&load.name) => {
                    Some(key.clone())
                }
                _ => None,
            })
            .collect()
    }
}
