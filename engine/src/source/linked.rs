use crate::{context::global::GlobalContext, error::MigrationError};
use data_model::{transform::computed::ComputedField, transform::mapping::EntityMapping};
use smql::statements::{
    connection::DataFormat,
    expr::Expression,
    load::{Load, MatchPair},
};
use sql_adapter::join::{
    clause::{JoinClause, JoinColumn, JoinCondition, JoinType, JoinedTable},
    source::JoinSource,
};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub enum LinkedSource {
    Table(Box<JoinSource>),
    File { path: String, format: String },
}

impl LinkedSource {
    pub async fn new(
        ctx: &GlobalContext,
        format: DataFormat,
        load: &Load,
        mapping: &EntityMapping,
    ) -> Result<Self, MigrationError> {
        if !matches!(format, DataFormat::MySql | DataFormat::Postgres) {
            return Err(MigrationError::UnsupportedFormat(format.to_string()));
        }

        // check if the source adapter is available
        // if not, return an error
        let src_adapter = ctx
            .src_conn
            .as_ref()
            .ok_or(MigrationError::AdapterNotFound(format.to_string()))?;

        // precompute join clauses & projection
        let join_clauses = Self::build_join_clauses(&load.matches);
        let projection =
            Self::extract_projection(&load.entities, &mapping.field_mappings.computed_fields);

        // fetch metadata for all tables
        let mut meta = HashMap::new();
        for table in &load.entities {
            let table_meta = src_adapter.get_sql().fetch_metadata(table).await?;
            meta.insert(table.clone(), table_meta);
        }

        Ok(LinkedSource::Table(Box::new(JoinSource::new(
            meta,
            join_clauses,
            projection,
            mapping.clone(),
        ))))
    }

    fn build_join_clauses(matches: &[MatchPair]) -> Vec<JoinClause> {
        matches
            .iter()
            .map(|pair| {
                let left_entity = pair.left.entity().expect("Left entity name is required");
                let left_column = pair.left.key().expect("Left key name is required");
                let right_entity = pair.right.entity().expect("Right entity name is required");
                let right_column = pair.right.key().expect("Right key name is required");

                // build the single join condition
                // we don't support composite keys yet
                let condition = JoinCondition {
                    left: JoinColumn {
                        alias: left_entity.clone(),
                        column: left_column.clone(),
                    },
                    right: JoinColumn {
                        alias: right_entity.clone(),
                        column: right_column.clone(),
                    },
                };

                // assemble the clause
                JoinClause {
                    left: JoinedTable {
                        table: left_entity.clone(),
                        alias: left_entity.clone(),
                    },
                    right: JoinedTable {
                        table: right_entity.clone(),
                        alias: right_entity.clone(),
                    },
                    join_type: JoinType::Inner,
                    conditions: vec![condition],
                }
            })
            .collect()
    }

    fn extract_projection(
        tables: &[String],
        computed: &HashMap<String, Vec<ComputedField>>,
    ) -> HashMap<String, Vec<String>> {
        tables
            .iter()
            .map(|table| {
                let keys = computed
                    .values()
                    .flat_map(|fields| fields.iter())
                    .filter_map(|f| match &f.expression {
                        Expression::Lookup { entity, key, .. }
                            if entity.eq_ignore_ascii_case(table) =>
                        {
                            Some(key.clone())
                        }
                        _ => None,
                    })
                    .collect::<Vec<_>>();

                (table.clone(), keys)
            })
            .collect()
    }
}
