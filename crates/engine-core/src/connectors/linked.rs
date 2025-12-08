use crate::connectors::source::DataFormat;
use connectors::{
    adapter::Adapter,
    error::AdapterError,
    sql::base::join::{
        clause::{JoinClause, JoinColumn, JoinCondition, JoinType, JoinedTable},
        source::JoinSource,
    },
};
use model::{
    execution::{expr::CompiledExpression, pipeline::Join},
    transform::mapping::TransformationMetadata,
};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub enum LinkedSource {
    Table(Box<JoinSource>),
    File { path: String, format: String },
}

impl LinkedSource {
    pub async fn new(
        adapter: &Adapter,
        format: &DataFormat,
        joins: &Vec<Join>,
        mapping: &TransformationMetadata,
    ) -> Result<Option<Self>, AdapterError> {
        if joins.is_empty() {
            return Ok(None);
        }

        if !matches!(format, DataFormat::MySql | DataFormat::Postgres) {
            return Err(AdapterError::UnsupportedFormat(format.to_string()));
        }

        let tables = joins.iter().map(|j| j.table.clone()).collect::<Vec<_>>();

        // // precompute join clauses & projection
        let join_clauses = Self::build_join_clauses(&joins);
        let projection = Self::extract_projection(&tables, mapping);

        // fetch metadata for all tables
        let mut meta = HashMap::new();
        for table in &tables {
            let table_meta = adapter.get_sql().table_metadata(table).await?;
            meta.insert(table.clone(), table_meta);
        }

        Ok(Some(LinkedSource::Table(Box::new(JoinSource::new(
            meta,
            join_clauses,
            projection,
            mapping.clone(),
        )))))
    }

    fn build_join_clauses(joins: &[Join]) -> Vec<JoinClause> {
        joins
            .iter()
            .filter_map(|join| {
                // Extract join condition from the CompiledExpression
                // Expected format: Binary { left: DotPath, op: Equal, right: DotPath }
                // Example: customers.id = orders.customer_id
                let condition = join.condition.as_ref()?;

                if let CompiledExpression::Binary { left, right, .. } = condition {
                    // Extract both sides of the join condition
                    let (left_table_alias, left_column) = Self::extract_dotpath_parts(left)?;
                    let (right_table_alias, right_column) = Self::extract_dotpath_parts(right)?;

                    let join_condition = JoinCondition {
                        left: JoinColumn {
                            alias: left_table_alias.clone(),
                            column: left_column,
                        },
                        right: JoinColumn {
                            alias: right_table_alias.clone(),
                            column: right_column,
                        },
                    };

                    // Build the join clause
                    // The left table in the condition becomes the left side of the join
                    // The joined table (from join.table/alias) becomes the right side
                    Some(JoinClause {
                        left: JoinedTable {
                            table: left_table_alias.clone(),
                            alias: left_table_alias,
                        },
                        right: JoinedTable {
                            table: join.table.clone(),
                            alias: join.alias.clone(),
                        },
                        join_type: JoinType::Inner,
                        conditions: vec![join_condition],
                    })
                } else {
                    None
                }
            })
            .collect()
    }

    /// Extract table alias and column name from a DotPath expression
    fn extract_dotpath_parts(expr: &CompiledExpression) -> Option<(String, String)> {
        if let CompiledExpression::DotPath(segments) = expr {
            if segments.len() >= 2 {
                Some((segments[0].clone(), segments[1].clone()))
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Extracts the projection (columns to fetch) for each table from cross-entity references.
    fn extract_projection(
        tables: &[String],
        metadata: &TransformationMetadata,
    ) -> HashMap<String, Vec<String>> {
        tables
            .iter()
            .map(|table| {
                // Get all cross-entity references for this table
                let refs = metadata.get_cross_entity_refs_for(table);

                // Extract unique field names
                let mut keys: Vec<String> = refs.iter().map(|r| r.field.clone()).collect();

                // Remove duplicates
                keys.sort();
                keys.dedup();

                (table.clone(), keys)
            })
            .collect()
    }
}
