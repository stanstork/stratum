use std::collections::{HashMap, HashSet};

use crate::{
    builder::analysis::{
        registry::{AnalysisState, PipelineAnalysisInput},
        {AnalysisContext, AnalyzerError, AnalyzerResult, PipelineAnalysisStage},
    },
    plan::transform::{
        join::JoinPlan,
        mapping::{ColumnMapping, MappingSource},
    },
};
use async_trait::async_trait;
use engine_processing::io::driver::SchemaDriver;

pub struct JoinUsageStage;

#[async_trait]
impl<S: SchemaDriver, D: SchemaDriver> PipelineAnalysisStage<S, D> for JoinUsageStage {
    fn name(&self) -> &'static str {
        "join_usage"
    }

    async fn run(
        &self,
        _input: &PipelineAnalysisInput,
        _ctx: &AnalysisContext<S, D>,
        state: &mut AnalysisState,
    ) -> AnalyzerResult<()> {
        let source_table = state.require_source()?.table.clone();

        let joins = state.joins.as_mut().ok_or_else(|| {
            AnalyzerError::error("join_usage", "Missing join analysis result".to_string())
        })?;
        let mappings = state.mappings.as_ref().ok_or_else(|| {
            AnalyzerError::error("join_usage", "Missing mapping analysis result".to_string())
        })?;

        populate_join_columns_used(joins, mappings, &source_table);
        Ok(())
    }
}

fn populate_join_columns_used(
    joins: &mut [JoinPlan],
    mappings: &[ColumnMapping],
    source_table: &str,
) {
    let mut table_columns: HashMap<String, HashSet<String>> = HashMap::new();

    for mapping in mappings {
        if let MappingSource::Column { table, column } = &mapping.source {
            if table != source_table {
                table_columns
                    .entry(table.clone())
                    .or_default()
                    .insert(column.clone());
            }
        } else if let MappingSource::Expression {
            columns_referenced, ..
        } = &mapping.source
        {
            for col_ref in columns_referenced {
                if let Some((table, column)) = col_ref.split_once('.')
                    && table != source_table
                {
                    table_columns
                        .entry(table.to_string())
                        .or_default()
                        .insert(column.to_string());
                }
            }
        }
    }

    for join in joins {
        if let Some(columns) = table_columns.get(&join.source_table) {
            join.columns_used = columns.iter().cloned().collect();
            join.columns_used.sort();
        }
    }
}
