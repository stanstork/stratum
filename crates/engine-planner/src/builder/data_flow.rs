use crate::plan::{
    pipeline::{
        data_flow_summary::DataFlowSummary, settings::PipelineSettings, source::SourcePlan,
    },
    transform::{
        join::JoinPlan,
        mapping::{ColumnMapping, MappingSource, MappingType},
    },
    validation::{plan::ValidationPlan, types::ValidationLevel},
};
use engine_config::settings::CopyColumns;
use std::collections::HashSet;

/// Refines data flow statistics for a specific pipeline
pub struct DataFlowAnalyzer;

impl DataFlowAnalyzer {
    pub fn analyze(
        mappings: &[ColumnMapping],
        joins: &[JoinPlan],
        validations: &[ValidationPlan],
        source: &SourcePlan,
        settings: &PipelineSettings,
    ) -> DataFlowSummary {
        let mut summary = DataFlowSummary::default();

        // Count explicit mappings
        for mapping in mappings {
            match &mapping.mapping_type {
                MappingType::Direct => summary.direct_columns += 1,
                MappingType::Renamed => summary.renamed_columns += 1,
                MappingType::Computed => summary.computed_columns += 1,
                MappingType::Conditional => summary.conditional_columns += 1,
                MappingType::Lookup => summary.lookup_columns += 1,
                MappingType::Generated | MappingType::Constant => summary.generated_columns += 1,
            }

            if let Some(tc) = &mapping.type_conversion {
                summary.type_conversions += 1;
                if !tc.is_safe {
                    summary.unsafe_conversions += 1;
                }
            }
        }

        // Build set of columns referenced in mappings (for computing dropped columns)
        let mapped_source_columns: HashSet<_> = mappings
            .iter()
            .flat_map(|m| match &m.source {
                MappingSource::Column { column, .. } => vec![column.clone()],
                MappingSource::Renamed { original_name, .. } => vec![original_name.clone()],
                MappingSource::Expression {
                    columns_referenced, ..
                } => columns_referenced.clone(),
                _ => vec![],
            })
            .collect();

        // When copy_columns is All, all source columns that aren't explicitly remapped
        // are implicitly copied as direct 1:1 mappings
        if matches!(settings.copy_columns, CopyColumns::All) {
            // Count source columns that don't have an explicit mapping to a different name
            let explicitly_remapped: HashSet<_> = mappings
                .iter()
                .filter_map(|m| match &m.source {
                    MappingSource::Column { column, .. } if m.target != *column => {
                        Some(column.clone())
                    }
                    MappingSource::Renamed { original_name, .. } => Some(original_name.clone()),
                    _ => None,
                })
                .collect();

            let implicit_direct_count = source
                .columns
                .iter()
                .filter(|c| !explicitly_remapped.contains(&c.name))
                .count();
            summary.direct_columns += implicit_direct_count;
        }

        // Build set of all source column names
        let source_column_names: HashSet<_> =
            source.columns.iter().map(|c| c.name.clone()).collect();

        // Dropped columns: only when copy_columns is MapOnly
        summary.dropped_columns = match settings.copy_columns {
            CopyColumns::MapOnly => {
                // Only mapped columns are copied, others are dropped
                source
                    .columns
                    .iter()
                    .filter(|c| !mapped_source_columns.contains(&c.name))
                    .map(|c| c.name.clone())
                    .collect()
            }
            CopyColumns::All => {
                // All source columns are copied, nothing is dropped
                vec![]
            }
        };

        // New columns: destination columns that don't exist in source
        summary.new_columns = mappings
            .iter()
            .filter(|m| {
                // Check if target column exists in source
                !source_column_names.contains(&m.target)
            })
            .map(|m| m.target.clone())
            .collect();

        summary.join_count = joins.len();
        summary.assert_count = validations
            .iter()
            .filter(|v| v.level == ValidationLevel::Assert)
            .count();
        summary.warn_count = validations
            .iter()
            .filter(|v| v.level == ValidationLevel::Warn)
            .count();

        summary
    }
}
