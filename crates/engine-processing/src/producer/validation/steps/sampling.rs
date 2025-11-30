use crate::{error::ProducerError, transform::pipeline::TransformPipeline};
use engine_config::{
    report::{finding::Finding, transform::TransformationRecord},
    settings::validated::ValidatedSettings,
    validation::schema_validator::DestinationSchemaValidator,
};
use engine_core::connectors::source::Source;
use model::{pagination::cursor::Cursor, records::row::RowData, transform::mapping::EntityMapping};
use smql_syntax::ast::setting::CopyColumns;
use std::collections::{HashMap, HashSet};

/// Result of sampling and transformation
#[derive(Debug, Clone)]
pub struct SampleResult {
    pub records_sampled: usize,
    pub reached_end: bool,
    pub next_cursor: Option<Cursor>,
    pub source_entity: Option<String>,
    pub transformation_records: Vec<TransformationRecord>,
    pub omitted_columns: HashMap<String, HashSet<String>>,
}

/// Step responsible for sampling data from source and applying transformations
pub struct SamplingStep {
    source: Source,
    pipeline: TransformPipeline,
    mapping: EntityMapping,
    settings: ValidatedSettings,
    cursor: Cursor,
    sample_size: usize,
}

impl SamplingStep {
    pub fn new(
        source: Source,
        pipeline: TransformPipeline,
        mapping: EntityMapping,
        settings: ValidatedSettings,
        cursor: Cursor,
        sample_size: usize,
    ) -> Self {
        Self {
            source,
            pipeline,
            mapping,
            settings,
            cursor,
            sample_size,
        }
    }

    fn is_allowed_output_field(&self, table: &str, field_name: &str) -> Result<bool, Finding> {
        if self.settings.copy_columns == CopyColumns::All {
            return Ok(true);
        }

        let colmap = self
            .mapping
            .field_mappings
            .column_mappings
            .get(table)
            .ok_or_else(|| Finding::new_mapping_missing(table, ""))?;

        if colmap.contains_target_key(field_name) {
            return Ok(true);
        }

        if let Some(computed_fields) = self.mapping.field_mappings.get_computed(table)
            && computed_fields
                .iter()
                .any(|cf| cf.name.eq_ignore_ascii_case(field_name))
        {
            return Ok(true);
        }

        Ok(false)
    }

    /// Prune unmapped columns when CopyColumns::MapOnly is set.
    fn prune_row(
        &self,
        row: &mut RowData,
        findings: &mut Vec<Finding>,
        omitted: &mut HashMap<String, HashSet<String>>,
    ) {
        if self.settings.copy_columns == CopyColumns::All {
            return;
        }

        let table = row.entity.as_str();

        // If no mapping exists for the table, we cannot determine which columns to keep.
        // Record a single finding and remove all fields.
        if !self
            .mapping
            .field_mappings
            .column_mappings
            .contains_key(table)
        {
            findings.push(Finding::new_mapping_missing(
                table,
                " Output row will be empty.",
            ));
            let omitted_for_table = omitted.entry(table.to_string()).or_default();
            for fv in row.field_values.drain(..) {
                omitted_for_table.insert(fv.name);
            }
            return;
        }

        // Partition the fields into retained and dropped lists.
        let (retained, dropped): (Vec<_>, Vec<_>) = row.field_values.drain(..).partition(|fv| {
            match self.is_allowed_output_field(table, &fv.name) {
                Ok(true) => true,
                Ok(false) => false,
                Err(finding) => {
                    findings.push(finding);
                    false
                }
            }
        });

        let source_name = self.mapping.entity_name_map.reverse_resolve(table);
        if !dropped.is_empty() {
            omitted
                .entry(source_name)
                .or_default()
                .extend(dropped.into_iter().map(|fv| fv.name));
        }

        row.field_values = retained;
    }

    pub async fn sample_and_transform(
        &self,
        validator: &mut DestinationSchemaValidator,
    ) -> Result<SampleResult, ProducerError> {
        let mut prune_findings = Vec::new();
        let mut omitted_columns: HashMap<String, HashSet<String>> = HashMap::new();

        match self
            .source
            .fetch_data(self.sample_size, self.cursor.clone())
            .await
        {
            Ok(data) => {
                let total = data.row_count;
                let next_cursor = data.next_cursor.clone();
                let reached_end = data.reached_end;
                let source_entity = data.rows.first().map(|r| r.entity.clone());

                let transformation_records: Vec<TransformationRecord> = data
                    .rows
                    .into_iter()
                    .map(|input_row| {
                        let input_clone = input_row.clone();
                        let mut output_row = self.pipeline.apply(&input_row);

                        if output_row.entity.is_empty() {
                            output_row.entity = input_clone.entity.clone();
                        }
                        self.prune_row(&mut output_row, &mut prune_findings, &mut omitted_columns);
                        validator.validate(&output_row);

                        TransformationRecord {
                            input: input_clone,
                            output: Some(output_row),
                            error: None,
                            warnings: None,
                        }
                    })
                    .collect();

                Ok(SampleResult {
                    records_sampled: total,
                    reached_end,
                    next_cursor,
                    source_entity,
                    transformation_records,
                    omitted_columns,
                })
            }
            Err(e) => Err(ProducerError::Other(format!("Fetch error: {e}"))),
        }
    }
}
