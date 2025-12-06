use crate::error::ProducerError;
use engine_config::{
    report::mapping::EntityMappingReport, settings::validated::ValidatedSettings,
    validation::schema_validator::DestinationSchemaValidator,
};
use engine_core::connectors::{destination::Destination, source::Source};
use model::transform::mapping::EntityMapping;
use smql_syntax::ast_v2::setting::CopyColumns;
use std::collections::HashSet;

/// Step responsible for schema validation
pub struct SchemaValidationStep {
    source: Source,
    destination: Destination,
    mapping: EntityMapping,
    settings: ValidatedSettings,
}

impl SchemaValidationStep {
    pub fn new(
        source: Source,
        destination: Destination,
        mapping: EntityMapping,
        settings: ValidatedSettings,
    ) -> Self {
        Self {
            source,
            destination,
            mapping,
            settings,
        }
    }

    /// Initialize the destination schema validator
    pub async fn init_validator(&self) -> Result<DestinationSchemaValidator, ProducerError> {
        DestinationSchemaValidator::new(&self.destination, self.mapping.clone(), &self.settings)
            .await
            .map_err(|e| ProducerError::Other(format!("Init schema validator: {e}")))
    }

    /// Update one-to-one mapped columns if CopyColumns::All is set
    pub async fn update_one_to_one_mapped(
        &self,
        entity_report: &mut EntityMappingReport,
    ) -> Result<(), ProducerError> {
        if self.settings.copy_columns != CopyColumns::All {
            return Ok(());
        }

        match self
            .source
            .primary
            .fetch_meta(entity_report.source_entity.clone())
            .await
        {
            Ok(meta) => {
                let source_columns: HashSet<String> =
                    meta.columns().iter().map(|c| c.name().to_owned()).collect();
                let target_columns: HashSet<String> = entity_report
                    .renames
                    .iter()
                    .map(|r| r.from.clone())
                    .collect();
                let computed_columns: HashSet<String> = entity_report
                    .computed
                    .iter()
                    .map(|c| c.name.clone())
                    .collect();

                entity_report.one_to_one.extend(
                    source_columns
                        .difference(&target_columns)
                        .filter(|col| !computed_columns.contains(*col))
                        .cloned()
                        .collect::<Vec<_>>(),
                );
                Ok(())
            }
            Err(e) => Err(ProducerError::Other(format!(
                "Failed to fetch metadata for {}: {e}",
                entity_report.source_entity
            ))),
        }
    }
}
