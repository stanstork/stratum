use connectors::metadata::entity::EntityMetadata;
use engine_config::report::dry_run::{FastPathCapabilities, FastPathSummary};
use engine_core::connectors::destination::Destination;
use engine_core::connectors::source::Source;
use model::transform::mapping::EntityMapping;
use smql_syntax::ast::setting::Settings;

/// Step responsible for validating fast path capabilities
pub struct FastPathValidationStep {
    source: Source,
    destination: Destination,
    mapping: EntityMapping,
    settings: Settings,
}

impl FastPathValidationStep {
    pub fn new(
        source: Source,
        destination: Destination,
        mapping: EntityMapping,
        settings: Settings,
    ) -> Self {
        Self {
            source,
            destination,
            mapping,
            settings,
        }
    }

    pub async fn eval_fast_path(&self) -> FastPathSummary {
        let sink = self.destination.sink();
        let adapter = self.destination.data_dest.adapter().await;

        let (capabilities, capability_probe_error) = match adapter.capabilities().await {
            Ok(caps) => (
                Some(FastPathCapabilities {
                    copy_streaming: caps.copy_streaming,
                    merge_statements: caps.merge_statements,
                }),
                None,
            ),
            Err(e) => (
                None,
                Some(format!("Fast path capability probe failed: {e}")),
            ),
        };

        match sink.support_fast_path().await {
            Ok(true) => match adapter.table_exists(&self.destination.name).await {
                Ok(true) => match self
                    .destination
                    .data_dest
                    .fetch_meta(self.destination.name.clone())
                    .await
                {
                    Ok(meta) => {
                        if meta.primary_keys.is_empty() {
                            FastPathSummary {
                                supported: false,
                                reason: Some(
                                    "Fast path disabled: destination table has no primary key"
                                        .to_string(),
                                ),
                                capabilities,
                            }
                        } else {
                            FastPathSummary {
                                supported: true,
                                reason: None,
                                capabilities,
                            }
                        }
                    }
                    Err(e) => FastPathSummary {
                        supported: false,
                        reason: Some(format!("Failed to fetch destination metadata: {e}")),
                        capabilities,
                    },
                },
                Ok(false) if self.settings.create_missing_tables => {
                    let source_table = self
                        .mapping
                        .entity_name_map
                        .reverse_resolve(&self.destination.name);
                    match self.source.primary.fetch_meta(source_table.clone()).await {
                        Ok(EntityMetadata::Table(table_meta)) => {
                            if table_meta.primary_keys.is_empty() {
                                FastPathSummary {
                                    supported: false,
                                    reason: Some(format!(
                                        "Fast path disabled: source table `{source_table}` has no primary key"
                                    )),
                                    capabilities,
                                }
                            } else {
                                FastPathSummary {
                                    supported: true,
                                    reason: None,
                                    capabilities,
                                }
                            }
                        }
                        Ok(_) => FastPathSummary {
                            supported: false,
                            reason: Some(format!(
                                "Fast path disabled: cannot infer primary keys for `{source_table}`"
                            )),
                            capabilities,
                        },
                        Err(e) => FastPathSummary {
                            supported: false,
                            reason: Some(format!(
                                "Failed to fetch source metadata for `{source_table}`: {e}"
                            )),
                            capabilities,
                        },
                    }
                }
                Ok(false) => FastPathSummary {
                    supported: false,
                    reason: Some(
                        "Destination table does not exist and auto-creation is disabled"
                            .to_string(),
                    ),
                    capabilities,
                },
                Err(e) => FastPathSummary {
                    supported: false,
                    reason: Some(format!("Fast path table existence check failed: {e}")),
                    capabilities,
                },
            },
            Ok(false) => FastPathSummary {
                supported: false,
                reason: Some("Destination sink does not support fast path".to_string()),
                capabilities,
            },
            Err(e) => FastPathSummary {
                supported: false,
                reason: Some(
                    capability_probe_error
                        .unwrap_or_else(|| format!("Fast path check failed: {e}")),
                ),
                capabilities,
            },
        }
    }
}
