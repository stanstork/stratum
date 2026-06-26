use crate::{
    builder::diagnostics::message_catalog::{MessageCatalog, ThresholdCatalog},
    plan::{
        connection::{plan::ConnectionPlan, status::ConnectionStatus},
        diagnostics::diagnostic::Diagnostic,
        execution::execution_settings::{ExecutionSettings, ExecutionStrategy},
        pagination::{plan::PaginationPlan, strategy::PaginationStrategy},
        pipeline::{
            destination::{DestinationPlan, WriteMode},
            plan::PipelinePlan,
            source::SourcePlan,
        },
        transform::{
            filter::FilterPlan,
            join::JoinPlan,
            mapping::{ColumnMapping, MappingType},
        },
    },
};
use once_cell::sync::Lazy;

/// Static catalog for diagnostic messages and thresholds loaded at compile time.
struct DiagnosticCatalog {
    messages: MessageCatalog,
    thresholds: ThresholdCatalog,
}

static CATALOG: Lazy<DiagnosticCatalog> = Lazy::new(|| DiagnosticCatalog {
    messages: MessageCatalog::from_toml(include_str!("../../../resources/diagnostic.toml"), ""),
    thresholds: ThresholdCatalog::from_toml(include_str!("../../../resources/diagnostic.toml")),
});

// Message category constants
mod msg {
    pub const CONNECTIONS: &str = "connections";
    pub const SOURCE: &str = "source";
    pub const DESTINATION: &str = "destination";
    pub const FILTERS: &str = "filters";
    pub const JOINS: &str = "joins";
    pub const MAPPINGS: &str = "mappings";
    pub const PAGINATION: &str = "pagination";
    pub const RESOURCES: &str = "resources";
    pub const INTEGRITY: &str = "integrity";
    pub const THRESHOLDS: &str = "thresholds";
}

// Diagnostic code constants
mod code {
    // Connection codes
    pub const CONNECTION_FAILED: &str = "CONNECTION_FAILED";
    pub const CONNECTION_UNTESTED: &str = "CONNECTION_UNTESTED";
    pub const HIGH_LATENCY: &str = "HIGH_LATENCY";

    // Source codes
    pub const NO_PRIMARY_KEY: &str = "NO_PRIMARY_KEY";
    pub const LARGE_DATASET: &str = "LARGE_DATASET";
    pub const VERY_LARGE_DATASET: &str = "VERY_LARGE_DATASET";
    pub const EMPTY_AFTER_FILTER: &str = "EMPTY_AFTER_FILTER";
    pub const EMPTY_SOURCE: &str = "EMPTY_SOURCE";

    // Destination codes
    pub const DESTRUCTIVE_MODE: &str = "DESTRUCTIVE_MODE";
    pub const LARGE_TRUNCATE: &str = "LARGE_TRUNCATE";
    pub const TABLE_WILL_BE_CREATED: &str = "TABLE_WILL_BE_CREATED";
    pub const MISSING_CONFLICT_KEYS: &str = "MISSING_CONFLICT_KEYS";

    // Filter codes
    pub const UNINDEXED_FILTER: &str = "UNINDEXED_FILTER";
    pub const HIGHLY_SELECTIVE_FILTER: &str = "HIGHLY_SELECTIVE_FILTER";

    // Join codes
    pub const UNINDEXED_JOIN: &str = "UNINDEXED_JOIN";
    pub const LARGE_LOOKUP_TABLE: &str = "LARGE_LOOKUP_TABLE";
    pub const LOW_JOIN_MATCH_RATE: &str = "LOW_JOIN_MATCH_RATE";
    pub const MANY_JOINS: &str = "MANY_JOINS";

    // Mapping codes
    pub const UNSAFE_TYPE_CONVERSION: &str = "UNSAFE_TYPE_CONVERSION";
    pub const MANY_COMPUTED_COLUMNS: &str = "MANY_COMPUTED_COLUMNS";

    // Pagination codes
    pub const MISSING_PAGINATION: &str = "MISSING_PAGINATION";
    pub const UNINDEXED_CURSOR: &str = "UNINDEXED_CURSOR";
    pub const OFFSET_PAGINATION: &str = "OFFSET_PAGINATION";

    // Resource codes
    pub const HIGH_MEMORY_USAGE: &str = "HIGH_MEMORY_USAGE";
    pub const LONG_MIGRATION: &str = "LONG_MIGRATION";
    pub const VERY_LONG_MIGRATION: &str = "VERY_LONG_MIGRATION";

    // Integrity codes
    pub const NO_VALIDATIONS: &str = "NO_VALIDATIONS";
    pub const NO_ERROR_HANDLING: &str = "NO_ERROR_HANDLING";
}

pub struct DiagnosticGenerator;

impl DiagnosticGenerator {
    /// Generate all diagnostics for a plan
    pub fn generate(
        pipelines: &[PipelinePlan],
        connections: &[ConnectionPlan],
        settings: &ExecutionSettings,
    ) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();

        diagnostics.extend(Self::check_connections(connections));
        diagnostics.extend(Self::check_pipelines(pipelines));
        diagnostics.extend(Self::check_resources(pipelines, settings));
        diagnostics.extend(Self::check_data_integrity(pipelines));

        diagnostics
    }

    /// Generate diagnostics for a single pipeline
    pub fn for_pipeline(
        name: &str,
        source: &SourcePlan,
        destination: &DestinationPlan,
        filter: &Option<FilterPlan>,
        joins: &[JoinPlan],
        mappings: &[ColumnMapping],
        pagination: &Option<PaginationPlan>,
    ) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();

        diagnostics.extend(Self::check_source(name, source));
        diagnostics.extend(Self::check_destination(name, destination));

        if let Some(f) = filter {
            diagnostics.extend(Self::check_filters(name, f));
        }

        diagnostics.extend(Self::check_joins(name, joins));
        diagnostics.extend(Self::check_mappings(name, mappings));
        diagnostics.extend(Self::check_pagination(name, source, pagination));

        diagnostics
    }

    fn check_connections(connections: &[ConnectionPlan]) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();
        let latency_threshold = Self::get_threshold("high_latency_ms") as u64;

        for conn in connections {
            match &conn.status {
                ConnectionStatus::Failed { error } => {
                    diagnostics.push(
                        Diagnostic::error(
                            code::CONNECTION_FAILED,
                            &Self::format_msg(msg::CONNECTIONS, "failed", &[&conn.name, error]),
                        )
                        .with_suggestion(&Self::get_msg(msg::CONNECTIONS, "failed_suggestion")),
                    );
                }
                ConnectionStatus::Untested => {
                    diagnostics.push(
                        Diagnostic::info(
                            code::CONNECTION_UNTESTED,
                            &Self::format_msg(msg::CONNECTIONS, "untested", &[&conn.name]),
                        )
                        .with_suggestion(&Self::get_msg(msg::CONNECTIONS, "untested_suggestion")),
                    );
                }
                ConnectionStatus::Connected { latency_ms, .. }
                    if *latency_ms > latency_threshold =>
                {
                    diagnostics.push(
                        Diagnostic::warning(
                            code::HIGH_LATENCY,
                            &Self::format_msg(
                                msg::CONNECTIONS,
                                "high_latency",
                                &[&conn.name, &latency_ms.to_string()],
                            ),
                        )
                        .with_suggestion(&Self::get_msg(
                            msg::CONNECTIONS,
                            "high_latency_suggestion",
                        )),
                    );
                }
                _ => {}
            }
        }
        diagnostics
    }

    fn check_source(pipeline: &str, source: &SourcePlan) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();
        let large_threshold = Self::get_threshold("large_dataset_rows") as u64;
        let very_large_threshold = Self::get_threshold("very_large_dataset_rows") as u64;

        if source.primary_key.is_empty() {
            diagnostics.push(
                Diagnostic::warning(
                    code::NO_PRIMARY_KEY,
                    &Self::format_msg(msg::SOURCE, "no_primary_key", &[&source.fqn]),
                )
                .with_pipeline(pipeline)
                .with_suggestion(&Self::get_msg(msg::SOURCE, "no_primary_key_suggestion")),
            );
        }

        if source.effective_row_count().value > large_threshold {
            diagnostics.push(
                Diagnostic::info(
                    code::LARGE_DATASET,
                    &Self::format_msg(
                        msg::SOURCE,
                        "large_dataset",
                        &[&source.effective_row_count().display()],
                    ),
                )
                .with_pipeline(pipeline)
                .with_suggestion(&Self::get_msg(msg::SOURCE, "large_dataset_suggestion")),
            );
        }

        if source.effective_row_count().value > very_large_threshold {
            diagnostics.push(
                Diagnostic::warning(
                    code::VERY_LARGE_DATASET,
                    &Self::format_msg(
                        msg::SOURCE,
                        "very_large_dataset",
                        &[&source.effective_row_count().display()],
                    ),
                )
                .with_pipeline(pipeline)
                .with_suggestion(&Self::get_msg(msg::SOURCE, "very_large_dataset_suggestion")),
            );
        }

        if source.effective_row_count().value == 0 && source.total_rows.value > 0 {
            diagnostics.push(
                Diagnostic::warning(
                    code::EMPTY_AFTER_FILTER,
                    &Self::format_msg(msg::SOURCE, "empty_after_filter", &[]),
                )
                .with_pipeline(pipeline)
                .with_suggestion(&Self::get_msg(msg::SOURCE, "empty_after_filter_suggestion")),
            );
        }

        diagnostics
    }

    fn check_destination(pipeline: &str, destination: &DestinationPlan) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();
        let large_truncate_threshold = Self::get_threshold("large_truncate_rows") as u64;

        if destination.mode == WriteMode::Replace && destination.current_rows.value > 0 {
            diagnostics.push(
                Diagnostic::warning(
                    code::DESTRUCTIVE_MODE,
                    &Self::format_msg(
                        msg::DESTINATION,
                        "destructive_mode",
                        &[&destination.current_rows.display(), &destination.fqn],
                    ),
                )
                .with_pipeline(pipeline)
                .with_suggestion(&Self::get_msg(
                    msg::DESTINATION,
                    "destructive_mode_suggestion",
                )),
            );
        }

        if destination.mode == WriteMode::Replace
            && destination.current_rows.value > large_truncate_threshold
        {
            diagnostics.push(
                Diagnostic::warning(
                    code::LARGE_TRUNCATE,
                    &Self::format_msg(
                        msg::DESTINATION,
                        "large_truncate",
                        &[&destination.current_rows.display()],
                    ),
                )
                .with_pipeline(pipeline)
                .with_suggestion(&Self::get_msg(
                    msg::DESTINATION,
                    "large_truncate_suggestion",
                )),
            );
        }

        if !destination.exists {
            diagnostics.push(
                Diagnostic::info(
                    code::TABLE_WILL_BE_CREATED,
                    &Self::format_msg(
                        msg::DESTINATION,
                        "table_will_be_created",
                        &[&destination.fqn],
                    ),
                )
                .with_pipeline(pipeline),
            );
        }

        if destination.mode == WriteMode::Upsert && destination.conflict_keys.is_empty() {
            diagnostics.push(
                Diagnostic::error(
                    code::MISSING_CONFLICT_KEYS,
                    &Self::format_msg(msg::DESTINATION, "missing_conflict_keys", &[]),
                )
                .with_pipeline(pipeline)
                .with_suggestion(&Self::get_msg(
                    msg::DESTINATION,
                    "missing_conflict_keys_suggestion",
                )),
            );
        }

        diagnostics
    }

    fn check_filters(pipeline: &str, filter: &FilterPlan) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();
        let selective_threshold = Self::get_threshold("selective_filter_threshold") as f32;

        if !filter.uses_index {
            diagnostics.push(
                Diagnostic::hint(
                    code::UNINDEXED_FILTER,
                    &Self::format_msg(msg::FILTERS, "unindexed_filter", &[&filter.name]),
                )
                .with_pipeline(pipeline)
                .with_suggestion(&Self::format_msg(
                    msg::FILTERS,
                    "unindexed_filter_suggestion",
                    &[&filter.columns_referenced.join(", ")],
                )),
            );
        }

        if filter.selectivity.selectivity < selective_threshold {
            diagnostics.push(
                Diagnostic::info(
                    code::HIGHLY_SELECTIVE_FILTER,
                    &Self::format_msg(msg::FILTERS, "highly_selective_filter", &[&filter.name]),
                )
                .with_pipeline(pipeline)
                .with_suggestion(&Self::get_msg(
                    msg::FILTERS,
                    "highly_selective_filter_suggestion",
                )),
            );
        }

        diagnostics
    }

    fn check_joins(pipeline: &str, joins: &[JoinPlan]) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();
        let large_lookup_threshold = Self::get_threshold("large_lookup_rows") as u64;
        let low_match_threshold = Self::get_threshold("low_match_rate_threshold") as f32;
        let many_joins_threshold = Self::get_threshold("many_joins_count") as usize;

        for join in joins {
            if !join.conditions.iter().all(|c| c.indexed) {
                // Collect all unindexed columns
                let unindexed_columns: Vec<String> = join
                    .conditions
                    .iter()
                    .filter(|c| !c.indexed)
                    .map(|c| format!("{}.{}", &join.source_table, &c.right.column))
                    .collect();

                let suggestion = if unindexed_columns.len() == 1 {
                    format!("Add index on {}", unindexed_columns[0])
                } else {
                    format!("Add indexes on: {}", unindexed_columns.join(", "))
                };

                diagnostics.push(
                    Diagnostic::warning(
                        code::UNINDEXED_JOIN,
                        &Self::format_msg(msg::JOINS, "unindexed_join", &[&join.source_table]),
                    )
                    .with_pipeline(pipeline)
                    .with_suggestion(&suggestion),
                );
            }

            if join.table_rows.value > large_lookup_threshold {
                diagnostics.push(
                    Diagnostic::warning(
                        code::LARGE_LOOKUP_TABLE,
                        &Self::format_msg(
                            msg::JOINS,
                            "large_lookup_table",
                            &[&join.source_table, &join.table_rows.display()],
                        ),
                    )
                    .with_pipeline(pipeline)
                    .with_suggestion(&Self::get_msg(msg::JOINS, "large_lookup_table_suggestion")),
                );
            }

            if let Some(rate) = join.match_rate
                && rate < low_match_threshold
            {
                diagnostics.push(
                    Diagnostic::warning(
                        code::LOW_JOIN_MATCH_RATE,
                        &Self::format_msg(
                            msg::JOINS,
                            "low_join_match_rate",
                            &[&join.alias, &format!("{}", (rate * 100.0) as u32)],
                        ),
                    )
                    .with_pipeline(pipeline)
                    .with_suggestion(&Self::get_msg(msg::JOINS, "low_join_match_rate_suggestion")),
                );
            }
        }

        if joins.len() > many_joins_threshold {
            diagnostics.push(
                Diagnostic::warning(
                    code::MANY_JOINS,
                    &Self::format_msg(msg::JOINS, "many_joins", &[&joins.len().to_string()]),
                )
                .with_pipeline(pipeline)
                .with_suggestion(&Self::get_msg(msg::JOINS, "many_joins_suggestion")),
            );
        }

        diagnostics
    }

    fn check_mappings(pipeline: &str, mappings: &[ColumnMapping]) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();
        let many_computed_threshold = Self::get_threshold("many_computed_count") as usize;

        for mapping in mappings {
            if let Some(tc) = &mapping.type_conversion
                && !tc.is_safe
            {
                diagnostics.push(
                    Diagnostic::warning(
                        code::UNSAFE_TYPE_CONVERSION,
                        &Self::format_msg(
                            msg::MAPPINGS,
                            "unsafe_type_conversion",
                            &[&mapping.target, &tc.from_type, &tc.to_type],
                        ),
                    )
                    .with_pipeline(pipeline)
                    .with_suggestion(tc.warning.as_deref().unwrap_or(&Self::get_msg(
                        msg::MAPPINGS,
                        "unsafe_type_conversion_suggestion",
                    ))),
                );
            }
        }

        let computed = mappings
            .iter()
            .filter(|m| {
                matches!(
                    m.mapping_type,
                    MappingType::Computed | MappingType::Conditional
                )
            })
            .count();

        if computed > many_computed_threshold {
            diagnostics.push(
                Diagnostic::info(
                    code::MANY_COMPUTED_COLUMNS,
                    &Self::format_msg(
                        msg::MAPPINGS,
                        "many_computed_columns",
                        &[&computed.to_string()],
                    ),
                )
                .with_pipeline(pipeline)
                .with_suggestion(&Self::get_msg(
                    msg::MAPPINGS,
                    "many_computed_columns_suggestion",
                )),
            );
        }

        diagnostics
    }

    fn check_pagination(
        pipeline: &str,
        source: &SourcePlan,
        pagination: &Option<PaginationPlan>,
    ) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();
        let pagination_threshold = Self::get_threshold("pagination_threshold_rows") as u64;

        match pagination {
            None => {
                if source.effective_row_count().value > pagination_threshold {
                    diagnostics.push(
                        Diagnostic::hint(
                            code::MISSING_PAGINATION,
                            &Self::get_msg(msg::PAGINATION, "missing_pagination"),
                        )
                        .with_pipeline(pipeline)
                        .with_suggestion(&Self::get_msg(
                            msg::PAGINATION,
                            "missing_pagination_suggestion",
                        )),
                    );
                }
            }
            Some(pag) => {
                if pag.column_indexed != Some(true)
                    && let Some(cursor_col) = &pag.cursor_column
                {
                    diagnostics.push(
                        Diagnostic::warning(
                            code::UNINDEXED_CURSOR,
                            &Self::format_msg(
                                msg::PAGINATION,
                                "unindexed_cursor",
                                &[&cursor_col.table, &cursor_col.column],
                            ),
                        )
                        .with_pipeline(pipeline)
                        .with_suggestion(&Self::get_msg(
                            msg::PAGINATION,
                            "unindexed_cursor_suggestion",
                        )),
                    );
                }

                if pag.strategy == PaginationStrategy::Default {
                    diagnostics.push(
                        Diagnostic::warning(
                            code::OFFSET_PAGINATION,
                            &Self::get_msg(msg::PAGINATION, "offset_pagination"),
                        )
                        .with_pipeline(pipeline)
                        .with_suggestion(&Self::get_msg(
                            msg::PAGINATION,
                            "offset_pagination_suggestion",
                        )),
                    );
                }
            }
        }

        diagnostics
    }

    fn check_pipelines(pipelines: &[PipelinePlan]) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();

        for p in pipelines {
            if p.source.total_rows.is_unknown() {
                continue;
            }

            if p.source.effective_row_count().value == 0 && p.source.total_rows.value == 0 {
                diagnostics.push(
                    Diagnostic::info(
                        code::EMPTY_SOURCE,
                        &Self::format_msg(msg::SOURCE, "empty_source", &[&p.name]),
                    )
                    .with_pipeline(&p.name),
                );
            }
        }

        diagnostics
    }

    fn check_resources(
        pipelines: &[PipelinePlan],
        settings: &ExecutionSettings,
    ) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();
        let high_memory_threshold = Self::get_threshold("high_memory_mb") as u64;
        let long_migration_threshold = Self::get_threshold("long_migration_seconds") as u64;
        let very_long_threshold = Self::get_threshold("very_long_migration_seconds") as u64;

        let total_memory: u64 = match settings.strategy {
            ExecutionStrategy::Sequential => pipelines
                .iter()
                .map(|p| p.estimations.memory_mb)
                .max()
                .unwrap_or(0),
            ExecutionStrategy::Parallel => pipelines.iter().map(|p| p.estimations.memory_mb).sum(),
        };

        if total_memory > high_memory_threshold {
            diagnostics.push(
                Diagnostic::warning(
                    code::HIGH_MEMORY_USAGE,
                    &Self::format_msg(
                        msg::RESOURCES,
                        "high_memory_usage",
                        &[&total_memory.to_string()],
                    ),
                )
                .with_suggestion(&Self::get_msg(
                    msg::RESOURCES,
                    "high_memory_usage_suggestion",
                )),
            );
        }

        let total_duration: u64 = pipelines
            .iter()
            .map(|p| p.estimations.duration.likely_seconds)
            .sum();

        if total_duration > long_migration_threshold {
            let hours = total_duration / 3600;
            diagnostics.push(
                Diagnostic::info(
                    code::LONG_MIGRATION,
                    &Self::format_msg(msg::RESOURCES, "long_migration", &[&hours.to_string()]),
                )
                .with_suggestion(&Self::get_msg(msg::RESOURCES, "long_migration_suggestion")),
            );
        }

        if total_duration > very_long_threshold {
            diagnostics.push(
                Diagnostic::warning(
                    code::VERY_LONG_MIGRATION,
                    &Self::get_msg(msg::RESOURCES, "very_long_migration"),
                )
                .with_suggestion(&Self::get_msg(
                    msg::RESOURCES,
                    "very_long_migration_suggestion",
                )),
            );
        }

        diagnostics
    }

    fn check_data_integrity(pipelines: &[PipelinePlan]) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();
        let validation_threshold = Self::get_threshold("validation_threshold_rows") as u64;

        for p in pipelines {
            if p.validations.is_empty()
                && p.source.effective_row_count().value > validation_threshold
            {
                diagnostics.push(
                    Diagnostic::hint(
                        code::NO_VALIDATIONS,
                        &Self::format_msg(msg::INTEGRITY, "no_validations", &[&p.name]),
                    )
                    .with_pipeline(&p.name)
                    .with_suggestion(&Self::get_msg(msg::INTEGRITY, "no_validations_suggestion")),
                );
            }

            if p.error_handling.retry.is_none() && p.error_handling.failed_rows.is_none() {
                diagnostics.push(
                    Diagnostic::hint(
                        code::NO_ERROR_HANDLING,
                        &Self::format_msg(msg::INTEGRITY, "no_error_handling", &[&p.name]),
                    )
                    .with_pipeline(&p.name)
                    .with_suggestion(&Self::get_msg(
                        msg::INTEGRITY,
                        "no_error_handling_suggestion",
                    )),
                );
            }
        }

        diagnostics
    }

    fn get_msg(cat: &str, key: &str) -> String {
        CATALOG.messages.get_message(cat, key)
    }
    fn get_threshold(key: &str) -> f64 {
        CATALOG.thresholds.get_f64(msg::THRESHOLDS, key, 0.0)
    }

    fn format_msg(cat: &str, key: &str, args: &[&str]) -> String {
        let mut template = Self::get_msg(cat, key);
        for arg in args {
            template = template.replacen("{}", arg, 1);
        }
        template
    }
}
