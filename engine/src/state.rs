use crate::{
    destination::{data::DataDestination, Destination},
    expr::expr_to_string,
    report::validation::{
        ComputedPreview, DryRunReport, EndpointType, EntityMappingReport, FieldRename,
        LookupMappingReport, MappingReport, MappingTotals,
    },
    source::{data::DataSource, Source},
};
use common::mapping::{EntityMapping, LookupField};
use serde::Serialize;
use smql::statements::setting::{CopyColumns, Settings};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
pub struct MigrationState {
    pub batch_size: usize,
    pub ignore_constraints: bool,
    pub infer_schema: bool,
    pub create_missing_columns: bool,
    pub create_missing_tables: bool,
    pub cascade_schema: bool,
    pub copy_columns: CopyColumns,
    pub is_dry_run: bool,
    pub dry_run_report: Arc<Mutex<DryRunReport>>,
}

impl MigrationState {
    pub async fn new(
        settings: &Settings,
        source: &Source,
        dest: &Destination,
        mapping: &EntityMapping,
        config_hash: String,
        dry_run: bool,
    ) -> Self {
        let mut state = Self::from_settings(settings);
        state.is_dry_run = dry_run;
        state.dry_run_report = Arc::new(Mutex::new(
            Self::create_report(source, dest, mapping, &config_hash).await,
        ));
        state
    }

    pub fn from_settings(settings: &Settings) -> Self {
        MigrationState {
            batch_size: settings.batch_size,
            ignore_constraints: settings.ignore_constraints,
            infer_schema: settings.infer_schema,
            create_missing_columns: settings.create_missing_columns,
            create_missing_tables: settings.create_missing_tables,
            cascade_schema: settings.cascade_schema,
            copy_columns: settings.copy_columns.clone(),
            is_dry_run: false,
            dry_run_report: Arc::new(Mutex::new(DryRunReport::default())),
        }
    }

    pub fn mark_validation_run(&mut self) {
        self.is_dry_run = true;
    }

    pub fn dry_run_report(&mut self) -> Arc<Mutex<DryRunReport>> {
        Arc::clone(&self.dry_run_report)
    }

    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    pub async fn create_report(
        source: &Source,
        dest: &Destination,
        mapping: &EntityMapping,
        config_hash: &str,
    ) -> DryRunReport {
        let source_endpoint = match &source.primary {
            DataSource::Database(_) => EndpointType::Database {
                dialect: source.dialect().name(),
            },
            DataSource::File(_) => EndpointType::File {
                format: source.format().to_string(),
            },
        };

        let dest_endpoint = match &dest.data_dest {
            DataDestination::Database(_) => EndpointType::Database {
                dialect: dest.dialect().name(),
            },
        };

        let mut report = DryRunReport::default();

        report.run_id = uuid::Uuid::new_v4().to_string();
        report.config_hash = config_hash.to_string();
        report.engine_version = env!("CARGO_PKG_VERSION").to_string();
        report.summary.source = source_endpoint;
        report.summary.destination = dest_endpoint;
        report.summary.timestamp = chrono::Utc::now();
        // report.mapping = build_mapping_report(mapping);

        report.mapping = build_mapping_report(mapping);

        report
    }
}

fn build_mapping_report(mapping: &EntityMapping) -> MappingReport {
    let mut entities: Vec<EntityMappingReport> = Vec::new();
    let mut total_mapped_fields = 0usize;
    let mut total_computed_fields = 0usize;

    for (source_entity, dest_entity) in &mapping.entity_name_map.source_to_target {
        let rename_map = mapping.field_mappings.column_mappings.get(dest_entity);
        let computed = mapping
            .field_mappings
            .computed_fields
            .get(dest_entity)
            .map(|v| v.as_slice())
            .unwrap_or(&[]);

        let renames: Vec<FieldRename> = rename_map
            .map(|nm| {
                nm.source_to_target
                    .iter()
                    .map(|(from, to)| FieldRename {
                        from: from.clone(),
                        to: to.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default();

        let computed_prev: Vec<ComputedPreview> = computed
            .iter()
            .map(|c| ComputedPreview {
                name: c.name.clone(),
                expression_preview: expr_to_string(&c.expression)
                    .unwrap_or_else(|_| "<complex expression>".to_string()),
            })
            .collect();

        let mapped_fields = renames.len();
        let created_fields = computed_prev.len();

        let mut warnings: Vec<String> = Vec::new();

        // collision: two renames mapping to the same target
        if let Some(nm) = rename_map {
            if has_duplicate_values(&nm.source_to_target) {
                warnings.push("Duplicate target column in renames".into());
            }
        }

        // collision: computed field name equals a rename target
        if !computed_prev.is_empty() && rename_map.is_some() {
            let targets = rename_map
                .unwrap()
                .source_to_target
                .values()
                .cloned()
                .collect::<HashSet<_>>();
            for c in &computed_prev {
                if targets.contains(&c.name) {
                    warnings.push(format!(
                        "Computed field '{}' overwrites a renamed column",
                        c.name
                    ));
                }
            }
        }

        entities.push(EntityMappingReport {
            source_entity: source_entity.clone(),
            dest_entity: dest_entity.clone(),
            copy_policy: "ALL".to_string(), // TODO: set based on mapping settings
            mapped_fields,
            created_fields,
            renames,
            omitted_source_columns: Vec::new(), // fill later when we have settings
            computed: computed_prev,
            coercions: Vec::new(), // fill later wehn we have types
            warnings,
        });
    }

    let mut lookup_reports = Vec::new();
    let mut lookup_count = 0usize;

    for (source_entity, lookups) in &mapping.lookups {
        for l in lookups {
            lookup_reports.push(LookupMappingReport {
                source_entity: source_entity.clone(),
                entity: l.entity.clone(),
                key: l.key.clone(),
                target: l.target.clone(),
                warnings: lookup_warnings(mapping, source_entity, l),
            });
            lookup_count += 1;
        }
    }

    let totals = MappingTotals {
        entities: entities.len(),
        mapped_fields: total_mapped_fields,
        computed_fields: total_computed_fields,
        lookup_count,
    };
    let mapping_hash = Some(compute_mapping_hash(mapping));

    MappingReport {
        totals,
        entities,
        lookups: lookup_reports,
        mapping_hash,
    }
}

fn has_duplicate_values(map: &HashMap<String, String>) -> bool {
    use std::collections::HashSet;
    let mut seen = HashSet::new();
    for v in map.values() {
        if !seen.insert(v) {
            return true;
        }
    }
    false
}

fn lookup_warnings(
    mapping: &EntityMapping,
    source_entity_key: &str,
    l: &LookupField,
) -> Vec<String> {
    let mut w = Vec::new();

    // warn if lookup entity is not mapped to any destination entity name map
    if !mapping
        .entity_name_map
        .source_to_target
        .contains_key(&l.entity)
        && !mapping
            .entity_name_map
            .target_to_source
            .contains_key(&l.entity)
    {
        w.push(format!(
            "Lookup entity '{}' is not present in entity_name_map",
            l.entity
        ));
    }

    // warn if target might collide with a rename on its DEST entity
    if let Some(dest_entity) = mapping
        .entity_name_map
        .source_to_target
        .get(source_entity_key)
    {
        if let Some(nm) = mapping.field_mappings.column_mappings.get(dest_entity) {
            if nm.source_to_target.values().any(|t| t == &l.target) {
                w.push(format!(
                    "Lookup target '{}' collides with a renamed column in '{}'",
                    l.target, dest_entity
                ));
            }
        }
    }
    w
}

fn compute_mapping_hash(mapping: &EntityMapping) -> String {
    // Hash a stable, minimal view (entity pairs + field renames + computed names)
    #[derive(Serialize)]
    struct Minimal<'a> {
        entities: Vec<(&'a String, &'a String)>,
        renames: Vec<(&'a String, Vec<(&'a String, &'a String)>)>, // dest_entity -> (from,to)
        computed: Vec<(&'a String, Vec<&'a String>)>,              // dest_entity -> [name]
        lookup_targets: Vec<(&'a String, Vec<&'a String>)>,        // source_entity -> [target]
    }

    let mut entities: Vec<_> = mapping.entity_name_map.source_to_target.iter().collect();
    entities.sort_by(|a, b| a.0.cmp(b.0));

    let mut renames: Vec<_> = mapping
        .field_mappings
        .column_mappings
        .iter()
        .map(|(dest, nm)| {
            let mut pairs: Vec<_> = nm.source_to_target.iter().collect();
            pairs.sort_by(|a, b| a.0.cmp(b.0));
            (dest, pairs)
        })
        .collect();
    renames.sort_by(|a, b| a.0.cmp(b.0));

    let mut computed: Vec<_> = mapping
        .field_mappings
        .computed_fields
        .iter()
        .map(|(dest, v)| {
            let mut names: Vec<_> = v.iter().map(|c| &c.name).collect();
            names.sort();
            (dest, names)
        })
        .collect();
    computed.sort_by(|a, b| a.0.cmp(b.0));

    let mut lookup_targets: Vec<_> = mapping
        .lookups
        .iter()
        .map(|(src, v)| {
            let mut names: Vec<_> = v.iter().map(|l| &l.target).collect();
            names.sort();
            (src, names)
        })
        .collect();
    lookup_targets.sort_by(|a, b| a.0.cmp(b.0));

    let minimal = Minimal {
        entities,
        renames,
        computed,
        lookup_targets,
    };

    let json = serde_json::to_vec(&minimal).unwrap_or_default();
    let hash = md5::compute(&json);
    format!("{:x}", hash)
}
