use model::transform::mapping::{CrossEntityReference, NameResolver, TransformationMetadata};
use serde::Serialize;
use crate::settings::CopyColumns;
use std::collections::{HashMap, HashSet};

/// A detailed report on the entity and field mappings.
#[derive(Serialize, Debug, Default, Clone)]
pub struct MappingReport {
    pub totals: MappingTotals,
    pub entities: Vec<EntityMappingReport>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub lookups: Vec<LookupMappingReport>,
    pub mapping_hash: Option<String>,
}

/// A summary of counts across all entity mappings.
#[derive(Serialize, Debug, Default, Clone)]
pub struct MappingTotals {
    pub entities: usize,
    pub mapped_fields: usize,
    pub computed_fields: usize,
    pub lookup_count: usize,
}

/// A report on the mapping for a single entity (e.g., a table).
#[derive(Serialize, Debug, Default, Clone)]
pub struct EntityMappingReport {
    pub source_entity: String,
    pub dest_entity: String,
    pub copy_policy: String,
    pub mapped_fields: usize,
    pub created_fields: usize,
    // Columns mapped 1:1 without rename or expression.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub one_to_one: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub renames: Vec<FieldRename>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub omitted_source_columns: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub computed: Vec<ComputedPreview>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

/// Represents the renaming of a single field.
#[derive(Serialize, Debug, Default, Clone)]
pub struct FieldRename {
    pub from: String,
    pub to: String,
}

/// A preview of a computed field.
#[derive(Serialize, Debug, Default, Clone)]
pub struct ComputedPreview {
    pub name: String,
    pub expression_preview: String,
}

/// A report on a single lookup mapping.
#[derive(Serialize, Debug, Default, Clone)]
pub struct LookupMappingReport {
    pub source_entity: String,
    pub entity: String,
    pub key: String,
    pub target: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

impl MappingReport {
    /// Creates a `MappingReport` from a given `TransformationMetadata`.
    pub fn from_mapping(meta: &TransformationMetadata, copy_columns: &CopyColumns) -> Self {
        let (entities, total_mapped_fields, total_computed_fields) =
            Self::process_entity_reports(meta, copy_columns);
        let lookup_reports = Self::process_lookup_reports(meta);

        let totals = MappingTotals {
            entities: entities.len(),
            mapped_fields: total_mapped_fields,
            computed_fields: total_computed_fields,
            lookup_count: lookup_reports.len(),
        };

        Self {
            totals,
            entities,
            lookups: lookup_reports,
            mapping_hash: Some(compute_mapping_hash(meta)),
        }
    }

    fn process_entity_reports(
        meta: &TransformationMetadata,
        copy_columns: &CopyColumns,
    ) -> (Vec<EntityMappingReport>, usize, usize) {
        let mut total_mapped = 0;
        let mut total_computed = 0;

        let reports = meta
            .entities
            .source_to_target
            .iter()
            .map(|(source, dest)| {
                let report = Self::create_single_entity_report(meta, source, dest, copy_columns);
                total_mapped += report.mapped_fields;
                total_computed += report.created_fields;
                report
            })
            .collect();

        (reports, total_mapped, total_computed)
    }

    fn create_single_entity_report(
        meta: &TransformationMetadata,
        source_entity: &str,
        dest_entity: &str,
        copy_columns: &CopyColumns,
    ) -> EntityMappingReport {
        let rename_map = meta.field_mappings.field_renames.get(dest_entity);
        let computed_fields = meta
            .field_mappings
            .computed_fields
            .get(dest_entity)
            .map_or(&[][..], |v| v.as_slice());

        let one_to_one = rename_map
            .map(|nm| {
                nm.source_to_target
                    .iter()
                    .filter(|(s, t)| s == t) // One-to-one mappings
                    .map(|(col, _)| col.clone())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        let renames: Vec<FieldRename> = rename_map
            .map(|nm| {
                nm.source_to_target
                    .iter()
                    .filter(|(s, t)| s != t) // Exclude one-to-one mappings
                    .map(|(from, to)| FieldRename {
                        from: from.clone(),
                        to: to.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default();

        let computed = computed_fields
            .iter()
            .map(|c| ComputedPreview {
                name: c.name.clone(),
                expression_preview: format!("{:?}", c.expression),
            })
            .collect::<Vec<_>>();

        let warnings = Self::collect_entity_warnings(rename_map, &computed);

        EntityMappingReport {
            source_entity: source_entity.to_string(),
            dest_entity: dest_entity.to_string(),
            copy_policy: copy_columns.to_string(),
            mapped_fields: renames.len(),
            created_fields: computed.len(),
            one_to_one,
            renames,
            omitted_source_columns: Vec::new(), // Will be filled in later steps
            computed,
            warnings,
        }
    }

    fn collect_entity_warnings(
        rename_map: Option<&NameResolver>,
        computed: &[ComputedPreview],
    ) -> Vec<String> {
        let mut warnings = Vec::new();

        if let Some(nm) = rename_map {
            if has_duplicate_values(&nm.source_to_target) {
                warnings.push("Duplicate target column in renames".into());
            }

            if !computed.is_empty() {
                let targets: HashSet<_> = nm.source_to_target.values().cloned().collect();
                for c in computed {
                    if targets.contains(&c.name) {
                        warnings.push(format!(
                            "Computed field '{}' overwrites a renamed column",
                            c.name
                        ));
                    }
                }
            }
        }
        warnings
    }

    fn process_lookup_reports(mapping: &TransformationMetadata) -> Vec<LookupMappingReport> {
        mapping
            .foreign_fields
            .iter()
            .flat_map(|(source_entity, lookups)| {
                lookups.iter().map(move |l| LookupMappingReport {
                    source_entity: source_entity.clone(),
                    entity: l.entity.clone(),
                    key: l.field.clone(),
                    target: l.target.clone(),
                    warnings: lookup_warnings(mapping, source_entity, l),
                })
            })
            .collect()
    }
}

fn has_duplicate_values(map: &HashMap<String, String>) -> bool {
    let mut seen = HashSet::with_capacity(map.len());
    map.values().any(|v| !seen.insert(v))
}

fn lookup_warnings(
    meta: &TransformationMetadata,
    source_entity_key: &str,
    l: &CrossEntityReference,
) -> Vec<String> {
    let mut w = Vec::new();
    if !meta.entities.source_to_target.contains_key(&l.entity)
        && !meta.entities.target_to_source.contains_key(&l.entity)
    {
        w.push(format!(
            "Lookup entity '{}' is not present in entity_name_map",
            l.entity
        ));
    }

    if let Some(dest_entity) = meta.entities.source_to_target.get(source_entity_key)
        && let Some(nm) = meta.field_mappings.field_renames.get(dest_entity)
        && let Some(target) = &l.target
        && nm.source_to_target.values().any(|t| t == target)
    {
        w.push(format!(
            "Lookup target '{target}' collides with a renamed column in '{dest_entity}'"
        ));
    }
    w
}

fn compute_mapping_hash(meta: &TransformationMetadata) -> String {
    #[derive(Serialize)]
    struct Minimal<'a> {
        entities: Vec<(&'a String, &'a String)>,
        renames: Vec<(&'a String, Vec<(&'a String, &'a String)>)>,
        computed: Vec<(&'a String, Vec<&'a String>)>,
        lookup_targets: Vec<(&'a String, Vec<&'a Option<String>>)>,
    }

    let mut entities: Vec<_> = meta.entities.source_to_target.iter().collect();
    entities.sort_by_key(|a| a.0);

    let mut renames: Vec<_> = meta
        .field_mappings
        .field_renames
        .iter()
        .map(|(dest, nm)| {
            let mut pairs: Vec<_> = nm.source_to_target.iter().collect();
            pairs.sort_by_key(|a| a.0);
            (dest, pairs)
        })
        .collect();
    renames.sort_by_key(|a| a.0);

    let mut computed: Vec<_> = meta
        .field_mappings
        .computed_fields
        .iter()
        .map(|(dest, v)| {
            let mut names: Vec<_> = v.iter().map(|c| &c.name).collect();
            names.sort();
            (dest, names)
        })
        .collect();
    computed.sort_by_key(|a| a.0);

    let mut lookup_targets: Vec<_> = meta
        .foreign_fields
        .iter()
        .map(|(src, v)| {
            let mut names: Vec<_> = v.iter().map(|l| &l.target).collect();
            names.sort();
            (src, names)
        })
        .collect();
    lookup_targets.sort_by_key(|a| a.0);

    let minimal = Minimal {
        entities,
        renames,
        computed,
        lookup_targets,
    };

    let json = serde_json::to_vec(&minimal).expect("Failed to serialize mapping for hashing.");
    let hash = md5::compute(&json);
    format!("{hash:x}")
}
