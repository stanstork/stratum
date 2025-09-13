use std::collections::{HashMap, HashSet};

use common::mapping::{EntityMapping, LookupField};
use serde::Serialize;
use smql::statements::setting::Settings;

use crate::expr::expr_to_string;

#[derive(Serialize, Debug, Default, Clone)]
pub struct MappingReport {
    pub totals: MappingTotals,
    pub entities: Vec<EntityMappingReport>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub lookups: Vec<LookupMappingReport>,
    pub mapping_hash: Option<String>,
}

#[derive(Serialize, Debug, Default, Clone)]
pub struct MappingTotals {
    pub entities: usize,
    pub mapped_fields: usize,
    pub computed_fields: usize,
    pub lookup_count: usize,
}

#[derive(Serialize, Debug, Default, Clone)]
pub struct EntityMappingReport {
    pub source_entity: String,
    pub dest_entity: String,
    pub copy_policy: String,
    pub mapped_fields: usize,
    pub created_fields: usize,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub renames: Vec<FieldRename>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub omitted_source_columns: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub computed: Vec<ComputedPreview>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

#[derive(Serialize, Debug, Default, Clone)]
pub struct FieldRename {
    pub from: String,
    pub to: String,
}

#[derive(Serialize, Debug, Default, Clone)]
pub struct ComputedPreview {
    pub name: String,
    pub expression_preview: String,
}

#[derive(Serialize, Debug, Default, Clone)]
pub struct LookupMappingReport {
    pub source_entity: String,
    pub entity: String,
    pub key: String,
    pub target: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

impl MappingReport {
    pub fn from_mapping(mapping: &EntityMapping, settings: &Settings) -> Self {
        let mut total_mapped_fields = 0;
        let mut total_computed_fields = 0;

        let entities: Vec<EntityMappingReport> = mapping
            .entity_name_map
            .source_to_target
            .iter()
            .map(|(source_entity, dest_entity)| {
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

                let mut warnings: Vec<String> = Vec::new();
                if let Some(nm) = rename_map {
                    if has_duplicate_values(&nm.source_to_target) {
                        warnings.push("Duplicate target column in renames".into());
                    }
                }
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

                total_mapped_fields += renames.len();
                total_computed_fields += computed_prev.len();

                EntityMappingReport {
                    source_entity: source_entity.clone(),
                    dest_entity: dest_entity.clone(),
                    copy_policy: settings.copy_columns.to_string(),
                    mapped_fields: renames.len(),
                    created_fields: computed_prev.len(),
                    renames,
                    omitted_source_columns: Vec::new(),
                    computed: computed_prev,
                    warnings,
                }
            })
            .collect();

        let lookup_reports = mapping
            .lookups
            .iter()
            .flat_map(|(source_entity, lookups)| {
                lookups.iter().map(move |l| LookupMappingReport {
                    source_entity: source_entity.clone(),
                    entity: l.entity.clone(),
                    key: l.key.clone(),
                    target: l.target.clone(),
                    warnings: lookup_warnings(mapping, source_entity, l),
                })
            })
            .collect::<Vec<_>>();

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
            mapping_hash: Some(compute_mapping_hash(mapping)),
        }
    }
}

fn has_duplicate_values(map: &HashMap<String, String>) -> bool {
    let mut seen = HashSet::with_capacity(map.len());
    map.values().any(|v| !seen.insert(v))
}

fn lookup_warnings(
    mapping: &EntityMapping,
    source_entity_key: &str,
    l: &LookupField,
) -> Vec<String> {
    let mut w = Vec::new();
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
    #[derive(Serialize)]
    struct Minimal<'a> {
        entities: Vec<(&'a String, &'a String)>,
        renames: Vec<(&'a String, Vec<(&'a String, &'a String)>)>,
        computed: Vec<(&'a String, Vec<&'a String>)>,
        lookup_targets: Vec<(&'a String, Vec<&'a String>)>,
    }

    let mut entities: Vec<_> = mapping.entity_name_map.source_to_target.iter().collect();
    entities.sort_by_key(|a| a.0);

    let mut renames: Vec<_> = mapping
        .field_mappings
        .column_mappings
        .iter()
        .map(|(dest, nm)| {
            let mut pairs: Vec<_> = nm.source_to_target.iter().collect();
            pairs.sort_by_key(|a| a.0);
            (dest, pairs)
        })
        .collect();
    renames.sort_by_key(|a| a.0);

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
    computed.sort_by_key(|a| a.0);

    let mut lookup_targets: Vec<_> = mapping
        .lookups
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

    let json = serde_json::to_vec(&minimal).unwrap_or_default();
    let hash = md5::compute(&json);
    format!("{:x}", hash)
}
