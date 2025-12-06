use crate::transform::computed_field::ComputedField;
use smql_syntax::ast_v2::{expr::Expression, migrate::MigrateItem};
use std::collections::HashMap;

#[derive(Default, Clone, Debug)]
pub struct FieldMappings {
    /// Maps entity name (table, file, API) to field name mapping.
    pub column_mappings: HashMap<String, NameMap>,

    /// Maps entity name to computed fields that populate new columns.
    pub computed_fields: HashMap<String, Vec<ComputedField>>,
}

#[derive(Clone, Debug, Default)]
pub struct NameMap {
    pub source_to_target: HashMap<String, String>, // old_name -> new_name
    pub target_to_source: HashMap<String, String>, // new_name -> old_name
}

#[derive(Clone, Debug)]
pub struct LookupField {
    /// The entity name (table, file, API) where the lookup is performed.
    pub entity: String,
    /// The key used for the lookup (e.g., column name).
    pub key: String,
    /// The target field name in the destination entity.
    pub target: Option<String>,
}

#[derive(Clone, Debug)]
pub struct EntityMapping {
    /// Maps each source entity name to its corresponding
    /// destination entity name.
    pub entity_name_map: NameMap,

    /// For each destination entity:
    /// - a `HashMap` of simple field renames (`source_field` -> `target_field`)
    /// - a list of `ComputedField`s for any expressions or lookups.
    pub field_mappings: FieldMappings,

    /// Lookup fields grouped by by their source_entity.
    pub lookups: HashMap<String, Vec<LookupField>>,
}

impl FieldMappings {
    pub fn new() -> Self {
        Self {
            column_mappings: HashMap::new(),
            computed_fields: HashMap::new(),
        }
    }

    pub fn add_mapping(&mut self, entity: &str, map: HashMap<String, String>) {
        self.column_mappings
            .insert(entity.to_string(), NameMap::new(map));
    }

    pub fn add_computed(&mut self, entity: &str, computed: Vec<ComputedField>) {
        self.computed_fields.insert(entity.to_string(), computed);
    }

    pub fn get_entity(&self, entity: &str) -> Option<&NameMap> {
        self.column_mappings.get(entity)
    }

    pub fn get_computed(&self, entity: &str) -> Option<&Vec<ComputedField>> {
        self.computed_fields.get(entity)
    }

    pub fn resolve(&self, entity: &str, name: &str) -> String {
        if let Some(name_map) = self.column_mappings.get(entity) {
            name_map.resolve(name)
        } else {
            name.to_string()
        }
    }

    pub fn is_empty(&self) -> bool {
        self.column_mappings.is_empty()
    }

    pub fn contains(&self, entity: &str) -> bool {
        self.column_mappings.contains_key(entity)
    }
}

impl NameMap {
    pub fn new(map: HashMap<String, String>) -> Self {
        let mut source_to_target = HashMap::new();
        let mut target_to_source = HashMap::new();

        for (k, v) in map.into_iter() {
            let k_lower = k.to_ascii_lowercase();
            let v_lower = v.to_ascii_lowercase();

            source_to_target.insert(k_lower.clone(), v_lower.clone());
            target_to_source.insert(v_lower, k_lower);
        }

        Self {
            source_to_target,
            target_to_source,
        }
    }

    /// Resolve old -> new (default direction)
    pub fn resolve(&self, name: &str) -> String {
        let lower = name.to_ascii_lowercase();
        self.source_to_target
            .get(&lower)
            .cloned()
            .unwrap_or_else(|| name.to_string())
    }

    /// Reverse resolve new -> old
    pub fn reverse_resolve(&self, name: &str) -> String {
        let lower = name.to_ascii_lowercase();
        self.target_to_source
            .get(&lower)
            .cloned()
            .unwrap_or_else(|| name.to_string())
    }

    pub fn is_empty(&self) -> bool {
        self.source_to_target.is_empty() && self.target_to_source.is_empty()
    }

    pub fn get_field_mappings(mi: &MigrateItem) -> FieldMappings {
        let mut entity_map = FieldMappings::new();

        let entity = mi
            .destination
            .names
            .first()
            .expect("MigrateItem must have destination name")
            .to_ascii_lowercase();

        let mut field_map = HashMap::new();
        let mut computed_fields = Vec::new();

        if let Some(map_spec) = &mi.map {
            for mapping in &map_spec.mappings {
                match &mapping.source {
                    // direct identifier -> simple rename
                    Expression::Identifier(field) => {
                        field_map.insert(
                            field.to_ascii_lowercase(),
                            mapping.target.to_ascii_lowercase(),
                        );
                    }
                    // everything else is a computed field
                    other_expr => {
                        computed_fields.push(ComputedField::new(&mapping.target, other_expr));
                    }
                }
            }
        }

        entity_map.add_mapping(&entity, field_map);
        entity_map.add_computed(&entity, computed_fields);

        entity_map
    }

    pub fn get_entities_name_map(mi: &MigrateItem) -> NameMap {
        let mut name_map = HashMap::new();

        let src = mi
            .source
            .names
            .first()
            .expect("MigrateItem must have at least one source name")
            .to_ascii_lowercase(); // currently supports only one source
        let dst = mi
            .destination
            .names
            .first()
            .expect("MigrateItem must have destination name")
            .to_ascii_lowercase();

        name_map.insert(src.to_ascii_lowercase(), dst.to_ascii_lowercase());

        if let Some(load) = &mi.load {
            name_map.extend(load.entities.iter().map(|name| {
                let lower = name.to_ascii_lowercase();
                (lower.clone(), lower)
            }));
        }

        NameMap::new(name_map)
    }

    pub fn forward_map(&self) -> HashMap<String, String> {
        self.source_to_target.clone()
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.source_to_target.contains_key(key)
    }

    pub fn contains_target_key(&self, key: &str) -> bool {
        self.target_to_source.contains_key(key)
    }
}

impl EntityMapping {
    pub fn new(mi: &MigrateItem) -> Self {
        let entity_name_map = NameMap::get_entities_name_map(mi);
        let field_mappings = NameMap::get_field_mappings(mi);
        let lookups = Self::get_lookups(&field_mappings);

        Self {
            entity_name_map,
            field_mappings,
            lookups,
        }
    }

    pub fn get_field_name_map(&self) -> &NameMap {
        &self.entity_name_map
    }

    pub fn get_field_mappings(&self) -> &FieldMappings {
        &self.field_mappings
    }

    pub fn get_computed_fields(&self, entity: &str) -> Option<&Vec<ComputedField>> {
        self.field_mappings.get_computed(entity)
    }

    pub fn get_lookups_for(&self, entity: &str) -> &[LookupField] {
        self.lookups.get(entity).map(Vec::as_slice).unwrap_or(&[])
    }

    /// Build a map from “entity name” → all lookups that reference it.
    fn get_lookups(field_mappings: &FieldMappings) -> HashMap<String, Vec<LookupField>> {
        let mut lookups: HashMap<String, Vec<LookupField>> = HashMap::new();

        for computed_list in field_mappings.computed_fields.values() {
            for computed in computed_list {
                // collect *all* lookups inside this computed field
                let mut found = Vec::new();
                Self::extract_lookups(
                    &computed.expression,
                    &Some(computed.name.clone()),
                    &mut found,
                );

                // group them by entity
                for lf in found {
                    lookups.entry(lf.entity.clone()).or_default().push(lf);
                }
            }
        }

        lookups
    }

    /// Walks `expr` and pushes every `Lookup` it finds into `out`.
    fn extract_lookups(expr: &Expression, target: &Option<String>, out: &mut Vec<LookupField>) {
        match expr {
            Expression::Lookup { entity, key, .. } => {
                out.push(LookupField {
                    entity: entity.clone(),
                    key: key.clone(),
                    target: target.clone(),
                });
            }

            Expression::Arithmetic { left, right, .. } => {
                Self::extract_lookups(left, target, out);
                Self::extract_lookups(right, target, out);
            }

            Expression::FunctionCall { arguments, .. } => {
                for arg in arguments {
                    Self::extract_lookups(arg, &None, out);
                }
            }

            // Identifiers and literals never contain nested lookups:
            Expression::Identifier(_) | Expression::Literal(_) => {}
        }
    }
}
