use crate::computed::ComputedField;
use smql::{
    plan::MigrationPlan,
    statements::{
        expr::Expression,
        mapping::{EntityMapping, Mapping},
    },
};
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct FieldMappings {
    /// Maps entity name (table, file, API) to field name mapping.
    pub column_mappings: HashMap<String, NameMap>,

    /// Maps entity name to computed fields that populate new columns.
    pub computed_fields: HashMap<String, Vec<ComputedField>>,
}

#[derive(Clone, Debug)]
pub struct NameMap {
    source_to_target: HashMap<String, String>, // old_name → new_name
    target_to_source: HashMap<String, String>, // new_name → old_name
}

#[derive(Clone, Debug)]
pub struct LookupField {
    pub entity: String, // The entity name (table, file, API) where the lookup is performed
    pub key: String,    // The key used for the lookup (e.g., column name)
    pub target: String, // The target field name in the destination entity
}

#[derive(Clone, Debug)]
pub struct EntityMappingContext {
    pub entity_name_map: NameMap,
    pub field_mappings: FieldMappings,
    pub computed_flat: Vec<ComputedField>,

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

    /// Resolve old → new (default direction)
    pub fn resolve(&self, name: &str) -> String {
        let lower = name.to_ascii_lowercase();
        self.source_to_target
            .get(&lower)
            .cloned()
            .unwrap_or_else(|| name.to_string())
    }

    /// Reverse resolve new → old
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

    pub fn get_field_mappings(mappings: &Vec<EntityMapping>) -> FieldMappings {
        let mut entity_map = FieldMappings::new();
        for mapping in mappings {
            let entity = mapping.entity.clone();
            let mut field_map = HashMap::new();
            let mut computed_fields = Vec::new();

            for mapping in &mapping.mappings {
                match mapping {
                    Mapping::ExpressionToColumn { expression, target } => {
                        match expression {
                            Expression::Identifier(column) => {
                                field_map.insert(column.clone(), target.clone());
                            }
                            Expression::Arithmetic { .. } => {
                                computed_fields.push(ComputedField::new(target, expression));
                            }
                            Expression::FunctionCall { .. } => {
                                computed_fields.push(ComputedField::new(target, expression));
                            }
                            Expression::Lookup { .. } => {
                                computed_fields.push(ComputedField::new(target, expression));
                            }
                            _ => {} // Handle other expression types
                        }
                    }
                    _ => {} // Skip other types of mappings
                }
            }

            // Add the field map and computed fields to the entity map
            entity_map.add_mapping(&entity, field_map);
            entity_map.add_computed(&entity, computed_fields);
        }
        entity_map
    }

    pub fn get_entities_name_map(plan: &MigrationPlan) -> NameMap {
        let mut name_map = HashMap::new();

        for migration in plan.migration.migrations.iter() {
            let source = migration.sources.first().unwrap().clone();
            let target = migration.target.clone();

            name_map.insert(source.to_ascii_lowercase(), target.to_ascii_lowercase());
        }

        for load in plan.loads.iter() {
            name_map.insert(
                load.name.to_ascii_lowercase(),
                load.source.to_ascii_lowercase(),
            );
        }

        NameMap::new(name_map)
    }

    pub fn get_entities_name_map_v2(plan: &smql_v02::plan::MigrationPlan) -> NameMap {
        let mut name_map = HashMap::new();

        for mi in plan.migration.migrate_items.iter() {
            let src = mi
                .source
                .names
                .first()
                .expect("each migrate_item must have at least one source name")
                .to_ascii_lowercase(); // currently supports only one source
            let dst = mi
                .destination
                .names
                .first()
                .expect("each migrate_item must have at least one destination name")
                .to_ascii_lowercase(); // currently supports only one destination

            name_map.insert(src.to_ascii_lowercase(), dst.to_ascii_lowercase());

            if let Some(load) = &mi.load {
                name_map.extend(load.entities.iter().map(|name| {
                    let lower = name.to_ascii_lowercase();
                    (lower.clone(), lower)
                }));
            }
        }

        NameMap::new(name_map)
    }

    pub fn forward_map(&self) -> HashMap<String, String> {
        self.source_to_target.clone()
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.source_to_target.contains_key(key)
    }
}

impl EntityMappingContext {
    pub fn new(plan: &MigrationPlan) -> Self {
        let entity_name_map = NameMap::get_entities_name_map(plan);
        let field_mappings = NameMap::get_field_mappings(&plan.mapping);
        let computed_flat = field_mappings
            .computed_fields
            .values()
            .flat_map(|fields| fields.clone())
            .collect::<Vec<_>>();

        let mut lookups: HashMap<String, Vec<LookupField>> = HashMap::new();
        for computed in &computed_flat {
            if let Expression::Lookup { table, key, .. } = &computed.expression {
                let lf = LookupField {
                    entity: table.clone(),
                    key: key.clone(),
                    target: computed.name.clone(),
                };
                lookups.entry(table.clone()).or_default().push(lf);
            }
        }

        Self {
            entity_name_map,
            field_mappings,
            computed_flat,
            lookups,
        }
    }

    pub fn from_plan(plan: &smql_v02::plan::MigrationPlan) -> Self {
        let entity_name_map = NameMap::get_entities_name_map_v2(plan);
        println!("Entity name map: {:#?}", entity_name_map);

        todo!("Implement from_plan");
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
}

impl Default for FieldMappings {
    fn default() -> Self {
        Self::new()
    }
}
