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
pub struct EntityFieldsMap {
    pub entities: HashMap<String, NameMap>,
    pub computed: HashMap<String, Vec<ComputedField>>,
}

#[derive(Clone, Debug)]
pub struct NameMap {
    forward: HashMap<String, String>, // old_name → new_name
    reverse: HashMap<String, String>, // new_name → old_name
}

impl EntityFieldsMap {
    pub fn new() -> Self {
        Self {
            entities: HashMap::new(),
            computed: HashMap::new(),
        }
    }

    pub fn add_mapping(&mut self, entity: &str, map: HashMap<String, String>) {
        self.entities.insert(entity.to_string(), NameMap::new(map));
    }

    pub fn add_computed(&mut self, entity: &str, computed: Vec<ComputedField>) {
        self.computed.insert(entity.to_string(), computed);
    }

    pub fn get_entity(&self, entity: &str) -> Option<&NameMap> {
        self.entities.get(entity)
    }

    pub fn get_computed(&self, entity: &str) -> Option<&Vec<ComputedField>> {
        self.computed.get(entity)
    }

    pub fn resolve(&self, entity: &str, name: &str) -> String {
        if let Some(name_map) = self.entities.get(entity) {
            name_map.resolve(name)
        } else {
            name.to_string()
        }
    }

    pub fn is_empty(&self) -> bool {
        self.entities.is_empty()
    }

    pub fn contains(&self, entity: &str) -> bool {
        self.entities.contains_key(entity)
    }
}

impl NameMap {
    pub fn new(map: HashMap<String, String>) -> Self {
        let mut forward = HashMap::new();
        let mut reverse = HashMap::new();

        for (k, v) in map.into_iter() {
            let k_lower = k.to_ascii_lowercase();
            let v_lower = v.to_ascii_lowercase();

            forward.insert(k_lower.clone(), v_lower.clone());
            reverse.insert(v_lower, k_lower);
        }

        Self { forward, reverse }
    }

    /// Resolve old → new (default direction)
    pub fn resolve(&self, name: &str) -> String {
        let lower = name.to_ascii_lowercase();
        self.forward
            .get(&lower)
            .cloned()
            .unwrap_or_else(|| name.to_string())
    }

    /// Reverse resolve new → old
    pub fn reverse_resolve(&self, name: &str) -> String {
        let lower = name.to_ascii_lowercase();
        self.reverse
            .get(&lower)
            .cloned()
            .unwrap_or_else(|| name.to_string())
    }

    pub fn is_empty(&self) -> bool {
        self.forward.is_empty() && self.reverse.is_empty()
    }

    pub fn extract_field_map(mappings: &Vec<EntityMapping>) -> EntityFieldsMap {
        let mut entity_map = EntityFieldsMap::new();
        for entities in mappings {
            let entity = entities.entity.clone();
            let mut field_map = HashMap::new();
            let mut computed_fields = Vec::new();

            for mapping in &entities.mappings {
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

    pub fn extract_name_map(plan: &MigrationPlan) -> NameMap {
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

    pub fn forward_map(&self) -> HashMap<String, String> {
        self.forward.clone()
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.forward.contains_key(key)
    }
}
