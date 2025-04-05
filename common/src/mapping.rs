use smql::statements::{
    expr::Expression,
    mapping::{Mapping, ScopeMapping},
    migrate::MigrateBlock,
};
use std::collections::HashMap;

use crate::computed::ComputedField;

#[derive(Clone, Debug)]
pub struct ScopedNameMap {
    pub scopes: HashMap<String, FieldNameMap>,
    pub computed: HashMap<String, Vec<ComputedField>>,
}

#[derive(Clone, Debug)]
pub struct FieldNameMap {
    forward: HashMap<String, String>, // old_name → new_name
    reverse: HashMap<String, String>, // new_name → old_name
}

pub struct FieldMapping;

impl ScopedNameMap {
    pub fn new() -> Self {
        Self {
            scopes: HashMap::new(),
            computed: HashMap::new(),
        }
    }

    pub fn add_mapping(&mut self, scope: &str, map: HashMap<String, String>) {
        self.scopes
            .insert(scope.to_string(), FieldNameMap::new(map));
    }

    pub fn add_computed(&mut self, scope: &str, computed: Vec<ComputedField>) {
        self.computed.insert(scope.to_string(), computed);
    }

    pub fn get_scope(&self, scope: &str) -> Option<&FieldNameMap> {
        self.scopes.get(scope)
    }

    pub fn get_computed(&self, scope: &str) -> Option<&Vec<ComputedField>> {
        self.computed.get(scope)
    }

    pub fn resolve(&self, scope: &str, name: &str) -> String {
        if let Some(name_map) = self.scopes.get(scope) {
            name_map.resolve(name)
        } else {
            name.to_string()
        }
    }

    pub fn is_empty(&self) -> bool {
        self.scopes.is_empty()
    }
}

impl FieldNameMap {
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
}

impl FieldMapping {
    pub fn extract_field_map(mappings: &Vec<ScopeMapping>) -> ScopedNameMap {
        let mut scope_map = ScopedNameMap::new();
        for scopes in mappings {
            let scope = scopes.scope.clone();
            let mut field_map = HashMap::new();
            let mut computed_fields = Vec::new();

            for mapping in &scopes.mappings {
                match mapping {
                    Mapping::ExpressionToColumn { expression, target } => {
                        match expression {
                            Expression::Identifier(column) => {
                                field_map.insert(column.clone(), target.clone());
                            }
                            Expression::Arithmetic { .. } => {
                                computed_fields.push(ComputedField::new(target, expression));
                            }
                            _ => {} // Handle other expression types
                        }
                    }
                    _ => {} // Skip other types of mappings
                }
            }

            // Add the field map and computed fields to the scope map
            scope_map.add_mapping(&scope, field_map);
            scope_map.add_computed(&scope, computed_fields);
        }
        scope_map
    }

    pub fn extract_name_map(migrate: &MigrateBlock) -> FieldNameMap {
        let mut name_map = HashMap::new();

        for migration in migrate.migrations.iter() {
            let source = migration.sources.first().unwrap().clone();
            let target = migration.target.clone();

            name_map.insert(source.to_ascii_lowercase(), target.to_ascii_lowercase());
        }

        FieldNameMap::new(name_map)
    }
}
