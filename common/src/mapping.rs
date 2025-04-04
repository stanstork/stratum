use smql::statements::{
    expr::Expression,
    mapping::{Mapping, NamespaceMapping},
    migrate::MigrateBlock,
};
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct NamespaceMap {
    pub namespaces: HashMap<String, NameMap>,
}

#[derive(Clone, Debug)]
pub struct NameMap {
    forward: HashMap<String, String>, // old_name → new_name
    reverse: HashMap<String, String>, // new_name → old_name
}

pub struct FieldMapping;

impl NamespaceMap {
    pub fn new() -> Self {
        Self {
            namespaces: HashMap::new(),
        }
    }

    pub fn add_namespace(&mut self, namespace: String, map: NameMap) {
        self.namespaces.insert(namespace, map);
    }

    pub fn get_namespace(&self, namespace: &str) -> Option<&NameMap> {
        self.namespaces.get(namespace)
    }

    pub fn resolve(&self, namespace: &str, name: &str) -> String {
        if let Some(name_map) = self.namespaces.get(namespace) {
            name_map.resolve(name)
        } else {
            name.to_string()
        }
    }

    pub fn is_empty(&self) -> bool {
        self.namespaces.is_empty()
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
}

impl FieldMapping {
    pub fn extract_field_map(mappings: &Vec<NamespaceMapping>) -> NamespaceMap {
        let mut ns_map = NamespaceMap::new();
        for namespace in mappings {
            let mut field_map = HashMap::new();
            for mapping in &namespace.mappings {
                match mapping {
                    Mapping::ColumnToColumn { source, target } => {
                        field_map.insert(source.clone(), target.clone());
                    }
                    Mapping::ExpressionToColumn { expression, target } => {
                        if let Expression::Identifier(column) = expression {
                            field_map.insert(column.clone(), target.clone());
                        } else {
                            // Handle other expression types
                            // For now, we just ignore them
                        }
                    }
                    _ => {} // Skip other types of mappings
                }
            }
            ns_map.add_namespace(namespace.namespace.clone(), NameMap::new(field_map));
        }
        ns_map
    }

    pub fn extract_name_map(migrate: &MigrateBlock) -> NameMap {
        let mut name_map = HashMap::new();

        for migration in migrate.migrations.iter() {
            let source = migration.sources.first().unwrap().clone();
            let target = migration.target.clone();

            name_map.insert(source.to_ascii_lowercase(), target.to_ascii_lowercase());
        }

        NameMap::new(name_map)
    }
}
