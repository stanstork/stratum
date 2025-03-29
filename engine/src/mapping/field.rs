use smql::statements::{expr::Expression, mapping::Mapping, migrate::Migrate};
use std::collections::HashMap;

pub struct FieldMapping;

impl FieldMapping {
    pub fn extract_field_map(mappings: &Vec<Mapping>) -> HashMap<String, String> {
        let mut col_mappings = HashMap::new();
        for mapping in mappings {
            match mapping {
                Mapping::ColumnToColumn { source, target } => {
                    col_mappings.insert(source.clone(), target.clone());
                }
                Mapping::ExpressionToColumn { expression, target } => {
                    if let Expression::Identifier(column) = expression {
                        col_mappings.insert(column.clone(), target.clone());
                    } else {
                        // Handle other expression types
                        // For now, we just ignore them
                    }
                }
                _ => {} // Skip other types of mappings
            }
        }
        col_mappings
    }

    pub fn extract_name_map(migrate: &Migrate) -> HashMap<String, String> {
        let mut name_map = HashMap::new();

        let source = migrate.source.first().unwrap();
        let target = migrate.target.clone();

        name_map.insert(source.clone(), target.clone());
        name_map
    }
}
