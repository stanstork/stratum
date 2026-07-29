use super::value_ext::CanonicalValueMapExt;
use model::core::value::Value;
use std::collections::HashMap;

/// Migration settings structure
#[derive(Debug, Clone)]
pub struct Settings {
    pub skip_primary_keys: bool,
    pub skip_foreign_keys: bool,
    pub skip_indexes: bool,
    pub create_missing_columns: bool,
    pub create_missing_tables: bool,
    pub batch_size: usize,
    pub lanes: usize,
}

impl Settings {
    pub fn from_map(map: &HashMap<String, Value>) -> Settings {
        Settings {
            skip_primary_keys: map.get_bool("skip_primary_keys").unwrap_or(false),
            skip_foreign_keys: map.get_bool("skip_foreign_keys").unwrap_or(false),
            skip_indexes: map.get_bool("skip_indexes").unwrap_or(false),
            create_missing_columns: map.get_bool("create_missing_columns").unwrap_or(false),
            create_missing_tables: map.get_bool("create_missing_tables").unwrap_or(false),
            batch_size: map.get_usize("batch_size").unwrap_or(0),
            lanes: map.get_usize("lanes").unwrap_or(0),
        }
    }
}
