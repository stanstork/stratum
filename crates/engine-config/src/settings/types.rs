use super::value_ext::CanonicalValueMapExt;
use model::core::value::Value;
use std::collections::HashMap;

/// Migration settings structure
#[derive(Debug, Clone)]
pub struct Settings {
    pub skip_pk: bool,
    pub skip_fk: bool,
    pub skip_idx: bool,
    pub skip_seq: bool,
    pub skip_unique: bool,
    pub skip_check: bool,
    pub create_missing_columns: bool,
    pub create_missing_tables: bool,
    pub batch_size: usize,
    pub lanes: usize,
}

impl Settings {
    pub fn from_map(map: &HashMap<String, Value>) -> Settings {
        Settings {
            skip_pk: map.get_bool("skip_pk").unwrap_or(false),
            skip_fk: map.get_bool("skip_fk").unwrap_or(false),
            skip_idx: map.get_bool("skip_idx").unwrap_or(false),
            skip_seq: map.get_bool("skip_seq").unwrap_or(false),
            skip_unique: map.get_bool("skip_unique").unwrap_or(false),
            skip_check: map.get_bool("skip_check").unwrap_or(false),
            create_missing_columns: map.get_bool("create_missing_columns").unwrap_or(false),
            create_missing_tables: map.get_bool("create_missing_tables").unwrap_or(false),
            batch_size: map.get_usize("batch_size").unwrap_or(0),
            lanes: map.get_usize("lanes").unwrap_or(0),
        }
    }
}
