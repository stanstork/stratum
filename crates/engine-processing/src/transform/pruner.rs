use super::pipeline::Transform;
use crate::transform::error::TransformError;
use model::{records::Record, transform::mapping::TransformationMetadata};
use std::collections::{HashMap, HashSet};

/// Prunes unmapped columns from rows when copy_columns = MAP_ONLY.
pub struct FieldPruner {
    /// Pre-lowercased set of fields to keep, per table. Each set already
    /// includes the plugin-transform output columns.
    per_table: HashMap<String, HashSet<String>>,
    /// Keep set for tables without explicit renames/computed fields: the
    /// plugin output columns only.
    plugin_only: HashSet<String>,
}

impl FieldPruner {
    pub fn new(metadata: TransformationMetadata) -> Self {
        let plugin_only: HashSet<String> = metadata
            .plugin_columns
            .iter()
            .map(|(name, _)| name.to_ascii_lowercase())
            .collect();

        // Precompute the keep set for every table that has renames or computed
        // fields, lowercasing names once here instead of on every row.
        let mut per_table: HashMap<String, HashSet<String>> = HashMap::new();
        let tables = metadata
            .field_mappings
            .field_renames
            .keys()
            .chain(metadata.field_mappings.computed_fields.keys());

        for table in tables {
            if per_table.contains_key(table) {
                continue;
            }
            let mut keep = plugin_only.clone();

            if let Some(field_renames) = metadata.field_mappings.field_renames.get(table) {
                for target_field in field_renames.target_to_source.keys() {
                    keep.insert(target_field.to_ascii_lowercase());
                }
            }
            if let Some(computed_fields) = metadata.field_mappings.computed_fields.get(table) {
                for computed in computed_fields {
                    keep.insert(computed.name.to_ascii_lowercase());
                }
            }

            per_table.insert(table.clone(), keep);
        }

        Self {
            per_table,
            plugin_only,
        }
    }
}

impl Transform for FieldPruner {
    fn apply(&self, row: &mut Record) -> Result<(), TransformError> {
        let keep_fields = self.per_table.get(&row.schema).unwrap_or(&self.plugin_only);

        // Filter the row to only keep the allowed fields.
        row.fields.retain(|field| {
            keep_fields.contains(field.name.as_str())
                || keep_fields.contains(&field.name.to_ascii_lowercase())
        });

        Ok(())
    }
}
