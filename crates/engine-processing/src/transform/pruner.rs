use super::pipeline::Transform;
use crate::transform::error::TransformError;
use model::{records::Record, transform::mapping::TransformationMetadata};
use std::collections::HashSet;

/// Prunes unmapped columns from rows when copy_columns = MAP_ONLY.
pub struct FieldPruner {
    metadata: TransformationMetadata,
}

impl FieldPruner {
    pub fn new(metadata: TransformationMetadata) -> Self {
        Self { metadata }
    }
}

impl Transform for FieldPruner {
    fn apply(&self, row: &mut Record) -> Result<(), TransformError> {
        let table = row.schema.clone();
        let mut keep_fields = HashSet::new();

        // Add all target fields from field renames (mapped fields)
        if let Some(field_renames) = self.metadata.field_mappings.field_renames.get(&table) {
            for target_field in field_renames.target_to_source.keys() {
                keep_fields.insert(target_field.to_ascii_lowercase());
            }
        }

        // Add all computed field names
        if let Some(computed_fields) = self.metadata.field_mappings.computed_fields.get(&table) {
            for computed in computed_fields {
                keep_fields.insert(computed.name.to_ascii_lowercase());
            }
        }

        // Filter the row to only keep the allowed fields
        row.fields
            .retain(|field| keep_fields.contains(&field.name.to_ascii_lowercase()));

        Ok(())
    }
}
