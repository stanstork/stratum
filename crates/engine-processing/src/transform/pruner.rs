use super::pipeline::Transform;
use model::{records::row::RowData, transform::mapping::TransformationMetadata};

/// Prunes unmapped columns from rows when copy_columns = MAP_ONLY.
/// Keeps only fields that are explicitly mapped or computed.
pub struct FieldPruner {
    mapping: TransformationMetadata,
}

impl FieldPruner {
    pub fn new(mapping: TransformationMetadata) -> Self {
        Self { mapping }
    }
}

impl Transform for FieldPruner {
    fn apply(&self, row: &RowData) -> RowData {
        let mut row = row.clone();
        let table = row.entity.clone();

        eprintln!("=== FieldPruner for table '{}', row has {} fields before pruning", table, row.field_values.len());

        // Get the list of fields that should be kept
        let mut keep_fields = std::collections::HashSet::new();

        // Add all target fields from field renames (mapped fields)
        if let Some(field_renames) = self.mapping.field_mappings.field_renames.get(&table) {
            eprintln!("=== Found {} field renames", field_renames.source_to_target.len());
            for target_field in field_renames.source_to_target.keys() {
                eprintln!("=== Keeping mapped field: {}", target_field);
                keep_fields.insert(target_field.to_ascii_lowercase());
            }
        }

        // Add all computed field names
        if let Some(computed_fields) = self.mapping.field_mappings.computed_fields.get(&table) {
            eprintln!("=== Found {} computed fields", computed_fields.len());
            for computed in computed_fields {
                eprintln!("=== Keeping computed field: {}", computed.name);
                keep_fields.insert(computed.name.to_ascii_lowercase());
            }
        }

        eprintln!("=== Total fields to keep: {}", keep_fields.len());

        // Filter the row to only keep the allowed fields
        row.field_values.retain(|field| {
            let keep = keep_fields.contains(&field.name.to_ascii_lowercase());
            if !keep {
                eprintln!("=== Pruning field: {}", field.name);
            }
            keep
        });

        eprintln!("=== After pruning, row has {} fields", row.field_values.len());
        row
    }
}
