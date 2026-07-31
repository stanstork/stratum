use super::pipeline::{Transform, for_each_table};
use crate::transform::error::TransformError;
use model::{
    records::{Record, RecordSchema, SchemaColumn},
    transform::mapping::TransformationMetadata,
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Prunes unmapped columns from rows when the pipeline declares a `select`
/// projection (only the projected columns are migrated).
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

    /// Kept source positions + pruned output schema for `input`. Identical for
    /// every row of a batch, so the caller derives it once and reuses it.
    fn prune_plan(&self, input: &Arc<RecordSchema>) -> (Arc<Vec<usize>>, Arc<RecordSchema>) {
        let keep_fields = self
            .per_table
            .get(input.table())
            .unwrap_or(&self.plugin_only);
        let mut kept_positions = Vec::new();
        let mut kept_columns = Vec::new();

        for (i, col) in input.columns().iter().enumerate() {
            if keep_fields.contains(col.name.as_ref())
                || keep_fields.contains(&col.name.to_ascii_lowercase())
            {
                kept_positions.push(i);
                kept_columns.push(SchemaColumn::new(col.name.clone(), col.data_type.clone()));
            }
        }

        let kept = Arc::new(kept_positions);
        let out = RecordSchema::new(input.table_arc(), kept_columns);

        (kept, out)
    }
}

impl Transform for FieldPruner {
    fn apply(&self, row: &mut Record) -> Result<(), TransformError> {
        // A table without a projection (no `select`/named select) is copied in full - never pruned.
        if !self.per_table.contains_key(row.table()) {
            return Ok(());
        }

        let input = Arc::clone(row.schema());
        let (kept, output) = self.prune_plan(&input);

        row.project(output, &kept);

        Ok(())
    }

    fn apply_batch(&self, rows: &mut [Record], _failures: &mut Vec<(usize, TransformError)>) {
        // A graph/cascade batch mixes tables; prune each per-table run only if
        // that table declares a projection.
        for_each_table(rows, |_offset, run| {
            let Some(first) = run.first() else {
                return;
            };

            // No projection for this table -> copy it in full.
            if !self.per_table.contains_key(first.table()) {
                return;
            }

            let input = Arc::clone(first.schema());
            let (kept, output) = self.prune_plan(&input);

            for row in run.iter_mut() {
                row.project(Arc::clone(&output), &kept);
            }
        });
    }
}
