use super::pipeline::{Transform, for_each_table_mut};
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
    /// includes the plugin-transform output columns. A table absent from this
    /// map declares no projection and is copied in full (never pruned).
    per_table: HashMap<String, HashSet<String>>,
}

impl FieldPruner {
    pub fn new(metadata: TransformationMetadata) -> Self {
        // Plugin output columns are kept for every projected table.
        let plugin_only: HashSet<String> = metadata
            .plugin_columns
            .iter()
            .map(|(name, _)| name.to_ascii_lowercase())
            .collect();

        // Collect all unique table names that have projections
        let tables: HashSet<&String> = metadata
            .field_mappings
            .field_renames
            .keys()
            .chain(metadata.field_mappings.computed_fields.keys())
            .collect();

        // Precompute the keep set for every table that has renames or computed
        // fields, lowercasing names once here instead of on every row.
        let mut per_table: HashMap<String, HashSet<String>> = HashMap::new();

        for table in tables {
            let mut keep = plugin_only.clone();

            if let Some(field_renames) = metadata.field_mappings.field_renames.get(table) {
                keep.extend(
                    field_renames
                        .target_to_source
                        .keys()
                        .map(|k| k.to_ascii_lowercase()),
                );
            }

            if let Some(computed_fields) = metadata.field_mappings.computed_fields.get(table) {
                keep.extend(computed_fields.iter().map(|c| c.name.to_ascii_lowercase()));
            }

            per_table.insert(table.clone(), keep);
        }

        Self { per_table }
    }

    /// Kept source positions + pruned output schema for `input`, given the already-resolved keep set.
    fn prune_plan(
        keep_fields: &HashSet<String>,
        input: &Arc<RecordSchema>,
    ) -> (Vec<usize>, Arc<RecordSchema>) {
        let cols = input.columns();
        let mut kept_positions = Vec::with_capacity(cols.len());
        let mut kept_columns = Vec::with_capacity(cols.len());
        let mut lower = String::new(); // reused across columns: one alloc per plan

        for (i, col) in cols.iter().enumerate() {
            let name: &str = col.name.as_ref();

            // Fast-path: check exact match first (which hits if already lowercase).
            // Fallback: lowercase into reused buffer and check again.
            let hit = keep_fields.contains(name) || {
                lower.clear();
                lower.push_str(name);
                lower.make_ascii_lowercase();
                keep_fields.contains(&lower)
            };

            if hit {
                kept_positions.push(i);
                kept_columns.push(SchemaColumn::new(col.name.clone(), col.data_type.clone()));
            }
        }

        (
            kept_positions,
            RecordSchema::new(input.table_arc(), kept_columns),
        )
    }
}

impl Transform for FieldPruner {
    fn kind(&self) -> &'static str {
        "prune"
    }

    fn apply(&self, row: &mut Record) -> Result<(), TransformError> {
        // Single lookup: absence of a keep set *is* the "no projection, copy in
        // full" case (no `select`/named select) - never pruned.
        if let Some(keep_fields) = self.per_table.get(row.table()) {
            let (kept, output) = Self::prune_plan(keep_fields, row.schema());
            row.project(output, &kept);
        }

        Ok(())
    }

    fn apply_batch(&self, rows: &mut [Record], _failures: &mut Vec<(usize, TransformError)>) {
        // A graph/cascade batch mixes tables; prune each per-table run only if
        // that table declares a projection.
        for_each_table_mut(rows, |_offset, run| {
            let Some(first) = run.first() else {
                return;
            };

            // No projection for this table -> copy it in full.
            let Some(keep_fields) = self.per_table.get(first.table()) else {
                return;
            };

            let (kept, output) = Self::prune_plan(keep_fields, first.schema());

            for row in run.iter_mut() {
                row.project(Arc::clone(&output), &kept);
            }
        });
    }
}
