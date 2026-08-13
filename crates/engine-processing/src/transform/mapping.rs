use super::pipeline::{Transform, for_each_table};
use crate::transform::error::TransformError;
use model::{
    records::{Record, RecordSchema},
    transform::mapping::{FieldTransformations, NameResolver},
};
use std::borrow::Cow;
use std::sync::Arc;

/// Apply a batch-wide schema swap.
fn set_batch_schema(
    rows: &mut [Record],
    input: &Arc<RecordSchema>,
    new_schema: &Arc<RecordSchema>,
    mut per_row: impl FnMut(&mut Record),
) {
    for row in rows.iter_mut() {
        if Arc::ptr_eq(row.schema(), input) {
            row.set_schema(Arc::clone(new_schema));
        } else {
            per_row(row);
        }
    }
}

pub struct FieldMapper {
    ns_map: FieldTransformations,
}

pub struct TableMapper {
    name_map: NameResolver,
}

impl FieldMapper {
    pub fn new(ns_map: FieldTransformations) -> Self {
        Self { ns_map }
    }

    /// Derive the renamed schema for `input`.
    fn rename_schema(&self, input: &Arc<RecordSchema>) -> Arc<RecordSchema> {
        let table = input.table();

        input.remapped(|name| match self.ns_map.resolve_cow(table, name) {
            Cow::Owned(n) => Some(Arc::from(n.as_str())),
            Cow::Borrowed(_) => None,
        })
    }
}

impl TableMapper {
    pub fn new(name_map: NameResolver) -> Self {
        Self { name_map }
    }

    /// Derive the re-tabled schema for `input` (once per batch).
    fn retable_schema(&self, input: &Arc<RecordSchema>) -> Arc<RecordSchema> {
        let new_table = self.name_map.resolve(input.table());

        if new_table.eq_ignore_ascii_case(input.table()) {
            Arc::clone(input)
        } else {
            input.with_table(new_table)
        }
    }
}

impl Transform for FieldMapper {
    fn kind(&self) -> &'static str {
        "field-rename"
    }

    fn apply(&self, row: &mut Record) -> Result<(), TransformError> {
        if self.ns_map.contains(row.table()) {
            let renamed = self.rename_schema(row.schema());
            row.set_schema(renamed);
        }
        Ok(())
    }

    fn apply_batch(&self, rows: &mut [Record], _failures: &mut Vec<(usize, TransformError)>) {
        // A graph/cascade batch mixes tables; rename each per-table run on its own.
        for_each_table(rows, |_offset, run| {
            let Some(first) = run.first() else {
                return;
            };

            // No renames for this table -> nothing to do for this run.
            if !self.ns_map.contains(first.table()) {
                return;
            }

            let input = Arc::clone(first.schema());
            let renamed = self.rename_schema(&input);

            set_batch_schema(run, &input, &renamed, |row| {
                let _ = self.apply(row);
            });
        });
    }
}

impl Transform for TableMapper {
    fn kind(&self) -> &'static str {
        "table-map"
    }

    fn apply(&self, row: &mut Record) -> Result<(), TransformError> {
        let retabled = self.retable_schema(row.schema());
        row.set_schema(retabled);
        Ok(())
    }

    fn apply_batch(&self, rows: &mut [Record], _failures: &mut Vec<(usize, TransformError)>) {
        // A graph/cascade batch mixes source tables; re-table each per-table run.
        for_each_table(rows, |_offset, run| {
            let Some(first) = run.first() else {
                return;
            };

            let input = Arc::clone(first.schema());
            let retabled = self.retable_schema(&input);

            set_batch_schema(run, &input, &retabled, |row| {
                let _ = self.apply(row);
            });
        });
    }
}
