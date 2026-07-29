use super::pipeline::Transform;
use crate::transform::error::TransformError;
use engine_core::context::env::EnvContext;
use expression_engine::{Evaluator, PreparedExpr};
use model::{
    core::value::Value,
    records::{Record, RecordSchema, SchemaColumn},
    transform::{computed_field::ComputedField, mapping::TransformationMetadata},
};
use std::sync::Arc;

pub struct ComputedTransform {
    mapping: TransformationMetadata,
    env: Arc<EnvContext>,
}

/// Where each computed field's value goes, and the resulting output schema.
/// Derived once per batch (all rows share one input schema).
struct ComputedPlan {
    input: Arc<RecordSchema>,
    output: Arc<RecordSchema>,
    slots: Vec<Slot>,
}

enum Slot {
    /// Overwrite an existing column at this position.
    Overwrite(usize),
    /// Append as a new column (in slot order).
    Append,
}

impl ComputedTransform {
    pub fn new(mapping: TransformationMetadata, env: Arc<EnvContext>) -> Self {
        Self { mapping, env }
    }
}

impl Transform for ComputedTransform {
    fn apply(&self, row: &mut Record) -> Result<(), TransformError> {
        let Some(computed_fields) = self.mapping.field_mappings.computed_fields.get(row.table())
        else {
            return Ok(());
        };

        if computed_fields.is_empty() {
            return Ok(());
        }

        let env = &self.env;
        let env_getter = |key: &str| env.get(key);

        // Evaluate every computed expression against the (input) row.
        let mut values = Vec::with_capacity(computed_fields.len());

        for computed in computed_fields {
            match computed
                .expression
                .evaluate(row, &self.mapping, &env_getter)
            {
                Some(v) => values.push(v),
                None => {
                    return Err(TransformError::Transformation(format!(
                        "Failed to evaluate computed column `{}` in `{}`",
                        computed.name,
                        row.table()
                    )));
                }
            }
        }

        let input = Arc::clone(row.schema());
        let plan = build_plan(&input, computed_fields, &values);

        apply_plan(row, &mut values, &plan);

        Ok(())
    }

    fn apply_batch(&self, rows: &mut [Record], failures: &mut Vec<(usize, TransformError)>) {
        let Some(first) = rows.first() else {
            return;
        };

        let Some(computed_fields) = self
            .mapping
            .field_mappings
            .computed_fields
            .get(first.table())
        else {
            return;
        };

        if computed_fields.is_empty() {
            return;
        }

        let input_schema = Arc::clone(first.schema());
        let table = input_schema.table();

        let prepared: Vec<PreparedExpr> = computed_fields
            .iter()
            .map(|c| PreparedExpr::compile(&c.expression, &input_schema, &self.mapping, table))
            .collect();

        let env = &self.env;
        let env_getter = |key: &str| env.get(key);

        let mut scratch: Vec<Value> = Vec::with_capacity(computed_fields.len());
        let mut plan: Option<Arc<ComputedPlan>> = None;

        for (i, row) in rows.iter_mut().enumerate() {
            scratch.clear();

            // Evaluate every computed expression against this row.
            let mut evaluated = true;

            for (expr, computed) in prepared.iter().zip(computed_fields) {
                match expr.eval(row, &self.mapping, &env_getter) {
                    Some(v) => scratch.push(v),
                    None => {
                        failures.push((
                            i,
                            TransformError::Transformation(format!(
                                "Failed to evaluate computed column `{}` in `{}`",
                                computed.name,
                                row.table()
                            )),
                        ));
                        evaluated = false;
                        break;
                    }
                }
            }
            if !evaluated {
                continue;
            }

            // Build the plan on the first row (or if the schema changes mid-batch,
            // which shouldn't happen but is handled for safety).
            let need_plan = match &plan {
                Some(p) => !Arc::ptr_eq(&p.input, row.schema()),
                None => true,
            };

            if need_plan {
                plan = Some(build_plan(row.schema(), computed_fields, &scratch));
            }

            apply_plan(row, &mut scratch, plan.as_ref().unwrap());
        }
    }
}

/// Derive the output schema + per-field slots for `input`. Needs one row's
/// computed `values` to type appended columns.
fn build_plan(
    input: &Arc<RecordSchema>,
    computed_fields: &[ComputedField],
    values: &[Value],
) -> Arc<ComputedPlan> {
    let mut slots = Vec::with_capacity(computed_fields.len());
    let mut output = Arc::clone(input);

    for (field, val) in computed_fields.iter().zip(values) {
        match input.index_of(&field.name) {
            Some(i) => slots.push(Slot::Overwrite(i)),
            None => {
                slots.push(Slot::Append);
                output =
                    output.with_appended(SchemaColumn::new(field.name.as_str(), val.data_type()));
            }
        }
    }

    Arc::new(ComputedPlan {
        input: Arc::clone(input),
        output,
        slots,
    })
}

/// Write the computed `values` into `row` per the plan's slots, then stamp the
/// row with the plan's shared output schema.
fn apply_plan(row: &mut Record, values: &mut Vec<Value>, plan: &ComputedPlan) {
    // Overwrite existing columns in place; append new ones in slot order so the
    // values stay aligned with `plan.output`.
    for (value, slot) in values.drain(..).zip(&plan.slots) {
        match slot {
            Slot::Overwrite(i) => row.set_value_at(*i, Some(value)),
            Slot::Append => row.push_value(Some(value)),
        }
    }

    row.set_schema(Arc::clone(&plan.output));
}
