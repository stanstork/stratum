use super::pipeline::{Transform, for_each_table_mut};
use crate::transform::error::TransformError;
use engine_core::context::env::EnvContext;
use expression_engine::{EvalContext, Evaluator, Program, TreeExpr, infer_expression_type};
use model::{
    core::{types::Type, value::Value},
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

/// A computed field compiled either to the bytecode stack machine (fast path) or,
/// when the VM declines to lower it, to the `TreeExpr` tree-walk.
enum Compiled {
    Vm(Program),
    Tree(TreeExpr),
}

impl ComputedTransform {
    pub fn new(mapping: TransformationMetadata, env: Arc<EnvContext>) -> Self {
        Self { mapping, env }
    }
}

impl Transform for ComputedTransform {
    fn kind(&self) -> &'static str {
        "computed"
    }

    fn apply(&self, row: &mut Record) -> Result<(), TransformError> {
        let computed_fields = match self.mapping.field_mappings.computed_fields.get(row.table()) {
            Some(fields) if !fields.is_empty() => fields,
            _ => return Ok(()),
        };

        let env_getter = |key: &str| self.env.get(key);
        let mut values = Vec::with_capacity(computed_fields.len());

        // Evaluate every computed expression against the (input) row.
        for computed in computed_fields {
            let val = computed
                .expression
                .evaluate(row, &self.mapping, &env_getter)
                .ok_or_else(|| {
                    TransformError::Transformation(format!(
                        "Failed to evaluate computed column `{}` in `{}`",
                        computed.name,
                        row.table()
                    ))
                })?;
            values.push(val);
        }

        let input = Arc::clone(row.schema());
        let plan = build_plan(&input, computed_fields, &values);

        apply_plan(row, &mut values, &plan);

        Ok(())
    }

    fn apply_batch(&self, rows: &mut [Record], failures: &mut Vec<(usize, TransformError)>) {
        for_each_table_mut(rows, |offset, run| {
            let Some(first) = run.first() else {
                return;
            };

            let computed_fields = match self
                .mapping
                .field_mappings
                .computed_fields
                .get(first.table())
            {
                Some(fields) if !fields.is_empty() => fields,
                _ => return,
            };

            let input_schema = Arc::clone(first.schema());
            let table = input_schema.table();

            // compile: bind each expression to the schema (once per table run).
            // Prefer the bytecode VM; fall back to the tree-walk for anything it declines to lower.
            let compiled: Vec<Compiled> = computed_fields
                .iter()
                .map(|c| {
                    match Program::compile(&c.expression, &input_schema, &self.mapping, table) {
                        Some(p) => Compiled::Vm(p),
                        None => Compiled::Tree(TreeExpr::compile(
                            &c.expression,
                            &input_schema,
                            &self.mapping,
                            table,
                        )),
                    }
                })
                .collect();

            let env_getter = |key: &str| self.env.get(key);
            let mut scratch: Vec<Value> = Vec::with_capacity(computed_fields.len());
            let mut plan: Option<Arc<ComputedPlan>> = None;

            for (i, row) in run.iter_mut().enumerate() {
                scratch.clear();

                // One runtime context per row, shared by every VM field.
                let ctx = EvalContext::Runtime {
                    row_data: row,
                    mapping: &self.mapping,
                    env_getter: &env_getter,
                };

                let mut success = true;

                for (compiled, computed) in compiled.iter().zip(computed_fields) {
                    let value = match compiled {
                        Compiled::Vm(p) => p.eval(row, &ctx),
                        Compiled::Tree(e) => e.eval(row, &self.mapping, &env_getter),
                    };

                    match value {
                        Some(v) => scratch.push(v.into_owned()),
                        None => {
                            failures.push((
                                offset + i,
                                TransformError::Transformation(format!(
                                    "Failed to evaluate computed column `{}` in `{}`",
                                    computed.name,
                                    row.table()
                                )),
                            ));
                            success = false;
                            break;
                        }
                    }
                }

                if !success {
                    continue;
                }

                // Build the plan on the first row of the run (all rows share a schema);
                // re-derive only if the schema changes.
                if plan
                    .as_ref()
                    .is_none_or(|p| !Arc::ptr_eq(&p.input, row.schema()))
                {
                    plan = Some(build_plan(row.schema(), computed_fields, &scratch));
                }

                if let Some(p) = &plan {
                    apply_plan(row, &mut scratch, p);
                }
            }
        });
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

    // Resolve a column name to its type from the input schema, for static
    // inference of appended columns.
    let column_lookup = |name: &str| -> Option<Type> {
        input
            .index_of(name)
            .and_then(|i| input.column(i))
            .map(|c| c.data_type.clone())
    };

    for (field, val) in computed_fields.iter().zip(values) {
        match input.index_of(&field.name) {
            Some(i) => slots.push(Slot::Overwrite(i)),
            None => {
                slots.push(Slot::Append);
                let data_type = infer_expression_type(&field.expression, &column_lookup)
                    .unwrap_or_else(|| val.data_type());
                output = output.with_appended(SchemaColumn::new(field.name.as_str(), data_type));
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
