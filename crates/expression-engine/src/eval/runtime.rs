use crate::{context::EvalContext, eval::binary::BinaryOpEvaluator, functions::FunctionRegistry};
use model::{
    core::value::Value,
    execution::expr::{BinaryOp, CompiledExpression},
    records::row::RowData,
    transform::mapping::TransformationMetadata,
};
use tracing::warn;

/// Trait for evaluating compiled expressions with runtime row data
pub trait Evaluator {
    fn evaluate(
        &self,
        row: &RowData,
        mapping: &TransformationMetadata,
        env_getter: fn(&str) -> Option<String>,
    ) -> Option<Value>;
}

impl Evaluator for CompiledExpression {
    fn evaluate(
        &self,
        row: &RowData,
        mapping: &TransformationMetadata,
        env_getter: fn(&str) -> Option<String>,
    ) -> Option<Value> {
        match self {
            CompiledExpression::Identifier(identifier) => row
                .field_values
                .iter()
                .find(|col| col.name.eq_ignore_ascii_case(identifier))
                .map(|col| col.value.clone())
                .unwrap_or(None),

            CompiledExpression::Literal(value) => Some(value.clone()),

            CompiledExpression::Binary { left, op, right } => {
                let left_val = left.evaluate(row, mapping, env_getter)?;
                let right_val = right.evaluate(row, mapping, env_getter)?;
                eval_binary_op(&left_val, &right_val, op)
            }

            CompiledExpression::FunctionCall { name, args } => {
                let evaluated_args: Vec<Value> = args
                    .iter()
                    .map(|arg| arg.evaluate(row, mapping, env_getter))
                    .collect::<Option<Vec<_>>>()?;
                eval_function(name, &evaluated_args, row, mapping, env_getter)
            }

            // DotPath with 2+ segments = cross-entity reference (table.column)
            CompiledExpression::DotPath(segments) if segments.len() >= 2 => {
                let entity = &segments[0];
                let key = &segments[1];

                let mapped = mapping
                    .foreign_fields
                    .get(entity)
                    .and_then(|fields| fields.iter().find(|lk| lk.field.eq_ignore_ascii_case(key)))
                    // Given the CrossEntityReference, find the matching column in the current row
                    .and_then(|lk| {
                        if let Some(target) = &lk.target {
                            row.field_values
                                .iter()
                                .find(|col| col.name.eq_ignore_ascii_case(target))
                                .and_then(|col| col.value.clone())
                        } else {
                            // Lookup target is not specified. Used in function arguments.
                            None
                        }
                    });

                let raw = row
                    .field_values
                    .iter()
                    .find(|col| col.name.eq_ignore_ascii_case(key))
                    .and_then(|col| col.value.clone());

                // If a mapped value is found, return it. Otherwise, return the raw value.
                // Note: When the mapping contains lookups from joined tables, it generates a select with the mapped name.
                // However, if there is no join, no additional fields are included in the select.
                mapped.or(raw).or_else(|| {
                    warn!("Cross-entity reference failed for {}.{}", entity, key);
                    None
                })
            }

            // Single-segment DotPath is just a field reference
            CompiledExpression::DotPath(segments) if segments.len() == 1 => row
                .field_values
                .iter()
                .find(|col| col.name.eq_ignore_ascii_case(&segments[0]))
                .map(|col| col.value.clone())
                .unwrap_or(None),

            CompiledExpression::Unary { operand, .. } => {
                // For now, just evaluate the operand
                // TODO: Handle negation and NOT operations
                operand.evaluate(row, mapping, env_getter)
            }

            CompiledExpression::Grouped(expr) => expr.evaluate(row, mapping, env_getter),

            CompiledExpression::When {
                branches,
                else_expr,
            } => {
                for branch in branches {
                    if let Some(Value::Boolean(true)) =
                        branch.condition.evaluate(row, mapping, env_getter)
                    {
                        return branch.value.evaluate(row, mapping, env_getter);
                    }
                }
                else_expr
                    .as_ref()
                    .and_then(|e| e.evaluate(row, mapping, env_getter))
            }

            CompiledExpression::IsNull(expr) => Some(Value::Boolean(matches!(
                expr.evaluate(row, mapping, env_getter),
                Some(Value::Null) | None
            ))),

            CompiledExpression::IsNotNull(expr) => Some(Value::Boolean(!matches!(
                expr.evaluate(row, mapping, env_getter),
                Some(Value::Null) | None
            ))),

            CompiledExpression::Array(_) => {
                warn!("Array expressions are not yet supported");
                None
            }

            CompiledExpression::DotPath(_) => None, // Empty DotPath
        }
    }
}

/// Evaluates a binary operation (arithmetic, comparison, logical, string ops)
fn eval_binary_op(left: &Value, right: &Value, op: &BinaryOp) -> Option<Value> {
    BinaryOpEvaluator::new(left, right, op).evaluate()
}

fn eval_function(
    name: &str,
    args: &[Value],
    row: &RowData,
    mapping: &TransformationMetadata,
    env_getter: fn(&str) -> Option<String>,
) -> Option<Value> {
    let registry = FunctionRegistry::new();
    let ctx = EvalContext::Runtime {
        row_data: row,
        mapping,
        env_getter,
    };

    match registry.call(name, args, &ctx) {
        Ok(value) => Some(value),
        Err(e) => {
            warn!("Function evaluation failed for '{}': {}", name, e);
            None
        }
    }
}
