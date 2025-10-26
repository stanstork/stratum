use common::{mapping::EntityMapping, row_data::RowData, value::Value};
use smql::statements::expr::{Expression, Literal, Operator};
use tracing::warn;

pub trait Evaluator {
    fn evaluate(&self, row: &RowData, mapping: &EntityMapping) -> Option<Value>;
}

impl Evaluator for Expression {
    fn evaluate(&self, row: &RowData, mapping: &EntityMapping) -> Option<Value> {
        match self {
            Expression::Identifier(identifier) => row
                .field_values
                .iter()
                .find(|col| col.name.eq_ignore_ascii_case(identifier))
                .map(|col| col.value.clone())
                .unwrap_or(None),

            Expression::Literal(literal) => Some(match literal {
                Literal::String(s) => Value::String(s.clone()),
                Literal::Integer(i) => Value::Int(*i),
                Literal::Float(f) => Value::Float(*f),
                Literal::Boolean(b) => Value::Boolean(*b),
            }),

            Expression::Arithmetic {
                left,
                operator,
                right,
            } => {
                let left_val = left.evaluate(row, mapping)?;
                let right_val = right.evaluate(row, mapping)?;
                eval_arithmetic(&left_val, &right_val, operator)
            }

            Expression::FunctionCall { name, arguments } => {
                let evaluated_args: Vec<Value> = arguments
                    .iter()
                    .map(|arg| arg.evaluate(row, mapping))
                    .collect::<Option<Vec<_>>>()?;
                eval_function(name, &evaluated_args)
            }

            Expression::Lookup { entity, key, .. } => {
                let mapped = mapping
                    .lookups
                    .get(entity)
                    .and_then(|fields| fields.iter().find(|lk| lk.key.eq_ignore_ascii_case(key)))
                    // Given the LookupField, find the matching column in the current row
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
                    warn!("Lookup failed for {}[{}]", entity, key);
                    None
                })
            }
        }
    }
}

fn eval_arithmetic(left: &Value, right: &Value, op: &Operator) -> Option<Value> {
    use Value::*;

    let as_float = |v: &Value| match v {
        Int(i) => Some(*i as f64),
        Float(f) => Some(*f),
        _ => None,
    };

    match (left, right) {
        (Int(l), Int(r)) => Some(match op {
            Operator::Add => Int(l + r),
            Operator::Subtract => Int(l - r),
            Operator::Multiply => Int(l * r),
            Operator::Divide => Int(l / r),
        }),

        (Int(_), Float(_)) | (Float(_), Int(_)) | (Float(_), Float(_)) => {
            let l = as_float(left)?;
            let r = as_float(right)?;
            Some(match op {
                Operator::Add => Float(l + r),
                Operator::Subtract => Float(l - r),
                Operator::Multiply => Float(l * r),
                Operator::Divide => Float(l / r),
            })
        }

        _ => None,
    }
}

fn eval_function(name: &str, args: &[Value]) -> Option<Value> {
    match name.to_ascii_lowercase().as_str() {
        "lower" => match args.first()? {
            Value::String(s) => Some(Value::String(s.to_lowercase())),
            _ => None,
        },
        "upper" => match args.first()? {
            Value::String(s) => Some(Value::String(s.to_uppercase())),
            _ => None,
        },
        "concat" => {
            let concatenated = args
                .iter()
                .map(|arg| match arg {
                    Value::String(s) => s
                        .trim_start_matches('\"')
                        .trim_end_matches('\"')
                        .to_string(),
                    Value::Int(i) => i.to_string(),
                    Value::Uint(u) => u.to_string(),
                    Value::Usize(u) => u.to_string(),
                    Value::Float(f) => f.to_string(),
                    Value::Boolean(b) => b.to_string(),
                    Value::Uuid(u) => u.to_string(),
                    Value::Date(d) => d.to_string(),
                    Value::Timestamp(t) => t.to_rfc3339(),
                    Value::Bytes(b) => String::from_utf8_lossy(b).to_string(),
                    Value::Json(v) => v.to_string(),
                    Value::Null => "NULL".to_string(),
                    Value::Enum(_, v) => v.clone(),
                    Value::StringArray(v) => format!("{v:?}"),
                })
                .collect::<Vec<_>>()
                .join("");
            Some(Value::String(concatenated))
        }

        _ => {
            warn!("Unsupported function: {}", name);
            None
        }
    }
}
