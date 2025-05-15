use common::mapping::EntityMapping;
use smql::statements::expr::{Expression, Literal, Operator};
use sql_adapter::{metadata::column::value::ColumnValue, row::row_data::RowData};
use tracing::warn;

pub trait Evaluator {
    fn evaluate(&self, row: &RowData, mapping: &EntityMapping) -> Option<ColumnValue>;
}

impl Evaluator for Expression {
    fn evaluate(&self, row: &RowData, mapping: &EntityMapping) -> Option<ColumnValue> {
        match self {
            Expression::Identifier(identifier) => row
                .columns
                .iter()
                .find(|col| col.name.eq_ignore_ascii_case(identifier))
                .map(|col| col.value.clone())
                .unwrap_or(None),

            Expression::Literal(literal) => Some(match literal {
                Literal::String(s) => ColumnValue::String(s.clone()),
                Literal::Integer(i) => ColumnValue::Int(*i),
                Literal::Float(f) => ColumnValue::Float(*f),
                Literal::Boolean(b) => ColumnValue::Boolean(*b),
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
                let evaluated_args: Vec<ColumnValue> = arguments
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
                        row.columns
                            .iter()
                            .find(|col| col.name.eq_ignore_ascii_case(&lk.target))
                            .and_then(|col| col.value.clone())
                    });

                let raw = row
                    .columns
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

fn eval_arithmetic(left: &ColumnValue, right: &ColumnValue, op: &Operator) -> Option<ColumnValue> {
    use ColumnValue::*;

    let as_float = |v: &ColumnValue| match v {
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

fn eval_function(name: &str, args: &[ColumnValue]) -> Option<ColumnValue> {
    match name.to_ascii_lowercase().as_str() {
        "lower" => match args.first()? {
            ColumnValue::String(s) => Some(ColumnValue::String(s.to_lowercase())),
            _ => None,
        },
        "upper" => match args.first()? {
            ColumnValue::String(s) => Some(ColumnValue::String(s.to_uppercase())),
            _ => None,
        },
        "concat" => {
            let concatenated = args
                .iter()
                .map(|arg| match arg {
                    ColumnValue::String(s) => s
                        .trim_start_matches('\"')
                        .trim_end_matches('\"')
                        .to_string(),
                    ColumnValue::Int(i) => i.to_string(),
                    ColumnValue::Float(f) => f.to_string(),
                    ColumnValue::Boolean(b) => b.to_string(),
                    ColumnValue::Uuid(u) => u.to_string(),
                    ColumnValue::Date(d) => d.to_string(),
                    ColumnValue::Timestamp(t) => t.to_rfc3339(),
                    ColumnValue::Bytes(b) => String::from_utf8_lossy(b).to_string(),
                    ColumnValue::Json(v) => v.to_string(),
                })
                .collect::<Vec<_>>()
                .join("");
            Some(ColumnValue::String(concatenated))
        }

        _ => {
            warn!("Unsupported function: {}", name);
            None
        }
    }
}
