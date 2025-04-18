use smql::statements::expr::{Expression, Literal, Operator};
use sql_adapter::{metadata::column::value::ColumnValue, row::row_data::RowData};

pub trait Evaluator {
    fn evaluate(&self, row: &RowData) -> Option<ColumnValue>;
}

impl Evaluator for Expression {
    fn evaluate(&self, row: &RowData) -> Option<ColumnValue> {
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
                let left_val = left.evaluate(row)?;
                let right_val = right.evaluate(row)?;
                eval_arithmetic(&left_val, &right_val, operator)
            }

            Expression::FunctionCall { name, arguments } => {
                let evaluated_args: Vec<ColumnValue> = arguments
                    .iter()
                    .map(|arg| arg.evaluate(row))
                    .collect::<Option<Vec<_>>>()?;
                eval_function(name, &evaluated_args)
            }

            _ => {
                eprintln!("Unsupported expression type: {:?}", self);
                None
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
            _ => return None,
        }),

        (Int(_), Float(_)) | (Float(_), Int(_)) | (Float(_), Float(_)) => {
            let l = as_float(left)?;
            let r = as_float(right)?;
            Some(match op {
                Operator::Add => Float(l + r),
                Operator::Subtract => Float(l - r),
                Operator::Multiply => Float(l * r),
                Operator::Divide => Float(l / r),
                _ => return None,
            })
        }

        _ => None,
    }
}

fn eval_function(name: &str, args: &Vec<ColumnValue>) -> Option<ColumnValue> {
    match name.to_ascii_lowercase().as_str() {
        "lower" => match args.get(0)? {
            ColumnValue::String(s) => Some(ColumnValue::String(s.to_lowercase())),
            _ => None,
        },
        "upper" => match args.get(0)? {
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
            eprintln!("Unsupported function: {}", name);
            None
        }
    }
}
