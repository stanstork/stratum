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
                apply_arithmetic(&left_val, &right_val, operator)
            }

            _ => {
                eprintln!("Unsupported expression type: {:?}", self);
                None
            }
        }
    }
}

fn apply_arithmetic(left: &ColumnValue, right: &ColumnValue, op: &Operator) -> Option<ColumnValue> {
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
