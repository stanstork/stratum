use bigdecimal::{FromPrimitive, ToPrimitive};
use model::{core::value::Value, execution::expr::BinaryOp};
use tracing::warn;

/// Binary operation evaluator that handles different value type combinations
pub(crate) struct BinaryOpEvaluator<'a> {
    left: &'a Value,
    right: &'a Value,
    op: &'a BinaryOp,
}

impl<'a> BinaryOpEvaluator<'a> {
    pub fn new(left: &'a Value, right: &'a Value, op: &'a BinaryOp) -> Self {
        Self { left, right, op }
    }

    pub fn evaluate(&self) -> Option<Value> {
        use Value::*;

        match (self.left, self.right) {
            (Int(l), Int(r)) => self.eval_int(*l, *r),
            // Handle combinations of integral types
            (SmallInt(_), _) | (Int(_), _) | (Int32(_), _) | (Uint(_), _) | (Usize(_), _)
                if self.right.as_i64().is_some() && self.left.as_i64().is_some() =>
            {
                self.eval_int(self.left.as_i64().unwrap(), self.right.as_i64().unwrap())
            }

            // Float interactions
            (Float(_), _) | (_, Float(_)) => self.eval_float(),

            // Decimal interactions (covers Decimal vs everything else not covered above)
            (Decimal(_), _) | (_, Decimal(_)) => self.eval_decimal(),

            // Uint and Usize interactions are handled as Decimal
            (Uint(_), _) | (_, Uint(_)) | (Usize(_), _) | (_, Usize(_)) => self.eval_decimal(),

            (String(l), String(r)) => self.eval_string(l, r),
            (Boolean(l), Boolean(r)) => self.eval_boolean(*l, *r),
            (Null, Null) => self.eval_null_null(),
            (Null, _) | (_, Null) => self.eval_null_other(),
            _ => None,
        }
    }

    fn eval_int(&self, l: i64, r: i64) -> Option<Value> {
        use Value::*;
        Some(match self.op {
            BinaryOp::Add => Int(l + r),
            BinaryOp::Subtract => Int(l - r),
            BinaryOp::Multiply => Int(l * r),
            BinaryOp::Divide => Int(l / r),
            BinaryOp::Modulo => Int(l % r),
            BinaryOp::Equal => Boolean(l == r),
            BinaryOp::NotEqual => Boolean(l != r),
            BinaryOp::GreaterThan => Boolean(l > r),
            BinaryOp::LessThan => Boolean(l < r),
            BinaryOp::GreaterOrEqual => Boolean(l >= r),
            BinaryOp::LessOrEqual => Boolean(l <= r),
            _ => {
                warn!("Unsupported binary operation for Int: {:?}", self.op);
                return None;
            }
        })
    }

    fn eval_float(&self) -> Option<Value> {
        use Value::*;
        let l = self.as_float(self.left)?;
        let r = self.as_float(self.right)?;

        Some(match self.op {
            BinaryOp::Add => Float(l + r),
            BinaryOp::Subtract => Float(l - r),
            BinaryOp::Multiply => Float(l * r),
            BinaryOp::Divide => Float(l / r),
            BinaryOp::Modulo => Float(l % r),
            BinaryOp::Equal => Boolean((l - r).abs() < f64::EPSILON),
            BinaryOp::NotEqual => Boolean((l - r).abs() >= f64::EPSILON),
            BinaryOp::GreaterThan => Boolean(l > r),
            BinaryOp::LessThan => Boolean(l < r),
            BinaryOp::GreaterOrEqual => Boolean(l >= r),
            BinaryOp::LessOrEqual => Boolean(l <= r),
            _ => {
                warn!("Unsupported binary operation for Float: {:?}", self.op);
                return None;
            }
        })
    }

    fn eval_decimal(&self) -> Option<Value> {
        use Value::*;
        let l = self.as_float(self.left)?;
        let r = self.as_float(self.right)?;

        Some(match self.op {
            BinaryOp::Add => Decimal(bigdecimal::BigDecimal::from_f64(l + r)?),
            BinaryOp::Subtract => Decimal(bigdecimal::BigDecimal::from_f64(l - r)?),
            BinaryOp::Multiply => Decimal(bigdecimal::BigDecimal::from_f64(l * r)?),
            BinaryOp::Divide => Decimal(bigdecimal::BigDecimal::from_f64(l / r)?),
            BinaryOp::Modulo => Decimal(bigdecimal::BigDecimal::from_f64(l % r)?),
            BinaryOp::Equal => Boolean((l - r).abs() < f64::EPSILON),
            BinaryOp::NotEqual => Boolean((l - r).abs() >= f64::EPSILON),
            BinaryOp::GreaterThan => Boolean(l > r),
            BinaryOp::LessThan => Boolean(l < r),
            BinaryOp::GreaterOrEqual => Boolean(l >= r),
            BinaryOp::LessOrEqual => Boolean(l <= r),
            _ => {
                warn!("Unsupported binary operation for Decimal: {:?}", self.op);
                return None;
            }
        })
    }

    fn eval_string(&self, l: &str, r: &str) -> Option<Value> {
        use Value::*;
        Some(match self.op {
            BinaryOp::Equal => Boolean(l == r),
            BinaryOp::NotEqual => Boolean(l != r),
            BinaryOp::GreaterThan => Boolean(l > r),
            BinaryOp::LessThan => Boolean(l < r),
            BinaryOp::GreaterOrEqual => Boolean(l >= r),
            BinaryOp::LessOrEqual => Boolean(l <= r),
            BinaryOp::Add => String(format!("{}{}", l, r)),
            _ => {
                warn!("Unsupported binary operation for String: {:?}", self.op);
                return None;
            }
        })
    }

    fn eval_boolean(&self, l: bool, r: bool) -> Option<Value> {
        use Value::*;
        Some(match self.op {
            BinaryOp::And => Boolean(l && r),
            BinaryOp::Or => Boolean(l || r),
            BinaryOp::Equal => Boolean(l == r),
            BinaryOp::NotEqual => Boolean(l != r),
            _ => {
                warn!("Unsupported binary operation for Boolean: {:?}", self.op);
                return None;
            }
        })
    }

    fn eval_null_null(&self) -> Option<Value> {
        use Value::*;
        match self.op {
            BinaryOp::Equal => Some(Boolean(true)),
            BinaryOp::NotEqual => Some(Boolean(false)),
            _ => None,
        }
    }

    fn eval_null_other(&self) -> Option<Value> {
        use Value::*;
        match self.op {
            BinaryOp::Equal => Some(Boolean(false)),
            BinaryOp::NotEqual => Some(Boolean(true)),
            _ => None,
        }
    }

    fn as_float(&self, v: &Value) -> Option<f64> {
        match v {
            Value::Int(i) => Some(*i as f64),
            Value::SmallInt(i) => Some(*i as f64),
            Value::Int32(i) => Some(*i as f64),
            Value::Uint(i) => Some(*i as f64),
            Value::Usize(i) => Some(*i as f64),
            Value::Float(f) => Some(*f),
            Value::Decimal(d) => d.to_f64(),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use model::core::value::Value;
    use model::execution::expr::BinaryOp;

    #[test]
    fn test_smallint_numeric_combinations() {
        let cases = vec![
            (
                Value::SmallInt(6),
                Value::Int(7),
                BinaryOp::LessOrEqual,
                Value::Boolean(true),
            ),
            (
                Value::Int(7),
                Value::SmallInt(6),
                BinaryOp::GreaterOrEqual,
                Value::Boolean(true),
            ),
            (
                Value::SmallInt(6),
                Value::SmallInt(6),
                BinaryOp::Equal,
                Value::Boolean(true),
            ),
        ];

        for (left, right, op, expected) in cases {
            let evaluator = BinaryOpEvaluator::new(&left, &right, &op);
            let result = evaluator.evaluate();
            assert_eq!(
                result,
                Some(expected),
                "Failed for {:?} {:?} {:?}",
                left,
                op,
                right
            );
        }
    }

    #[test]
    fn test_numeric_combinations() {
        let numeric_values = vec![
            Value::SmallInt(1),
            Value::Int32(1),
            Value::Int(1),
            Value::Uint(1),
            Value::Usize(1),
            Value::Float(1.0),
            Value::Decimal(bigdecimal::BigDecimal::from(1)),
        ];

        let ops = vec![
            BinaryOp::Equal,
            BinaryOp::NotEqual,
            BinaryOp::GreaterThan,
            BinaryOp::LessThan,
            BinaryOp::GreaterOrEqual,
            BinaryOp::LessOrEqual,
            BinaryOp::Add,
            BinaryOp::Subtract,
            BinaryOp::Multiply,
            BinaryOp::Divide,
        ];

        for l in &numeric_values {
            for r in &numeric_values {
                for op in &ops {
                    let evaluator = BinaryOpEvaluator::new(l, r, op);
                    let result = evaluator.evaluate();
                    assert!(result.is_some(), "Failed for {:?} {:?} {:?}", l, op, r);
                }
            }
        }
    }
}
