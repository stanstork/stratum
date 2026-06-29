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
            // Integer operations
            (Int(l), Int(r)) => self.eval_int(*l, *r),
            (UInt(l), UInt(r)) => self.eval_uint(*l, *r),
            (Int(l), UInt(r)) => self.eval_int(*l, *r as i64),
            (UInt(l), Int(r)) => self.eval_int(*l as i64, *r),

            // Float operations
            (Float(l), Float(r)) => self.eval_float(*l, *r),
            (Float(l), Int(r)) => self.eval_float(*l, *r as f64),
            (Int(l), Float(r)) => self.eval_float(*l as f64, *r),
            (Float(l), UInt(r)) => self.eval_float(*l, *r as f64),
            (UInt(l), Float(r)) => self.eval_float(*l as f64, *r),

            // Decimal operations
            (Decimal(l), Decimal(r)) => self.eval_decimal_values(l, r),
            (Decimal(l), Int(r)) => {
                let r_dec = bigdecimal::BigDecimal::from(*r);
                self.eval_decimal_values(l, &r_dec)
            }
            (Int(l), Decimal(r)) => {
                let l_dec = bigdecimal::BigDecimal::from(*l);
                self.eval_decimal_values(&l_dec, r)
            }
            (Decimal(l), Float(r)) => {
                if let Ok(r_dec) = bigdecimal::BigDecimal::try_from(*r) {
                    self.eval_decimal_values(l, &r_dec)
                } else {
                    None
                }
            }
            (Float(l), Decimal(r)) => {
                if let Ok(l_dec) = bigdecimal::BigDecimal::try_from(*l) {
                    self.eval_decimal_values(&l_dec, r)
                } else {
                    None
                }
            }

            // String operations
            (String(l), String(r)) => self.eval_string(l, r),

            // Boolean operations
            (Boolean(l), Boolean(r)) => self.eval_boolean(*l, *r),

            // Null handling
            (Null, Null) => self.eval_null_null(),
            (Null, _) | (_, Null) => self.eval_null_other(),

            _ => {
                warn!(
                    left = ?self.left,
                    op = ?self.op,
                    right = ?self.right,
                    "unsupported type combination for binary op"
                );
                None
            }
        }
    }

    fn eval_int(&self, l: i64, r: i64) -> Option<Value> {
        Some(match self.op {
            BinaryOp::Add => Value::Int(l.checked_add(r)?),
            BinaryOp::Subtract => Value::Int(l.checked_sub(r)?),
            BinaryOp::Multiply => Value::Int(l.checked_mul(r)?),
            BinaryOp::Divide => {
                if r == 0 {
                    return None;
                }
                Value::Int(l / r)
            }
            BinaryOp::Modulo => {
                if r == 0 {
                    return None;
                }
                Value::Int(l % r)
            }
            BinaryOp::Equal => Value::Boolean(l == r),
            BinaryOp::NotEqual => Value::Boolean(l != r),
            BinaryOp::GreaterThan => Value::Boolean(l > r),
            BinaryOp::LessThan => Value::Boolean(l < r),
            BinaryOp::GreaterOrEqual => Value::Boolean(l >= r),
            BinaryOp::LessOrEqual => Value::Boolean(l <= r),
            _ => {
                warn!(op = ?self.op, "unsupported binary operation for integer");
                return None;
            }
        })
    }

    fn eval_uint(&self, l: u64, r: u64) -> Option<Value> {
        Some(match self.op {
            BinaryOp::Add => Value::UInt(l.checked_add(r)?),
            BinaryOp::Subtract => Value::UInt(l.checked_sub(r)?),
            BinaryOp::Multiply => Value::UInt(l.checked_mul(r)?),
            BinaryOp::Divide => {
                if r == 0 {
                    return None;
                }
                Value::UInt(l / r)
            }
            BinaryOp::Modulo => {
                if r == 0 {
                    return None;
                }
                Value::UInt(l % r)
            }
            BinaryOp::Equal => Value::Boolean(l == r),
            BinaryOp::NotEqual => Value::Boolean(l != r),
            BinaryOp::GreaterThan => Value::Boolean(l > r),
            BinaryOp::LessThan => Value::Boolean(l < r),
            BinaryOp::GreaterOrEqual => Value::Boolean(l >= r),
            BinaryOp::LessOrEqual => Value::Boolean(l <= r),
            _ => {
                warn!(op = ?self.op, "unsupported binary operation for unsigned integer");
                return None;
            }
        })
    }

    fn eval_float(&self, l: f64, r: f64) -> Option<Value> {
        Some(match self.op {
            BinaryOp::Add => Value::Float(l + r),
            BinaryOp::Subtract => Value::Float(l - r),
            BinaryOp::Multiply => Value::Float(l * r),
            BinaryOp::Divide => Value::Float(l / r),
            BinaryOp::Modulo => Value::Float(l % r),
            BinaryOp::Equal => Value::Boolean((l - r).abs() < f64::EPSILON),
            BinaryOp::NotEqual => Value::Boolean((l - r).abs() >= f64::EPSILON),
            BinaryOp::GreaterThan => Value::Boolean(l > r),
            BinaryOp::LessThan => Value::Boolean(l < r),
            BinaryOp::GreaterOrEqual => Value::Boolean(l >= r),
            BinaryOp::LessOrEqual => Value::Boolean(l <= r),
            _ => {
                warn!(op = ?self.op, "unsupported binary operation for float");
                return None;
            }
        })
    }

    fn eval_decimal_values(
        &self,
        l: &bigdecimal::BigDecimal,
        r: &bigdecimal::BigDecimal,
    ) -> Option<Value> {
        use bigdecimal::BigDecimal;

        Some(match self.op {
            BinaryOp::Add => Value::Decimal(l + r),
            BinaryOp::Subtract => Value::Decimal(l - r),
            BinaryOp::Multiply => Value::Decimal(l * r),
            BinaryOp::Divide => {
                if r == &BigDecimal::from(0) {
                    return None;
                }
                Value::Decimal(l / r)
            }
            BinaryOp::Modulo => {
                if r == &BigDecimal::from(0) {
                    return None;
                }
                Value::Decimal(l % r)
            }
            BinaryOp::Equal => Value::Boolean(l == r),
            BinaryOp::NotEqual => Value::Boolean(l != r),
            BinaryOp::GreaterThan => Value::Boolean(l > r),
            BinaryOp::LessThan => Value::Boolean(l < r),
            BinaryOp::GreaterOrEqual => Value::Boolean(l >= r),
            BinaryOp::LessOrEqual => Value::Boolean(l <= r),
            _ => {
                warn!(op = ?self.op, "unsupported binary operation for decimal");
                return None;
            }
        })
    }

    fn eval_string(&self, l: &str, r: &str) -> Option<Value> {
        Some(match self.op {
            BinaryOp::Equal => Value::Boolean(l == r),
            BinaryOp::NotEqual => Value::Boolean(l != r),
            BinaryOp::GreaterThan => Value::Boolean(l > r),
            BinaryOp::LessThan => Value::Boolean(l < r),
            BinaryOp::GreaterOrEqual => Value::Boolean(l >= r),
            BinaryOp::LessOrEqual => Value::Boolean(l <= r),
            BinaryOp::Add => Value::String(format!("{}{}", l, r)),
            _ => {
                warn!(op = ?self.op, "unsupported binary operation for string");
                return None;
            }
        })
    }

    fn eval_boolean(&self, l: bool, r: bool) -> Option<Value> {
        Some(match self.op {
            BinaryOp::And => Value::Boolean(l && r),
            BinaryOp::Or => Value::Boolean(l || r),
            BinaryOp::Equal => Value::Boolean(l == r),
            BinaryOp::NotEqual => Value::Boolean(l != r),
            _ => {
                warn!(op = ?self.op, "unsupported binary operation for boolean");
                return None;
            }
        })
    }

    fn eval_null_null(&self) -> Option<Value> {
        match self.op {
            BinaryOp::Equal => Some(Value::Boolean(true)),
            BinaryOp::NotEqual => Some(Value::Boolean(false)),
            _ => None,
        }
    }

    fn eval_null_other(&self) -> Option<Value> {
        match self.op {
            BinaryOp::Equal => Some(Value::Boolean(false)),
            BinaryOp::NotEqual => Some(Value::Boolean(true)),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_int_operations() {
        let cases = vec![
            (
                Value::Int(6),
                Value::Int(7),
                BinaryOp::LessOrEqual,
                Value::Boolean(true),
            ),
            (
                Value::Int(7),
                Value::Int(6),
                BinaryOp::GreaterOrEqual,
                Value::Boolean(true),
            ),
            (
                Value::Int(6),
                Value::Int(6),
                BinaryOp::Equal,
                Value::Boolean(true),
            ),
            (Value::Int(10), Value::Int(3), BinaryOp::Add, Value::Int(13)),
            (
                Value::Int(10),
                Value::Int(3),
                BinaryOp::Subtract,
                Value::Int(7),
            ),
            (
                Value::Int(10),
                Value::Int(3),
                BinaryOp::Multiply,
                Value::Int(30),
            ),
            (
                Value::Int(10),
                Value::Int(3),
                BinaryOp::Divide,
                Value::Int(3),
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
    fn test_string_operations() {
        let left = Value::String("hello".to_string());
        let right = Value::String("hello".to_string());

        let evaluator = BinaryOpEvaluator::new(&left, &right, &BinaryOp::Equal);
        assert_eq!(evaluator.evaluate(), Some(Value::Boolean(true)));
    }

    #[test]
    fn test_boolean_operations() {
        let left = Value::Boolean(true);
        let right = Value::Boolean(false);

        let evaluator = BinaryOpEvaluator::new(&left, &right, &BinaryOp::And);
        assert_eq!(evaluator.evaluate(), Some(Value::Boolean(false)));

        let evaluator = BinaryOpEvaluator::new(&left, &right, &BinaryOp::Or);
        assert_eq!(evaluator.evaluate(), Some(Value::Boolean(true)));
    }
}
