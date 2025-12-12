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
            (Int(_), Float(_)) | (Float(_), Int(_)) | (Float(_), Float(_)) => self.eval_float(),
            (Decimal(_), Decimal(_))
            | (Decimal(_), Int(_))
            | (Int(_), Decimal(_))
            | (Decimal(_), Float(_))
            | (Float(_), Decimal(_)) => self.eval_decimal(),
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
            Value::Float(f) => Some(*f),
            Value::Decimal(d) => d.to_f64(),
            _ => None,
        }
    }
}
