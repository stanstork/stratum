use crate::{
    context::EvalContext,
    error::{ExpressionError, Result},
};
use chrono::{Datelike, NaiveDate, NaiveDateTime, Utc};
use model::core::value::Value;

/// `date(ts)` - truncate a timestamp to its date component.
pub fn eval_date(args: &[Value], _ctx: &EvalContext) -> Result<Value> {
    match args.first() {
        Some(Value::Null) | None => Ok(Value::Null),
        Some(v) => as_date(v)
            .map(Value::Date)
            .ok_or_else(|| invalid("date", v)),
    }
}

/// `year(ts)` - extract the year as an integer.
pub fn eval_year(args: &[Value], _ctx: &EvalContext) -> Result<Value> {
    extract("year", args, |d| d.year() as i64)
}

/// `month(ts)` - extract the month (1-12) as an integer.
pub fn eval_month(args: &[Value], _ctx: &EvalContext) -> Result<Value> {
    extract("month", args, |d| d.month() as i64)
}

/// `quarter(ts)` - extract the quarter (1-4) as an integer.
pub fn eval_quarter(args: &[Value], _ctx: &EvalContext) -> Result<Value> {
    extract("quarter", args, |d| ((d.month0() / 3) + 1) as i64)
}

/// `now()` - the current UTC timestamp.
pub fn eval_now(_args: &[Value], _ctx: &EvalContext) -> Result<Value> {
    let now: NaiveDateTime = Utc::now().naive_utc();
    Ok(Value::Timestamp {
        value: now,
        offset_secs: Some(0),
    })
}

/// Pull a `NaiveDate` out of a temporal value, or `None` if the value is not date-like.
fn as_date(value: &Value) -> Option<NaiveDate> {
    match value {
        Value::Date(d) => Some(*d),
        Value::Timestamp { value, .. } => Some(value.date()),
        _ => None,
    }
}

fn invalid<T: std::fmt::Debug>(function: &str, got: &T) -> ExpressionError {
    ExpressionError::InvalidFunctionArgs {
        function: function.to_string(),
        message: format!("Expected a date or timestamp, got {:?}", got),
    }
}

fn extract(function: &str, args: &[Value], f: impl Fn(NaiveDate) -> i64) -> Result<Value> {
    match args.first() {
        Some(Value::Null) | None => Ok(Value::Null),
        Some(v) => as_date(v)
            .map(|d| Value::Int(f(d)))
            .ok_or_else(|| invalid(function, v)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveTime;
    use std::collections::HashMap;

    fn dummy_env_getter(_key: &str) -> Option<String> {
        None
    }

    fn with_dummy_ctx<F, R>(f: F) -> R
    where
        F: FnOnce(&EvalContext) -> R,
    {
        let definitions = HashMap::new();
        let ctx = EvalContext::BuildTime {
            definitions: &definitions,
            env_getter: &dummy_env_getter,
        };
        f(&ctx)
    }

    fn ts(y: i32, m: u32, d: u32) -> Value {
        Value::Timestamp {
            value: NaiveDate::from_ymd_opt(y, m, d)
                .unwrap()
                .and_time(NaiveTime::from_hms_opt(3, 4, 5).unwrap()),
            offset_secs: None,
        }
    }

    #[test]
    fn test_date_from_timestamp() {
        with_dummy_ctx(|ctx| {
            let result = eval_date(&[ts(2024, 7, 15)], ctx).unwrap();
            assert_eq!(
                result,
                Value::Date(NaiveDate::from_ymd_opt(2024, 7, 15).unwrap())
            );
        });
    }

    #[test]
    fn test_year_month_quarter() {
        with_dummy_ctx(|ctx| {
            assert_eq!(
                eval_year(&[ts(2024, 11, 2)], ctx).unwrap(),
                Value::Int(2024)
            );
            assert_eq!(eval_month(&[ts(2024, 11, 2)], ctx).unwrap(), Value::Int(11));
            // November is in Q4.
            assert_eq!(
                eval_quarter(&[ts(2024, 11, 2)], ctx).unwrap(),
                Value::Int(4)
            );
            // January is in Q1.
            assert_eq!(eval_quarter(&[ts(2024, 1, 2)], ctx).unwrap(), Value::Int(1));
        });
    }

    #[test]
    fn test_null_propagates() {
        with_dummy_ctx(|ctx| {
            assert_eq!(eval_year(&[Value::Null], ctx).unwrap(), Value::Null);
            assert_eq!(eval_date(&[], ctx).unwrap(), Value::Null);
        });
    }

    #[test]
    fn test_non_temporal_errors() {
        with_dummy_ctx(|ctx| {
            assert!(eval_year(&[Value::Int(5)], ctx).is_err());
        });
    }

    #[test]
    fn test_now_returns_timestamp() {
        with_dummy_ctx(|ctx| {
            assert!(matches!(
                eval_now(&[], ctx).unwrap(),
                Value::Timestamp { .. }
            ));
        });
    }
}
