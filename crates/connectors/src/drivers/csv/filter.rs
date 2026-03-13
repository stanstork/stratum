use crate::drivers::csv::metadata::CsvColumnMetadata;
use crate::drivers::csv::types::CsvType;
use csv::StringRecord;
use model::core::value::Value;
use std::{cmp::Ordering, str::FromStr};

/// A CSV‐side filter: holds an optional expression tree.
#[derive(Clone, Debug, Default)]
pub struct CsvFilter {
    expr: Option<CsvFilterExpr>,
}

/// An expression over one or more leaf Conditions.
#[derive(Clone, Debug)]
pub enum CsvFilterExpr {
    Leaf(CsvCondition),
    And(Vec<CsvFilterExpr>),
    Or(Vec<CsvFilterExpr>),
}

#[derive(Clone, Debug)]
pub struct CsvCondition {
    pub left: String,
    pub op: CsvComparator,
    pub right: String,
}

#[derive(Clone, Debug)]
pub enum CsvComparator {
    Equal,
    NotEqual,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
}

impl CsvComparator {
    pub fn test(&self, actual: &Value, target: &Value) -> bool {
        match self {
            CsvComparator::Equal => values_equal(actual, target),
            CsvComparator::NotEqual => !values_equal(actual, target),
            CsvComparator::GreaterThan => {
                matches!(compare_values(actual, target), Some(Ordering::Greater))
            }
            CsvComparator::GreaterThanOrEqual => matches!(
                compare_values(actual, target),
                Some(Ordering::Greater) | Some(Ordering::Equal)
            ),
            CsvComparator::LessThan => {
                matches!(compare_values(actual, target), Some(Ordering::Less))
            }
            CsvComparator::LessThanOrEqual => matches!(
                compare_values(actual, target),
                Some(Ordering::Less) | Some(Ordering::Equal)
            ),
        }
    }
}

/// Check if two Values are equal.
fn values_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Null, Value::Null) => true,
        (Value::Null, _) | (_, Value::Null) => false,
        (Value::Int(x), Value::Int(y)) => x == y,
        (Value::UInt(x), Value::UInt(y)) => x == y,
        (Value::Int(x), Value::UInt(y)) => *x >= 0 && (*x as u64) == *y,
        (Value::UInt(x), Value::Int(y)) => *y >= 0 && *x == (*y as u64),
        (Value::Float(x), Value::Float(y)) => (x - y).abs() < f64::EPSILON,
        (Value::Decimal(x), Value::Decimal(y)) => x == y,
        (Value::String(x), Value::String(y)) => x == y,
        (Value::Boolean(x), Value::Boolean(y)) => x == y,
        (Value::Date(x), Value::Date(y)) => x == y,
        (Value::Time { value: x, .. }, Value::Time { value: y, .. }) => x == y,
        (Value::Timestamp { value: x, .. }, Value::Timestamp { value: y, .. }) => x == y,
        _ => a == b, // fallback to PartialEq
    }
}

/// Compare two Values, returning ordering if comparable.
fn compare_values(a: &Value, b: &Value) -> Option<Ordering> {
    match (a, b) {
        (Value::Null, Value::Null) => Some(Ordering::Equal),
        (Value::Null, _) => Some(Ordering::Less),
        (_, Value::Null) => Some(Ordering::Greater),

        (Value::Int(x), Value::Int(y)) => Some(x.cmp(y)),
        (Value::UInt(x), Value::UInt(y)) => Some(x.cmp(y)),
        (Value::Int(x), Value::UInt(y)) => {
            if *x < 0 {
                Some(Ordering::Less)
            } else {
                Some((*x as u64).cmp(y))
            }
        }
        (Value::UInt(x), Value::Int(y)) => {
            if *y < 0 {
                Some(Ordering::Greater)
            } else {
                Some(x.cmp(&(*y as u64)))
            }
        }

        (Value::Float(x), Value::Float(y)) => x.partial_cmp(y),
        (Value::Decimal(x), Value::Decimal(y)) => Some(x.cmp(y)),
        (Value::String(x), Value::String(y)) => Some(x.cmp(y)),
        (Value::Date(x), Value::Date(y)) => Some(x.cmp(y)),
        (Value::Time { value: x, .. }, Value::Time { value: y, .. }) => Some(x.cmp(y)),
        (Value::Timestamp { value: x, .. }, Value::Timestamp { value: y, .. }) => Some(x.cmp(y)),
        (Value::Year(x), Value::Year(y)) => Some(x.cmp(y)),

        // Non-comparable types
        _ => None,
    }
}

impl FromStr for CsvComparator {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "=" => Ok(CsvComparator::Equal),
            "!=" => Ok(CsvComparator::NotEqual),
            ">" => Ok(CsvComparator::GreaterThan),
            ">=" => Ok(CsvComparator::GreaterThanOrEqual),
            "<" => Ok(CsvComparator::LessThan),
            "<=" => Ok(CsvComparator::LessThanOrEqual),
            _ => Err(format!("Unsupported comparator: {s}")),
        }
    }
}

impl CsvFilter {
    /// Create a new empty filter.
    pub fn new() -> Self {
        CsvFilter { expr: None }
    }

    /// Create a new filter with the given expression.
    pub fn with_expr(expr: CsvFilterExpr) -> Self {
        CsvFilter { expr: Some(expr) }
    }

    /// Returns true if this row passes the filter (or if there's no filter).
    ///
    /// `record` is the CSV row, `headers` the list of column names, and
    /// `meta` is `CsvMetadata` used to know each column's declared type.
    pub fn eval(
        &self,
        record: &StringRecord,
        headers_meta: &[(String, CsvColumnMetadata)],
    ) -> bool {
        match &self.expr {
            Some(expr) => expr.eval(record, headers_meta),
            None => true,
        }
    }
}

impl CsvFilterExpr {
    /// Create a new leaf condition.
    pub fn leaf(cond: CsvCondition) -> Self {
        CsvFilterExpr::Leaf(cond)
    }

    /// Create a new AND expression.
    pub fn and(exprs: Vec<CsvFilterExpr>) -> Self {
        CsvFilterExpr::And(exprs)
    }

    /// Create a new OR expression.
    pub fn or(exprs: Vec<CsvFilterExpr>) -> Self {
        CsvFilterExpr::Or(exprs)
    }

    /// Recursively evaluate this expression against one CSV row.
    pub fn eval(
        &self,
        record: &StringRecord,
        headers_meta: &[(String, CsvColumnMetadata)],
    ) -> bool {
        match self {
            CsvFilterExpr::Leaf(cond) => {
                // locate the metadata for this condition's column
                let (_, col_meta) = match headers_meta
                    .iter()
                    .find(|(hdr, _)| hdr.eq_ignore_ascii_case(&cond.left))
                {
                    Some(pair) => pair,
                    None => return false,
                };

                // parse the two Values
                let actual = match col_meta
                    .data_type
                    .get_value(record.get(col_meta.ordinal).unwrap_or(""))
                {
                    Some(v) => v,
                    None => return false,
                };
                let target = match col_meta.data_type.get_value(&cond.right) {
                    Some(v) => v,
                    None => return false,
                };

                // run the comparator
                cond.op.test(&actual, &target)
            }

            CsvFilterExpr::And(children) => children.iter().all(|c| c.eval(record, headers_meta)),

            CsvFilterExpr::Or(children) => children.iter().any(|c| c.eval(record, headers_meta)),
        }
    }
}
