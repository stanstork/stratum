use crate::{metadata::CsvColumnMetadata, types::CsvType};
use common::value::Value;
use csv::StringRecord;
use std::cmp::Ordering;

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
    pub fn from_str(op: &str) -> Option<Self> {
        match op {
            "=" => Some(CsvComparator::Equal),
            "!=" => Some(CsvComparator::NotEqual),
            ">" => Some(CsvComparator::GreaterThan),
            ">=" => Some(CsvComparator::GreaterThanOrEqual),
            "<" => Some(CsvComparator::LessThan),
            "<=" => Some(CsvComparator::LessThanOrEqual),
            _ => None,
        }
    }

    pub fn test(&self, actual: &Value, target: &Value) -> bool {
        match self {
            CsvComparator::Equal => actual.equal(target),
            CsvComparator::NotEqual => !actual.equal(target),

            CsvComparator::GreaterThan => match actual.compare(target) {
                Some(Ordering::Greater) => true,
                _ => false,
            },
            CsvComparator::GreaterThanOrEqual => match actual.compare(target) {
                Some(Ordering::Greater) | Some(Ordering::Equal) => true,
                _ => false,
            },
            CsvComparator::LessThan => match actual.compare(target) {
                Some(Ordering::Less) => true,
                _ => false,
            },
            CsvComparator::LessThanOrEqual => match actual.compare(target) {
                Some(Ordering::Less) | Some(Ordering::Equal) => true,
                _ => false,
            },
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
    /// `meta` is `CsvMetadata` used to know each column’s declared type.
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
                // locate the metadata for this condition’s column
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
