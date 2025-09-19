use crate::{metadata::CsvColumnMetadata, types::CsvType};
use common::value::Value;
use csv::StringRecord;
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
            CsvComparator::Equal => actual.equal(target),
            CsvComparator::NotEqual => !actual.equal(target),

            CsvComparator::GreaterThan => matches!(actual.compare(target), Some(Ordering::Greater)),
            CsvComparator::GreaterThanOrEqual => matches!(
                actual.compare(target),
                Some(Ordering::Greater) | Some(Ordering::Equal)
            ),
            CsvComparator::LessThan => matches!(actual.compare(target), Some(Ordering::Less)),
            CsvComparator::LessThanOrEqual => matches!(
                actual.compare(target),
                Some(Ordering::Less) | Some(Ordering::Equal)
            ),
        }
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
            _ => Err(format!("Unsupported comparator: {}", s)),
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
