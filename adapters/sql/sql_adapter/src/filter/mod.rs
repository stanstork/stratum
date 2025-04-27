use crate::join::clause::JoinClause;
use std::fmt;

/// A single table-column filter condition
#[derive(Debug, Clone)]
pub struct Condition {
    pub table: String,
    pub column: String,
    pub comparator: String,
    pub value: String,
}

impl Condition {
    /// Render just this one condition as SQL
    pub fn to_sql_fragment(&self) -> String {
        format!(
            "{}.{} {} {}",
            self.table, self.column, self.comparator, self.value
        )
    }

    /// Does this condition apply to `table` or one of the joined tables?
    pub fn applies_to<'a>(
        &self,
        table: &str,
        joins: impl IntoIterator<Item = &'a JoinClause>,
    ) -> bool {
        if self.table.eq_ignore_ascii_case(table) {
            true
        } else {
            joins.into_iter().any(|j| {
                j.left.alias.eq_ignore_ascii_case(&self.table)
                    || j.right.alias.eq_ignore_ascii_case(&self.table)
            })
        }
    }
}

impl fmt::Display for Condition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_sql_fragment())
    }
}

/// A collection of SQL conditions, e.g. to be AND-joined
#[derive(Debug, Clone, Default)]
pub struct SqlFilter {
    pub conditions: Vec<Condition>,
}

impl SqlFilter {
    pub fn push(&mut self, cond: Condition) {
        self.conditions.push(cond);
    }

    /// Return a new `SqlFilter` containing *only* the conditions
    /// that apply to `table` or its `joins`.
    pub fn for_table(&self, table: &str, joins: &[JoinClause]) -> Self {
        let conditions = self
            .conditions
            .iter()
            .filter(|c| c.applies_to(table, joins))
            .cloned()
            .collect();
        SqlFilter { conditions }
    }

    /// Render `WHERE …` (without the leading keyword)
    pub fn to_sql(&self) -> String {
        self.conditions
            .iter()
            .map(Condition::to_sql_fragment)
            .collect::<Vec<_>>()
            .join(" AND ")
    }
}
