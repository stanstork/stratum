use crate::join::clause::JoinClause;
use std::fmt;

/// A single table-column filter condition
#[derive(Debug, Clone, PartialEq)]
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
