use crate::join::clause::JoinClause;
use expr::SqlFilterExpr;

pub mod condition;
pub mod expr;

/// A collection of SQL filter expressions.
#[derive(Debug, Clone, Default)]
pub struct SqlFilter {
    pub expr: Option<SqlFilterExpr>,
}

impl SqlFilter {
    pub fn new() -> Self {
        SqlFilter { expr: None }
    }

    pub fn with_expr(expr: SqlFilterExpr) -> Self {
        SqlFilter { expr: Some(expr) }
    }

    /// Render "WHERE ..." or empty string if no expr.
    pub fn to_sql(&self) -> String {
        self.expr
            .as_ref()
            .map(|e| format!(" WHERE {}", e.to_sql()))
            .unwrap_or_default()
    }

    /// Return a new `SqlFilter` whose expression only contains
    /// leaves (conditions) that apply to `table` or one of `joins`.
    pub fn for_table(&self, table: &str, joins: &[JoinClause]) -> SqlFilter {
        let expr = self
            .expr
            .as_ref()
            .and_then(|e| e.filter_for_table(table, joins));
        SqlFilter { expr }
    }

    pub fn tables(&self) -> Vec<String> {
        self.expr.as_ref().map(|e| e.tables()).unwrap_or_default()
    }
}
