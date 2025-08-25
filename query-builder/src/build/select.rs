//! Provides a type-safe, fluent builder for constructing `Select` ASTs.

// --- Typestate Marker Structs ---
// These zero-sized structs represent the state of the builder.
// They ensure that methods are called in the correct SQL order at compile time.

use crate::ast::{
    common::{JoinKind, OrderDir, TableRef},
    expr::Expr,
    select::{FromClause, JoinClause, OrderByExpr, Select},
};

/// The initial state of the builder before any clauses have been added.
#[derive(Debug, Default, Clone)]
pub struct InitialState;

/// The state after the `SELECT` clause has been added.
#[derive(Debug, Default, Clone)]
pub struct SelectState;

/// The state after the `FROM` clause has been added.
#[derive(Debug, Default, Clone)]
pub struct FromState;

// --- The Main Builder ---

#[derive(Debug, Clone)]
pub struct SelectBuilder<State> {
    ast: Select,
    state: State,
}

/// Implementation for the initial state of the builder.
impl SelectBuilder<InitialState> {
    pub fn new() -> Self {
        Self {
            ast: Select::default(),
            state: InitialState,
        }
    }

    /// Adds a `SELECT` clause with a list of columns.
    /// This is the entry point for building a select query.
    pub fn select(mut self, columns: Vec<Expr>) -> SelectBuilder<SelectState> {
        self.ast.columns = columns;
        SelectBuilder {
            ast: self.ast,
            state: SelectState,
        }
    }
}

/// Implementation for the state after `SELECT` has been called.
/// The only valid next step is to specify a `FROM` table.
impl SelectBuilder<SelectState> {
    /// Adds a `FROM` clause specifying the primary table.
    pub fn from(mut self, table: TableRef, alias: Option<&str>) -> SelectBuilder<FromState> {
        self.ast.from = Some(FromClause {
            table,
            alias: alias.map(String::from),
        });
        SelectBuilder {
            ast: self.ast,
            state: FromState,
        }
    }
}

/// Implementation for the state after `FROM` has been called.
/// From here, we can add optional clauses like `JOIN`, `WHERE`, etc.
impl SelectBuilder<FromState> {
    /// Adds a `JOIN` clause to the query.
    pub fn join(mut self, kind: JoinKind, table: TableRef, alias: Option<&str>, on: Expr) -> Self {
        self.ast.joins.push(JoinClause {
            kind,
            table,
            alias: alias.map(String::from),
            on,
        });
        self
    }

    /// Adds a `WHERE` clause to the query.
    pub fn where_clause(mut self, condition: Expr) -> Self {
        self.ast.where_clause = Some(condition);
        self
    }

    /// Adds an `ORDER BY` clause to the query.
    pub fn order_by(mut self, expr: Expr, direction: Option<OrderDir>) -> Self {
        self.ast.order_by.push(OrderByExpr { expr, direction });
        self
    }

    /// Adds a `LIMIT` clause to the query.
    pub fn limit(mut self, limit: Expr) -> Self {
        self.ast.limit = Some(limit);
        self
    }

    /// Adds an `OFFSET` clause to the query.
    pub fn offset(mut self, offset: Expr) -> Self {
        self.ast.offset = Some(offset);
        self
    }

    /// Finalizes and returns the constructed `Select` AST.
    pub fn build(self) -> Select {
        self.ast
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::{
        ast::{
            common::{JoinKind, OrderDir, TableRef},
            expr::{BinaryOp, BinaryOperator, Expr, Ident},
        },
        build::select::SelectBuilder,
    };

    fn ident(name: &str) -> Expr {
        Expr::Identifier(Ident {
            qualifier: None,
            name: name.to_string(),
        })
    }

    fn qual_ident(qualifier: &str, name: &str) -> Expr {
        Expr::Identifier(Ident {
            qualifier: Some(qualifier.to_string()),
            name: name.to_string(),
        })
    }

    fn value(val: serde_json::Value) -> Expr {
        Expr::Value(val)
    }

    fn table(name: &str) -> TableRef {
        TableRef {
            schema: None,
            name: name.to_string(),
        }
    }

    #[test]
    fn test_build_simple_select() {
        let builder = SelectBuilder::new();
        let ast = builder
            .select(vec![ident("id"), ident("name")])
            .from(table("users"), None)
            .build();

        assert_eq!(ast.columns, vec![ident("id"), ident("name")]);
        assert_eq!(ast.from.unwrap().table.name, "users");
        assert!(ast.where_clause.is_none());
    }

    #[test]
    fn test_build_with_where_clause() {
        let builder = SelectBuilder::new();
        let ast = builder
            .select(vec![ident("email")])
            .from(table("users"), Some("u"))
            .where_clause(Expr::BinaryOp(Box::new(BinaryOp {
                left: qual_ident("u", "status"),
                op: BinaryOperator::Eq,
                right: value(json!("active")),
            })))
            .build();

        assert_eq!(ast.from.unwrap().alias, Some("u".to_string()));
        let where_clause = ast.where_clause.unwrap();
        assert!(matches!(where_clause, Expr::BinaryOp(_)));
    }

    #[test]
    fn test_build_with_join_and_ordering() {
        let builder = SelectBuilder::new();
        let ast = builder
            .select(vec![qual_ident("u", "name"), qual_ident("p", "title")])
            .from(table("users"), Some("u"))
            .join(
                JoinKind::Left,
                table("posts"),
                Some("p"),
                Expr::BinaryOp(Box::new(BinaryOp {
                    left: qual_ident("u", "id"),
                    op: BinaryOperator::Eq,
                    right: qual_ident("p", "user_id"),
                })),
            )
            .order_by(qual_ident("p", "created_at"), Some(OrderDir::Desc))
            .build();

        assert_eq!(ast.joins.len(), 1);
        assert_eq!(ast.order_by.len(), 1);
        assert_eq!(ast.order_by[0].direction, Some(OrderDir::Desc));
    }

    #[test]
    fn test_build_with_limit_and_offset() {
        let builder = SelectBuilder::new();
        let ast = builder
            .select(vec![ident("id")])
            .from(table("products"), None)
            .limit(value(json!(50)))
            .offset(value(json!(100)))
            .build();

        assert_eq!(ast.limit, Some(value(json!(50))));
        assert_eq!(ast.offset, Some(value(json!(100))));
    }
}
