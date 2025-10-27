//! Provides a type-safe, fluent builder for constructing `Select` ASTs.

use data_model::pagination::cursor::Cursor;

use crate::{
    ast::{
        common::{JoinKind, OrderDir, TableRef},
        expr::Expr,
        select::{FromClause, JoinClause, OrderByExpr, Select},
    },
    offsets::offset_strategy_from_cursor,
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
    pub ast: Select,
    _state: State,
}

/// Implementation for the initial state of the builder.
impl SelectBuilder<InitialState> {
    pub fn new() -> Self {
        Self {
            ast: Select::default(),
            _state: InitialState,
        }
    }

    /// Adds a `SELECT` clause with a list of columns.
    /// This is the entry point for building a select query.
    pub fn select(mut self, columns: Vec<Expr>) -> SelectBuilder<SelectState> {
        self.ast.columns = columns;
        SelectBuilder {
            ast: self.ast,
            _state: SelectState,
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
            _state: FromState,
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

    /// Applies an offset-based pagination strategy to the query.
    /// This will add the appropriate WHERE, ORDER BY, and LIMIT clauses
    /// based on the cursor.
    pub fn paginate(self, cursor: &Cursor, limit: usize) -> Self {
        let strategy = offset_strategy_from_cursor(cursor);
        strategy.apply_to_builder(self, cursor, limit)
    }

    /// Finalizes and returns the constructed `Select` AST.
    pub fn build(self) -> Select {
        self.ast
    }
}

#[cfg(test)]
mod tests {
    use data_model::{core::value::Value, pagination::cursor::Cursor};

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

    fn value(val: Value) -> Expr {
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
                right: value(Value::String("active".to_string())),
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
            .limit(value(Value::Int(50)))
            .offset(value(Value::Int(100)))
            .build();

        assert_eq!(ast.limit, Some(value(Value::Int(50))));
        assert_eq!(ast.offset, Some(value(Value::Int(100))));
    }

    #[test]
    fn test_build_with_pagination_pk() {
        let builder = SelectBuilder::new();
        let cursor = Cursor::Pk {
            pk_col: "id".to_string(),
            id: 100,
        };

        let ast = builder
            .select(vec![ident("id"), ident("name")])
            .from(table("users"), None)
            .paginate(&cursor, 50)
            .build();

        // Check limit
        assert_eq!(ast.limit, Some(value(Value::Uint(50))));

        // Check order by
        assert_eq!(ast.order_by.len(), 1);
        assert_eq!(ast.order_by[0].expr, ident("id"));
        assert_eq!(ast.order_by[0].direction, Some(OrderDir::Asc));

        // Check where clause
        let where_clause = ast.where_clause.unwrap();
        // Expected: (id > 100)
        assert_eq!(
            where_clause,
            Expr::BinaryOp(Box::new(BinaryOp {
                left: ident("id"),
                op: BinaryOperator::Gt,
                right: value(Value::Uint(100)),
            }))
        );
    }

    #[test]
    fn test_build_pagination_first_page() {
        let builder = SelectBuilder::new();
        // Cursor::None means first page
        let cursor = Cursor::None;

        let ast = builder
            .select(vec![ident("id"), ident("name")])
            .from(table("users"), None)
            .paginate(&cursor, 50)
            .build();

        // Check limit
        assert_eq!(ast.limit, Some(value(Value::Uint(50))));

        // Check order by
        assert_eq!(ast.order_by.len(), 1);
        assert_eq!(ast.order_by[0].expr, ident("id"));
        assert_eq!(ast.order_by[0].direction, Some(OrderDir::Asc));

        // Check where clause - should be None
        assert!(ast.where_clause.is_none());
    }

    #[test]
    fn test_build_pagination_with_existing_where() {
        let builder = SelectBuilder::new();
        let cursor = Cursor::CompositeTsPk {
            ts_col: "created_at".to_string(),
            pk_col: "id".to_string(),
            ts: 123456789,
            id: 42,
        };

        let original_where = Expr::BinaryOp(Box::new(BinaryOp {
            left: ident("status"),
            op: BinaryOperator::Eq,
            right: value(Value::String("active".to_string())),
        }));

        let ast = builder
            .select(vec![ident("id")])
            .from(table("posts"), None)
            .where_clause(original_where.clone())
            .paginate(&cursor, 25)
            .build();

        // Check limit
        assert_eq!(ast.limit, Some(value(Value::Uint(25))));

        // Check order by
        assert_eq!(ast.order_by.len(), 2);
        assert_eq!(ast.order_by[0].expr, ident("created_at"));
        assert_eq!(ast.order_by[1].expr, ident("id"));

        // Check where clause
        // Expected: (status = 'active') AND ((created_at > 123...) OR (created_at = 123... AND id > 42))
        let where_clause = ast.where_clause.unwrap();

        let (left, op, right) = match where_clause {
            Expr::BinaryOp(op) => (op.left, op.op, op.right),
            _ => panic!("Expected outer BinaryOp(AND)"),
        };

        assert_eq!(op, BinaryOperator::And);
        assert_eq!(left, original_where);

        // Check pagination part of clause
        let (cond1, op_or, cond2) = match right {
            Expr::BinaryOp(op) => (op.left, op.op, op.right),
            _ => panic!("Expected inner BinaryOp(OR)"),
        };
        assert_eq!(op_or, BinaryOperator::Or);

        // cond1: (created_at > 123...)
        assert_eq!(
            cond1,
            Expr::BinaryOp(Box::new(BinaryOp {
                left: ident("created_at"),
                op: BinaryOperator::Gt,
                right: value(Value::Int(123456789)),
            }))
        );

        // cond2: (created_at = 123... AND id > 42)
        let (cond2_left, op_and, cond2_right) = match cond2 {
            Expr::BinaryOp(op) => (op.left, op.op, op.right),
            _ => panic!("Expected inner-right BinaryOp(AND)"),
        };
        assert_eq!(op_and, BinaryOperator::And);

        assert_eq!(
            cond2_left,
            Expr::BinaryOp(Box::new(BinaryOp {
                left: ident("created_at"),
                op: BinaryOperator::Eq,
                right: value(Value::Int(123456789)),
            }))
        );
        assert_eq!(
            cond2_right,
            Expr::BinaryOp(Box::new(BinaryOp {
                left: ident("id"),
                op: BinaryOperator::Gt,
                right: value(Value::Uint(42)),
            }))
        );
    }
}
