//! Provides a fluent builder for constructing `Insert` ASTs.

use crate::ast::{common::TableRef, expr::Expr, insert::Insert};

#[derive(Debug, Clone)]
pub struct InsertBuilder {
    ast: Insert,
}

impl InsertBuilder {
    pub fn new(table: TableRef) -> Self {
        Self {
            ast: Insert {
                table,
                ..Default::default()
            },
        }
    }

    pub fn columns(mut self, columns: &[&str]) -> Self {
        self.ast.columns = columns.iter().map(|s| s.to_string()).collect();
        self
    }

    /// Adds a row of values to the insert statement.
    /// This can be called multiple times for a batch insert.
    pub fn values(mut self, values: Vec<Expr>) -> Self {
        // TODO: Add a check here to ensure `values.len()` matches `columns.len()`.
        self.ast.values.push(values);
        self
    }

    fn build(self) -> Insert {
        self.ast
    }
}

#[cfg(test)]
mod tests {
    use common::value::Value;

    use crate::{
        ast::{common::TableRef, expr::Expr},
        build::insert::InsertBuilder,
    };

    fn table(name: &str) -> TableRef {
        TableRef {
            schema: None,
            name: name.to_string(),
        }
    }

    fn value(val: Value) -> Expr {
        Expr::Value(val)
    }

    #[test]
    fn test_build_single_insert() {
        let builder = InsertBuilder::new(table("users"));
        let ast = builder
            .columns(&["name", "email"])
            .values(vec![
                value(Value::String("Alice".to_string())),
                value(Value::String("a@test.com".to_string())),
            ])
            .build();

        assert_eq!(ast.table.name, "users");
        assert_eq!(ast.columns, vec!["name", "email"]);
        assert_eq!(ast.values.len(), 1);
        assert_eq!(ast.values[0].len(), 2);
    }

    #[test]
    fn test_build_batch_insert() {
        let builder = InsertBuilder::new(table("logs"));
        let ast = builder
            .columns(&["level", "message"])
            .values(vec![
                value(Value::String("info".to_string())),
                value(Value::String("started".to_string())),
            ])
            .values(vec![
                value(Value::String("warn".to_string())),
                value(Value::String("deprecated".to_string())),
            ])
            .build();

        assert_eq!(ast.values.len(), 2);
        assert_eq!(ast.values[1][0], value(Value::String("warn".to_string())));
    }
}
