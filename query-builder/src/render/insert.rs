use crate::{ast::insert::Insert, render::Render};

impl Render for Insert {
    fn render(&self, r: &mut super::Renderer) {
        // 1. INSERT INTO table (...)
        r.sql.push_str("INSERT INTO ");
        r.sql
            .push_str(&r.dialect.quote_identifier(&self.table.name));
        r.sql.push_str(" (");
        let quoted_columns: Vec<String> = self
            .columns
            .iter()
            .map(|c| r.dialect.quote_identifier(c))
            .collect();
        r.sql.push_str(&quoted_columns.join(", "));
        r.sql.push(')');

        // 2. VALUES (...)
        r.sql.push_str(" VALUES ");
        for (i, row) in self.values.iter().enumerate() {
            if i > 0 {
                r.sql.push_str(", ");
            }
            r.sql.push('(');
            for (j, val) in row.iter().enumerate() {
                if j > 0 {
                    r.sql.push_str(", ");
                }
                // Each value expression is rendered, which for Expr::Value
                // will add a parameter and its placeholder.
                val.render(r);
            }
            r.sql.push(')');
        }
        r.sql.push(';');
    }
}

#[cfg(test)]
mod tests {
    use data_model::core::value::Value;

    use crate::{
        ast::{common::TableRef, expr::Expr, insert::Insert},
        dialect::{MySql, Postgres},
        render::{Render, Renderer},
    };

    fn value(val: Value) -> Expr {
        Expr::Value(val)
    }

    #[test]
    fn test_render_batch_insert_postgres() {
        let ast = Insert {
            table: TableRef {
                schema: None,
                name: "users".to_string(),
            },
            columns: vec!["name".to_string(), "is_active".to_string()],
            values: vec![
                vec![
                    value(Value::String("Alice".to_string())),
                    value(Value::Boolean(true)),
                ],
                vec![
                    value(Value::String("Bob".to_string())),
                    value(Value::Boolean(false)),
                ],
            ],
        };

        let dialect = Postgres;
        let mut renderer = Renderer::new(&dialect);
        ast.render(&mut renderer);
        let (sql, params) = renderer.finish();

        let expected_sql =
            r#"INSERT INTO "users" ("name", "is_active") VALUES ($1, $2), ($3, $4);"#;
        assert_eq!(sql, expected_sql);
        assert_eq!(
            params,
            vec![
                Value::String("Alice".to_string()),
                Value::Boolean(true),
                Value::String("Bob".to_string()),
                Value::Boolean(false)
            ]
        );
    }

    #[test]
    fn test_render_batch_insert_mysql() {
        let ast = Insert {
            table: TableRef {
                schema: None,
                name: "users".to_string(),
            },
            columns: vec!["name".to_string(), "is_active".to_string()],
            values: vec![
                vec![
                    value(Value::String("Alice".to_string())),
                    value(Value::Boolean(true)),
                ],
                vec![
                    value(Value::String("Bob".to_string())),
                    value(Value::Boolean(false)),
                ],
            ],
        };

        let dialect = MySql;
        let mut renderer = Renderer::new(&dialect);
        ast.render(&mut renderer);
        let (sql, params) = renderer.finish();

        let expected_sql = "INSERT INTO `users` (`name`, `is_active`) VALUES (?, ?), (?, ?);";
        assert_eq!(sql, expected_sql);
        assert_eq!(
            params,
            vec![
                Value::String("Alice".to_string()),
                Value::Boolean(true),
                Value::String("Bob".to_string()),
                Value::Boolean(false)
            ]
        );
    }
}
