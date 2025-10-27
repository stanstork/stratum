use crate::{
    ast::{
        common::{JoinKind, OrderDir},
        select::{FromClause, JoinClause, OrderByExpr, Select},
    },
    render::{Render, Renderer},
};

impl Render for Select {
    fn render(&self, r: &mut Renderer) {
        // 1. SELECT clause
        r.sql.push_str("SELECT ");
        for (i, col) in self.columns.iter().enumerate() {
            if i > 0 {
                r.sql.push_str(", ");
            }
            col.render(r);
        }

        // 2. FROM
        if let Some(from) = &self.from {
            r.sql.push(' ');
            from.render(r);
        }

        // 3. JOIN
        for join in &self.joins {
            r.sql.push(' ');
            join.render(r);
        }

        // 4. WHERE
        if let Some(where_clause) = &self.where_clause {
            r.sql.push_str(" WHERE ");
            where_clause.render(r);
        }

        // 5. ORDER BY
        if !self.order_by.is_empty() {
            r.sql.push_str(" ORDER BY ");
            for (i, order) in self.order_by.iter().enumerate() {
                if i > 0 {
                    r.sql.push_str(", ");
                }
                order.render(r);
            }
        }

        // 6. LIMIT
        if let Some(limit) = &self.limit {
            r.sql.push_str(" LIMIT ");
            limit.render(r);
        }

        // 7. OFFSET
        if let Some(offset) = &self.offset {
            r.sql.push_str(" OFFSET ");
            offset.render(r);
        }
    }
}

impl Render for FromClause {
    fn render(&self, r: &mut Renderer) {
        r.sql.push_str("FROM ");
        r.sql
            .push_str(&r.dialect.quote_identifier(&self.table.name));
        if let Some(alias) = &self.alias {
            r.sql.push_str(" AS ");
            r.sql.push_str(&r.dialect.quote_identifier(alias));
        }
    }
}

impl Render for JoinClause {
    fn render(&self, r: &mut Renderer) {
        let join_str = match self.kind {
            JoinKind::Inner => "INNER JOIN",
            JoinKind::Left => "LEFT JOIN",
            JoinKind::Right => "RIGHT JOIN",
            JoinKind::Full => "FULL OUTER JOIN",
        };
        r.sql.push_str(&format!("{join_str} "));
        r.sql
            .push_str(&r.dialect.quote_identifier(&self.table.name));
        if let Some(alias) = &self.alias {
            r.sql.push_str(" AS ");
            r.sql.push_str(&r.dialect.quote_identifier(alias));
        }
        r.sql.push_str(" ON ");
        self.on.render(r);
    }
}

impl Render for OrderByExpr {
    fn render(&self, r: &mut Renderer) {
        self.expr.render(r);
        if let Some(dir) = &self.direction {
            let dir_str = match dir {
                OrderDir::Asc => "ASC",
                OrderDir::Desc => "DESC",
            };
            r.sql.push(' ');
            r.sql.push_str(dir_str);
        }
    }
}

#[cfg(test)]
mod tests {
    use data_model::core::value::Value;

    use crate::{
        ast::{
            common::{JoinKind, OrderDir, TableRef},
            expr::{BinaryOp, BinaryOperator, Expr, FunctionCall, Ident},
            select::{FromClause, JoinClause, OrderByExpr, Select},
        },
        dialect::{MySql, Postgres},
        render::{Render, Renderer},
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

    #[test]
    fn test_simple_select_postgres() {
        let ast = Select {
            columns: vec![ident("id"), ident("name")],
            from: Some(FromClause {
                table: TableRef {
                    schema: None,
                    name: "users".to_string(),
                },
                alias: None,
            }),
            where_clause: Some(Expr::BinaryOp(Box::new(BinaryOp {
                left: ident("id"),
                op: BinaryOperator::Eq,
                right: value(Value::Int(123)),
            }))),
            ..Default::default()
        };

        let dialect = Postgres;
        let mut renderer = Renderer::new(&dialect);
        ast.render(&mut renderer);
        let (sql, params) = renderer.finish();

        assert_eq!(sql, r#"SELECT "id", "name" FROM "users" WHERE ("id" = $1)"#);
        assert_eq!(params, vec![Value::Int(123)]);
    }

    #[test]
    fn test_simple_select_mysql() {
        let ast = Select {
            columns: vec![ident("id"), ident("name")],
            from: Some(FromClause {
                table: TableRef {
                    schema: None,
                    name: "users".to_string(),
                },
                alias: None,
            }),
            where_clause: Some(Expr::BinaryOp(Box::new(BinaryOp {
                left: ident("id"),
                op: BinaryOperator::Eq,
                right: value(Value::String("abc".to_string())),
            }))),
            ..Default::default()
        };

        let dialect = MySql;
        let mut renderer = Renderer::new(&dialect);
        ast.render(&mut renderer);
        let (sql, params) = renderer.finish();

        assert_eq!(sql, "SELECT `id`, `name` FROM `users` WHERE (`id` = ?)");
        assert_eq!(params, vec![Value::String("abc".to_string())]);
    }

    #[test]
    fn test_complex_select_postgres() {
        let ast = Select {
            columns: vec![
                qual_ident("u", "id"),
                Expr::Alias {
                    expr: Box::new(Expr::FunctionCall(FunctionCall {
                        name: "COUNT".to_string(),
                        args: vec![qual_ident("p", "id")],
                        wildcard: false,
                    })),
                    alias: "post_count".to_string(),
                },
            ],
            from: Some(FromClause {
                table: TableRef {
                    schema: None,
                    name: "users".to_string(),
                },
                alias: Some("u".to_string()),
            }),
            joins: vec![JoinClause {
                kind: JoinKind::Left,
                table: TableRef {
                    schema: None,
                    name: "posts".to_string(),
                },
                alias: Some("p".to_string()),
                on: Expr::BinaryOp(Box::new(BinaryOp {
                    left: qual_ident("u", "id"),
                    op: BinaryOperator::Eq,
                    right: qual_ident("p", "user_id"),
                })),
            }],
            where_clause: Some(Expr::BinaryOp(Box::new(BinaryOp {
                left: qual_ident("u", "status"),
                op: BinaryOperator::NotEq,
                right: value(Value::String("inactive".to_string())),
            }))),
            order_by: vec![OrderByExpr {
                expr: qual_ident("u", "created_at"),
                direction: Some(OrderDir::Desc),
            }],
            limit: Some(value(Value::Int(10))),
            offset: Some(value(Value::Int(20))),
        };

        let dialect = Postgres;
        let mut renderer = Renderer::new(&dialect);
        ast.render(&mut renderer);
        let (sql, params) = renderer.finish();

        let expected_sql = r#"SELECT "u"."id", COUNT("p"."id") AS "post_count" FROM "users" AS "u" LEFT JOIN "posts" AS "p" ON ("u"."id" = "p"."user_id") WHERE ("u"."status" <> $1) ORDER BY "u"."created_at" DESC LIMIT $2 OFFSET $3"#;
        assert_eq!(sql, expected_sql);
        assert_eq!(
            params,
            vec![
                Value::String("inactive".to_string()),
                Value::Int(10),
                Value::Int(20)
            ]
        );
    }
}
