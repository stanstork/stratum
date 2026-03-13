use crate::{
    ast::create_index::{CreateIndex, IndexColumnExpr},
    renderer::{Render, Renderer},
};

impl Render for CreateIndex {
    fn render(&self, r: &mut Renderer) {
        r.sql.push_str("CREATE ");
        if self.unique {
            r.sql.push_str("UNIQUE ");
        }
        r.sql.push_str("INDEX ");
        if self.concurrent {
            r.sql.push_str("CONCURRENTLY ");
        }
        if self.if_not_exists {
            r.sql.push_str("IF NOT EXISTS ");
        }
        r.sql.push_str(&r.dialect.quote_identifier(&self.name));
        r.sql.push_str(" ON ");
        r.render_table_ref(&self.table);

        // USING method
        if let Some(method) = &self.index_type {
            r.sql.push_str(" USING ");
            r.sql.push_str(method);
        }

        // Column list
        r.sql.push_str(" (");
        for (i, col) in self.columns.iter().enumerate() {
            if i > 0 {
                r.sql.push_str(", ");
            }
            render_index_column(col, r);
        }
        r.sql.push(')');

        // WHERE clause (partial index)
        if let Some(condition) = &self.condition {
            r.sql.push_str(" WHERE ");
            r.sql.push_str(condition);
        }

        r.sql.push(';');
    }
}

fn render_index_column(col: &IndexColumnExpr, r: &mut Renderer) {
    r.sql.push_str(&r.dialect.quote_identifier(&col.expr));
    if let Some(order) = &col.sort_order {
        r.sql.push(' ');
        r.sql.push_str(order);
    }
    if let Some(nulls) = &col.nulls {
        r.sql.push(' ');
        r.sql.push_str(nulls);
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ast::{
            common::TableRef,
            create_index::{CreateIndex, IndexColumnExpr},
        },
        dialect::Postgres,
        renderer::{Render, Renderer},
    };

    #[test]
    fn test_render_simple_index() {
        let ast = CreateIndex {
            name: "idx_users_email".to_string(),
            table: TableRef {
                schema: None,
                name: "users".to_string(),
            },
            columns: vec![IndexColumnExpr {
                expr: "email".to_string(),
                sort_order: None,
                nulls: None,
            }],
            unique: false,
            if_not_exists: true,
            concurrent: false,
            index_type: None,
            condition: None,
        };

        let dialect = Postgres;
        let mut renderer = Renderer::new(&dialect);
        ast.render(&mut renderer);
        let (sql, _) = renderer.finish();

        assert_eq!(
            sql,
            r#"CREATE INDEX IF NOT EXISTS "idx_users_email" ON "users" ("email");"#
        );
    }

    #[test]
    fn test_render_unique_index_with_type_and_condition() {
        let ast = CreateIndex {
            name: "idx_users_active_email".to_string(),
            table: TableRef {
                schema: None,
                name: "users".to_string(),
            },
            columns: vec![IndexColumnExpr {
                expr: "email".to_string(),
                sort_order: Some("ASC".to_string()),
                nulls: Some("NULLS LAST".to_string()),
            }],
            unique: true,
            if_not_exists: false,
            concurrent: false,
            index_type: Some("btree".to_string()),
            condition: Some("active = true".to_string()),
        };

        let dialect = Postgres;
        let mut renderer = Renderer::new(&dialect);
        ast.render(&mut renderer);
        let (sql, _) = renderer.finish();

        assert_eq!(
            sql,
            r#"CREATE UNIQUE INDEX "idx_users_active_email" ON "users" USING btree ("email" ASC NULLS LAST) WHERE active = true;"#
        );
    }

    #[test]
    fn test_render_composite_index() {
        let ast = CreateIndex {
            name: "idx_orders_user_date".to_string(),
            table: TableRef {
                schema: None,
                name: "orders".to_string(),
            },
            columns: vec![
                IndexColumnExpr {
                    expr: "user_id".to_string(),
                    sort_order: None,
                    nulls: None,
                },
                IndexColumnExpr {
                    expr: "created_at".to_string(),
                    sort_order: Some("DESC".to_string()),
                    nulls: None,
                },
            ],
            unique: false,
            if_not_exists: true,
            concurrent: true,
            index_type: None,
            condition: None,
        };

        let dialect = Postgres;
        let mut renderer = Renderer::new(&dialect);
        ast.render(&mut renderer);
        let (sql, _) = renderer.finish();

        assert_eq!(
            sql,
            r#"CREATE INDEX CONCURRENTLY IF NOT EXISTS "idx_orders_user_date" ON "orders" ("user_id", "created_at" DESC);"#
        );
    }
}
