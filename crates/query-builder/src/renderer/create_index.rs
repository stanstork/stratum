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
        if self.concurrent && r.dialect.supports_index_concurrently() {
            r.sql.push_str("CONCURRENTLY ");
        }
        if self.if_not_exists && r.dialect.supports_index_if_not_exists() {
            r.sql.push_str("IF NOT EXISTS ");
        }
        r.sql.push_str(&r.dialect.quote_identifier(&self.name));
        r.sql.push_str(" ON ");
        r.render_table_ref(&self.table);

        let method_before = r.dialect.index_method_before_cols();

        // PostgreSQL: `ON tbl USING btree (col)`
        if method_before && let Some(method) = &self.index_type {
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

        // MySQL: `ON tbl (col) USING BTREE`
        if !method_before && let Some(method) = &self.index_type {
            r.sql.push_str(" USING ");
            r.sql.push_str(method);
        }

        // WHERE clause (partial index) - PostgreSQL only.
        if let Some(condition) = &self.condition
            && r.dialect.supports_partial_index()
        {
            r.sql.push_str(" WHERE ");
            r.sql.push_str(condition);
        }

        r.sql.push(';');
    }
}

fn render_index_column(col: &IndexColumnExpr, r: &mut Renderer) {
    r.sql.push_str(&r.dialect.quote_identifier(&col.expr));

    // Key prefix, e.g. MySQL `col`(255) - must directly follow the identifier.
    if let Some(prefix) = col.prefix_length
        && r.dialect.supports_index_prefix()
    {
        r.sql.push('(');
        r.sql.push_str(&prefix.to_string());
        r.sql.push(')');
    }

    if let Some(order) = &col.sort_order {
        r.sql.push(' ');
        r.sql.push_str(order);
    }
    if let Some(nulls) = &col.nulls
        && r.dialect.supports_index_nulls()
    {
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

    /// MySQL's CREATE INDEX is narrower than PostgreSQL's: no IF NOT EXISTS,
    /// no CONCURRENTLY, no partial WHERE, no NULLS ordering, and `USING` goes
    /// after the column list.
    #[test]
    fn test_render_mysql_drops_pg_only_clauses() {
        use crate::dialect::MySql;

        let ast = CreateIndex {
            name: "idx_fk_customer_id".to_string(),
            table: TableRef {
                schema: None,
                name: "payment".to_string(),
            },
            columns: vec![IndexColumnExpr {
                expr: "customer_id".to_string(),
                sort_order: Some("DESC".to_string()),
                nulls: Some("NULLS LAST".to_string()),
                prefix_length: None,
            }],
            unique: false,
            if_not_exists: true,
            concurrent: true,
            index_type: Some("btree".to_string()),
            condition: Some("active = true".to_string()),
        };

        let dialect = MySql;
        let mut renderer = Renderer::new(&dialect);
        ast.render(&mut renderer);
        let (sql, _) = renderer.finish();

        assert_eq!(
            sql,
            "CREATE INDEX `idx_fk_customer_id` ON `payment` (`customer_id` DESC) USING btree;"
        );
        assert!(!sql.contains("IF NOT EXISTS"));
        assert!(!sql.contains("CONCURRENTLY"));
        assert!(!sql.contains("WHERE"));
        assert!(!sql.contains("NULLS"));
    }

    /// MySQL needs a key prefix to index TEXT/BLOB columns; PostgreSQL has no
    /// such syntax and must never emit one.
    #[test]
    fn test_render_index_column_prefix() {
        use crate::dialect::MySql;

        let ast = || CreateIndex {
            name: "idx_last_name".to_string(),
            table: TableRef {
                schema: None,
                name: "customer".to_string(),
            },
            columns: vec![IndexColumnExpr {
                expr: "last_name".to_string(),
                sort_order: None,
                nulls: None,
                prefix_length: Some(255),
            }],
            unique: false,
            if_not_exists: false,
            concurrent: false,
            index_type: Some("btree".to_string()),
            condition: None,
        };

        let my = MySql;
        let mut r = Renderer::new(&my);
        ast().render(&mut r);
        assert_eq!(
            r.finish().0,
            "CREATE INDEX `idx_last_name` ON `customer` (`last_name`(255)) USING btree;"
        );

        // PostgreSQL ignores the prefix entirely.
        let pg = Postgres;
        let mut r = Renderer::new(&pg);
        ast().render(&mut r);
        let sql = r.finish().0;
        assert!(!sql.contains("(255)"), "got: {sql}");
    }

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
                prefix_length: None,
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
                prefix_length: None,
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
                    prefix_length: None,
                },
                IndexColumnExpr {
                    expr: "created_at".to_string(),
                    sort_order: Some("DESC".to_string()),
                    nulls: None,
                    prefix_length: None,
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
