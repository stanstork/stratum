//! MySQL -> PostgreSQL with a JS filter plugin.

#[cfg(test)]
mod tests {
    use crate::{
        plugins::fixture,
        reset_postgres_schema,
        utils::{DbType, assert_table_exists, get_cell_as_usize, get_row_count, run_smql},
    };
    use tracing_test::traced_test;

    /// Build an SMQL doc for `customer -> <dest>` with a single filter rule. The
    /// JS `test_filter` plugin is declared as `positive`.
    fn smql(dest_table: &str, filter_field: &str, on_fail: &str) -> String {
        format!(
            r#"
            plugin "positive" {{ path = "{plugin}" }}

            connection "mysql_source" {{
                driver = "mysql"
                url    = "mysql://sakila_user:qwerty123@localhost:3306/sakila"
            }}
            connection "pg_destination" {{
                driver = "postgres"
                url    = "postgres://user:password@localhost:5432/testdb"
            }}

            pipeline "filter_customers" {{
                from {{ connection = connection.mysql_source  table = "customer" }}
                to   {{ connection = connection.pg_destination table = "{dest}" }}

                select {{
                    customer_id = customer.customer_id
                    active      = customer.active
                }}

                validate {{
                    rule "must_be_positive" {{
                        filter  = plugin.positive({{ value: customer.{field} }})
                        on_fail = {on_fail}
                    }}
                }}

                settings {{
                    create_missing_tables = true
                    batch_size            = 1000
                    copy_columns          = "MAP_ONLY"
                }}
            }}
            "#,
            plugin = fixture("test_filter_js.wasm"),
            dest = dest_table,
            field = filter_field,
            on_fail = on_fail,
        )
    }

    /// `on_fail = skip` drops the rows the filter rejects (here: `active = 0`),
    /// migrating only the rows that pass.
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn js_filter_skips_rejected_rows() {
        reset_postgres_schema().await;

        run_smql(&smql("customers_active", "active", "skip"), false)
            .await
            .expect("migration succeeds");

        assert_table_exists("customers_active", true).await;

        let expected = get_cell_as_usize(
            "SELECT COUNT(*) c FROM customer WHERE active > 0",
            "sakila",
            DbType::MySql,
            "c",
        )
        .await;
        let migrated = get_row_count("customers_active", "sakila", DbType::Postgres).await;
        assert_eq!(
            migrated, expected as i64,
            "only active customers should migrate"
        );

        let total = get_row_count("customer", "sakila", DbType::MySql).await;
        assert!(
            migrated < total,
            "some rows ({total} total) should have been skipped"
        );
    }

    /// When every row passes the filter (`customer_id > 0` always), all rows are
    /// migrated.
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn js_filter_passes_all_valid_rows() {
        reset_postgres_schema().await;

        run_smql(&smql("customers_all", "customer_id", "skip"), false)
            .await
            .expect("migration succeeds");

        let total = get_row_count("customer", "sakila", DbType::MySql).await;
        let migrated = get_row_count("customers_all", "sakila", DbType::Postgres).await;
        assert_eq!(migrated, total, "no rows should be filtered when all pass");
    }

    /// `on_fail = fail` aborts the pipeline on the first rejected row; with a
    /// single batch covering all customers, nothing is written.
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn js_filter_on_fail_fail_aborts_pipeline() {
        reset_postgres_schema().await;

        // Expected to error out (validation failure is fatal).
        let _ = run_smql(&smql("customers_strict", "active", "fail"), false).await;

        // Table is created during schema setup, but no rows are committed.
        assert_table_exists("customers_strict", true).await;
        let migrated = get_row_count("customers_strict", "sakila", DbType::Postgres).await;
        assert_eq!(
            migrated, 0,
            "fail action should abort before any rows are written"
        );
    }
}
