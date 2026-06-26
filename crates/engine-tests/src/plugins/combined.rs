//! All plugin roles in one pipeline.

#[cfg(test)]
mod tests {
    use crate::{
        plugins::fixture,
        reset_postgres_schema,
        utils::{
            DbType, assert_table_exists, get_cell_as_f64, get_column_names, get_pg_column_type,
            get_row_count, run_smql,
        },
    };
    use tracing_test::traced_test;

    /// Build a `wasm source -> postgres` pipeline that exercises a transform
    /// plugin (select) and a filter plugin (validate) together. `total` source
    /// rows are generated.
    fn smql(dest_table: &str, total: u64) -> String {
        format!(
            r#"
            plugin "feed"     {{ path = "{source}" config {{ total = "{total}" page_size = "3" }} }}
            plugin "adder"    {{ path = "{transform}" }}
            plugin "posfilter" {{ path = "{filter}" }}

            connection "src" {{ driver = "wasm"     plugin = "feed" }}
            connection "dst" {{ driver = "postgres" url    = "postgres://user:password@localhost:5432/testdb" }}

            pipeline "enrich" {{
                from {{ connection = connection.src table = "counter" }}
                to   {{ connection = connection.dst table = "{dest}" }}

                select {{
                    id  = counter.id
                    sum = plugin.adder({{ a: counter.id, b: counter.id }})
                }}

                validate {{
                    rule "positive_id" {{
                        filter  = plugin.posfilter({{ value: counter.id }})
                        on_fail = skip
                    }}
                }}

                settings {{
                    create_missing_tables = true
                    copy_columns          = "MAP_ONLY"
                }}
            }}
            "#,
            source = fixture("test_source.wasm"),
            transform = fixture("test_transform.wasm"),
            filter = fixture("test_filter.wasm"),
            dest = dest_table,
            total = total,
        )
    }

    /// Full pipeline: the table is created with the mapped column + transform
    /// output, the filter drops `id = 0`, and the transform values are correct.
    #[traced_test]
    #[tokio::test]
    async fn combined_source_transform_filter_into_postgres() {
        reset_postgres_schema().await;

        // Source emits ids 0..=9 (10 rows); filter drops id 0 -> 9 rows.
        run_smql(&smql("enriched", 10), false)
            .await
            .expect("migration succeeds");

        assert_table_exists("enriched", true).await;

        // MAP_ONLY: only the mapped id + the transform output `sum`.
        let cols = get_column_names(DbType::Postgres, "testdb", "enriched")
            .await
            .unwrap();
        assert_eq!(cols.len(), 2, "expected id + sum, got {cols:?}");
        assert_eq!(get_pg_column_type("enriched", "id").await, "bigint");
        assert_eq!(
            get_pg_column_type("enriched", "sum").await,
            "double precision"
        );

        // Filter dropped exactly id = 0.
        let migrated = get_row_count("enriched", "testdb", DbType::Postgres).await;
        assert_eq!(migrated, 9, "filter should drop the single id = 0 row");

        let zero_rows = get_row_count_where("enriched", "id = 0").await;
        assert_eq!(zero_rows, 0, "id = 0 should have been filtered out");

        // Transform computed sum = id + id for the surviving rows.
        for id in [1, 5, 9] {
            let sum = get_cell_as_f64(
                &format!("SELECT sum FROM enriched WHERE id = {id}"),
                "testdb",
                DbType::Postgres,
                "sum",
            )
            .await;
            assert!(
                (sum - (2 * id) as f64).abs() < 1e-6,
                "id {id}: expected sum {}, got {sum}",
                2 * id
            );
        }
    }

    /// The filter removes only the rows it rejects (id = 0), independent of total.
    #[traced_test]
    #[tokio::test]
    async fn combined_filter_drops_only_failing_rows() {
        reset_postgres_schema().await;

        // ids 0..=5 (6 rows); filter drops id 0 -> 5 rows.
        run_smql(&smql("enriched_small", 6), false)
            .await
            .expect("migration succeeds");

        let migrated = get_row_count("enriched_small", "testdb", DbType::Postgres).await;
        assert_eq!(migrated, 5);
    }

    /// Count rows in a Postgres table matching a WHERE clause.
    async fn get_row_count_where(table: &str, predicate: &str) -> i64 {
        let pg = crate::pg_pool().await;
        let sql = format!("SELECT COUNT(*) FROM {table} WHERE {predicate}");
        pg.query_one(&sql, &[]).await.unwrap().get(0)
    }
}
