//! Memory-limit enforcement (stress).

#[cfg(test)]
mod tests {
    use crate::{
        plugins::fixture,
        reset_postgres_schema,
        utils::{DbType, get_row_count, run_smql},
    };
    use tracing_test::traced_test;

    const LIMIT_64MB: u64 = 64 * 1024 * 1024;

    /// `test_alloc` transform draining `actor` (200 rows) into `<dest>`, capped at
    /// `limit_bytes`, allocating `alloc_mb` MiB per row.
    fn alloc_smql(dest: &str, limit_bytes: u64, alloc_mb: u64) -> String {
        format!(
            r#"
            plugin "al" {{
                path = "{plugin}"
                memory_limit_bytes = {limit}
                config {{ alloc_mb = "{alloc}" }}
            }}

            connection "src" {{ driver = "mysql"    url = "mysql://sakila_user:qwerty123@localhost:3306/sakila" }}
            connection "dst" {{ driver = "postgres" url = "postgres://user:password@localhost:5432/testdb" }}

            pipeline "stress" {{
                from {{ connection = connection.src table = "actor" }}
                to   {{ connection = connection.dst table = "{dest}" }}

                select {{
                    actor_id = actor.actor_id
                    sz       = plugin.al({{ a: actor.actor_id }})
                }}

                settings {{
                    create_missing_tables = true
                    copy_columns          = "MAP_ONLY"
                    batch_size            = 64
                }}
            }}
            "#,
            plugin = fixture("test_alloc.wasm"),
            limit = limit_bytes,
            alloc = alloc_mb,
            dest = dest,
        )
    }

    /// Allocating well under the limit (16 MiB under 64 MiB) succeeds for every row.
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn allocation_under_memory_limit_succeeds() {
        reset_postgres_schema().await;

        run_smql(&alloc_smql("alloc_ok", LIMIT_64MB, 16), false)
            .await
            .expect("migration succeeds");

        let total = get_row_count("actor", "sakila", DbType::MySql).await;
        let migrated = get_row_count("alloc_ok", "testdb", DbType::Postgres).await;
        assert_eq!(migrated, total, "all rows should migrate under the limit");
    }

    /// Allocating over the limit (256 MiB over 64 MiB) is denied: the plugin
    /// traps on every row, the host survives, and nothing is written.
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn allocation_over_memory_limit_is_trapped() {
        reset_postgres_schema().await;

        // Transform traps are non-fatal -> migration completes with rows dropped.
        let _ = run_smql(&alloc_smql("alloc_over", LIMIT_64MB, 256), false).await;

        let migrated = get_row_count("alloc_over", "testdb", DbType::Postgres).await;
        assert_eq!(migrated, 0, "over-limit allocation should trap every row");
    }

    /// An unbounded-allocation plugin is contained: it traps at the cap rather
    /// than exhausting host memory, the migration still completes, and no rows land.
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn runaway_allocation_is_contained() {
        reset_postgres_schema().await;

        let doc = format!(
            r#"
            plugin "hog" {{ path = "{plugin}" memory_limit_bytes = {limit} }}

            connection "src" {{ driver = "mysql"    url = "mysql://sakila_user:qwerty123@localhost:3306/sakila" }}
            connection "dst" {{ driver = "postgres" url = "postgres://user:password@localhost:5432/testdb" }}

            pipeline "stress_hog" {{
                from {{ connection = connection.src table = "actor" }}
                to   {{ connection = connection.dst table = "hog_out" }}

                select {{
                    actor_id = actor.actor_id
                    hog      = plugin.hog({{ a: actor.actor_id }})
                }}

                settings {{
                    create_missing_tables = true
                    copy_columns          = "MAP_ONLY"
                    batch_size            = 64
                }}
            }}
            "#,
            plugin = fixture("test_memory_hog.wasm"),
            limit = 32 * 1024 * 1024,
        );

        let _ = run_smql(&doc, false).await;

        let migrated = get_row_count("hog_out", "testdb", DbType::Postgres).await;
        assert_eq!(
            migrated, 0,
            "runaway plugin should be trapped, nothing written"
        );
    }
}
