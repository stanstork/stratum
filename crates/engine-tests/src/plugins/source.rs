//! WASM source -> PostgreSQL.

#[cfg(test)]
mod tests {
    use crate::{
        plugins::fixture,
        reset_postgres_schema,
        utils::{
            DbType, assert_table_exists, get_cell_as_string, get_cell_as_usize, get_column_names,
            get_pg_column_type, get_row_count, run_smql,
        },
    };
    use tracing_test::traced_test;

    /// Build a `wasm-source -> postgres` SMQL doc. `config_block` is inserted
    /// verbatim inside the plugin block (use "" for plugin defaults).
    fn smql(dest_table: &str, config_block: &str) -> String {
        format!(
            r#"
            plugin "feed" {{
                path = "{plugin}"
                {config}
            }}

            connection "src" {{ driver = "wasm"     plugin = "feed" }}
            connection "dst" {{ driver = "postgres" url    = "postgres://user:password@localhost:5432/testdb" }}

            pipeline "ingest" {{
                from {{ connection = connection.src table = "counter" }}
                to   {{ connection = connection.dst table = "{dest}" }}

                select {{
                    id    = counter.id
                    label = counter.label
                }}

                settings {{
                    create_missing_tables = true
                }}
            }}
            "#,
            plugin = fixture("test_source.wasm"),
            config = config_block,
            dest = dest_table,
        )
    }

    /// The destination table is created from the plugin's `output_schema` and all
    /// generated rows (default `total = 10`) are migrated.
    #[traced_test]
    #[tokio::test]
    async fn wasm_source_creates_table_and_migrates_rows() {
        reset_postgres_schema().await;

        run_smql(&smql("synth", ""), false)
            .await
            .expect("migration succeeds");

        assert_table_exists("synth", true).await;

        let cols = get_column_names(DbType::Postgres, "testdb", "synth")
            .await
            .unwrap();
        assert_eq!(cols.len(), 2, "expected id + label, got {cols:?}");

        let migrated = get_row_count("synth", "testdb", DbType::Postgres).await;
        assert_eq!(migrated, 10, "default test_source emits 10 rows");
    }

    /// Column types are inferred from the plugin's canonical output types
    /// (i64 -> bigint, string -> text).
    #[traced_test]
    #[tokio::test]
    async fn wasm_source_infers_column_types() {
        reset_postgres_schema().await;

        run_smql(&smql("synth", ""), false)
            .await
            .expect("migration succeeds");

        assert_eq!(get_pg_column_type("synth", "id").await, "bigint");
        assert_eq!(get_pg_column_type("synth", "label").await, "text");
    }

    /// Row contents are correct end-to-end: id `0..total` with label `row-{id}`.
    #[traced_test]
    #[tokio::test]
    async fn wasm_source_row_values_are_correct() {
        reset_postgres_schema().await;

        run_smql(&smql("synth", ""), false)
            .await
            .expect("migration succeeds");

        for id in [0usize, 1, 5, 9] {
            let label = get_cell_as_string(
                &format!("SELECT label FROM synth WHERE id = {id}"),
                "testdb",
                DbType::Postgres,
                "label",
            )
            .await;
            assert_eq!(label, format!("row-{id}"));
        }

        // Sanity: ids form the full 0..=9 contiguous range.
        let max_id = get_cell_as_usize(
            "SELECT MAX(id) AS m FROM synth",
            "testdb",
            DbType::Postgres,
            "m",
        )
        .await;
        assert_eq!(max_id, 9);
    }

    /// Plugin `config` (total/page_size) is honored, and cursor paging consumes
    /// every page (total = 5 over page_size = 2 -> pages of 2 + 2 + 1).
    #[traced_test]
    #[tokio::test]
    async fn wasm_source_respects_config_and_pages_to_completion() {
        reset_postgres_schema().await;

        let cfg = r#"config { total = "5" page_size = "2" }"#;
        run_smql(&smql("synth_cfg", cfg), false)
            .await
            .expect("migration succeeds");

        let migrated = get_row_count("synth_cfg", "testdb", DbType::Postgres).await;
        assert_eq!(migrated, 5, "config total should cap the row count");
    }
}
