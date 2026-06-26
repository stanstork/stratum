//! WASM source -> WASM sink (fully plugin-driven, no database).

#[cfg(test)]
mod tests {
    use crate::{plugins::fixture, reset_postgres_schema, utils::run_smql};
    use tracing_test::traced_test;

    /// Build a `wasm source -> wasm sink` SMQL doc: `test_source` emits `total`
    /// rows in pages of `page_size`; `test_sink` asserts it received `expect`.
    fn smql(total: u64, page_size: u64, expect: u64) -> String {
        format!(
            r#"
            plugin "feed" {{
                path = "{source}"
                config {{ total = "{total}" page_size = "{page}" }}
            }}
            plugin "drain" {{
                path = "{sink}"
                config {{ expect = "{expect}" }}
            }}

            connection "src" {{ driver = "wasm" plugin = "feed"  }}
            connection "dst" {{ driver = "wasm" plugin = "drain" }}

            pipeline "pump" {{
                from {{ connection = connection.src table = "counter" }}
                to   {{ connection = connection.dst table = "sink" }}

                select {{ id = counter.id }}

                settings {{ batch_size = 4 }}
            }}
            "#,
            source = fixture("test_source.wasm"),
            sink = fixture("test_sink.wasm"),
            total = total,
            page = page_size,
            expect = expect,
        )
    }

    /// All generated rows flow source -> sink (default-style total of 10).
    #[traced_test]
    #[tokio::test]
    async fn wasm_source_to_sink_drains_all_rows() {
        reset_postgres_schema().await;

        run_smql(&smql(10, 3, 10), false)
            .await
            .expect("all source rows should reach the sink");
    }

    /// Both plugins honor their config, and cursor paging delivers every row
    /// exactly once (7 rows over page_size 3 -> 3 + 3 + 1; sink expects 7).
    #[traced_test]
    #[tokio::test]
    async fn wasm_source_to_sink_respects_config_and_paging() {
        reset_postgres_schema().await;

        run_smql(&smql(7, 3, 7), false)
            .await
            .expect("paged source rows should all reach the sink");
    }

    /// The end-to-end count is real: if the sink expects more than the source
    /// emits, finalize fails and the migration errors.
    #[traced_test]
    #[tokio::test]
    async fn wasm_sink_detects_count_mismatch() {
        reset_postgres_schema().await;

        let result = run_smql(&smql(5, 2, 6), false).await;
        assert!(
            result.is_err(),
            "sink expecting 6 but source emitting 5 should fail the migration"
        );
    }
}
