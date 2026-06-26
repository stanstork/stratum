//! MySQL -> WASM sink.

#[cfg(test)]
mod tests {
    use crate::{
        plugins::fixture,
        reset_postgres_schema,
        utils::{DbType, get_row_count, run_smql},
    };
    use tracing_test::traced_test;

    /// Build a `mysql -> wasm sink` SMQL doc draining `actor` into `test_sink`
    /// (declared `sink`) with `expect` rows. Only `id` is produced, matching the
    /// sink's declared input.
    fn smql(expect: i64) -> String {
        format!(
            r#"
            plugin "sink" {{
                path = "{plugin}"
                config {{ expect = "{expect}" }}
            }}

            connection "mysql_source" {{
                driver = "mysql"
                url    = "mysql://sakila_user:qwerty123@localhost:3306/sakila"
            }}
            connection "wasm_sink" {{ driver = "wasm" plugin = "sink" }}

            pipeline "drain_actor" {{
                from {{ connection = connection.mysql_source table = "actor" }}
                to   {{ connection = connection.wasm_sink   table = "sink" }}

                select {{ id = actor.actor_id }}

                settings {{ batch_size = 64 }}
            }}
            "#,
            plugin = fixture("test_sink.wasm"),
            expect = expect,
        )
    }

    /// Every source row drains through the sink: with `expect` = the source row
    /// count, the sink's finalize check passes and the migration succeeds.
    #[traced_test]
    #[tokio::test]
    async fn wasm_sink_receives_all_rows() {
        // Sink has no DB side effect; reset only to clear sled state between runs.
        reset_postgres_schema().await;

        let total = get_row_count("actor", "sakila", DbType::MySql).await;

        run_smql(&smql(total), false)
            .await
            .expect("sink should receive exactly the source row count");
    }

    /// The finalize hook actually runs and observes the true total: an `expect`
    /// that doesn't match the source count fails the migration.
    #[traced_test]
    #[tokio::test]
    async fn wasm_sink_finalize_observes_wrong_count() {
        reset_postgres_schema().await;

        let total = get_row_count("actor", "sakila", DbType::MySql).await;

        let result = run_smql(&smql(total + 1), false).await;
        assert!(
            result.is_err(),
            "finalize should fail when the received total != expect (proves finalize ran)"
        );
    }
}
