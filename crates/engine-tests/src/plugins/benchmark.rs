//! Performance benchmark - migration with vs. without a plugin.
//!
//! cargo test -p engine-tests plugins::perf_benchmark -- --test-threads=1 --nocapture

#[cfg(test)]
mod tests {
    use crate::{
        plugins::fixture,
        reset_postgres_schema,
        utils::{DbType, get_row_count, run_smql},
    };
    use std::time::{Duration, Instant};

    /// Generous upper bound on the plugin run - only catches hangs / pathological
    /// regressions, not normal variance.
    const MAX_PLUGIN_RUN: Duration = Duration::from_secs(120);

    fn baseline_smql(dest: &str) -> String {
        format!(
            r#"
            connection "src" {{ driver = "mysql"    url = "mysql://sakila_user:qwerty123@localhost:3306/sakila" }}
            connection "dst" {{ driver = "postgres" url = "postgres://user:password@localhost:5432/testdb" }}

            pipeline "bench" {{
                from {{ connection = connection.src table = "payment" }}
                to   {{ connection = connection.dst table = "{dest}" }}

                select {{
                    payment_id = payment.payment_id
                    amount     = payment.amount
                }}

                settings {{
                    create_missing_tables = true
                    copy_columns          = "MAP_ONLY"
                    batch_size            = 500
                }}
            }}
            "#,
            dest = dest,
        )
    }

    fn plugin_smql(dest: &str) -> String {
        format!(
            r#"
            plugin "adder" {{ path = "{plugin}" }}

            connection "src" {{ driver = "mysql"    url = "mysql://sakila_user:qwerty123@localhost:3306/sakila" }}
            connection "dst" {{ driver = "postgres" url = "postgres://user:password@localhost:5432/testdb" }}

            pipeline "bench" {{
                from {{ connection = connection.src table = "payment" }}
                to   {{ connection = connection.dst table = "{dest}" }}

                select {{
                    payment_id = payment.payment_id
                    sum        = plugin.adder({{ a: payment.amount, b: payment.amount }})
                }}

                settings {{
                    create_missing_tables = true
                    copy_columns          = "MAP_ONLY"
                    batch_size            = 500
                }}
            }}
            "#,
            plugin = fixture("test_transform.wasm"),
            dest = dest,
        )
    }

    /// Reset, run the SMQL, and return the wall-clock duration.
    async fn time_run(smql: &str) -> Duration {
        reset_postgres_schema().await;
        let start = Instant::now();
        run_smql(smql, false).await.expect("migration failed");
        start.elapsed()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn benchmark_plugin_transform_overhead() {
        let src = get_row_count("payment", "sakila", DbType::MySql).await;

        // Baseline: plain column copy.
        let base = time_run(&baseline_smql("bench_baseline")).await;
        let base_rows = get_row_count("bench_baseline", "testdb", DbType::Postgres).await;

        // With a per-row Rust transform plugin.
        let plug = time_run(&plugin_smql("bench_plugin")).await;
        let plug_rows = get_row_count("bench_plugin", "testdb", DbType::Postgres).await;

        // Correctness: both migrate every row.
        assert_eq!(base_rows, src, "baseline should migrate all rows");
        assert_eq!(plug_rows, src, "plugin run should migrate all rows");

        let ratio = plug.as_secs_f64() / base.as_secs_f64().max(1e-6);
        eprintln!(
            "\n[bench] payment rows: {src}\n[bench] baseline:    {base:?}\n[bench] with plugin: {plug:?}\n[bench] overhead:    {ratio:.2}x ({:.1} us/row added)\n",
            (plug.saturating_sub(base)).as_micros() as f64 / src as f64,
        );

        // Loose sanity gate (not a strict perf assertion).
        assert!(
            plug < MAX_PLUGIN_RUN,
            "plugin run took {plug:?}, exceeding the {MAX_PLUGIN_RUN:?} sanity bound"
        );
    }
}
