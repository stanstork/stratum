//! Crash/resume with a WASM source.

#[cfg(test)]
mod tests {
    use crate::{
        plugins::fixture,
        reset_postgres_schema,
        utils::{DbType, get_row_count, run_smql, run_smql_with_pause},
    };

    const TOTAL: i64 = 3000;

    fn smql() -> String {
        format!(
            r#"
            plugin "feed" {{ path = "{plugin}" config {{ total = "{total}" page_size = "50" }} }}

            connection "src" {{ driver = "wasm"     plugin = "feed" }}
            connection "dst" {{ driver = "postgres" url = "postgres://user:password@localhost:5432/testdb" }}

            pipeline "resumable" {{
                from {{ connection = connection.src table = "counter" }}
                to   {{ connection = connection.dst table = "resumed" }}

                select {{
                    id    = counter.id
                    label = counter.label
                }}

                settings {{
                    create_missing_tables = true
                    batch_size            = 50
                }}
            }}
            "#,
            plugin = fixture("test_source.wasm"),
            total = TOTAL,
        )
    }

    /// Pause mid-migration, then resume to completion; every row lands exactly
    /// once. The resume checkpoint is consumer-acked (written only after a batch
    /// is durably written), so batches the producer read ahead but the consumer
    /// hadn't written are simply re-read on resume - no loss, no duplicates.
    #[tokio::test]
    async fn wasm_source_resumes_after_pause() {
        reset_postgres_schema().await;

        // Run 1: stop gracefully after partial progress (do NOT reset afterward).
        run_smql_with_pause(&smql(), "resumed", 200).await;

        let partial = get_row_count("resumed", "testdb", DbType::Postgres).await;
        assert!(
            partial > 0 && partial < TOTAL,
            "expected a partial migration after pause, got {partial} of {TOTAL}"
        );

        // Run 2: resume from the checkpoint (same plan, state intact).
        run_smql(&smql(), false).await.expect("resume run failed");

        // Every source row present exactly once: count and distinct ids match TOTAL,
        // and the id range is contiguous 0..TOTAL-1.
        let final_count = get_row_count("resumed", "testdb", DbType::Postgres).await;
        assert_eq!(
            final_count, TOTAL,
            "all rows should be present after resume"
        );

        let pg = crate::pg_pool().await;
        let distinct: i64 = pg
            .query_one("SELECT COUNT(DISTINCT id) FROM resumed", &[])
            .await
            .unwrap()
            .get(0);
        assert_eq!(distinct, TOTAL, "no duplicate or missing ids after resume");

        let max_id: i64 = pg
            .query_one("SELECT MAX(id) FROM resumed", &[])
            .await
            .unwrap()
            .get(0);
        assert_eq!(max_id, TOTAL - 1, "id range should be contiguous");
    }
}
