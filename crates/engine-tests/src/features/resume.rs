#[cfg(test)]
mod tests {
    use crate::harness::ppl::feature_ppl;
    use crate::{
        harness::runner::{
            DbType, get_row_count, run_ppl, run_ppl_with_pause, run_ppl_with_pause_mode,
            run_verify_ppl,
        },
        reset_postgres_schema,
    };
    use tracing_test::traced_test;

    /// MySQL `film` (1000 rows, film_id PK) -> Postgres, only the mapped columns
    /// (avoids ENUM/SET so the created table is simple). Small batches so a pause
    /// reliably lands mid-migration.
    fn ppl(dest: &str) -> String {
        feature_ppl(&format!(
            r#"

            pipeline "copy_film" {{
                from {{ connection = connection.src table = "film" }}
                to   {{ connection = connection.dst table = "{dest}" }}

                select {{
                    film_id = film.film_id
                    title   = film.title
                }}

                settings {{
                    create_missing_tables = true
                    batch_size            = 20
                }}
            }}
            "#,
            dest = dest,
        ))
    }

    async fn distinct_film_ids(table: &str) -> i64 {
        let pg = crate::pg_pool().await;
        pg.query_one(&format!("SELECT COUNT(DISTINCT film_id) FROM {table}"), &[])
            .await
            .unwrap()
            .get(0)
    }

    /// Pause mid-migration, then resume: every source row lands exactly once
    /// (no gaps, no duplicates).
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn db_migration_resumes_after_pause() {
        reset_postgres_schema().await;
        let ppl = ppl("film_resume");
        let total = get_row_count("film", "sakila", DbType::MySql).await;

        // Run 1: stop gracefully after partial progress (do NOT reset afterward).
        run_ppl_with_pause(&ppl, "film_resume", 100).await;
        let partial = get_row_count("film_resume", "testdb", DbType::Postgres).await;
        assert!(
            partial > 0 && partial < total,
            "expected partial progress after pause, got {partial} of {total}"
        );

        // Run 2: resume from the checkpoint (same plan, state intact).
        run_ppl(&ppl, false).await.expect("resume run failed");

        let final_count = get_row_count("film_resume", "testdb", DbType::Postgres).await;
        assert_eq!(final_count, total, "all rows present after resume");
        assert_eq!(
            distinct_film_ids("film_resume").await,
            total,
            "no duplicate or missing rows after resume"
        );
    }

    /// Resuming/re-running an already-completed migration is a no-op: the row
    /// count is unchanged and no duplicates are introduced.
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn completed_db_migration_rerun_is_noop() {
        reset_postgres_schema().await;
        let ppl = ppl("film_done");
        let total = get_row_count("film", "sakila", DbType::MySql).await;

        run_ppl(&ppl, false).await.expect("first run failed");
        assert_eq!(
            get_row_count("film_done", "testdb", DbType::Postgres).await,
            total
        );

        // Re-run the identical plan without clearing state.
        run_ppl(&ppl, false).await.expect("rerun failed");

        assert_eq!(
            get_row_count("film_done", "testdb", DbType::Postgres).await,
            total,
            "re-running a completed migration must not change the row count"
        );
        assert_eq!(distinct_film_ids("film_done").await, total, "no duplicates");
    }

    /// Pausing and resuming twice still converges to the full, duplicate-free
    /// result (multiple checkpoints exercised).
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn db_migration_survives_repeated_pauses() {
        reset_postgres_schema().await;
        let ppl = ppl("film_multi");
        let total = get_row_count("film", "sakila", DbType::MySql).await;

        run_ppl_with_pause(&ppl, "film_multi", 100).await;
        run_ppl_with_pause(&ppl, "film_multi", 400).await;
        run_ppl(&ppl, false).await.expect("final resume failed");

        assert_eq!(
            get_row_count("film_multi", "testdb", DbType::Postgres).await,
            total
        );
        assert_eq!(
            distinct_film_ids("film_multi").await,
            total,
            "no duplicates"
        );
    }

    /// An interrupted integrity run still produces a receipt for the *whole*
    /// table once resumed.
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn integrity_receipt_survives_pause_and_resume() {
        reset_postgres_schema().await;
        let ppl = ppl("film_integrity_resume");
        let total = get_row_count("film", "sakila", DbType::MySql).await;

        // Run 1: hash part of the table, then stop gracefully.
        run_ppl_with_pause_mode(&ppl, "film_integrity_resume", 100, true).await;
        let partial = get_row_count("film_integrity_resume", "testdb", DbType::Postgres).await;
        assert!(
            partial > 0 && partial < total,
            "expected partial progress after pause, got {partial} of {total}"
        );

        // Run 2: resume with integrity still on, then verify.
        run_ppl(&ppl, true).await.expect("resume run failed");
        assert_eq!(
            get_row_count("film_integrity_resume", "testdb", DbType::Postgres).await,
            total
        );

        run_verify_ppl(&ppl)
            .await
            .expect("verify should match after a resumed integrity run");
    }
}
