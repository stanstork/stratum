#[cfg(test)]
mod tests {
    use crate::{
        reset_postgres_schema,
        utils::{DbType, get_row_count, run_smql, run_smql_with_pause},
    };
    use tracing_test::traced_test;

    /// MySQL `film` (1000 rows, film_id PK) -> Postgres, only the mapped columns
    /// (avoids ENUM/SET so the created table is simple). Small batches so a pause
    /// reliably lands mid-migration.
    fn smql(dest: &str) -> String {
        format!(
            r#"
            connection "src" {{ driver = "mysql"    url = "mysql://sakila_user:qwerty123@localhost:3306/sakila" }}
            connection "dst" {{ driver = "postgres" url = "postgres://user:password@localhost:5432/testdb" }}

            pipeline "copy_film" {{
                from {{ connection = connection.src table = "film" }}
                to   {{ connection = connection.dst table = "{dest}" }}

                select {{
                    film_id = film.film_id
                    title   = film.title
                }}

                settings {{
                    create_missing_tables = true
                    copy_columns          = "MAP_ONLY"
                    batch_size            = 20
                }}
            }}
            "#,
            dest = dest,
        )
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
        let smql = smql("film_resume");
        let total = get_row_count("film", "sakila", DbType::MySql).await;

        // Run 1: stop gracefully after partial progress (do NOT reset afterward).
        run_smql_with_pause(&smql, "film_resume", 100).await;
        let partial = get_row_count("film_resume", "testdb", DbType::Postgres).await;
        assert!(
            partial > 0 && partial < total,
            "expected partial progress after pause, got {partial} of {total}"
        );

        // Run 2: resume from the checkpoint (same plan, state intact).
        run_smql(&smql, false).await.expect("resume run failed");

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
        let smql = smql("film_done");
        let total = get_row_count("film", "sakila", DbType::MySql).await;

        run_smql(&smql, false).await.expect("first run failed");
        assert_eq!(
            get_row_count("film_done", "testdb", DbType::Postgres).await,
            total
        );

        // Re-run the identical plan without clearing state.
        run_smql(&smql, false).await.expect("rerun failed");

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
        let smql = smql("film_multi");
        let total = get_row_count("film", "sakila", DbType::MySql).await;

        run_smql_with_pause(&smql, "film_multi", 100).await;
        run_smql_with_pause(&smql, "film_multi", 400).await;
        run_smql(&smql, false).await.expect("final resume failed");

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
}
