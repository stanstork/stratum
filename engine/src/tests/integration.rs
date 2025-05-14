#[cfg(test)]
mod tests {
    use crate::{
        runner::run,
        tests::{pg_pool, reset_postgres_schema, TEST_MYSQL_URL, TEST_PG_URL},
    };
    use smql::parser::parse;

    // Test Settings: Default (no special flags).
    // Scenario: The target table does not exist in Postgres, and no setting to create it is specified.
    // Expected Outcome: The test should pass without creating the table in Postgres.
    #[tokio::test]
    async fn tc01() {
        // Reset PG
        reset_postgres_schema().await;

        // Build an SMQL plan pointing at your DBs
        let smql = r#"
            CONNECTIONS(
                SOURCE(MYSQL,  "{mysq_url}"),
                DESTINATION(POSTGRES, "{pg_url}")
            );
            MIGRATE(
                SOURCE(TABLE, actor) -> DEST(TABLE, actor) []
            );
        "#
        .replace("{mysq_url}", TEST_MYSQL_URL)
        .replace("{pg_url}", TEST_PG_URL);

        let plan = parse(&smql).expect("parse smql");

        // Run the migration
        run(plan).await.expect("migration ran");

        // Assert on the Postgres side
        let pg: sqlx::Pool<sqlx::Postgres> = pg_pool().await;
        let (exists,): (bool,) = sqlx::query_as(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = 'actor'
            );
        "#,
        )
        .fetch_one(&pg)
        .await
        .expect("check table exists");

        assert!(!exists, "actor table should not exist in Postgres");
    }
}
