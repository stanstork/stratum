#[cfg(test)]
mod tests {
    use crate::tests::{
        reset_migration_buffer, reset_postgres_schema,
        utils::{
            assert_column_exists, assert_table_exists, execute, get_row_count, run_smql, DbType,
            ACTORS_TABLE_DDL,
        },
    };
    use tracing_test::traced_test;

    // Test Settings: Default (no special flags).
    // Scenario: The target table does not exist in Postgres, and no setting to create it is specified.
    // Expected Outcome: The test should pass without creating the table in Postgres.
    #[traced_test]
    #[tokio::test]
    async fn tc01() {
        reset_migration_buffer().expect("reset migration buffer");
        reset_postgres_schema().await;

        let tmpl = r#"
            CONNECTIONS(
                SOURCE(MYSQL,  "{mysq_url}"),
                DESTINATION(POSTGRES, "{pg_url}")
            );
            MIGRATE(
                SOURCE(TABLE, actor) -> DEST(TABLE, actor) []
            );
        "#;

        run_smql(tmpl).await;
        assert_table_exists("actor", false).await;
    }

    // Test Settings: CREATE_MISSING_TABLES = TRUE.
    // Scenario: The target table does not exist in Postgres, and the setting to create it is specified.
    // Expected Outcome:
    // - The test should pass.
    // - The table should be created in Postgres.
    // - Data should be copied, and the row count should match the source table.
    #[traced_test]
    #[tokio::test]
    async fn tc02() {
        reset_migration_buffer().expect("reset migration buffer");
        reset_postgres_schema().await;

        let tmpl = r#"
            CONNECTIONS(
                SOURCE(MYSQL,  "{mysq_url}"),
                DESTINATION(POSTGRES, "{pg_url}")
            );
            MIGRATE(
                SOURCE(TABLE, actor) -> DEST(TABLE, actor) [
                    SETTINGS(CREATE_MISSING_TABLES=TRUE)
                ]
            );
        "#;

        run_smql(tmpl).await;
        assert_table_exists("actor", true).await;

        let source_count = get_row_count("actor", DbType::MySql).await;
        let dest_count = get_row_count("actor", DbType::Postgres).await;

        assert_eq!(
            source_count, dest_count,
            "expected row count in source and destination to match"
        );
    }

    // Test Settings: CREATE_MISSING_COLUMNS = TRUE.
    // Scenario:
    // - The target table exists in Postgres, but the required column does not exist.
    // - The setting to create the missing column is specified.
    // Expected Outcome:
    // - The test should pass.
    // - The missing column should be created in Postgres.
    // - Data should be copied, and the row count should match between the source and destination tables.
    // - The new column should be populated with the concatenated values of `first_name` and `last_name`.
    #[traced_test]
    #[tokio::test]
    async fn tc03() {
        reset_migration_buffer().expect("reset migration buffer");
        reset_postgres_schema().await;

        // Create the actor table in Postgres without the full_name column
        execute(ACTORS_TABLE_DDL).await;

        let tmpl = r#"
            CONNECTIONS(
                SOURCE(MYSQL,  "{mysq_url}"),
                DESTINATION(POSTGRES, "{pg_url}")
            );
            MIGRATE(
                SOURCE(TABLE, actor) -> DEST(TABLE, actor) [
                    SETTINGS(CREATE_MISSING_COLUMNS=TRUE),
                    MAP(CONCAT(actor[first_name], actor[last_name]) -> full_name)
                ]
            );
        "#;

        run_smql(tmpl).await;

        let source_count = get_row_count("actor", DbType::MySql).await;
        let dest_count = get_row_count("actor", DbType::Postgres).await;

        assert_eq!(
            source_count, dest_count,
            "expected row count in source and destination to match"
        );

        assert_column_exists("actor", "full_name", true).await;
    }
}
