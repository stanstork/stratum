#[cfg(test)]
mod tests {
    use crate::tests::{
        reset_postgres_schema,
        utils::{assert_table_exists, get_row_count, run_smql, DbType},
    };
    use tracing_test::traced_test;

    // Test Settings: Default (no special flags).
    // Scenario: The target table does not exist in Postgres, and no setting to create it is specified.
    // Expected Outcome: The test should pass without creating the table in Postgres.
    #[traced_test]
    #[tokio::test]
    async fn tc01() {
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

    #[traced_test]
    #[tokio::test]
    async fn tc03() {
        reset_postgres_schema().await;

        let tmpl = r#"
            CONNECTIONS(
                SOURCE(MYSQL,  "{mysq_url}"),
                DESTINATION(POSTGRES, "{pg_url}")
            );
            MIGRATE(
                SOURCE(TABLE, actor) -> DEST(TABLE, actor) [
                    SETTINGS(
                        CREATE_MISSING_TABLES=TRUE,
                        CREATE_MISSING_COLUMNS=TRUE
                    )
                ]
            );
        "#;
    }
}
