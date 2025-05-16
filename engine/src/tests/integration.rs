#[cfg(test)]
mod tests {
    use crate::tests::{
        reset_migration_buffer, reset_postgres_schema,
        utils::{
            assert_column_exists, assert_row_count, assert_table_exists, execute, get_column_names,
            get_row_count, get_table_names, run_smql, DbType, ACTORS_TABLE_DDL,
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

        run_smql(tmpl, "sakila").await;
        assert_table_exists("actor", false).await;
    }

    // Test Settings: CREATE_MISSING_TABLES = TRUE.
    // Scenario: The target table does not exist in Postgres, and the setting to create it is specified.
    // Expected Outcome:
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

        run_smql(tmpl, "sakila").await;

        assert_table_exists("actor", true).await;
        assert_row_count("actor", "sakila", "actor").await;
    }

    // Test Settings: CREATE_MISSING_COLUMNS = TRUE.
    // Scenario:
    // - The target table exists in Postgres, but the required column does not exist.
    // - The setting to create the missing column is specified.
    // Expected Outcome:
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

        run_smql(tmpl, "sakila").await;

        assert_table_exists("actor", true).await;
        assert_row_count("actor", "sakila", "actor").await;
        assert_column_exists("actor", "full_name", true).await;
    }

    // Test Settings: INFER_SCHEMA = TRUE.
    // Scenario:
    // - The target table does not exist in Postgres.
    // - The setting to infer the schema is specified.
    // Expected Outcome:
    // - The full schema should be inferred from the source table, including all foreign key dependencies.
    // - Data should be copied only for the target table and not for the related tables.
    #[traced_test]
    #[tokio::test]
    async fn tc04() {
        reset_migration_buffer().expect("reset migration buffer");
        reset_postgres_schema().await;

        let tmpl = r#"
            CONNECTIONS(
                SOURCE(MYSQL,  "{mysq_url}"),
                DESTINATION(POSTGRES, "{pg_url}")
            );
            MIGRATE(
                SOURCE(TABLE, film) -> DEST(TABLE, film) [SETTINGS(INFER_SCHEMA=TRUE)]
            );
        "#;

        run_smql(tmpl, "sakila").await;

        let source_tables = get_table_names(DbType::MySql, "sakila").await.unwrap();
        let dest_tables = get_table_names(DbType::Postgres, "sakila").await.unwrap();

        for table in source_tables.iter() {
            // Skip the film_text table as it does not have related tables and is not migrated
            if table.eq("film_text") {
                continue;
            }

            assert!(
                dest_tables.contains(table),
                "Table {} not found in destination",
                table
            );
        }
    }

    // Test Settings: INFER_SCHEMA = TRUE, CASCADE_SCHEMA = TRUE.
    // Scenario:
    // - The target table does not exist in Postgres.
    // - The settings to infer the schema and cascade the schema are specified.
    // Expected Outcome:
    // - The full schema should be inferred from the source table, including all foreign key dependencies.
    // - Data should be copied for all related tables.
    // - The row count should match between the source and destination tables for all related tables.
    #[traced_test]
    #[tokio::test]
    #[ignore = "This test is ignored because it infers a large schema and copies a lot of data, so it takes a long time to run. It should be run manually when needed."]
    async fn tc05() {
        reset_migration_buffer().expect("reset migration buffer");
        reset_postgres_schema().await;

        let tmpl = r#"
            CONNECTIONS(
                SOURCE(MYSQL,  "{mysq_url}"),
                DESTINATION(POSTGRES, "{pg_url}")
            );
            MIGRATE(
                SOURCE(TABLE, film) -> DEST(TABLE, film) [
                    SETTINGS(INFER_SCHEMA=TRUE,CASCADE_SCHEMA=TRUE)
                ]
            );
        "#;

        run_smql(tmpl, "sakila").await;

        let tables = get_table_names(DbType::MySql, "sakila").await.unwrap();
        for table in tables.iter() {
            // Skip the film_text table as it does not have related tables and is not migrated
            if table.eq("film_text") {
                continue;
            }
            assert_row_count(table, "sakila", table).await;
        }
    }

    // Test Settings: INFER_SCHEMA = TRUE, CASCADE_SCHEMA = TRUE, IGNORE_CONSTRAINTS = TRUE.
    // Scenario:
    // - The target table does not exist in Postgres.
    // - The settings to infer the schema, cascade the schema, and ignore constraints are specified.
    // Expected Outcome:
    // - The full schema should be inferred from the source table, but constraints should be ignored.
    // - Data should be copied only for the target table, while related tables should remain empty.
    #[traced_test]
    #[tokio::test]
    async fn tc06() {
        reset_migration_buffer().expect("reset migration buffer");
        reset_postgres_schema().await;

        let tmpl = r#"
            CONNECTIONS(
                SOURCE(MYSQL,  "{mysq_url}"),
                DESTINATION(POSTGRES, "{pg_url}")
            );
            MIGRATE(
                SOURCE(TABLE, orders) -> DEST(TABLE, orders) [
                    SETTINGS(INFER_SCHEMA=TRUE,CASCADE_SCHEMA=TRUE,IGNORE_CONSTRAINTS=TRUE)
                ]
            );
        "#;

        run_smql(tmpl, "orders").await;
        assert_row_count("orders", "orders", "orders").await;

        let depndent_tables = vec!["order_items", "products", "users"];
        for table in depndent_tables.iter() {
            let dest_count = get_row_count(table, "orders", DbType::Postgres).await;

            assert_eq!(
                0, dest_count,
                "expected no rows in destination table {}",
                table
            );
        }
    }

    // Test Settings: CREATE_MISSING_TABLES=TRUE, IGNORE_CONSTRAINTS=TRUE, COPY_COLUMNS=MAP_ONLY.
    // Scenario:
    // - The target table does not exist in Postgres.
    // - The settings to create missing tables, ignore constraints, and copy columns (map only) are specified.
    // Expected Outcome:
    // - The table should be created in Postgres.
    // - The destination table should have only one column (`order_id`).
    // - Data should be copied, and the row count should match between the source and destination tables.
    #[traced_test]
    #[tokio::test]
    async fn tc07() {
        reset_migration_buffer().expect("reset migration buffer");
        reset_postgres_schema().await;

        let tmpl = r#"
            CONNECTIONS(
                SOURCE(MYSQL,  "{mysq_url}"),
                DESTINATION(POSTGRES, "{pg_url}")
            );
            MIGRATE(
                SOURCE(TABLE, orders) -> DEST(TABLE, orders_flat) [
                    SETTINGS(CREATE_MISSING_TABLES=TRUE,IGNORE_CONSTRAINTS=TRUE,COPY_COLUMNS=MAP_ONLY),
                    MAP(id->order_id)
                ]
            );
        "#;

        run_smql(tmpl, "orders").await;

        let dest_columns = get_column_names(DbType::Postgres, "orders", "orders_flat")
            .await
            .unwrap();

        assert_row_count("orders", "orders", "orders_flat").await;
        assert_column_exists("orders_flat", "order_id", true).await;
        assert_eq!(
            1,
            dest_columns.len(),
            "expected only one column in destination table"
        );
    }
}
