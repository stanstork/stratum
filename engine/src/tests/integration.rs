#[cfg(test)]
mod tests {
    use crate::tests::{
        reset_migration_buffer, reset_postgres_schema,
        utils::{
            assert_column_exists, assert_row_count, assert_table_exists, execute, fetch_rows,
            get_cell_as_f64, get_cell_as_string, get_column_names, get_row_count, get_table_names,
            run_smql, DbType, ACTORS_TABLE_DDL, ORDERS_FLAT_JOIN_QUERY,
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

    // Test Settings: INFER_SCHEMA = TRUE, CREATE_MISSING_COLUMNS = TRUE.
    // Scenario:
    // - The target table does not exist in Postgres.
    // - The settings to infer the schema and create missing columns are specified.
    // Expected Outcome:
    // - The table should be created in Postgres.
    // - The destination table should include the new column (`order_price_with_tax`).
    // - Data should be copied, and the row count should match between the source and destination tables.
    #[traced_test]
    #[tokio::test]
    async fn tc08() {
        reset_migration_buffer().expect("reset migration buffer");
        reset_postgres_schema().await;

        let tmpl = r#"
            CONNECTIONS(
                SOURCE(MYSQL,  "{mysq_url}"),
                DESTINATION(POSTGRES, "{pg_url}")
            );
            MIGRATE(
                SOURCE(TABLE, orders) -> DEST(TABLE, orders_flat) [
                    SETTINGS(INFER_SCHEMA=TRUE,CREATE_MISSING_COLUMNS=TRUE),
                    MAP(total * 1.4 -> order_price_with_tax)
                ]
            );
        "#;

        run_smql(tmpl, "orders").await;

        assert_row_count("orders", "orders", "orders_flat").await;
        assert_column_exists("orders_flat", "order_price_with_tax", true).await;
    }

    // Test Settings: Default (no special flags).
    // Scenario:
    // - The target table exists in Postgres with the same schema as the source table.
    // - The target table is empty.
    // Expected Outcome:
    // - Data should be copied without any modifications.
    // - The row count should match between the source and destination tables.
    #[traced_test]
    #[tokio::test]
    async fn tc09() {
        reset_migration_buffer().expect("reset migration buffer");
        reset_postgres_schema().await;

        // Create the actor table in Postgres
        execute(ACTORS_TABLE_DDL).await;

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
        assert_row_count("actor", "sakila", "actor").await;
    }

    // Test Settings: INFER_SCHEMA = TRUE, CREATE_MISSING_COLUMNS = TRUE.
    // Scenario:
    // - The target table does not exist in Postgres.
    // - The settings to infer the schema and create missing columns are specified.
    // - Mapping includes an arithmetic expression (`total * 1.4 -> order_price_with_tax`).
    // Expected Outcome:
    // - The table should be created in Postgres.
    // - The destination table should include the new column (`order_price_with_tax`).
    // - Data should be copied, and the row count should match between the source and destination tables.
    // - The new column should be populated with the computed value (`total * 1.4`).
    #[traced_test]
    #[tokio::test]
    async fn tc10() {
        reset_migration_buffer().expect("reset migration buffer");
        reset_postgres_schema().await;

        let tmpl = r#"
            CONNECTIONS(
                SOURCE(MYSQL,  "{mysq_url}"),
                DESTINATION(POSTGRES, "{pg_url}")
            );
            MIGRATE(
                SOURCE(TABLE, orders) -> DEST(TABLE, orders) [
                    SETTINGS(INFER_SCHEMA=TRUE,CREATE_MISSING_COLUMNS=TRUE),
                    MAP(total * 1.4 -> order_price_with_tax)
                ]
            );
        "#;

        run_smql(tmpl, "orders").await;

        assert_row_count("orders", "orders", "orders").await;
        assert_column_exists("orders", "order_price_with_tax", true).await;

        let query = "SELECT * FROM orders WHERE id = 1";

        let src_total = get_cell_as_f64(query, "orders", DbType::MySql, "total").await;
        let dst_tax =
            get_cell_as_f64(query, "orders", DbType::Postgres, "order_price_with_tax").await;

        assert!(
            (dst_tax - (src_total * 1.4)).abs() < f64::EPSILON,
            "expected order_price_with_tax == total*1.4 (got {} vs {})",
            dst_tax,
            src_total * 1.4
        );
    }

    // Test Settings: CREATE_MISSING_TABLES = TRUE, IGNORE_CONSTRAINTS = TRUE.
    // Scenario:
    // - The target table does not exist in Postgres.
    // - The settings to create missing tables and ignore constraints are specified.
    // - Mapping includes a concatenation expression (`CONCAT(actor[first_name], actor[last_name]) -> full_name`).
    // Expected Outcome:
    // - The table should be created in Postgres.
    // - The destination table should include the new column `full_name`.
    // - Data should be copied, and the row count should match between the source and destination tables.
    // - The new column should be populated with the concatenated values of `first_name` and `last_name`.
    #[traced_test]
    #[tokio::test]
    async fn tc11() {
        reset_migration_buffer().expect("reset migration buffer");
        reset_postgres_schema().await;

        let tmpl = r#"
            CONNECTIONS(
                SOURCE(MYSQL,  "{mysq_url}"),
                DESTINATION(POSTGRES, "{pg_url}")
            );
            MIGRATE(
                SOURCE(TABLE, actor) -> DEST(TABLE, actor) [
                    SETTINGS(CREATE_MISSING_TABLES=TRUE,IGNORE_CONSTRAINTS=TRUE),
                    MAP(CONCAT(actor[first_name], actor[last_name]) -> full_name)
                ]
            );
        "#;

        run_smql(tmpl, "sakila").await;

        assert_row_count("actor", "sakila", "actor").await;
        assert_column_exists("actor", "full_name", true).await;

        let query = "SELECT * FROM actor WHERE actor_id = 1";

        let src_first = get_cell_as_string(query, "sakila", DbType::MySql, "first_name").await;
        let src_last = get_cell_as_string(query, "sakila", DbType::MySql, "last_name").await;
        let dst_full = get_cell_as_string(query, "sakila", DbType::Postgres, "full_name").await;

        assert_eq!(dst_full, format!("{}{}", src_first, src_last));
    }

    // Test Settings: CREATE_MISSING_TABLES = TRUE, IGNORE_CONSTRAINTS = TRUE.
    // Scenario:
    // - The target table does not exist in Postgres.
    // - The settings to create missing tables and ignore constraints are specified.
    // - The `users` table is loaded and matched on `user_id`.
    // Expected Outcome:
    // - The target table should be created in Postgres.
    // - The destination table should not include any columns from the loaded table (`users`) since they are not mapped.
    // - Data should be copied, and the row count should match between the source and destination tables.
    #[traced_test]
    #[tokio::test]
    async fn tc12() {
        reset_migration_buffer().expect("reset migration buffer");
        reset_postgres_schema().await;

        let tmpl = r#"
            CONNECTIONS(
                SOURCE(MYSQL,  "{mysq_url}"),
                DESTINATION(POSTGRES, "{pg_url}")
            );
            MIGRATE(
                SOURCE(TABLE, orders) -> DEST(TABLE, orders_flat) [
                    SETTINGS(CREATE_MISSING_TABLES=TRUE,IGNORE_CONSTRAINTS=TRUE),
                    LOAD(TABLES(users),MATCH(ON(users[id] -> orders[user_id])))
                ]
            );
        "#;

        run_smql(tmpl, "orders").await;
        assert_row_count("orders", "orders", "orders_flat").await;

        let src_cols = get_column_names(DbType::MySql, "orders", "users")
            .await
            .unwrap();
        let dst_cols = get_column_names(DbType::Postgres, "orders", "orders_flat")
            .await
            .unwrap();

        for column in src_cols.iter() {
            assert!(
                !dst_cols.contains(&format!("users_{}", column)),
                "Column {} should not exist in destination table",
                column
            );
        }
    }

    // Test Settings: CREATE_MISSING_TABLES = TRUE, IGNORE_CONSTRAINTS = TRUE.
    // Scenario:
    // - The target table does not exist in Postgres.
    // - The settings to create missing tables and ignore constraints are specified.
    // - The `users`, `order_items`, and `products` tables are loaded and matched by their respective IDs.
    // - Mapping includes:
    //   - `users[email] -> user_email`
    //   - `order_items[price] -> order_price`
    //   - `products[name] -> product_name`.
    // Expected Outcome:
    // - The target table should be created in Postgres.
    // - The destination table should include the mapped columns (`user_email`, `order_price`, `product_name`).
    // - Data should be copied, and the row count should match between the source and destination tables.
    // - The new columns should be populated with the corresponding values from the loaded tables.
    #[traced_test]
    #[tokio::test]
    async fn tc13() {
        reset_migration_buffer().expect("reset migration buffer");
        reset_postgres_schema().await;

        let tmpl = r#"
            CONNECTIONS(
                SOURCE(MYSQL,  "{mysq_url}"),
                DESTINATION(POSTGRES, "{pg_url}")
            );
            MIGRATE(
                SOURCE(TABLE, orders) -> DEST(TABLE, orders_flat) [
                    SETTINGS(CREATE_MISSING_TABLES=TRUE,IGNORE_CONSTRAINTS=TRUE),
                    LOAD(TABLES(users,order_items,products),MATCH(
                        ON(users[id] -> orders[user_id]),
                        ON(order_items[order_id] -> orders[id]),
                        ON(products[id] -> order_items[id])
                    )),
                    MAP(users[email] -> user_email, order_items[price] -> order_price, products[name] -> product_name)
                ]
            );
        "#;

        run_smql(tmpl, "orders").await;

        //  Assert that the mapped columns exist in the destination
        for col in &["user_email", "order_price", "product_name"] {
            assert_column_exists("orders_flat", col, true).await;
        }

        // Fetch from source and count in dest
        let src_rows = fetch_rows(ORDERS_FLAT_JOIN_QUERY, "orders", DbType::MySql)
            .await
            .expect("fetch source rows");
        let dst_count = get_row_count("orders_flat", "orders", DbType::Postgres).await;

        assert_eq!(
            src_rows.len(),
            dst_count as usize,
            "expected same number of joined rows"
        );
    }
}
