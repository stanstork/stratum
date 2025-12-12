#[cfg(test)]
mod tests {
    use crate::{
        reset_postgres_schema,
        utils::{
            ACTORS_TABLE_DDL, CUSTOMERS_TABLE_DDL, DbType, ORDERS_FLAT_FILTER_QUERY,
            ORDERS_FLAT_JOIN_QUERY, assert_column_exists, assert_row_count, assert_table_exists,
            execute, fetch_rows, file_row_count, get_cell_as_f64, get_cell_as_string,
            get_cell_as_usize, get_column_names, get_row_count, get_table_names, run_smql,
        },
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
            connection "mysql_source" {
                driver = "mysql"
                url    = "mysql://sakila_user:qwerty123@localhost:3306/sakila"
            }
            connection "pg_destination" {
                driver = "postgres"
                url    = "postgres://user:password@localhost:5432/testdb"
            }
            pipeline "migrate_actor" {
                from {
                    connection = connection.mysql_source
                    table      = "actor"
                }
                to {
                    connection = connection.pg_destination
                    table      = "actor"
                }
            }
        "#;

        run_smql(tmpl).await;
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
        reset_postgres_schema().await;

        let tmpl = r#"
            connection "mysql_source" {
                driver = "mysql"
                url    = "mysql://sakila_user:qwerty123@localhost:3306/sakila"
            }
            connection "pg_destination" {
                driver = "postgres"
                url    = "postgres://user:password@localhost:5432/testdb"
            }
            pipeline "migrate_actor" {
                from {
                    connection = connection.mysql_source
                    table      = "actor"
                }
                to {
                    connection = connection.pg_destination
                    table      = "actor"
                }
                settings {
                    create_missing_tables = true
                }
            }
        "#;

        run_smql(tmpl).await;

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
        reset_postgres_schema().await;

        // Create the actor table in Postgres without the full_name column
        execute(ACTORS_TABLE_DDL).await;

        let tmpl = r#"
            connection "mysql_source" {
                driver = "mysql"
                url    = "mysql://sakila_user:qwerty123@localhost:3306/sakila"
            }
            connection "pg_destination" {
                driver = "postgres"
                url    = "postgres://user:password@localhost:5432/testdb"
            }
            pipeline "migrate_actor" {
                from {
                    connection = connection.mysql_source
                    table      = "actor"
                }
                to {
                    connection = connection.pg_destination
                    table      = "actor"
                }
                select {
                    full_name = concat(actor.first_name, actor.last_name)
                }
                settings {
                    create_missing_columns = true
                }
            }
        "#;

        run_smql(tmpl).await;

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
        reset_postgres_schema().await;

        let tmpl = r#"
            connection "mysql_source" {
                driver = "mysql"
                url    = "mysql://sakila_user:qwerty123@localhost:3306/sakila"
            }
            connection "pg_destination" {
                driver = "postgres"
                url    = "postgres://user:password@localhost:5432/testdb"
            }
            pipeline "migrate_film" {
                from {
                    connection = connection.mysql_source
                    table      = "film"
                }
                to {
                    connection = connection.pg_destination
                    table      = "film"
                }
                settings {
                    infer_schema = true
                }
            }
        "#;

        run_smql(tmpl).await;

        let source_tables = get_table_names(DbType::MySql, "sakila").await.unwrap();
        let dest_tables = get_table_names(DbType::Postgres, "sakila").await.unwrap();

        for table in source_tables.iter() {
            // Skip the film_text table as it does not have related tables and is not migrated
            if table.eq("film_text") {
                continue;
            }

            assert!(
                dest_tables.contains(table),
                "Table {table} not found in destination"
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
        reset_postgres_schema().await;

        let tmpl = r#"
            CONNECTIONS(
                SOURCE(MYSQL,  "{mysql_url}"),
                DESTINATION(POSTGRES, "{pg_url}")
            );
            MIGRATE(
                SOURCE(TABLE, film) -> DEST(TABLE, film) [
                    SETTINGS(INFER_SCHEMA=TRUE,CASCADE_SCHEMA=TRUE)
                ]
            );
        "#;

        run_smql(tmpl).await;

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
    #[ignore = "CASCADE_SCHEMA will be reworked, so this test is ignored for now."]
    async fn tc06() {
        reset_postgres_schema().await;

        let tmpl = r#"
            CONNECTIONS(
                SOURCE(MYSQL,  "{mysql_url}"),
                DESTINATION(POSTGRES, "{pg_url}")
            );
            MIGRATE(
                SOURCE(TABLE, orders) -> DEST(TABLE, orders) [
                    SETTINGS(INFER_SCHEMA=TRUE,CASCADE_SCHEMA=TRUE,IGNORE_CONSTRAINTS=TRUE)
                ]
            );
        "#;

        run_smql(tmpl).await;
        assert_row_count("orders", "orders", "orders").await;

        let dependent_tables = ["order_items", "products", "users"];
        for table in dependent_tables.iter() {
            let dest_count = get_row_count(table, "orders", DbType::Postgres).await;

            assert_eq!(
                0, dest_count,
                "expected no rows in destination table {table}"
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
        reset_postgres_schema().await;

        let tmpl = r#"
            connection "mysql_source" {
                driver = "mysql"
                url    = "mysql://user:password@localhost:3306/testdb"
            }
            connection "pg_destination" {
                driver = "postgres"
                url    = "postgres://user:password@localhost:5432/testdb"
            }
            pipeline "migrate_orders" {
                from {
                    connection = connection.mysql_source
                    table      = "orders"
                }
                to {
                    connection = connection.pg_destination
                    table      = "orders_flat"
                }
                select {
                    id = orders.id
                }
                settings {
                    create_missing_tables = true
                    ignore_constraints    = true
                    copy_columns          = "MAP_ONLY"
                }
            }
        "#;

        run_smql(tmpl).await;

        let dest_columns = get_column_names(DbType::Postgres, "orders", "orders_flat")
            .await
            .unwrap();

        assert_row_count("orders", "orders", "orders_flat").await;
        assert_column_exists("orders_flat", "id", true).await;
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
    #[ignore = "Constaints handling will be reworked, so this test is ignored for now."]
    async fn tc08() {
        reset_postgres_schema().await;

        let tmpl = r#"
            CONNECTIONS(
                SOURCE(MYSQL,  "{mysql_url}"),
                DESTINATION(POSTGRES, "{pg_url}")
            );
            MIGRATE(
                SOURCE(TABLE, orders) -> DEST(TABLE, orders_flat) [
                    SETTINGS(INFER_SCHEMA=TRUE,CREATE_MISSING_COLUMNS=TRUE),
                    MAP(total * 1.4 -> order_price_with_tax)
                ]
            );
        "#;

        run_smql(tmpl).await;

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
        reset_postgres_schema().await;

        // Create the actor table in Postgres
        execute(ACTORS_TABLE_DDL).await;

        let tmpl = r#"
            connection "mysql_source" {
                driver = "mysql"
                url    = "mysql://sakila_user:qwerty123@localhost:3306/sakila"
            }
            connection "pg_destination" {
                driver = "postgres"
                url    = "postgres://user:password@localhost:5432/testdb"
            }
            pipeline "migrate_actor" {
                from {
                    connection = connection.mysql_source
                    table      = "actor"
                }
                to {
                    connection = connection.pg_destination
                    table      = "actor"
                }
            }
        "#;

        run_smql(tmpl).await;
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
    #[ignore = "Constaints handling will be reworked, so this test is ignored for now."]
    async fn tc10() {
        reset_postgres_schema().await;

        let tmpl = r#"
            CONNECTIONS(
                SOURCE(MYSQL,  "{mysql_url}"),
                DESTINATION(POSTGRES, "{pg_url}")
            );
            MIGRATE(
                SOURCE(TABLE, orders) -> DEST(TABLE, orders) [
                    SETTINGS(INFER_SCHEMA=TRUE,CREATE_MISSING_COLUMNS=TRUE),
                    MAP(total * 1.4 -> order_price_with_tax)
                ]
            );
        "#;

        run_smql(tmpl).await;

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
        reset_postgres_schema().await;

        let tmpl = r#"
            connection "mysql_source" {
                driver = "mysql"
                url    = "mysql://sakila_user:qwerty123@localhost:3306/sakila"
            }
            connection "pg_destination" {
                driver = "postgres"
                url    = "postgres://user:password@localhost:5432/testdb"
            }
            pipeline "migrate_actor" {
                from {
                    connection = connection.mysql_source
                    table      = "actor"
                }
                to {
                    connection = connection.pg_destination
                    table      = "actor"
                }
                select {
                    full_name = concat(actor.first_name, actor.last_name)
                }
                settings {
                    create_missing_tables = true
                    ignore_constraints    = true
                }
            }
        "#;

        run_smql(tmpl).await;

        assert_row_count("actor", "sakila", "actor").await;
        assert_column_exists("actor", "full_name", true).await;

        let query = "SELECT * FROM actor WHERE actor_id = 1";

        let src_first = get_cell_as_string(query, "sakila", DbType::MySql, "first_name").await;
        let src_last = get_cell_as_string(query, "sakila", DbType::MySql, "last_name").await;
        let dst_full = get_cell_as_string(query, "sakila", DbType::Postgres, "full_name").await;

        assert_eq!(dst_full, format!("{src_first}{src_last}"));
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
        reset_postgres_schema().await;

        let tmpl = r#"
            connection "mysql_source" {
                driver = "mysql"
                url    = "mysql://user:password@localhost:3306/testdb"
            }
            connection "pg_destination" {
                driver = "postgres"
                url    = "postgres://user:password@localhost:5432/testdb"
            }
            pipeline "migrate_orders" {
                from {
                    connection = connection.mysql_source
                    table      = "orders"
                }
                to {
                    connection = connection.pg_destination
                    table      = "orders_flat"
                }
                with {
                    users    from users    where users.id == orders.user_id
                }
                settings {
                    create_missing_tables = true
                    ignore_constraints    = true
                }
            }
        "#;

        run_smql(tmpl).await;
        assert_row_count("orders", "orders", "orders_flat").await;

        let src_cols = get_column_names(DbType::MySql, "orders", "users")
            .await
            .unwrap();
        let dst_cols = get_column_names(DbType::Postgres, "orders", "orders_flat")
            .await
            .unwrap();

        for column in src_cols.iter() {
            assert!(
                !dst_cols.contains(&format!("users_{column}")),
                "Column {column} should not exist in destination table"
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
        reset_postgres_schema().await;

        let tmpl = r#"
            connection "mysql_source" {
                driver = "mysql"
                url    = "mysql://user:password@localhost:3306/testdb"
            }
            connection "pg_destination" {
                driver = "postgres"
                url    = "postgres://user:password@localhost:5432/testdb"
            }
            pipeline "migrate_orders" {
                from {
                    connection = connection.mysql_source
                    table      = "orders"
                }
                to {
                    connection = connection.pg_destination
                    table      = "orders_flat"
                }
                with {
                    users       from users    where users.id == orders.user_id
                    order_items from order_items where order_items.order_id == orders.id
                    products    from products where products.id == order_items.id
                }
                select {
                    user_email    = users.email
                    order_price   = order_items.price
                    product_name  = products.name
                }
                settings {
                    create_missing_tables = true
                    ignore_constraints    = true
                }
            }
        "#;

        run_smql(tmpl).await;

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

    // Test Settings: CREATE_MISSING_TABLES = TRUE, IGNORE_CONSTRAINTS = TRUE.
    // Scenario:
    // - The target table does not exist in Postgres.
    // - The settings to create missing tables and ignore constraints are specified.
    // - A filter is applied to copy only rows where `total > 400`.
    // Expected Outcome:
    // - The target table should be created in Postgres.
    // - The destination table should contain only the rows that satisfy the filter condition.
    #[traced_test]
    #[tokio::test]
    async fn tc14() {
        reset_postgres_schema().await;

        let tmpl = r#"
            connection "mysql_source" {
                driver = "mysql"
                url    = "mysql://user:password@localhost:3306/testdb"
            }
            connection "pg_destination" {
                driver = "postgres"
                url    = "postgres://user:password@localhost:5432/testdb"
            }
            pipeline "migrate_orders" {
                from {
                    connection = connection.mysql_source
                    table      = "orders"
                }
                to {
                    connection = connection.pg_destination
                    table      = "orders"
                }
                where "valid_orders" {
                    orders.total > 400
                }
                settings {
                    create_missing_tables = true
                    ignore_constraints    = true
                }
            }
        "#;

        run_smql(tmpl).await;

        let query = "SELECT COUNT(*) cnt FROM orders WHERE total > 400";
        let src_cnt = get_cell_as_usize(query, "orders", DbType::MySql, "cnt").await;
        let dst_cnt = get_cell_as_usize(query, "orders", DbType::Postgres, "cnt").await;

        assert_eq!(
            src_cnt, dst_cnt,
            "expected same number of rows in destination table"
        );
    }

    // Test Settings: CREATE_MISSING_TABLES = TRUE, IGNORE_CONSTRAINTS = TRUE.
    // Scenario:
    // - The target table does not exist in Postgres.
    // - The settings to create missing tables and ignore constraints are specified.
    // - A nested filter is applied by combining multiple conditions based on loaded tables:
    //     - `total > 400`
    //     - `user_id != 1` or `price < 1200`.
    // Expected Outcome:
    // - The target table should be created in Postgres.
    // - The destination table should contain only the rows that satisfy the filter condition.
    #[traced_test]
    #[tokio::test]
    async fn tc15() {
        reset_postgres_schema().await;

        let tmpl = r#"
            connection "mysql_source" {
                driver = "mysql"
                url    = "mysql://user:password@localhost:3306/testdb"
            }
            connection "pg_destination" {
                driver = "postgres"
                url    = "postgres://user:password@localhost:5432/testdb"
            }
            pipeline "migrate_orders" {
                from {
                    connection = connection.mysql_source
                    table      = "orders"
                }
                to {
                    connection = connection.pg_destination
                    table      = "orders"
                }
                with {
                    users       from users    where users.id == orders.user_id
                    order_items from order_items where order_items.order_id == orders.id
                    products    from products where products.id == order_items.id
                }
                where "valid_orders" {
                    orders.total > 400
                    users.id != 1 || order_items.price < 1200
                }
                select {
                    user_email    = users.email
                    order_price   = order_items.price
                    product_name  = products.name
                }
                settings {
                    create_missing_tables = true
                    ignore_constraints    = true
                }
            }
        "#;

        run_smql(tmpl).await;

        // Fetch from source and count in dest
        let src_rows = fetch_rows(ORDERS_FLAT_FILTER_QUERY, "orders", DbType::MySql)
            .await
            .expect("fetch source rows");
        let dst_count = get_row_count("orders", "orders", DbType::Postgres).await;

        assert_eq!(
            src_rows.len(),
            dst_count as usize,
            "expected same number of filtered rows"
        );
    }

    // Test Settings: Default (no special flags).
    // Scenario: The source is a CSV file, and the target table does not exist in Postgres.
    // Expected Outcome: The test should pass without creating the table in Postgres.
    #[traced_test]
    #[tokio::test]
    async fn tc16() {
        reset_postgres_schema().await;

        let csv_path = "src/data/customers.csv";
        let tmpl = r#"
            connection "csv_source" {
                driver = "csv"
                path   = "src/data/customers.csv"
            }
            connection "pg_destination" {
                driver = "postgres"
                url    = "postgres://user:password@localhost:5432/testdb"
            }
            pipeline "migrate_customers" {
                from {
                    connection = connection.csv_source
                    table      = "customers"
                }
                to {
                    connection = connection.pg_destination
                    table      = "customers"
                }
            }
        "#
        .replace("{csv_path}", csv_path)
        .to_string();

        run_smql(&tmpl).await;
        assert_table_exists("customers", false).await;
    }

    // Test Settings: CREATE_MISSING_TABLES = TRUE.
    // Scenario:
    // - The source is a CSV file.
    // - The target table does not exist in Postgres.
    // Expected Outcome:
    // - The table should be created in Postgres.
    // - Data should be copied, and the row count should match the source CSV file.
    // - The new table should have the same schema as the CSV file.
    #[traced_test]
    #[tokio::test]
    async fn tc17() {
        reset_postgres_schema().await;

        let csv_path = "src/data/customers.csv";
        let tmpl = r#"
            connection "csv_source" {
                driver = "csv"
                path   = "src/data/customers.csv"
            }
            connection "pg_destination" {
                driver = "postgres"
                url    = "postgres://user:password@localhost:5432/testdb"
            }
            pipeline "migrate_customers" {
                from {
                    connection = connection.csv_source
                    table      = "customers"
                }
                to {
                    connection = connection.pg_destination
                    table      = "customers"
                }
                settings {
                    create_missing_tables = true
                }
            }
        "#
        .replace("{csv_path}", csv_path)
        .to_string();

        run_smql(&tmpl).await;

        // Verify table exists
        assert_table_exists("customers", true).await;

        // Compare row counts
        let actual = get_row_count("customers", "", DbType::Postgres).await as usize;
        let expected = file_row_count(csv_path, true).unwrap();
        assert_eq!(
            actual, expected,
            "Expected {expected} rows in `customers`, got {actual}"
        );
    }

    // Test Settings: CREATE_MISSING_COLUMNS = TRUE.
    // Scenario:
    // - The source is a CSV file.
    // - The target table exists in Postgres but does not have the `full_name` column.
    // Expected Outcome:
    // - The `full_name` column should be created in Postgres.
    // - Data should be copied, and the row count should match the source CSV file.
    // - The `full_name` column should be populated with the concatenated values of `first_name` and `last_name`.
    #[traced_test]
    #[tokio::test]
    async fn tc18() {
        reset_postgres_schema().await;

        // Create the customers table in Postgres without the full_name column
        execute(CUSTOMERS_TABLE_DDL).await;

        let csv_path = "src/data/customers.csv";
        let tmpl = r#"
            connection "csv_source" {
                driver = "csv"
                path   = "src/data/customers.csv"
            }
            connection "pg_destination" {
                driver = "postgres"
                url    = "postgres://user:password@localhost:5432/testdb"
            }
            pipeline "migrate_customers" {
                from {
                    connection = connection.csv_source
                    table      = "customers"
                }
                to {
                    connection = connection.pg_destination
                    table      = "customers"
                }
                select {
                    full_name = concat(customers.first_name, customers.last_name)
                }
                settings {
                    create_missing_columns = true
                }
            }
        "#
        .replace("{csv_path}", csv_path)
        .to_string();

        run_smql(&tmpl).await;

        // Verify table exists
        assert_table_exists("customers", true).await;

        // Compare row counts
        let actual = get_row_count("customers", "", DbType::Postgres).await as usize;
        let expected = file_row_count(csv_path, true).unwrap();
        assert_eq!(
            actual, expected,
            "Expected {expected} rows in `customers`, got {actual}"
        );

        // Verify the full_name column exists and is populated
        assert_column_exists("customers", "full_name", true).await;
    }

    // Test Settings: CREATE_MISSING_TABLES = TRUE, CREATE_MISSING_COLUMNS = TRUE, MAP.
    // Scenario:
    // - The source is a CSV file.
    // - The target table does not exist in Postgres.
    // - The settings to create missing tables and columns are specified.
    // - Mapping includes:
    //   - `index -> id`.
    //   - `CONCAT(customers[first_name], customers[last_name]) -> full_name`.
    // Expected Outcome:
    // - The table should be created in Postgres.
    // - The destination table should include the new column `full_name`.
    // - Data should be copied, and the row count should match the source CSV file.
    // - The `full_name` column should be populated with the concatenated values of `first_name` and `last_name`.
    #[traced_test]
    #[tokio::test]
    async fn tc19() {
        reset_postgres_schema().await;

        let csv_path = "src/data/customers.csv";
        let tmpl = r#"
            connection "csv_source" {
                driver = "csv"
                path   = "src/data/customers.csv"
            }
            connection "pg_destination" {
                driver = "postgres"
                url    = "postgres://user:password@localhost:5432/testdb"
            }
            pipeline "migrate_customers" {
                from {
                    connection = connection.csv_source
                    table      = "customers"
                }
                to {
                    connection = connection.pg_destination
                    table      = "customers"
                }
                select {
                    id = customers.index
                    full_name = concat(customers.first_name, customers.last_name)
                }
                settings {
                    create_missing_tables = true
                }
            }
        "#
        .replace("{csv_path}", csv_path)
        .to_string();

        run_smql(&tmpl).await;

        // Verify table exists
        assert_table_exists("customers", true).await;

        let query = "SELECT * FROM customers WHERE id = 1";

        let first = get_cell_as_string(query, "customers", DbType::Postgres, "first_name").await;
        let last = get_cell_as_string(query, "customers", DbType::Postgres, "last_name").await;
        let full = get_cell_as_string(query, "customers", DbType::Postgres, "full_name").await;

        assert_eq!(full, format!("{first}{last}"));
    }

    // Test Settings: CREATE_MISSING_TABLES = TRUE, CREATE_MISSING_COLUMNS = TRUE, FILTER.
    // Scenario:
    // - The source is a CSV file.
    // - The target table does not exist in Postgres.
    // - The settings to create missing tables and columns are specified.
    // - A filter is applied to copy only rows where `country = 'Poland'`.
    // Expected Outcome:
    // - The table should be created in Postgres.
    // - The destination table should contain only the rows that satisfy the filter condition.
    // - The row count in the destination table should match the number of rows in the source CSV file that satisfy the filter condition.
    #[traced_test]
    #[tokio::test]
    async fn tc20() {
        reset_postgres_schema().await;

        let csv_path = "src/data/customers.csv";
        let tmpl = r#"
            connection "csv_source" {
                driver = "csv"
                path   = "src/data/customers.csv"
            }
            connection "pg_destination" {
                driver = "postgres"
                url    = "postgres://user:password@localhost:5432/testdb"
            }
            pipeline "migrate_customers" {
                from {
                    connection = connection.csv_source
                    table      = "customers"
                }
                to {
                    connection = connection.pg_destination
                    table      = "customers"
                }
                where "valid_country" {
                    customers.country == Poland
                }
                settings {
                    create_missing_tables = true
                }
            }
        "#
        .replace("{csv_path}", csv_path)
        .to_string();

        run_smql(&tmpl).await;

        let query = "SELECT COUNT(*) cnt FROM customers WHERE country = 'Poland'";
        let cnt = get_cell_as_usize(query, "orders", DbType::Postgres, "cnt").await;

        assert_eq!(1, cnt, "expected same number of rows in destination table");
    }

    // Test Validation: SKIP, WARN, and mixed validation actions.
    // Scenario:
    // - Multiple pipelines test different validation actions (skip, warn) with realistic data.
    // - Pipeline 1: Uses SKIP action to filter out invalid payment records.
    // - Pipeline 2: Uses WARN block syntax to log warnings for film data quality issues.
    // - Pipeline 3: Uses WARN block syntax to validate customer data integrity.
    // - Pipeline 4: Combines SKIP and WARN actions to filter and warn about address data.
    // Expected Outcome:
    // - Pipelines with SKIP action should migrate fewer rows than the source (invalid rows excluded).
    // - Pipelines with WARN action should migrate all rows while logging warnings.
    // - All destination tables should be created and populated according to their validation rules.
    #[traced_test]
    #[tokio::test]
    async fn tc21() {
        reset_postgres_schema().await;

        let tmpl = r#"
            connection "mysql_source" {
                driver = "mysql"
                url    = "mysql://sakila_user:qwerty123@localhost:3306/sakila"
            }
            connection "pg_destination" {
                driver = "postgres"
                url    = "postgres://user:password@localhost:5432/testdb"
            }

            // Pipeline 1: Test SKIP action - skip rows with invalid payments
            pipeline "migrate_payments_skip_invalid" {
                from {
                    connection = connection.mysql_source
                    table = "payment"
                }

                to {
                    connection = connection.pg_destination
                    table = "payments_valid"
                }

                settings {
                    create_missing_tables = true
                    batch_size = env("BATCH_SIZE", 1000)
                    copy_columns = "MAP_ONLY"
                }

                validate {
                    // Skip rows with zero or negative amounts
                    assert "positive_amount" {
                        check   = payment.amount > 0
                        message = "Payment amount must be positive"
                        action  = skip
                    }
                    // Skip rows with amount over 5.00 (only accept low-value payments)
                    assert "reasonable_amount" {
                        check   = payment.amount <= 5.00
                        message = "Payment amount exceeds maximum allowed (5.00)"
                        action  = skip
                    }
                }

                select {
                    payment_id = payment.payment_id
                    customer_id = payment.customer_id
                    staff_id = payment.staff_id
                    rental_id = payment.rental_id
                    amount = payment.amount
                    payment_date = payment.payment_date
                }
            }

            // Pipeline 2: Test WARN action - warn about short film titles
            pipeline "migrate_films_with_warnings" {
                from {
                    connection = connection.mysql_source
                    table = "film"
                }

                to {
                    connection = connection.pg_destination
                    table = "films"
                }

                settings {
                    create_missing_tables = true
                    batch_size = 500
                    copy_columns = "MAP_ONLY"
                }

                validate {
                    // Warn about very long rental durations
                    warn "reasonable_rental_duration" {
                        check   = film.rental_duration <= 7
                        message = "Film rental duration exceeds recommended 7 days"
                    }
                    // Warn about low rental rates
                    warn "minimum_rental_rate" {
                        check   = film.rental_rate >= 0.99
                        message = "Film rental rate is below minimum recommended price"
                    }
                    // Warn about very high replacement costs
                    warn "reasonable_replacement_cost" {
                        check   = film.replacement_cost <= 25.00
                        message = "Film replacement cost is unusually high"
                    }
                }

                select {
                    film_id = film.film_id
                    title = film.title
                    description = film.description
                    release_year = film.release_year
                    rental_duration = film.rental_duration
                    rental_rate = film.rental_rate
                    replacement_cost = film.replacement_cost
                }
            }

            // Pipeline 3: Test WARN action on customer data with some fail checks
            pipeline "migrate_customers_strict" {
                from {
                    connection = connection.mysql_source
                    table = "customer"
                }

                to {
                    connection = connection.pg_destination
                    table = "customers"
                }

                settings {
                    create_missing_tables = true
                    batch_size = 1000
                    copy_columns = "MAP_ONLY"
                }

                validate {
                    // This should pass - all customers should have valid IDs
                    warn "valid_customer_id" {
                        check   = customer.customer_id > 0
                        message = "Customer ID must be positive - data integrity violation"
                    }
                    // This should pass - all customers should be linked to a store
                    warn "valid_store_id" {
                        check   = customer.store_id > 0
                        message = "Customer must be assigned to a valid store"
                    }
                }

                select {
                    customer_id = customer.customer_id
                    store_id = customer.store_id
                    first_name = customer.first_name
                    last_name = customer.last_name
                    email = customer.email
                    active = customer.active
                    create_date = customer.create_date
                }
            }

            // Pipeline 4: Test address table with mixed skip/warn validations
            pipeline "migrate_addresses_filtered" {
                from {
                    connection = connection.mysql_source
                    table = "address"
                }

                to {
                    connection = connection.pg_destination
                    table = "addresses"
                }

                settings {
                    create_missing_tables = true
                    batch_size = 500
                    copy_columns = "MAP_ONLY"
                }

                validate {
                    // SKIP: Skip addresses without postal code
                    assert "has_postal_code" {
                        check   = address.postal_code != ""
                        message = "Address must have a postal code"
                        action  = skip
                    }
                    // SKIP: Skip addresses without district
                    assert "has_district" {
                        check   = address.district != ""
                        message = "Address must have a district"
                        action  = skip
                    }
                    // WARN: Warn about addresses with empty phone numbers
                    warn "valid_phone" {
                        check   = address.phone != ""
                        message = "Address has empty phone number"
                    }
                }

                select {
                    address_id = address.address_id
                    address = address.address
                    district = address.district
                    city_id = address.city_id
                    postal_code = address.postal_code
                    phone = address.phone
                }
            }
        "#;

        run_smql(tmpl).await;

        // Pipeline 1: Verify payments with SKIP validation
        // Validation: amount > 0 AND amount <= 5.00
        assert_table_exists("payments_valid", true).await;
        let payment_count = get_row_count("payments_valid", "sakila", DbType::Postgres).await;

        // Query source with same conditions as validation
        let expected_payment_query =
            "SELECT COUNT(*) as cnt FROM payment WHERE amount > 0 AND amount <= 5.00";
        let expected_payment_count =
            get_cell_as_usize(expected_payment_query, "sakila", DbType::MySql, "cnt").await;

        assert_eq!(
            payment_count, expected_payment_count as i64,
            "Destination payment count should match source count with validation filters"
        );

        // Verify some payments were filtered out
        let total_source_payments = get_row_count("payment", "sakila", DbType::MySql).await;
        assert!(
            payment_count < total_source_payments,
            "Expected some payments to be skipped (got {} out of {})",
            payment_count,
            total_source_payments
        );

        // Pipeline 2: Verify films with WARN validation (all rows should be migrated)
        assert_table_exists("films", true).await;
        assert_row_count("film", "sakila", "films").await;

        // Pipeline 3: Verify customers with WARN validation (all rows should be migrated)
        assert_table_exists("customers", true).await;
        assert_row_count("customer", "sakila", "customers").await;

        // Pipeline 4: Verify addresses with SKIP validation
        // Validation: postal_code != "" AND district != ""
        assert_table_exists("addresses", true).await;
        let address_count = get_row_count("addresses", "sakila", DbType::Postgres).await;

        // Query source with same conditions as validation
        let expected_address_query =
            "SELECT COUNT(*) as cnt FROM address WHERE postal_code != '' AND district != ''";
        let expected_address_count =
            get_cell_as_usize(expected_address_query, "sakila", DbType::MySql, "cnt").await;

        assert_eq!(
            address_count, expected_address_count as i64,
            "Destination address count should match source count with validation filters"
        );

        // Verify some addresses were filtered out (if any have empty postal_code or district)
        let total_source_addresses = get_row_count("address", "sakila", DbType::MySql).await;
        if address_count < total_source_addresses {
            println!(
                "Filtered {} addresses (from {} to {})",
                total_source_addresses - address_count,
                total_source_addresses,
                address_count
            );
        }
    }

    // Test Validation: Complex validations with joined tables and transformed fields.
    // Scenario:
    // - Pipeline 1: Joins film_actor, actor, and film tables.
    //   - Validates fields from joined tables (film.rental_rate, film.replacement_cost).
    //   - Validates transformed fields DIRECTLY (actor_full_name, estimated_weekly_cost).
    //   - Demonstrates that transformed fields can be referenced in validation blocks.
    // - Pipeline 2: Joins customer, address, and city tables.
    //   - Validates transformed concatenated fields (full_name, full_address).
    //   - Shows validation blocks can be placed AFTER select blocks.
    // - Uses SKIP action to filter based on joined and transformed field values.
    // - Uses WARN action to flag data quality issues.
    // Expected Outcome:
    // - Transformed fields (concat, calculations) can be validated directly in validate blocks.
    // - Validations work on both source fields and computed/transformed fields.
    // - All destination tables created with proper transformed columns.
    // - Validation filtering applied based on transformed field values.
    #[traced_test]
    #[tokio::test]
    async fn tc22() {
        reset_postgres_schema().await;

        let tmpl = r#"
            connection "mysql_source" {
                driver = "mysql"
                url    = "mysql://sakila_user:qwerty123@localhost:3306/sakila"
            }
            connection "pg_destination" {
                driver = "postgres"
                url    = "postgres://user:password@localhost:5432/testdb"
            }

            // Pipeline: Test validation with joined tables and transformed fields
            pipeline "migrate_film_actors" {
                from {
                    connection = connection.mysql_source
                    table = "film_actor"
                }

                to {
                    connection = connection.pg_destination
                    table = "film_actor_details"
                }

                with {
                    actor from actor where actor.actor_id == film_actor.actor_id
                    film  from film  where film.film_id == film_actor.film_id
                }

                settings {
                    create_missing_tables = true
                    ignore_constraints = true
                    batch_size = 500
                    copy_columns = "MAP_ONLY"
                }

                select {
                    actor_id = film_actor.actor_id
                    film_id = film_actor.film_id
                    actor_full_name = concat(actor.first_name, " ", actor.last_name)
                    film_title = film.title
                    film_rental_rate = film.rental_rate
                    film_rental_duration = film.rental_duration
                    film_replacement_cost = film.replacement_cost
                    estimated_weekly_cost = film.rental_rate * 7
                }

                validate {
                    // SKIP: Validate joined table field - skip films with high rental rates (> 2.99)
                    assert "reasonable_rental_rate" {
                        check   = film.rental_rate <= 2.99
                        message = "Film rental rate is too high"
                        action  = skip
                    }

                    // SKIP: Validate joined table field - skip expensive replacements (> 20.00)
                    assert "reasonable_replacement_cost" {
                        check   = film.replacement_cost <= 20.00
                        message = "Film has high replacement cost"
                        action  = skip
                    }

                    // WARN: Validate transformed field (source fields) - warn about actor name completeness
                    warn "complete_actor_name" {
                        check   = actor.first_name != "" && actor.last_name != ""
                        message = "Actor should have complete name"
                    }

                    // WARN: Validate transformed field directly - warn about actor full name
                    warn "actor_full_name_valid" {
                        check   = actor_full_name != " "
                        message = "Actor full name should not be just a space"
                    }
                }
            }

            // Pipeline 2: Test validation with transformed fields and expressions
            pipeline "migrate_customer_summary" {
                from {
                    connection = connection.mysql_source
                    table = "customer"
                }

                to {
                    connection = connection.pg_destination
                    table = "customer_summary"
                }

                with {
                    address from address where address.address_id == customer.address_id
                    city    from city    where city.city_id == address.city_id
                }

                settings {
                    create_missing_tables = true
                    ignore_constraints = true
                    batch_size = 500
                    copy_columns = "MAP_ONLY"
                }

                select {
                    customer_id = customer.customer_id
                    full_name = concat(customer.first_name, " ", customer.last_name)
                    email = customer.email
                    full_address = concat(address.address, ", ", city.city)
                    store_id = customer.store_id
                    is_active = customer.active
                }

                validate {
                    // WARN: Validate transformed field - warn if customer identifier is too short
                    warn "valid_customer_identifier" {
                        check   = customer.first_name != "" && customer.email != ""
                        message = "Customer should have both name and email for identification"
                    }

                    // WARN: Check store assignment from joined context
                    warn "valid_store" {
                        check   = customer.store_id > 0
                        message = "Customer store assignment should be verified"
                    }

                    // WARN: Validate joined field - warn about address completeness
                    warn "check_address_complete" {
                        check   = address.address != "" && city.city != ""
                        message = "Address or city information may be incomplete"
                    }

                    // SKIP: Validate the transformed full_name field directly
                    assert "full_name_length" {
                        check   = full_name != " "
                        message = "Full name must not be just a space"
                        action  = skip
                    }

                    // WARN: Validate the transformed full_address field directly
                    warn "full_address_reasonable" {
                        check   = full_address != ", "
                        message = "Full address appears to be incomplete"
                    }
                }
            }
        "#;

        run_smql(tmpl).await;

        // Pipeline 1: Verify film_actor_details with joined validations
        // Validation: film.rental_rate <= 2.99 AND film.replacement_cost <= 20.00
        assert_table_exists("film_actor_details", true).await;
        let film_actor_details_count =
            get_row_count("film_actor_details", "sakila", DbType::Postgres).await;

        // Query source with same conditions as validation (using joins)
        let expected_film_actor_query = r#"
            SELECT COUNT(*) as cnt
            FROM film_actor fa
            JOIN film f ON f.film_id = fa.film_id
            WHERE f.rental_rate <= 2.99
              AND f.replacement_cost <= 20.00
        "#;
        let expected_film_actor_count =
            get_cell_as_usize(expected_film_actor_query, "sakila", DbType::MySql, "cnt").await;

        assert_eq!(
            film_actor_details_count, expected_film_actor_count as i64,
            "Destination film_actor count should match source with validation filters"
        );

        // Verify some rows were filtered out
        let total_film_actor = get_row_count("film_actor", "sakila", DbType::MySql).await;
        assert!(
            film_actor_details_count < total_film_actor,
            "Expected some film actors to be filtered (got {} out of {})",
            film_actor_details_count,
            total_film_actor
        );

        // Pipeline 2: Verify customer_summary with transformed field validation
        // Validation: full_name != " " (SKIP action)
        assert_table_exists("customer_summary", true).await;
        let customer_summary_count =
            get_row_count("customer_summary", "sakila", DbType::Postgres).await;

        // Query source with same conditions - customers with both first and last names
        // (full_name != " " means both first_name and last_name must exist)
        let expected_customer_query = r#"
            SELECT COUNT(*) as cnt
            FROM customer c
            JOIN address a ON a.address_id = c.address_id
            JOIN city ci ON ci.city_id = a.city_id
            WHERE CONCAT(c.first_name, ' ', c.last_name) != ' '
        "#;
        let expected_customer_count =
            get_cell_as_usize(expected_customer_query, "sakila", DbType::MySql, "cnt").await;

        assert_eq!(
            customer_summary_count, expected_customer_count as i64,
            "Destination customer count should match source with validation filters"
        );

        // Verify transformed fields exist and contain data
        assert_column_exists("film_actor_details", "actor_full_name", true).await;
        assert_column_exists("film_actor_details", "estimated_weekly_cost", true).await;
        assert_column_exists("customer_summary", "full_name", true).await;
        assert_column_exists("customer_summary", "full_address", true).await;

        // Verify transformed fields have actual data
        let query = "SELECT actor_full_name, estimated_weekly_cost FROM film_actor_details LIMIT 1";
        let rows = fetch_rows(query, "sakila", DbType::Postgres)
            .await
            .expect("fetch rows");
        assert!(
            !rows.is_empty(),
            "Expected at least one film actor detail row"
        );

        let query2 = "SELECT full_name, full_address FROM customer_summary LIMIT 1";
        let rows2 = fetch_rows(query2, "sakila", DbType::Postgres)
            .await
            .expect("fetch rows");
        assert!(!rows2.is_empty(), "Expected at least one customer summary row");
    }

    // Test Validation: FAIL action stops pipeline execution.
    // Scenario:
    // - Pipeline attempts to migrate actor data with a strict validation.
    // - Uses FAIL action requiring actor_id == 1 (only one actor matches).
    // - When a row with actor_id != 1 is encountered, validation fails.
    // Expected Outcome:
    // - The FAIL action stops the pipeline immediately upon validation failure.
    // - Zero rows are migrated to the destination (pipeline stops before any inserts).
    // - The table is created but remains empty.
    // - This demonstrates that FAIL action prevents data migration when validation fails.
    #[traced_test]
    #[tokio::test]
    async fn tc23() {
        reset_postgres_schema().await;

        let tmpl = r#"
            connection "mysql_source" {
                driver = "mysql"
                url    = "mysql://sakila_user:qwerty123@localhost:3306/sakila"
            }
            connection "pg_destination" {
                driver = "postgres"
                url    = "postgres://user:password@localhost:5432/testdb"
            }

            pipeline "migrate_actors_with_fail" {
                from {
                    connection = connection.mysql_source
                    table = "actor"
                }

                to {
                    connection = connection.pg_destination
                    table = "actors_validated"
                }

                settings {
                    create_missing_tables = true
                    batch_size = 10
                    copy_columns = "MAP_ONLY"
                }

                validate {
                    // FAIL: This validation will fail on the second row
                    // The Sakila database has 200 actors with various IDs
                    // Actor ID 1 exists, but actor ID 2 will fail this validation
                    assert "impossible_condition" {
                        check   = actor.actor_id == 1
                        message = "Only actor with ID 1 is allowed - this will fail on actor_id=2"
                        action  = fail
                    }
                }

                select {
                    actor_id = actor.actor_id
                    first_name = actor.first_name
                    last_name = actor.last_name
                }
            }
        "#;

        // Run the migration with fail validation
        run_smql(tmpl).await;

        // Verify that the table was created (pipeline starts execution)
        assert_table_exists("actors_validated", true).await;

        // Verify that NO rows were migrated (fail action stopped the pipeline)
        let count = get_row_count("actors_validated", "sakila", DbType::Postgres).await;

        // Query source to see how many rows WOULD have been migrated with validation
        let expected_query = "SELECT COUNT(*) as cnt FROM actor WHERE actor_id = 1";
        let expected_count = get_cell_as_usize(expected_query, "sakila", DbType::MySql, "cnt").await;

        // With FAIL action, the pipeline stops on the first validation failure
        // Since we have actors with actor_id != 1, the pipeline should fail and migrate 0 rows
        assert_eq!(
            count, 0,
            "Expected 0 rows migrated with fail action (pipeline stopped on first failure), but got {}",
            count
        );

        // Verify the source has rows that don't match the validation (actor_id != 1)
        let total_source_count = get_row_count("actor", "sakila", DbType::MySql).await;
        let failing_rows = total_source_count - (expected_count as i64);
        assert!(
            failing_rows > 0,
            "Expected some rows to fail validation (actors with id != 1), found {} out of {} total",
            failing_rows,
            total_source_count
        );
    }
}
