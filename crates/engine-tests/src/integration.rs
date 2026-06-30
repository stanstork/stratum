#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use crate::{
        reset_postgres_schema,
        utils::{
            ACTORS_TABLE_DDL, DbType, ORDERS_FLAT_FILTER_QUERY, ORDERS_FLAT_JOIN_QUERY,
            PIPELINE_FAILURES_TABLE_DDL, assert_column_exists, assert_row_count,
            assert_table_exists, execute, fetch_rows, get_cell_as_string, get_cell_as_usize,
            get_column_names, get_row_count, run_smql,
        },
    };
    use engine_core::plan::execution::ExecutionPlan as CoreExecutionPlan;
    use engine_planner::{
        builder::{ReportBuilder, ReportBuilderConfig},
        plan::{diagnostics::level::DiagnosticLevel, validation::types::ValidationAction},
    };
    use engine_processing::EnvContext;
    use engine_runtime::dag::builder::DagBuilder;
    use smql_syntax::builder::parse;
    use tracing_test::traced_test;

    // Test Settings: Default (no special flags).
    // Scenario: The target table does not exist in Postgres, and no setting to create it is specified.
    // Expected Outcome: The test should pass without creating the table in Postgres.
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn missing_table_not_created_without_setting() {
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

        let _ = run_smql(tmpl, false).await;
        assert_table_exists("actor", false).await;
    }

    // Test Settings: CREATE_MISSING_TABLES = TRUE.
    // Scenario: The target table does not exist in Postgres, and the setting to create it is specified.
    // Expected Outcome:
    // - The table should be created in Postgres.
    // - Data should be copied, and the row count should match the source table.
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn create_missing_tables_creates_table_and_copies_data() {
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

        let _ = run_smql(tmpl, false).await;

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
    #[tokio::test(flavor = "multi_thread")]
    async fn create_missing_columns_adds_computed_concat_column() {
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

        let _ = run_smql(tmpl, false).await;

        assert_table_exists("actor", true).await;
        assert_row_count("actor", "sakila", "actor").await;
        assert_column_exists("actor", "full_name", true).await;
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
    #[tokio::test(flavor = "multi_thread")]
    async fn map_only_setting_copies_only_mapped_columns() {
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

        let _ = run_smql(tmpl, false).await;

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

    // Test Settings: Default (no special flags).
    // Scenario:
    // - The target table exists in Postgres with the same schema as the source table.
    // - The target table is empty.
    // Expected Outcome:
    // - Data should be copied without any modifications.
    // - The row count should match between the source and destination tables.
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn migrate_into_existing_table_copies_all_rows() {
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

        let _ = run_smql(tmpl, false).await;
        assert_row_count("actor", "sakila", "actor").await;
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
    #[tokio::test(flavor = "multi_thread")]
    async fn create_table_with_computed_column_and_ignore_constraints() {
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

        let _ = run_smql(tmpl, false).await;

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
    #[tokio::test(flavor = "multi_thread")]
    async fn join_without_select_excludes_joined_table_columns() {
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

        let _ = run_smql(tmpl, false).await;
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
    #[tokio::test(flavor = "multi_thread")]
    async fn multi_join_with_column_mappings_copies_joined_data() {
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

        let _ = run_smql(tmpl, false).await;

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
    #[tokio::test(flavor = "multi_thread")]
    async fn where_filter_restricts_migrated_rows() {
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

        let _ = run_smql(tmpl, false).await;

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
    #[tokio::test(flavor = "multi_thread")]
    async fn multi_join_with_nested_where_filter() {
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

        let _ = run_smql(tmpl, false).await;

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
    #[tokio::test(flavor = "multi_thread")]
    async fn validation_skip_and_warn_actions_filter_rows() {
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

        let _ = run_smql(tmpl, false).await;

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
    #[tokio::test(flavor = "multi_thread")]
    async fn validation_on_joined_tables_and_computed_fields() {
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

        let _ = run_smql(tmpl, false).await;

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
        assert!(
            !rows2.is_empty(),
            "Expected at least one customer summary row"
        );
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
    #[tokio::test(flavor = "multi_thread")]
    async fn validation_fail_action_stops_pipeline() {
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
        let _ = run_smql(tmpl, false).await;

        // Verify that the table was created (pipeline starts execution)
        assert_table_exists("actors_validated", true).await;

        // Verify that NO rows were migrated (fail action stopped the pipeline)
        let count = get_row_count("actors_validated", "sakila", DbType::Postgres).await;

        // Query source to see how many rows WOULD have been migrated with validation
        let expected_query = "SELECT COUNT(*) as cnt FROM actor WHERE actor_id = 1";
        let expected_count =
            get_cell_as_usize(expected_query, "sakila", DbType::MySql, "cnt").await;

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

    // Test Validation: Validation failures (FAIL action) go to DLQ and stop migration.
    // Scenario:
    // - Pipeline has strict validation requiring all actors to have first_name = "PENELOPE".
    // - Only some actors match this condition.
    // - When validation fails (first actor that isn't PENELOPE), migration should stop.
    // - Failed validation rows should be written to DLQ.
    // Expected Outcome:
    // - DLQ table created with failed validation rows and metadata.
    // - Migration stops after first batch with validation failure.
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn validation_fail_action_writes_failed_rows_to_dlq_table() {
        reset_postgres_schema().await;

        // Create the DLQ table before running the migration
        execute(PIPELINE_FAILURES_TABLE_DDL).await;

        let tmpl = r#"
            connection "mysql_source" {
                driver = "mysql"
                url    = "mysql://sakila_user:qwerty123@localhost:3306/sakila"
            }
            connection "pg_destination" {
                driver = "postgres"
                url    = "postgres://user:password@localhost:5432/testdb"
            }

            pipeline "migrate_actors_strict_validation" {
                from {
                    connection = connection.mysql_source
                    table = "actor"
                }

                to {
                    connection = connection.pg_destination
                    table = "actors_validated_strict"
                }

                settings {
                    create_missing_tables = true
                    batch_size = 1000
                    copy_columns = "MAP_ONLY"
                }

                validate {
                    // FAIL: Strict validation - only allow specific actor
                    assert "only_penelope" {
                        check   = actor.first_name == "PENELOPE"
                        message = "Only PENELOPE actors allowed - validation will fail on others"
                        action  = fail
                    }
                }

                select {
                    actor_id = actor.actor_id
                    first_name = actor.first_name
                    last_name = actor.last_name
                }

                on_error {
                    failed_rows {
                        table {
                            connection = connection.pg_destination
                            table = "pipeline_failures"
                        }
                    }
                }
            }
        "#;

        let _ = run_smql(tmpl, false).await;

        // Verify destination table was created
        assert_table_exists("actors_validated_strict", true).await;

        // Verify DLQ table was created
        assert_table_exists("pipeline_failures", true).await;

        // Check how many rows were migrated (should be 0 due to FAIL action stopping pipeline)
        let migrated_count =
            get_row_count("actors_validated_strict", "sakila", DbType::Postgres).await;

        // With FAIL action, pipeline should stop on first validation failure
        assert_eq!(
            migrated_count, 0,
            "Expected 0 rows migrated (FAIL action stops pipeline immediately)"
        );

        let total_source_count = get_row_count("actor", "sakila", DbType::MySql).await;
        let expected_query = "SELECT COUNT(*) as cnt FROM actor WHERE first_name = 'PENELOPE'";
        let expected_count =
            get_cell_as_usize(expected_query, "sakila", DbType::MySql, "cnt").await;

        // Verify DLQ has failed validation rows
        let dlq_count = get_row_count("pipeline_failures", "sakila", DbType::Postgres).await;
        assert_eq!(
            dlq_count,
            (total_source_count - expected_count as i64),
            "DLQ should contain all failed validation rows"
        );

        // Verify source has more rows (only some actors are PENELOPE)
        let source_count = get_row_count("actor", "sakila", DbType::MySql).await;
        assert!(
            source_count > dlq_count,
            "Source should have more rows than DLQ (expected {} > {})",
            source_count,
            dlq_count
        );
    }

    // Test Validation: File-based DLQ (JSONL format).
    // Scenario:
    // - Pipeline with transformation that causes validation failures (division by zero).
    // - DLQ configured to write to file instead of database table.
    // - Failed rows should be written in JSONL format (one JSON object per line).
    // Expected Outcome:
    // - DLQ file created with failed rows in JSONL format.
    // - Each line is a valid JSON object with failed row metadata.
    // - Migration continues for valid rows.
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn dlq_file_output_in_jsonl_format() {
        reset_postgres_schema().await;

        let dlq_path = "/tmp/failed_payments.jsonl";

        // Clean up previous test file if exists
        let _ = std::fs::remove_file(dlq_path);

        let tmpl = r#"
            connection "mysql_source" {
                driver = "mysql"
                url    = "mysql://sakila_user:qwerty123@localhost:3306/sakila"
            }
            connection "pg_destination" {
                driver = "postgres"
                url    = "postgres://user:password@localhost:5432/testdb"
            }

            pipeline "migrate_payments_file_dlq" {
                from {
                    connection = connection.mysql_source
                    table = "payment"
                }

                to {
                    connection = connection.pg_destination
                    table = "payments_file_dlq"
                }

                settings {
                    create_missing_tables = true
                    batch_size = 50
                    copy_columns = "MAP_ONLY"
                }

                select {
                    payment_id = payment.payment_id
                    customer_id = payment.customer_id
                    amount = payment.amount / 0 // Intentional error to trigger validation
                    payment_date = payment.payment_date
                }

                on_error {
                    failed_rows {
                        file {
                            path = "/tmp/failed_payments.jsonl"
                            format = "Json"
                        }
                    }
                }
            }
        "#;

        let _ = run_smql(tmpl, false).await;

        // Verify destination table has data
        assert_table_exists("payments_file_dlq", true).await;
        let migrated_count = get_row_count("payments_file_dlq", "sakila", DbType::Postgres).await;

        // Query source to see how many should be filtered
        let filtered_query = "SELECT COUNT(*) as cnt FROM payment";
        let expected = get_cell_as_usize(filtered_query, "sakila", DbType::MySql, "cnt").await;

        if expected > 0 {
            // DLQ file should exist and contain JSONL data
            assert!(
                std::path::Path::new(dlq_path).exists(),
                "Expected DLQ file to be created at {}",
                dlq_path
            );

            // Read and verify JSONL format
            let file_content =
                std::fs::read_to_string(dlq_path).expect("Should be able to read DLQ file");

            let lines: Vec<&str> = file_content.lines().collect();
            assert!(
                !lines.is_empty(),
                "Expected at least one failed row in DLQ file"
            );

            // Verify each line is valid JSON
            for line in lines.iter() {
                let json: serde_json::Value =
                    serde_json::from_str(line).expect("Each line should be valid JSON");

                // Verify required fields exist
                assert!(
                    json.get("id").is_some(),
                    "Failed row should have 'id' field"
                );
                assert!(
                    json.get("pipeline_name").is_some(),
                    "Failed row should have 'pipeline_name' field"
                );
                assert!(
                    json.get("error").is_some(),
                    "Failed row should have 'error_type' field"
                );
                let error = json.get("error").unwrap();
                assert!(
                    error.get("message").is_some(),
                    "Failed row error should have 'message' field"
                );
                assert!(
                    error.get("error_type").is_some(),
                    "Failed row error should have 'type' field"
                );
                assert!(
                    json.get("original_data").is_some(),
                    "Failed row should have 'original_data' field"
                );
            }

            // Verify row counts
            let source_count = get_row_count("payment", "sakila", DbType::MySql).await;
            assert_eq!(
                migrated_count + (lines.len() as i64),
                source_count,
                "Migrated + DLQ should equal source count"
            );
        }

        // Clean up test file
        let _ = std::fs::remove_file(dlq_path);
    }

    // Test Settings: BEFORE HOOKS.
    // Scenario: Before hooks create temp table and index before migration starts.
    // Expected Outcome:
    // - Before hooks execute successfully.
    // - Migration completes successfully.
    // - Index created by before hook exists in the destination table.
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn before_hooks_run_ddl_before_migration() {
        reset_postgres_schema().await;

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
            pipeline "migrate_actor_with_before_hooks" {
                from {
                    connection = connection.mysql_source
                    table      = "actor"
                }
                to {
                    connection = connection.pg_destination
                    table      = "actor"
                }

                before {
                    sql = [
                        "CREATE TEMP TABLE staging_actor (actor_id INT, first_name TEXT, last_name TEXT)",
                        "CREATE INDEX IF NOT EXISTS idx_actor_last_name ON actor(last_name)"
                    ]
                }

                select {
                    actor_id   = actor.actor_id
                    first_name = actor.first_name
                    last_name  = actor.last_name
                }
            }
        "#;

        let _ = run_smql(tmpl, false).await;

        assert_table_exists("actor", true).await;
        assert_row_count("actor", "sakila", "actor").await;

        let index_query = "SELECT indexname FROM pg_indexes WHERE tablename = 'actor' AND indexname = 'idx_actor_last_name'";
        let rows = fetch_rows(index_query, "testdb", DbType::Postgres)
            .await
            .unwrap();
        assert_eq!(
            rows.len(),
            1,
            "Index should have been created by before hook"
        );
    }

    // Test Settings: AFTER HOOKS.
    // Scenario: After hooks create multiple indexes and analyze table after migration.
    // Expected Outcome:
    // - Migration completes successfully.
    // - After hooks execute after migration completes.
    // - All indexes created by after hooks exist.
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn after_hooks_create_indexes_after_migration() {
        reset_postgres_schema().await;

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
            pipeline "migrate_actor_with_after_hooks" {
                from {
                    connection = connection.mysql_source
                    table      = "actor"
                }
                to {
                    connection = connection.pg_destination
                    table      = "actor"
                }

                after {
                    sql = [
                        "CREATE INDEX IF NOT EXISTS idx_actor_first_name ON actor(first_name)",
                        "CREATE INDEX IF NOT EXISTS idx_actor_full_name ON actor(first_name, last_name)",
                        "ANALYZE actor"
                    ]
                }

                select {
                    actor_id   = actor.actor_id
                    first_name = actor.first_name
                    last_name  = actor.last_name
                }
            }
        "#;

        let _ = run_smql(tmpl, false).await;

        assert_table_exists("actor", true).await;
        assert_row_count("actor", "sakila", "actor").await;

        let index_query = "SELECT indexname FROM pg_indexes WHERE tablename = 'actor' AND indexname IN ('idx_actor_first_name', 'idx_actor_full_name') ORDER BY indexname";
        let rows = fetch_rows(index_query, "testdb", DbType::Postgres)
            .await
            .unwrap();

        assert_eq!(
            rows.len(),
            2,
            "Both indexes should have been created by after hooks"
        );
    }

    // Test Settings: BEFORE AND AFTER HOOKS.
    // Scenario:
    // - Before hooks disable triggers and drop indexes.
    // - Migration proceeds.
    // - After hooks re-enable triggers and create indexes.
    // Expected Outcome:
    // - All hooks execute in correct order (before -> migration -> after).
    // - Migration completes successfully.
    // - Index created by after hook exists.
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn before_and_after_hooks_full_lifecycle() {
        reset_postgres_schema().await;

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
            pipeline "migrate_actor_full_lifecycle" {
                from {
                    connection = connection.mysql_source
                    table      = "actor"
                }
                to {
                    connection = connection.pg_destination
                    table      = "actor"
                }

                before {
                    sql = [
                        "DROP INDEX IF EXISTS idx_actor_search",
                        "ALTER TABLE actor DISABLE TRIGGER ALL"
                    ]
                }

                after {
                    sql = [
                        "ALTER TABLE actor ENABLE TRIGGER ALL",
                        "CREATE INDEX IF NOT EXISTS idx_actor_search ON actor(first_name, last_name)",
                        "ANALYZE actor"
                    ]
                }

                select {
                    actor_id   = actor.actor_id
                    first_name = actor.first_name
                    last_name  = actor.last_name
                }
            }
        "#;

        let _ = run_smql(tmpl, false).await;

        assert_table_exists("actor", true).await;
        assert_row_count("actor", "sakila", "actor").await;

        let index_query = "SELECT indexname FROM pg_indexes WHERE tablename = 'actor' AND indexname = 'idx_actor_search'";
        let rows = fetch_rows(index_query, "testdb", DbType::Postgres)
            .await
            .unwrap();
        assert_eq!(rows.len(), 1, "Index should exist after migration");
    }

    // Test Settings: COMPREHENSIVE PLAN GENERATION TEST (DRY RUN).
    // Scenario:
    // - Tests all SMQL v2.1 features using the Sakila database.
    // - Configuration includes 14 pipelines covering:
    //   * Execution settings (parallel, max_concurrency, on_failure)
    //   * Define block with constants and environment variables
    //   * Simple migrations with dependencies
    //   * Computed columns and transformations (concat, arithmetic)
    //   * WHERE filters with comparison operators
    //   * Single and multiple JOINs with lookups
    //   * Validation blocks (SKIP, WARN, FAIL actions)
    //   * Mixed validation scenarios
    //   * Error handling with file-based DLQ (JSONL format)
    //   * Before/after SQL hooks for DDL operations
    //   * Complex multi-table denormalization
    //   * Diamond dependency patterns
    // Expected Outcome:
    // - Plan generation succeeds without errors (no actual migration).
    // - All 14 pipelines are analyzed and planned.
    // - Dependencies are correctly resolved into execution stages.
    // - Plan is marked as executable.
    // - Pipeline metadata includes:
    //   * Source/destination table information
    //   * Computed column mappings
    //   * Filter conditions
    //   * Join specifications
    //   * Validation rules
    //   * Before/after hooks
    // - Row count estimates are calculated from source databases.
    // - No actual data is migrated (dry run only).
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn plan_generation_covers_all_smql_features() {
        // Load comprehensive test configuration
        let path = "../../examples/configs/plan-generation.smql";
        let config_path = PathBuf::from(path);
        let config_content = fs::read_to_string(&config_path).expect("read config file");

        // Parse SMQL configuration
        let doc = parse(&config_content).expect("parse SMQL config");

        // Build core execution plan.
        let mut env = EnvContext::empty();
        env.set(
            "MYSQL_URL".to_string(),
            "mysql://sakila_user:qwerty123@localhost:3306/sakila".to_string(),
        );
        env.set(
            "POSTGRES_URL".to_string(),
            "postgres://user:password@localhost:5432/testdb".to_string(),
        );
        let core_plan = CoreExecutionPlan::build(&doc, std::sync::Arc::new(env))
            .expect("build core execution plan");

        // Build DAG from pipeline dependencies
        let mut dag_builder = DagBuilder::new();
        for pipeline in &core_plan.pipelines {
            dag_builder
                .add_pipeline(pipeline.name.clone(), pipeline.dependencies.clone())
                .expect("add pipeline to DAG");
        }
        let dag = dag_builder.build().expect("build DAG");

        // Create report builder configuration
        let report_config = ReportBuilderConfig::default();
        let report_builder = ReportBuilder::new(report_config);

        // Generate the detailed migration report (this is what `stratum plan` does)
        let report = report_builder
            .build(&core_plan, &dag, config_path.as_ref())
            .await
            .expect("generate migration report");

        // ========================================================================
        // VALIDATE REPORT STRUCTURE
        // ========================================================================

        // Report should be executable
        assert!(
            report.is_executable,
            "Report should be executable (no blocking errors)"
        );
        assert_eq!(
            report.blocking_reason, None,
            "Report should have no blocking reason"
        );

        // Should have 14 pipelines
        assert_eq!(
            report.pipelines.len(),
            14,
            "Expected 14 pipelines in report"
        );

        // Should have execution stages (parallelization)
        assert!(
            !report.execution_order.is_empty(),
            "Expected at least one execution stage"
        );

        // Verify summary statistics
        assert_eq!(
            report.summary.total_pipelines, 14,
            "Summary should show 14 pipelines"
        );
        assert!(
            report.summary.total_connections >= 2,
            "Should have at least 2 connections (source + destination)"
        );

        // ========================================================================
        // VALIDATE INDIVIDUAL PIPELINES
        // ========================================================================

        // Helper to find pipeline by name
        let find_pipeline = |name: &str| {
            report
                .pipelines
                .iter()
                .find(|p| p.name == name)
                .unwrap_or_else(|| panic!("Pipeline '{}' not found", name))
        };

        // PIPELINE 1: migrate_language (simple migration)
        let lang_pipeline = find_pipeline("migrate_language");
        assert_eq!(lang_pipeline.source.table, "language");
        assert_eq!(lang_pipeline.destination.table, "language");
        assert!(
            lang_pipeline.settings.create_missing_tables,
            "Should create missing tables"
        );
        assert_eq!(
            lang_pipeline.mappings.len(),
            3,
            "Should have 3 column mappings"
        );

        // PIPELINE 2: migrate_category (with dependency)
        let cat_pipeline = find_pipeline("migrate_category");
        assert_eq!(
            cat_pipeline.depends_on.len(),
            1,
            "Should depend on migrate_language"
        );
        assert!(
            cat_pipeline
                .depends_on
                .contains(&"migrate_language".to_string())
        );

        // PIPELINE 3: migrate_actors_enriched (computed columns)
        let actors_pipeline = find_pipeline("migrate_actors_enriched");
        assert_eq!(actors_pipeline.source.table, "actor");
        assert_eq!(actors_pipeline.destination.table, "actors_enriched");

        // Verify computed columns in mappings
        let has_full_name = actors_pipeline
            .mappings
            .iter()
            .any(|m| m.target == "full_name");
        let has_search_name = actors_pipeline
            .mappings
            .iter()
            .any(|m| m.target == "search_name");
        assert!(has_full_name, "Should have full_name computed column");
        assert!(has_search_name, "Should have search_name computed column");

        // PIPELINE 4: migrate_films_affordable (WHERE filters)
        let films_pipeline = find_pipeline("migrate_films_affordable");
        assert_eq!(films_pipeline.filters.len(), 1, "Should have 1 filter");
        assert_eq!(films_pipeline.filters[0].name, "affordable_films");
        assert!(
            films_pipeline.filters[0]
                .columns_referenced
                .contains(&"film.rental_rate".to_string())
                || films_pipeline.filters[0]
                    .columns_referenced
                    .contains(&"rental_rate".to_string()),
            "Filter should reference rental_rate"
        );

        // PIPELINE 5: migrate_customers_with_store (single JOIN)
        let customers_pipeline = find_pipeline("migrate_customers_with_store");
        assert_eq!(customers_pipeline.joins.len(), 1, "Should have 1 join");
        assert_eq!(customers_pipeline.joins[0].alias, "store");

        // Verify lookup column
        let has_store_address = customers_pipeline
            .mappings
            .iter()
            .any(|m| m.target == "store_address_id");
        assert!(
            has_store_address,
            "Should have store_address_id lookup column"
        );

        // PIPELINE 6: migrate_film_details (multiple JOINs, arithmetic)
        let film_details = find_pipeline("migrate_film_details");
        assert_eq!(film_details.joins.len(), 1, "Should have 1 join");

        // Verify arithmetic transformations
        let has_weekly_cost = film_details
            .mappings
            .iter()
            .any(|m| m.target == "weekly_rental_cost");
        let has_cost_with_tax = film_details
            .mappings
            .iter()
            .any(|m| m.target == "cost_with_tax");
        assert!(has_weekly_cost, "Should have weekly_rental_cost computed");
        assert!(has_cost_with_tax, "Should have cost_with_tax computed");

        // PIPELINE 7: migrate_payments_validated (SKIP validation)
        let payments_pipeline = find_pipeline("migrate_payments_validated");
        assert!(
            payments_pipeline.validations.len() >= 2,
            "Should have at least 2 validations"
        );

        // Verify validations have correct actions
        let has_skip_validation = payments_pipeline
            .validations
            .iter()
            .any(|v| matches!(v.action, Some(ValidationAction::Skip)));
        assert!(has_skip_validation, "Should have SKIP validation actions");

        // PIPELINE 8: migrate_films_with_warnings (WARN validation)
        let films_warn = find_pipeline("migrate_films_with_warnings");
        assert!(
            films_warn.validations.len() >= 3,
            "Should have at least 3 warn validations"
        );

        // PIPELINE 9: migrate_addresses_filtered (mixed SKIP + WARN)
        let addresses = find_pipeline("migrate_addresses_filtered");
        assert!(
            addresses.validations.len() >= 3,
            "Should have mixed validations"
        );

        // PIPELINE 10: migrate_film_actors_enriched (complex joins + validation)
        let film_actors = find_pipeline("migrate_film_actors_enriched");
        assert_eq!(
            film_actors.joins.len(),
            2,
            "Should have 2 joins (actor + film)"
        );
        assert!(
            film_actors.validations.len() >= 2,
            "Should have validations on joined data"
        );

        // PIPELINE 11: migrate_inventory_with_file_dlq (error handling)
        let inventory = find_pipeline("migrate_inventory_with_file_dlq");
        assert!(
            inventory.error_handling.failed_rows.is_some(),
            "Should have failed_rows DLQ configured"
        );

        // PIPELINE 12: migrate_staff_with_hooks (before/after hooks)
        let staff = find_pipeline("migrate_staff_with_hooks");
        assert!(!staff.hooks.before.is_empty(), "Should have before hooks");
        assert!(!staff.hooks.after.is_empty(), "Should have after hooks");

        // PIPELINE 13: create_customer_360_view (multi-table denormalization)
        let customer_360 = find_pipeline("create_customer_360_view");
        assert_eq!(customer_360.joins.len(), 3, "Should have 3 joins");
        assert_eq!(
            customer_360.filters.len(),
            1,
            "Should have 1 filter (active customers)"
        );

        // Verify denormalized columns
        let has_full_address = customer_360
            .mappings
            .iter()
            .any(|m| m.target == "full_address");
        assert!(
            has_full_address,
            "Should have full_address denormalized column"
        );

        // PIPELINE 14: create_migration_summary (diamond dependency)
        let summary_pipeline = find_pipeline("create_migration_summary");
        assert!(
            summary_pipeline.depends_on.len() >= 2,
            "Should have multiple dependencies (diamond pattern)"
        );

        // ========================================================================
        // VALIDATE EXECUTION ORDER (DAG RESOLUTION)
        // ========================================================================

        // Verify execution stages make sense
        let total_pipelines_in_stages: usize = report
            .execution_order
            .iter()
            .map(|stage| stage.pipelines.len())
            .sum();
        assert_eq!(
            total_pipelines_in_stages, 14,
            "All 14 pipelines should be in execution stages"
        );

        // First stage should contain pipelines with no dependencies
        let first_stage = &report.execution_order[0];
        for pipeline_name in &first_stage.pipelines {
            let pipeline = find_pipeline(pipeline_name);
            assert!(
                pipeline.depends_on.is_empty()
                    || pipeline
                        .depends_on
                        .iter()
                        .all(|dep| !core_plan.pipelines.iter().any(|p| p.name == *dep)),
                "First stage pipeline '{}' should have no unresolved dependencies",
                pipeline_name
            );
        }

        // ========================================================================
        // VALIDATE METADATA
        // ========================================================================

        assert!(!report.plan_id.is_empty(), "Report should have an ID");
        assert!(
            !report.config_hash.is_empty(),
            "Report should have config hash"
        );
        assert!(
            report.config_path.contains("plan-generation.smql"),
            "Config path should be correct"
        );

        // Verify defines are resolved
        assert!(
            report.defines.constants.len() >= 4,
            "Should have resolved define constants"
        );

        // Verify connections are present
        assert!(
            report.connections.len() >= 2,
            "Should have at least source and destination connections"
        );

        // ========================================================================
        // VALIDATE DIAGNOSTICS (WARNINGS/ERRORS)
        // ========================================================================

        // Report might have warnings but should not have blocking errors
        if report.summary.error_count > 0 {
            // Print diagnostics for debugging
            for diagnostic in &report.diagnostics {
                if diagnostic.level == DiagnosticLevel::Error {
                    eprintln!(
                        "Error diagnostic: {} - {}",
                        diagnostic.code, diagnostic.message
                    );
                }
            }
            panic!(
                "Report has {} errors but should be executable",
                report.summary.error_count
            );
        }

        println!("Report generation test passed!");
        println!("  - {} pipelines planned", report.pipelines.len());
        println!("  - {} execution stages", report.execution_order.len());
        println!("  - {} warnings", report.summary.warning_count);
        println!("  - Report is executable: {}", report.is_executable);
    }
}
