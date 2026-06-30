#[cfg(test)]
mod tests {
    use crate::{
        reset_postgres_schema,
        utils::{assert_row_count, assert_table_exists, run_smql},
    };
    use tracing_test::traced_test;

    // Test DAG: Simple sequential dependencies (A -> B -> C)
    // Scenario:
    // - Pipeline A (copy_actors) has no dependencies
    // - Pipeline B (copy_customers) depends on A
    // - Pipeline C (copy_film) depends on B
    // Expected Outcome:
    // - Pipelines execute in order: A, then B, then C
    // - All tables should be created and populated
    // - Data should be migrated successfully for all pipelines
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn tc_dag_01_sequential_dependencies() {
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

            // First pipeline - no dependencies
            pipeline "copy_actors" {
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

            // Second pipeline - depends on copy_actors
            pipeline "copy_customers" {
                after = [pipeline.copy_actors]

                from {
                    connection = connection.mysql_source
                    table      = "customer"
                }
                to {
                    connection = connection.pg_destination
                    table      = "customer"
                }
                settings {
                    create_missing_tables = true
                    ignore_constraints    = true
                }
            }

            // Third pipeline - depends on copy_customers
            pipeline "copy_film" {
                after = [pipeline.copy_customers]

                from {
                    connection = connection.mysql_source
                    table      = "film"
                }
                to {
                    connection = connection.pg_destination
                    table      = "film"
                }
                settings {
                    create_missing_tables = true
                    ignore_constraints    = true
                }
            }
        "#;

        let _ = run_smql(tmpl, false).await;

        // Verify all tables were created and populated
        assert_table_exists("actor", true).await;
        assert_table_exists("customer", true).await;
        assert_table_exists("film", true).await;

        assert_row_count("actor", "sakila", "actor").await;
        assert_row_count("customer", "sakila", "customer").await;
        assert_row_count("film", "sakila", "film").await;
    }

    // Test DAG: Parallel execution with shared dependency
    // Scenario:
    // - Pipeline A (copy_actors) has no dependencies
    // - Pipelines B (copy_customers) and C (copy_film) both depend on A
    // - Pipeline D (copy_payment) depends on both B and C
    // Expected Outcome:
    // - A executes first
    // - B and C can execute in parallel (both depend only on A)
    // - D executes last (after both B and C complete)
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn tc_dag_02_parallel_execution() {
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

            // Root pipeline - no dependencies
            pipeline "copy_actors" {
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

            // Can run in parallel with copy_film
            pipeline "copy_customers" {
                after = [pipeline.copy_actors]

                from {
                    connection = connection.mysql_source
                    table      = "customer"
                }
                to {
                    connection = connection.pg_destination
                    table      = "customer"
                }
                settings {
                    create_missing_tables = true
                    ignore_constraints    = true
                }
            }

            // Can run in parallel with copy_customers
            pipeline "copy_film" {
                after = [pipeline.copy_actors]

                from {
                    connection = connection.mysql_source
                    table      = "film"
                }
                to {
                    connection = connection.pg_destination
                    table      = "film"
                }
                settings {
                    create_missing_tables = true
                    ignore_constraints    = true
                }
            }

            // Runs after both copy_customers and copy_film complete
            pipeline "copy_payment" {
                after = [pipeline.copy_customers, pipeline.copy_film]

                from {
                    connection = connection.mysql_source
                    table      = "payment"
                }
                to {
                    connection = connection.pg_destination
                    table      = "payment"
                }
                settings {
                    create_missing_tables = true
                    ignore_constraints    = true
                }
            }
        "#;

        let _ = run_smql(tmpl, false).await;

        // Verify all tables were created and populated
        assert_table_exists("actor", true).await;
        assert_table_exists("customer", true).await;
        assert_table_exists("film", true).await;
        assert_table_exists("payment", true).await;

        assert_row_count("actor", "sakila", "actor").await;
        assert_row_count("customer", "sakila", "customer").await;
        assert_row_count("film", "sakila", "film").await;
        assert_row_count("payment", "sakila", "payment").await;
    }

    // Test DAG: Diamond dependency pattern
    // Scenario:
    // - Pipeline A (copy_actors) has no dependencies
    // - Pipelines B (copy_film) and C (copy_customer) both depend on A
    // - Pipeline D (copy_film_actor) depends on both B and C
    // - Pipeline E (copy_payment) depends on D
    // Expected Outcome:
    // - A executes first (level 0)
    // - B and C execute in parallel (level 1)
    // - D executes after B and C (level 2)
    // - E executes last (level 3)
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn tc_dag_03_diamond_dependencies() {
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

            // Level 0: Root
            pipeline "copy_actors" {
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

            // Level 1: Depends on copy_actors
            pipeline "copy_film" {
                after = [pipeline.copy_actors]

                from {
                    connection = connection.mysql_source
                    table      = "film"
                }
                to {
                    connection = connection.pg_destination
                    table      = "film"
                }
                settings {
                    create_missing_tables = true
                    ignore_constraints    = true
                }
            }

            // Level 1: Depends on copy_actors (parallel with copy_film)
            pipeline "copy_customer" {
                after = [pipeline.copy_actors]

                from {
                    connection = connection.mysql_source
                    table      = "customer"
                }
                to {
                    connection = connection.pg_destination
                    table      = "customer"
                }
                settings {
                    create_missing_tables = true
                    ignore_constraints    = true
                }
            }

            // Level 2: Depends on both copy_film and copy_customer
            pipeline "copy_film_actor" {
                after = [pipeline.copy_film, pipeline.copy_customer]

                from {
                    connection = connection.mysql_source
                    table      = "film_actor"
                }
                to {
                    connection = connection.pg_destination
                    table      = "film_actor"
                }
                settings {
                    create_missing_tables = true
                    ignore_constraints    = true
                }
            }

            // Level 3: Depends on copy_film_actor
            pipeline "copy_payment" {
                after = [pipeline.copy_film_actor]

                from {
                    connection = connection.mysql_source
                    table      = "payment"
                }
                to {
                    connection = connection.pg_destination
                    table      = "payment"
                }
                settings {
                    create_missing_tables = true
                    ignore_constraints    = true
                }
            }
        "#;

        let _ = run_smql(tmpl, false).await;

        // Verify all tables were created and populated
        assert_table_exists("actor", true).await;
        assert_table_exists("film", true).await;
        assert_table_exists("customer", true).await;
        assert_table_exists("film_actor", true).await;
        assert_table_exists("payment", true).await;

        assert_row_count("actor", "sakila", "actor").await;
        assert_row_count("film", "sakila", "film").await;
        assert_row_count("customer", "sakila", "customer").await;
        assert_row_count("film_actor", "sakila", "film_actor").await;
        assert_row_count("payment", "sakila", "payment").await;
    }

    // Test DAG: Multiple independent pipelines (no dependencies)
    // Scenario:
    // - Multiple pipelines with no dependencies between them
    // - All pipelines should be able to run in parallel
    // Expected Outcome:
    // - All pipelines execute in parallel (single level)
    // - All tables created and populated successfully
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn tc_dag_04_independent_pipelines() {
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

            // All independent pipelines
            pipeline "copy_actors" {
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

            pipeline "copy_film" {
                from {
                    connection = connection.mysql_source
                    table      = "film"
                }
                to {
                    connection = connection.pg_destination
                    table      = "film"
                }
                settings {
                    create_missing_tables = true
                    ignore_constraints    = true
                }
            }

            pipeline "copy_customer" {
                from {
                    connection = connection.mysql_source
                    table      = "customer"
                }
                to {
                    connection = connection.pg_destination
                    table      = "customer"
                }
                settings {
                    create_missing_tables = true
                    ignore_constraints    = true
                }
            }
        "#;

        let _ = run_smql(tmpl, false).await;

        // Verify all tables were created and populated
        assert_table_exists("actor", true).await;
        assert_table_exists("film", true).await;
        assert_table_exists("customer", true).await;

        assert_row_count("actor", "sakila", "actor").await;
        assert_row_count("film", "sakila", "film").await;
        assert_row_count("customer", "sakila", "customer").await;
    }

    // Test DAG: Complex multi-level dependencies
    // Scenario:
    // - 6 pipelines with complex dependencies spanning 4 levels
    // - Tests deep dependency chains
    // Expected Outcome:
    // - Pipelines execute in correct topological order
    // - All data migrated successfully
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn tc_dag_05_complex_dependencies() {
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

            // Level 0: Two independent roots
            pipeline "copy_actors" {
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

            pipeline "copy_language" {
                from {
                    connection = connection.mysql_source
                    table      = "language"
                }
                to {
                    connection = connection.pg_destination
                    table      = "language"
                }
                settings {
                    create_missing_tables = true
                }
            }

            // Level 1: Depends on copy_language
            pipeline "copy_film" {
                after = [pipeline.copy_language]

                from {
                    connection = connection.mysql_source
                    table      = "film"
                }
                to {
                    connection = connection.pg_destination
                    table      = "film"
                }
                settings {
                    create_missing_tables = true
                    ignore_constraints    = true
                }
            }

            // Level 2: Depends on both copy_actors and copy_film
            pipeline "copy_film_actor" {
                after = [pipeline.copy_actors, pipeline.copy_film]

                from {
                    connection = connection.mysql_source
                    table      = "film_actor"
                }
                to {
                    connection = connection.pg_destination
                    table      = "film_actor"
                }
                settings {
                    create_missing_tables = true
                    ignore_constraints    = true
                }
            }

            // Level 1: Depends on copy_actors (parallel with copy_film)
            pipeline "copy_customer" {
                after = [pipeline.copy_actors]

                from {
                    connection = connection.mysql_source
                    table      = "customer"
                }
                to {
                    connection = connection.pg_destination
                    table      = "customer"
                }
                settings {
                    create_missing_tables = true
                    ignore_constraints    = true
                }
            }

            // Level 3: Depends on both copy_film_actor and copy_customer
            pipeline "copy_payment" {
                after = [pipeline.copy_film_actor, pipeline.copy_customer]

                from {
                    connection = connection.mysql_source
                    table      = "payment"
                }
                to {
                    connection = connection.pg_destination
                    table      = "payment"
                }
                settings {
                    create_missing_tables = true
                    ignore_constraints    = true
                }
            }
        "#;

        let _ = run_smql(tmpl, false).await;

        // Verify all tables were created and populated
        assert_table_exists("actor", true).await;
        assert_table_exists("language", true).await;
        assert_table_exists("film", true).await;
        assert_table_exists("film_actor", true).await;
        assert_table_exists("customer", true).await;
        assert_table_exists("payment", true).await;

        assert_row_count("actor", "sakila", "actor").await;
        assert_row_count("language", "sakila", "language").await;
        assert_row_count("film", "sakila", "film").await;
        assert_row_count("film_actor", "sakila", "film_actor").await;
        assert_row_count("customer", "sakila", "customer").await;
        assert_row_count("payment", "sakila", "payment").await;
    }

    // Test DAG: Failure handling with ContinueIndependent strategy
    // Scenario:
    // - Pipeline A (copy_actors) succeeds
    // - Pipeline B (copy_invalid) fails with invalid validation
    // - Pipeline C (copy_film) depends on A (should succeed)
    // - Pipeline D (copy_customer) depends on B (should be skipped)
    // - Pipeline E (copy_payment) depends on C and D (should be skipped)
    // Expected Outcome:
    // - A and C should succeed
    // - B should fail
    // - D and E should be skipped due to failed dependency
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn tc_dag_07_failure_continue_independent() {
        reset_postgres_schema().await;

        let tmpl = r#"
            execution {
                max_concurrency = 8
                on_failure = "continue"
            }

            connection "mysql_source" {
                driver = "mysql"
                url    = "mysql://sakila_user:qwerty123@localhost:3306/sakila"
            }
            connection "pg_destination" {
                driver = "postgres"
                url    = "postgres://user:password@localhost:5432/testdb"
            }

            // Should succeed
            pipeline "copy_actors" {
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

            // Should fail - validation that always fails
            pipeline "copy_invalid" {
                from {
                    connection = connection.mysql_source
                    table      = "language"
                }
                to {
                    connection = connection.pg_destination
                    table      = "language_invalid"
                }
                settings {
                    create_missing_tables = true
                    batch_size = 1
                }
                validate {
                    assert "always_fail" {
                        check   = false
                        message = "This validation always fails"
                        action  = fail
                    }
                }
            }

            // Should succeed - independent of copy_invalid
            pipeline "copy_film" {
                after = [pipeline.copy_actors]

                from {
                    connection = connection.mysql_source
                    table      = "film"
                }
                to {
                    connection = connection.pg_destination
                    table      = "film"
                }
                settings {
                    create_missing_tables = true
                    ignore_constraints    = true
                }
            }

            // Should be skipped - depends on failed pipeline
            pipeline "copy_customer" {
                after = [pipeline.copy_invalid]

                from {
                    connection = connection.mysql_source
                    table      = "customer"
                }
                to {
                    connection = connection.pg_destination
                    table      = "customer"
                }
                settings {
                    create_missing_tables = true
                    ignore_constraints    = true
                }
            }

            // Should be skipped - depends on skipped pipeline
            pipeline "copy_payment" {
                after = [pipeline.copy_film, pipeline.copy_customer]

                from {
                    connection = connection.mysql_source
                    table      = "payment"
                }
                to {
                    connection = connection.pg_destination
                    table      = "payment"
                }
                settings {
                    create_missing_tables = true
                    ignore_constraints    = true
                }
            }
        "#;

        // This should fail but continue with independent pipelines
        let _ = run_smql(tmpl, false).await;

        // Verify successful pipelines
        assert_table_exists("actor", true).await;
        assert_table_exists("film", true).await;
        assert_row_count("actor", "sakila", "actor").await;
        assert_row_count("film", "sakila", "film").await;

        // Verify failed/skipped pipelines
        assert_table_exists("customer", false).await;
        assert_table_exists("payment", false).await;
    }

    // Test DAG: Wide dependency tree
    // Scenario:
    // - One root pipeline with many dependent pipelines
    // - Tests parallel execution capability
    // Expected Outcome:
    // - Root executes first
    // - All dependent pipelines can execute in parallel
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn tc_dag_06_wide_dependencies() {
        reset_postgres_schema().await;

        let tmpl = r#"
            execution {
                strategy = "parallel"
                max_concurrency = 8
                on_failure = "fail_fast"
            }

            connection "mysql_source" {
                driver = "mysql"
                url    = "mysql://sakila_user:qwerty123@localhost:3306/sakila"
            }
            connection "pg_destination" {
                driver = "postgres"
                url    = "postgres://user:password@localhost:5432/testdb"
            }

            // Root pipeline
            pipeline "copy_actors" {
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

            // All depend on copy_actors and can run in parallel
            pipeline "copy_film" {
                after = [pipeline.copy_actors]

                from {
                    connection = connection.mysql_source
                    table      = "film"
                }
                to {
                    connection = connection.pg_destination
                    table      = "film"
                }
                settings {
                    create_missing_tables = true
                    ignore_constraints    = true
                }
            }

            pipeline "copy_customer" {
                after = [pipeline.copy_actors]

                from {
                    connection = connection.mysql_source
                    table      = "customer"
                }
                to {
                    connection = connection.pg_destination
                    table      = "customer"
                }
                settings {
                    create_missing_tables = true
                    ignore_constraints    = true
                }
            }

            pipeline "copy_language" {
                after = [pipeline.copy_actors]

                from {
                    connection = connection.mysql_source
                    table      = "language"
                }
                to {
                    connection = connection.pg_destination
                    table      = "language"
                }
                settings {
                    create_missing_tables = true
                }
            }

            pipeline "copy_category" {
                after = [pipeline.copy_actors]

                from {
                    connection = connection.mysql_source
                    table      = "category"
                }
                to {
                    connection = connection.pg_destination
                    table      = "category"
                }
                settings {
                    create_missing_tables = true
                }
            }
        "#;

        let _ = run_smql(tmpl, false).await;

        // Verify all tables were created and populated
        assert_table_exists("actor", true).await;
        assert_table_exists("film", true).await;
        assert_table_exists("customer", true).await;
        assert_table_exists("language", true).await;
        assert_table_exists("category", true).await;

        assert_row_count("actor", "sakila", "actor").await;
        assert_row_count("film", "sakila", "film").await;
        assert_row_count("customer", "sakila", "customer").await;
        assert_row_count("language", "sakila", "language").await;
        assert_row_count("category", "sakila", "category").await;
    }
}
