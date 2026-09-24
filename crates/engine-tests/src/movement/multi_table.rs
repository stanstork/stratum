#[cfg(test)]
mod tests {
    use crate::direction_tests;
    use crate::harness::{Direction, runner};

    /// `from { tables = [...] }` fans out into one full-copy pipeline per table:
    /// every listed table is created at the destination and every row copied,
    /// with column names preserved.
    async fn multi_table_full_copy_case(dir: Direction) {
        dir.run(
            r#"
            pipeline "several" {
                from {
                    connection = connection.src
                    tables = ["actor", "category", "language"]
                }
                to { connection = connection.dst }
                settings {
                    create_missing_tables = true
                    batch_size            = 200
                }
            }
            "#,
        )
        .await;

        for t in ["actor", "category", "language"] {
            assert!(
                dir.dst_table_exists(t).await,
                "[{dir}] table '{t}' should be created"
            );
            dir.assert_row_parity(t, t).await;
        }

        // A straight copy keeps every column, names unchanged.
        assert_eq!(
            dir.dst_columns("actor").await,
            dir.src_columns("actor").await,
            "[{dir}] full copy should preserve actor's columns verbatim"
        );
    }
    direction_tests!(multi_table_full_copy, multi_table_full_copy_case);

    /// A per-table `select "T"` remaps that table's columns and `map` renames
    /// its destination table, while other listed tables copy in full. This is
    /// the multi-table twin of the single-pipeline column-remap in `columns.rs`.
    async fn multi_table_select_remap_case(dir: Direction) {
        dir.run(
            r#"
            pipeline "warehouse" {
                from {
                    connection = connection.src
                    tables = ["actor", "customer"]
                }
                to {
                    connection = connection.dst
                    map { customer = "dim_customer" }
                }
                select "customer" {
                    id          = customer.customer_id
                    given_name  = customer.first_name
                    family_name = customer.last_name
                }
                settings {
                    create_missing_tables = true
                    batch_size            = 200
                }
            }
            "#,
        )
        .await;

        // `actor` is untouched: full copy under its own name, all columns kept.
        assert!(dir.dst_table_exists("actor").await, "[{dir}] actor created");
        dir.assert_row_parity("actor", "actor").await;
        assert_eq!(
            dir.dst_columns("actor").await,
            dir.src_columns("actor").await,
            "[{dir}] actor should be copied verbatim"
        );

        // `customer` is renamed via `map` and projected via `select "customer"`.
        assert!(
            dir.dst_table_exists("dim_customer").await,
            "[{dir}] map should create the renamed destination table"
        );
        assert!(
            !dir.dst_table_exists("customer").await,
            "[{dir}] the source table name should not exist at the destination"
        );
        dir.assert_row_parity("customer", "dim_customer").await;

        // Exactly the projected+renamed columns, nothing else (dst_columns is
        // returned already sorted).
        assert_eq!(
            dir.dst_columns("dim_customer").await,
            vec![
                "family_name".to_string(),
                "given_name".to_string(),
                "id".to_string()
            ],
            "[{dir}] only the projected, renamed columns should exist"
        );

        // The rename carries data through: given_name holds source first_name,
        // and the source column names are gone.
        assert!(
            !dir.dst_column_exists("dim_customer", "first_name").await,
            "[{dir}] source column 'first_name' should not survive the projection"
        );
        let src_first = dir
            .src
            .scalar_string(
                dir.src.source_url(),
                "SELECT first_name FROM customer WHERE customer_id = 1",
            )
            .await;
        let dst_given = dir
            .dst
            .scalar_string(
                dir.dst.dest_url(),
                "SELECT given_name FROM dim_customer WHERE id = 1",
            )
            .await;
        assert!(
            src_first.is_some(),
            "[{dir}] source customer 1 should exist"
        );
        assert_eq!(
            src_first, dst_given,
            "[{dir}] given_name should carry the source first_name"
        );
    }
    direction_tests!(multi_table_select_remap, multi_table_select_remap_case);

    /// Several per-table overrides at once: two `map` renames and two
    /// `select "T"` projections in one block, with a third table copied in full.
    async fn multi_table_multi_rename_case(dir: Direction) {
        dir.run(
            r#"
            pipeline "warehouse" {
                from {
                    connection = connection.src
                    tables = ["actor", "category", "language"]
                }
                to {
                    connection = connection.dst
                    map { actor = "dim_actor"  category = "dim_category" }
                }
                select "actor"    { actor_id    = actor.actor_id      name  = actor.first_name }
                select "category" { category_id = category.category_id  label = category.name }
                settings { create_missing_tables = true  batch_size = 200 }
            }
            "#,
        )
        .await;

        // Two renamed+projected tables and one verbatim copy.
        assert!(dir.dst_table_exists("dim_actor").await, "[{dir}] dim_actor");
        assert!(
            dir.dst_table_exists("dim_category").await,
            "[{dir}] dim_category"
        );
        assert!(dir.dst_table_exists("language").await, "[{dir}] language");
        dir.assert_row_parity("actor", "dim_actor").await;
        dir.assert_row_parity("category", "dim_category").await;
        dir.assert_row_parity("language", "language").await;

        assert_eq!(
            dir.dst_columns("dim_actor").await,
            vec!["actor_id".to_string(), "name".to_string()],
            "[{dir}] dim_actor projected columns"
        );
        assert_eq!(
            dir.dst_columns("dim_category").await,
            vec!["category_id".to_string(), "label".to_string()],
            "[{dir}] dim_category projected columns"
        );
    }
    direction_tests!(multi_table_multi_rename, multi_table_multi_rename_case);

    /// A fanned-out `tables = [...]` migration run with `--integrity` produces
    /// valid Merkle receipts that `verify` accepts for each expanded table, in
    /// every direction.
    async fn multi_table_integrity_verify_case(dir: Direction) {
        let body = r#"
            pipeline "several" {
                from {
                    connection = connection.src
                    tables = ["actor", "category"]
                }
                to { connection = connection.dst }
                settings { create_missing_tables = true  batch_size = 200 }
            }
        "#;
        dir.reset().await;
        let ppl = dir.ppl(body);

        runner::run_ppl(&ppl, true)
            .await
            .unwrap_or_else(|e| panic!("[{dir}] integrity apply failed: {e:?}"));

        dir.assert_row_parity("actor", "actor").await;
        dir.assert_row_parity("category", "category").await;

        runner::run_verify_ppl(&ppl)
            .await
            .unwrap_or_else(|e| panic!("[{dir}] verify failed: {e:?}"));
    }
    direction_tests!(
        multi_table_integrity_verify,
        multi_table_integrity_verify_case
    );
}
