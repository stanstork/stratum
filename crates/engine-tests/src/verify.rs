#[cfg(test)]
mod tests {
    use crate::{
        reset_postgres_schema,
        utils::{
            DbType, get_row_count, run_smql, run_smql_file, run_smql_full_integrity,
            run_verify_smql,
        },
    };
    use tracing_test::traced_test;

    macro_rules! verify_config {
        ($name:expr) => {
            concat!(env!("CARGO_MANIFEST_DIR"), "/configs/verify/", $name)
        };
    }

    /// Simplest case: actor table, small row count, straightforward types.
    // Config: crates/engine-tests/configs/verify/actor.smql
    #[traced_test]
    #[tokio::test]
    async fn verify_actor_matches() {
        reset_postgres_schema().await;
        let smql = std::fs::read_to_string(verify_config!("actor.smql")).expect("read smql");
        run_smql(&smql, true).await.expect("apply failed");
        run_verify_smql(&smql).await.expect("verify failed");
    }

    /// Larger table with DECIMAL, TIMESTAMP, nullable INT columns.
    // Config: crates/engine-tests/configs/verify/payment.smql
    #[traced_test]
    #[tokio::test]
    async fn verify_payment_matches() {
        reset_postgres_schema().await;
        let smql = std::fs::read_to_string(verify_config!("payment.smql")).expect("read smql");
        run_smql(&smql, true).await.expect("apply failed");
        run_verify_smql(&smql).await.expect("verify failed");
    }

    /// Type-rich table: ENUM, SET, YEAR, DECIMAL, TEXT, nullable SMALLINT.
    // Config: crates/engine-tests/configs/verify/language.smql (prereq, no integrity)
    //         examples/configs/verify/film.smql      (with integrity)
    #[traced_test]
    #[tokio::test]
    async fn verify_film_matches() {
        reset_postgres_schema().await;
        // Language must exist first to satisfy film's FK constraint.
        // No integrity receipt is needed for this prerequisite step.
        run_smql_file(verify_config!("language.smql"))
            .await
            .expect("language apply failed");
        let film_smql =
            std::fs::read_to_string(verify_config!("film.smql")).expect("read film smql");
        run_smql(&film_smql, true).await.expect("film apply failed");
        run_verify_smql(&film_smql)
            .await
            .expect("film verify failed");
    }

    /// Verify works when batch_size divides row count evenly (no partial last batch).
    // Config: crates/engine-tests/configs/verify/actor_exact_batch.smql  (batch_size=200, actor=200 rows)
    #[traced_test]
    #[tokio::test]
    async fn verify_with_exact_batch_boundary() {
        reset_postgres_schema().await;
        let smql =
            std::fs::read_to_string(verify_config!("actor_exact_batch.smql")).expect("read smql");
        run_smql(&smql, true).await.expect("apply failed");
        run_verify_smql(&smql).await.expect("verify failed");
    }

    /// Verify works when the last batch is partial (total rows % batch_size != 0).
    // Config: crates/engine-tests/configs/verify/actor_partial_batch.smql  (batch_size=77, actor=200 rows)
    #[traced_test]
    #[tokio::test]
    async fn verify_with_partial_last_batch() {
        reset_postgres_schema().await;
        let smql =
            std::fs::read_to_string(verify_config!("actor_partial_batch.smql")).expect("read smql");
        run_smql(&smql, true).await.expect("apply failed");
        run_verify_smql(&smql).await.expect("verify failed");
    }

    /// Cascade migration: payment + FK depth-1 (customer, staff, rental).
    // Config: crates/engine-tests/configs/verify/payment_cascade.smql
    #[traced_test]
    #[tokio::test]
    async fn verify_cascade_payment_matches() {
        reset_postgres_schema().await;
        let smql =
            std::fs::read_to_string(verify_config!("payment_cascade.smql")).expect("read smql");
        run_smql(&smql, true).await.expect("cascade apply failed");
        run_verify_smql(&smql).await.expect("cascade verify failed");
    }

    /// Verify detects when a row has been modified in the destination after apply.
    // Config: crates/engine-tests/configs/verify/actor.smql
    #[traced_test]
    #[tokio::test]
    async fn verify_detects_modified_row() {
        use crate::utils::execute;

        reset_postgres_schema().await;
        let smql = std::fs::read_to_string(verify_config!("actor.smql")).expect("read smql");
        run_smql(&smql, true).await.expect("apply failed");

        execute("UPDATE actor SET first_name = 'TAMPERED' WHERE actor_id = 1").await;

        let result = run_verify_smql(&smql).await;
        assert!(result.is_err(), "verify should have detected the mismatch");
    }

    /// Verify detects when a row has been deleted from the destination after apply.
    // Config: crates/engine-tests/configs/verify/actor.smql
    #[traced_test]
    #[tokio::test]
    async fn verify_detects_deleted_row() {
        use crate::utils::execute;

        reset_postgres_schema().await;
        let smql = std::fs::read_to_string(verify_config!("actor.smql")).expect("read smql");
        run_smql(&smql, true).await.expect("apply failed");

        execute("DELETE FROM actor WHERE actor_id = 1").await;

        let result = run_verify_smql(&smql).await;
        assert!(result.is_err(), "verify should have detected the mismatch");
    }

    /// Verify returns NoPriorRun (no error) when no receipt exists yet.
    // Config: crates/engine-tests/configs/verify/actor.smql
    #[traced_test]
    #[tokio::test]
    async fn verify_no_receipt_is_not_error() {
        reset_postgres_schema().await;
        let smql = std::fs::read_to_string(verify_config!("actor.smql")).expect("read smql");
        // Migrate without --integrity so no receipt is written.
        run_smql(&smql, false).await.expect("apply failed");
        run_verify_smql(&smql)
            .await
            .expect("verify should not error on missing receipt");
    }

    /// WHERE filter: only rows passing the filter are migrated.
    ///
    /// The receipt is built from filtered rows only. The destination contains
    /// only those rows. Verify reads the destination as-is (no re-applying of
    /// the filter) and compares against the receipt - hashes should match.
    ///
    /// Key insight: verify checks "destination == receipt", NOT "destination == source".
    /// A filtered migration verifies correctly even though it's a subset of the source.
    // Config: crates/engine-tests/configs/verify/payment_staff_filter.smql
    #[traced_test]
    #[tokio::test]
    async fn verify_with_where_filter_matches() {
        reset_postgres_schema().await;
        let smql = std::fs::read_to_string(verify_config!("payment_staff_filter.smql"))
            .expect("read smql");

        run_smql(&smql, true).await.expect("apply failed");

        // Destination has only staff_id=1 rows - fewer than the full payment table.
        let dest_count = get_row_count("payment", "sakila", DbType::Postgres).await;
        let total_count = get_row_count("payment", "sakila", DbType::MySql).await;
        assert!(
            dest_count < total_count,
            "filter should have excluded some rows (got {dest_count} of {total_count})"
        );

        run_verify_smql(&smql).await.expect("verify failed");
    }

    /// SKIP validation: rows failing the check are excluded from the destination.
    ///
    /// `action = skip` silently drops rows that fail the check - they never reach
    /// the destination and are not included in the receipt. Verify sees a smaller
    /// destination but the receipt matches exactly what was written.
    // Config: crates/engine-tests/configs/verify/payment_skip_validation.smql
    #[traced_test]
    #[tokio::test]
    async fn verify_with_skip_validation_matches() {
        reset_postgres_schema().await;
        let smql = std::fs::read_to_string(verify_config!("payment_skip_validation.smql"))
            .expect("read smql");

        run_smql(&smql, true).await.expect("apply failed");

        // Destination has only amount <= 5.00 rows.
        let dest_count = get_row_count("payment", "sakila", DbType::Postgres).await;
        let total_count = get_row_count("payment", "sakila", DbType::MySql).await;
        assert!(
            dest_count < total_count,
            "skip validation should have excluded some rows (got {dest_count} of {total_count})"
        );

        run_verify_smql(&smql).await.expect("verify failed");
    }

    /// WARN validation: rows failing the check are still migrated (warn != skip).
    ///
    /// All rows reach the destination. The receipt covers the full table.
    /// Verify should pass with the complete row count.
    // Config: crates/engine-tests/configs/verify/actor_warn_validation.smql
    #[traced_test]
    #[tokio::test]
    async fn verify_with_warn_validation_all_rows_present() {
        reset_postgres_schema().await;
        let smql = std::fs::read_to_string(verify_config!("actor_warn_validation.smql"))
            .expect("read smql");

        run_smql(&smql, true).await.expect("apply failed");

        // Warn does NOT skip rows - destination should have the full actor count.
        let dest_count = get_row_count("actor", "sakila", DbType::Postgres).await;
        let source_count = get_row_count("actor", "sakila", DbType::MySql).await;
        assert_eq!(
            dest_count, source_count,
            "warn validation must not skip rows (dest={dest_count}, src={source_count})"
        );

        run_verify_smql(&smql).await.expect("verify failed");
    }

    /// WHERE filter + SKIP validation combined.
    ///
    /// The filter narrows the source rows first (pushes SQL WHERE to source query),
    /// then the skip validation further excludes rows that pass the filter but fail
    /// the check. Verify only sees the doubly-reduced destination and compares it
    /// against the receipt, which was built from the same doubly-reduced set.
    // Config: crates/engine-tests/configs/verify/payment_filter_and_skip.smql
    #[traced_test]
    #[tokio::test]
    async fn verify_filter_and_skip_combined() {
        reset_postgres_schema().await;
        let smql = std::fs::read_to_string(verify_config!("payment_filter_and_skip.smql"))
            .expect("read smql");

        run_smql(&smql, true).await.expect("apply failed");

        // Both conditions reduce the destination below the full payment count.
        let dest_count = get_row_count("payment", "sakila", DbType::Postgres).await;
        let total_count = get_row_count("payment", "sakila", DbType::MySql).await;
        assert!(
            dest_count < total_count,
            "filter + skip should have excluded rows (got {dest_count} of {total_count})"
        );

        run_verify_smql(&smql).await.expect("verify failed");
    }

    /// JOIN + WHERE filter + TIMESTAMP pagination, then verify.
    ///
    /// Migrates rental rows for staff_id=1 only (≈8,000 of 16,044), joined with
    /// customer to produce a computed customer_name column, paginated with a
    /// timestamp cursor on rental_date + tiebreaker rental_id (batch=200, ≈40 pages).
    ///
    /// Verify must handle:
    ///   - computed columns in the receipt hashes
    ///   - a filtered source (receipt covers only the staff_id=1 rows)
    ///   - non-PK cursor: verify replays identical timestamp batch boundaries
    // Config: crates/engine-tests/configs/verify/rental_join_filter_ts.smql
    #[traced_test]
    #[tokio::test]
    async fn verify_join_filter_timestamp_pagination() {
        reset_postgres_schema().await;
        let smql = std::fs::read_to_string(verify_config!("rental_join_filter_ts.smql"))
            .expect("read smql");
        run_smql(&smql, true).await.expect("apply failed");
        run_verify_smql(&smql).await.expect("verify failed");
    }

    /// CASCADE migration of payment with NUMERIC pagination, then verify.
    ///
    /// Root table is payment, cascade depth=1 pulls customer/staff/rental
    /// alongside each payment batch. The root is paginated by the composite
    /// numeric cursor (payment.staff_id + payment.payment_id, batch=500).
    ///
    /// staff_id has only 2 distinct values so every page boundary lands
    /// mid-run, exercising the composite cursor on every page.
    ///
    /// Verify must handle:
    ///   - cascade receipts with sorted_hashes=true (FK-dependent row order varies)
    ///   - non-PK cursor on the root table
    ///   - variable cascade row counts per batch (customer/staff/rental counts
    ///     differ between the staff_id=1 and staff_id=2 halves)
    // Config: crates/engine-tests/configs/verify/payment_cascade_numeric.smql
    #[traced_test]
    #[tokio::test]
    async fn verify_cascade_with_numeric_pagination() {
        reset_postgres_schema().await;
        let smql = std::fs::read_to_string(verify_config!("payment_cascade_numeric.smql"))
            .expect("read smql");
        run_smql(&smql, true).await.expect("apply failed");
        run_verify_smql(&smql).await.expect("verify failed");
    }

    /// Tamper detection still works on a filtered result set.
    ///
    /// Even though only a subset was migrated, modifying any row in that subset
    /// changes its hash and causes verify to detect the mismatch.
    // Config: crates/engine-tests/configs/verify/payment_staff_filter.smql
    #[traced_test]
    #[tokio::test]
    async fn verify_filtered_detects_tampering() {
        use crate::utils::execute;

        reset_postgres_schema().await;
        let smql = std::fs::read_to_string(verify_config!("payment_staff_filter.smql"))
            .expect("read smql");

        run_smql(&smql, true).await.expect("apply failed");

        execute(
            "UPDATE payment SET amount = amount + 100.00 WHERE payment_id = \
             (SELECT MIN(payment_id) FROM payment)",
        )
        .await;

        let result = run_verify_smql(&smql).await;
        assert!(
            result.is_err(),
            "verify should detect tampering in filtered result set"
        );
    }

    /// Verify detects when a row has been inserted into the destination after apply.
    ///
    /// The receipt records N rows across M batches. If the destination gains a new
    /// row after apply, the hash of at least one batch will differ from the receipt.
    // Config: crates/engine-tests/configs/verify/actor.smql
    #[traced_test]
    #[tokio::test]
    async fn verify_detects_inserted_row() {
        use crate::utils::execute;

        reset_postgres_schema().await;
        let smql = std::fs::read_to_string(verify_config!("actor.smql")).expect("read smql");
        run_smql(&smql, true).await.expect("apply failed");

        execute(
            "INSERT INTO actor (actor_id, first_name, last_name, last_update) \
             VALUES (9999, 'GHOST', 'ROW', NOW())",
        )
        .await;

        let result = run_verify_smql(&smql).await;
        assert!(
            result.is_err(),
            "verify should detect the extra inserted row"
        );
    }

    /// Verify detects when a computed column value is modified in the destination.
    ///
    /// customer_name is a concat() of first_name + last_name from the joined
    /// customer table. If someone updates customer_name directly in the
    /// destination, the row hash changes and verify must catch it.
    // Config: crates/engine-tests/configs/verify/rental_join_filter_ts.smql
    #[traced_test]
    #[tokio::test]
    async fn verify_detects_tamper_in_computed_column() {
        use crate::utils::execute;

        reset_postgres_schema().await;
        let smql = std::fs::read_to_string(verify_config!("rental_join_filter_ts.smql"))
            .expect("read smql");
        run_smql(&smql, true).await.expect("apply failed");

        execute(
            "UPDATE rental SET customer_name = 'TAMPERED NAME' \
             WHERE rental_id = (SELECT MIN(rental_id) FROM rental)",
        )
        .await;

        let result = run_verify_smql(&smql).await;
        assert!(
            result.is_err(),
            "verify should detect tampering in a computed column"
        );
    }

    /// Verify detects tampering in a cascade leaf table.
    ///
    /// The cascade migration writes payment, customer, staff, and rental rows.
    /// Each cascade table has its own receipt with sorted_hashes=true.
    /// Modifying a row in a leaf table (customer) must change its receipt hash
    /// and cause verify to report a mismatch.
    // Config: crates/engine-tests/configs/verify/payment_cascade.smql
    #[traced_test]
    #[tokio::test]
    async fn verify_cascade_detects_tamper_in_leaf_table() {
        use crate::utils::execute;

        reset_postgres_schema().await;
        let smql =
            std::fs::read_to_string(verify_config!("payment_cascade.smql")).expect("read smql");
        run_smql(&smql, true).await.expect("cascade apply failed");

        // Tamper with a customer row - a cascade leaf table
        execute(
            "UPDATE customer SET first_name = 'TAMPERED' \
             WHERE customer_id = (SELECT MIN(customer_id) FROM customer)",
        )
        .await;

        let result = run_verify_smql(&smql).await;
        assert!(
            result.is_err(),
            "verify should detect tampering in a cascade leaf table"
        );
    }

    /// Verify works with PK (keyset) pagination - the most common real-world strategy.
    ///
    /// Receipt batch boundaries are keyed by payment_id cursor positions.
    /// Verify replays identical page boundaries by re-reading payment in
    /// the same order.
    // Config: crates/engine-tests/configs/verify/payment_pk.smql
    #[traced_test]
    #[tokio::test]
    async fn verify_with_pk_pagination() {
        reset_postgres_schema().await;
        let smql = std::fs::read_to_string(verify_config!("payment_pk.smql")).expect("read smql");
        run_smql(&smql, true).await.expect("apply failed");
        run_verify_smql(&smql).await.expect("verify failed");
    }

    /// Verify works with NUMERIC pagination on a plain (non-cascade) table.
    ///
    /// Distinct from the cascade+numeric test: single table, no FK-dependent rows.
    /// Confirms the numeric composite cursor (staff_id + payment_id) replays
    /// correctly in verify when there is no cascade receipt overhead.
    // Config: crates/engine-tests/configs/verify/payment_numeric_plain.smql
    #[traced_test]
    #[tokio::test]
    async fn verify_with_numeric_pagination_no_cascade() {
        reset_postgres_schema().await;
        let smql = std::fs::read_to_string(verify_config!("payment_numeric_plain.smql"))
            .expect("read smql");
        run_smql(&smql, true).await.expect("apply failed");
        run_verify_smql(&smql).await.expect("verify failed");
    }

    /// Verify works with DEFAULT (OFFSET/LIMIT) pagination at scale.
    ///
    /// OFFSET-based batches carry no cursor state; the receipt records row counts
    /// per batch index and verify replays pages sequentially by position.
    // Config: crates/engine-tests/configs/verify/payment_default.smql
    #[traced_test]
    #[tokio::test]
    async fn verify_with_default_pagination_large_table() {
        reset_postgres_schema().await;
        let smql =
            std::fs::read_to_string(verify_config!("payment_default.smql")).expect("read smql");
        run_smql(&smql, true).await.expect("apply failed");
        run_verify_smql(&smql).await.expect("verify failed");
    }

    /// Verify handles an empty result set (0 rows, 0 batches in receipt).
    ///
    /// A WHERE clause that matches no rows produces an empty migration.
    /// Verify must exit its batch loop immediately and return OK rather
    /// than looping indefinitely or reporting a spurious mismatch.
    // Config: crates/engine-tests/configs/verify/actor_empty.smql
    #[traced_test]
    #[tokio::test]
    async fn verify_empty_table() {
        reset_postgres_schema().await;
        let smql = std::fs::read_to_string(verify_config!("actor_empty.smql")).expect("read smql");
        run_smql(&smql, true).await.expect("apply failed");
        run_verify_smql(&smql)
            .await
            .expect("verify of empty result should pass");
    }

    /// Verify handles a single-row migration where batch_size = 1.
    ///
    /// The first batch is simultaneously the last - exercises the boundary
    /// where start = end = only batch. The receipt has exactly one entry.
    // Config: crates/engine-tests/configs/verify/actor_single_row.smql
    #[traced_test]
    #[tokio::test]
    async fn verify_single_row() {
        reset_postgres_schema().await;
        let smql =
            std::fs::read_to_string(verify_config!("actor_single_row.smql")).expect("read smql");
        run_smql(&smql, true).await.expect("apply failed");

        let dest_count = get_row_count("actor", "sakila", DbType::Postgres).await;
        assert_eq!(dest_count, 1, "expected exactly 1 row in destination");

        run_verify_smql(&smql).await.expect("verify failed");
    }

    /// Verify hashes NULL values consistently across source and destination.
    ///
    /// rental.return_date is DATETIME NULL; many rows have NULL there (open
    /// rentals not yet returned). Stable NULL encoding is required for hashes
    /// to match between MySQL and PostgreSQL.
    // Config: crates/engine-tests/configs/verify/rental_plain.smql
    #[traced_test]
    #[tokio::test]
    async fn verify_many_null_columns() {
        reset_postgres_schema().await;
        let smql = std::fs::read_to_string(verify_config!("rental_plain.smql")).expect("read smql");
        run_smql(&smql, true).await.expect("apply failed");
        run_verify_smql(&smql).await.expect("verify failed");
    }

    /// Verify is idempotent - running it twice on unchanged data must both pass.
    ///
    /// Verify is a read-only operation. Re-running it against the same receipt
    /// and the same destination must produce identical results without side effects.
    // Config: crates/engine-tests/configs/verify/actor.smql
    #[traced_test]
    #[tokio::test]
    async fn verify_idempotent() {
        reset_postgres_schema().await;
        let smql = std::fs::read_to_string(verify_config!("actor.smql")).expect("read smql");
        run_smql(&smql, true).await.expect("apply failed");
        run_verify_smql(&smql).await.expect("first verify failed");
        // Run a second time - receipt and data are unchanged; must still pass
        run_verify_smql(&smql)
            .await
            .expect("second verify should also pass");
    }

    /// Verify handles a config with multiple pipelines linked by an after dependency.
    ///
    /// Both language (6 rows) and film (1,000 rows) pipelines are migrated with
    /// integrity. run_verify_smql must verify every pipeline in the config and
    /// return OK only if all receipts match.
    // Config: crates/engine-tests/configs/verify/dag_language_film.smql
    #[traced_test]
    #[tokio::test]
    async fn verify_dag_pipeline() {
        reset_postgres_schema().await;
        let smql =
            std::fs::read_to_string(verify_config!("dag_language_film.smql")).expect("read smql");
        run_smql(&smql, true).await.expect("dag apply failed");
        run_verify_smql(&smql).await.expect("dag verify failed");
    }

    /// `--full-integrity` stores individual row hashes; verify still passes when
    /// the destination is untouched.
    // Config: crates/engine-tests/configs/verify/actor.smql
    #[traced_test]
    #[tokio::test]
    async fn verify_full_integrity_matches() {
        reset_postgres_schema().await;
        let smql = std::fs::read_to_string(verify_config!("actor.smql")).expect("read smql");
        run_smql_full_integrity(&smql).await.expect("apply failed");
        run_verify_smql(&smql).await.expect("verify failed");
    }

    /// `--full-integrity` enables row-level detection: verify reports the exact
    /// row index that was tampered, not just the batch range.
    // Config: crates/engine-tests/configs/verify/actor.smql
    #[traced_test]
    #[tokio::test]
    async fn verify_full_integrity_detects_modified_row_at_index() {
        use crate::utils::execute;
        use engine_core::{context::env::EnvContext, plan::execution::ExecutionPlan};
        use engine_verify::verifier::verify as run_verify;
        use smql_syntax::builder::parse;
        use std::sync::Arc;

        reset_postgres_schema().await;
        let smql = std::fs::read_to_string(verify_config!("actor.smql")).expect("read smql");
        run_smql_full_integrity(&smql).await.expect("apply failed");

        // Tamper with actor_id = 5 (the 5th row, index 4).
        execute("UPDATE actor SET last_name = 'HACKED' WHERE actor_id = 5").await;

        // Run verify and check that it returns a Mismatch with row-level detail.
        let doc = parse(&smql).expect("parse");
        let env = Arc::new(EnvContext::empty());
        let plan = ExecutionPlan::build(&doc, env.clone()).expect("build plan");
        let results = run_verify(plan, env).await.expect("verify call failed");

        let has_mismatch = results.iter().any(|r| {
            matches!(
                r,
                model::integrity::result::VerificationResult::Mismatch { .. }
            )
        });
        assert!(
            has_mismatch,
            "verify should detect the tampered row with full-integrity"
        );
    }
}
