#[cfg(test)]
mod tests {
    use crate::{
        mysql_pool, reset_postgres_schema,
        utils::{DbType, assert_row_count, get_row_count, run_smql},
    };
    use mysql_async::prelude::Queryable;
    use tracing_test::traced_test;

    macro_rules! paginate_config {
        ($name:expr) => {
            concat!(env!("CARGO_MANIFEST_DIR"), "/configs/paginate/", $name)
        };
    }

    /// Default strategy: plain OFFSET/LIMIT pagination, no ordering.
    ///
    /// 200 actor rows, batch_size=50 -> 4 pages.
    /// Verifies that OFFSET-based pagination fetches every row exactly once.
    #[traced_test]
    #[tokio::test]
    async fn paginate_default_offset() {
        reset_postgres_schema().await;
        let smql =
            std::fs::read_to_string(paginate_config!("paginate_default.smql")).expect("read smql");
        run_smql(&smql, false).await.expect("apply failed");
        assert_row_count("actor", "sakila", "actor").await;
    }

    /// PK strategy on a SMALLINT UNSIGNED auto-increment column.
    ///
    /// WHERE actor_id > :last_id  ORDER BY actor_id ASC  LIMIT 50
    /// 200 rows, batch_size=50 -> 4 full pages, O(1) index seek per page.
    #[traced_test]
    #[tokio::test]
    async fn paginate_pk_small_table() {
        reset_postgres_schema().await;
        let smql =
            std::fs::read_to_string(paginate_config!("paginate_pk.smql")).expect("read smql");
        run_smql(&smql, false).await.expect("apply failed");
        assert_row_count("actor", "sakila", "actor").await;
    }

    /// PK strategy on a large table - tests multi-page traversal and partial last batch.
    ///
    /// 16,044 payment rows, batch_size=1000 -> 16 full pages + 1 partial (44 rows).
    /// Partial last batch detection: the final fetch returns < batch_size rows.
    #[traced_test]
    #[tokio::test]
    async fn paginate_pk_large_table() {
        reset_postgres_schema().await;
        let smql =
            std::fs::read_to_string(paginate_config!("paginate_pk_large.smql")).expect("read smql");
        run_smql(&smql, false).await.expect("apply failed");
        assert_row_count("payment", "sakila", "payment").await;
    }

    /// Numeric strategy on a column with only 2 distinct values (staff_id 1-2).
    ///
    /// WHERE (staff_id > :val)
    ///    OR (staff_id = :val AND payment_id > :last_id)
    /// ORDER BY staff_id ASC, payment_id ASC  LIMIT 1000
    ///
    /// 16,044 payment rows, batch_size=1000 -> 16 full pages + 1 partial (44 rows).
    /// Every page boundary lands inside a duplicate-value run, so the tie-breaker
    /// is exercised on every fetch after the first.
    #[traced_test]
    #[tokio::test]
    async fn paginate_numeric_with_tiebreaker() {
        reset_postgres_schema().await;
        let smql =
            std::fs::read_to_string(paginate_config!("paginate_numeric.smql")).expect("read smql");
        run_smql(&smql, false).await.expect("apply failed");
        assert_row_count("payment", "sakila", "payment").await;
    }

    /// Timestamp strategy on a DATETIME column (many rows per second).
    ///
    /// WHERE (payment_date > :ts) OR (payment_date = :ts AND payment_id > :last_id)
    /// ORDER BY payment_date ASC, payment_id ASC  LIMIT 500
    ///
    /// 16,044 payment rows, batch_size=500 -> 33 pages.
    /// Sakila payment_date values are densely clustered: hundreds of payments share
    /// the same second, so the tie-breaker branch fires on nearly every page.
    #[traced_test]
    #[tokio::test]
    async fn paginate_timestamp_datetime_column() {
        reset_postgres_schema().await;
        let smql = std::fs::read_to_string(paginate_config!("paginate_timestamp.smql"))
            .expect("read smql");
        run_smql(&smql, false).await.expect("apply failed");
        assert_row_count("payment", "sakila", "payment").await;
    }

    /// Timestamp strategy on a MySQL TIMESTAMP column with non-UTC timezone.
    ///
    /// actor.last_update (TIMESTAMP, UTC) with timezone = "US/Eastern":
    ///   utc_to_local_sql() converts the cursor UTC microseconds -> US/Eastern
    ///   local time string for the WHERE clause.
    ///
    /// All 200 actor rows share a single timestamp value, so pages 2-4 always hit the
    /// tie-breaker branch:  last_update = :ts AND actor_id > :last_id
    ///
    /// batch_size=50, 4 pages — every page after the first exercises timezone
    /// conversion in the cursor-to-SQL path.
    #[traced_test]
    #[tokio::test]
    async fn paginate_timestamp_with_timezone() {
        reset_postgres_schema().await;
        let smql = std::fs::read_to_string(paginate_config!("paginate_timestamp_tz.smql"))
            .expect("read smql");
        run_smql(&smql, false).await.expect("apply failed");
        assert_row_count("actor", "sakila", "actor").await;
    }

    /// Cross-check: after PK pagination of actor, confirm the destination has
    /// exactly the right number of rows, not zero or partial.
    ///
    /// This catches the "off-by-one at last-page boundary" class of bug where
    /// the producer exits one batch early because the final page returns exactly
    /// batch_size rows and the next fetch returns 0.
    ///
    /// actor has 200 rows; batch_size=200 means exactly 1 full batch.
    /// The producer must still fetch a second (empty) page to detect end-of-data.
    #[traced_test]
    #[tokio::test]
    async fn paginate_pk_exact_batch_boundary() {
        reset_postgres_schema().await;
        let smql = std::fs::read_to_string(paginate_config!("paginate_pk_exact_boundary.smql"))
            .expect("read smql");
        run_smql(&smql, false).await.expect("apply failed");
        let count = get_row_count("actor", "sakila", DbType::Postgres).await;
        let expected = get_row_count("actor", "sakila", DbType::MySql).await;
        assert_eq!(
            count, expected,
            "exact-batch boundary: expected {expected} rows, got {count}"
        );
    }

    /// Cross-check: PK pagination where batch_size does NOT evenly divide row count.
    ///
    /// 200 actor rows, batch_size=77 -> 2 full pages (154 rows) + 1 partial (46 rows).
    /// Verifies that the partial last page is not silently dropped.
    #[traced_test]
    #[tokio::test]
    async fn paginate_pk_partial_last_page() {
        reset_postgres_schema().await;
        let smql = std::fs::read_to_string(paginate_config!("paginate_pk_partial_last.smql"))
            .expect("read smql");
        run_smql(&smql, false).await.expect("apply failed");
        let count = get_row_count("actor", "sakila", DbType::Postgres).await;
        let expected = get_row_count("actor", "sakila", DbType::MySql).await;
        assert_eq!(
            count, expected,
            "partial last page: expected {expected} rows, got {count}"
        );
    }

    /// PK strategy on a joined + filtered dataset.
    ///
    /// Migrates rental rows for staff_id=1 only (~8,000 of 16,044), joined with
    /// customer to produce a computed customer_name column. Tests that the keyset
    /// cursor correctly pages through a filtered result set without skipping or
    /// duplicating rows.
    ///
    /// Expected row count is verified against a MySQL query that applies the same
    /// filter: SELECT COUNT(*) FROM rental WHERE staff_id = 1
    #[traced_test]
    #[tokio::test]
    async fn paginate_pk_join_and_filter() {
        reset_postgres_schema().await;
        let smql = std::fs::read_to_string(paginate_config!("paginate_pk_join_filter.smql"))
            .expect("read smql");
        run_smql(&smql, false).await.expect("apply failed");

        // Count the expected rows directly in MySQL with the same filter
        let mysql = mysql_pool("sakila").await;
        let mut conn = mysql.get_conn().await.unwrap();
        let expected: i64 = conn
            .query_first("SELECT COUNT(*) FROM rental WHERE staff_id = 1")
            .await
            .unwrap()
            .unwrap_or(0);

        let actual = get_row_count("rental_enriched", "sakila", DbType::Postgres).await;
        assert_eq!(
            expected, actual,
            "join+filter: expected {expected} rows (staff_id=1), got {actual}"
        );
    }
}
