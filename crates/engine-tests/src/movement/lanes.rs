#[cfg(test)]
mod tests {
    use crate::direction_tests;
    use crate::harness::Direction;

    /// `lanes = 4` range-splits a single integer-PK table into 4 parallel
    /// key-range workers. Every row must land exactly once - nothing dropped or
    /// duplicated across the lane boundaries.
    async fn lanes_single_table_case(dir: Direction) {
        dir.run(
            r#"
            pipeline "lanes_payment" {
                from { connection = connection.src  table = "payment" }
                to   { connection = connection.dst  table = "payment" }
                settings {
                    create_missing_tables = true
                    batch_size            = 500
                    lanes                 = 4
                }
            }
            "#,
        )
        .await;

        dir.assert_row_parity("payment", "payment").await;

        // distinct PKs == row count proves the range split neither dropped nor
        // duplicated any row at a boundary.
        let count = dir.dst_count("payment").await;
        let distinct = dir
            .dst_scalar_i64(&format!(
                "SELECT COUNT(DISTINCT {}) FROM {}",
                dir.dst.quote("payment_id"),
                dir.dst.quote("payment"),
            ))
            .await;
        assert_eq!(
            count, distinct,
            "[{dir}] 4 lanes must not drop or duplicate rows (count={count}, distinct={distinct})"
        );
    }
    direction_tests!(lanes_single_table, lanes_single_table_case);
}
