#[cfg(test)]
mod tests {
    use crate::direction_tests;
    use crate::harness::Direction;

    /// `copy_columns = "MAP_ONLY"` copies exactly the mapped columns, renaming as
    /// specified and dropping everything else.
    async fn map_only_case(dir: Direction) {
        dir.run(
            r#"
            pipeline "customers_flat" {
                from { connection = connection.src table = "customer" }
                to   { connection = connection.dst table = "customers_flat" }

                select {
                    id         = customer.customer_id
                    user_email = customer.email
                    full_name  = concat(customer.first_name, " ", customer.last_name)
                }

                settings {
                    create_missing_tables = true
                    batch_size            = 200
                    copy_columns          = "MAP_ONLY"
                }
            }
            "#,
        )
        .await;

        dir.assert_row_parity("customer", "customers_flat").await;

        // Exactly the mapped columns, under their new names, and nothing else.
        let mut expected = vec![
            "full_name".to_string(),
            "id".to_string(),
            "user_email".to_string(),
        ];
        expected.sort();
        assert_eq!(
            dir.dst_columns("customers_flat").await,
            expected,
            "[{dir}] MAP_ONLY should copy exactly the mapped columns"
        );
    }
    direction_tests!(map_only, map_only_case);
}
