#[cfg(test)]
mod tests {
    use crate::direction_tests;
    use crate::harness::Direction;

    /// `with { ... }` joins plus computed fields.
    async fn multi_join_case(dir: Direction) {
        dir.run(
            r#"
            pipeline "customer_summary" {
                from { connection = connection.src table = "customer" }
                to   { connection = connection.dst table = "customer_summary" }

                with {
                    address from address where address.address_id == customer.address_id
                    city    from city    where city.city_id == address.city_id
                }

                select {
                    customer_id  = customer.customer_id
                    full_name    = concat(customer.first_name, " ", customer.last_name)
                    email        = customer.email
                    full_address = concat(address.address, ", ", city.city)
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

        dir.assert_row_parity("customer", "customer_summary").await;

        // MAP_ONLY copies exactly the mapped columns.
        assert_eq!(
            dir.dst_column_count("customer_summary").await,
            4,
            "[{dir}] MAP_ONLY should copy only the mapped columns"
        );

        // Computed fields are populated from the joined tables, not left NULL.
        for (column, pattern) in [("full_name", "% %"), ("full_address", "%, %")] {
            let bad = dir
                .dst_scalar_i64(&format!(
                    "SELECT COUNT(*) FROM {} WHERE {c} IS NULL OR {c} NOT LIKE '{pattern}'",
                    dir.dst.quote("customer_summary"),
                    c = dir.dst.quote(column),
                ))
                .await;
            assert_eq!(
                bad, 0,
                "[{dir}] '{column}' should be computed for every row"
            );
        }
    }
    direction_tests!(multi_join, multi_join_case);

    /// A join that fans out: one film has many actors, so the destination holds a
    /// row per (film, actor) pair rather than per film.
    async fn fan_out_join_case(dir: Direction) {
        dir.run(
            r#"
            pipeline "film_actor_details" {
                from { connection = connection.src table = "film_actor" }
                to   { connection = connection.dst table = "film_actor_details" }

                with {
                    actor from actor where actor.actor_id == film_actor.actor_id
                    film  from film  where film.film_id == film_actor.film_id
                }

                select {
                    actor_id        = film_actor.actor_id
                    film_id         = film_actor.film_id
                    actor_full_name = concat(actor.first_name, " ", actor.last_name)
                    film_title      = film.title
                }

                settings {
                    create_missing_tables = true
                    batch_size            = 500
                    copy_columns          = "MAP_ONLY"
                }
            }
            "#,
        )
        .await;

        // One destination row per film_actor pair - the join must not collapse or
        // multiply them.
        dir.assert_row_parity("film_actor", "film_actor_details")
            .await;
    }
    direction_tests!(fan_out_join, fan_out_join_case);

    /// A join with no `select` block: the destination gets the source table's own
    /// columns, and none from the joined tables.
    async fn join_without_select_case(dir: Direction) {
        dir.run(
            r#"
            pipeline "rentals" {
                from { connection = connection.src table = "rental" }
                to   { connection = connection.dst table = "rentals_copy" }

                with {
                    customer from customer where customer.customer_id == rental.customer_id
                }

                settings {
                    create_missing_tables = true
                    batch_size            = 1000
                }
            }
            "#,
        )
        .await;

        dir.assert_row_parity("rental", "rentals_copy").await;

        // Without a `select`, the destination gets exactly the source table's own
        // columns: nothing from the joined table, under any name. Comparing the
        // whole set also catches a joined column arriving prefixed (`customer_email`).
        let expected = dir.src_columns("rental").await;
        let actual = dir.dst_columns("rentals_copy").await;
        assert_eq!(
            expected, actual,
            "[{dir}] destination columns should match the source table's exactly"
        );
    }
    direction_tests!(join_without_select, join_without_select_case);

    /// A join combined with a nested boolean filter (`AND` of an `OR`), where the
    /// predicate spans both the source and a joined table.
    async fn join_with_nested_filter_case(dir: Direction) {
        dir.run(
            r#"
            pipeline "selected_inventory" {
                from { connection = connection.src table = "inventory" }
                to   { connection = connection.dst table = "selected_inventory" }

                with {
                    film from film where film.film_id == inventory.film_id
                }

                where "cheap_or_short" {
                    inventory.store_id > 0
                    film.rental_rate <= 2.99 || film.rental_duration < 4
                }

                select {
                    inventory_id = inventory.inventory_id
                    film_id      = inventory.film_id
                    rental_rate  = film.rental_rate
                }

                settings {
                    create_missing_tables = true
                    batch_size            = 500
                    copy_columns          = "MAP_ONLY"
                }
            }
            "#,
        )
        .await;

        let q = |t: &str| dir.src.quote(t);
        let expected = dir
            .src_scalar_i64(&format!(
                "SELECT COUNT(*) FROM {inv} i JOIN {film} f ON f.film_id = i.film_id \
                 WHERE i.store_id > 0 AND (f.rental_rate <= 2.99 OR f.rental_duration < 4)",
                inv = q("inventory"),
                film = q("film"),
            ))
            .await;
        assert!(expected > 0, "[{dir}] fixture sanity: some rows match");

        assert_eq!(
            dir.dst_count("selected_inventory").await,
            expected,
            "[{dir}] nested filter should be applied across the join"
        );
    }
    direction_tests!(join_with_nested_filter, join_with_nested_filter_case);
}
