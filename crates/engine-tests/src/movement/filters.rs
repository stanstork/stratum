#[cfg(test)]
mod tests {
    use crate::direction_tests;
    use crate::harness::Direction;

    /// `where { ... }` pushed down to the source.
    ///
    /// `film.rental_rate` is `numeric`/`decimal`, which makes this a regression
    /// test for binding a filter literal against a non-text column.
    async fn where_filter_case(dir: Direction) {
        dir.run(
            r#"
            pipeline "affordable_films" {
                from { connection = connection.src table = "film" }
                to   { connection = connection.dst table = "affordable_films" }

                where "affordable" {
                    film.rental_rate <= 2.99
                }

                select {
                    film_id     = film.film_id
                    title       = film.title
                    rental_rate = film.rental_rate
                    rating      = film.rating
                }

                settings {
                    create_missing_tables = true
                    batch_size            = 200
                }
            }
            "#,
        )
        .await;

        let expected = dir
            .src_scalar_i64(&format!(
                "SELECT COUNT(*) FROM {} WHERE rental_rate <= 2.99",
                dir.src.quote("film")
            ))
            .await;
        assert!(expected > 0, "[{dir}] fixture sanity: some films are cheap");

        assert_eq!(
            dir.dst_count("affordable_films").await,
            expected,
            "[{dir}] only films matching the filter should migrate"
        );

        // The filter must be applied, not merely produce a matching count.
        let above = dir
            .dst_scalar_i64(&format!(
                "SELECT COUNT(*) FROM {} WHERE rental_rate > 2.99",
                dir.dst.quote("affordable_films")
            ))
            .await;
        assert_eq!(above, 0, "[{dir}] no film above the threshold should exist");
    }
    direction_tests!(where_filter, where_filter_case);
}
