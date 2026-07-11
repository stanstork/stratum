#[cfg(test)]
mod tests {
    use crate::direction_tests;
    use crate::harness::Direction;

    /// Row-level `validate { assert ... action = skip }` drops non-conforming rows.
    async fn validation_skip_case(dir: Direction) {
        dir.run(
            r#"
            pipeline "validated_films" {
                from { connection = connection.src table = "film" }
                to   { connection = connection.dst table = "validated_films" }

                select {
                    film_id     = film.film_id
                    title       = film.title
                    rental_rate = film.rental_rate
                }

                validate {
                    assert "affordable" {
                        check   = film.rental_rate <= 2.99
                        message = "Film rental rate is too high"
                        action  = skip
                    }
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

        let expected = dir
            .src_scalar_i64(&format!(
                "SELECT COUNT(*) FROM {} WHERE rental_rate <= 2.99",
                dir.src.quote("film")
            ))
            .await;
        assert_eq!(
            dir.dst_count("validated_films").await,
            expected,
            "[{dir}] rows failing the assertion should be skipped"
        );
    }
    direction_tests!(validation_skip, validation_skip_case);
}
