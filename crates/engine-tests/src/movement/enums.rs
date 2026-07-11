#[cfg(test)]
mod tests {
    use crate::direction_tests;
    use crate::harness::{Dbms, Direction};

    /// `film.rating` is an enum in both fixtures (MySQL inline `ENUM`, PostgreSQL
    /// `mpaa_rating`). The values must survive the round trip, and a MySQL
    /// destination must receive an inline `ENUM` rather than TEXT.
    async fn enum_column_case(dir: Direction) {
        dir.run(
            r#"
            pipeline "film_ratings" {
                from { connection = connection.src table = "film" }
                to   { connection = connection.dst table = "film_ratings" }
                select {
                    film_id = film.film_id
                    title   = film.title
                    rating  = film.rating
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

        dir.assert_row_parity("film", "film_ratings").await;

        // Values survive: same number of 'G'-rated films, and none turned NULL.
        let expected_g = dir
            .src_scalar_i64(&format!(
                "SELECT COUNT(*) FROM {} WHERE rating = 'G'",
                dir.src.quote("film")
            ))
            .await;
        assert!(expected_g > 0, "[{dir}] fixture sanity: some films are 'G'");
        assert_eq!(
            dir.dst_scalar_i64(&format!(
                "SELECT COUNT(*) FROM {} WHERE rating = 'G'",
                dir.dst.quote("film_ratings")
            ))
            .await,
            expected_g,
            "[{dir}] enum values must migrate, not arrive as NULL"
        );

        // The destination column type differs per pair.
        let column_type = dir.dst_column_type("film_ratings", "rating").await;
        match (dir.src, dir.dst) {
            // MySQL spells the variants inline in the column.
            (_, Dbms::MySql) => assert!(
                column_type.starts_with("enum("),
                "[{dir}] expected an inline MySQL ENUM, got '{column_type}'"
            ),
            // A PostgreSQL source names its own type, which is recreated verbatim.
            (Dbms::Postgres, Dbms::Postgres) => assert_eq!(
                column_type, "mpaa_rating",
                "[{dir}] the source's enum type should be recreated"
            ),
            // A MySQL source has no standalone type, so the created PostgreSQL
            // enum is named after the column.
            (Dbms::MySql, Dbms::Postgres) => assert_eq!(
                column_type, "rating",
                "[{dir}] MySQL ENUM should become a native PostgreSQL enum, got '{column_type}'"
            ),
        }
    }
    direction_tests!(enum_column, enum_column_case);
}
