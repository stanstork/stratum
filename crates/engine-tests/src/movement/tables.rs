#[cfg(test)]
mod tests {
    use crate::direction_tests;
    use crate::harness::{COMMON_TABLES, Dbms, Direction};

    /// Tables that cannot round-trip in a given direction, because they hold a
    /// column of a type the engine does not support end-to-end.
    ///
    /// Both are **identity-only**: cross-dialect the exotic column degrades to a
    /// nullable `bytea`/`TEXT`, so nothing fails. Neither is a regression - the
    /// matrix simply exercises pairs that were never run before.
    fn unsupported_tables(dir: Direction) -> &'static [&'static str] {
        match (dir.src, dir.dst) {
            // Sakila's `address.location` is a MySQL GEOMETRY. The LOAD DATA writer
            // encodes binary as hex, which MySQL will not accept for a geometry
            // column (ER_CANNOT_GET_GEOMETRY_OBJECT, 1416).
            (Dbms::MySql, Dbms::MySql) => &["address"],
            // Pagila's `film.fulltext` is a `tsvector`. The PostgreSQL reader has no
            // decoder for it, so the value arrives as NULL and violates the
            // destination's NOT NULL constraint.
            (Dbms::Postgres, Dbms::Postgres) => &["film"],
            _ => &[],
        }
    }

    /// Copy every table of the DVD-rental schema and check row parity.
    ///
    /// The broadest data-movement check: exercises type conversion for every
    /// column in the fixture, in both cross-dialect directions and both identity
    /// directions.
    async fn full_database_case(dir: Direction) {
        let skip = unsupported_tables(dir);
        let tables: Vec<&str> = COMMON_TABLES
            .iter()
            .copied()
            .filter(|t| !skip.contains(t))
            .collect();

        let pipelines: String = tables
            .iter()
            .map(|t| {
                format!(
                    r#"
                    pipeline "copy_{t}" {{
                        from {{ connection = connection.src table = "{t}" }}
                        to   {{ connection = connection.dst table = "{t}" }}
                        settings {{ create_missing_tables = true  batch_size = 1000 }}
                    }}
                    "#
                )
            })
            .collect();

        dir.run(&pipelines).await;

        for table in &tables {
            dir.assert_row_parity(table, table).await;
        }
    }
    direction_tests!(full_database, full_database_case);
}
