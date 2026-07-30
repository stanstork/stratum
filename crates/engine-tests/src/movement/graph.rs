#[cfg(test)]
mod tests {
    use crate::direction_tests;
    use crate::harness::Direction;

    /// `with references { data = cascade }` discovers related tables via the FK
    /// graph, creates them in topological order, cascades their data and adds the
    /// foreign keys afterwards.
    ///
    /// Rooted at `city` with `depth = 1` to keep the closure small: `country` via
    /// the forward FK. `address` (a backward FK from `city`) is excluded because
    /// Sakila's `address.location` is a GEOMETRY that a MySQL destination cannot
    /// be written back to - see `unsupported_tables`.
    async fn graph_cascade_case(dir: Direction) {
        dir.run(
            r#"
            pipeline "graph_from_city" {
                from {
                    connection = connection.src
                    table      = "city"

                    with references {
                        data    = cascade
                        depth   = 1
                        exclude = ["address"]
                    }
                }
                to { connection = connection.dst }
                settings { create_missing_tables = true  batch_size = 500 }
            }
            "#,
        )
        .await;

        // The root and its FK neighbour are discovered and created.
        for table in ["city", "country"] {
            assert!(
                dir.dst_table_exists(table).await,
                "[{dir}] '{table}' should be discovered from 'city'"
            );
        }
        assert!(
            !dir.dst_table_exists("address").await,
            "[{dir}] 'address' was excluded and should not be created"
        );

        // Cascade migrates the referential closure of the root's rows. Every city
        // migrates, so every country they reference does too - which is all of them.
        dir.assert_row_parity("city", "city").await;
        dir.assert_row_parity("country", "country").await;

        // Foreign keys are recreated after the data load, so nothing is orphaned.
        let orphans = dir
            .dst_scalar_i64(&format!(
                "SELECT COUNT(*) FROM {city} c LEFT JOIN {country} p \
                 ON c.{cid} = p.{cid} WHERE p.{cid} IS NULL",
                city = dir.dst.quote("city"),
                country = dir.dst.quote("country"),
                cid = dir.dst.quote("country_id"),
            ))
            .await;
        assert_eq!(
            orphans, 0,
            "[{dir}] no city should reference a missing country"
        );
    }
    direction_tests!(graph_cascade, graph_cascade_case);

    /// Graph cascade honours `lanes`: discovered tables migrate concurrently
    /// (each table its own lane, pooled by `lanes`). Correctness must match the
    /// single-lane cascade - same rows, no orphans - under that parallelism.
    async fn graph_cascade_lanes_case(dir: Direction) {
        dir.run(
            r#"
            pipeline "graph_lanes_from_city" {
                from {
                    connection = connection.src
                    table      = "city"

                    with references {
                        data    = cascade
                        depth   = 1
                        exclude = ["address"]
                    }
                }
                to { connection = connection.dst }
                settings { create_missing_tables = true  batch_size = 500  lanes = 4 }
            }
            "#,
        )
        .await;

        for table in ["city", "country"] {
            assert!(
                dir.dst_table_exists(table).await,
                "[{dir}] '{table}' should be discovered from 'city'"
            );
        }
        dir.assert_row_parity("city", "city").await;
        dir.assert_row_parity("country", "country").await;

        let orphans = dir
            .dst_scalar_i64(&format!(
                "SELECT COUNT(*) FROM {city} c LEFT JOIN {country} p \
                 ON c.{cid} = p.{cid} WHERE p.{cid} IS NULL",
                city = dir.dst.quote("city"),
                country = dir.dst.quote("country"),
                cid = dir.dst.quote("country_id"),
            ))
            .await;
        assert_eq!(
            orphans, 0,
            "[{dir}] no city should reference a missing country under graph lanes"
        );
    }
    direction_tests!(graph_cascade_lanes, graph_cascade_lanes_case);
}
