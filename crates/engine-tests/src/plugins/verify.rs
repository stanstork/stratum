//! Merkle integrity verification of plugin migrations.

#[cfg(test)]
mod tests {
    use crate::{
        plugins::fixture,
        reset_postgres_schema,
        utils::{execute, run_smql, run_verify_smql},
    };
    use tracing_test::traced_test;

    /// MySQL `film` -> Postgres with a transform plugin output, integrity on.
    fn transform_smql(dest: &str) -> String {
        format!(
            r#"
            plugin "adder" {{ path = "{plugin}" }}

            connection "src" {{ driver = "mysql"    url = "mysql://sakila_user:qwerty123@localhost:3306/sakila" }}
            connection "dst" {{ driver = "postgres" url = "postgres://user:password@localhost:5432/testdb" }}

            pipeline "verify_transform" {{
                from {{ connection = connection.src table = "film" }}
                to   {{ connection = connection.dst table = "{dest}" }}

                select {{
                    film_id = film.film_id
                    total   = plugin.adder({{ a: film.rental_rate, b: film.replacement_cost }})
                }}

                settings {{
                    create_missing_tables = true
                    copy_columns          = "MAP_ONLY"
                    batch_size            = 128
                }}
            }}
            "#,
            plugin = fixture("test_transform.wasm"),
            dest = dest,
        )
    }

    /// WASM source -> Postgres, integrity on.
    fn source_smql(dest: &str) -> String {
        format!(
            r#"
            plugin "feed" {{ path = "{plugin}" config {{ total = "50" page_size = "7" }} }}

            connection "src" {{ driver = "wasm"     plugin = "feed" }}
            connection "dst" {{ driver = "postgres" url = "postgres://user:password@localhost:5432/testdb" }}

            pipeline "verify_source" {{
                from {{ connection = connection.src table = "counter" }}
                to   {{ connection = connection.dst table = "{dest}" }}

                select {{
                    id    = counter.id
                    label = counter.label
                }}

                settings {{
                    create_missing_tables = true
                    batch_size            = 7
                }}
            }}
            "#,
            plugin = fixture("test_source.wasm"),
            dest = dest,
        )
    }

    /// A transform-plugin migration verifies cleanly: the receipt (hashed after
    /// the plugin runs) matches the destination re-read.
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn verify_matches_with_transform_plugin() {
        reset_postgres_schema().await;
        let smql = transform_smql("films_verified");
        run_smql(&smql, true).await.expect("apply failed");
        run_verify_smql(&smql).await.expect("verify failed");
    }

    /// A WASM-source migration verifies cleanly (verify reads only the dest, so a
    /// plugin source is transparent to it).
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn verify_matches_with_wasm_source() {
        reset_postgres_schema().await;
        let smql = source_smql("synth_verified");
        run_smql(&smql, true).await.expect("apply failed");
        run_verify_smql(&smql).await.expect("verify failed");
    }

    /// Tampering with the destination after a plugin migration is detected: the
    /// rebuilt Merkle root no longer matches the receipt.
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn verify_detects_tampered_transform_output() {
        reset_postgres_schema().await;
        let smql = transform_smql("films_verified");
        run_smql(&smql, true).await.expect("apply failed");

        // Corrupt the plugin-computed column in one row.
        execute("UPDATE films_verified SET total = 0 WHERE film_id = 1").await;

        let result = run_verify_smql(&smql).await;
        assert!(result.is_err(), "verify should detect the tampered row");
    }
}
