#[cfg(test)]
mod tests {
    use crate::harness::ppl::feature_ppl;
    use crate::{
        features::plugins::fixture,
        harness::runner::{
            DbType, get_cell_as_string, get_cell_as_usize, get_row_count, run_ppl_with_env,
        },
        reset_postgres_schema,
    };
    use tracing_test::traced_test;

    /// `test_caps` reads `CAPS_ENV` (env), reads the file at `CAPS_FILE` (fs), and
    /// keeps a per-instance kv counter, emitting `kv=<n>;env=<..>;fs=<..>` per row.
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn plugin_capabilities_env_fs_kv_end_to_end() {
        reset_postgres_schema().await;

        // A file the plugin reads via the fs capability.
        let dir = std::env::temp_dir().join("paganel-caps-e2e");
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("probe.txt");
        std::fs::write(&file, "filecontents\n").unwrap();
        let file_path = file.to_string_lossy().into_owned();
        let dir_path = dir.to_string_lossy().into_owned();

        let doc = feature_ppl(&format!(
            r#"
            plugin "caps" {{
                path          = "{plugin}"
                allow_kv      = true
                allow_metrics = true
                allow_env     = ["CAPS_ENV", "CAPS_FILE"]
                allow_fs_read = ["{dir}"]
            }}

            pipeline "migrate_caps" {{
                from {{ connection = connection.src table = "actor" }}
                to   {{ connection = connection.dst table = "actor_caps" }}
                select {{
                    actor_id = actor.actor_id
                    probe    = plugin.caps({{ seed: actor.first_name }})
                }}
                settings {{ create_missing_tables = true }}
            }}
            "#,
            plugin = fixture("test_caps.wasm"),
            dir = dir_path,
        ));

        run_ppl_with_env(&doc, &[("CAPS_ENV", "envvalue"), ("CAPS_FILE", &file_path)])
            .await
            .expect("migration succeeds");

        // Every source row migrated.
        let src = get_row_count("actor", "sakila", DbType::MySql).await;
        let dst = get_row_count("actor_caps", "sakila", DbType::Postgres).await;
        assert_eq!(src, dst, "all actor rows should be migrated");

        // env + fs resolved through the full engine (constant across every row).
        let probe = get_cell_as_string(
            "SELECT probe FROM actor_caps ORDER BY actor_id LIMIT 1",
            "sakila",
            DbType::Postgres,
            "probe",
        )
        .await;
        assert!(
            probe.contains("env=envvalue"),
            "env capability: got {probe}"
        );
        assert!(
            probe.contains("fs=filecontents"),
            "fs capability: got {probe}"
        );
        assert!(probe.starts_with("kv="), "kv capability: got {probe}");

        // kv persisted across rows/batches of the pipeline instance: env and fs
        // are constant, so a distinct `probe` per row means the kv counter was
        // unique (1..N) for every row.
        let distinct = get_cell_as_usize(
            "SELECT COUNT(DISTINCT probe) AS c FROM actor_caps",
            "sakila",
            DbType::Postgres,
            "c",
        )
        .await;
        assert_eq!(
            distinct as i64, dst,
            "kv counter should be unique per row (accumulated across the instance)"
        );

        // (metrics + http are granted-and-exercised too; their emission/response
        // is asserted directly in engine-wasm's integration tests, where the
        // plugin runs on the test thread rather than a spawned producer task.)

        std::fs::remove_dir_all(&dir).ok();
    }
}
