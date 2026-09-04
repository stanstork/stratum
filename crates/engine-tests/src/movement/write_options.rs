#[cfg(test)]
mod tests {
    use crate::harness::{Direction, runner};
    use tracing_test::traced_test;

    // ----- on_conflict -------------------------------------------------------

    /// Load `actor`, tamper one destination row, then reload with `on_conflict`.
    /// `expect_overwritten` says whether the tampered row should be restored to
    /// the source value (upsert/replace) or kept (do_nothing/ignore).
    async fn on_conflict_case(dir: Direction, action: &str, expect_overwritten: bool) {
        // First load creates + copies `actor` in full (with its primary key).
        dir.run(
            r#"
            pipeline "actor_load" {
                from { connection = connection.src  table = "actor" }
                to   { connection = connection.dst  table = "actor" }
                settings { create_missing_tables = true  batch_size = 200 }
            }
            "#,
        )
        .await;

        // Tamper the row that will collide on reload.
        dir.dst_execute(&format!(
            "UPDATE {} SET {} = 'TAMPERED' WHERE {} = 1",
            dir.dst.quote("actor"),
            dir.dst.quote("first_name"),
            dir.dst.quote("actor_id"),
        ))
        .await;

        // Reload every row into the existing table; the colliding PK is resolved
        // per `action`. Runs WITHOUT a reset so the tampered row is present.
        let reload = format!(
            r#"
            pipeline "actor_reload" {{
                from {{ connection = connection.src  table = "actor" }}
                to   {{
                    connection = connection.dst  table = "actor"
                    {dialect} {{ on_conflict = "{action}" }}
                }}
                settings {{ create_missing_tables = true  batch_size = 200 }}
            }}
            "#,
            dialect = dir.dst.driver(),
            action = action,
        );
        runner::run_ppl(&dir.ppl(&reload), false)
            .await
            .unwrap_or_else(|e| panic!("[{dir}] {action} reload failed: {e:?}"));

        // The reload must not duplicate rows regardless of the action.
        dir.assert_row_parity("actor", "actor").await;

        let src_name = dir
            .src_scalar_string(&format!(
                "SELECT {} FROM {} WHERE {} = 1",
                dir.src.quote("first_name"),
                dir.src.quote("actor"),
                dir.src.quote("actor_id"),
            ))
            .await;
        let dst_name = dir
            .dst_scalar_string(&format!(
                "SELECT {} FROM {} WHERE {} = 1",
                dir.dst.quote("first_name"),
                dir.dst.quote("actor"),
                dir.dst.quote("actor_id"),
            ))
            .await;

        if expect_overwritten {
            assert_eq!(
                dst_name, src_name,
                "[{dir}] on_conflict={action} should overwrite the tampered row"
            );
        } else {
            assert_eq!(
                dst_name.as_deref(),
                Some("TAMPERED"),
                "[{dir}] on_conflict={action} should keep the tampered row"
            );
        }
    }

    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn on_conflict_pg_do_update_overwrites() {
        on_conflict_case(Direction::MYSQL_TO_POSTGRES, "do_update", true).await;
    }

    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn on_conflict_pg_do_nothing_keeps() {
        on_conflict_case(Direction::MYSQL_TO_POSTGRES, "do_nothing", false).await;
    }

    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn on_conflict_mysql_replace_overwrites() {
        on_conflict_case(Direction::POSTGRES_TO_MYSQL, "replace", true).await;
    }

    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn on_conflict_mysql_ignore_keeps() {
        on_conflict_case(Direction::POSTGRES_TO_MYSQL, "ignore", false).await;
    }

    // ----- pk_creation = "post" (Postgres) -----------------------------------

    /// `pk_creation = "post"` creates the table without its PK, loads, then adds
    /// the PK. The destination must end with both the primary key and all rows.
    async fn pk_creation_post_case(dir: Direction) {
        dir.run(
            r#"
            pipeline "pk_post" {
                from { connection = connection.src  table = "actor" }
                to   {
                    connection = connection.dst  table = "actor"
                    postgres { pk_creation = "post" }
                }
                settings { create_missing_tables = true  batch_size = 200 }
            }
            "#,
        )
        .await;

        dir.assert_row_parity("actor", "actor").await;

        let pks = dir
            .dst_scalar_i64(
                "SELECT COUNT(*) FROM information_schema.table_constraints \
                 WHERE table_schema = 'public' AND table_name = 'actor' \
                 AND constraint_type = 'PRIMARY KEY'",
            )
            .await;
        assert_eq!(
            pks, 1,
            "[{dir}] actor must have its primary key after pk_creation=post"
        );
    }

    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn pk_creation_post_mysql_to_pg() {
        pk_creation_post_case(Direction::MYSQL_TO_POSTGRES).await;
    }

    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn pk_creation_post_pg_to_pg() {
        pk_creation_post_case(Direction::POSTGRES_TO_POSTGRES).await;
    }

    // ----- copy_format = "text" (Postgres) -----------------------------------

    /// The text `COPY` path (default is binary). Force it and assert the data
    /// still round-trips.
    async fn copy_format_text_case(dir: Direction) {
        dir.run(
            r#"
            pipeline "text_copy" {
                from { connection = connection.src  table = "city" }
                to   {
                    connection = connection.dst  table = "city"
                    postgres { copy_format = "text" }
                }
                settings { create_missing_tables = true  batch_size = 500 }
            }
            "#,
        )
        .await;

        dir.assert_row_parity("city", "city").await;
    }

    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn copy_format_text_mysql_to_pg() {
        copy_format_text_case(Direction::MYSQL_TO_POSTGRES).await;
    }

    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn copy_format_text_pg_to_pg() {
        copy_format_text_case(Direction::POSTGRES_TO_POSTGRES).await;
    }
}
