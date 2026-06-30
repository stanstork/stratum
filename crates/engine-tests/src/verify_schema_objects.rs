#[cfg(test)]
mod tests {
    use crate::{
        reset_postgres_schema,
        utils::{run_smql, run_verify_smql},
    };
    use tracing_test::traced_test;

    macro_rules! phase2_config {
        ($name:expr) => {
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/configs/schema-objects/",
                $name
            )
        };
    }

    /// Verify works on a cascade migration: payment + FK depth-1 tables
    /// (customer, staff, rental).
    ///
    /// Cascade receipts use sorted_hashes=true because FK-scoped fetches
    /// deliver related-table rows in non-PK order with duplicates across batches.
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn verify_phase2_cascade_payment() {
        reset_postgres_schema().await;
        let smql =
            std::fs::read_to_string(phase2_config!("p2-02-cascade-data.smql")).expect("read smql");
        run_smql(&smql, true).await.expect("apply failed");
        run_verify_smql(&smql).await.expect("verify failed");
    }

    /// Verify works on a full-graph cascade from rental through all Sakila tables.
    ///
    /// All 15 Sakila tables are migrated; verify confirms the Merkle roots
    /// match for every table in the receipt.
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn verify_phase2_full_chain() {
        reset_postgres_schema().await;
        let smql =
            std::fs::read_to_string(phase2_config!("p2-03-full-chain.smql")).expect("read smql");
        run_smql(&smql, true).await.expect("apply failed");
        run_verify_smql(&smql).await.expect("verify failed");
    }

    /// Verify works after a circular-FK migration (store ↔ staff).
    ///
    /// Two-phase strategy (CREATE TABLE without FK -> data -> ALTER TABLE ADD FK)
    /// must not affect the integrity receipt or verification.
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn verify_phase2_circular_fk() {
        reset_postgres_schema().await;
        let smql =
            std::fs::read_to_string(phase2_config!("p2-06-circular-fk.smql")).expect("read smql");
        run_smql(&smql, true).await.expect("apply failed");
        run_verify_smql(&smql).await.expect("verify failed");
    }

    /// Verify works after an ENUM migration.
    ///
    /// MySQL ENUM('G','PG','PG-13','R','NC-17') -> PostgreSQL VARCHAR(255).
    /// MySQL text protocol sends ENUM values as strings; PG stores them as
    /// varchar. Canonical hashing treats both as TAG_STRING - hashes must match.
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn verify_phase2_enum_migration() {
        reset_postgres_schema().await;
        let smql = std::fs::read_to_string(phase2_config!("p2-08-enum-migration.smql"))
            .expect("read smql");
        run_smql(&smql, true).await.expect("apply failed");
        run_verify_smql(&smql).await.expect("verify failed");
    }

    /// Verify works after a migration that includes MySQL STORED generated columns.
    ///
    /// Generated columns are excluded from INSERT/COPY (values are recomputed by
    /// the DB engine). The receipt is built from the written columns only;
    /// verify reads the same columns from the destination and confirms they match.
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn verify_phase2_generated_columns() {
        use mysql_async::prelude::Queryable;

        // --- Setup: add generated columns to MySQL film table ---
        let mysql = crate::mysql_pool("sakila").await;
        {
            let mut conn = mysql.get_conn().await.unwrap();
            for col in &["title_length", "rental_revenue"] {
                let exists: Option<u64> = conn
                    .exec_first(
                        "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS \
                         WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'film' \
                         AND COLUMN_NAME = ?",
                        (col,),
                    )
                    .await
                    .unwrap();
                if exists.unwrap_or(0) > 0 {
                    conn.query_drop(format!("ALTER TABLE film DROP COLUMN {col}"))
                        .await
                        .unwrap_or_else(|e| panic!("failed to drop existing {col}: {e}"));
                }
            }
            conn.query_drop(
                "ALTER TABLE film \
                 ADD COLUMN title_length INT \
                 GENERATED ALWAYS AS (CHAR_LENGTH(title)) STORED",
            )
            .await
            .expect("add title_length");
            conn.query_drop(
                "ALTER TABLE film \
                 ADD COLUMN rental_revenue DECIMAL(10,2) \
                 GENERATED ALWAYS AS (rental_rate * rental_duration) STORED",
            )
            .await
            .expect("add rental_revenue");
        }

        reset_postgres_schema().await;

        let result = async {
            let smql = std::fs::read_to_string(phase2_config!("p2-09-generated-columns.smql"))
                .expect("read smql");
            run_smql(&smql, true).await.expect("apply failed");
            run_verify_smql(&smql).await.expect("verify failed");
        }
        .await;

        // --- Teardown: remove generated columns from MySQL ---
        {
            let mysql_teardown = crate::mysql_pool("sakila").await;
            let mut conn = mysql_teardown.get_conn().await.unwrap();
            let _ = conn
                .query_drop("ALTER TABLE film DROP COLUMN IF EXISTS title_length")
                .await;
            let _ = conn
                .query_drop("ALTER TABLE film DROP COLUMN IF EXISTS rental_revenue")
                .await;
        }

        result
    }

    /// Verify works after a migration that renames tables on the destination.
    ///
    /// Source `film` -> dest `dim_film`, source `language` -> dest `dim_language`.
    /// The receipt stores destination table names. Verify queries dim_film and
    /// dim_language (not film/language) and confirms the hashes match.
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn verify_phase2_table_rename() {
        reset_postgres_schema().await;
        let smql =
            std::fs::read_to_string(phase2_config!("p2-10-table-rename.smql")).expect("read smql");
        run_smql(&smql, true).await.expect("apply failed");
        run_verify_smql(&smql).await.expect("verify failed");
    }

    /// Verify works on the full Sakila warehouse migration.
    ///
    /// All 15 tables renamed (fact_payment, dim_film, bridge_film_actor, …),
    /// a computed column (amount_cents) added to fact_payment, and two field
    /// renames (pmt_date, updated_at). The receipt covers every destination
    /// table; verify confirms each Merkle root matches.
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn verify_phase2_full_sakila() {
        reset_postgres_schema().await;
        let smql =
            std::fs::read_to_string(phase2_config!("p2-11-full-sakila.smql")).expect("read smql");
        run_smql(&smql, true).await.expect("apply failed");
        run_verify_smql(&smql).await.expect("verify failed");
    }
}
