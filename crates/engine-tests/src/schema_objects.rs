#[cfg(test)]
mod tests {
    use crate::{
        reset_postgres_schema,
        utils::{DbType, assert_row_count, assert_table_exists, get_row_count, run_smql_file},
    };
    use mysql_async::prelude::Queryable;
    use tracing_test::traced_test;

    /// All tables expected after a full-graph schema-only migration from `film`.
    const SAKILA_TABLES: &[&str] = &[
        "actor",
        "address",
        "category",
        "city",
        "country",
        "customer",
        "film",
        "film_actor",
        "film_category",
        "inventory",
        "language",
        "payment",
        "rental",
        "staff",
        "store",
    ];

    // Phase 2 Test 01 - Schema-Only: with references { data = schema_only, depth = all }
    //
    // Config: crates/engine-tests/configs/schema-objects/p2-01-schema-only.smql
    //
    // Scenario:
    //   - Source: sakila.film (MySQL)
    //   - No `to.table`; schema_only mode
    //   - Graph expander discovers all FK-reachable tables from `film`
    //
    // Expected:
    //   - All 15 Sakila tables are created in Postgres (DDL only)
    //   - Every table is empty (no rows migrated)
    #[traced_test]
    #[tokio::test]
    async fn schema_only_creates_all_sakila_tables_empty() {
        reset_postgres_schema().await;

        // CARGO_MANIFEST_DIR = crates/engine-tests; configs live in configs/ inside this crate
        let config = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/configs/schema-objects/p2-01-schema-only.smql"
        );
        run_smql_file(config)
            .await
            .expect("schema-only migration failed");

        // All tables must exist
        for table in SAKILA_TABLES {
            assert_table_exists(table, true).await;
        }

        // No data should have been migrated
        for table in SAKILA_TABLES {
            let count = get_row_count(table, "sakila", DbType::Postgres).await;
            assert_eq!(
                count, 0,
                "table '{table}' should be empty in schema-only mode"
            );
        }
    }

    // Phase 2 Test 02 - Cascade Data: with references { data = cascade, depth = 1 }
    //
    // Config: crates/engine-tests/configs/schema-objects/p2-02-cascade-data.smql
    //
    // Scenario:
    //   - Source: sakila.payment (MySQL), depth = 1
    //   - Graph expander discovers: customer, staff, rental (FK depth-1 from payment)
    //   - Schema AND data migrated for all discovered tables
    //
    // Expected:
    //   - Tables payment, customer, staff, rental are created with data
    //   - Row counts match source
    //   - No orphaned payments (FK integrity)
    #[traced_test]
    #[tokio::test]
    async fn cascade_migrates_schema_and_data_with_fk_integrity() {
        reset_postgres_schema().await;

        let config = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/configs/schema-objects/p2-02-cascade-data.smql"
        );
        run_smql_file(config)
            .await
            .expect("cascade-data migration failed");

        // All four tables must exist and have data
        for table in &["payment", "customer", "staff", "rental"] {
            assert_table_exists(table, true).await;
        }

        // Row counts must match source
        for table in &["payment", "customer", "staff", "rental"] {
            assert_row_count(table, "sakila", table).await;
        }

        // FK integrity: no orphaned payments (payment.customer_id must exist in customer)
        let pg = crate::pg_pool().await;
        let orphans: i64 = pg
            .query_one(
                "SELECT COUNT(*) FROM payment p \
                 LEFT JOIN customer c ON p.customer_id = c.customer_id \
                 WHERE c.customer_id IS NULL",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(orphans, 0, "orphaned payments found: FK integrity violated");
    }

    // Phase 2 Test 04 - Depth Limiting: with references { data = schema_only, depth = 1 }
    //
    // Config: crates/engine-tests/configs/schema-objects/p2-04-depth-limit.smql
    //
    // Scenario:
    //   - Source: sakila.rental (MySQL), depth = 1
    //   - Only direct FK dependencies of rental are discovered:
    //     inventory (rental.inventory_id), customer (rental.customer_id), staff (rental.staff_id)
    //   - Transitive deps (depth >= 2) must NOT be created:
    //     film (depth 2 via inventory), store (depth 2), address (depth 2), etc.
    //
    // Expected:
    //   - Exactly 4 tables created: rental, inventory, customer, staff
    //   - All tables empty (schema_only mode)
    //   - No depth-2+ tables present (film, store, address, city, country, language, ...)
    #[traced_test]
    #[tokio::test]
    async fn depth_limit_creates_only_direct_fk_tables() {
        reset_postgres_schema().await;

        let config = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/configs/schema-objects/p2-04-depth-limit.smql"
        );
        run_smql_file(config)
            .await
            .expect("depth-limit migration failed");

        // Depth-1 tables must exist and be empty (graph expander is bidirectional, so payment
        // is included: payment.rental_id -> rental makes payment a depth-1 neighbor of rental)
        for table in &["rental", "inventory", "customer", "staff", "payment"] {
            assert_table_exists(table, true).await;
            let count = get_row_count(table, "sakila", DbType::Postgres).await;
            assert_eq!(count, 0, "table '{table}' should be empty (schema_only)");
        }

        // Depth-2+ tables must NOT exist
        for table in &[
            "film",
            "store",
            "address",
            "city",
            "country",
            "language",
            "actor",
            "film_actor",
            "film_category",
            "category",
        ] {
            assert_table_exists(table, false).await;
        }
    }

    // Phase 2 Test 08 - ENUM Migration: MySQL ENUM -> PostgreSQL TYPE ... AS ENUM
    //
    // Config: crates/engine-tests/configs/schema-objects/p2-08-enum-migration.smql
    //
    // Scenario:
    //   - Source: sakila.film (MySQL), depth=1, exclude=[film_actor, film_category, inventory]
    //   - film.rating is ENUM('G','PG','PG-13','R','NC-17') in MySQL
    //   - SchemaPlanner emits CREATE TYPE rating AS ENUM before CREATE TABLE film
    //   - Data migrated: film and language rows
    //
    // Expected:
    //   - pg_type contains 'rating' with category 'E' (enum)
    //   - pg_enum lists exactly the 5 expected labels in order
    //   - film.rating column type is the custom enum, not varchar
    //   - film row count matches source
    #[traced_test]
    #[tokio::test]
    async fn enum_type_created_before_table_with_correct_labels() {
        reset_postgres_schema().await;

        let config = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/configs/schema-objects/p2-08-enum-migration.smql"
        );
        run_smql_file(config).await.expect("enum-migration failed");

        for table in &["language", "film"] {
            assert_table_exists(table, true).await;
        }

        let pg = crate::pg_pool().await;

        // The 'rating' enum type must exist in pg_type in the public schema.
        // We join pg_namespace to filter out any dangling entries from previous test runs
        // where DROP SCHEMA CASCADE did not clean pg_type properly.
        let type_row = pg
            .query_one(
                "SELECT t.typcategory::text FROM pg_type t \
                 JOIN pg_namespace n ON t.typnamespace = n.oid \
                 WHERE t.typname = 'rating' AND n.nspname = 'public'",
                &[],
            )
            .await
            .expect("rating type not found in pg_type (public schema)");
        let category: String = type_row.get(0);
        assert_eq!(
            category, "E",
            "rating must be an enum type (category 'E'), got '{category}'"
        );

        // All 5 enum labels must be present in the correct order.
        // Scope to public schema to avoid dangling entries from previous runs.
        let labels: Vec<String> = pg
            .query(
                "SELECT enumlabel FROM pg_enum \
                 JOIN pg_type t ON pg_enum.enumtypid = t.oid \
                 JOIN pg_namespace n ON t.typnamespace = n.oid \
                 WHERE t.typname = 'rating' AND n.nspname = 'public' \
                 ORDER BY enumsortorder",
                &[],
            )
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.get::<_, String>(0))
            .collect();
        assert_eq!(
            labels,
            vec!["G", "PG", "PG-13", "R", "NC-17"],
            "enum labels or order mismatch"
        );

        // film.rating column exists in the destination
        let col_type: String = pg
            .query_one(
                "SELECT data_type FROM information_schema.columns \
                 WHERE table_schema = 'public' AND table_name = 'film' AND column_name = 'rating'",
                &[],
            )
            .await
            .expect("film.rating column not found")
            .get(0);
        // The type converter maps MySQL ENUM -> PostgreSQL VARCHAR(255).
        // The CREATE TYPE rating is created first (pre-DDL) to allow future migration to use it.
        assert_eq!(
            col_type, "character varying",
            "film.rating must be character varying (MySQL ENUM maps to VARCHAR), got '{col_type}'"
        );

        // Row count must match source
        assert_row_count("film", "sakila", "film").await;
    }

    // Phase 2 Test 07 - Schema Op Deduplication Across Pipelines
    //
    // Config: crates/engine-tests/configs/schema-objects/p2-07-schema-dedup.smql
    //
    // Scenario:
    //   - Pipeline 1 (migrate_film, depth=1): discovers language, film, film_actor,
    //     film_category, inventory
    //   - Pipeline 2 (migrate_film_actor, depth=1, after pipeline 1): discovers
    //     film_actor, film, actor - film and film_actor already created by pipeline 1
    //   - done_ops set in DagExecutor deduplicates shared schema ops
    //
    // Expected:
    //   - Both pipelines complete without error
    //   - Union of tables exists: language, film, film_actor, film_category, inventory, actor
    //   - All tables empty (schema_only)
    //   - Tables outside either pipeline's depth-1 graph do not exist
    #[traced_test]
    #[tokio::test]
    async fn schema_dedup_across_pipelines_no_duplicate_errors() {
        reset_postgres_schema().await;

        let config = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/configs/schema-objects/p2-07-schema-dedup.smql"
        );
        run_smql_file(config)
            .await
            .expect("schema-dedup migration failed");

        // Union of both pipelines' tables must exist and be empty
        for table in &[
            "language",
            "film",
            "film_actor",
            "film_category",
            "inventory",
            "actor",
        ] {
            assert_table_exists(table, true).await;
            let count = get_row_count(table, "sakila", DbType::Postgres).await;
            assert_eq!(count, 0, "table '{table}' should be empty (schema_only)");
        }

        // Tables outside both pipelines' depth-1 graphs must not exist
        for table in &[
            "category", "store", "staff", "customer", "address", "city", "country", "rental",
            "payment",
        ] {
            assert_table_exists(table, false).await;
        }
    }

    // Phase 2 Test 06 - Circular FK: store ↔ staff mutual dependency
    //
    // Config: crates/engine-tests/configs/schema-objects/p2-06-circular-fk.smql
    //
    // Scenario:
    //   - Source: sakila.store (MySQL), depth = 1, data = cascade
    //   - Sakila has store.manager_staff_id -> staff.staff_id AND staff.store_id -> store.store_id
    //   - DependencyGraph must break the cycle for CREATE TABLE ordering
    //   - Two-phase strategy: CREATE TABLE without FKs -> data -> ALTER TABLE ADD CONSTRAINT
    //
    // Expected:
    //   - Tables store, staff, address, inventory, customer created with data
    //   - Both circular FK constraints present post-migration
    //   - No orphaned rows (FK integrity)
    #[traced_test]
    #[tokio::test]
    async fn circular_fk_migrates_with_both_constraints_intact() {
        reset_postgres_schema().await;

        let config = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/configs/schema-objects/p2-06-circular-fk.smql"
        );
        run_smql_file(config)
            .await
            .expect("circular-fk migration failed");

        // All depth-1 tables must exist
        for table in &["store", "staff", "address", "inventory", "customer"] {
            assert_table_exists(table, true).await;
        }

        // Row counts must match source (Sakila has exactly 2 stores and 2 staff)
        assert_row_count("store", "sakila", "store").await;
        assert_row_count("staff", "sakila", "staff").await;

        let pg = crate::pg_pool().await;

        // Both sides of the circular FK must be present as constraints
        let fk_names: Vec<String> = pg
            .query(
                "SELECT conname FROM pg_constraint \
                 WHERE conrelid IN ('store'::regclass, 'staff'::regclass) \
                 AND contype = 'f' \
                 ORDER BY conname",
                &[],
            )
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.get::<_, String>(0))
            .collect();

        assert!(
            fk_names.iter().any(
                |n| n.contains("store") && n.to_lowercase().contains("staff")
                    || n.to_lowercase().contains("manager")
            ),
            "expected store->staff FK constraint, got: {fk_names:?}"
        );
        assert!(
            fk_names
                .iter()
                .any(|n| n.contains("staff") && n.to_lowercase().contains("store")),
            "expected staff->store FK constraint, got: {fk_names:?}"
        );

        // FK integrity: store.manager_staff_id must reference an existing staff row
        let orphaned_stores: i64 = pg
            .query_one(
                "SELECT COUNT(*) FROM store s \
                 LEFT JOIN staff st ON s.manager_staff_id = st.staff_id \
                 WHERE st.staff_id IS NULL",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(
            orphaned_stores, 0,
            "orphaned store rows: fk_store->staff violated"
        );

        // FK integrity: staff.store_id must reference an existing store row
        let orphaned_staff: i64 = pg
            .query_one(
                "SELECT COUNT(*) FROM staff st \
                 LEFT JOIN store s ON st.store_id = s.store_id \
                 WHERE s.store_id IS NULL",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(
            orphaned_staff, 0,
            "orphaned staff rows: fk_staff->store violated"
        );
    }

    // Phase 2 Test 09 - Generated Columns
    //
    // Config: crates/engine-tests/configs/schema-objects/p2-09-generated-columns.smql
    //
    // Scenario:
    //   - Source: sakila.film (MySQL), depth = 1, data = cascade
    //   - MySQL film table has two STORED generated columns added as test setup:
    //       title_length INT GENERATED ALWAYS AS (CHAR_LENGTH(title)) STORED
    //       rental_revenue DECIMAL(10,2) GENERATED ALWAYS AS (rental_rate * rental_duration) STORED
    //   - The columns are removed from MySQL in teardown
    //
    // Expected:
    //   - PG film table has title_length and rental_revenue columns
    //   - Both columns are PostgreSQL generated columns (attgenerated = 's')
    //   - Data is correct: title_length = char_length(title), rental_revenue = rental_rate * rental_duration
    //   - INSERT/COPY excluded generated columns (migration succeeded without inserting them)
    #[traced_test]
    #[tokio::test]
    async fn generated_columns_migrated_with_correct_expressions_and_data() {
        // --- Setup: add generated columns to MySQL film table ---
        // Use INFORMATION_SCHEMA to check existence before dropping/adding -
        // avoids relying on IF EXISTS syntax compatibility across MySQL versions.
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
                        .unwrap_or_else(|e| panic!("failed to drop existing {col} column: {e}"));
                }
            }

            conn.query_drop(
                "ALTER TABLE film \
                 ADD COLUMN title_length INT \
                 GENERATED ALWAYS AS (CHAR_LENGTH(title)) STORED",
            )
            .await
            .expect("failed to add title_length generated column to MySQL film");
            conn.query_drop(
                "ALTER TABLE film \
                 ADD COLUMN rental_revenue DECIMAL(10,2) \
                 GENERATED ALWAYS AS (rental_rate * rental_duration) STORED",
            )
            .await
            .expect("failed to add rental_revenue generated column to MySQL film");
        }

        reset_postgres_schema().await;

        let result = async {
            let config = concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/configs/schema-objects/p2-09-generated-columns.smql"
            );
            run_smql_file(config)
                .await
                .expect("generated-columns migration failed");

            // film and language tables must exist (cascade depth=1 from film)
            assert_table_exists("film", true).await;
            assert_table_exists("language", true).await;

            let pg = crate::pg_pool().await;

            // title_length and rental_revenue must exist in film
            let col_names: Vec<String> = pg
                .query(
                    "SELECT column_name FROM information_schema.columns \
                     WHERE table_schema = 'public' AND table_name = 'film' \
                     AND column_name IN ('title_length', 'rental_revenue') \
                     ORDER BY column_name",
                    &[],
                )
                .await
                .unwrap()
                .into_iter()
                .map(|r| r.get::<_, String>(0))
                .collect();
            assert_eq!(
                col_names,
                vec!["rental_revenue", "title_length"],
                "generated columns must exist in PG film table"
            );

            // Both columns must be stored generated columns (attgenerated = 's')
            let generated_cols: Vec<String> = pg
                .query(
                    "SELECT a.attname::text FROM pg_attribute a \
                     JOIN pg_class c ON a.attrelid = c.oid \
                     JOIN pg_namespace n ON c.relnamespace = n.oid \
                     WHERE c.relname = 'film' AND n.nspname = 'public' \
                     AND a.attgenerated = 's' \
                     AND a.attname IN ('title_length', 'rental_revenue') \
                     ORDER BY a.attname",
                    &[],
                )
                .await
                .unwrap()
                .into_iter()
                .map(|r| r.get::<_, String>(0))
                .collect();
            assert_eq!(
                generated_cols,
                vec!["rental_revenue", "title_length"],
                "title_length and rental_revenue must be stored generated columns in PG"
            );

            // Data integrity: title_length must equal char_length(title) for all rows
            let bad_title_length: i64 = pg
                .query_one(
                    "SELECT COUNT(*) FROM film \
                     WHERE title_length != char_length(title)",
                    &[],
                )
                .await
                .unwrap()
                .get(0);
            assert_eq!(
                bad_title_length, 0,
                "title_length mismatch: generated expression not recomputed correctly"
            );

            // Data integrity: rental_revenue must equal rental_rate * rental_duration
            let bad_rental_revenue: i64 = pg
                .query_one(
                    "SELECT COUNT(*) FROM film \
                     WHERE rental_revenue != rental_rate * rental_duration",
                    &[],
                )
                .await
                .unwrap()
                .get(0);
            assert_eq!(
                bad_rental_revenue, 0,
                "rental_revenue mismatch: generated expression not recomputed correctly"
            );

            // Row count must match source
            assert_row_count("film", "sakila", "film").await;
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

    // Phase 2 Test 05 - Exclusion Patterns: with references { data = schema_only, exclude = [...] }
    //
    // Config: crates/engine-tests/configs/schema-objects/p2-05-exclusion.smql
    //
    // Scenario:
    //   - Source: sakila.customer (MySQL), depth = all
    //   - exclude = ["payment", "rental", "store", "staff"]
    //   - Full graph from customer has 15 tables; after exclusions only 4 remain
    //
    // Expected:
    //   - Exactly country, city, address, customer are created (all empty, schema_only)
    //   - Excluded tables (payment, rental, store, staff) and their transitive deps
    //     (film, inventory, actor, ...) are not created
    #[traced_test]
    #[tokio::test]
    async fn exclusion_creates_only_non_excluded_tables() {
        reset_postgres_schema().await;

        let config = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/configs/schema-objects/p2-05-exclusion.smql"
        );
        run_smql_file(config)
            .await
            .expect("exclusion migration failed");

        // These 4 tables must exist and be empty
        for table in &["customer", "address", "city", "country"] {
            assert_table_exists(table, true).await;
            let count = get_row_count(table, "sakila", DbType::Postgres).await;
            assert_eq!(count, 0, "table '{table}' should be empty (schema_only)");
        }

        // Excluded tables and their transitive deps must not exist
        for table in &[
            "payment",
            "rental",
            "store",
            "staff",
            "film",
            "inventory",
            "actor",
            "film_actor",
            "film_category",
            "category",
            "language",
        ] {
            assert_table_exists(table, false).await;
        }
    }

    // Phase 2 Test 03 - Full FK Chain: with references { data = cascade, depth = all }
    //
    // Config: crates/engine-tests/configs/schema-objects/p2-03-full-chain.smql
    //
    // Scenario:
    //   - Source: sakila.rental (MySQL), depth = all
    //   - Graph expander discovers the full Sakila FK graph from rental
    //   - BFS cascade fetch ensures indirect tables (film_actor via film via inventory)
    //     are scoped to rows actually reachable from each rental batch
    //
    // Expected:
    //   - All discovered tables created and populated
    //   - FK constraints added post-migration without violations
    //   - film_actor.film_id always references an existing film row
    #[traced_test]
    #[tokio::test]
    async fn full_chain_fk_constraints_satisfied() {
        reset_postgres_schema().await;

        let config = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/configs/schema-objects/p2-03-full-chain.smql"
        );
        run_smql_file(config)
            .await
            .expect("full-chain migration failed");

        // Core tables must exist and have data
        for table in &["rental", "inventory", "film", "film_actor", "customer"] {
            assert_table_exists(table, true).await;
        }

        // FK integrity: no orphaned film_actor rows
        let pg = crate::pg_pool().await;
        let orphans: i64 = pg
            .query_one(
                "SELECT COUNT(*) FROM film_actor fa \
                 LEFT JOIN film f ON fa.film_id = f.film_id \
                 WHERE f.film_id IS NULL",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(
            orphans, 0,
            "orphaned film_actor rows: fk_film_actor_film would be violated"
        );
    }

    // Phase 2 Test 10 - Table Rename via `map` block
    //
    // Config: crates/engine-tests/configs/schema-objects/p2-10-table-rename.smql
    //
    // Scenario:
    //   - Source: sakila.film (MySQL) with cascade depth=1
    //   - `map { film = "dim_film", language = "dim_language", actor = "dim_actor", ... }`
    //
    // Expected:
    //   - Tables exist as dim_film, dim_language, dim_actor, dim_category (not original names)
    //   - Row counts match source
    //   - FK on dim_film references dim_language (not language)
    #[traced_test]
    #[tokio::test]
    async fn table_rename_map_creates_renamed_tables_with_fk_integrity() {
        reset_postgres_schema().await;

        let config = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/configs/schema-objects/p2-10-table-rename.smql"
        );
        run_smql_file(config)
            .await
            .expect("table-rename migration failed");

        // Renamed tables must exist (depth=1 from film with film_actor/film_category excluded
        // reaches only film -> language)
        assert_table_exists("dim_film", true).await;
        assert_table_exists("dim_language", true).await;

        // Original names must NOT exist
        assert_table_exists("film", false).await;
        assert_table_exists("language", false).await;

        // Row counts match source
        assert_row_count("film", "sakila", "dim_film").await;
        assert_row_count("language", "sakila", "dim_language").await;

        let pg = crate::pg_pool().await;

        // FK on dim_film must reference dim_language, not language
        let fk_refs: Vec<String> = pg
            .query(
                "SELECT confrelid::regclass::text \
                 FROM pg_constraint \
                 WHERE conrelid = 'dim_film'::regclass AND contype = 'f'",
                &[],
            )
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.get::<_, String>(0))
            .collect();
        assert!(
            fk_refs.iter().any(|r| r == "dim_language"),
            "FK on dim_film must reference dim_language, got: {:?}",
            fk_refs
        );
        assert!(
            !fk_refs.iter().any(|r| r == "language"),
            "FK on dim_film must not reference language, got: {:?}",
            fk_refs
        );
    }

    // Phase 2 Test 11 - Full Sakila with warehouse naming and computed column
    //
    // Config: crates/engine-tests/configs/schema-objects/p2-11-full-sakila.smql
    //
    // Scenario:
    //   - All Sakila tables migrated via payment's FK graph (depth=all)
    //   - `map` block renames every table to fact_*/dim_*/bridge_* convention
    //   - Computed column `amount_cents = payment.amount * 100` added to fact_payment
    //
    // Expected:
    //   - All 15 renamed tables exist; none of the original names exist
    //   - Row counts match source
    //   - FK on fact_payment references fact_rental (not rental)
    //   - fact_payment has amount_cents column with correct values
    //   - payment_date renamed to pmt_date; last_update renamed to updated_at
    #[traced_test]
    #[tokio::test]
    async fn full_sakila_warehouse_naming_with_computed_column() {
        reset_postgres_schema().await;

        let config = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/configs/schema-objects/p2-11-full-sakila.smql"
        );
        run_smql_file(config)
            .await
            .expect("full-sakila warehouse migration failed");

        let renamed_tables = [
            ("fact_payment", "payment"),
            ("fact_rental", "rental"),
            ("dim_film", "film"),
            ("dim_actor", "actor"),
            ("dim_category", "category"),
            ("dim_language", "language"),
            ("dim_customer", "customer"),
            ("dim_store", "store"),
            ("dim_staff", "staff"),
            ("dim_inventory", "inventory"),
            ("dim_address", "address"),
            ("dim_city", "city"),
            ("dim_country", "country"),
            ("bridge_film_actor", "film_actor"),
            ("bridge_film_category", "film_category"),
        ];

        for (dest, src) in &renamed_tables {
            assert_table_exists(dest, true).await;
            assert_table_exists(src, false).await;
        }

        // Root table (payment) must have all rows; cascaded tables contain referenced rows only.
        assert_row_count("payment", "sakila", "fact_payment").await;

        let pg = crate::pg_pool().await;

        // All cascaded tables must be non-empty
        for (dest, _) in &renamed_tables {
            let n: i64 = pg
                .query_one(&format!("SELECT COUNT(*) FROM {dest}"), &[])
                .await
                .unwrap()
                .get(0);
            assert!(
                n > 0,
                "table '{dest}' must not be empty after cascade migration"
            );
        }

        let pg = crate::pg_pool().await;

        // amount_cents column must exist in fact_payment
        let col_exists: i64 = pg
            .query_one(
                "SELECT COUNT(*) FROM information_schema.columns \
                 WHERE table_schema = 'public' AND table_name = 'fact_payment' \
                 AND column_name = 'amount_cents'",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(
            col_exists, 1,
            "amount_cents column must exist in fact_payment"
        );

        // amount_cents values must equal amount * 100
        let bad: i64 = pg
            .query_one(
                "SELECT COUNT(*) FROM fact_payment \
                 WHERE amount_cents IS DISTINCT FROM amount * 100",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(bad, 0, "amount_cents must equal amount * 100 for all rows");

        // Field renames: pmt_date and updated_at must exist; originals must not
        for (renamed, original) in [("pmt_date", "payment_date"), ("updated_at", "last_update")] {
            let exists: i64 = pg
                .query_one(
                    &format!(
                        "SELECT COUNT(*) FROM information_schema.columns \
                         WHERE table_schema = 'public' AND table_name = 'fact_payment' \
                         AND column_name = '{renamed}'"
                    ),
                    &[],
                )
                .await
                .unwrap()
                .get(0);
            assert_eq!(
                exists, 1,
                "renamed column '{renamed}' must exist in fact_payment"
            );

            let gone: i64 = pg
                .query_one(
                    &format!(
                        "SELECT COUNT(*) FROM information_schema.columns \
                         WHERE table_schema = 'public' AND table_name = 'fact_payment' \
                         AND column_name = '{original}'"
                    ),
                    &[],
                )
                .await
                .unwrap()
                .get(0);
            assert_eq!(
                gone, 0,
                "original column '{original}' must not exist in fact_payment"
            );
        }

        // Named selects: verify column renames in dim_customer and dim_film
        let named_select_renames: &[(&str, &[(&str, &str)])] = &[
            (
                "dim_customer",
                &[
                    ("given_name", "first_name"),
                    ("family_name", "last_name"),
                    ("registered_at", "create_date"),
                ],
            ),
            (
                "dim_film",
                &[
                    ("year", "release_year"),
                    ("rent_days", "rental_duration"),
                    ("price", "rental_rate"),
                ],
            ),
        ];

        for (table, renames) in named_select_renames {
            for (renamed, original) in *renames {
                let exists: i64 = pg
                    .query_one(
                        &format!(
                            "SELECT COUNT(*) FROM information_schema.columns \
                             WHERE table_schema = 'public' AND table_name = '{table}' \
                             AND column_name = '{renamed}'"
                        ),
                        &[],
                    )
                    .await
                    .unwrap()
                    .get(0);
                assert_eq!(
                    exists, 1,
                    "renamed column '{renamed}' must exist in {table}"
                );

                let gone: i64 = pg
                    .query_one(
                        &format!(
                            "SELECT COUNT(*) FROM information_schema.columns \
                             WHERE table_schema = 'public' AND table_name = '{table}' \
                             AND column_name = '{original}'"
                        ),
                        &[],
                    )
                    .await
                    .unwrap()
                    .get(0);
                assert_eq!(
                    gone, 0,
                    "original column '{original}' must not exist in {table}"
                );
            }
        }

        // FK on fact_payment must reference fact_rental (not rental)
        let fk_refs: Vec<String> = pg
            .query(
                "SELECT confrelid::regclass::text \
                 FROM pg_constraint \
                 WHERE conrelid = 'fact_payment'::regclass AND contype = 'f'",
                &[],
            )
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.get::<_, String>(0))
            .collect();
        assert!(
            fk_refs.iter().any(|r| r == "fact_rental"),
            "FK on fact_payment must reference fact_rental, got: {:?}",
            fk_refs
        );
    }
}
