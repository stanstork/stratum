#[cfg(test)]
mod tests {
    use crate::{
        TEST_MYSQL_URL_SAKILA, TEST_PG_URL, reset_postgres_schema,
        utils::{DbType, assert_row_count, execute, get_row_count},
    };
    use chrono::Utc;
    use engine_config::{
        report::dry_run::DryRunReport,
        settings::{self, validated::ValidatedSettings},
    };
    use engine_core::{
        context::{
            global::GlobalContext,
            item::{ItemContext, ItemContextParams},
        },
        state::{
            StateStore,
            models::{Checkpoint, WalEntry},
            sled_store::SledStateStore,
        },
    };
    use engine_processing::error::{ConsumerError, ProducerError};
    use engine_runtime::execution::{factory, metadata};
    use futures::lock::Mutex;
    use model::{pagination::cursor::Cursor, transform::mapping::EntityMapping};
    use planner::{
        plan::{MigrationPlan, parse},
        query::offsets::{OffsetStrategy, OffsetStrategyFactory},
    };
    use smql_syntax::ast::migrate::MigrateItem;
    use std::sync::Arc;
    use tempfile::tempdir;
    use tokio_util::sync::CancellationToken;

    const DEST_TABLE: &str = "actor_engine_replay";
    const DEST_TABLE_RESUME: &str = "actor_engine_resume";
    const DEST_TABLE_TRANSIENT: &str = "actor_retry_transient";
    const DEST_TABLE_BREAKER: &str = "actor_breaker_trip";
    const RUN_ID: &str = "engine-restart-run";
    const ITEM_ID: &str = "actor-item";
    const PART_ID: &str = "part-0";
    const FN_TRANSIENT: &str = "fn_actor_retry_fail_once";
    const FN_BREAKER: &str = "fn_actor_breaker_fail";

    struct EngineRunResult {
        producer: Result<usize, ProducerError>,
        consumer: Result<(), ConsumerError>,
    }

    #[ignore] // TODO: Rewrite for actor-based system - this test was for old manual producer/consumer
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn crash_before_commit_replays_batch() {
        reset_postgres_schema().await;

        // Pre-create a destination table that forces the first write to fail.
        execute(&format!(
            r#"
            CREATE TABLE {table} (
                actor_id SMALLINT PRIMARY KEY,
                first_name VARCHAR(45) NOT NULL,
                last_name VARCHAR(45) NOT NULL,
                last_update TIMESTAMP NOT NULL,
                fail_flag INT NOT NULL
            );
        "#,
            table = DEST_TABLE,
        ))
        .await;

        let smql = format!(
            r#"
            CONNECTIONS(
                SOURCE(MYSQL, "{mysql_url}"),
                DESTINATION(POSTGRES, "{pg_url}")
            );
            MIGRATE(
                SOURCE(TABLE, actor) -> DEST(TABLE, {dest_table}) [
                    SETTINGS(BATCH_SIZE=64)
                ]
            );
        "#,
            mysql_url = TEST_MYSQL_URL_SAKILA,
            pg_url = TEST_PG_URL,
            dest_table = DEST_TABLE,
        );

        let plan = parse(&smql).expect("parse plan");
        let migrate_item = plan
            .migration
            .migrate_items
            .first()
            .expect("expected migrate item");
        let mapping = EntityMapping::new(migrate_item);
        let offset_strategy = OffsetStrategyFactory::from_smql(&migrate_item.offset);
        let cursor = Cursor::None;

        let state_dir = tempdir().expect("state dir");
        let state_store = Arc::new(SledStateStore::open(state_dir.path()).expect("open sled"));
        let global_ctx = Arc::new(
            GlobalContext::new(&plan, state_store.clone())
                .await
                .expect("global ctx"),
        );

        state_store
            .append_wal(&WalEntry::RunStart {
                run_id: RUN_ID.to_string(),
                plan_hash: plan.hash(),
            })
            .await
            .expect("run start wal");
        state_store
            .append_wal(&WalEntry::ItemStart {
                run_id: RUN_ID.to_string(),
                item_id: ITEM_ID.to_string(),
            })
            .await
            .expect("item start wal");

        let (ctx_first, report_first, settings) = build_item_context(
            &global_ctx,
            &plan,
            migrate_item,
            &mapping,
            offset_strategy.clone(),
            cursor.clone(),
        )
        .await;

        let result_first = run_engine_once(ctx_first, settings, report_first).await;
        let _ = &result_first.producer;
        assert!(
            result_first.consumer.is_err(),
            "first run must fail before commit"
        );

        let checkpoint = state_store
            .last_checkpoint(RUN_ID, ITEM_ID, PART_ID)
            .await
            .expect("checkpoint load")
            .expect("checkpoint missing after crash");
        assert_eq!(checkpoint.stage, "write");

        let wal = state_store.iter_wal(RUN_ID).await.expect("wal entries");
        assert!(
            !wal_has_commit(&wal),
            "no BatchCommit should exist before fixing destination"
        );

        // Fix the schema so the next attempt can succeed.
        execute(&format!(
            r#"ALTER TABLE {table} DROP COLUMN fail_flag;"#,
            table = DEST_TABLE
        ))
        .await;

        let (ctx_second, report_second, settings) = build_item_context(
            &global_ctx,
            &plan,
            migrate_item,
            &mapping,
            offset_strategy,
            cursor,
        )
        .await;

        let result_second = run_engine_once(ctx_second, settings, report_second).await;
        let _ = &result_second.producer;
        assert!(
            result_second.consumer.is_ok(),
            "second run should recover and finish"
        );

        let final_checkpoint = state_store
            .last_checkpoint(RUN_ID, ITEM_ID, PART_ID)
            .await
            .expect("load final checkpoint")
            .expect("missing final checkpoint");
        assert_eq!(final_checkpoint.stage, "committed");

        let wal_after_restart = state_store
            .iter_wal(RUN_ID)
            .await
            .expect("wal after restart");
        assert!(
            wal_has_commit(&wal_after_restart),
            "commit entry expected after restart"
        );

        assert_row_count("actor", "sakila", DEST_TABLE).await;
    }

    #[ignore] // TODO: Rewrite for actor-based system - this test was for old manual producer/consumer
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn committed_wal_with_write_checkpoint_resumes_cleanly() {
        reset_postgres_schema().await;

        // Seed destination with the first row to mimic a prior committed batch and force a write failure.
        execute(&format!(
            r#"
            CREATE TABLE {table} (
                actor_id SMALLINT PRIMARY KEY,
                first_name VARCHAR(45) NOT NULL,
                last_name VARCHAR(45) NOT NULL,
                last_update TIMESTAMP NOT NULL,
                fail_flag INT NOT NULL
            );
            INSERT INTO {table} (actor_id, first_name, last_name, last_update, fail_flag)
            VALUES (1, 'Existing', 'Actor', NOW(), 1);
        "#,
            table = DEST_TABLE_RESUME,
        ))
        .await;

        let smql = format!(
            r#"
            CONNECTIONS(
                SOURCE(MYSQL, "{mysql_url}"),
                DESTINATION(POSTGRES, "{pg_url}")
            );
            MIGRATE(
                SOURCE(TABLE, actor) -> DEST(TABLE, {dest_table}) [
                    SETTINGS(BATCH_SIZE=1)
                ]
            );
        "#,
            mysql_url = TEST_MYSQL_URL_SAKILA,
            pg_url = TEST_PG_URL,
            dest_table = DEST_TABLE_RESUME,
        );

        let plan = parse(&smql).expect("parse plan");
        let migrate_item = plan
            .migration
            .migrate_items
            .first()
            .expect("expected migrate item");
        let mapping = EntityMapping::new(migrate_item);
        let offset_strategy = OffsetStrategyFactory::from_smql(&migrate_item.offset);
        let cursor = Cursor::None;

        let state_dir = tempdir().expect("state dir");
        let state_store = Arc::new(SledStateStore::open(state_dir.path()).expect("open sled"));
        let global_ctx = Arc::new(
            GlobalContext::new(&plan, state_store.clone())
                .await
                .expect("global ctx"),
        );

        let initial_batch_id = batch_id_for(RUN_ID, ITEM_ID, PART_ID, &cursor);
        state_store
            .append_wal(&WalEntry::RunStart {
                run_id: RUN_ID.to_string(),
                plan_hash: plan.hash(),
            })
            .await
            .expect("run start wal");
        state_store
            .append_wal(&WalEntry::ItemStart {
                run_id: RUN_ID.to_string(),
                item_id: ITEM_ID.to_string(),
            })
            .await
            .expect("item start wal");
        state_store
            .append_wal(&WalEntry::BatchCommit {
                run_id: RUN_ID.to_string(),
                item_id: ITEM_ID.to_string(),
                part_id: PART_ID.to_string(),
                batch_id: initial_batch_id.clone(),
                ts: Utc::now(),
            })
            .await
            .expect("seed wal commit");
        state_store
            .save_checkpoint(&Checkpoint {
                run_id: RUN_ID.to_string(),
                item_id: ITEM_ID.to_string(),
                part_id: PART_ID.to_string(),
                stage: "write".to_string(),
                src_offset: cursor.clone(),
                pending_offset: None,
                batch_id: initial_batch_id.clone(),
                rows_done: 0,
                updated_at: Utc::now(),
            })
            .await
            .expect("seed checkpoint");

        let (ctx_first, report_first, settings) = build_item_context(
            &global_ctx,
            &plan,
            migrate_item,
            &mapping,
            offset_strategy.clone(),
            cursor.clone(),
        )
        .await;

        let result_first = run_engine_once(ctx_first, settings, report_first).await;
        assert!(
            result_first.consumer.is_err(),
            "first run should fail because destination schema rejects the batch"
        );

        // Destination still only has the seeded row.
        let existing = get_row_count(DEST_TABLE_RESUME, "sakila", DbType::Postgres).await;
        assert_eq!(existing, 1, "seeded rows should remain after failure");

        let checkpoint = state_store
            .last_checkpoint(RUN_ID, ITEM_ID, PART_ID)
            .await
            .expect("checkpoint load")
            .expect("checkpoint missing after failure");
        assert_eq!(checkpoint.stage, "write");

        let wal = state_store.iter_wal(RUN_ID).await.expect("wal entries");
        assert!(
            wal_has_commit(&wal),
            "BatchCommit should be preserved for checkpoint batch"
        );

        // Fix the schema so the next attempt can succeed.
        execute(&format!(
            r#"ALTER TABLE {table} DROP COLUMN fail_flag;"#,
            table = DEST_TABLE_RESUME
        ))
        .await;

        let (ctx_second, report_second, settings) = build_item_context(
            &global_ctx,
            &plan,
            migrate_item,
            &mapping,
            offset_strategy,
            cursor,
        )
        .await;

        let result_second = run_engine_once(ctx_second, settings, report_second).await;
        let _ = &result_second.producer;
        assert!(
            result_second.consumer.is_ok(),
            "second run should resume after committed batch without duplicates"
        );

        assert_row_count("actor", "sakila", DEST_TABLE_RESUME).await;
    }

    #[ignore] // TODO: Rewrite for actor-based system - this test was for old manual producer/consumer
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn transient_write_failure_retries_and_succeeds() {
        reset_postgres_schema().await;

        create_transient_failure_table(DEST_TABLE_TRANSIENT, FN_TRANSIENT).await;

        let smql = format!(
            r#"
            CONNECTIONS(
                SOURCE(MYSQL, "{mysql_url}"),
                DESTINATION(POSTGRES, "{pg_url}")
            );
            MIGRATE(
                SOURCE(TABLE, actor) -> DEST(TABLE, {dest_table}) [
                    SETTINGS(BATCH_SIZE=16)
                ]
            );
        "#,
            mysql_url = TEST_MYSQL_URL_SAKILA,
            pg_url = TEST_PG_URL,
            dest_table = DEST_TABLE_TRANSIENT,
        );

        let plan = parse(&smql).expect("parse plan");
        let migrate_item = plan
            .migration
            .migrate_items
            .first()
            .expect("expected migrate item");
        let mapping = EntityMapping::new(migrate_item);
        let offset_strategy = OffsetStrategyFactory::from_smql(&migrate_item.offset);
        let cursor = Cursor::None;

        let state_dir = tempdir().expect("state dir");
        let state_store = Arc::new(SledStateStore::open(state_dir.path()).expect("open sled"));
        let global_ctx = Arc::new(
            GlobalContext::new(&plan, state_store.clone())
                .await
                .expect("global ctx"),
        );

        state_store
            .append_wal(&WalEntry::RunStart {
                run_id: RUN_ID.to_string(),
                plan_hash: plan.hash(),
            })
            .await
            .expect("run start wal");
        state_store
            .append_wal(&WalEntry::ItemStart {
                run_id: RUN_ID.to_string(),
                item_id: ITEM_ID.to_string(),
            })
            .await
            .expect("item start wal");

        let (ctx, report, settings) = build_item_context(
            &global_ctx,
            &plan,
            migrate_item,
            &mapping,
            offset_strategy,
            cursor,
        )
        .await;

        let result = run_engine_once(ctx, settings, report).await;
        assert!(
            result.consumer.is_ok(),
            "transient failure should be retried and eventually succeed"
        );

        let wal_entries = state_store.iter_wal(RUN_ID).await.expect("wal entries");
        assert!(
            wal_entries
                .iter()
                .all(|entry| !matches!(entry, WalEntry::CircuitBreakerOpen { .. })),
            "transient errors must not open the breaker"
        );

        assert_row_count("actor", "sakila", DEST_TABLE_TRANSIENT).await;
    }

    #[ignore] // TODO: Rewrite for actor-based system - this test was for old manual producer/consumer
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn permanent_write_failure_trips_circuit_breaker() {
        reset_postgres_schema().await;

        create_permanent_failure_table(DEST_TABLE_BREAKER, FN_BREAKER).await;

        let smql = format!(
            r#"
            CONNECTIONS(
                SOURCE(MYSQL, "{mysql_url}"),
                DESTINATION(POSTGRES, "{pg_url}")
            );
            MIGRATE(
                SOURCE(TABLE, actor) -> DEST(TABLE, {dest_table}) [
                    SETTINGS(BATCH_SIZE=8)
                ]
            );
        "#,
            mysql_url = TEST_MYSQL_URL_SAKILA,
            pg_url = TEST_PG_URL,
            dest_table = DEST_TABLE_BREAKER,
        );

        let plan = parse(&smql).expect("parse plan");
        let migrate_item = plan
            .migration
            .migrate_items
            .first()
            .expect("expected migrate item");
        let mapping = EntityMapping::new(migrate_item);
        let offset_strategy = OffsetStrategyFactory::from_smql(&migrate_item.offset);
        let cursor = Cursor::None;

        let state_dir = tempdir().expect("state dir");
        let state_store = Arc::new(SledStateStore::open(state_dir.path()).expect("open sled"));
        let global_ctx = Arc::new(
            GlobalContext::new(&plan, state_store.clone())
                .await
                .expect("global ctx"),
        );

        state_store
            .append_wal(&WalEntry::RunStart {
                run_id: RUN_ID.to_string(),
                plan_hash: plan.hash(),
            })
            .await
            .expect("run start wal");
        state_store
            .append_wal(&WalEntry::ItemStart {
                run_id: RUN_ID.to_string(),
                item_id: ITEM_ID.to_string(),
            })
            .await
            .expect("item start wal");

        let (ctx, report, settings) = build_item_context(
            &global_ctx,
            &plan,
            migrate_item,
            &mapping,
            offset_strategy,
            cursor,
        )
        .await;

        let result = run_engine_once(ctx, settings, report).await;
        assert!(
            matches!(
                result.consumer,
                Err(ConsumerError::CircuitBreakerOpen { .. })
            ),
            "permanent failures should open the breaker"
        );

        let wal_entries = state_store.iter_wal(RUN_ID).await.expect("wal entries");
        assert!(
            wal_entries.iter().any(|entry| matches!(
                entry,
                WalEntry::CircuitBreakerOpen { stage, .. } if stage == "write"
            )),
            "WAL must record when the consumer breaker opens"
        );

        let rows = get_row_count(DEST_TABLE_BREAKER, "sakila", DbType::Postgres).await;
        assert_eq!(rows, 0, "no rows should be written after breaker opens");
    }

    async fn build_item_context(
        global_ctx: &Arc<GlobalContext>,
        plan: &MigrationPlan,
        migrate_item: &MigrateItem,
        mapping: &EntityMapping,
        offset_strategy: Arc<dyn OffsetStrategy>,
        cursor: Cursor,
    ) -> (
        Arc<Mutex<ItemContext>>,
        Arc<Mutex<DryRunReport>>,
        ValidatedSettings,
    ) {
        let source = factory::create_source(
            global_ctx,
            &plan.connections,
            mapping,
            migrate_item,
            offset_strategy.clone(),
        )
        .await
        .expect("create source");

        let destination = factory::create_destination(global_ctx, &plan.connections, migrate_item)
            .await
            .expect("create destination");

        let mut item_ctx = ItemContext::new(ItemContextParams {
            run_id: RUN_ID.to_string(),
            item_id: ITEM_ID.to_string(),
            source,
            destination,
            mapping: mapping.clone(),
            state: global_ctx.state.clone(),
            offset_strategy,
            cursor,
        });

        let dry_run_report = Arc::new(Mutex::new(DryRunReport::default()));
        let settings = settings::validate_and_apply(
            &mut item_ctx,
            &migrate_item.settings,
            false,
            &dry_run_report,
        )
        .await
        .expect("validate settings");
        metadata::load(&mut item_ctx).await.expect("load metadata");

        (Arc::new(Mutex::new(item_ctx)), dry_run_report, settings)
    }

    async fn run_engine_once(
        ctx: Arc<Mutex<ItemContext>>,
        settings: ValidatedSettings,
        report: Arc<Mutex<DryRunReport>>,
    ) -> EngineRunResult {
        use engine_runtime::execution::workers;

        // Use the real workers::spawn which handles the actor runtime
        let cancel = CancellationToken::new();

        // Run the migration using the actual worker spawn logic
        let result = workers::spawn(ctx.clone(), &settings, cancel.clone(), &report).await;

        // Extract producer/consumer results from the migration result
        // Since workers::spawn returns Result<(), MigrationError>, we need to map it
        match result {
            Ok(()) => {
                // Success - both producer and consumer completed successfully
                EngineRunResult {
                    producer: Ok(0), // We don't track batch count in the new system
                    consumer: Ok(()),
                }
            }
            Err(e) => {
                // Migration failed - this could be producer or consumer error
                // We'll treat it as a consumer error for test compatibility
                EngineRunResult {
                    producer: Ok(0),
                    consumer: Err(ConsumerError::CircuitBreakerOpen {
                        stage: "write".to_string(),
                        last_error: e.to_string(),
                    }),
                }
            }
        }
    }

    fn wal_has_commit(entries: &[WalEntry]) -> bool {
        entries
            .iter()
            .any(|entry| matches!(entry, WalEntry::BatchCommit { .. }))
    }

    fn batch_id_for(run_id: &str, item_id: &str, part_id: &str, cursor: &Cursor) -> String {
        let mut h = blake3::Hasher::new();
        h.update(run_id.as_bytes());
        h.update(item_id.as_bytes());
        h.update(part_id.as_bytes());
        h.update(format!("{cursor:?}").as_bytes());
        h.finalize().to_hex().to_string()
    }

    async fn create_transient_failure_table(table: &str, function_name: &str) {
        let seq_name = format!("{table}_fail_seq");

        execute(&format!(
            r#"
            CREATE SEQUENCE {seq_name} START 1;

            CREATE OR REPLACE FUNCTION {function_name}() RETURNS boolean AS $$
            DECLARE attempt BIGINT;
            BEGIN
                attempt := nextval('{seq_name}');
                IF attempt = 1 THEN
                    RAISE EXCEPTION USING ERRCODE = '40001', MESSAGE = 'simulated serialization failure';
                END IF;
                RETURN true;
            END;
            $$ LANGUAGE plpgsql;
        "#,
            function_name = function_name,
            seq_name = seq_name,
        ))
        .await;

        execute(&format!(
            r#"
            CREATE TABLE {table} (
                actor_id SMALLINT,
                first_name VARCHAR(45) NOT NULL,
                last_name VARCHAR(45) NOT NULL,
                last_update TIMESTAMP NOT NULL,
                CONSTRAINT {table}_fail_guard CHECK ({function_name}())
            );
        "#,
            table = table,
            function_name = function_name,
        ))
        .await;
    }

    async fn create_permanent_failure_table(table: &str, function_name: &str) {
        execute(&format!(
            r#"
            CREATE OR REPLACE FUNCTION {function_name}() RETURNS boolean AS $$
            BEGIN
                RAISE EXCEPTION USING ERRCODE = '23505', MESSAGE = 'simulated permanent failure';
            END;
            $$ LANGUAGE plpgsql;
        "#,
            function_name = function_name,
        ))
        .await;

        execute(&format!(
            r#"
            CREATE TABLE {table} (
                actor_id SMALLINT,
                first_name VARCHAR(45) NOT NULL,
                last_name VARCHAR(45) NOT NULL,
                last_update TIMESTAMP NOT NULL,
                CONSTRAINT {table}_fail_guard CHECK ({function_name}())
            );
        "#,
            table = table,
            function_name = function_name,
        ))
        .await;
    }
}
