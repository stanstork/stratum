use crate::{
    error::ProducerError,
    item::ItemId,
    producer::{
        DataProducer, ProducerStatus,
        components::{
            coordinator::BatchCoordinator, reader::SnapshotReader, transformer::TransformService,
        },
        config::ProducerConfig,
        pipeline_for_mapping,
    },
    state_manager::StateManager,
};
use async_trait::async_trait;
use engine_config::settings::validated::ValidatedSettings;
use engine_core::{context::item::ItemContext, retry::RetryPolicy};
use futures::lock::Mutex;
use model::{pagination::cursor::Cursor, records::batch::Batch};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::info;

#[derive(Clone, Debug, PartialEq)]
enum ProducerMode {
    Idle,
    Snapshot,
    Cdc,
    Finished,
}

pub struct LiveProducer {
    // Components
    reader: SnapshotReader,
    transformer: TransformService,
    coordinator: BatchCoordinator,

    // State
    cursor: Cursor,
    mode: ProducerMode,
    ids: ItemId,

    // Config
    config: ProducerConfig,
}

impl LiveProducer {
    pub async fn new(
        ctx: &Arc<Mutex<ItemContext>>,
        batch_tx: mpsc::Sender<Batch>,
        settings: &ValidatedSettings,
    ) -> Self {
        let (run_id, item_id, part_id, source, mapping, state_store, cursor) = {
            let c = ctx.lock().await;
            (
                c.run_id.clone(),
                c.item_id.clone(),
                "part-0".to_string(),
                c.source.clone(),
                c.mapping.clone(),
                c.state.clone(),
                c.cursor.clone(),
            )
        };

        let config = ProducerConfig::from_settings(settings);
        let ids = ItemId::new(run_id, item_id, part_id);

        // Create components
        let reader = SnapshotReader::new(source, RetryPolicy::for_database(), config.batch_size);

        let pipeline = pipeline_for_mapping(&mapping);
        let transformer = TransformService::new(pipeline, config.transform_concurrency);

        let state_manager = StateManager::new(ids.clone(), state_store);
        let coordinator = BatchCoordinator::new(batch_tx, state_manager);

        Self {
            reader,
            transformer,
            coordinator,
            cursor,
            mode: ProducerMode::Idle,
            ids,
            config,
        }
    }

    fn batch_id(&self, next: &Cursor) -> String {
        let mut h = blake3::Hasher::new();
        h.update(self.ids.run_id().as_bytes());
        h.update(self.ids.item_id().as_bytes());
        h.update(self.ids.part_id().as_bytes());
        h.update(format!("{next:?}").as_bytes());
        h.finalize().to_hex().to_string()
    }

    async fn process_snapshot_batch(&mut self) -> Result<ProducerStatus, ProducerError> {
        let fetch_result = self.reader.fetch(self.cursor.clone()).await?;

        // Handle empty/end cases
        if SnapshotReader::is_complete(&fetch_result) {
            self.mode = ProducerMode::Finished;
            return Ok(ProducerStatus::Finished);
        }

        if SnapshotReader::should_advance(&fetch_result) {
            self.cursor = fetch_result.next_cursor.unwrap();
            return Ok(ProducerStatus::Working);
        }

        if fetch_result.row_count == 0 {
            self.mode = ProducerMode::Finished;
            return Ok(ProducerStatus::Finished);
        }

        // Transform data
        let transformed_rows = self.transformer.transform(fetch_result.rows).await;

        // Coordinate batch delivery
        let batch_id = self.batch_id(&self.cursor);
        let next = fetch_result.next_cursor.unwrap_or(Cursor::None);

        self.coordinator
            .process_batch(
                batch_id,
                self.cursor.clone(),
                transformed_rows,
                next.clone(),
            )
            .await?;

        // Advance cursor
        self.cursor = next;

        if self.cursor == Cursor::None {
            self.mode = ProducerMode::Finished;
            // Close the batch channel to signal consumers we're done
            self.coordinator.close_channel();
            return Ok(ProducerStatus::Finished);
        }

        Ok(ProducerStatus::Working)
    }
}

#[async_trait]
impl DataProducer for LiveProducer {
    async fn start_snapshot(&mut self) -> Result<(), ProducerError> {
        self.mode = ProducerMode::Snapshot;
        Ok(())
    }

    async fn start_cdc(&mut self) -> Result<(), ProducerError> {
        self.mode = ProducerMode::Cdc;
        Ok(())
    }

    async fn resume(
        &mut self,
        run_id: &str,
        item_id: &str,
        part_id: &str,
    ) -> Result<(), ProducerError> {
        self.cursor = self.coordinator.state_manager().resume_cursor().await?;
        info!(
            run_id = run_id,
            item_id = item_id,
            part_id = part_id,
            "Resuming producer from cursor: {:?}",
            self.cursor
        );
        Ok(())
    }

    async fn tick(&mut self) -> Result<ProducerStatus, ProducerError> {
        match self.mode {
            ProducerMode::Idle => Ok(ProducerStatus::Idle),
            ProducerMode::Finished => Ok(ProducerStatus::Finished),
            ProducerMode::Snapshot => self.process_snapshot_batch().await,
            ProducerMode::Cdc => {
                // CDC logic here
                tokio::time::sleep(self.config.idle_poll_interval).await;
                Ok(ProducerStatus::Working)
            }
        }
    }

    async fn stop(&mut self) -> Result<(), ProducerError> {
        self.mode = ProducerMode::Finished;
        Ok(())
    }

    fn rows_produced(&self) -> u64 {
        self.coordinator.rows_produced()
    }
}
