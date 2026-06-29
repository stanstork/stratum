use crate::context::PipelineContext;
use crate::transform::wasm::WasmTransform;
use crate::{
    error::ProducerError,
    item::ItemId,
    producer::{
        components::{
            coordinator::BatchCoordinator, reader::SnapshotReader, transformer::TransformService,
        },
        config::ProducerConfig,
    },
    state_manager::StateManager,
    transform::{
        computed::ComputedTransform,
        mapping::{FieldMapper, TableMapper},
        pipeline::{TransformPipeline, TransformPipelineExt},
        pruner::FieldPruner,
        validation::PipelineValidator,
    },
};
use engine_core::{context::env::EnvContext, retry::RetryPolicy};
use engine_state::MerkleStore;
use engine_wasm::registry::PluginRegistry;
use model::{
    execution::pipeline::Pipeline, pagination::cursor::Cursor, records::batch::Batch,
    transform::mapping::TransformationMetadata,
};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::debug;

pub mod components;
pub mod config;

#[allow(clippy::result_large_err)]
pub fn build_transform_pipeline(
    pipeline: &Pipeline,
    plugin_registry: &PluginRegistry,
    mapping: &TransformationMetadata,
    mapped_columns_only: bool,
    env: Arc<EnvContext>,
) -> Result<TransformPipeline, ProducerError> {
    let mut tp = TransformPipeline::new();

    // Each transform is only added if it's needed.
    tp = tp
        .add_if(!mapping.entities.is_empty(), || {
            TableMapper::new(mapping.entities.clone())
        })
        .add_if(!mapping.field_mappings.field_renames.is_empty(), || {
            FieldMapper::new(mapping.field_mappings.clone())
        })
        .add_if(!mapping.field_mappings.computed_fields.is_empty(), || {
            ComputedTransform::new(mapping.clone(), env.clone())
        });

    // WASM transforms run AFTER built-in transforms but BEFORE pruning/validation.
    for call in &pipeline.plugin_transforms {
        let plugin = plugin_registry
            .instantiate(&call.plugin_name)
            .map_err(|e| {
                ProducerError::Other(format!(
                    "plugin '{}' instantiation failed: {e}",
                    call.plugin_name
                ))
            })?;
        tp = tp.add_transform(WasmTransform::new(
            plugin,
            call.output_column.clone(),
            call.input_mapping.clone(),
        ));
    }

    // Prune unmapped columns last, once plugin inputs have been consumed.
    tp = tp.add_if(mapped_columns_only, || FieldPruner::new(mapping.clone()));

    if !pipeline.validations.is_empty() {
        let validator = PipelineValidator::new(
            pipeline.validations.clone(),
            mapping.clone(),
            env.clone(),
            plugin_registry,
        )?;
        tp = tp.add_validator(validator);
    }

    Ok(tp)
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ProducerStatus {
    /// Work is ongoing; the actor should schedule another tick.
    Working,
    /// The producer is idle (e.g. waiting for CDC events or backpressure).
    Idle,
    /// The producer has finished its task (e.g. Snapshot complete).
    Finished,
}

#[derive(Clone, Debug, PartialEq)]
enum ProducerMode {
    Idle,
    Snapshot,
    Cdc,
    Finished,
}

pub struct Producer {
    // Components
    reader: SnapshotReader,
    transformer: TransformService,
    coordinator: BatchCoordinator,

    // State
    pipeline_name: String,
    cursor: Cursor,
    mode: ProducerMode,
    ids: ItemId,

    // Config
    config: ProducerConfig,
}

impl Producer {
    pub async fn new(
        ctx: &PipelineContext,
        batch_tx: mpsc::Sender<Batch>,
        mut config: ProducerConfig,
        mapped_columns_only: bool,
    ) -> Result<Self, ProducerError> {
        let exec_ctx = ctx.exec_ctx.clone();
        let run_id = ctx.run_id.clone();
        let item_id = ctx.item_id.clone();
        let part_id = "part-0".to_string();
        let source = ctx.source.clone();
        let pipeline = ctx.pipeline.clone();
        let mapping = ctx.mapping.clone();
        let state_store = ctx.state.clone();
        let cursor = ctx.cursor.clone();

        let ids = ItemId::new(run_id, item_id, part_id);

        // Create retry policy from pipeline config, fallback to database defaults
        let retry_config = pipeline
            .error_handling
            .as_ref()
            .and_then(|eh| eh.retry.as_ref());
        let retry_policy = RetryPolicy::from_config(retry_config);

        // Create components
        let reader = SnapshotReader::new(source, retry_policy, config.batch_size);

        let env = exec_ctx.env.clone();
        let transform_pipeline = build_transform_pipeline(
            &pipeline,
            &ctx.plugin_registry,
            &mapping,
            mapped_columns_only,
            env,
        )?;
        let transformer = TransformService::new(
            exec_ctx,
            transform_pipeline,
            pipeline.name.clone(),
            pipeline.error_handling.clone(),
        );

        let state_manager = StateManager::new(ids.clone(), state_store.clone());
        let mut coordinator = BatchCoordinator::new(batch_tx, state_manager);
        if let Some(integrity) = config.integrity.take() {
            let merkle_store = state_store as Arc<dyn MerkleStore>;
            coordinator = coordinator.enable_integrity(integrity, merkle_store);
        }

        Ok(Self {
            reader,
            transformer,
            coordinator,
            cursor,
            mode: ProducerMode::Idle,
            ids,
            config,
            pipeline_name: pipeline.name.clone(),
        })
    }

    pub async fn start_snapshot(&mut self) -> Result<(), ProducerError> {
        self.mode = ProducerMode::Snapshot;
        Ok(())
    }

    pub async fn start_cdc(&mut self) -> Result<(), ProducerError> {
        self.mode = ProducerMode::Cdc;
        Ok(())
    }

    pub async fn resume(
        &mut self,
        run_id: &str,
        item_id: &str,
        part_id: &str,
    ) -> Result<(), ProducerError> {
        self.cursor = self.coordinator.state_manager().resume_cursor().await?;
        debug!(
            run_id = run_id,
            item_id = item_id,
            part_id = part_id,
            cursor = ?self.cursor,
            "resuming producer from cursor"
        );
        Ok(())
    }

    pub async fn tick(&mut self) -> Result<ProducerStatus, ProducerError> {
        match self.mode {
            ProducerMode::Idle => Ok(ProducerStatus::Idle),
            ProducerMode::Finished => Ok(ProducerStatus::Finished),
            ProducerMode::Snapshot => {
                let status = self.process_snapshot_batch().await?;
                if status == ProducerStatus::Finished {
                    self.coordinator
                        .finalize_integrity(&self.pipeline_name)
                        .await?;
                }
                Ok(status)
            }
            ProducerMode::Cdc => {
                // CDC logic here
                tokio::time::sleep(self.config.idle_poll_interval).await;
                Ok(ProducerStatus::Working)
            }
        }
    }

    pub async fn stop(&mut self) -> Result<(), ProducerError> {
        self.mode = ProducerMode::Finished;
        Ok(())
    }

    pub fn rows_produced(&self) -> u64 {
        self.coordinator.rows_produced()
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

        // Coordinate batch delivery
        let batch_id = self.batch_id(&self.cursor);
        let next = fetch_result.next_cursor.unwrap_or(Cursor::None);

        // Transform data - will process entire batch even if some rows fail
        let transform_result = self
            .transformer
            .transform(&self.ids.run_id(), &batch_id, fetch_result.rows)
            .await?;

        // Process batch - stats are recorded only after successful completion
        self.coordinator
            .process_batch(
                batch_id,
                self.cursor.clone(),
                transform_result,
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

    /// Returns the total number of batches processed
    pub fn batches_processed(&self) -> u64 {
        self.coordinator.batches_processed()
    }

    /// Returns the total number of rows skipped during transformation
    pub fn total_rows_skipped(&self) -> u64 {
        self.coordinator.rows_skipped()
    }

    /// Returns the total number of rows that failed during transformation
    pub fn total_rows_failed(&self) -> u64 {
        self.coordinator.rows_failed()
    }
}
