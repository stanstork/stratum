use chrono::{DateTime, Utc};
use engine_core::plan::execution::ExecutionPlan as CoreExecutionPlan;
use std::path::Path;
use uuid::Uuid;

pub struct PlanMetadata {
    pub plan_id: String,
    pub generated_at: DateTime<Utc>,
    pub engine_version: String,
    pub config_hash: String,
    pub config_path: String,
}

/// Handles the generation of plan-level metadata
pub struct MetadataGenerator;

impl MetadataGenerator {
    pub fn generate(core_plan: &CoreExecutionPlan, config_path: &Path) -> PlanMetadata {
        PlanMetadata {
            plan_id: Uuid::new_v4().to_string(),
            generated_at: Utc::now(),
            engine_version: env!("CARGO_PKG_VERSION").to_string(),
            config_hash: core_plan.hash().to_string(),
            config_path: config_path.display().to_string(),
        }
    }
}
