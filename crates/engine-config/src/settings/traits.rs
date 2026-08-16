use super::{error::SettingsError, phase::MigrationSettingsPhase};
use async_trait::async_trait;
use engine_processing::context::PipelineContext;
use engine_schema::schema_ops::SchemaOps;

#[async_trait]
pub trait MigrationSetting: Send + Sync {
    fn phase(&self) -> MigrationSettingsPhase;

    /// Check whether this setting should be applied in the given context.
    /// By default, all settings are applicable.
    fn can_apply(&self, _ctx: &PipelineContext) -> bool {
        true
    }

    /// Collect DDL operations without executing them.
    /// Schema settings override this to return pre/post ops.
    /// Non-schema settings return empty by default.
    async fn plan(&mut self, _ctx: &PipelineContext) -> Result<SchemaOps, SettingsError> {
        Ok(SchemaOps::empty())
    }
}
