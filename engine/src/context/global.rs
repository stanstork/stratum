use crate::{adapter::Adapter, error::MigrationError};
use smql_v02::{plan::MigrationPlan, statements::connection::DataFormat};

#[derive(Clone)]
/// Represents the global context for a migration process.
pub struct GlobalContext {
    pub src_format: DataFormat,
    pub src_adapter: Adapter,

    pub dest_format: DataFormat,
    pub dest_adapter: Adapter,

    /// Batch size for data processing
    pub batch_size: usize,
}

impl GlobalContext {
    pub async fn new(plan: &MigrationPlan) -> Result<Self, MigrationError> {
        let src_format = plan.connections.source.format;
        let src_adapter = Adapter::new(
            plan.connections.source.format,
            &plan.connections.source.conn_str,
        )
        .await?;

        let dest_format = plan.connections.dest.format;
        let dest_adapter = Adapter::new(
            plan.connections.dest.format,
            &plan.connections.dest.conn_str,
        )
        .await?;
        let batch_size = plan.migration.settings.batch_size;

        Ok(GlobalContext {
            src_format,
            src_adapter,
            dest_format,
            dest_adapter,
            batch_size,
        })
    }
}
