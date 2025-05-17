use crate::{adapter::Adapter, error::MigrationError};
use smql::{plan::MigrationPlan, statements::connection::ConnectionPair};

#[derive(Clone)]
/// Represents the global context for a migration process.
pub struct GlobalContext {
    pub src_adapter: Option<Adapter>,
    pub dst_adapter: Option<Adapter>,

    /// Batch size for data processing
    pub batch_size: usize,
}

impl GlobalContext {
    pub async fn new(plan: &MigrationPlan) -> Result<Self, MigrationError> {
        let src_adapter = Self::create_adapter(&plan.connections.source).await?;
        let dst_adapter = Self::create_adapter(&plan.connections.dest).await?;
        let batch_size = plan.migration.settings.batch_size;

        Ok(GlobalContext {
            src_adapter,
            dst_adapter,
            batch_size,
        })
    }

    async fn create_adapter(
        conn: &Option<ConnectionPair>,
    ) -> Result<Option<Adapter>, MigrationError> {
        if let Some(c) = conn {
            // build the adapter and wrap it in Some(...)
            Ok(Some(Adapter::new(c.format, &c.conn_str).await?))
        } else {
            Ok(None)
        }
    }
}
