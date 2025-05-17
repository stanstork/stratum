use crate::{adapter::Adapter, error::MigrationError};
use smql::{
    plan::MigrationPlan,
    statements::{
        connection::{ConnectionPair, DataFormat},
        setting::Settings,
    },
};

#[derive(Clone)]
/// Represents the global context for a migration process.
pub struct GlobalContext {
    pub src_conn: Option<Adapter>,
    pub dst_conn: Option<Adapter>,

    /// Batch size for data processing
    pub batch_size: usize,
}

impl GlobalContext {
    pub async fn new(plan: &MigrationPlan) -> Result<Self, MigrationError> {
        let src_conn = Self::create_adapter(&plan.connections.source).await?;
        let dst_conn = Self::create_adapter(&plan.connections.dest).await?;
        let batch_size = plan.migration.settings.batch_size;

        Ok(GlobalContext {
            src_conn,
            dst_conn,
            batch_size,
        })
    }

    async fn create_adapter(
        conn: &Option<ConnectionPair>,
    ) -> Result<Option<Adapter>, MigrationError> {
        if let Some(c) = conn {
            // build the adapter and wrap it in Some(...)
            Ok(Some(Adapter::new_sql(c.format, &c.conn_str).await?))
        } else {
            Ok(None)
        }
    }
}
