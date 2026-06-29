use crate::{
    drivers::postgres::driver::PgDriver,
    error::DriverError,
    traits::transaction::{Transaction, Transactional},
};
use async_trait::async_trait;
use tokio_postgres::{Client, NoTls};
use tracing::{error, warn};

/// PostgreSQL transaction that owns a dedicated connection.
/// Uses manual BEGIN/COMMIT/ROLLBACK for transaction control.
pub struct PgTransaction {
    client: Client,
    committed: bool,
}

impl PgTransaction {
    /// Start a new transaction on a fresh connection.
    pub async fn begin(url: &str) -> Result<Self, DriverError> {
        let (client, connection) = tokio_postgres::connect(url, NoTls)
            .await
            .map_err(|e| DriverError::ConnectionError(e.to_string()))?;

        // Spawn connection handler
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!(error = %e, "Postgres transaction connection error");
            }
        });

        // Start the transaction
        client
            .execute("BEGIN", &[])
            .await
            .map_err(|e| DriverError::TransactionError(e.to_string()))?;

        Ok(Self {
            client,
            committed: false,
        })
    }
}

impl Drop for PgTransaction {
    fn drop(&mut self) {
        if !self.committed {
            warn!("transaction dropped without commit, rolling back");
        }
    }
}

#[async_trait]
impl Transaction for PgTransaction {
    async fn commit(mut self: Box<Self>) -> Result<(), DriverError> {
        self.client
            .execute("COMMIT", &[])
            .await
            .map_err(|e| DriverError::TransactionError(e.to_string()))?;
        self.committed = true;
        Ok(())
    }

    async fn rollback(mut self: Box<Self>) -> Result<(), DriverError> {
        self.client
            .execute("ROLLBACK", &[])
            .await
            .map_err(|e| DriverError::TransactionError(e.to_string()))?;
        self.committed = true; // Prevent warning on drop
        Ok(())
    }
}

#[async_trait]
impl Transactional for PgDriver {
    async fn begin(&self) -> Result<Box<dyn Transaction>, DriverError> {
        let tx = PgTransaction::begin(self.url()).await?;
        Ok(Box::new(tx))
    }
}
