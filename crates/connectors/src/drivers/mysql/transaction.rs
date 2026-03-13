use crate::{
    drivers::mysql::driver::MySqlDriver,
    error::DriverError,
    traits::transaction::{Transaction, Transactional},
};
use async_trait::async_trait;
use mysql_async::{Conn, prelude::Queryable};

/// MySQL transaction that owns a connection with an active transaction.
/// Uses manual "START TRANSACTION", "COMMIT", and "ROLLBACK" control to ensure proper transaction handling.
pub struct MySqlTransaction {
    conn: Conn,
    committed: bool,
}

impl MySqlTransaction {
    /// Start a new transaction on the given connection.
    pub(crate) async fn begin(mut conn: Conn) -> Result<Self, DriverError> {
        // Use manual transaction control to ensure proper handling of isolation levels and other settings.
        conn.query_drop("START TRANSACTION")
            .await
            .map_err(|e| DriverError::TransactionError(e.to_string()))?;

        Ok(Self {
            conn,
            committed: false,
        })
    }
}

impl Drop for MySqlTransaction {
    fn drop(&mut self) {
        if !self.committed {
            tracing::warn!("MySqlTransaction dropped without commit - will be rolled back");
        }
    }
}

#[async_trait]
impl Transaction for MySqlTransaction {
    async fn commit(mut self: Box<Self>) -> Result<(), DriverError> {
        self.conn
            .query_drop("COMMIT")
            .await
            .map_err(|e| DriverError::TransactionError(e.to_string()))?;
        self.committed = true;
        Ok(())
    }

    async fn rollback(mut self: Box<Self>) -> Result<(), DriverError> {
        self.conn
            .query_drop("ROLLBACK")
            .await
            .map_err(|e| DriverError::TransactionError(e.to_string()))?;
        self.committed = true; // Prevent warning on drop
        Ok(())
    }
}

#[async_trait]
impl Transactional for MySqlDriver {
    async fn begin(&self) -> Result<Box<dyn Transaction>, DriverError> {
        let conn = self.pool().get_conn().await?;
        let tx = MySqlTransaction::begin(conn).await?;
        Ok(Box::new(tx))
    }
}
