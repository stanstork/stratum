use crate::{error::DriverError, traits::driver::Driver};
use async_trait::async_trait;

/// A handle to an active database transaction.
/// The transaction is automatically rolled back if dropped without calling commit().
#[async_trait]
pub trait Transaction: Send {
    /// Commit the transaction, consuming the handle.
    async fn commit(self: Box<Self>) -> Result<(), DriverError>;

    /// Rollback the transaction, consuming the handle.
    async fn rollback(self: Box<Self>) -> Result<(), DriverError>;
}

/// Trait for drivers that support transactions.
#[async_trait]
pub trait Transactional: Driver {
    /// Begin a new transaction.
    /// Returns a boxed transaction handle that can be passed around.
    async fn begin(&self) -> Result<Box<dyn Transaction>, DriverError>;
}
