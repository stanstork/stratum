use engine_core::schema::type_registry::Dialect;
use std::sync::Arc;

/// Represents a database endpoint (source or destination) with its metadata.
#[derive(Clone)]
pub struct Endpoint<D> {
    pub driver: Arc<D>,
    pub name: String,
    pub dialect: Dialect,
}

impl<D> Endpoint<D> {
    pub fn new(driver: Arc<D>, name: impl Into<String>, dialect: Dialect) -> Self {
        Self {
            driver,
            name: name.into(),
            dialect,
        }
    }
}
