use crate::io::sink::Sink;
use async_trait::async_trait;
use connectors::{
    drivers::mysql::driver::MySqlDriver, sql::metadata::table::TableMetadata,
    traits::writer::DataWriter,
};
use engine_core::schema::type_registry::{Dialect, TypeRegistry};
use model::records::Record;
use std::sync::Arc;

pub struct MySqlSink {
    driver: Arc<MySqlDriver>,
    _type_registry: TypeRegistry,
}

impl MySqlSink {
    pub fn new(driver: Arc<MySqlDriver>, source_dialect: Dialect) -> Self {
        let type_registry = TypeRegistry::new(source_dialect, Dialect::MySql);
        Self {
            driver,
            _type_registry: type_registry,
        }
    }
}

#[async_trait]
impl Sink for MySqlSink {
    async fn write_batch(
        &self,
        meta: &TableMetadata,
        rows: &[Record],
    ) -> Result<u64, connectors::error::DriverError> {
        self.driver.write_batch(meta, rows).await
    }
}
