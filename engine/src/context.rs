use crate::{
    buffer::RecordBuffer,
    destination::data_dest::DataDestination,
    source::{data_source::DataSource, record::DataRecord},
    state::MigrationState,
};
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct MigrationContext {
    pub state: Arc<Mutex<MigrationState>>,
    pub source: Arc<Box<dyn DataSource<Record = Box<dyn DataRecord + Send + Sync>>>>,
    pub destination: Arc<Box<dyn DataDestination<Record = Box<dyn DataRecord + Send + Sync>>>>,
    pub buffer: Arc<RecordBuffer>,
}
