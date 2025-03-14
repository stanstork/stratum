use crate::{
    buffer::RecordBuffer,
    destination::data_dest::DataDestination,
    source::{data_source::DataSource, record::DataRecord},
    state::MigrationState,
};
use smql::statements::connection::DataFormat;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct MigrationContext {
    pub state: Arc<Mutex<MigrationState>>,
    pub source: Arc<dyn DataSource<Record = Box<dyn DataRecord + Send + Sync>>>,
    pub destination: Arc<dyn DataDestination<Record = Box<dyn DataRecord + Send + Sync>>>,
    pub buffer: Arc<RecordBuffer>,
    pub source_data_format: DataFormat,
    pub destination_data_format: DataFormat,
}
