use crate::{
    buffer::RecordBuffer, destination::data_dest::DataDestination, source::data_source::DataSource,
    state::MigrationState,
};
use smql::statements::connection::DataFormat;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct MigrationContext {
    pub state: Arc<Mutex<MigrationState>>,
    pub source: DataSource,
    pub destination: DataDestination,
    pub buffer: Arc<RecordBuffer>,
    pub source_data_format: DataFormat,
    pub destination_data_format: DataFormat,
}
