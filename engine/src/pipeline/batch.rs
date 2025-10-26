use crate::{destination::Destination, source::Source, state::StateStore};
use common::row_data::RowData;
use std::sync::{
    atomic::{AtomicU64, AtomicU8},
    Arc,
};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

pub struct BatchContext {
    pub id: String,         // deterministic: hash(run_id, item_id, part_id, next_offset)
    pub rows: Vec<RowData>, // transformed rows (ready for sink)
    pub next_offset: usize, // source offset to resume after this batch
}

pub struct Manifest {
    pub row_count: usize,
    pub null_count: Vec<[u32; 8]>, // per-column optional
    pub checksum: u64,             // simple checksum of all data in the batch
}

pub struct PartCtx {
    pub run_id: String,
    pub item_id: String,
    pub part_id: String,
    pub dry_run: bool,
    pub cancel: CancellationToken,
    pub state: Arc<dyn StateStore>,
    pub source: Source,
    pub destination: Destination,
    pub progress: Arc<Progress>,
}

pub struct Progress {
    pub stage: AtomicU8, // 0=idle 1=read 2=write 3=commit 4=validated
    pub last_batch_id: Mutex<Option<String>>,
    pub rows_done: AtomicU64,
    pub last_heartbeat: AtomicU64, // unix timestamp in seconds
}
