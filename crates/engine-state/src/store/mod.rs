use crate::error::StateStoreError;
use std::path::{Path, PathBuf};

pub mod live;
pub mod merkle;
pub mod state;

/// Page-cache ceiling for the state store.
const CACHE_CAPACITY_BYTES: u64 = 128 * 1024 * 1024; // 128 MiB

/// Storage-layer failure (I/O, the embedded database, the row-hash log).
#[inline]
pub(super) fn to_storage(e: impl std::fmt::Display) -> StateStoreError {
    StateStoreError::Storage(e.to_string())
}

/// Whether this open failed because another process holds the database lock.
fn is_under_lock(e: &sled::Error) -> bool {
    match e {
        sled::Error::Io(io) => {
            io.kind() == std::io::ErrorKind::WouldBlock
                || io.to_string().contains("could not acquire lock")
        }
        _ => false,
    }
}

/// Encoding or decoding failure for a stored value.
#[inline]
pub(super) fn to_ser(e: impl std::fmt::Display) -> StateStoreError {
    StateStoreError::Serialization(e.to_string())
}

/// Receipts, checkpoints, WAL, and run state: small records, read by key.
pub struct SledStateStore {
    pub(super) db: sled::Db,
    pub(super) dir: PathBuf,
}

impl SledStateStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, sled::Error> {
        let path = path.as_ref();
        let db = sled::Config::new()
            .path(path)
            .cache_capacity(CACHE_CAPACITY_BYTES)
            .open()?;

        Ok(Self {
            db,
            dir: path.to_path_buf(),
        })
    }

    /// Open the store, or return `None` when another process holds the lock.
    pub fn try_open(path: impl AsRef<Path>) -> Result<Option<Self>, sled::Error> {
        match Self::open(path) {
            Ok(store) => Ok(Some(store)),
            Err(e) if is_under_lock(&e) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Directory backing this store.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Helper to generate consistent keys for checkpoints
    #[inline]
    fn chk_key(run_id: &str, item_id: &str, part_id: &str) -> String {
        format!("chk:{}:{}:{}", run_id, item_id, part_id)
    }
}
