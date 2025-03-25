use serde::{Deserialize, Serialize};
use sled::{Db, IVec};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug)]
pub struct StoredRecord {
    pub id: String,
    pub data: Vec<u8>,
}

#[derive(Clone)]
pub struct SledBuffer {
    db: Db,
}

impl SledBuffer {
    const OFFSET_KEY: &'static [u8] = b"last_offset";
    const RECORD_PREFIX: &'static str = "rec_";

    pub fn new(path: &str) -> Self {
        let db = sled::open(path).expect("Failed to open database");
        SledBuffer { db }
    }

    pub fn store(&self, record: Vec<u8>) -> sled::Result<()> {
        let key = format!("{}{}", Self::RECORD_PREFIX, Uuid::new_v4());
        self.db.insert(key, IVec::from(record))?;
        self.db.flush()?; // Ensure data is written to disk
        Ok(())
    }

    pub fn store_last_offset(&self, offset: usize) -> sled::Result<()> {
        let bytes = bincode::serialize(&offset).expect("failed to serialize offset");
        self.db.insert(Self::OFFSET_KEY, bytes)?;
        self.db.flush()?; // Flush offset update
        Ok(())
    }

    pub fn read_next(&self) -> Option<Vec<u8>> {
        let iter = self.db.scan_prefix(Self::RECORD_PREFIX);

        for item in iter {
            if let Ok((key, value)) = item {
                let _ = self.db.remove(&key); // Remove after reading
                return Some(value.to_vec());
            }
        }

        None
    }

    pub fn read_last_offset(&self) -> usize {
        self.db
            .get(Self::OFFSET_KEY)
            .ok()
            .flatten()
            .and_then(|ivec| bincode::deserialize::<usize>(&ivec).ok())
            .unwrap_or(0)
    }
}
