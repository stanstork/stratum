use serde::{Deserialize, Serialize};
use sled::{Db, IVec};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug)]
pub struct StoredRecord {
    pub id: String,
    pub data: Vec<u8>,
}

#[derive(Clone)]
pub struct RecordBuffer {
    db: Db,
}

impl RecordBuffer {
    pub fn new(path: &str) -> Self {
        let db = sled::open(path).expect("Failed to open database");
        RecordBuffer { db }
    }

    pub fn store(&self, record: Vec<u8>) -> sled::Result<()> {
        let key = Uuid::new_v4().to_string();
        self.db.insert(key, IVec::from(record))?;
        self.db.flush()?; // Ensure data is written to disk
        Ok(())
    }

    pub fn read_next(&self) -> Option<Vec<u8>> {
        let mut iter = self.db.iter();
        if let Some(Ok((key, value))) = iter.next() {
            let _ = self.db.remove(&key); // Remove after reading
            Some(value.to_vec())
        } else {
            None
        }
    }
}
