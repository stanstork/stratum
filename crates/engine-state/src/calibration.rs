use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, error::Error, path::Path, path::PathBuf};

/// Destination write path, the primary determinant of throughput.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum WriteClass {
    /// PostgreSQL destination (binary/text COPY).
    Postgres,
    /// MySQL destination (`LOAD DATA` fast path).
    MySql,
    /// Anything else (e.g. INSERT fallback, plugin sinks).
    Other,
}

impl WriteClass {
    /// Classify from a connection driver string.
    pub fn from_driver(driver: &str) -> Self {
        match driver.to_ascii_lowercase().as_str() {
            "postgres" | "postgresql" => Self::Postgres,
            "mysql" => Self::MySql,
            _ => Self::Other,
        }
    }
}

/// Moving-average throughput for one write class.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct ClassStat {
    /// Exponential moving average of observed throughput (rows/sec).
    pub avg_throughput: u64,
    /// How many runs have contributed.
    pub observation_count: u64,
}

/// Historical throughput, per write class, learned from prior runs.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CalibrationData {
    classes: HashMap<WriteClass, ClassStat>,
    pub updated_at: Option<DateTime<Utc>>,
}

impl CalibrationData {
    const DB_KEY: &'static str = "calibration_v1";

    pub fn path_for(home: &Path) -> PathBuf {
        home.join(".stratum/calibration")
    }

    pub fn load(db_path: impl AsRef<Path>) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let db = sled::open(db_path)?;
        Ok(match db.get(Self::DB_KEY)? {
            Some(bytes) => bincode::deserialize(&bytes).unwrap_or_default(),
            None => Self::default(),
        })
    }

    pub fn save(&self, db_path: impl AsRef<Path>) -> Result<(), Box<dyn Error + Send + Sync>> {
        let db = sled::open(db_path)?;
        db.insert(Self::DB_KEY, bincode::serialize(self)?)?;
        db.flush()?;
        Ok(())
    }

    pub fn record(&mut self, class: WriteClass, throughput: u64) {
        let stat = self.classes.entry(class).or_default();

        stat.avg_throughput = if stat.observation_count == 0 {
            throughput
        } else {
            (stat.avg_throughput * 9 + throughput) / 10
        };

        stat.observation_count += 1;
        self.updated_at = Some(Utc::now());
    }

    pub fn throughput_for(&self, class: WriteClass) -> Option<u64> {
        self.classes
            .get(&class)
            .filter(|s| s.observation_count > 0 && s.avg_throughput > 0)
            .map(|s| s.avg_throughput)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn from_driver_classifies() {
        assert_eq!(WriteClass::from_driver("postgres"), WriteClass::Postgres);
        assert_eq!(WriteClass::from_driver("PostgreSQL"), WriteClass::Postgres);
        assert_eq!(WriteClass::from_driver("mysql"), WriteClass::MySql);
        assert_eq!(WriteClass::from_driver("csv"), WriteClass::Other);
    }

    #[test]
    fn first_observation_seeds_then_ewma() {
        let mut data = CalibrationData::default();
        assert_eq!(data.throughput_for(WriteClass::Postgres), None);

        data.record(WriteClass::Postgres, 400_000);
        assert_eq!(data.throughput_for(WriteClass::Postgres), Some(400_000));

        // (400000*9 + 500000)/10 = 410000
        data.record(WriteClass::Postgres, 500_000);
        assert_eq!(data.throughput_for(WriteClass::Postgres), Some(410_000));

        // Classes are independent.
        assert_eq!(data.throughput_for(WriteClass::MySql), None);
    }

    #[test]
    fn save_and_load_round_trip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("calibration");

        let mut data = CalibrationData::default();
        data.record(WriteClass::MySql, 120_000);
        data.save(&path).unwrap();

        let loaded = CalibrationData::load(&path).unwrap();
        assert_eq!(loaded.throughput_for(WriteClass::MySql), Some(120_000));
        assert_eq!(loaded.throughput_for(WriteClass::Postgres), None);
    }
}
