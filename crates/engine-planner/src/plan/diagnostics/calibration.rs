use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::{error::Error, path::Path};

/// Historical calibration data from previous migrations
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CalibrationData {
    /// Average observed throughput (rows/sec)
    pub avg_throughput: u64,

    /// Throughput by pipeline complexity
    pub throughput_by_joins: Vec<(usize, u64)>, // (num_joins, observed_throughput)

    /// Memory usage observations
    pub memory_samples: Vec<MemorySample>,

    /// Last updated
    pub updated_at: DateTime<Utc>,

    /// Total number of observations recorded
    pub observation_count: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MemorySample {
    pub batch_size: usize,
    pub workers: usize,
    pub observed_mb: u64,
}

/// Observation data to add to calibration
#[derive(Clone, Debug)]
pub struct Observation {
    pub throughput: u64,
    pub join_count: usize,
    pub memory_sample: Option<MemorySample>,
}

impl CalibrationData {
    const DB_KEY: &'static str = "calibration_data";

    pub fn load(db_path: impl AsRef<Path>) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let db = sled::open(db_path)?;

        match db.get(Self::DB_KEY)? {
            Some(bytes) => {
                let data: CalibrationData = bincode::deserialize(&bytes)?;
                Ok(data)
            }
            None => Ok(CalibrationData::default()),
        }
    }

    pub fn save(&self, db_path: impl AsRef<Path>) -> Result<(), Box<dyn Error + Send + Sync>> {
        let db = sled::open(db_path)?;
        let bytes = bincode::serialize(self)?;
        db.insert(Self::DB_KEY, bytes)?;
        db.flush()?;
        Ok(())
    }

    pub fn add_observation(&mut self, observation: Observation) {
        self.avg_throughput = if self.observation_count == 0 {
            observation.throughput
        } else {
            // Exponential moving average for throughput
            (self.avg_throughput * 9 + observation.throughput) / 10
        };

        // Add to throughput by joins
        self.throughput_by_joins
            .push((observation.join_count, observation.throughput));
        // Keep only the most recent 100 entries to prevent unbounded growth
        if self.throughput_by_joins.len() > 100 {
            self.throughput_by_joins.remove(0);
        }

        // Add memory sample if provided
        if let Some(mem_sample) = observation.memory_sample {
            self.memory_samples.push(mem_sample);

            // Keep only the most recent 100 samples to prevent unbounded growth
            if self.memory_samples.len() > 100 {
                self.memory_samples.remove(0);
            }
        }

        self.observation_count += 1;
        self.updated_at = Utc::now();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_save_and_load() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("calibration.db");

        // Create and save calibration data
        let mut data = CalibrationData {
            avg_throughput: 5000,
            ..Default::default()
        };
        data.throughput_by_joins.push((2, 3000));
        data.observation_count = 10;

        data.save(&db_path).unwrap();

        // Load and verify
        let loaded = CalibrationData::load(&db_path).unwrap();
        assert_eq!(loaded.avg_throughput, 5000);
        assert_eq!(loaded.throughput_by_joins.len(), 1);
        assert_eq!(loaded.observation_count, 10);
    }

    #[test]
    fn test_add_observation() {
        let mut data = CalibrationData::default();

        // Add first observation
        let obs1 = Observation {
            throughput: 1000,
            join_count: 2,
            memory_sample: Some(MemorySample {
                batch_size: 100,
                workers: 4,
                observed_mb: 256,
            }),
        };
        data.add_observation(obs1);

        assert_eq!(data.observation_count, 1);
        assert_eq!(data.avg_throughput, 1000);
        assert_eq!(data.memory_samples.len(), 1);

        // Add second observation
        let obs2 = Observation {
            throughput: 2000,
            join_count: 2,
            memory_sample: None,
        };
        data.add_observation(obs2);

        assert_eq!(data.observation_count, 2);
        assert_eq!(data.avg_throughput, 1100); // (1000*9 + 2000)/10 
        assert_eq!(data.memory_samples.len(), 1); // Only one memory sample
    }
}
