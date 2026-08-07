use engine_state::{CalibrationData, WriteClass};
use std::sync::Mutex;
use tracing::warn;

/// A run's throughput observations, buffered in memory and written out once at the end.
#[derive(Default)]
pub struct CalibrationRecorder {
    /// (write class, single-lane-equivalent rows/sec) per recorded pipeline.
    observations: Mutex<Vec<(WriteClass, u64)>>,
}

impl CalibrationRecorder {
    /// Only record substantial runs: a tiny or very fast pipeline's rate is
    /// dominated by fixed startup and would poison the moving average.
    const MIN_ROWS: u64 = 10_000;
    const MIN_SECS: f64 = 0.5;

    pub fn new() -> Self {
        Self::default()
    }

    pub fn observe(&self, driver: &str, rows: u64, elapsed_secs: f64, lanes: usize) {
        if rows < Self::MIN_ROWS || elapsed_secs < Self::MIN_SECS {
            return;
        }

        let lane_factor = (lanes.max(1) as f64).sqrt();
        let per_lane = (rows as f64 / elapsed_secs / lane_factor).round() as u64;

        if per_lane == 0 {
            return;
        }

        if let Ok(mut obs) = self.observations.lock() {
            obs.push((WriteClass::from_driver(driver), per_lane));
        }
    }

    pub fn flush(&self) {
        let observations = match self.observations.lock() {
            Ok(obs) if !obs.is_empty() => obs.clone(),
            _ => return,
        };

        let Some(home) = dirs::home_dir() else {
            return;
        };
        let path = CalibrationData::path_for(&home);

        let mut data = CalibrationData::load(&path).unwrap_or_default();
        for (class, throughput) in observations {
            data.record(class, throughput);
        }

        if let Err(error) = data.save(&path) {
            warn!(%error, "failed to persist plan calibration data");
        }
    }
}
