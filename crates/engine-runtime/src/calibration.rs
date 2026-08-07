use engine_state::{CalibrationData, WriteClass};
use std::sync::Mutex;
use tracing::{debug, info, warn};

/// A run's throughput observations, buffered in memory and written out once at the end.
#[derive(Default)]
pub struct CalibrationRecorder {
    /// (write class, single-lane-equivalent rows/sec) per recorded pipeline.
    observations: Mutex<Vec<(WriteClass, u64)>>,
}

impl CalibrationRecorder {
    /// Skip pipelines too small to yield a meaningful rate.
    const MIN_ROWS: u64 = 10_000;

    /// Guard against dividing by a near-zero duration.
    const MIN_SECS: f64 = 0.005;

    pub fn new() -> Self {
        Self::default()
    }

    pub fn observe(&self, driver: &str, rows: u64, elapsed_secs: f64, lanes: usize) {
        if rows < Self::MIN_ROWS || elapsed_secs < Self::MIN_SECS {
            debug!(
                rows,
                elapsed_secs, "run too small to calibrate plan estimates; skipping"
            );
            return;
        }

        let lane_factor = (lanes.max(1) as f64).sqrt();
        let per_lane = (rows as f64 / elapsed_secs / lane_factor).round() as u64;

        if per_lane == 0 {
            return;
        }

        let class = WriteClass::from_driver(driver);
        debug!(
            ?class,
            rows,
            elapsed_secs,
            per_lane_rows_per_sec = per_lane,
            "calibration observation"
        );
        if let Ok(mut obs) = self.observations.lock() {
            obs.push((class, per_lane));
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
        let count = observations.len();
        for (class, throughput) in observations {
            data.record(class, throughput);
        }

        match data.save(&path) {
            Ok(()) => info!(observations = count, "updated plan-estimate calibration"),
            Err(error) => warn!(%error, "failed to persist plan calibration data"),
        }
    }

    #[cfg(test)]
    fn pending(&self) -> Vec<(WriteClass, u64)> {
        self.observations.lock().unwrap().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skips_runs_below_thresholds() {
        let rec = CalibrationRecorder::new();
        rec.observe("postgres", 5_000, 1.0, 1); // below MIN_ROWS (10k)
        rec.observe("postgres", 50_000, 0.001, 1); // duration below the divide-guard
        assert!(rec.pending().is_empty());
    }

    #[test]
    fn records_substantial_run_normalized_by_lanes() {
        let rec = CalibrationRecorder::new();
        // 400k rows in 1s across 4 lanes -> 400k/sqrt(4) = 200k single-lane.
        rec.observe("postgres", 400_000, 1.0, 4);
        assert_eq!(rec.pending(), vec![(WriteClass::Postgres, 200_000)]);
    }
}
