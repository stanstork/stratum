use serde::Serialize;

/// Duration estimate with min/likely/max range.
/// Calculated based on row counts, transformations, and historical data.
#[derive(Serialize, Debug, Clone, Default)]
pub struct DurationEstimate {
    /// Minimum expected duration (optimistic)
    pub min_seconds: u64,

    /// Most likely duration
    pub likely_seconds: u64,

    /// Maximum expected duration (pessimistic)
    pub max_seconds: u64,

    /// Human-readable format (e.g., "5m", "2h 30m")
    pub formatted: String,

    /// Whether this estimate is backed by measured calibration for the write path(s) involved.
    pub calibrated: bool,
}

impl DurationEstimate {
    /// Build from a likely duration, widening the min/max band when the estimate
    /// is not yet calibrated.
    pub fn from_seconds(likely: u64, calibrated: bool) -> Self {
        // Calibrated: a tight +/-. Uncalibrated: an order-of-magnitude spread.
        let (lo, hi) = if calibrated { (0.7, 1.5) } else { (0.3, 3.0) };

        Self {
            min_seconds: (likely as f64 * lo) as u64,
            likely_seconds: likely,
            max_seconds: (likely as f64 * hi) as u64,
            formatted: Self::format_duration(likely),
            calibrated,
        }
    }

    fn format_duration(seconds: u64) -> String {
        if seconds < 60 {
            format!("{}s", seconds)
        } else if seconds < 3600 {
            format!("~{}m", seconds / 60)
        } else {
            let hours = seconds / 3600;
            let mins = (seconds % 3600) / 60;
            format!("~{}h {}m", hours, mins)
        }
    }

    /// Take max of estimates (for parallel execution).
    pub fn max_of(estimates: &[DurationEstimate]) -> Self {
        if estimates.is_empty() {
            return Self::from_seconds(0, false);
        }

        let min = estimates.iter().map(|e| e.min_seconds).max().unwrap_or(0);
        let likely = estimates
            .iter()
            .map(|e| e.likely_seconds)
            .max()
            .unwrap_or(0);
        let max = estimates.iter().map(|e| e.max_seconds).max().unwrap_or(0);

        Self {
            min_seconds: min,
            likely_seconds: likely,
            max_seconds: max,
            formatted: Self::format_duration(likely),
            calibrated: estimates.iter().all(|e| e.calibrated),
        }
    }

    /// Combine multiple estimates (sum for sequential execution).
    pub fn combine(estimates: &[DurationEstimate]) -> Self {
        if estimates.is_empty() {
            return Self::from_seconds(0, false);
        }

        let min: u64 = estimates.iter().map(|e| e.min_seconds).sum();
        let likely: u64 = estimates.iter().map(|e| e.likely_seconds).sum();
        let max: u64 = estimates.iter().map(|e| e.max_seconds).sum();

        Self {
            min_seconds: min,
            likely_seconds: likely,
            max_seconds: max,
            formatted: Self::format_duration(likely),
            calibrated: estimates.iter().all(|e| e.calibrated),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uncalibrated_band_is_wider() {
        let calibrated = DurationEstimate::from_seconds(100, true);
        assert_eq!((calibrated.min_seconds, calibrated.max_seconds), (70, 150));

        let rough = DurationEstimate::from_seconds(100, false);
        assert_eq!((rough.min_seconds, rough.max_seconds), (30, 300));

        assert!(calibrated.calibrated);
        assert!(!rough.calibrated);
    }

    #[test]
    fn combined_is_calibrated_only_if_all_inputs_are() {
        let a = DurationEstimate::from_seconds(10, true);
        let b = DurationEstimate::from_seconds(5, false);

        assert!(DurationEstimate::combine(&[a.clone(), a.clone()]).calibrated);
        assert!(!DurationEstimate::combine(&[a.clone(), b.clone()]).calibrated);
        assert!(!DurationEstimate::max_of(&[a, b]).calibrated);
    }
}
