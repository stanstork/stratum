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
}

impl DurationEstimate {
    pub fn from_seconds(likely: u64) -> Self {
        let min = (likely as f64 * 0.7) as u64;
        let max = (likely as f64 * 1.5) as u64;

        Self {
            min_seconds: min,
            likely_seconds: likely,
            max_seconds: max,
            formatted: Self::format_duration(likely),
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

    /// Take max of estimates (for parallel execution)
    pub fn max_of(estimates: &[DurationEstimate]) -> Self {
        if estimates.is_empty() {
            return Self::from_seconds(0);
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
        }
    }

    /// Combine multiple estimates (sum for sequential execution)
    pub fn combine(estimates: &[DurationEstimate]) -> Self {
        if estimates.is_empty() {
            return Self::from_seconds(0);
        }

        let min: u64 = estimates.iter().map(|e| e.min_seconds).sum();
        let likely: u64 = estimates.iter().map(|e| e.likely_seconds).sum();
        let max: u64 = estimates.iter().map(|e| e.max_seconds).sum();

        Self {
            min_seconds: min,
            likely_seconds: likely,
            max_seconds: max,
            formatted: Self::format_duration(likely),
        }
    }
}
