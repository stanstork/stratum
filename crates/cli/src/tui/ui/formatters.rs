use std::time::Duration;

/// Format a number in compact notation (K, M, B)
pub fn format_compact_number(num: u64) -> String {
    if num >= 1_000_000_000 {
        format!("{:.1}B", num as f64 / 1e9)
    } else if num >= 1_000_000 {
        format!("{:.1}M", num as f64 / 1e6)
    } else if num >= 1_000 {
        format!("{:.1}K", num as f64 / 1e3)
    } else {
        num.to_string()
    }
}

/// Format bytes with appropriate unit (B, KB, MB, GB)
pub fn format_bytes(bytes: u64) -> String {
    if bytes >= 1_073_741_824 {
        format!("{:.2} GB", bytes as f64 / 1_073_741_824.0)
    } else if bytes >= 1_048_576 {
        format!("{:.2} MB", bytes as f64 / 1_048_576.0)
    } else if bytes >= 1024 {
        format!("{:.2} KB", bytes as f64 / 1024.0)
    } else {
        format!("{} B", bytes)
    }
}

/// Format duration in human-readable format
pub fn format_duration(d: Duration) -> String {
    let secs = d.as_secs();
    if secs > 3600 {
        format!("{}h{}m", secs / 3600, (secs % 3600) / 60)
    } else if secs > 60 {
        format!("{}m{}s", secs / 60, secs % 60)
    } else {
        format!("{}s", secs)
    }
}

/// Format a throughput rate
pub fn format_rate(rate: f64) -> String {
    if rate == 0.0 {
        return "--/s".into();
    }
    format!("{}/s", format_compact_number(rate as u64))
}

/// Format row counts (processed/total)
pub fn format_row_counts(processed: u64, total: u64) -> String {
    if total > 0 {
        format!(
            "{}/{}",
            format_compact_number(processed),
            format_compact_number(total)
        )
    } else {
        format_compact_number(processed)
    }
}

/// Format a progress bar with blocks
pub fn format_progress_bar(progress: f64, width: usize) -> String {
    // Clamp progress between 0.0 and 1.0
    let progress = progress.clamp(0.0, 1.0);

    // Calculate filled blocks
    let filled = (progress * width as f64) as usize;
    let empty = width.saturating_sub(filled);

    // Format with fixed 4-character width for percentage (accounts for "100%")
    format!(
        "[{}{}] {:>4.0}%",
        "█".repeat(filled),
        " ".repeat(empty),
        progress * 100.0
    )
}

/// Create a sparkline from data points
pub fn create_sparkline(data: &[u64], width: usize) -> String {
    use crate::tui::ui::constants::SPARKLINE_CHARS;

    if data.is_empty() {
        return " ".repeat(width);
    }

    let max = *data.iter().max().unwrap_or(&1).max(&1);
    let mut result = String::with_capacity(width);
    let start_idx = data.len().saturating_sub(width);

    for i in 0..width {
        let data_idx = start_idx + i;
        if data_idx < data.len() {
            let val = data[data_idx];
            let idx =
                ((val as f64 / max as f64) * (SPARKLINE_CHARS.len() - 1) as f64).round() as usize;
            result.push(SPARKLINE_CHARS[idx]);
        } else {
            result.push(' ');
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_compact_number() {
        assert_eq!(format_compact_number(500), "500");
        assert_eq!(format_compact_number(1_500), "1.5K");
        assert_eq!(format_compact_number(1_500_000), "1.5M");
        assert_eq!(format_compact_number(2_500_000_000), "2.5B");
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(500), "500 B");
        assert_eq!(format_bytes(2048), "2.00 KB");
        assert_eq!(format_bytes(2_097_152), "2.00 MB");
        assert_eq!(format_bytes(2_147_483_648), "2.00 GB");
    }

    #[test]
    fn test_format_duration() {
        assert_eq!(format_duration(Duration::from_secs(45)), "45s");
        assert_eq!(format_duration(Duration::from_secs(125)), "2m5s");
        assert_eq!(format_duration(Duration::from_secs(3665)), "1h1m");
    }

    #[test]
    fn test_format_progress_bar() {
        let bar = format_progress_bar(0.5, 10);
        assert!(bar.contains("[█████     ]"));
        assert!(bar.contains(" 50%"));

        // Test 100%
        let bar_full = format_progress_bar(1.0, 10);
        assert!(bar_full.contains("[██████████]"));
        assert!(bar_full.contains("100%"));

        // Test overflow protection
        let bar_overflow = format_progress_bar(1.5, 10);
        assert!(bar_overflow.contains("100%"));
    }
}
