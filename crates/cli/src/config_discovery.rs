use std::path::PathBuf;
use tracing::debug;

/// Discovers the config file path by searching in multiple locations
///
/// Search order:
/// 1. Current working directory: stratum.smql
/// 2. Current working directory: .stratum.smql
/// 3. Current working directory: config/stratum.smql
/// 4. User home directory: ~/.stratum/stratum.smql
/// 5. User home directory: ~/.config/stratum/stratum.smql
pub fn discover_config() -> Option<PathBuf> {
    let search_paths = get_search_paths();

    for path in search_paths {
        debug!("Checking for config file at: {}", path.display());
        if path.exists() && path.is_file() {
            debug!("Found config file at: {}", path.display());
            return Some(path);
        }
    }

    debug!("No config file found in any of the search paths");
    None
}

fn get_search_paths() -> Vec<PathBuf> {
    let mut paths = Vec::new();

    // Current working directory variations
    paths.push(PathBuf::from("stratum.smql"));
    paths.push(PathBuf::from(".stratum.smql"));
    paths.push(PathBuf::from("config/stratum.smql"));

    // Home directory variations
    if let Some(home) = home_dir() {
        paths.push(home.join(".stratum").join("stratum.smql"));
        paths.push(home.join(".config").join("stratum").join("stratum.smql"));
    }

    paths
}

/// Gets the user's home directory
fn home_dir() -> Option<PathBuf> {
    #[cfg(target_os = "windows")]
    {
        std::env::var("USERPROFILE").ok().map(PathBuf::from)
    }
    #[cfg(not(target_os = "windows"))]
    {
        std::env::var("HOME").ok().map(PathBuf::from)
    }
}

pub fn display_search_paths() -> String {
    let paths = get_search_paths();
    let mut output = String::from("Config file search order:\n");

    for (i, path) in paths.iter().enumerate() {
        output.push_str(&format!("  {}. {}\n", i + 1, path.display()));
    }

    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_search_paths_not_empty() {
        let paths = get_search_paths();
        assert!(!paths.is_empty());
        assert!(paths.len() >= 3); // At least CWD paths
    }

    #[test]
    fn test_search_paths_include_cwd() {
        let paths = get_search_paths();
        assert!(paths.contains(&PathBuf::from("stratum.smql")));
        assert!(paths.contains(&PathBuf::from(".stratum.smql")));
        assert!(paths.contains(&PathBuf::from("config/stratum.smql")));
    }

    #[test]
    fn test_home_dir_exists() {
        // Just verify this doesn't panic
        let _ = home_dir();
    }

    #[test]
    fn test_display_search_paths() {
        let output = display_search_paths();
        assert!(output.contains("Config file search order:"));
        assert!(output.contains("stratum.smql"));
    }
}
