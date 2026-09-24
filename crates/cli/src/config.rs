use crate::{compile::ensure_plugins_compiled, error::CliError};
use engine_core::{context::env::EnvContext, plan::execution::ExecutionPlan};
use ppl_syntax::ast::doc::PplDocument;
use std::{path::PathBuf, sync::Arc};
use tracing::{debug, info};

/// Resolves config file path from CLI argument, environment, or auto-discovery
pub fn resolve_path(config: Option<String>) -> Result<String, CliError> {
    // Priority order:
    // 1. Explicit --config argument
    // 2. PAGANEL_CONFIG environment variable
    // 3. Auto-discovery

    match config {
        Some(path) => {
            // User explicitly provided a config path via CLI argument
            Ok(path)
        }
        None => {
            // Check environment variable
            if let Ok(env_path) = std::env::var("PAGANEL_CONFIG") {
                info!(path = %env_path, "using config from PAGANEL_CONFIG");
                return Ok(env_path);
            }

            // Try to auto-discover the config file
            match discover_config() {
                Some(path) => {
                    info!(path = %path.display(), "auto-discovered config file");
                    Ok(path.to_string_lossy().to_string())
                }
                None => Err(CliError::ConfigNotFound(display_search_paths())),
            }
        }
    }
}

/// Loads and parses a migration plan from the config file
pub async fn load_plan(
    path: &str,
    from_ast: bool,
    env: Arc<EnvContext>,
) -> Result<ExecutionPlan, CliError> {
    let source = tokio::fs::read_to_string(path).await?;
    let doc: PplDocument = if from_ast {
        // If `from_ast` is true, read the config file as a pre-parsed AST
        serde_json::from_str(&source)?
    } else {
        // Otherwise, read the config file and parse it
        ppl_syntax::builder::parse(&source)?
    };
    let mut plan = ExecutionPlan::build(&doc, env)?;
    plan.config_path = path.to_string();
    // Transparently compile any `.js` plugin sources to WASM (cached).
    ensure_plugins_compiled(&mut plan)?;
    Ok(plan)
}

/// Discovers the config file path by searching in multiple locations
///
/// Search order:
/// 1. Current working directory: paganel.ppl
/// 2. Current working directory: .paganel.ppl
/// 3. Current working directory: config/paganel.ppl
/// 4. User home directory: ~/.paganel/paganel.ppl
/// 5. User home directory: ~/.config/paganel/paganel.ppl
fn discover_config() -> Option<PathBuf> {
    let search_paths = get_search_paths();

    for path in search_paths {
        debug!(path = %path.display(), "checking for config file");
        if path.exists() && path.is_file() {
            debug!(path = %path.display(), "found config file");
            return Some(path);
        }
    }

    debug!("no config file found in search paths");
    None
}

fn get_search_paths() -> Vec<PathBuf> {
    let mut paths = Vec::new();

    // Current working directory variations
    paths.push(PathBuf::from("paganel.ppl"));
    paths.push(PathBuf::from(".paganel.ppl"));
    paths.push(PathBuf::from("config/paganel.ppl"));

    // Home directory variations
    if let Some(home) = home_dir() {
        paths.push(home.join(".paganel").join("paganel.ppl"));
        paths.push(home.join(".config").join("paganel").join("paganel.ppl"));
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

fn display_search_paths() -> String {
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
        assert!(paths.contains(&PathBuf::from("paganel.ppl")));
        assert!(paths.contains(&PathBuf::from(".paganel.ppl")));
        assert!(paths.contains(&PathBuf::from("config/paganel.ppl")));
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
        assert!(output.contains("paganel.ppl"));
    }
}
