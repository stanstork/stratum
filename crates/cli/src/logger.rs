use crate::Cli;
use tracing::{Level, info};
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

/// Initializes the tracing logger based on CLI configuration and mode
pub fn init(cli: &Cli, is_tui_mode: bool, is_pretty_mode: bool) {
    let log_level = determine_log_level(cli);
    let env_filter = create_env_filter(log_level);

    if is_tui_mode {
        init_tui_logger(cli, env_filter);
    } else if is_pretty_mode {
        init_pretty_logger(cli, env_filter);
    } else if cli.log_file.is_some() {
        init_dual_logger(cli, env_filter);
    } else {
        init_stdout_logger(cli, env_filter);
    }
}

/// Determines the log level based on CLI arguments and environment
fn determine_log_level(cli: &Cli) -> Level {
    // Priority order:
    // 1. --log-level CLI argument
    // 2. STRATUM_LOG_LEVEL environment variable
    // 3. --quiet flag
    // 4. --verbose flag(s)
    // 5. Default to INFO

    if let Some(ref level_str) = cli.log_level {
        return parse_log_level(level_str);
    }

    if let Ok(env_level) = std::env::var("STRATUM_LOG_LEVEL") {
        return parse_log_level(&env_level);
    }

    if cli.quiet {
        return Level::ERROR;
    }

    match cli.verbose {
        0 => Level::INFO,
        1 => Level::DEBUG,
        _ => Level::TRACE,
    }
}

/// Creates an environment filter with the specified log level
fn create_env_filter(level: Level) -> EnvFilter {
    EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(format!("{}", level)))
}

/// Initializes file-only logging for TUI mode
fn init_tui_logger(cli: &Cli, env_filter: EnvFilter) {
    let log_file_path = determine_tui_log_path(cli);

    // Open log file
    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_file_path)
        .unwrap_or_else(|e| panic!("Failed to open log file {}: {}", log_file_path, e));

    // Set up file-only logging (no stdout to avoid interfering with TUI)
    let file_layer = fmt::layer()
        .with_writer(std::sync::Arc::new(file))
        .with_ansi(false); // No colors in file

    tracing_subscriber::registry()
        .with(env_filter)
        .with(file_layer)
        .init();

    // Log where logs are being written (this goes to the file)
    info!(path = %log_file_path, "TUI mode: logging to file");
}

/// Determines the log file path for TUI mode
fn determine_tui_log_path(cli: &Cli) -> String {
    if let Some(ref log_file) = cli.log_file {
        log_file.clone()
    } else {
        // Create .stratum directory if it doesn't exist
        let home_dir = dirs::home_dir().expect("Could not determine home directory");
        let stratum_dir = home_dir.join(".stratum");
        std::fs::create_dir_all(&stratum_dir).expect("Failed to create .stratum directory");
        stratum_dir.join("tui.log").to_string_lossy().to_string()
    }
}

/// Initializes file-only logging for pretty output mode
fn init_pretty_logger(cli: &Cli, env_filter: EnvFilter) {
    let log_file_path = determine_pretty_log_path(cli);

    // Open log file
    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_file_path)
        .unwrap_or_else(|e| panic!("Failed to open log file {}: {}", log_file_path, e));

    // Set up file-only logging (no stdout to avoid interfering with pretty output)
    let file_layer = fmt::layer()
        .with_writer(std::sync::Arc::new(file))
        .with_ansi(false); // No colors in file

    tracing_subscriber::registry()
        .with(env_filter)
        .with(file_layer)
        .init();

    // Log where logs are being written (this goes to the file)
    info!(path = %log_file_path, "pretty output mode: logging to file");
}

/// Determines the log file path for pretty output mode
fn determine_pretty_log_path(cli: &Cli) -> String {
    if let Some(ref log_file) = cli.log_file {
        log_file.clone()
    } else {
        // Create .stratum directory if it doesn't exist
        let home_dir = dirs::home_dir().expect("Could not determine home directory");
        let stratum_dir = home_dir.join(".stratum");
        std::fs::create_dir_all(&stratum_dir).expect("Failed to create .stratum directory");
        stratum_dir.join("pretty.log").to_string_lossy().to_string()
    }
}

/// Initializes dual logging (stdout + file)
fn init_dual_logger(cli: &Cli, env_filter: EnvFilter) {
    let log_file = cli.log_file.as_ref().unwrap();

    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_file)
        .expect("Failed to open log file");

    let file_layer = fmt::layer()
        .with_writer(std::sync::Arc::new(file))
        .with_ansi(false); // No colors in file

    let stdout_layer = fmt::layer()
        .with_writer(std::io::stdout)
        .with_ansi(!cli.no_color);

    tracing_subscriber::registry()
        .with(env_filter)
        .with(file_layer)
        .with(stdout_layer)
        .init();
}

/// Initializes stdout-only logging
fn init_stdout_logger(cli: &Cli, env_filter: EnvFilter) {
    let stdout_layer = fmt::layer()
        .with_writer(std::io::stdout)
        .with_ansi(!cli.no_color);

    tracing_subscriber::registry()
        .with(env_filter)
        .with(stdout_layer)
        .init();
}

/// Parses a log level string into a tracing Level
pub fn parse_log_level(level_str: &str) -> Level {
    match level_str.to_lowercase().as_str() {
        "error" => Level::ERROR,
        "warn" | "warning" => Level::WARN,
        "info" => Level::INFO,
        "debug" => Level::DEBUG,
        "trace" => Level::TRACE,
        _ => {
            eprintln!(
                "Warning: Invalid log level '{}', defaulting to INFO",
                level_str
            );
            Level::INFO
        }
    }
}
