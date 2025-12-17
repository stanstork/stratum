use crate::{
    config_discovery::discover_config,
    conn::{ConnectionKind, ConnectionPinger, MySqlConnectionPinger, PostgresConnectionPinger},
    env::EnvManager,
    error::CliError,
    shutdown::ShutdownCoordinator,
};
use clap::Parser;
use commands::Commands;
use engine_core::plan::execution::ExecutionPlan;
use engine_processing::env_context::{EnvContext, init_env_context};
use engine_runtime::{error::MigrationError, execution::executor};
use smql_syntax::ast::doc::SmqlDocument;
use std::{process, str::FromStr};
use tokio_util::sync::CancellationToken;
use tracing::{Level, info};

mod commands;
mod config_discovery;
mod conn;
mod env;
mod error;
mod output;
mod shutdown;

fn build_version_string() -> &'static str {
    concat!(
        env!("CARGO_PKG_VERSION"),
        "\nGit commit:  ",
        env!("GIT_HASH"),
        " (",
        env!("GIT_BRANCH"),
        ")",
        "\nBuild date:  ",
        env!("BUILD_TIMESTAMP"),
        "\nRust:        ",
        env!("RUSTC_VERSION")
    )
}

#[derive(Parser)]
#[command(
    name = "stratum",
    version = env!("CARGO_PKG_VERSION"),
    about = "Data migration tool",
    long_version = build_version_string(),
    after_help = "ENVIRONMENT VARIABLES:
  STRATUM_CONFIG      Path to config file (overrides auto-discovery)
  STRATUM_LOG_LEVEL   Log level: error, warn, info, debug, trace"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Load environment variables from file
    #[arg(short = 'e', long, global = true)]
    env_file: Option<String>,

    /// Increase verbosity (-v, -vv)
    #[arg(short, long, action = clap::ArgAction::Count, global = true)]
    verbose: u8,

    /// Suppress non-essential output
    #[arg(short, long, global = true)]
    quiet: bool,

    /// Disable colored output
    #[arg(long, global = true)]
    no_color: bool,

    /// Set log level (error, warn, info, debug, trace)
    #[arg(long, value_name = "LEVEL", global = true)]
    log_level: Option<String>,

    /// Write logs to file
    #[arg(long, value_name = "FILE", global = true)]
    log_file: Option<String>,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // Initialize logger based on global options
    init_logger(&cli);

    let exit_code = match run_cli(cli).await {
        Ok(()) => 0,
        Err(e) => {
            match &e {
                CliError::ShutdownRequested => {
                    info!("Application shutdown gracefully");
                    130 // Standard exit code for SIGINT
                }
                _ => {
                    tracing::error!("Application error: {}", e);
                    1
                }
            }
        }
    };

    process::exit(exit_code);
}

async fn run_cli(cli: Cli) -> Result<(), CliError> {
    let cancel = CancellationToken::new();
    let shutdown_coordinator = ShutdownCoordinator::new(cancel.clone());

    shutdown_coordinator.register_handlers();

    // Initialize environment variables
    init_env(cli.env_file.as_deref())?;

    match cli.command {
        Commands::Plan { config, output } => {
            let config_path = resolve_config_path(config)?;
            info!("Running dry-run migration: {}", config_path);

            let plan = load_migration_plan(&config_path, false).await?;
            // Run in dry-run mode
            let states = executor::run(plan, true, cancel).await?;

            match output {
                Some(path) => output::write_report(states, path).await?,
                None => {
                    if !cli.quiet {
                        output::print_report(states).await?;
                    }
                }
            }

            Ok(())
        }
        Commands::Apply { config } => {
            let config_path = resolve_config_path(config)?;
            info!("Executing migration: {}", config_path);

            let plan = load_migration_plan(&config_path, false).await?;
            let result = executor::run(plan, false, cancel).await;

            match result {
                Ok(_) => {
                    info!("Migration completed successfully");
                    Ok(())
                }
                Err(MigrationError::ShutdownRequested) => {
                    info!("Migration stopped due to shutdown request - progress has been saved");
                    Err(CliError::ShutdownRequested)
                }
                Err(e) => Err(CliError::Migration(e)),
            }
        }
        Commands::Verify { config, output } => {
            let config_path = resolve_config_path(config)?;
            info!("Verifying migrated data: {}", config_path);

            // TODO: Implement post-migration data verification
            // This will verify that migrated data matches source data
            if !cli.quiet {
                println!("Data verification not yet implemented");
            }

            if let Some(ref path) = output {
                info!("Verification report would be written to: {}", path);
            }

            Ok(())
        }
        Commands::TestConn { url, format } => {
            // Determine connection kind from format or URL
            let kind = if let Some(format_str) = format {
                ConnectionKind::from_str(&format_str).map_err(CliError::InvalidConnectionFormat)?
            } else {
                ConnectionKind::from_url(&url).map_err(CliError::InvalidConnectionFormat)?
            };

            info!("Testing {:?} connection to: {}", kind, url);

            // Test the connection based on the kind
            match kind {
                ConnectionKind::MySql => {
                    MySqlConnectionPinger { conn_str: url }.ping().await?;
                    if !cli.quiet {
                        println!("✓ MySQL connection successful");
                    }
                }
                ConnectionKind::Postgres => {
                    PostgresConnectionPinger { conn_str: url }.ping().await?;
                    if !cli.quiet {
                        println!("✓ PostgreSQL connection successful");
                    }
                }
                _ => return Err(CliError::UnsupportedConnectionKind),
            }

            Ok(())
        }
        Commands::Version => {
            print_version_info();
            Ok(())
        }
    }
}

fn init_env(env_file: Option<&str>) -> Result<(), CliError> {
    let mut env_manager = EnvManager::new();

    if let Some(path) = env_file {
        info!("Loading environment variables from: {}", path);
        env_manager.load_from_file(path)?;
    }

    let mut env_context = EnvContext::empty();
    for (key, value) in env_manager.all() {
        env_context.set(key.clone(), value.clone());
    }

    init_env_context(env_context);

    Ok(())
}

fn init_logger(cli: &Cli) {
    use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

    // Determine log level based on priority:
    // 1. --log-level CLI argument
    // 2. STRATUM_LOG_LEVEL environment variable
    // 3. --quiet flag
    // 4. --verbose flag(s)
    // 5. Default to INFO
    let log_level = if let Some(ref level_str) = cli.log_level {
        parse_log_level(level_str)
    } else if let Ok(env_level) = std::env::var("STRATUM_LOG_LEVEL") {
        parse_log_level(&env_level)
    } else if cli.quiet {
        Level::ERROR
    } else {
        match cli.verbose {
            0 => Level::INFO,
            1 => Level::DEBUG,
            _ => Level::TRACE,
        }
    };

    // Create env filter with the determined log level
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(format!("{}", log_level)));

    if let Some(ref log_file) = cli.log_file {
        // Set up dual logging: both stdout and file
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
    } else {
        // Only log to stdout
        let stdout_layer = fmt::layer()
            .with_writer(std::io::stdout)
            .with_ansi(!cli.no_color);

        tracing_subscriber::registry()
            .with(env_filter)
            .with(stdout_layer)
            .init();
    }
}

fn print_version_info() {
    let version = env!("CARGO_PKG_VERSION");
    let git_hash = env!("GIT_HASH");
    let git_branch = env!("GIT_BRANCH");
    let build_timestamp = env!("BUILD_TIMESTAMP");
    let rustc_version = env!("RUSTC_VERSION");

    println!("stratum {}", version);
    println!("Git commit:  {} ({})", git_hash, git_branch);
    println!("Build date:  {}", build_timestamp);
    println!("Rust:        {}", rustc_version);
}

fn parse_log_level(level_str: &str) -> Level {
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

fn resolve_config_path(config: Option<String>) -> Result<String, CliError> {
    // Priority order:
    // 1. Explicit --config argument
    // 2. STRATUM_CONFIG environment variable
    // 3. Auto-discovery

    match config {
        Some(path) => {
            // User explicitly provided a config path via CLI argument
            Ok(path)
        }
        None => {
            // Check environment variable
            if let Ok(env_path) = std::env::var("STRATUM_CONFIG") {
                info!("Using config file from STRATUM_CONFIG: {}", env_path);
                return Ok(env_path);
            }

            // Try to auto-discover the config file
            match discover_config() {
                Some(path) => {
                    info!("Auto-discovered config file at: {}", path.display());
                    Ok(path.to_string_lossy().to_string())
                }
                None => Err(CliError::ConfigNotFound(
                    config_discovery::display_search_paths(),
                )),
            }
        }
    }
}

async fn load_migration_plan(path: &str, from_ast: bool) -> Result<ExecutionPlan, CliError> {
    let source = tokio::fs::read_to_string(path).await?;
    let doc: SmqlDocument = if from_ast {
        // If `from_ast` is true, read the config file as a pre-parsed AST
        serde_json::from_str(&source)?
    } else {
        // Otherwise, read the config file and parse it
        smql_syntax::builder::parse(&source)?
    };
    Ok(ExecutionPlan::build(&doc)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_log_level() {
        assert_eq!(parse_log_level("error").to_string(), "ERROR");
        assert_eq!(parse_log_level("ERROR").to_string(), "ERROR");
        assert_eq!(parse_log_level("warn").to_string(), "WARN");
        assert_eq!(parse_log_level("warning").to_string(), "WARN");
        assert_eq!(parse_log_level("WARN").to_string(), "WARN");
        assert_eq!(parse_log_level("info").to_string(), "INFO");
        assert_eq!(parse_log_level("INFO").to_string(), "INFO");
        assert_eq!(parse_log_level("debug").to_string(), "DEBUG");
        assert_eq!(parse_log_level("DEBUG").to_string(), "DEBUG");
        assert_eq!(parse_log_level("trace").to_string(), "TRACE");
        assert_eq!(parse_log_level("TRACE").to_string(), "TRACE");
    }

    #[test]
    fn test_parse_log_level_invalid() {
        // Invalid levels should default to INFO
        assert_eq!(parse_log_level("invalid").to_string(), "INFO");
        assert_eq!(parse_log_level("").to_string(), "INFO");
        assert_eq!(parse_log_level("foo").to_string(), "INFO");
    }
}
