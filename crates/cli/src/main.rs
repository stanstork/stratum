use crate::{
    args::Cli, commands::execute_command, env::EnvManager, error::CliError,
    shutdown::ShutdownCoordinator,
};
use clap::Parser;
use engine_infra::shutdown::ShutdownSignal;
use engine_processing::EnvContext;
use std::{process, sync::Arc};
use tracing::info;

mod args;
mod commands;
mod config;
mod env;
mod error;
mod logger;
mod output;
mod pretty_printer;
mod shutdown;
mod tui;
mod version;

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // Initialize logger based on mode (before any logging occurs)
    logger::init(&cli, cli.is_tui_mode(), cli.is_pretty_mode());

    // Run the application and handle exit
    let exit_code = match run(cli).await {
        Ok(()) => 0,
        Err(e) => handle_error(e),
    };

    process::exit(exit_code);
}

/// Main application logic
async fn run(cli: Cli) -> Result<(), CliError> {
    // Set up graceful shutdown
    let shutdown = ShutdownSignal::new();
    let coordinator = ShutdownCoordinator::new(shutdown.cancel.clone(), shutdown.pause.clone());
    coordinator.register_handlers();

    // Initialize environment variables
    let env = init_environment(cli.env_file.as_deref())?;

    // Execute the command
    execute_command(&cli, shutdown, env).await
}

/// Initializes environment variables from file if provided
fn init_environment(env_file: Option<&str>) -> Result<Arc<EnvContext>, CliError> {
    let mut env_manager = EnvManager::new();

    if let Some(path) = env_file {
        info!("Loading environment variables from: {}", path);
        env_manager.load_from_file(path)?;
    }

    let mut env_context = EnvContext::empty();
    for (key, value) in env_manager.all() {
        env_context.set(key.clone(), value.clone());
    }

    Ok(Arc::new(env_context))
}

/// Handles application errors and returns appropriate exit code
fn handle_error(error: CliError) -> i32 {
    match error {
        CliError::ShutdownRequested => {
            info!("Application shutdown gracefully");
            130 // Standard exit code for SIGINT
        }
        CliError::Paused => {
            info!("Migration paused - resume with the same config");
            2
        }
        CliError::UserMessage(msg) => {
            eprintln!("{}", msg);
            1
        }
        _ => {
            tracing::error!("Application error: {}", error);
            1
        }
    }
}
