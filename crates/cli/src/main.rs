use crate::{
    conn::{ConnectionKind, ConnectionPinger, MySqlConnectionPinger, PostgresConnectionPinger},
    env::EnvManager,
    error::CliError,
    shutdown::ShutdownCoordinator,
};
use clap::Parser;
use commands::Commands;
use engine_core::{
    plan::execution::ExecutionPlan,
    progress::{ProgressService, ProgressStatus},
    state::{StateStore, sled_store::SledStateStore},
};
use engine_processing::env_context::{EnvContext, init_env_context};
use engine_runtime::{
    error::MigrationError,
    execution::{executor, source::load_metadata},
};
use smql_syntax::{ast::doc::SmqlDocument, builder::parse};
use std::{process, str::FromStr, sync::Arc};
use tokio_util::sync::CancellationToken;
use tracing::{Level, info};

mod commands;
mod conn;
mod env;
mod error;
mod output;
mod shutdown;

#[derive(Parser)]
#[command(name = "stratum", version = "0.0.1", about = "Data migration tool")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[tokio::main]
async fn main() {
    // Initialize logger
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    let exit_code = match run_cli().await {
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

async fn run_cli() -> Result<(), CliError> {
    let cli = Cli::parse();
    let cancel = CancellationToken::new();
    let shutdown_coordinator = ShutdownCoordinator::new(cancel.clone());

    shutdown_coordinator.register_handlers();

    match cli.command {
        Commands::Migrate {
            config,
            from_ast,
            env_file,
        } => {
            init_env(env_file.as_deref())?;

            let plan = load_migration_plan(&config, from_ast).await?;
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
        Commands::Validate {
            config,
            from_ast,
            output,
            env_file,
        } => {
            info!(
                "Validating migration config: {}, from_ast: {}, output: {:?}",
                config, from_ast, output
            );

            init_env(env_file.as_deref())?;

            let plan = load_migration_plan(&config, from_ast).await?;
            let states = executor::run(plan, true, cancel).await?;

            match output {
                Some(path) => output::write_report(states, path).await?,
                None => output::print_report(states).await?,
            }

            Ok(())
        }
        Commands::Source { command } => match command {
            commands::SourceCommand::Info {
                conn_str,
                format,
                output,
            } => {
                let kind = ConnectionKind::from_str(&format)
                    .map_err(|_| CliError::InvalidConnectionFormat(format.clone()))?;

                let metadata = load_metadata(&conn_str, kind.driver()).await?;

                let metadata_json =
                    serde_json::to_string_pretty(&metadata).map_err(CliError::JsonSerialize)?;

                if let Some(output_file) = output {
                    std::fs::write(output_file, metadata_json)?;
                } else {
                    println!("{metadata_json}");
                }

                Ok(())
            }
        },
        Commands::Ast { config } => {
            let source = tokio::fs::read_to_string(&config).await?;
            let plan = parse(&source)?;
            let json = serde_json::to_string_pretty(&plan).map_err(CliError::JsonSerialize)?;
            println!("{json}");
            Ok(())
        }
        Commands::TestConn { format, conn_str } => {
            let kind = ConnectionKind::from_str(&format)
                .map_err(|_| CliError::InvalidConnectionFormat(format))?;
            match kind {
                ConnectionKind::MySql => {
                    MySqlConnectionPinger { conn_str }.ping().await?;
                }
                ConnectionKind::Postgres => {
                    PostgresConnectionPinger { conn_str }.ping().await?;
                }
                _ => return Err(CliError::UnsupportedConnectionKind),
            }
            Ok(())
        }
        Commands::Progress { run, item, json } => {
            show_progress(&run, &item, json).await?;
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

fn open_state_store() -> Result<Arc<dyn StateStore>, CliError> {
    let home = dirs::home_dir()
        .ok_or_else(|| CliError::Unexpected("Could not determine home directory".into()))?;
    let path = home.join(".stratum/state");
    let store = SledStateStore::open(&path).map_err(|err| {
        CliError::Unexpected(format!(
            "Failed to open state store at {}: {err}",
            path.display()
        ))
    })?;
    Ok(Arc::new(store))
}

async fn show_progress(run: &str, item: &str, as_json: bool) -> Result<(), CliError> {
    let store = open_state_store()?;
    let service = ProgressService::new(store);

    let status = service
        .item_status(run, item)
        .await
        .map_err(|err| CliError::Unexpected(format!("Failed to load progress: {err}")))?;

    if as_json {
        let json = serde_json::to_string_pretty(&status).map_err(CliError::JsonSerialize)?;
        println!("{json}");
    } else {
        print_progress_table(run, item, &status);
    }

    Ok(())
}

fn print_progress_table(run: &str, item: &str, status: &ProgressStatus) {
    println!("Progress for run '{run}' / item '{item}':");
    println!("-----------------------------");
    println!("{:<16} {}", "Stage", status.stage);
    println!("{:<16} {}", "Rows done", status.rows_done);
    println!("{:<16} {:?}", "Last cursor", status.last_cursor);
    let heartbeat = status
        .last_heartbeat
        .map(|ts| ts.to_rfc3339())
        .unwrap_or_else(|| "n/a".to_string());
    println!("{:<16} {}", "Last heartbeat", heartbeat);
}
