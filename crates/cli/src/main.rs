use crate::{
    conn::{ConnectionKind, ConnectionPinger, MySqlConnectionPinger, PostgresConnectionPinger},
    error::CliError,
};
use clap::Parser;
use commands::Commands;
use engine_core::{
    progress::{ProgressService, ProgressStatus},
    state::{StateStore, sled_store::SledStateStore},
};
use engine_runtime::execution::{executor, source::load_metadata};
use planner::plan::MigrationPlan;
use std::{str::FromStr, sync::Arc};
use tracing::{Level, info};

mod commands;
mod conn;
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
async fn main() -> Result<(), CliError> {
    // Initialize logger
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Migrate { config, from_ast } => {
            let plan = load_migration_plan(&config, from_ast).await?;
            executor::run(plan, false).await?;
        }
        Commands::Validate {
            config,
            from_ast,
            output,
        } => {
            info!(
                "Validating migration config: {}, from_ast: {}, output: {:?}",
                config, from_ast, output
            );

            let plan = load_migration_plan(&config, from_ast).await?;
            let states = executor::run(plan, true).await?;

            match output {
                Some(path) => output::write_report(states, path).await?,
                None => output::print_report(states).await?,
            }
        }
        Commands::Source { command } => match command {
            commands::SourceCommand::Info {
                conn_str,
                format,
                output,
            } => {
                let kind = ConnectionKind::from_str(&format)
                    .map_err(|_| CliError::InvalidConnectionFormat(format.clone()))?;
                let metadata = load_metadata(&conn_str, kind.data_format()).await?;

                let metadata_json =
                    serde_json::to_string_pretty(&metadata).map_err(CliError::JsonSerialize)?;

                if let Some(output_file) = output {
                    std::fs::write(output_file, metadata_json)?;
                } else {
                    println!("{metadata_json}");
                }
            }
        },
        Commands::Ast { config } => {
            let source = tokio::fs::read_to_string(&config).await?;
            let plan = planner::plan::parse(&source)?;
            let json = serde_json::to_string_pretty(&plan).map_err(CliError::JsonSerialize)?;
            println!("{json}");
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
        }
        Commands::Progress { run, item, json } => {
            show_progress(&run, &item, json).await?;
        }
    }

    Ok(())
}

async fn load_migration_plan(path: &str, from_ast: bool) -> Result<MigrationPlan, CliError> {
    let source = tokio::fs::read_to_string(path).await?;
    if from_ast {
        // If `from_ast` is true, read the config file as a pre-parsed AST
        let plan = serde_json::from_str(&source)?;
        Ok(plan)
    } else {
        // Otherwise, read the config file and parse it
        let plan = planner::plan::parse(&source)?;
        Ok(plan)
    }
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
