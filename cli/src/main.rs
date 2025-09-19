use crate::error::CliError;
use clap::Parser;
use commands::Commands;
use engine::{
    conn::{ConnectionKind, ConnectionPinger, MySqlConnectionPinger, PostgresConnectionPinger},
    runner,
};
use smql::plan::MigrationPlan;
use std::str::FromStr;
use tracing::{info, Level};

pub mod commands;
pub mod error;
pub mod output;

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
            runner::run(plan, false).await?;
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
            let states = runner::run(plan, true).await?;

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
                let metadata = runner::load_src_metadata(&conn_str, kind.data_format()).await?;

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
            let plan = smql::parser::parse(&source)?;
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
        let plan = smql::parser::parse(&source)?;
        Ok(plan)
    }
}
