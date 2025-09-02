use clap::Parser;
use commands::Commands;
use engine::{
    conn::{ConnectionKind, ConnectionPinger, MySqlConnectionPinger, PostgresConnectionPinger},
    runner,
};
use std::str::FromStr;
use tracing::{info, Level};

pub mod commands;

#[derive(Parser)]
#[command(name = "stratum", version = "0.0.1", about = "Data migration tool")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logger
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Migrate { config, from_ast } => {
            let plan = if from_ast {
                // If `from_ast` is true, read the config file as a pre-parsed AST
                let source = read_migration_config(&config).expect("Failed to read config file");
                serde_json::from_str(&source)
                    .expect("Failed to deserialize config file into MigrationConfig")
            } else {
                // Otherwise, read the config file and parse it
                let source = read_migration_config(&config).expect("Failed to read config file");
                smql::parser::parse(&source).expect("Failed to parse config file")
            };

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

            let plan = if from_ast {
                // If `from_ast` is true, read the config file as a pre-parsed AST
                let source = read_migration_config(&config).expect("Failed to read config file");
                serde_json::from_str(&source)
                    .expect("Failed to deserialize config file into MigrationConfig")
            } else {
                // Otherwise, read the config file and parse it
                let source = read_migration_config(&config).expect("Failed to read config file");
                smql::parser::parse(&source).expect("Failed to parse config file")
            };

            runner::run(plan, true).await?;

            todo!("Implement validation report output");
        }
        Commands::Source { command } => match command {
            commands::SourceCommand::Info {
                conn_str,
                format,
                output,
            } => {
                let kind =
                    ConnectionKind::from_str(&format).expect("Failed to parse connection kind");
                let metadata = runner::load_src_metadata(&conn_str, kind.data_format())
                    .await
                    .expect("Failed to load source metadata");

                // If an output file is specified, write metadata to it
                if let Some(output_file) = output {
                    std::fs::write(output_file, serde_json::to_string_pretty(&metadata)?)
                        .expect("Failed to write metadata to file");
                } else {
                    // Otherwise, print metadata to stdout
                    println!("{}", serde_json::to_string_pretty(&metadata)?);
                }
            }
        },
        Commands::Ast { config } => {
            let source = read_migration_config(&config).expect("Failed to read config file");
            let plan = smql::parser::parse(&source).expect("Failed to parse config file");
            let json =
                serde_json::to_string_pretty(&plan).expect("Failed to serialize plan to JSON");
            println!("{}", json);
        }
        Commands::TestConn { format, conn_str } => {
            let kind = ConnectionKind::from_str(&format).expect("Failed to parse connection kind");
            match kind {
                ConnectionKind::MySql => {
                    let pinger = MySqlConnectionPinger { conn_str };
                    pinger.ping().await?;
                }
                ConnectionKind::Postgres => {
                    let pinger = PostgresConnectionPinger { conn_str };
                    pinger.ping().await?;
                }
                _ => panic!("Unsupported connection kind for testing"),
            }
        }
    }

    Ok(())
}

fn read_migration_config(path: &str) -> Result<String, Box<dyn std::error::Error>> {
    let config = std::fs::read_to_string(path)?;
    Ok(config)
}
