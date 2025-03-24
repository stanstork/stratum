use clap::Parser;
use commands::Commands;
use engine::runner;
use smql::parser::parse;
use sql_adapter::metadata::table::TableMetadata;
use std::collections::HashSet;
use tracing::Level;
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
        Commands::Migrate { config } => {
            let source = read_migration_config(&config).expect("Failed to read config file");
            let plan = parse(&source).expect("Failed to parse config file");

            runner::run(plan).await?;
        }
        Commands::Source { command } => match command {
            commands::SourceCommand::Info { config, verbose } => {
                let source = read_migration_config(&config).expect("Failed to read config file");
                let plan = parse(&source).expect("Failed to parse config file");
                let metadata = runner::load_src_metadata(&plan).await?;

                if verbose {
                    println!("{:#?}", metadata);
                } else {
                    let mut visited = HashSet::new();
                    TableMetadata::print_tables_tree(&metadata, 1, &mut visited);
                }
            }
        },
    }

    Ok(())
}

fn read_migration_config(path: &str) -> Result<String, Box<dyn std::error::Error>> {
    let config = std::fs::read_to_string(path)?;
    Ok(config)
}
