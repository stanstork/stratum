use clap::Parser;
use commands::Commands;
use engine::{
    config::config::Config,
    destination::postgres::PgDestination,
    source::{
        data_source::{create_data_source, DataSource, DataSourceType},
        record::DataRecord,
    },
};
use smql::parser::parse;
use sql_adapter::db_type::DbType;
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
            producer::run(plan).expect("Failed to run migration");
        }
    }

    Ok(())
}

async fn init_source(
    config: &Config,
) -> Result<Box<dyn DataSource<Record = Box<dyn DataRecord>>>, Box<dyn std::error::Error>> {
    match create_data_source(DataSourceType::Database(DbType::MySql), &config).await {
        Ok(source) => return Ok(source),
        Err(e) => return Err(e),
    }
}

async fn init_destination(config: &Config) -> Result<PgDestination, Box<dyn std::error::Error>> {
    let mapping = config.mappings.first().unwrap();
    let columns = mapping.columns.clone().unwrap();

    match PgDestination::new(config.dest(), mapping.table.clone(), columns).await {
        Ok(dest) => return Ok(dest),
        Err(e) => return Err(e),
    }
}

fn read_migration_config(path: &str) -> Result<String, Box<dyn std::error::Error>> {
    let config = std::fs::read_to_string(path)?;
    Ok(config)
}
