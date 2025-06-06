use clap::Parser;
use commands::Commands;
use engine::runner;
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

            runner::run(plan).await?;
        }
        Commands::Source { command } => match command {
            commands::SourceCommand::Info { config, verbose: _ } => {
                let source = read_migration_config(&config).expect("Failed to read config file");
                let plan = smql::parser::parse(&source).expect("Failed to parse config file");
                let metadata = runner::load_src_metadata(&plan).await?;

                for (name, meta) in metadata {
                    println!("Source: {} - Metadata: {:#?}", name, meta);
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
    }

    Ok(())
}

fn read_migration_config(path: &str) -> Result<String, Box<dyn std::error::Error>> {
    let config = std::fs::read_to_string(path)?;
    Ok(config)
}
