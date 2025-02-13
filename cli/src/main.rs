use clap::Parser;
use engine::{config::read_config, database::source::MySqlDataSource};

#[derive(Parser)]
struct Cli {
    #[arg(long, help = "Config file path")]
    config: String,
}

#[tokio::main]
async fn main() -> Result<(), sqlx::Error> {
    let cli = Cli::parse();
    let config = read_config(&cli.config).expect("Failed to read config file");
    let mysql_source = MySqlDataSource::new(config.source(), config.mappings.clone()).await?;

    println!("Metadata: {:#?}", mysql_source.metadata());

    Ok(())
}
