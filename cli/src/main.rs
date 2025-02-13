use clap::Parser;
use engine::{
    config::config::read_config,
    source::{datasource::DataSource, mysql::MySqlDataSource},
};

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
    let data = mysql_source.fetch_data().await?;

    println!("{:#?}", data);

    Ok(())
}
