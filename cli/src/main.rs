use clap::Parser;
use engine::{
    config::{self, read_config},
    db::source::{DataSource, MySqlDataSource},
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

    println!("{:#?}", config);

    // let data_source = MySqlDataSource::new(&config.source()).await?;
    // let query = "SELECT * FROM products";
    // let data = data_source.fetch_data(query).await?;

    // for row in data {
    //     println!("{}", row);
    // }

    Ok(())
}
