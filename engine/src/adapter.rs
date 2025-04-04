use mysql::mysql::MySqlAdapter;
use postgres::postgres::PgAdapter;
use smql::statements::connection::DataFormat;
use sql_adapter::adapter::SqlAdapter;

pub enum Adapter {
    MySql(MySqlAdapter),
    Postgres(PgAdapter),
}

pub async fn get_adapter(
    data_format: DataFormat,
    con_str: &str,
) -> Result<Adapter, Box<dyn std::error::Error>> {
    match data_format {
        DataFormat::MySql => {
            let adapter = MySqlAdapter::connect(con_str).await?;
            Ok(Adapter::MySql(adapter))
        }
        DataFormat::Postgres => {
            let adapter = PgAdapter::connect(con_str).await?;
            Ok(Adapter::Postgres(adapter))
        }
        _ => panic!("Unsupported data format"),
    }
}
