use smql::statements::connection::DataFormat;
use sql_adapter::{adapter::DbAdapter, mysql::MySqlAdapter, postgres::PgAdapter};

pub enum Adapter {
    MySql(MySqlAdapter),
    Postgres(PgAdapter),
}

pub async fn get_adapter(
    data_format: DataFormat,
    conn_str: &str,
) -> Result<Adapter, Box<dyn std::error::Error>> {
    match data_format {
        DataFormat::MySql => {
            let adapter = MySqlAdapter::connect(conn_str).await?;
            Ok(Adapter::MySql(adapter))
        }
        DataFormat::Postgres => {
            let adapter = PgAdapter::connect(conn_str).await?;
            Ok(Adapter::Postgres(adapter))
        }
        _ => panic!("Unsupported data format"),
    }
}
