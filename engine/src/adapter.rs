use mysql::adapter::MySqlAdapter;
use postgres::postgres::PgAdapter;
use smql::statements::connection::DataFormat;
use sql_adapter::adapter::SqlAdapter;

pub enum Adapter {
    MySql(MySqlAdapter),
    Postgres(PgAdapter),
}

impl Adapter {
    pub async fn new(
        data_format: DataFormat,
        con_str: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
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

    pub fn get_adapter(&self) -> &dyn SqlAdapter {
        match self {
            Adapter::MySql(adapter) => adapter,
            Adapter::Postgres(adapter) => adapter,
        }
    }
}
