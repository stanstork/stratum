use mysql::adapter::MySqlAdapter;
use postgres::adapter::PgAdapter;
use smql_v02::statements::connection::DataFormat;
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
            DataFormat::MYSQL => {
                let adapter = MySqlAdapter::connect(con_str).await?;
                Ok(Adapter::MySql(adapter))
            }
            DataFormat::POSTGRES => {
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
