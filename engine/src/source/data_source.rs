use super::linked_source::LinkedSource;
use crate::{adapter::Adapter, filter::filter::Filter};
use mysql::source::MySqlDataSource;
use smql_v02::statements::connection::DataFormat;
use sql_adapter::source::DbDataSource;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone)]
pub enum DataSource {
    Database(Arc<Mutex<dyn DbDataSource>>),
}

impl DataSource {
    pub fn from_adapter(
        format: DataFormat,
        adapter: &Adapter,
        linked: &Option<LinkedSource>,
        filter: &Option<Filter>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        match (format, adapter) {
            // MySQL + MySqlAdapter -> build a MySqlDataSource
            (DataFormat::MySql, Adapter::MySql(mysql_adapter)) => {
                let sql_filter = filter.as_ref().and_then(|f| {
                    if let Filter::Sql(sf) = f {
                        Some(sf.clone())
                    } else {
                        None
                    }
                });
                let join = linked.as_ref().and_then(|ls| {
                    if let LinkedSource::Table(j) = ls {
                        Some(j.clone())
                    } else {
                        None
                    }
                });

                let ds = MySqlDataSource::new(mysql_adapter.clone(), join, sql_filter);
                Ok(DataSource::Database(Arc::new(Mutex::new(ds))))
            }

            // Postgres + PostgresAdapter -> stub for future implementation
            (DataFormat::Postgres, Adapter::Postgres(_pg_adapter)) => {
                // TODO: implement PostgresDataSource
                Err("Postgres data source is not implemented yet".into())
            }

            // Format says MySql but adapter is wrong
            (DataFormat::MySql, _) => Err("Adapter mismatch: expected MySql adapter".into()),

            // Format says Postgres but adapter is wrong
            (DataFormat::Postgres, _) => Err("Adapter mismatch: expected Postgres adapter".into()),

            // Anything else isn’t a SQL format we support
            (fmt, _) => Err(format!("Unsupported data source format: {:?}", fmt).into()),
        }
    }
}
