use super::linked_source::LinkedSource;
use crate::{adapter::Adapter, filter::filter::Filter};
use mysql::source::MySqlDataSource;
use smql::statements::connection::DataFormat;
use sql_adapter::{join::source::JoinSource, source::DbDataSource};
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
        linked: &Vec<LinkedSource>,
        filter: &Option<Filter>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        match format {
            DataFormat::MySql => match adapter {
                Adapter::MySql(mysql_adapter) => {
                    let sql_filter = if let Some(filter) = filter {
                        match filter {
                            Filter::Sql(sql) => {
                                // Convert the filter to a SQL filter
                                Some(sql.clone())
                            }
                        }
                    } else {
                        None
                    };

                    let source = MySqlDataSource::new(
                        mysql_adapter.clone(),
                        linked_joins(linked),
                        sql_filter,
                    );
                    Ok(DataSource::Database(Arc::new(Mutex::new(source))))
                }
                _ => Err("Expected MySql adapter, but got a different type".into()),
            },
            DataFormat::Postgres => {
                // Add once implemented
                Err("Postgres data source is not implemented yet".into())
            }
            other => Err(format!("Unsupported data source format: {:?}", other).into()),
        }
    }
}

// Filter out linked sources that are not tables
// and return only the join sources
fn linked_joins(linked: &[LinkedSource]) -> Vec<JoinSource> {
    linked
        .iter()
        .filter_map(|ls| {
            if let LinkedSource::Table(j) = ls {
                Some(j.clone())
            } else {
                None
            }
        })
        .collect()
}
