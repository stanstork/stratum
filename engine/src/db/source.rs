use super::conn::DbConnection;
use async_trait::async_trait;
use futures::TryStreamExt;
use sqlx::types::BigDecimal;
use sqlx::{Column, Error, MySql, Pool, Row, TypeInfo};

#[async_trait]
pub trait DataSource {
    async fn fetch_data(&self, query: &str) -> Result<Vec<String>, sqlx::Error>;
}

pub struct MySqlDataSource {
    pool: Pool<MySql>,
}

impl MySqlDataSource {
    pub async fn new(url: &str) -> Result<Self, Error> {
        let pool = DbConnection::connect(url).await?;
        Ok(Self { pool })
    }
}

#[async_trait]
impl DataSource for MySqlDataSource {
    async fn fetch_data(&self, query: &str) -> Result<Vec<String>, Error> {
        let mut rows = sqlx::query(query).fetch(&self.pool);
        let mut data = Vec::new();

        while let Some(row) = rows.try_next().await? {
            let columns = row.columns();
            let mut row_string = Vec::new();
            for col in columns.iter() {
                let col_type = col.type_info();
                let type_name = col_type.name();

                match type_name {
                    "INT" => {
                        let value: i32 = row.get::<i32, _>(col.name());
                        row_string.push(value.to_string());
                    }
                    "VARCHAR" => {
                        let value: String = row.get::<String, _>(col.name());
                        row_string.push(value);
                    }
                    "DECIMAL" | "NUMERIC" => {
                        let value: BigDecimal = row.get::<BigDecimal, _>(col.name());
                        row_string.push(value.to_string());
                    }
                    _ => {
                        row_string.push("Unknown".to_string());
                    }
                }
            }

            data.push(row_string.join(", "));
        }

        Ok(data)
    }
}
