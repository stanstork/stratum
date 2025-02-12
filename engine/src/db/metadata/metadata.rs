use crate::db::{
    col::ColumnMetadata,
    conn::DbConnection,
    mapping::TableMapping,
    metadata::{mysql::MySqlMetadataProvider, DatabaseType},
    types::DbType,
};
use sqlx::{Column, MySql, Pool, Row};

pub struct TableMetadata {
    pub name: String,
    pub columns: Vec<ColumnMetadata>,
    pub primary_key: Option<Vec<String>>,
    pub foreign_keys: Vec<ForeignKeyMetadata>,
}

pub struct ForeignKeyMetadata {
    pub column: String,
    pub foreign_table: String,
    pub foreign_column: String,
}

impl TableMetadata {
    pub async fn from_mapping(mapping: TableMapping, conn: &str) -> Result<Self, sqlx::Error> {
        let pool: Pool<MySql> = DbConnection::connect(conn).await?;
        let query = format!("SELECT * FROM {} LIMIT 1", mapping.table);
        let row = sqlx::query(&query).fetch_one(&pool).await?;

        let mut columns = Vec::new();
        for col in row.columns().iter() {
            if mapping.columns.contains_key(col.name()) {
                let column_metadata = ColumnMetadata::from(col);
                columns.push(column_metadata);
            }
        }

        todo!()
    }
}
