use crate::db::{
    col::ColumnMetadata,
    conn::DbConnection,
    mapping::TableMapping,
    metadata::{mysql::MySqlMetadataProvider, DbMetadataProvider},
};
use sqlx::{Column, MySql, Pool, Row};

#[derive(Debug)]
pub struct TableMetadata {
    pub name: String,
    pub columns: Vec<ColumnMetadata>,
    pub primary_key: Vec<String>,
    pub foreign_keys: Vec<ForeignKeyMetadata>,
}

#[derive(Debug)]
pub struct ForeignKeyMetadata {
    pub column: String,
    pub foreign_table: String,
    pub foreign_column: String,
}

impl TableMetadata {
    pub async fn from_mapping(mapping: TableMapping, conn: &str) -> Result<Self, sqlx::Error> {
        println!("{:?}", mapping);

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

        let metadata_provider = MySqlMetadataProvider::new(pool);
        let primary_key = metadata_provider.get_primary_key(&mapping.table).await?;
        let foreign_keys = metadata_provider.get_foreign_keys(&mapping.table).await?;

        Ok(Self {
            name: mapping.table,
            columns,
            primary_key,
            foreign_keys,
        })
    }
}
