use sqlx::{Pool, Postgres, Row};

pub async fn pg_table_exists(pool: &Pool<Postgres>, table: &str) -> Result<bool, sqlx::Error> {
    let query = "SELECT EXISTS (
        SELECT FROM information_schema.tables
        WHERE  table_schema = 'public'
        AND    table_name   = $1
    )";

    let row = sqlx::query(query).bind(table).fetch_one(pool).await?;
    let exists: bool = row.get(0);
    Ok(exists)
}
