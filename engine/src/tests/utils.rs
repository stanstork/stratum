use super::{mysql_pool, TEST_MYSQL_URL_ORDERS, TEST_MYSQL_URL_SAKILA, TEST_PG_URL};
use crate::{runner::run, tests::pg_pool};
use smql::parser::parse;
use sql_adapter::row::{db_row::DbRow, row_data::RowData};
use sqlx::{mysql::MySqlRow, Row};

/// DDL statement to precreate the `actor` table in Postgres for testing various scenarios involving existing tables.
pub const ACTORS_TABLE_DDL: &str = r#"CREATE TABLE actor (
  actor_id SMALLINT PRIMARY KEY,
  first_name VARCHAR(45) NOT NULL,
  last_name VARCHAR(45) NOT NULL,
  last_update TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);"#;

/// A query that performs a multi-table join and selects specific columns.
/// Primarily used for testing the LOAD statement.
pub const ORDERS_FLAT_JOIN_QUERY: &str = r#"
    SELECT orders.id AS id,
        orders.user_id AS user_id,
        orders.order_date AS order_date,
        orders.total AS total,
        users.email AS user_email,
        order_items.price AS order_price,
        products.name AS product_name
    FROM orders
    INNER JOIN users        ON users.id           = orders.user_id
    INNER JOIN order_items  ON order_items.order_id = orders.id
    INNER JOIN products     ON products.id        = order_items.id
    LIMIT 1000 OFFSET 0
"#;

/// The type of database to use for the test
pub enum DbType {
    MySql,
    Postgres,
}

/// Parse & run the SMQL plan, panicking on any error
pub async fn run_smql(template: &str, source_db: &str) {
    let smql = templated_smql(template, source_db);
    let plan = parse(&smql).expect("parse smql");
    run(plan).await.expect("migration ran");
}

/// Assert that a table exists (or not) in Postgres
pub async fn assert_table_exists(table: &str, should: bool) {
    let pg = pg_pool().await;
    let (exists,): (bool,) = sqlx::query_as(
        r#"
        SELECT EXISTS (
          SELECT 1
            FROM information_schema.tables
           WHERE table_schema='public'
             AND table_name=$1
        );
        "#,
    )
    .bind(table)
    .fetch_one(&pg)
    .await
    .unwrap();
    assert_eq!(
        exists, should,
        "expected table '{}' existence == {}",
        table, should
    );
}

pub async fn assert_column_exists(table: &str, column: &str, should: bool) {
    let pg = pg_pool().await;
    let (exists,): (bool,) = sqlx::query_as(
        r#"
        SELECT EXISTS (
          SELECT 1
            FROM information_schema.columns
           WHERE table_schema='public'
             AND table_name=$1
             AND column_name=$2
        );
        "#,
    )
    .bind(table)
    .bind(column)
    .fetch_one(&pg)
    .await
    .unwrap();
    assert_eq!(
        exists, should,
        "expected column '{}' existence == {}",
        column, should
    );
}

/// Ensure that the row counts of the migrated tables in the source and destination databases are identical
pub async fn assert_row_count(source_table: &str, source_db: &str, dest_table: &str) {
    let source_count = get_row_count(source_table, source_db, DbType::MySql).await;
    let dest_count = get_row_count(dest_table, source_db, DbType::Postgres).await;

    assert_eq!(
        source_count, dest_count,
        "expected row count for table '{}' to be {} but got {}",
        dest_table, source_count, dest_count
    );
}

/// Get the row count of a table in either MySQL or Postgres
/// depending on the `db` parameter
pub async fn get_row_count(table: &str, source_db: &str, db: DbType) -> i64 {
    let query = format!("SELECT COUNT(*) FROM {};", table);

    // Use the appropriate database connection based on the `db` parameter
    match db {
        DbType::MySql => {
            let mysql = mysql_pool(source_db).await;
            let (count,): (i64,) = sqlx::query_as(&query).fetch_one(&mysql).await.unwrap();
            count
        }
        DbType::Postgres => {
            let pg = pg_pool().await;
            let (count,): (i64,) = sqlx::query_as(&query).fetch_one(&pg).await.unwrap();
            count
        }
    }
}

pub async fn get_table_names(db: DbType, source_db: &str) -> Result<Vec<String>, sqlx::Error> {
    match db {
        DbType::MySql => {
            let pool = mysql_pool(source_db).await;
            // SHOW TABLES can return VARBINARY -> decode to Vec<u8> first
            let sql = r#"
                SELECT table_name
                  FROM information_schema.tables
                 WHERE table_schema = DATABASE()
                   AND table_type   = 'BASE TABLE';
            "#;
            let raw_names: Vec<Vec<u8>> = sqlx::query(sql)
                .map(|row: MySqlRow| row.get::<Vec<u8>, _>(0))
                .fetch_all(&pool)
                .await?;

            // Convert each raw Vec<u8> into a String
            let names = raw_names
                .into_iter()
                .map(|bytes| String::from_utf8(bytes).expect("table name was not valid UTF-8"))
                .collect();

            Ok(names)
        }

        DbType::Postgres => {
            let pool = pg_pool().await;
            let names: Vec<String> = sqlx::query_scalar(
                r#"
                SELECT table_name
                  FROM information_schema.tables
                 WHERE table_schema = 'public'
                   AND table_type   = 'BASE TABLE';
                "#,
            )
            .fetch_all(&pool)
            .await?;

            Ok(names)
        }
    }
}

pub async fn get_column_names(
    db: DbType,
    source_db: &str,
    table: &str,
) -> Result<Vec<String>, sqlx::Error> {
    match db {
        DbType::MySql => {
            let pool = mysql_pool(source_db).await;
            let sql = r#"
                SELECT column_name
                  FROM information_schema.columns
                 WHERE table_schema = DATABASE()
                   AND table_name   = ?
            "#;

            // query_scalar will pull out the first column of each row as String
            let names: Vec<String> = sqlx::query_scalar(sql).bind(table).fetch_all(&pool).await?;

            Ok(names)
        }

        DbType::Postgres => {
            let pool = pg_pool().await;
            let sql = r#"
                SELECT column_name
                  FROM information_schema.columns
                 WHERE table_schema = 'public'
                   AND table_name   = $1
            "#;

            let names: Vec<String> = sqlx::query_scalar(sql).bind(table).fetch_all(&pool).await?;

            Ok(names)
        }
    }
}

pub async fn fetch_rows(
    query: &str,
    source_db: &str,
    db: DbType,
) -> Result<Vec<RowData>, sqlx::Error> {
    match db {
        DbType::MySql => {
            let mysql = mysql_pool(source_db).await;
            let rows = sqlx::query(query).fetch_all(&mysql).await?;
            Ok(rows
                .into_iter()
                .map(|row| RowData::from_db_row("source_table", &DbRow::MySqlRow(&row)))
                .collect())
        }
        DbType::Postgres => {
            let pg = pg_pool().await;
            let rows = sqlx::query(query).fetch_all(&pg).await?;
            Ok(rows
                .into_iter()
                .map(|row| RowData::from_db_row("source_table", &DbRow::PostgresRow(&row)))
                .collect())
        }
    }
}

/// Fetch a single cell from the first row of `query`,
/// and return it as a String (panicking if anything is missing).
pub async fn get_cell_as_string(query: &str, schema: &str, db: DbType, column: &str) -> String {
    let rows = fetch_rows(query, schema, db)
        .await
        .expect("fetch_rows failed");
    let row = rows
        .first()
        .unwrap_or_else(|| panic!("no rows returned for query `{}`", query));
    let col = row
        .get(column)
        .unwrap_or_else(|| panic!("column `{}` not found in row", column));
    col.value
        .as_ref()
        .unwrap_or_else(|| panic!("column `{}` was NULL", column))
        .as_string()
        .unwrap_or_else(|| panic!("column `{}` was not a string", column))
}

/// Fetch a single cell from the first row of `query`,
/// and return it as an f64 (panicking if anything is missing).
pub async fn get_cell_as_f64(query: &str, schema: &str, db: DbType, column: &str) -> f64 {
    let rows = fetch_rows(query, schema, db)
        .await
        .expect("fetch_rows failed");
    let row = rows
        .first()
        .unwrap_or_else(|| panic!("no rows returned for query `{}`", query));
    let col = row
        .get(column)
        .unwrap_or_else(|| panic!("column `{}` not found in row", column));
    col.value
        .as_ref()
        .unwrap_or_else(|| panic!("column `{}` was NULL", column))
        .as_f64()
        .unwrap_or_else(|| panic!("column `{}` was not a float", column))
}

/// Execute a SQL statement in Postgres, panicking on any error
pub async fn execute(sql: &str) {
    let pg = pg_pool().await;
    sqlx::query(sql).execute(&pg).await.expect("execute SQL");
}

/// Fill in the two `{mysq_url}` / `{pg_url}` placeholders
fn templated_smql(template: &str, source_db: &str) -> String {
    template
        .replace(
            "{mysq_url}",
            match source_db {
                "sakila" => TEST_MYSQL_URL_SAKILA,
                "orders" => TEST_MYSQL_URL_ORDERS,
                _ => panic!("Unknown source database: {}", source_db),
            },
        )
        .replace("{pg_url}", TEST_PG_URL)
}
