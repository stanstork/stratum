use super::{mysql_pool, TEST_MYSQL_URL, TEST_PG_URL};
use crate::{runner::run, tests::pg_pool};
use smql::parser::parse;

/// DDL statement to precreate the `actor` table in Postgres for testing various scenarios involving existing tables.
pub const ACTORS_TABLE_DDL: &str = r#"CREATE TABLE actor (
  actor_id SMALLINT PRIMARY KEY,
  first_name VARCHAR(45) NOT NULL,
  last_name VARCHAR(45) NOT NULL,
  last_update TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);"#;

/// The type of database to use for the test
pub enum DbType {
    MySql,
    Postgres,
}

/// Parse & run the SMQL plan, panicking on any error
pub async fn run_smql(template: &str) {
    let smql = templated_smql(template);
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

/// Get the row count of a table in either MySQL or Postgres
/// depending on the `db` parameter
pub async fn get_row_count(table: &str, db: DbType) -> i64 {
    let query = format!("SELECT COUNT(*) FROM {};", table);

    // Use the appropriate database connection based on the `db` parameter
    match db {
        DbType::MySql => {
            let mysql = mysql_pool().await;
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

/// Assert that a Postgres table has exactly `expected` rows
pub async fn assert_row_count(table: &str, expected: i64) {
    let pg = pg_pool().await;
    let query = format!("SELECT COUNT(*) FROM {};", table);
    let (count,): (i64,) = sqlx::query_as(&query).fetch_one(&pg).await.unwrap();
    assert_eq!(count, expected, "row count mismatch for '{}'", table);
}

/// Execute a SQL statement in Postgres, panicking on any error
pub async fn execute(sql: &str) {
    let pg = pg_pool().await;
    sqlx::query(sql).execute(&pg).await.expect("execute SQL");
}

/// Fill in the two `{mysq_url}` / `{pg_url}` placeholders
fn templated_smql(template: &str) -> String {
    template
        .replace("{mysq_url}", TEST_MYSQL_URL)
        .replace("{pg_url}", TEST_PG_URL)
}
