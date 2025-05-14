use super::{mysql_pool, TEST_MYSQL_URL, TEST_PG_URL};
use crate::{runner::run, tests::pg_pool};
use smql::parser::parse;

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

/// Fill in the two `{mysq_url}` / `{pg_url}` placeholders
fn templated_smql(template: &str) -> String {
    template
        .replace("{mysq_url}", TEST_MYSQL_URL)
        .replace("{pg_url}", TEST_PG_URL)
}
