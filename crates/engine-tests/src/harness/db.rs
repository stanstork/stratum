use crate::harness::fixtures;
use mysql_async::{Pool, prelude::Queryable};
use std::fmt;

/// A database engine, used as either end of a migration.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Dbms {
    MySql,
    Postgres,
}

impl Dbms {
    /// PPL `driver` name.
    pub fn driver(self) -> &'static str {
        match self {
            Dbms::MySql => "mysql",
            Dbms::Postgres => "postgres",
        }
    }

    /// Read-only fixture holding the DVD-rental schema.
    pub fn source_url(self) -> &'static str {
        match self {
            Dbms::MySql => fixtures::MYSQL_SOURCE_URL.as_str(),
            Dbms::Postgres => fixtures::PG_SOURCE_URL.as_str(),
        }
    }

    /// Scratch database, emptied before every case.
    pub fn dest_url(self) -> &'static str {
        match self {
            Dbms::MySql => fixtures::MYSQL_DEST_URL.as_str(),
            Dbms::Postgres => fixtures::PG_DEST_URL.as_str(),
        }
    }

    pub fn quote(self, ident: &str) -> String {
        match self {
            Dbms::MySql => format!("`{ident}`"),
            Dbms::Postgres => format!("\"{ident}\""),
        }
    }

    /// Rows in `table` of the given database.
    pub(crate) async fn count(self, url: &str, table: &str) -> i64 {
        let sql = format!("SELECT COUNT(*) FROM {}", self.quote(table));
        self.scalar_i64(url, &sql).await
    }

    /// Single `i64` from a query, e.g. a `COUNT(*)`.
    pub(crate) async fn scalar_i64(self, url: &str, sql: &str) -> i64 {
        match self {
            Dbms::MySql => {
                let pool = Pool::from_url(url).expect("mysql pool");
                let mut conn = pool.get_conn().await.expect("mysql connection");
                conn.query_first::<i64, _>(sql)
                    .await
                    .unwrap_or_else(|e| panic!("mysql query failed: {sql}\n{e}"))
                    .unwrap_or(0)
            }
            Dbms::Postgres => {
                let client = fixtures::connect_client(url)
                    .await
                    .expect("postgres connection");
                client
                    .query_one(sql, &[])
                    .await
                    .unwrap_or_else(|e| panic!("postgres query failed: {sql}\n{e}"))
                    .get(0)
            }
        }
    }

    /// Single optional `String` from a query.
    pub(crate) async fn scalar_string(self, url: &str, sql: &str) -> Option<String> {
        match self {
            Dbms::MySql => {
                let pool = Pool::from_url(url).expect("mysql pool");
                let mut conn = pool.get_conn().await.expect("mysql connection");
                conn.query_first::<String, _>(sql)
                    .await
                    .unwrap_or_else(|e| panic!("mysql query failed: {sql}\n{e}"))
            }
            Dbms::Postgres => {
                let client = fixtures::connect_client(url)
                    .await
                    .expect("postgres connection");
                client
                    .query_opt(sql, &[])
                    .await
                    .unwrap_or_else(|e| panic!("postgres query failed: {sql}\n{e}"))
                    .map(|row| row.get(0))
            }
        }
    }

    /// Run a statement (no result) against either engine - for seeding or
    /// tampering with the destination in write-semantics tests.
    pub(crate) async fn execute(self, url: &str, sql: &str) {
        match self {
            Dbms::MySql => {
                let pool = Pool::from_url(url).expect("mysql pool");
                let mut conn = pool.get_conn().await.expect("mysql connection");
                conn.query_drop(sql)
                    .await
                    .unwrap_or_else(|e| panic!("mysql exec failed: {sql}\n{e}"));
            }
            Dbms::Postgres => {
                let client = fixtures::connect_client(url)
                    .await
                    .expect("postgres connection");
                client
                    .batch_execute(sql)
                    .await
                    .unwrap_or_else(|e| panic!("postgres exec failed: {sql}\n{e}"));
            }
        }
    }

    /// Every value of a single text column.
    pub(crate) async fn strings(self, url: &str, sql: &str) -> Vec<String> {
        match self {
            Dbms::MySql => {
                let pool = Pool::from_url(url).expect("mysql pool");
                let mut conn = pool.get_conn().await.expect("mysql connection");
                conn.query::<String, _>(sql)
                    .await
                    .unwrap_or_else(|e| panic!("mysql query failed: {sql}\n{e}"))
            }
            Dbms::Postgres => {
                let client = fixtures::connect_client(url)
                    .await
                    .expect("postgres connection");
                client
                    .query(sql, &[])
                    .await
                    .unwrap_or_else(|e| panic!("postgres query failed: {sql}\n{e}"))
                    .iter()
                    .map(|row| row.get(0))
                    .collect()
            }
        }
    }

    /// Empty the scratch destination.
    pub(crate) async fn reset_dest(self) {
        match self {
            Dbms::Postgres => fixtures::reset_postgres_schema().await,
            Dbms::MySql => fixtures::reset_mysql_dest().await,
        }
    }
}

impl fmt::Display for Dbms {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.driver())
    }
}
