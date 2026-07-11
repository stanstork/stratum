use crate::harness::{db::Dbms, runner, smql};
use std::fmt;

/// One source -> destination pairing.
#[derive(Clone, Copy, Debug)]
pub struct Direction {
    pub src: Dbms,
    pub dst: Dbms,
}

impl Direction {
    pub const MYSQL_TO_POSTGRES: Direction = Direction {
        src: Dbms::MySql,
        dst: Dbms::Postgres,
    };
    pub const POSTGRES_TO_MYSQL: Direction = Direction {
        src: Dbms::Postgres,
        dst: Dbms::MySql,
    };
    pub const MYSQL_TO_MYSQL: Direction = Direction {
        src: Dbms::MySql,
        dst: Dbms::MySql,
    };
    pub const POSTGRES_TO_POSTGRES: Direction = Direction {
        src: Dbms::Postgres,
        dst: Dbms::Postgres,
    };

    /// True when no type conversion should occur.
    pub fn is_identity(&self) -> bool {
        self.src == self.dst
    }

    /// Empty the destination and clear the state store.
    ///
    /// Same-engine directions share one server between source and destination, but
    /// the scratch database is always distinct from the fixture, so the source is
    /// never touched.
    pub async fn reset(&self) {
        self.dst.reset_dest().await;
    }

    pub async fn src_count(&self, table: &str) -> i64 {
        self.src.count(self.src.source_url(), table).await
    }

    pub async fn dst_count(&self, table: &str) -> i64 {
        self.dst.count(self.dst.dest_url(), table).await
    }

    pub async fn src_scalar_i64(&self, sql: &str) -> i64 {
        self.src.scalar_i64(self.src.source_url(), sql).await
    }

    pub async fn dst_scalar_i64(&self, sql: &str) -> i64 {
        self.dst.scalar_i64(self.dst.dest_url(), sql).await
    }

    pub async fn dst_column_count(&self, table: &str) -> i64 {
        self.dst_scalar_i64(&format!(
            "SELECT COUNT(*) FROM information_schema.columns \
             WHERE {} AND table_name = '{table}'",
            self.dst_schema_predicate()
        ))
        .await
    }

    pub async fn dst_table_exists(&self, table: &str) -> bool {
        self.dst_scalar_i64(&format!(
            "SELECT COUNT(*) FROM information_schema.tables \
             WHERE {} AND table_name = '{table}'",
            self.dst_schema_predicate()
        ))
        .await
            > 0
    }

    pub async fn dst_column_exists(&self, table: &str, column: &str) -> bool {
        self.dst_scalar_i64(&format!(
            "SELECT COUNT(*) FROM information_schema.columns \
             WHERE {} AND table_name = '{table}' AND column_name = '{column}'",
            self.dst_schema_predicate()
        ))
        .await
            > 0
    }

    /// Destination column type, lowercased. The two engines spell types
    /// differently, so callers must branch on `dst`:
    /// MySQL yields `COLUMN_TYPE` (`enum('g',...)`, `int`), PostgreSQL yields
    /// `udt_name` (`mpaa_rating`, `int4`).
    pub async fn dst_column_type(&self, table: &str, column: &str) -> String {
        let expr = match self.dst {
            Dbms::MySql => "column_type",
            Dbms::Postgres => "udt_name",
        };
        self.dst
            .scalar_string(
                self.dst.dest_url(),
                &format!(
                    "SELECT LOWER({expr}) FROM information_schema.columns \
                     WHERE {} AND table_name = '{table}' AND column_name = '{column}'",
                    self.dst_schema_predicate()
                ),
            )
            .await
            .unwrap_or_else(|| panic!("no column '{table}.{column}' in destination"))
    }

    /// Sorted column names of a destination table.
    pub async fn dst_columns(&self, table: &str) -> Vec<String> {
        self.dst
            .strings(
                self.dst.dest_url(),
                &format!(
                    "SELECT column_name FROM information_schema.columns \
                     WHERE {} AND table_name = '{table}' ORDER BY column_name",
                    self.dst_schema_predicate()
                ),
            )
            .await
    }

    /// Sorted column names of a source table.
    pub async fn src_columns(&self, table: &str) -> Vec<String> {
        let predicate = match self.src {
            Dbms::MySql => "table_schema = DATABASE()",
            Dbms::Postgres => "table_schema = 'public'",
        };
        self.src
            .strings(
                self.src.source_url(),
                &format!(
                    "SELECT column_name FROM information_schema.columns \
                     WHERE {predicate} AND table_name = '{table}' ORDER BY column_name"
                ),
            )
            .await
    }

    fn dst_schema_predicate(&self) -> &'static str {
        match self.dst {
            Dbms::MySql => "table_schema = DATABASE()",
            Dbms::Postgres => "table_schema = 'public'",
        }
    }

    /// The destination must hold exactly as many rows as the source.
    pub async fn assert_row_parity(&self, src_table: &str, dst_table: &str) {
        let expected = self.src_count(src_table).await;
        let actual = self.dst_count(dst_table).await;
        assert_eq!(
            expected, actual,
            "[{self}] '{src_table}' has {expected} rows but '{dst_table}' has {actual}"
        );
    }

    /// Wrap a pipeline body with the `connection` blocks for this direction.
    /// Bodies refer to `connection.src` and `connection.dst`, never to a URL.
    pub fn smql(&self, body: &str) -> String {
        smql::render(*self, body)
    }

    /// Render a config file from `configs/` for this direction. The file holds a
    /// pipeline body only; the connections are supplied here.
    pub fn config(&self, name: &str) -> String {
        smql::render(*self, &smql::body(name))
    }

    /// Reset the destination, then run a config file for this direction.
    pub async fn run_config(&self, name: &str) {
        self.run(&smql::body(name)).await;
    }

    /// Reset the destination, then run `body` for this direction.
    pub async fn run(&self, body: &str) {
        self.reset().await;
        runner::run_smql(&self.smql(body), false)
            .await
            .unwrap_or_else(|e| panic!("[{self}] migration failed: {e:?}"));
    }
}

impl fmt::Display for Direction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} -> {}", self.src, self.dst)
    }
}

/// Expand one test body into a `#[tokio::test]` per direction.
#[macro_export]
macro_rules! direction_tests {
    ($name:ident, $case:path) => {
        mod $name {
            use super::*;
            use $crate::harness::Direction;

            #[tracing_test::traced_test]
            #[tokio::test(flavor = "multi_thread")]
            async fn mysql_to_postgres() {
                $case(Direction::MYSQL_TO_POSTGRES).await;
            }

            #[tracing_test::traced_test]
            #[tokio::test(flavor = "multi_thread")]
            async fn postgres_to_mysql() {
                $case(Direction::POSTGRES_TO_MYSQL).await;
            }

            #[tracing_test::traced_test]
            #[tokio::test(flavor = "multi_thread")]
            async fn mysql_to_mysql() {
                $case(Direction::MYSQL_TO_MYSQL).await;
            }

            #[tracing_test::traced_test]
            #[tokio::test(flavor = "multi_thread")]
            async fn postgres_to_postgres() {
                $case(Direction::POSTGRES_TO_POSTGRES).await;
            }
        }
    };
}
