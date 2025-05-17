use crate::parser::{Rule, StatementParser};
use bitflags::bitflags;
use core::fmt;
use pest::iterators::Pair;

// ─────────────────────────────────────────────────────────────
// CONNECTION statement
// Example: CONNECTIONS (
//    SOURCE(MYSQL,         "mysql://user:password@localhost:3306/testdb"),
//    DESTINATION(POSTGRES, "postgres://user:password@localhost:5432/testdb")
//  );
// ─────────────────────────────────────────────────────────────
#[derive(Debug, Clone)]
pub struct Connection {
    pub source: Option<ConnectionPair>,
    pub dest: Option<ConnectionPair>,
}

#[derive(Debug, Clone)]
pub struct ConnectionPair {
    pub conn_type: ConnectionType,
    pub format: DataFormat,
    pub conn_str: String,
}

#[derive(Debug, Clone)]
pub enum ConnectionType {
    Source,
    Dest,
}

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct DataFormat: u8 {
        const MySql    = 0b0000_0001; // 1
        const Postgres = 0b0000_0010; // 2
        const Sqlite   = 0b0000_0100; // 4
        const Mongo    = 0b0000_1000; // 8
        const Csv      = 0b0001_0000; // 16
        const Api      = 0b0010_0000; // 32
    }
}

const KEY_SOURCE: &str = "SOURCE";
const KEY_MYSQL: &str = "MYSQL";
const KEY_POSTGRES: &str = "POSTGRES";
const KEY_SQLITE: &str = "SQLITE";

impl StatementParser for Connection {
    fn parse(pair: Pair<crate::parser::Rule>) -> Self {
        let mut source = None;
        let mut dest = None;

        for inner_pair in pair.into_inner() {
            if inner_pair.as_rule() == Rule::connection_pair {
                let connection_pair = ConnectionPair::parse(inner_pair);
                match connection_pair.conn_type {
                    ConnectionType::Source => source = Some(connection_pair),
                    ConnectionType::Dest => dest = Some(connection_pair),
                }
            }
        }

        Connection { source, dest }
    }
}

impl StatementParser for ConnectionPair {
    fn parse(pair: Pair<crate::parser::Rule>) -> Self {
        let mut inner = pair.into_inner();

        // parse connection type
        let conn_type_str = inner.next().unwrap().as_str();
        let conn_type = if conn_type_str.eq_ignore_ascii_case(KEY_SOURCE) {
            ConnectionType::Source
        } else {
            ConnectionType::Dest
        };

        // parse data format
        let format_str = inner.next().unwrap().as_str().to_ascii_uppercase();
        let format = match format_str.as_str() {
            KEY_MYSQL => DataFormat::MySql,
            KEY_POSTGRES => DataFormat::Postgres,
            KEY_SQLITE => DataFormat::Sqlite,
            _ => DataFormat::empty(),
        };

        // parse connection string
        let conn_str = inner.next().unwrap().as_str().trim_matches('"').to_string();

        ConnectionPair {
            conn_type,
            format,
            conn_str,
        }
    }
}

impl fmt::Display for DataFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut formats = Vec::new();
        if self.contains(DataFormat::MySql) {
            formats.push("MySQL");
        }
        if self.contains(DataFormat::Postgres) {
            formats.push("Postgres");
        }
        if self.contains(DataFormat::Sqlite) {
            formats.push("SQLite");
        }
        if self.contains(DataFormat::Mongo) {
            formats.push("MongoDB");
        }
        if self.contains(DataFormat::Csv) {
            formats.push("CSV");
        }
        write!(f, "{}", formats.join(", "))
    }
}

impl Default for ConnectionPair {
    fn default() -> Self {
        ConnectionPair {
            conn_type: ConnectionType::Source,
            format: DataFormat::empty(),
            conn_str: String::new(),
        }
    }
}

impl DataFormat {
    pub fn is_file(&self) -> bool {
        self.contains(DataFormat::Csv)
    }

    pub fn is_sql(&self) -> bool {
        self.contains(DataFormat::MySql)
            || self.contains(DataFormat::Postgres)
            || self.contains(DataFormat::Sqlite)
    }
}
