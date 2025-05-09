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
    pub source: ConnectionPair,
    pub dest: ConnectionPair,
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
        const MySql    = 0b0001;
        const Postgres = 0b0010;
        const Sqlite   = 0b0100;
        const Mongo    = 0b1000;
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

        Connection {
            source: source.expect("Expected a source connection"),
            dest: dest.expect("Expected a destination connection"),
        }
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
        write!(f, "{}", formats.join(", "))
    }
}
