use crate::parser::{Rule, StatementParser};
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

#[derive(Debug, Clone)]
pub enum DataFormat {
    MYSQL,
    POSTGRES,
    SQLITE,
    MONGODB,
}

impl StatementParser for Connection {
    fn parse(pair: Pair<crate::parser::Rule>) -> Self {
        let mut source = None;
        let mut dest = None;

        for inner_pair in pair.into_inner() {
            match inner_pair.as_rule() {
                Rule::connection_pair => {
                    let connection_pair = ConnectionPair::parse(inner_pair);
                    match connection_pair.conn_type {
                        ConnectionType::Source => source = Some(connection_pair),
                        ConnectionType::Dest => dest = Some(connection_pair),
                    }
                }
                _ => {}
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
        let conn_type = match inner.next().unwrap().as_str().to_ascii_uppercase().as_str() {
            "SOURCE" => ConnectionType::Source,
            _ => ConnectionType::Dest,
        };
        let format = match inner.next().unwrap().as_str().to_ascii_uppercase().as_str() {
            "MYSQL" => DataFormat::MYSQL,
            "POSTGRES" => DataFormat::POSTGRES,
            "SQLITE" => DataFormat::SQLITE,
            _ => DataFormat::MONGODB,
        };
        let conn_str = inner.next().unwrap().as_str().trim_matches('"').to_string();
        ConnectionPair {
            conn_type,
            format,
            conn_str,
        }
    }
}
