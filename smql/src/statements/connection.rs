use crate::parser::{Rule, StatementParser};
use bitflags::bitflags;
use pest::iterators::Pair;

#[derive(Debug, Clone)]
pub struct Connection {
    pub source: ConnectionPair,
    pub destination: ConnectionPair,
}

#[derive(Debug, Clone)]
pub enum ConnectionType {
    Source,
    Destination,
}

#[derive(Debug, Clone)]
pub struct ConnectionPair {
    pub con_str: String,
    pub data_format: DataFormat,
    pub con_type: ConnectionType,
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

impl DataFormat {
    fn from_pair(pair: Pair<'_, Rule>) -> Result<Self, String> {
        match pair.as_str().to_lowercase().as_str() {
            "mysql" => Ok(DataFormat::MySql),
            "postgres" => Ok(DataFormat::Postgres),
            "sqlite" => Ok(DataFormat::Sqlite),
            "mongo" => Ok(DataFormat::Mongo),
            _ => Err(format!("Invalid data format: {}", pair.as_str())),
        }
    }

    pub const fn sql_databases() -> Self {
        Self::MySql.union(Self::Postgres).union(Self::Sqlite)
    }
}

impl Connection {
    const SOURCE: &'static str = "source";
    const DESTINATION: &'static str = "destination";
}

impl StatementParser for Connection {
    fn parse(pair: Pair<Rule>) -> Self {
        let mut source = None;
        let mut destination = None;

        for inner_pair in pair.into_inner() {
            if let Rule::connection_pair = inner_pair.as_rule() {
                let connection_pair = ConnectionPair::parse(inner_pair);
                match connection_pair.con_type {
                    ConnectionType::Source => source = Some(connection_pair),
                    ConnectionType::Destination => destination = Some(connection_pair),
                }
            }
        }

        Connection {
            source: source.expect("Missing source connection"),
            destination: destination.expect("Missing destination connection"),
        }
    }
}

impl StatementParser for ConnectionPair {
    fn parse(pair: Pair<Rule>) -> Self {
        let mut con_str = String::new();
        let mut data_format = None;
        let mut con_type = None;

        for inner_pair in pair.into_inner() {
            match inner_pair.as_rule() {
                Rule::con_str => {
                    con_str = inner_pair.as_str().trim_matches('"').to_string();
                }
                Rule::data_format => {
                    data_format =
                        Some(DataFormat::from_pair(inner_pair).expect("Invalid data format"));
                }
                Rule::connection_type => {
                    con_type = match inner_pair.as_str().to_lowercase().as_str() {
                        Connection::SOURCE => Some(ConnectionType::Source),
                        Connection::DESTINATION => Some(ConnectionType::Destination),
                        _ => None,
                    };
                }
                _ => panic!(
                    "Unexpected rule in connection pair: {:?}",
                    inner_pair.as_rule()
                ),
            }
        }

        ConnectionPair {
            con_str,
            data_format: data_format.expect("Missing data format"),
            con_type: con_type.expect("Missing connection type"),
        }
    }
}
