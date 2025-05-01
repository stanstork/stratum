use crate::parser::StatementParser;
use pest::iterators::Pair;

#[derive(Debug, Clone)]
pub struct Connection {
    pub conn_type: ConnectionType,
    pub format: DataFormat,
    pub conn_str: String,
}

#[derive(Debug, Clone)]
pub enum ConnectionType {
    Source,
    Destination,
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
        let mut inner = pair.into_inner();
        let conn_type = match inner.next().unwrap().as_str().to_ascii_uppercase().as_str() {
            "SOURCE" => ConnectionType::Source,
            _ => ConnectionType::Destination,
        };
        let format = match inner.next().unwrap().as_str().to_ascii_uppercase().as_str() {
            "MYSQL" => DataFormat::MYSQL,
            "POSTGRES" => DataFormat::POSTGRES,
            "SQLITE" => DataFormat::SQLITE,
            _ => DataFormat::MONGODB,
        };
        let conn_str = inner.next().unwrap().as_str().trim_matches('"').to_string();
        Connection {
            conn_type,
            format,
            conn_str,
        }
    }
}
