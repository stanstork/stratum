use connectors::{
    error::AdapterError,
    sql::base::error::{ConnectorError, DbError},
};
use engine_core::{error::SinkError, retry::RetryDisposition};
use mysql_async::Error as MySqlError;
use tokio_postgres::{Error as PgError, error::SqlState};

pub fn classify_adapter_error(err: &AdapterError) -> RetryDisposition {
    match err {
        AdapterError::Database(db_err) => classify_db_error(db_err),
        AdapterError::Connector(conn_err) => classify_connector_error(conn_err),
        AdapterError::Generic(_) => RetryDisposition::Stop,
        AdapterError::UnsupportedFormat(_) => RetryDisposition::Stop,
        AdapterError::AdapterNotFound(_) => RetryDisposition::Stop,
        AdapterError::FileError(_) => RetryDisposition::Stop,
        AdapterError::InvalidMetadata(_) => RetryDisposition::Stop,
        AdapterError::UnsupportedDriver(_) => RetryDisposition::Stop,
        AdapterError::MissingProperty(_) => RetryDisposition::Stop,
    }
}

pub fn classify_db_error(err: &DbError) -> RetryDisposition {
    match err {
        DbError::Io(_) => RetryDisposition::Retry,
        DbError::MySqlError(mysql_err) => classify_mysql_error(mysql_err),
        DbError::PgError(pg_err) => classify_pg_error(pg_err),
        DbError::Write(_) => RetryDisposition::Stop,
        DbError::InvalidAdapter(_) => RetryDisposition::Stop,
        DbError::CircularReference(_) => RetryDisposition::Stop,
        DbError::Utf8(_) => RetryDisposition::Stop,
        DbError::QueryBuildError(_) => RetryDisposition::Stop,
        DbError::Unknown(_) => RetryDisposition::Stop,
    }
}

pub fn classify_sink_error(err: &SinkError) -> RetryDisposition {
    match err {
        SinkError::Io(_) | SinkError::Protocol(_) | SinkError::Closed => RetryDisposition::Retry,
        SinkError::Db(db_err) => classify_db_error(db_err),
        SinkError::TokioPostgres(pg_err) => classify_pg_error(pg_err),
        SinkError::Capabilities => RetryDisposition::Stop,
        SinkError::FastPathNotSupported(_) => RetryDisposition::Stop,
        SinkError::Other(_) => RetryDisposition::Stop,
    }
}

fn classify_connector_error(err: &ConnectorError) -> RetryDisposition {
    match err {
        ConnectorError::MySql(mysql_err) => classify_mysql_error(mysql_err),
        ConnectorError::Connection(pg_err) => classify_pg_error(pg_err),
        ConnectorError::InvalidUrl(_) => RetryDisposition::Stop,
        ConnectorError::TlsConfig(_) => RetryDisposition::Retry,
    }
}

fn classify_pg_error(err: &PgError) -> RetryDisposition {
    if err.is_closed() {
        return RetryDisposition::Retry;
    }

    if let Some(code) = err.code()
        && is_retryable_pg_code(code)
    {
        return RetryDisposition::Retry;
    }

    RetryDisposition::Stop
}

fn is_retryable_pg_code(code: &SqlState) -> bool {
    matches!(
        *code,
        SqlState::T_R_SERIALIZATION_FAILURE
            | SqlState::T_R_DEADLOCK_DETECTED
            | SqlState::LOCK_NOT_AVAILABLE
            | SqlState::TOO_MANY_CONNECTIONS
            | SqlState::ADMIN_SHUTDOWN
            | SqlState::CRASH_SHUTDOWN
            | SqlState::CANNOT_CONNECT_NOW
            | SqlState::CONNECTION_FAILURE
            | SqlState::CONNECTION_DOES_NOT_EXIST
            | SqlState::SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION
            | SqlState::SQLSERVER_REJECTED_ESTABLISHMENT_OF_SQLCONNECTION
            | SqlState::CONNECTION_EXCEPTION
            | SqlState::QUERY_CANCELED
            | SqlState::OPERATOR_INTERVENTION
            | SqlState::FDW_UNABLE_TO_ESTABLISH_CONNECTION
    )
}

fn classify_mysql_error(err: &MySqlError) -> RetryDisposition {
    match err {
        MySqlError::Io(_) | MySqlError::Other(_) => RetryDisposition::Retry,
        MySqlError::Driver(_) => RetryDisposition::Retry,
        MySqlError::Server(server_err) => {
            if is_retryable_mysql_server_error(server_err.code, server_err.state.as_str()) {
                RetryDisposition::Retry
            } else {
                RetryDisposition::Stop
            }
        }
        _ => RetryDisposition::Stop,
    }
}

fn is_retryable_mysql_server_error(code: u16, state: &str) -> bool {
    // Common MySQL server error codes that are typically transient/retryable.
    // See: https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html
    const RETRYABLE_CODES: [u16; 8] = [1205, 1213, 2002, 2003, 2006, 2013, 1040, 1042];
    if RETRYABLE_CODES.contains(&code) {
        return true;
    }

    matches!(state, "40001" | "HYT00" | "08S01")
}
