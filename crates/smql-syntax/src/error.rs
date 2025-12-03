use thiserror::Error;

#[derive(Error, Debug)]
pub enum SmqlError {
    #[error("Parsing error: {0}")]
    Parse(String),
}
