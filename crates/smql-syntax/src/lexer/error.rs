use crate::parser::Rule;
use pest::error::Error as PestError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum LexerError {
    #[error("Parse error at line {line}, column {column}: {message}")]
    ParseError {
        message: String,
        line: usize,
        column: usize,
        source_snippet: String,
    },

    #[error("Invalid number format: {0}")]
    InvalidNumber(String),

    #[error("Unexpected token '{token}' at line {line}, column {column}")]
    UnexpectedToken {
        token: String,
        line: usize,
        column: usize,
    },
}

impl LexerError {
    pub fn from_pest_error(err: PestError<Rule>) -> Self {
        use pest::error::LineColLocation;

        let (line, column) = match err.line_col {
            LineColLocation::Pos((l, c)) => (l, c),
            LineColLocation::Span((l, c), _) => (l, c),
        };

        let message = format!("{}", err.variant);
        let source_snippet = err.line().to_string();

        LexerError::ParseError {
            message,
            line,
            column,
            source_snippet,
        }
    }

    /// Format error with context for display
    pub fn format_error(&self) -> String {
        match self {
            LexerError::ParseError {
                message,
                line,
                column,
                source_snippet,
            } => {
                format!(
                    "Parse Error at line {}, column {}:\n{}\n{}^\n{}",
                    line,
                    column,
                    source_snippet,
                    " ".repeat(column.saturating_sub(1)),
                    message
                )
            }
            _ => self.to_string(),
        }
    }
}
