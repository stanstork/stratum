use crate::ast::span::Span;
use std::fmt;

/// Dot-separated path (e.g., connection.mysql_prod, users.email)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DotPath {
    pub segments: Vec<String>,
    pub span: Span,
}

impl DotPath {
    pub fn new(segments: Vec<String>, span: Span) -> Self {
        Self { segments, span }
    }

    pub fn from_string(path: &str, span: Span) -> Self {
        Self {
            segments: path.split('.').map(|s| s.to_string()).collect(),
            span,
        }
    }
}

impl fmt::Display for DotPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.segments.join("."))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dot_path_from_string() {
        let span = Span::new(0, 12, 1, 1);
        let path = DotPath::from_string("connection.mysql_prod", span);

        assert_eq!(path.segments.len(), 2);
        assert_eq!(path.segments[0], "connection");
        assert_eq!(path.segments[1], "mysql_prod");
    }
}
