use crate::ast::span::Span;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Identifier {
    pub name: String,
    pub span: Span,
}

impl Identifier {
    pub fn new(name: &str, span: Span) -> Self {
        Self {
            name: name.to_string(),
            span,
        }
    }
}

impl fmt::Display for Identifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_identifier_display() {
        let span = Span::new(0, 4, 1, 1);
        let ident = Identifier::new("test", span);

        assert_eq!(format!("{}", ident), "test");
    }
}
