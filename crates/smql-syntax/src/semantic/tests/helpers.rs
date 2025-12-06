use crate::ast::{
    dotpath::DotPath,
    expr::{Expression, ExpressionKind},
    ident::Identifier,
    literal::Literal,
    span::Span,
};

/// Helper function to create a span
pub fn span(line: usize, col: usize) -> Span {
    Span::new(0, 10, line, col)
}

/// Helper to create a simple identifier
pub fn ident(name: &str, span: Span) -> Identifier {
    Identifier::new(name, span)
}

/// Helper to create a dot notation expression
pub fn dot_notation(path: &[&str], span: Span) -> Expression {
    Expression::new(
        ExpressionKind::DotNotation(DotPath::new(
            path.iter().map(|s| s.to_string()).collect(),
            span,
        )),
        span,
    )
}

/// Helper to create a literal expression
pub fn string_lit(value: &str, span: Span) -> Expression {
    Expression::new(
        ExpressionKind::Literal(Literal::String(value.to_string())),
        span,
    )
}
