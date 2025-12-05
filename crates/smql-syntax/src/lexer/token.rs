use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub struct Token {
    pub kind: TokenKind,
    pub lexeme: String,
    pub line: usize,
    pub column: usize,
    pub span: (usize, usize),
}

#[derive(Debug, Clone, PartialEq)]
pub enum TokenKind {
    // Keywords
    Define,
    Connection,
    Pipeline,
    From,
    To,
    Where,
    With,
    Select,
    When,
    Validate,
    OnError,
    Before,
    After,
    Paginate,
    Settings,

    // Literals
    String(String),
    Number(f64),
    Boolean(bool),
    Null,

    // Identifiers
    Identifier(String),
    DotNotation(String),

    // Operators
    Assign,         // =
    Equal,          // ==
    NotEqual,       // !=
    GreaterThan,    // >
    LessThan,       // <
    GreaterOrEqual, // >=
    LessOrEqual,    // <=
    And,            // &&
    Or,             // ||
    Not,            // !

    // Delimiters
    LeftBrace,    // {
    RightBrace,   // }
    LeftBracket,  // [
    RightBracket, // ]
    LeftParen,    // (
    RightParen,   // )
    Comma,        // ,

    // Complex constructs
    FunctionCall(String), // Function name
    ArrayStart,
    ArrayEnd,

    // Special
    Eof,
}

impl fmt::Display for TokenKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TokenKind::Define => write!(f, "define"),
            TokenKind::Connection => write!(f, "connection"),
            TokenKind::Pipeline => write!(f, "pipeline"),
            TokenKind::From => write!(f, "from"),
            TokenKind::To => write!(f, "to"),
            TokenKind::Where => write!(f, "where"),
            TokenKind::With => write!(f, "with"),
            TokenKind::Select => write!(f, "select"),
            TokenKind::When => write!(f, "when"),
            TokenKind::Validate => write!(f, "validate"),
            TokenKind::OnError => write!(f, "on_error"),
            TokenKind::Before => write!(f, "before"),
            TokenKind::After => write!(f, "after"),
            TokenKind::Paginate => write!(f, "paginate"),
            TokenKind::Settings => write!(f, "settings"),
            TokenKind::String(s) => write!(f, "\"{}\"", s),
            TokenKind::Number(n) => write!(f, "{}", n),
            TokenKind::Boolean(b) => write!(f, "{}", b),
            TokenKind::Null => write!(f, "null"),
            TokenKind::Identifier(s) => write!(f, "{}", s),
            TokenKind::DotNotation(s) => write!(f, "{}", s),
            TokenKind::Assign => write!(f, "="),
            TokenKind::Equal => write!(f, "=="),
            TokenKind::NotEqual => write!(f, "!="),
            TokenKind::GreaterThan => write!(f, ">"),
            TokenKind::LessThan => write!(f, "<"),
            TokenKind::GreaterOrEqual => write!(f, ">="),
            TokenKind::LessOrEqual => write!(f, "<="),
            TokenKind::And => write!(f, "&&"),
            TokenKind::Or => write!(f, "||"),
            TokenKind::Not => write!(f, "!"),
            TokenKind::LeftBrace => write!(f, "{{"),
            TokenKind::RightBrace => write!(f, "}}"),
            TokenKind::LeftBracket => write!(f, "["),
            TokenKind::RightBracket => write!(f, "]"),
            TokenKind::LeftParen => write!(f, "("),
            TokenKind::RightParen => write!(f, ")"),
            TokenKind::Comma => write!(f, ","),
            TokenKind::FunctionCall(name) => write!(f, "{}()", name),
            TokenKind::ArrayStart => write!(f, "array["),
            TokenKind::ArrayEnd => write!(f, "]"),
            TokenKind::Eof => write!(f, "EOF"),
        }
    }
}
