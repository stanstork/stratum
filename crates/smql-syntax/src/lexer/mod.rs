use crate::{
    lexer::{
        error::LexerError,
        token::{Token, TokenKind},
    },
    parser::{Rule, SmqlParser},
};
use pest::{Parser, iterators::Pair};

pub mod error;
pub mod token;

pub struct Lexer {
    tokens: Vec<Token>,
}

impl Lexer {
    pub fn new() -> Self {
        Lexer { tokens: Vec::new() }
    }

    pub fn tokenize(&mut self, input: &str) -> Result<Vec<Token>, LexerError> {
        self.tokens.clear();

        let pairs = SmqlParser::parse(Rule::program, input).map_err(LexerError::from_pest_error)?;

        for pair in pairs {
            self.process_pair(pair)?;
        }

        // Add EOF token
        self.tokens.push(Token {
            kind: TokenKind::Eof,
            lexeme: String::new(),
            line: input.lines().count(),
            column: input.lines().last().map(|l| l.len()).unwrap_or(0),
            span: (input.len(), input.len()),
        });

        Ok(self.tokens.clone())
    }

    pub fn tokens(&self) -> &[Token] {
        &self.tokens
    }

    fn process_pair(&mut self, pair: Pair<Rule>) -> Result<(), LexerError> {
        let rule = pair.as_rule();
        let span = pair.as_span();
        let (line, column) = span.start_pos().line_col();
        let lexeme = span.as_str().to_string();

        match rule {
            // Keywords
            Rule::kw_define => self.add_token(
                TokenKind::Define,
                lexeme,
                line,
                column,
                span.start(),
                span.end(),
            ),
            Rule::kw_connection => self.add_token(
                TokenKind::Connection,
                lexeme,
                line,
                column,
                span.start(),
                span.end(),
            ),
            Rule::kw_pipeline => self.add_token(
                TokenKind::Pipeline,
                lexeme,
                line,
                column,
                span.start(),
                span.end(),
            ),
            Rule::kw_from => self.add_token(
                TokenKind::From,
                lexeme,
                line,
                column,
                span.start(),
                span.end(),
            ),
            Rule::kw_to => self.add_token(
                TokenKind::To,
                lexeme,
                line,
                column,
                span.start(),
                span.end(),
            ),
            Rule::kw_where => self.add_token(
                TokenKind::Where,
                lexeme,
                line,
                column,
                span.start(),
                span.end(),
            ),
            Rule::kw_with => self.add_token(
                TokenKind::With,
                lexeme,
                line,
                column,
                span.start(),
                span.end(),
            ),
            Rule::kw_select => self.add_token(
                TokenKind::Select,
                lexeme,
                line,
                column,
                span.start(),
                span.end(),
            ),
            Rule::kw_when => self.add_token(
                TokenKind::When,
                lexeme,
                line,
                column,
                span.start(),
                span.end(),
            ),
            Rule::kw_validate => self.add_token(
                TokenKind::Validate,
                lexeme,
                line,
                column,
                span.start(),
                span.end(),
            ),
            Rule::kw_on_error => self.add_token(
                TokenKind::OnError,
                lexeme,
                line,
                column,
                span.start(),
                span.end(),
            ),
            Rule::kw_before => self.add_token(
                TokenKind::Before,
                lexeme,
                line,
                column,
                span.start(),
                span.end(),
            ),
            Rule::kw_after => self.add_token(
                TokenKind::After,
                lexeme,
                line,
                column,
                span.start(),
                span.end(),
            ),
            Rule::kw_paginate => self.add_token(
                TokenKind::Paginate,
                lexeme,
                line,
                column,
                span.start(),
                span.end(),
            ),
            Rule::kw_settings => self.add_token(
                TokenKind::Settings,
                lexeme,
                line,
                column,
                span.start(),
                span.end(),
            ),

            // Literals
            Rule::lit_null => self.add_token(
                TokenKind::Null,
                lexeme,
                line,
                column,
                span.start(),
                span.end(),
            ),
            Rule::lit_boolean => {
                let value = lexeme == "true";
                self.add_token(
                    TokenKind::Boolean(value),
                    lexeme,
                    line,
                    column,
                    span.start(),
                    span.end(),
                )
            }
            Rule::lit_number => {
                let num = lexeme
                    .parse::<f64>()
                    .map_err(|_| LexerError::InvalidNumber(lexeme.clone()))?;
                self.add_token(
                    TokenKind::Number(num),
                    lexeme,
                    line,
                    column,
                    span.start(),
                    span.end(),
                )
            }
            Rule::lit_string => {
                let content = lexeme.trim_matches(|c| c == '"' || c == '\'');
                // Handle escape sequences
                let unescaped = content
                    .replace("\\n", "\n")
                    .replace("\\t", "\t")
                    .replace("\\r", "\r")
                    .replace("\\\"", "\"")
                    .replace("\\'", "'")
                    .replace("\\\\", "\\");
                self.add_token(
                    TokenKind::String(unescaped),
                    lexeme,
                    line,
                    column,
                    span.start(),
                    span.end(),
                )
            }

            // Identifiers
            Rule::ident => self.add_token(
                TokenKind::Identifier(lexeme.clone()),
                lexeme,
                line,
                column,
                span.start(),
                span.end(),
            ),
            Rule::dotted_ident => self.add_token(
                TokenKind::DotNotation(lexeme.clone()),
                lexeme,
                line,
                column,
                span.start(),
                span.end(),
            ),

            // Operators
            Rule::op_eq_eq => self.add_token(
                TokenKind::Equal,
                lexeme,
                line,
                column,
                span.start(),
                span.end(),
            ),
            Rule::op_neq => self.add_token(
                TokenKind::NotEqual,
                lexeme,
                line,
                column,
                span.start(),
                span.end(),
            ),
            Rule::op_gte => self.add_token(
                TokenKind::GreaterOrEqual,
                lexeme,
                line,
                column,
                span.start(),
                span.end(),
            ),
            Rule::op_lte => self.add_token(
                TokenKind::LessOrEqual,
                lexeme,
                line,
                column,
                span.start(),
                span.end(),
            ),
            Rule::op_and => self.add_token(
                TokenKind::And,
                lexeme,
                line,
                column,
                span.start(),
                span.end(),
            ),
            Rule::op_or => self.add_token(
                TokenKind::Or,
                lexeme,
                line,
                column,
                span.start(),
                span.end(),
            ),
            Rule::op_eq => self.add_token(
                TokenKind::Assign,
                lexeme,
                line,
                column,
                span.start(),
                span.end(),
            ),
            Rule::op_gt => self.add_token(
                TokenKind::GreaterThan,
                lexeme,
                line,
                column,
                span.start(),
                span.end(),
            ),
            Rule::op_lt => self.add_token(
                TokenKind::LessThan,
                lexeme,
                line,
                column,
                span.start(),
                span.end(),
            ),
            Rule::op_not => self.add_token(
                TokenKind::Not,
                lexeme,
                line,
                column,
                span.start(),
                span.end(),
            ),

            // Delimiters
            Rule::lbrace => self.add_token(
                TokenKind::LeftBrace,
                lexeme,
                line,
                column,
                span.start(),
                span.end(),
            ),
            Rule::rbrace => self.add_token(
                TokenKind::RightBrace,
                lexeme,
                line,
                column,
                span.start(),
                span.end(),
            ),
            Rule::lbracket => self.add_token(
                TokenKind::LeftBracket,
                lexeme,
                line,
                column,
                span.start(),
                span.end(),
            ),
            Rule::rbracket => self.add_token(
                TokenKind::RightBracket,
                lexeme,
                line,
                column,
                span.start(),
                span.end(),
            ),
            Rule::lparen => self.add_token(
                TokenKind::LeftParen,
                lexeme,
                line,
                column,
                span.start(),
                span.end(),
            ),
            Rule::rparen => self.add_token(
                TokenKind::RightParen,
                lexeme,
                line,
                column,
                span.start(),
                span.end(),
            ),
            Rule::comma => self.add_token(
                TokenKind::Comma,
                lexeme,
                line,
                column,
                span.start(),
                span.end(),
            ),

            // Complex constructs
            Rule::fn_call => {
                // Extract function name from first inner pair
                let mut inner = pair.into_inner();
                if let Some(name_pair) = inner.next() {
                    let func_name = name_pair.as_str().to_string();
                    let (fn_line, fn_col) = name_pair.as_span().start_pos().line_col();

                    // Add function call token
                    self.add_token(
                        TokenKind::FunctionCall(func_name.clone()),
                        format!("{}(", func_name),
                        fn_line,
                        fn_col,
                        name_pair.as_span().start(),
                        name_pair.as_span().end(),
                    );

                    // Process remaining pairs (lparen, args, rparen)
                    for inner_pair in inner {
                        self.process_pair(inner_pair)?;
                    }
                }
            }

            Rule::array_lit => {
                // Process array elements
                for inner_pair in pair.into_inner() {
                    self.process_pair(inner_pair)?;
                }
            }

            // Recursively process other rules
            _ => {
                for inner_pair in pair.into_inner() {
                    self.process_pair(inner_pair)?;
                }
            }
        }

        Ok(())
    }

    fn add_token(
        &mut self,
        kind: TokenKind,
        lexeme: String,
        line: usize,
        column: usize,
        start: usize,
        end: usize,
    ) {
        self.tokens.push(Token {
            kind,
            lexeme,
            line,
            column,
            span: (start, end),
        });
    }
}

impl Default for Lexer {
    fn default() -> Self {
        Lexer { tokens: vec![] }
    }
}

#[cfg(test)]
mod tests;
