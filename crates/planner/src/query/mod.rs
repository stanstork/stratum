use crate::query::ast::expr::{Expr, Ident};
use model::core::value::Value;

pub mod ast;
pub mod builder;
pub mod dialect;
pub mod macros;
pub mod offsets;
pub mod renderer;

pub fn ident(name: &str) -> Expr {
    Expr::Identifier(Ident {
        qualifier: None,
        name: name.to_string(),
    })
}

pub fn value(val: Value) -> Expr {
    Expr::Value(val)
}
