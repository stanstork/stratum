use crate::ast::expr::{Expr, Ident};
use common::value::Value;

pub mod ast;
pub mod build;
pub mod dialect;
pub mod macros;
pub mod offsets;
pub mod render;

pub fn ident(name: &str) -> Expr {
    Expr::Identifier(Ident {
        qualifier: None,
        name: name.to_string(),
    })
}

pub fn value(val: Value) -> Expr {
    Expr::Value(val)
}
