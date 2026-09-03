#![deny(clippy::unwrap_used)]
#![cfg_attr(test, allow(clippy::unwrap_used))]

pub mod ast;
pub mod builder;
pub mod errors;
pub mod parser;
pub mod semantic;
