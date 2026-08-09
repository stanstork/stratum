#![deny(clippy::unwrap_used)]
#![cfg_attr(test, allow(clippy::unwrap_used))]

pub mod error;
pub mod reader;
pub mod verifier;

pub use verifier::verify;
