#![deny(clippy::unwrap_used)]
#![cfg_attr(test, allow(clippy::unwrap_used))]

pub mod error;
pub mod progress;
pub mod reader;
pub mod verifier;

pub use progress::{NoopProgress, VerifyProgress};
pub use verifier::{verify, verify_with_progress};
