pub mod column_parser;
pub mod column_validator;
pub mod dialect_resolver;
pub mod masking;

use std::time::Duration;

pub use column_parser::{ColumnRef, ColumnRefError, ColumnRefParser};
pub use column_validator::ColumnValidator;
pub use dialect_resolver::dialect_for_adapter;
pub use masking::MaskingPolicy;

pub fn format_duration(d: &Duration) -> String {
    let millis = d.as_millis();

    if millis < 1000 {
        format!("{}ms", millis)
    } else if millis < 60_000 {
        format!("{}s", millis / 1000)
    } else if millis < 3_600_000 {
        format!("{}m", millis / 60_000)
    } else {
        format!("{}h {}m", millis / 3_600_000, (millis % 3_600_000) / 60_000)
    }
}
