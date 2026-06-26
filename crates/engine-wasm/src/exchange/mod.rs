use serde::{Deserialize, Serialize};

pub mod json_v1;
pub mod types;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExchangeFormat {
    JsonV1,
    // Future:
    // MessagePackV1,
    // FlatBufferV1,
}

impl ExchangeFormat {
    /// Formats supported by the host, in preference order.
    pub fn supported() -> &'static [ExchangeFormat] {
        &[ExchangeFormat::JsonV1]
    }

    /// Negotiate format: pick the guest's preferred format if the host supports it.
    pub fn negotiate(guest_preferred: ExchangeFormat) -> Option<ExchangeFormat> {
        if Self::supported().contains(&guest_preferred) {
            Some(guest_preferred)
        } else {
            None
        }
    }
}
