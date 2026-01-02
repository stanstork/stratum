use serde::Serialize;

#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionStatus {
    /// Successfully connected during plan generation
    Connected {
        /// Round-trip latency in milliseconds
        latency_ms: u64,
        version: String,
    },

    /// Not tested (--no-connect flag)
    Untested,

    /// Connection failed during plan generation
    Failed { error: String },
}

#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionRole {
    Source,
    Destination,
    Both,
}
