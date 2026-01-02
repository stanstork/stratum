use serde::Serialize;

#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum PaginationStrategy {
    Timestamp,
    Numeric,
    Pk,
    Default,
}
