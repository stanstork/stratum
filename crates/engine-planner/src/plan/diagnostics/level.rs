use serde::Serialize;

#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticLevel {
    Error,
    Warning,
    Info,
    Hint,
}
