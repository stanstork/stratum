use serde::Serialize;

#[derive(Serialize, Debug, Clone)]
pub struct SourceLocation {
    /// Line number in SMQL file (1-indexed)
    pub line: usize,

    /// Column number in line (1-indexed)
    pub column: usize,

    /// Surrounding code context for error display
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<String>,
}
