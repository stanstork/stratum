use serde::Serialize;

#[derive(Serialize, Debug, Clone)]
pub struct CursorColumn {
    pub table: String,
    pub column: String,
}
