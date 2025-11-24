#[derive(Debug, Clone)]
pub struct JoinField {
    pub table_alias: String,   // e.g. "u", "oi", "p"
    pub source_column: String, // e.g. "name", "price"
    pub alias: Option<String>, // e.g. "user_name", if mapped
}
