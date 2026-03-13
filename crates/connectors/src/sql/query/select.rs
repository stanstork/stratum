#[derive(Debug, Clone)]
pub struct SelectField {
    pub table: String,
    pub column: String,
    pub alias: Option<String>,
    pub data_type: String,
}

impl SelectField {
    pub fn is_geometry(&self) -> bool {
        self.data_type.eq_ignore_ascii_case("geometry")
    }
}
