#[derive(Debug)]
pub struct Setting {
    pub key: String,
    pub value: SettingValue,
}

#[derive(Debug)]
pub enum SettingValue {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
}
