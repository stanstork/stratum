use super::setting::Setting;

#[derive(Debug)]
pub struct Migrate {
    pub source: Vec<String>,
    pub target: String,
    pub settings: Vec<Setting>,
}
