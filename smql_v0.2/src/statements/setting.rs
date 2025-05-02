use crate::parser::StatementParser;

#[derive(Debug, Clone)]
pub struct Settings {
    pub infer_schema: bool,
    pub ignore_constraints: bool,
    pub create_missing_columns: bool,
    pub create_missing_tables: bool,
    pub copy_columns: CopyColumns,
    pub batch_size: usize,
}

#[derive(Debug, Clone)]
pub struct SettingsPair {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone)]
pub enum CopyColumns {
    All,
    MapOnly,
}

impl StatementParser for SettingsPair {
    fn parse(pair: pest::iterators::Pair<crate::parser::Rule>) -> Self {
        let mut inner = pair.into_inner();

        let name = inner
            .next()
            .expect("Expected settings name")
            .as_str()
            .to_string();
        let value = inner
            .next()
            .expect("Expected settings value")
            .as_str()
            .to_string();

        SettingsPair { name, value }
    }
}
