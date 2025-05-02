use crate::parser::StatementParser;
use pest::iterators::Pair;

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
    fn parse(pair: Pair<crate::parser::Rule>) -> Self {
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

impl Settings {
    pub fn from_pairs(pairs: Vec<SettingsPair>) -> Self {
        let mut settings = Settings::default();

        for pair in pairs {
            match pair.name.as_str().to_ascii_uppercase().as_str() {
                "INFER_SCHEMA" => settings.infer_schema = pair.to_bool(),
                "IGNORE_CONSTRAINTS" => settings.ignore_constraints = pair.to_bool(),
                "CREATE_MISSING_COLUMNS" => settings.create_missing_columns = pair.to_bool(),
                "CREATE_MISSING_TABLES" => settings.create_missing_tables = pair.to_bool(),
                "COPY_COLUMNS" => settings.copy_columns = pair.to_copy_columns(),
                "BATCH_SIZE" => settings.batch_size = pair.to_usize(),
                _ => {}
            }
        }

        settings
    }
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            infer_schema: false,
            ignore_constraints: false,
            create_missing_columns: false,
            create_missing_tables: false,
            copy_columns: CopyColumns::All,
            batch_size: 1000,
        }
    }
}

impl SettingsPair {
    pub fn to_bool(&self) -> bool {
        self.value.to_ascii_uppercase() == "TRUE"
            || self.value.to_ascii_uppercase() == "YES"
            || self.value.to_ascii_uppercase() == "1"
    }

    pub fn to_string(&self) -> String {
        self.value.clone()
    }

    pub fn to_usize(&self) -> usize {
        self.value.parse().unwrap_or(1000)
    }

    pub fn to_copy_columns(&self) -> CopyColumns {
        match self.value.as_str().to_ascii_uppercase().as_str() {
            "ALL" => CopyColumns::All,
            "MAP_ONLY" => CopyColumns::MapOnly,
            _ => CopyColumns::All,
        }
    }
}
