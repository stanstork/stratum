use crate::parser::{Rule, StatementParser};
use pest::iterators::Pair;

#[derive(Debug, Clone)]
pub struct Settings {
    pub infer_schema: bool,
    pub ignore_constraints: bool,
    pub create_missing_columns: bool,
    pub create_missing_tables: bool,
    pub copy_columns: CopyColumns,
    pub batch_size: usize,
    pub cascade_schema: bool,
}

#[derive(Debug, Clone)]
pub struct SettingsPair {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CopyColumns {
    All,
    MapOnly,
}

// Constants for settings keys
const KEY_INFER_SCHEMA: &str = "INFER_SCHEMA";
const KEY_IGNORE_CONSTRAINTS: &str = "IGNORE_CONSTRAINTS";
const KEY_CREATE_MISSING_COLUMNS: &str = "CREATE_MISSING_COLUMNS";
const KEY_CREATE_MISSING_TABLES: &str = "CREATE_MISSING_TABLES";
const KEY_COPY_COLUMNS: &str = "COPY_COLUMNS";
const KEY_BATCH_SIZE: &str = "BATCH_SIZE";
const KEY_CASCADE_SCHEMA: &str = "CASCADE_SCHEMA";
const KEY_TRUE: &str = "TRUE";
const KEY_ALL: &str = "ALL";
const KEY_MAP_ONLY: &str = "MAP_ONLY";

impl StatementParser for Settings {
    fn parse(pair: Pair<Rule>) -> Self {
        let settings_pairs = pair
            .into_inner()
            .map(SettingsPair::parse)
            .collect::<Vec<_>>();
        Settings::from_pairs(settings_pairs)
    }
}

impl StatementParser for SettingsPair {
    fn parse(pair: Pair<Rule>) -> Self {
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
                KEY_INFER_SCHEMA => settings.infer_schema = pair.to_bool(),
                KEY_IGNORE_CONSTRAINTS => settings.ignore_constraints = pair.to_bool(),
                KEY_CREATE_MISSING_COLUMNS => settings.create_missing_columns = pair.to_bool(),
                KEY_CREATE_MISSING_TABLES => settings.create_missing_tables = pair.to_bool(),
                KEY_COPY_COLUMNS => settings.copy_columns = pair.to_copy_columns(),
                KEY_BATCH_SIZE => settings.batch_size = pair.to_usize(),
                KEY_CASCADE_SCHEMA => settings.cascade_schema = pair.to_bool(),
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
            cascade_schema: false,
        }
    }
}

impl SettingsPair {
    pub fn to_bool(&self) -> bool {
        self.value.eq_ignore_ascii_case(KEY_TRUE)
    }

    pub fn to_usize(&self) -> usize {
        self.value.parse().unwrap_or(1000)
    }

    pub fn to_copy_columns(&self) -> CopyColumns {
        match self.value.as_str().to_ascii_uppercase().as_str() {
            KEY_ALL => CopyColumns::All,
            KEY_MAP_ONLY => CopyColumns::MapOnly,
            _ => CopyColumns::All,
        }
    }
}
