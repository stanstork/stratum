use super::setting::Setting;
use crate::parser::{Rule, StatementParser};
use pest::iterators::Pair;

// ─────────────────────────────────────────────────────────────
// MIGRATE statement
// Example: MIGRATE (source1 -> target) WITH SETTINGS (setting1 = "value", setting2 = 42)
// ─────────────────────────────────────────────────────────────
#[derive(Debug, Clone)]
pub struct MigrateBlock {
    pub migrations: Vec<Migrate>,
    pub settings: Vec<Setting>,
}

#[derive(Debug, Clone)]
pub struct Migrate {
    pub sources: Vec<String>,
    pub target: String,
}

impl StatementParser for MigrateBlock {
    fn parse(pair: Pair<Rule>) -> Self {
        let mut migrations = vec![];
        let mut settings = vec![];

        for inner in pair.into_inner() {
            match inner.as_rule() {
                Rule::migrate_pair => {
                    migrations.push(Migrate::parse(inner));
                }
                Rule::migrate_settings => {
                    settings = inner.into_inner().map(Setting::parse).collect();
                }
                _ => {}
            }
        }

        MigrateBlock {
            migrations,
            settings,
        }
    }
}

impl StatementParser for Migrate {
    fn parse(pair: Pair<Rule>) -> Self {
        let mut inner = pair.into_inner();
        let migrate_pairs = inner
            .next()
            .expect("Expected source list")
            .into_inner()
            .filter(|e| e.as_rule() == Rule::ident)
            .map(|e| e.as_str().to_string())
            .collect::<Vec<String>>();

        let sources = migrate_pairs[..migrate_pairs.len() - 1].to_vec();
        let target = migrate_pairs.last().expect("Expected target").to_string();

        Migrate { sources, target }
    }
}
