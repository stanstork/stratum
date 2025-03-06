use super::setting::Setting;
use crate::parser::{Rule, StatementParser};
use pest::iterators::Pair;

// ─────────────────────────────────────────────────────────────
// MIGRATE statement
// Example: MIGRATE source1,source2 TO target WITH SETTINGS (setting1 = "value", setting2 = 42)
// ─────────────────────────────────────────────────────────────
#[derive(Debug)]
pub struct Migrate {
    pub source: Vec<String>,
    pub target: String,
    pub settings: Vec<Setting>,
}

impl StatementParser for Migrate {
    fn parse(pair: Pair<Rule>) -> Self {
        let mut inner = pair.into_inner();

        let source = inner
            .next()
            .expect("Expected source")
            .as_str()
            .split(',')
            .map(String::from)
            .collect();

        let target = inner.next().expect("Expected target").as_str().to_string();

        let settings = inner
            .filter(|p| p.as_rule() == Rule::migrate_settings)
            .flat_map(|p| p.into_inner().map(Setting::parse))
            .collect();

        Migrate {
            source,
            target,
            settings,
        }
    }
}
