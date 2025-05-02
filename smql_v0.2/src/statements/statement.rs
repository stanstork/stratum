use super::{
    connection::Connection,
    migrate::MigrateBlock,
    setting::{Settings, SettingsPair},
};
use crate::parser::{Rule, StatementParser};
use pest::iterators::Pair;

#[derive(Debug)]
pub enum Statement {
    Connection(Connection),
    Migrate(MigrateBlock),
    GlobalSettings(Settings),
    EOI,
}

impl StatementParser for Statement {
    fn parse(pair: Pair<Rule>) -> Self {
        match pair.as_rule() {
            Rule::connections => {
                let connection = Connection::parse(pair);
                Statement::Connection(connection)
            }
            Rule::migrate => Statement::Migrate(MigrateBlock::parse(pair)),
            Rule::migrate_settings => {
                let setting_pairs = pair
                    .into_inner()
                    .map(SettingsPair::parse)
                    .collect::<Vec<_>>();
                println!("Parsed settings: {:#?}", setting_pairs);
                let settings = Settings::from_pairs(setting_pairs);
                Statement::GlobalSettings(settings)
            }
            _ => Statement::EOI,
        }
    }
}
