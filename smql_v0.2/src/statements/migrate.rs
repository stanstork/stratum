use super::{filter::Filter, load::Load, mapping::MapSpec, setting::Settings};
use crate::{
    parser::{Rule, StatementParser},
    statements::setting::SettingsPair,
};
use pest::iterators::Pair;

#[derive(Debug, Clone)]
pub struct MigrateBlock {
    pub source: Spec,
    pub destination: Spec,
    pub settings: Vec<Settings>,
    pub filter: Option<Filter>,
    pub load: Option<Load>,
    pub map: Option<MapSpec>,
}

#[derive(Debug, Clone)]
pub struct Spec {
    pub kind: SpecKind,
    pub names: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum SpecKind {
    Table,
    Api,
    File,
}

impl StatementParser for MigrateBlock {
    fn parse(pair: Pair<Rule>) -> Self {
        for item_pair in pair
            .into_inner()
            .filter(|p| p.as_rule() == Rule::migrate_item)
        {
            let mut inner = item_pair.into_inner();

            // First item is the source
            let source_pair = inner.next().unwrap();
            let source = Spec::parse(source_pair);

            // Second item is the destination
            let dest_pair = inner.next().unwrap();
            let destination = Spec::parse(dest_pair);

            println!("Source: {:?}", source);
            println!("Destination: {:?}", destination);

            let clauses_pair = inner.next().unwrap();
            for clause in clauses_pair.into_inner() {
                match clause.as_rule() {
                    Rule::settings_clause => {
                        let settings = clause
                            .into_inner()
                            .map(SettingsPair::parse)
                            .collect::<Vec<_>>();
                        println!("Settings: {:?}", settings);
                    }
                    _ => {}
                }
            }
        }

        todo!("Parse MigrateBlock");
    }
}

impl StatementParser for Spec {
    fn parse(pair: Pair<Rule>) -> Self {
        let mut kind = SpecKind::Table;
        let mut names = vec![];

        for inner in pair.into_inner() {
            match inner.as_rule() {
                Rule::source_type => {
                    kind = match inner.as_str().to_ascii_uppercase().as_str() {
                        "TABLE" => SpecKind::Table,
                        "API" => SpecKind::Api,
                        _ => panic!("Unknown source type: {}", inner.as_str()),
                    };
                }
                Rule::ident => {
                    names.push(inner.as_str().to_string());
                }
                _ => {}
            }
        }

        Spec { kind, names }
    }
}
