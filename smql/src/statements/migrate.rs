use super::{filter::Filter, load::Load, mapping::MapSpec, setting::Settings};
use crate::parser::{Rule, StatementParser};
use pest::iterators::Pair;

#[derive(Default, Debug, Clone)]
pub struct MigrateBlock {
    pub migrate_items: Vec<MigrateItem>,
    pub settings: Settings,
}

#[derive(Default, Debug, Clone)]
pub struct MigrateItem {
    pub source: Spec,
    pub destination: Spec,
    pub settings: Settings,
    pub filter: Option<Filter>,
    pub load: Option<Load>,
    pub map: Option<MapSpec>,
}

#[derive(Debug, Clone)]
pub struct Spec {
    pub kind: SpecKind,
    pub names: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpecKind {
    Table,
    Api,
    Csv,
}

const KEY_TABLE: &str = "TABLE";
const KEY_API: &str = "API";
const KEY_CSV: &str = "CSV";

impl StatementParser for MigrateBlock {
    fn parse(pair: Pair<Rule>) -> Self {
        let mut migrate_block = MigrateBlock::default();

        for item_pair in pair.into_inner() {
            match item_pair.as_rule() {
                Rule::migrate_item => {
                    let mut migrate_item = MigrateItem::default();
                    let mut inner = item_pair.into_inner();

                    // First item is the source
                    let source_pair = inner.next().unwrap();
                    let source = Spec::parse(source_pair);

                    // Second item is the destination
                    let dest_pair = inner.next().unwrap();
                    let destination = Spec::parse(dest_pair);

                    migrate_item.source = source;
                    migrate_item.destination = destination;

                    let clauses_pair = inner.next().unwrap();

                    for clause in clauses_pair.into_inner() {
                        match clause.as_rule() {
                            Rule::settings_clause => {
                                migrate_item.settings = Settings::parse(clause);
                            }
                            Rule::filter_clause => {
                                let filter = Filter::parse(clause);
                                migrate_item.filter = Some(filter);
                            }
                            Rule::load_clause => {
                                let load = Load::parse(clause);
                                migrate_item.load = Some(load);
                            }
                            Rule::map_clause => {
                                let map = MapSpec::parse(clause);
                                migrate_item.map = Some(map);
                            }
                            _ => {}
                        }
                    }

                    migrate_block.migrate_items.push(migrate_item);
                }
                Rule::migrate_settings => {
                    migrate_block.settings = Settings::parse(item_pair);
                }
                _ => {}
            }
        }

        migrate_block
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
                        KEY_TABLE => SpecKind::Table,
                        KEY_API => SpecKind::Api,
                        KEY_CSV => SpecKind::Csv,
                        _ => panic!("Unknown source type: {}", inner.as_str()),
                    };
                }
                Rule::ident => {
                    names.push(inner.as_str().to_string());
                }
                Rule::string => {
                    names.push(
                        inner
                            .as_str()
                            .trim_start_matches('"')
                            .trim_end_matches('"')
                            .to_string(),
                    );
                }
                _ => {}
            }
        }

        Spec { kind, names }
    }
}

impl Default for Spec {
    fn default() -> Self {
        Spec {
            kind: SpecKind::Table,
            names: vec![],
        }
    }
}

impl Spec {
    pub fn name(&self) -> String {
        // Currently, we only support one name per spec
        self.names
            .first()
            .cloned()
            .unwrap_or_else(|| panic!("No name found for spec: {:?}", self))
    }
}
