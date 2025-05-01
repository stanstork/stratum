use crate::parser::StatementParser;

use super::{filter::Filter, load::Load, mapping::MapSpec, setting::Settings};

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
    fn parse(pair: pest::iterators::Pair<crate::parser::Rule>) -> Self {
        let mut inner = pair.into_inner();

        todo!("Parse MigrateBlock");
    }
}
