use super::expr::Expression;
use crate::parser::{Rule, StatementParser};
use pest::iterators::Pair;

// ─────────────────────────────────────────────────────────────
// MAP statement
// Example: MAP (price * 1.2 -> price_with_tax)
// ─────────────────────────────────────────────────────────────
#[derive(Debug)]
pub struct Map {
    pub mappings: Vec<ScopeMapping>,
}

#[derive(Debug, Clone)]
pub struct ScopeMapping {
    pub scope: String,
    pub mappings: Vec<Mapping>,
}

#[derive(Debug, Clone)]
pub enum Mapping {
    ColumnToColumn {
        source: String,
        target: String,
    },
    ExpressionToColumn {
        expression: Expression,
        target: String,
    },
}

impl StatementParser for Map {
    fn parse(pair: Pair<Rule>) -> Self {
        let table_mappings = pair
            .into_inner()
            .filter(|p| p.as_rule() == Rule::table_mapping)
            .map(ScopeMapping::parse)
            .collect();

        Map {
            mappings: table_mappings,
        }
    }
}

impl StatementParser for ScopeMapping {
    fn parse(pair: Pair<Rule>) -> Self {
        let mut inner = pair.into_inner();
        let table = inner
            .next()
            .expect("Expected table name")
            .as_str()
            .to_string();

        let mappings = inner
            .filter(|p| p.as_rule() == Rule::mapping)
            .map(Mapping::parse)
            .collect();

        ScopeMapping {
            scope: table,
            mappings,
        }
    }
}

impl StatementParser for Mapping {
    fn parse(pair: Pair<Rule>) -> Self {
        let mut inner = pair.into_inner();
        let source_pair = inner.next().expect("Expected source expression");
        let target = inner
            .next()
            .expect("Expected target column name")
            .as_str()
            .to_string();

        match source_pair.as_rule() {
            Rule::expression => {
                let expression = Expression::parse(source_pair);
                Mapping::ExpressionToColumn { expression, target }
            }
            Rule::ident => Mapping::ColumnToColumn {
                source: source_pair.as_str().to_string(),
                target,
            },
            _ => panic!("Unexpected source type in mapping: {:?}", source_pair),
        }
    }
}
