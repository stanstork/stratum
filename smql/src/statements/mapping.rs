use super::expr::Expression;

#[derive(Debug)]
pub struct Map {
    pub mappings: Vec<Mapping>,
}

#[derive(Debug)]
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
