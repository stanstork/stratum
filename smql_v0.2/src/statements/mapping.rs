use super::expr::Expression;

#[derive(Debug, Clone)]
pub struct MapSpec {
    pub mappings: Vec<(Expression, String)>,
}
