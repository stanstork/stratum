use crate::execution::expr::CompiledExpression;

#[derive(Clone, Debug)]
pub struct ComputedField {
    pub name: String,
    pub expression: CompiledExpression,
}

impl ComputedField {
    pub fn new(name: &str, expression: &CompiledExpression) -> Self {
        ComputedField {
            name: name.to_string(),
            expression: expression.clone(),
        }
    }
}
