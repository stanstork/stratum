use smql_syntax::ast_v2::expr::Expression;

#[derive(Clone, Debug)]
pub struct ComputedField {
    pub name: String,
    pub expression: Expression,
}

impl ComputedField {
    pub fn new(name: &str, expression: &Expression) -> Self {
        ComputedField {
            name: name.to_string(),
            expression: expression.clone(),
        }
    }
}
