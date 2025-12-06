use crate::ast::{
    attribute::Attribute,
    expr::Expression,
    ident::Identifier,
    span::Span,
    validation::{OnErrorBlock, ValidateBlock},
};

/// Pipeline block for data transformations
/// Syntax: pipeline "copy_customers" { from { ... }, to { ... }, ... }
#[derive(Debug, Clone, PartialEq)]
pub struct PipelineBlock {
    pub name: String,
    pub after: Option<Vec<Expression>>,
    pub from: Option<FromBlock>,
    pub to: Option<ToBlock>,
    pub where_clauses: Vec<WhereClause>,
    pub with_block: Option<WithBlock>,
    pub select_block: Option<SelectBlock>,
    pub validate_block: Option<ValidateBlock>,
    pub on_error_block: Option<OnErrorBlock>,
    pub paginate_block: Option<PaginateBlock>,
    pub before_block: Option<BeforeBlock>,
    pub after_block: Option<AfterBlock>,
    pub settings_block: Option<SettingsBlock>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FromBlock {
    pub attributes: Vec<Attribute>,
    pub nested_blocks: Vec<NestedBlock>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ToBlock {
    pub attributes: Vec<Attribute>,
    pub nested_blocks: Vec<NestedBlock>,
    pub span: Span,
}

/// Generic nested block (e.g., pool { max_size = 20 })
#[derive(Debug, Clone, PartialEq)]
pub struct NestedBlock {
    pub kind: String,
    pub attributes: Vec<Attribute>,
    pub span: Span,
}

/// Where clause with optional label
/// Syntax: where "active_only" { customers.status == "active" }
#[derive(Debug, Clone, PartialEq)]
pub struct WhereClause {
    pub label: Option<String>,
    pub conditions: Vec<Expression>,
    pub span: Span,
}

/// With block for joins
/// Syntax: with { users from users where users.id == orders.user_id }
#[derive(Debug, Clone, PartialEq)]
pub struct WithBlock {
    pub joins: Vec<JoinClause>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub struct JoinClause {
    pub alias: Identifier,
    pub table: Identifier,
    pub condition: Option<Expression>,
    pub span: Span,
}

/// Select block for field mappings
#[derive(Debug, Clone, PartialEq)]
pub struct SelectBlock {
    pub fields: Vec<FieldMapping>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FieldMapping {
    pub name: Identifier,
    pub value: Expression,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PaginateBlock {
    pub attributes: Vec<Attribute>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BeforeBlock {
    pub sql: Vec<String>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AfterBlock {
    pub sql: Vec<String>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SettingsBlock {
    pub attributes: Vec<Attribute>,
    pub span: Span,
}
