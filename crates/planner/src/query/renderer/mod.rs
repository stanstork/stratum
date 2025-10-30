//! Defines the core rendering trait and context for converting AST to SQL.

use model::core::value::Value;

use crate::query::dialect::Dialect;

pub mod alter_table;
pub mod create_enum;
pub mod create_table;
pub mod expr;
pub mod insert;
pub mod select;

/// A trait for any AST node that can be rendered into a SQL string.
pub trait Render {
    fn render(&self, renderer: &mut Renderer);
}

/// A context that holds the state during the rendering process.
///
/// It accumulates the SQL string and the parameters, and provides
/// access to the dialect for syntax-specific details.
pub struct Renderer<'a> {
    pub sql: String,
    pub params: Vec<Value>,
    pub dialect: &'a dyn Dialect,
}

impl<'a> Renderer<'a> {
    pub fn new(dialect: &'a dyn Dialect) -> Self {
        Self {
            sql: String::new(),
            params: Vec::new(),
            dialect,
        }
    }

    /// Consumes the renderer and returns the final SQL string and parameters.
    pub fn finish(self) -> (String, Vec<Value>) {
        (self.sql, self.params)
    }

    pub fn add_param(&mut self, value: Value) {
        self.params.push(value);
        let placeholder = self.dialect.get_placeholder(self.params.len() - 1);
        self.sql.push_str(&placeholder);
    }
}
