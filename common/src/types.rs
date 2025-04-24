use async_trait::async_trait;
use smql::statements::expr::{Literal, Operator};

#[async_trait]
pub trait TypeResolver {
    type Type: Send + Sync;

    /// Called when you see a string/number/bool literal.
    async fn resolve_literal(&self, lit: &Literal) -> Option<Self::Type>;

    /// Called when you see a bare identifier `foo`.
    async fn resolve_identifier(&self, ident: &str) -> Option<Self::Type>;

    /// Called when you see `table[key]`.
    async fn resolve_lookup(&self, table: &str, key: &str) -> Option<Self::Type>;

    /// Called when you see `left OP right`.
    async fn resolve_arithmetic(
        &self,
        left: &Self::Type,
        op: &Operator,
        right: &Self::Type,
    ) -> Option<Self::Type>;
}
