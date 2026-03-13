//! Provides a fluent builder for constructing `CreateSequence` ASTs.

use crate::ast::create_sequence::CreateSequence;

pub struct CreateSequenceBuilder {
    ast: CreateSequence,
}

impl CreateSequenceBuilder {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            ast: CreateSequence {
                name: name.into(),
                if_not_exists: false,
                start: None,
                increment: None,
                min_value: None,
                max_value: None,
                owned_by: None,
            },
        }
    }

    pub fn if_not_exists(mut self) -> Self {
        self.ast.if_not_exists = true;
        self
    }

    pub fn start(mut self, value: i64) -> Self {
        self.ast.start = Some(value);
        self
    }

    pub fn increment(mut self, value: i64) -> Self {
        self.ast.increment = Some(value);
        self
    }

    pub fn min_value(mut self, value: i64) -> Self {
        self.ast.min_value = Some(value);
        self
    }

    pub fn max_value(mut self, value: i64) -> Self {
        self.ast.max_value = Some(value);
        self
    }

    pub fn owned_by(mut self, table: impl Into<String>, column: impl Into<String>) -> Self {
        self.ast.owned_by = Some((table.into(), column.into()));
        self
    }

    pub fn build(self) -> CreateSequence {
        self.ast
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_simple_sequence() {
        let seq = CreateSequenceBuilder::new("users_id_seq")
            .if_not_exists()
            .start(1)
            .increment(1)
            .build();

        assert_eq!(seq.name, "users_id_seq");
        assert!(seq.if_not_exists);
        assert_eq!(seq.start, Some(1));
        assert_eq!(seq.increment, Some(1));
    }

    #[test]
    fn test_build_owned_sequence() {
        let seq = CreateSequenceBuilder::new("users_id_seq")
            .owned_by("users", "id")
            .build();

        assert_eq!(seq.owned_by, Some(("users".to_string(), "id".to_string())));
    }
}
