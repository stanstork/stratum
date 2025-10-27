use crate::ast::{common::TypeName, create_enum::CreateEnum};

pub struct CreateEnumBuilder {
    ast: CreateEnum,
}

impl CreateEnumBuilder {
    pub fn new(name: TypeName, values: &[String]) -> Self {
        Self {
            ast: CreateEnum {
                name,
                values: values.to_vec(),
            },
        }
    }

    pub fn build(self) -> CreateEnum {
        self.ast
    }
}

#[cfg(test)]
mod tests {
    use crate::{ast::common::TypeName, build::create_enum::CreateEnumBuilder};

    #[test]
    fn test_create_enum_builder() {
        let builder = CreateEnumBuilder::new(
            TypeName {
                schema: None,
                name: "mood".to_string(),
            },
            &[
                "happy".to_string(),
                "sad".to_string(),
                "neutral".to_string(),
            ],
        );
        let ast = builder.build();
        assert_eq!(ast.name.name, "mood");
        assert_eq!(ast.values, vec!["happy", "sad", "neutral"]);
    }
}
