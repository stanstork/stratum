use crate::query::ast::{
    common::TableRef,
    copy::{Copy, CopyDirection, CopyEndpoint, CopyOption},
};

#[derive(Debug, Clone)]
pub struct CopyBuilder {
    ast: Copy,
}

impl CopyBuilder {
    pub fn new(table: TableRef) -> Self {
        Self {
            ast: Copy {
                table,
                columns: Vec::new(),
                direction: CopyDirection::From,
                endpoint: CopyEndpoint::Stdin,
                options: Vec::new(),
            },
        }
    }

    pub fn columns(mut self, columns: &[&str]) -> Self {
        self.ast.columns = columns.iter().map(|s| s.to_string()).collect();
        self
    }

    pub fn direction(mut self, direction: CopyDirection) -> Self {
        self.ast.direction = direction;
        self
    }

    pub fn endpoint(mut self, endpoint: CopyEndpoint) -> Self {
        self.ast.endpoint = endpoint;
        self
    }

    pub fn option(mut self, key: &str, value: Option<&str>) -> Self {
        self.ast.options.push(CopyOption {
            key: key.to_string(),
            value: value.map(|v| v.to_string()),
        });
        self
    }

    pub fn build(self) -> Copy {
        self.ast
    }
}

#[cfg(test)]
mod tests {
    use crate::query::{
        ast::{
            common::TableRef,
            copy::{CopyDirection, CopyEndpoint},
        },
        builder::copy::CopyBuilder,
    };

    #[test]
    fn test_copy_builder_with_options() {
        let table = TableRef {
            schema: Some("public".to_string()),
            name: "users".to_string(),
        };

        let copy = CopyBuilder::new(table)
            .columns(&["id", "name"])
            .direction(CopyDirection::From)
            .endpoint(CopyEndpoint::Stdin)
            .option("FORMAT", Some("TEXT"))
            .option("DELIMITER", Some("','"))
            .build();

        assert_eq!(copy.columns, vec!["id", "name"]);
        assert_eq!(copy.options.len(), 2);
    }
}
