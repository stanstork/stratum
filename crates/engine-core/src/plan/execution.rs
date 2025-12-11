use model::execution::{
    connection::Connection, define::GlobalDefinitions, errors::ConvertError, pipeline::Pipeline,
};
use serde::{Deserialize, Serialize};
use smql_syntax::ast::doc::SmqlDocument;

use crate::plan::builder::PlanBuilder;

/// Top-level execution plan compiled from SMQL AST
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionPlan {
    pub definitions: GlobalDefinitions,
    pub connections: Vec<Connection>,
    pub pipelines: Vec<Pipeline>,
}

impl ExecutionPlan {
    /// Build execution plan from SMQL document
    pub fn build(doc: &SmqlDocument) -> Result<ExecutionPlan, ConvertError> {
        let mut builder = PlanBuilder::new();

        if let Some(def_block) = &doc.define_block {
            builder.global_definitions = builder.extract_definitions(def_block)?;
        }

        for conn_block in &doc.connections {
            let connection = builder.build_connection(conn_block)?;
            builder
                .connections
                .insert(connection.name.clone(), connection);
        }

        let mut pipelines = Vec::new();
        for pipeline_block in &doc.pipelines {
            let pipeline = builder.build_pipeline(pipeline_block)?;
            pipelines.push(pipeline);
        }

        Ok(ExecutionPlan {
            definitions: GlobalDefinitions {
                variables: builder.global_definitions,
            },
            connections: builder.connections.values().cloned().collect(),
            pipelines,
        })
    }

    pub fn get_connection(&self, name: &str) -> Option<&Connection> {
        self.connections.iter().find(|c| c.name == name)
    }

    /// Generate a deterministic hash for the plan
    pub fn hash(&self) -> String {
        use sha2::{Digest, Sha256};
        let serialized = serde_json::to_vec(self).unwrap();
        let mut hasher = Sha256::new();
        hasher.update(&serialized);
        format!("{:x}", hasher.finalize())
    }
}

#[cfg(test)]
mod tests {
    use model::{core::value::Value, execution::pipeline::WriteMode};
    use smql_syntax::builder::parse;

    use crate::plan::execution::ExecutionPlan;

    #[test]
    fn test_full_document_conversion() {
        let input = r#"
define {
    tax_rate = 1.4
}

connection "postgres_prod" {
    driver = "postgres"
    host = "localhost"
}

pipeline "copy_customers" {
    from {
        connection = connection.postgres_prod
        table = "customers"
    }

    to {
        connection = connection.postgres_prod
        table = "customers_copy"
        mode = "insert"
    }

    select {
        id = id
        total = amount * define.tax_rate
    }
}
        "#;

        let doc = parse(input).expect("Failed to parse SMQL");
        let plan = ExecutionPlan::build(&doc).expect("Failed to build execution plan");

        // Check definitions
        assert_eq!(plan.definitions.variables.len(), 1);
        assert_eq!(
            plan.definitions.variables.get("tax_rate"),
            Some(&Value::Float(1.4))
        );

        // Check connections
        assert_eq!(plan.connections.len(), 1);
        assert_eq!(plan.connections[0].name, "postgres_prod");
        assert_eq!(plan.connections[0].driver, "postgres");

        // Check pipelines
        assert_eq!(plan.pipelines.len(), 1);
        assert_eq!(plan.pipelines[0].name, "copy_customers");
        assert_eq!(plan.pipelines[0].source.table, "customers");
        assert_eq!(plan.pipelines[0].destination.table, "customers_copy");
        assert!(matches!(
            plan.pipelines[0].destination.mode,
            WriteMode::Insert
        ));

        // Check transformations
        assert_eq!(plan.pipelines[0].transformations.len(), 2);
        assert_eq!(plan.pipelines[0].transformations[0].target_field, "id");
        assert_eq!(plan.pipelines[0].transformations[1].target_field, "total");
    }
}
