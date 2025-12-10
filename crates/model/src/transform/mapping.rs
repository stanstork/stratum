use crate::{
    execution::{expr::CompiledExpression, pipeline::Pipeline},
    transform::computed_field::ComputedField,
};
use std::collections::HashMap;

/// Manages field mappings and computed fields for entities in a pipeline.
#[derive(Default, Clone, Debug)]
pub struct FieldTransformations {
    /// Maps entity name (table, file, API) to field name mapping.
    pub field_renames: HashMap<String, NameResolver>,

    /// Maps entity name to computed fields that populate new columns.
    pub computed_fields: HashMap<String, Vec<ComputedField>>,
}

/// Bidirectional case-insensitive name mapping.
#[derive(Clone, Debug, Default)]
pub struct NameResolver {
    pub source_to_target: HashMap<String, String>, // old_name -> new_name
    pub target_to_source: HashMap<String, String>, // new_name -> old_name
}

/// Represents a field reference from a foreign entity (joined table).
#[derive(Clone, Debug)]
pub struct CrossEntityReference {
    /// The entity name (table, file, API) where the field is located.
    pub entity: String,
    /// The field name (column) being referenced.
    pub field: String,
    /// The target field name in the destination entity.
    pub target: Option<String>,
}

/// Complete mapping information for a pipeline's data transformation.
///
/// Tracks entity name mappings, field renames, computed fields, and cross-entity references.
#[derive(Clone, Debug)]
pub struct TransformationMetadata {
    /// Maps each source entity name to its corresponding destination entity name.
    pub entities: NameResolver,

    /// Field transformations (renames and computed fields) for each entity.
    pub field_mappings: FieldTransformations,

    /// Cross-entity references grouped by the entity they reference.
    pub foreign_fields: HashMap<String, Vec<CrossEntityReference>>,
}

impl FieldTransformations {
    pub fn new() -> Self {
        Self {
            field_renames: HashMap::new(),
            computed_fields: HashMap::new(),
        }
    }

    /// Extracts field mappings from a pipeline's transformations.
    pub fn from_pipeline(pipeline: &Pipeline) -> Self {
        let mut entity_map = Self::new();
        let entity = pipeline.destination.table.to_ascii_lowercase();

        let mut field_map = HashMap::new();
        let mut computed_fields = Vec::new();

        for transform in &pipeline.transformations {
            match &transform.expression {
                CompiledExpression::Identifier(field) => {
                    field_map.insert(
                        transform.target_field.to_ascii_lowercase(),
                        field.to_ascii_lowercase(),
                    );
                }
                other_expr => {
                    computed_fields.push(ComputedField::new(&transform.target_field, other_expr));
                }
            }
        }

        entity_map.add_mapping(&entity, field_map);
        entity_map.add_computed(&entity, computed_fields);

        entity_map
    }

    pub fn add_mapping(&mut self, entity: &str, map: HashMap<String, String>) {
        self.field_renames
            .insert(entity.to_string(), NameResolver::new(map));
    }

    pub fn add_computed(&mut self, entity: &str, computed: Vec<ComputedField>) {
        self.computed_fields.insert(entity.to_string(), computed);
    }

    pub fn get_entity(&self, entity: &str) -> Option<&NameResolver> {
        self.field_renames.get(entity)
    }

    pub fn get_computed(&self, entity: &str) -> Option<&Vec<ComputedField>> {
        self.computed_fields.get(entity)
    }

    pub fn resolve(&self, entity: &str, name: &str) -> String {
        if let Some(name_map) = self.field_renames.get(entity) {
            name_map.resolve(name)
        } else {
            name.to_string()
        }
    }

    pub fn reverse_resolve(&self, entity: &str, name: &str) -> String {
        if let Some(name_map) = self.field_renames.get(entity) {
            name_map.reverse_resolve(name)
        } else {
            name.to_string()
        }
    }

    pub fn is_empty(&self) -> bool {
        self.field_renames.is_empty() && self.computed_fields.is_empty()
    }

    pub fn contains(&self, entity: &str) -> bool {
        self.field_renames.contains_key(entity)
    }
}

impl NameResolver {
    pub fn new(map: HashMap<String, String>) -> Self {
        let mut source_to_target = HashMap::new();
        let mut target_to_source = HashMap::new();

        for (k, v) in map.into_iter() {
            let k_lower = k.to_ascii_lowercase();
            let v_lower = v.to_ascii_lowercase();

            source_to_target.insert(k_lower.clone(), v_lower.clone());
            target_to_source.insert(v_lower, k_lower);
        }

        Self {
            source_to_target,
            target_to_source,
        }
    }

    /// Resolve old -> new (default direction)
    pub fn resolve(&self, name: &str) -> String {
        let lower = name.to_ascii_lowercase();
        self.source_to_target
            .get(&lower)
            .cloned()
            .unwrap_or_else(|| name.to_string())
    }

    /// Reverse resolve new -> old
    pub fn reverse_resolve(&self, name: &str) -> String {
        let lower = name.to_ascii_lowercase();
        self.target_to_source
            .get(&lower)
            .cloned()
            .unwrap_or_else(|| name.to_string())
    }

    pub fn is_empty(&self) -> bool {
        self.source_to_target.is_empty() && self.target_to_source.is_empty()
    }

    /// Extracts entity name mappings from a pipeline.
    pub fn from_pipeline(pipeline: &Pipeline) -> Self {
        let mut name_map = HashMap::new();

        let src = pipeline.source.table.to_ascii_lowercase();
        let dst = pipeline.destination.table.to_ascii_lowercase();

        name_map.insert(src, dst);

        for join in &pipeline.source.joins {
            name_map.insert(
                join.alias.to_ascii_lowercase(),
                join.table.to_ascii_lowercase(),
            );
        }

        Self::new(name_map)
    }

    pub fn forward_map(&self) -> HashMap<String, String> {
        self.source_to_target.clone()
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.source_to_target.contains_key(key)
    }

    pub fn contains_target_key(&self, key: &str) -> bool {
        self.target_to_source.contains_key(key)
    }
}

impl TransformationMetadata {
    /// Creates a new TransformationMetadata from a pipeline.
    pub fn new(pipeline: &Pipeline) -> Self {
        let entity_name_map = NameResolver::from_pipeline(pipeline);
        let field_mappings = FieldTransformations::from_pipeline(pipeline);
        let foreign_fields = Self::extract_all_cross_entity_refs(&field_mappings);

        Self {
            entities: entity_name_map,
            field_mappings,
            foreign_fields,
        }
    }

    /// Returns the entity name resolver (source <-> destination table names).
    pub fn entity_names(&self) -> &NameResolver {
        &self.entities
    }

    /// Returns the field transformations.
    pub fn fields(&self) -> &FieldTransformations {
        &self.field_mappings
    }

    /// Returns computed fields for a specific entity.
    pub fn get_computed_fields(&self, entity: &str) -> Option<&Vec<ComputedField>> {
        self.field_mappings.get_computed(entity)
    }

    /// Returns cross-entity references that reference a specific entity.
    pub fn get_cross_entity_refs_for(&self, entity: &str) -> &[CrossEntityReference] {
        self.foreign_fields
            .get(entity)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    /// Extracts all cross-entity references from computed expressions, grouped by entity.
    fn extract_all_cross_entity_refs(
        field_mappings: &FieldTransformations,
    ) -> HashMap<String, Vec<CrossEntityReference>> {
        let mut cross_entity_refs: HashMap<String, Vec<CrossEntityReference>> = HashMap::new();

        for computed_list in field_mappings.computed_fields.values() {
            for computed in computed_list {
                // collect *all* cross-entity references inside this computed field
                let mut found = Vec::new();
                Self::extract_cross_entity_refs(
                    &computed.expression,
                    &Some(computed.name.clone()),
                    &mut found,
                );

                // group them by entity
                for ref_item in found {
                    cross_entity_refs
                        .entry(ref_item.entity.clone())
                        .or_default()
                        .push(ref_item);
                }
            }
        }

        cross_entity_refs
    }

    /// Walks `expr` and pushes every `CrossEntityReference` it finds into `out`.
    fn extract_cross_entity_refs(
        expr: &CompiledExpression,
        target: &Option<String>,
        out: &mut Vec<CrossEntityReference>,
    ) {
        match expr {
            // DotPath represents table.column cross-entity reference
            CompiledExpression::DotPath(segments) if segments.len() >= 2 => {
                // First segment is the entity (table), second is the field (column)
                out.push(CrossEntityReference {
                    entity: segments[0].clone(),
                    field: segments[1].clone(),
                    target: target.clone(),
                });
            }

            // Recurse into binary operations
            CompiledExpression::Binary { left, right, .. } => {
                Self::extract_cross_entity_refs(left, target, out);
                Self::extract_cross_entity_refs(right, target, out);
            }

            // Recurse into unary operations
            CompiledExpression::Unary { operand, .. } => {
                Self::extract_cross_entity_refs(operand, target, out);
            }

            // Recurse into function call arguments
            CompiledExpression::FunctionCall { args, .. } => {
                for arg in args {
                    Self::extract_cross_entity_refs(arg, &None, out);
                }
            }

            // Recurse into array elements
            CompiledExpression::Array(elements) => {
                for elem in elements {
                    Self::extract_cross_entity_refs(elem, target, out);
                }
            }

            // Recurse into when expression branches
            CompiledExpression::When {
                branches,
                else_expr,
            } => {
                for branch in branches {
                    Self::extract_cross_entity_refs(&branch.condition, target, out);
                    Self::extract_cross_entity_refs(&branch.value, target, out);
                }
                if let Some(else_val) = else_expr {
                    Self::extract_cross_entity_refs(else_val, target, out);
                }
            }

            // Recurse into null checks
            CompiledExpression::IsNull(expr) | CompiledExpression::IsNotNull(expr) => {
                Self::extract_cross_entity_refs(expr, target, out);
            }

            // Recurse into grouped expressions
            CompiledExpression::Grouped(expr) => {
                Self::extract_cross_entity_refs(expr, target, out);
            }

            // Base cases: literals and identifiers never contain nested cross-entity references
            CompiledExpression::Literal(_) | CompiledExpression::Identifier(_) => {}

            // Single-segment DotPath is just a field reference, not a cross-entity reference
            CompiledExpression::DotPath(_) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::value::Value;
    use crate::execution::connection::Connection;
    use crate::execution::properties::Properties;
    use crate::execution::{
        expr::{BinaryOp, CompiledExpression, WhenBranch},
        pipeline::{DataDestination, DataSource, Join, Pipeline, Transformation, WriteMode},
    };

    fn make_test_pipeline() -> Pipeline {
        Pipeline {
            name: "test_pipeline".to_string(),
            description: None,
            dependencies: vec![],
            source: DataSource {
                connection: Connection {
                    name: "source_db".to_string(),
                    driver: "postgres".to_string(),
                    properties: Properties::new(),
                    nested_configs: HashMap::new(),
                },
                table: "customers".to_string(),
                filters: vec![],
                joins: vec![
                    Join {
                        alias: "orders".to_string(),
                        table: "orders".to_string(),
                        condition: None,
                    },
                    Join {
                        alias: "users".to_string(),
                        table: "users".to_string(),
                        condition: None,
                    },
                ],
                pagination: None,
            },
            destination: DataDestination {
                connection: Connection {
                    name: "destination_db".to_string(),
                    driver: "postgres".to_string(),
                    properties: Properties::new(),
                    nested_configs: HashMap::new(),
                },
                table: "customers_clean".to_string(),
                mode: WriteMode::Insert,
            },
            transformations: vec![
                // Simple field rename: id = id
                Transformation {
                    target_field: "id".to_string(),
                    expression: CompiledExpression::Identifier("id".to_string()),
                },
                // Simple field rename: customer_name = name
                Transformation {
                    target_field: "customer_name".to_string(),
                    expression: CompiledExpression::Identifier("name".to_string()),
                },
                // Computed field with arithmetic: total = amount * 1.4
                Transformation {
                    target_field: "total".to_string(),
                    expression: CompiledExpression::Binary {
                        left: Box::new(CompiledExpression::Identifier("amount".to_string())),
                        op: BinaryOp::Multiply,
                        right: Box::new(CompiledExpression::Literal(Value::Float(1.4))),
                    },
                },
                // Cross-entity reference: discount = users.discount_rate
                Transformation {
                    target_field: "discount".to_string(),
                    expression: CompiledExpression::DotPath(vec![
                        "users".to_string(),
                        "discount_rate".to_string(),
                    ]),
                },
                // Complex expression with multiple cross-entity references
                Transformation {
                    target_field: "final_price".to_string(),
                    expression: CompiledExpression::Binary {
                        left: Box::new(CompiledExpression::Identifier("amount".to_string())),
                        op: BinaryOp::Multiply,
                        right: Box::new(CompiledExpression::DotPath(vec![
                            "orders".to_string(),
                            "quantity".to_string(),
                        ])),
                    },
                },
            ],
            validations: vec![],
            lifecycle: None,
            error_handling: None,
            settings: std::collections::HashMap::new(),
        }
    }

    #[test]
    fn test_entity_name_map() {
        let pipeline = make_test_pipeline();
        let mapping = TransformationMetadata::new(&pipeline);

        // Check source -> destination mapping
        assert_eq!(mapping.entities.resolve("customers"), "customers_clean");
        assert_eq!(
            mapping.entities.reverse_resolve("customers_clean"),
            "customers"
        );

        // Check joined tables map to themselves
        assert_eq!(mapping.entities.resolve("orders"), "orders");
        assert_eq!(mapping.entities.resolve("users"), "users");
    }

    #[test]
    fn test_field_mappings_simple_renames() {
        let pipeline = make_test_pipeline();
        let mapping = TransformationMetadata::new(&pipeline);

        let entity = "customers_clean";

        // Check simple field renames (source -> target direction in FieldTransformations::from_pipeline)
        // The mapping is: target_field -> source_field (reversed from Transformation)
        assert_eq!(
            mapping
                .field_mappings
                .get_entity(entity)
                .unwrap()
                .resolve("id"),
            "id"
        );
        // "customer_name" (target) maps from "name" (source)
        // So resolve("customer_name") should return source field name
        assert_eq!(
            mapping
                .field_mappings
                .get_entity(entity)
                .unwrap()
                .resolve("customer_name"),
            "name"
        );
    }

    #[test]
    fn test_computed_fields() {
        let pipeline = make_test_pipeline();
        let mapping = TransformationMetadata::new(&pipeline);

        let entity = "customers_clean";
        let computed = mapping
            .field_mappings
            .get_computed(entity)
            .expect("Should have computed fields");

        // Should have 3 computed fields: total, discount, final_price
        assert_eq!(computed.len(), 3);

        let field_names: Vec<&str> = computed.iter().map(|f| f.name.as_str()).collect();
        assert!(field_names.contains(&"total"));
        assert!(field_names.contains(&"discount"));
        assert!(field_names.contains(&"final_price"));
    }

    #[test]
    fn test_cross_entity_refs_extraction() {
        let pipeline = make_test_pipeline();
        let mapping = TransformationMetadata::new(&pipeline);

        // Check cross-entity references for users table
        let users_refs = mapping.get_cross_entity_refs_for("users");
        assert_eq!(users_refs.len(), 1);
        assert_eq!(users_refs[0].entity, "users");
        assert_eq!(users_refs[0].field, "discount_rate");
        assert_eq!(users_refs[0].target, Some("discount".to_string()));

        // Check cross-entity references for orders table
        let orders_refs = mapping.get_cross_entity_refs_for("orders");
        assert_eq!(orders_refs.len(), 1);
        assert_eq!(orders_refs[0].entity, "orders");
        assert_eq!(orders_refs[0].field, "quantity");
        assert_eq!(orders_refs[0].target, Some("final_price".to_string()));
    }

    #[test]
    fn test_extract_cross_entity_refs_from_nested_expressions() {
        let mut refs = Vec::new();

        // Test nested binary expression: (users.rate + orders.tax) * 2
        let expr = CompiledExpression::Binary {
            left: Box::new(CompiledExpression::Binary {
                left: Box::new(CompiledExpression::DotPath(vec![
                    "users".to_string(),
                    "rate".to_string(),
                ])),
                op: BinaryOp::Add,
                right: Box::new(CompiledExpression::DotPath(vec![
                    "orders".to_string(),
                    "tax".to_string(),
                ])),
            }),
            op: BinaryOp::Multiply,
            right: Box::new(CompiledExpression::Literal(Value::Float(2.0))),
        };

        TransformationMetadata::extract_cross_entity_refs(
            &expr,
            &Some("result".to_string()),
            &mut refs,
        );

        assert_eq!(refs.len(), 2);
        assert!(
            refs.iter()
                .any(|l| l.entity == "users" && l.field == "rate")
        );
        assert!(
            refs.iter()
                .any(|l| l.entity == "orders" && l.field == "tax")
        );
    }

    #[test]
    fn test_extract_cross_entity_refs_from_when_expression() {
        let mut refs = Vec::new();

        // Test when expression: when users.active then users.discount else 0
        let expr = CompiledExpression::When {
            branches: vec![WhenBranch {
                condition: CompiledExpression::DotPath(vec![
                    "users".to_string(),
                    "active".to_string(),
                ]),
                value: CompiledExpression::DotPath(vec![
                    "users".to_string(),
                    "discount".to_string(),
                ]),
            }],
            else_expr: Some(Box::new(CompiledExpression::Literal(Value::Float(0.0)))),
        };

        TransformationMetadata::extract_cross_entity_refs(
            &expr,
            &Some("discount_value".to_string()),
            &mut refs,
        );

        assert_eq!(refs.len(), 2);
        assert!(
            refs.iter()
                .any(|l| l.entity == "users" && l.field == "active")
        );
        assert!(
            refs.iter()
                .any(|l| l.entity == "users" && l.field == "discount")
        );
    }

    #[test]
    fn test_extract_cross_entity_refs_from_function_call() {
        let mut refs = Vec::new();

        // Test function call: sum(orders.amount, users.balance)
        let expr = CompiledExpression::FunctionCall {
            name: "sum".to_string(),
            args: vec![
                CompiledExpression::DotPath(vec!["orders".to_string(), "amount".to_string()]),
                CompiledExpression::DotPath(vec!["users".to_string(), "balance".to_string()]),
            ],
        };

        TransformationMetadata::extract_cross_entity_refs(
            &expr,
            &Some("total".to_string()),
            &mut refs,
        );

        assert_eq!(refs.len(), 2);
        assert!(
            refs.iter()
                .any(|l| l.entity == "orders" && l.field == "amount")
        );
        assert!(
            refs.iter()
                .any(|l| l.entity == "users" && l.field == "balance")
        );
    }

    #[test]
    fn test_extract_cross_entity_refs_ignores_single_segment_dotpath() {
        let mut refs = Vec::new();

        // Single segment DotPath is just a field reference, not a cross-entity reference
        let expr = CompiledExpression::DotPath(vec!["column_name".to_string()]);

        TransformationMetadata::extract_cross_entity_refs(
            &expr,
            &Some("target".to_string()),
            &mut refs,
        );

        assert_eq!(
            refs.len(),
            0,
            "Single-segment DotPath should not be treated as cross-entity reference"
        );
    }

    #[test]
    fn test_extract_cross_entity_refs_from_null_checks() {
        let mut refs = Vec::new();

        // Test: orders.status is not null
        let expr = CompiledExpression::IsNotNull(Box::new(CompiledExpression::DotPath(vec![
            "orders".to_string(),
            "status".to_string(),
        ])));

        TransformationMetadata::extract_cross_entity_refs(
            &expr,
            &Some("has_status".to_string()),
            &mut refs,
        );

        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].entity, "orders");
        assert_eq!(refs[0].field, "status");
    }

    #[test]
    fn test_name_resolver_case_insensitive() {
        let mut map = HashMap::new();
        map.insert("OldName".to_string(), "NewName".to_string());
        let name_resolver = NameResolver::new(map);

        // NameResolver converts to lowercase internally, so all case variations work
        assert_eq!(name_resolver.resolve("oldname"), "newname");
        assert_eq!(name_resolver.resolve("OLDNAME"), "newname");
        assert_eq!(name_resolver.resolve("OldName"), "newname");

        // Reverse resolve also works case-insensitively
        assert_eq!(name_resolver.reverse_resolve("newname"), "oldname");
        assert_eq!(name_resolver.reverse_resolve("NEWNAME"), "oldname");
        assert_eq!(name_resolver.reverse_resolve("NewName"), "oldname");

        // Non-existent keys return the original input
        assert_eq!(name_resolver.resolve("UnknownField"), "UnknownField");
        assert_eq!(
            name_resolver.reverse_resolve("AnotherUnknown"),
            "AnotherUnknown"
        );
    }

    #[test]
    fn test_field_mappings_is_empty() {
        let mut mappings = FieldTransformations::new();
        assert!(mappings.is_empty());

        mappings.add_mapping(
            "test",
            vec![("a".to_string(), "b".to_string())]
                .into_iter()
                .collect(),
        );
        assert!(!mappings.is_empty());
    }

    #[test]
    fn test_field_mappings_contains() {
        let mut mappings = FieldTransformations::new();
        mappings.add_mapping(
            "test_entity",
            vec![("field1".to_string(), "field2".to_string())]
                .into_iter()
                .collect(),
        );

        assert!(mappings.contains("test_entity"));
        assert!(!mappings.contains("other_entity"));
    }

    #[test]
    fn test_field_mappings_resolve() {
        let mut mappings = FieldTransformations::new();
        mappings.add_mapping(
            "entity",
            vec![("old_field".to_string(), "new_field".to_string())]
                .into_iter()
                .collect(),
        );

        assert_eq!(mappings.resolve("entity", "old_field"), "new_field");
        assert_eq!(mappings.resolve("entity", "unknown_field"), "unknown_field");
        assert_eq!(mappings.resolve("unknown_entity", "field"), "field");
    }
}
