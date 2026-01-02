use crate::{
    builder::{
        analysis::{AnalysisContext, AnalyzerError, AnalyzerResult, PlanAnalyzer},
        errors::MappingAnalyzerError,
    },
    plan::{
        pipeline::source::SourcePlan,
        transform::{
            mapping::{ColumnMapping, ConditionalBranch, MappingSource, MappingType},
            type_conversion::{ConversionMethod, TypeConversion},
        },
    },
};
use async_trait::async_trait;
use connectors::sql::base::{metadata::column::ColumnMetadata, query::column::ColumnDef};
use engine_core::schema::plan::SchemaPlan;
use expression_engine::ExpressionAnalyzer;
use model::{
    core::data_type::DataType,
    execution::{expr::CompiledExpression, pipeline::Transformation},
    transform::mapping::TransformationMetadata,
};
use std::sync::Arc;
use tracing::{info, warn};

/// Analyzes column transformations and mappings to create a detailed physical execution plan.
pub struct MappingAnalyzer {
    schema_plan: Arc<SchemaPlan>,
    mapping: TransformationMetadata,
}

impl MappingAnalyzer {
    pub fn new(schema_plan: Arc<SchemaPlan>, mapping: &TransformationMetadata) -> Self {
        Self {
            schema_plan,
            mapping: mapping.clone(),
        }
    }

    /// Performs the core analysis of a single transformation.
    pub async fn analyze_single_mapping(
        &self,
        transformation: &Transformation,
        source_plan: &SourcePlan,
        resolved_columns: &[ColumnDef],
    ) -> Result<ColumnMapping, MappingAnalyzerError> {
        let target = transformation.target_field.clone();

        // Resolve source logic and data types
        let (source, mapping_type, source_data_type) = self
            .analyze_expression(transformation, source_plan, resolved_columns)
            .await?;

        // Infer target metadata from the destination schema
        let target_data_type = self.get_target_data_type(&target, resolved_columns)?;
        let nullable = self.check_target_nullability(&target, resolved_columns)?;

        // Evaluate conversion safety and requirements
        let type_conversion = self.evaluate_conversion_safety(&source_data_type, &target_data_type);

        Ok(ColumnMapping {
            target,
            source,
            mapping_type,
            source_type: Some(source_data_type.mysql_name().to_string()),
            target_type: target_data_type.postgres_name().to_string(),
            type_conversion,
            nullable,
        })
    }

    /// Analyze a compiled expression to determine the mapping source
    async fn analyze_expression(
        &self,
        transformation: &Transformation,
        source_plan: &SourcePlan,
        resolved_columns: &[ColumnDef],
    ) -> Result<(MappingSource, MappingType, DataType), MappingAnalyzerError> {
        match &transformation.expression {
            CompiledExpression::Identifier(name) => {
                let physical_table = self.mapping.entities.resolve(&source_plan.table);
                let meta = self.fetch_column_meta(&physical_table, name)?;

                Ok((
                    MappingSource::Column {
                        table: source_plan.table.clone(),
                        column: name.clone(),
                    },
                    MappingType::Direct,
                    meta.data_type,
                ))
            }

            CompiledExpression::DotPath(segments) if segments.len() >= 2 => {
                let alias = &segments[0];
                let column = &segments[1];
                let table = self.mapping.entities.reverse_resolve(alias);
                let meta = self.fetch_column_meta(&table, column)?;

                let m_type = if table == source_plan.table {
                    MappingType::Direct
                } else {
                    MappingType::Lookup
                };

                Ok((
                    MappingSource::Column {
                        table,
                        column: column.clone(),
                    },
                    m_type,
                    meta.data_type,
                ))
            }

            CompiledExpression::Literal(value) => {
                let dtype = value.data_type().clone();
                let (val_str, _) = ExpressionAnalyzer::format_literal(value);

                Ok((
                    MappingSource::Constant {
                        value: val_str,
                        value_type: dtype.mysql_name().to_string(),
                    },
                    MappingType::Constant,
                    dtype,
                ))
            }

            // Complex logic: Binary ops, functions, or conditionals
            _ => self.analyze_complex_expression(transformation, resolved_columns),
        }
    }

    /// Handles complex computed logic using the SchemaPlan for inferred types.
    fn analyze_complex_expression(
        &self,
        transformation: &Transformation,
        resolved_columns: &[ColumnDef],
    ) -> Result<(MappingSource, MappingType, DataType), MappingAnalyzerError> {
        let inferred_type =
            self.get_target_data_type(&transformation.target_field, resolved_columns)?;
        let expr = &transformation.expression;

        let source = match expr {
            CompiledExpression::Binary { .. } => MappingSource::Expression {
                expression: ExpressionAnalyzer::to_string(expr),
                columns_referenced: ExpressionAnalyzer::extract_columns(expr),
                functions_used: ExpressionAnalyzer::extract_functions(expr),
            },
            CompiledExpression::FunctionCall { name, args } => MappingSource::Function {
                name: name.clone(),
                args: args.iter().map(ExpressionAnalyzer::to_string).collect(),
            },
            CompiledExpression::When {
                branches,
                else_expr,
            } => MappingSource::Conditional {
                branches: branches
                    .iter()
                    .map(|b| ConditionalBranch {
                        condition: ExpressionAnalyzer::to_string(&b.condition),
                        value: ExpressionAnalyzer::to_string(&b.value),
                    })
                    .collect(),
                else_value: else_expr.as_ref().map(|e| ExpressionAnalyzer::to_string(e)),
                sql_preview: ExpressionAnalyzer::to_string(expr),
            },
            _ => {
                return Err(MappingAnalyzerError::UnsupportedExpression(format!(
                    "{:?}",
                    expr
                )));
            }
        };

        let m_type = match expr {
            CompiledExpression::Binary { .. } => MappingType::Computed,
            CompiledExpression::FunctionCall { .. } => MappingType::Generated,
            _ => MappingType::Conditional,
        };

        Ok((source, m_type, inferred_type))
    }

    /// Determines if a conversion is required and assesses the risk (lossy vs safe).
    fn evaluate_conversion_safety(
        &self,
        source: &DataType,
        target: &DataType,
    ) -> Option<TypeConversion> {
        if source == target {
            return None;
        }

        let is_lossy = self.check_lossy_risk(source, target);
        let method = self.conversion_method(source, target);

        let warning = if is_lossy {
            Some(format!(
                "Potential data loss or precision truncation during {} to {} conversion.",
                source.mysql_name(),
                target.postgres_name()
            ))
        } else if matches!(method, ConversionMethod::Explicit) {
            Some(format!(
                "Explicit type cast required for {} to {} mapping.",
                source.mysql_name(),
                target.postgres_name()
            ))
        } else {
            None
        };

        Some(TypeConversion {
            from_type: source.mysql_name().to_string(),
            to_type: target.postgres_name().to_string(),
            is_safe: !is_lossy,
            warning,
            conversion_method: method,
        })
    }

    /// Logic for determining conversion strategy based on the `DataType` enum categories.
    fn conversion_method(&self, from: &DataType, to: &DataType) -> ConversionMethod {
        use DataType::*;
        match (from, to) {
            // Implicit: Upscaling numeric types
            (
                Short | ShortUnsigned | Int | Int4 | IntUnsigned,
                Long | LongLong | Float | Double | Decimal | NewDecimal,
            ) => ConversionMethod::Implicit,

            // Explicit: Downscaling or cross-category
            (Float | Double | Decimal | NewDecimal, Short | Int | Int4 | Long) => {
                ConversionMethod::Explicit
            }
            (String | VarChar | Char, Int | Int4 | Long | Float | Double | Decimal | Boolean) => {
                ConversionMethod::Explicit
            }
            (Json, String | VarChar) => ConversionMethod::Explicit,

            // Function-based: Temporal transformations
            (Timestamp | TimestampTz | Date, String | VarChar) => ConversionMethod::Function {
                name: "to_char".to_string(),
            },
            (String | VarChar, Timestamp | TimestampTz | Date) => ConversionMethod::Function {
                name: "to_timestamp".to_string(),
            },

            // Binary to String (usually requires encoding like base64 or hex)
            (Blob | LongBlob | Bytea | Binary | VarBinary, String | VarChar) => {
                ConversionMethod::Function {
                    name: "encode".to_string(),
                }
            }

            // Default
            _ => ConversionMethod::Explicit,
        }
    }

    fn check_lossy_risk(&self, from: &DataType, to: &DataType) -> bool {
        use DataType::*;
        match (from, to) {
            (Float | Double | Decimal | NewDecimal, Short | Int | Int4 | Long | LongLong) => true, // Decimals to Int
            (Long | LongLong | IntUnsigned, Int | Int4 | Short) => true, // Overflow risk
            (TimestampTz, Timestamp | Date) => true,                     // Timezone truncation
            (Double, Float) => true,                                     // Precision loss
            (Timestamp | Date | Json, String | VarChar) => true,         // Format dependency
            _ => false,
        }
    }

    fn get_target_data_type(
        &self,
        name: &str,
        columns: &[ColumnDef],
    ) -> Result<DataType, MappingAnalyzerError> {
        columns
            .iter()
            .find(|c| c.name == name)
            .map(|c| c.data_type.clone())
            .ok_or_else(|| MappingAnalyzerError::TargetColumnNotFound {
                column: name.to_string(),
            })
    }

    fn check_target_nullability(
        &self,
        name: &str,
        columns: &[ColumnDef],
    ) -> Result<bool, MappingAnalyzerError> {
        columns
            .iter()
            .find(|c| c.name == name)
            .map(|c| c.is_nullable)
            .ok_or_else(|| MappingAnalyzerError::TargetColumnNotFound {
                column: name.to_string(),
            })
    }

    fn fetch_column_meta(
        &self,
        table: &str,
        column: &str,
    ) -> Result<ColumnMetadata, MappingAnalyzerError> {
        self.schema_plan
            .get_table_metadata(table)
            .and_then(|t| t.columns().iter().find(|c| c.name == column).cloned())
            .ok_or_else(|| MappingAnalyzerError::SourceColumnNotFound {
                column: column.to_string(),
            })
    }
}

#[async_trait]
impl PlanAnalyzer for MappingAnalyzer {
    type Input = (Vec<Transformation>, SourcePlan);
    type Output = Vec<ColumnMapping>;

    fn name(&self) -> &'static str {
        "mapping"
    }

    async fn analyze(
        &self,
        input: &Self::Input,
        ctx: &AnalysisContext,
    ) -> AnalyzerResult<Self::Output> {
        let (transformations, source_plan) = input;
        let resolved_columns = ctx.schema_plan.resolved_column_defs().await;

        let mut mappings = Vec::with_capacity(transformations.len());
        for trans in transformations {
            match self
                .analyze_single_mapping(trans, source_plan, &resolved_columns)
                .await
            {
                Ok(m) => mappings.push(m),
                Err(e) => {
                    warn!(target: "analyzer", field = %trans.target_field, error = %e, "Transformation analysis failed");
                    return Err(AnalyzerError::error("mapping", e.to_string()));
                }
            }
        }

        info!(target: "analyzer", count = mappings.len(), "Mapping analysis completed");
        Ok(mappings)
    }
}
