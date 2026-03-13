use model::core::value::Value;
use model::execution::expr::{BinaryOp, CompiledExpression, UnaryOp};

/// Analyzes compiled expressions to extract metadata
pub struct ExpressionAnalyzer;

impl ExpressionAnalyzer {
    /// Extract all column references from an expression
    pub fn extract_columns(expr: &CompiledExpression) -> Vec<String> {
        let mut columns = Vec::new();
        Self::extract_columns_recursive(expr, &mut columns);
        columns.sort();
        columns.dedup();
        columns
    }

    /// Extract tables
    pub fn extract_tables(expr: &CompiledExpression) -> Vec<String> {
        let mut tables = Vec::new();
        Self::extract_columns_recursive(expr, &mut tables);
        // Extract table names from column references
        let table_names: Vec<String> = tables
            .iter()
            .filter_map(|col| {
                if col.contains('.') {
                    Some(col.split('.').next().unwrap().to_string())
                } else {
                    None
                }
            })
            .collect();
        let mut unique_tables = table_names;
        unique_tables.sort();
        unique_tables.dedup();
        unique_tables
    }

    fn extract_columns_recursive(expr: &CompiledExpression, columns: &mut Vec<String>) {
        match expr {
            CompiledExpression::Identifier(name) => columns.push(name.clone()),
            CompiledExpression::DotPath(segments) => {
                if segments.len() >= 2 {
                    columns.push(format!("{}.{}", segments[0], segments[1]));
                }
            }
            CompiledExpression::Binary { left, right, .. } => {
                Self::extract_columns_recursive(left, columns);
                Self::extract_columns_recursive(right, columns);
            }
            CompiledExpression::Unary { operand, .. } => {
                Self::extract_columns_recursive(operand, columns);
            }
            CompiledExpression::FunctionCall { args, .. } => {
                for arg in args {
                    Self::extract_columns_recursive(arg, columns);
                }
            }
            CompiledExpression::When {
                branches,
                else_expr,
            } => {
                for branch in branches {
                    Self::extract_columns_recursive(&branch.condition, columns);
                    Self::extract_columns_recursive(&branch.value, columns);
                }
                if let Some(else_expr) = else_expr {
                    Self::extract_columns_recursive(else_expr, columns);
                }
            }
            CompiledExpression::Array(exprs) => {
                for expr in exprs {
                    Self::extract_columns_recursive(expr, columns);
                }
            }
            CompiledExpression::IsNull(operand) | CompiledExpression::IsNotNull(operand) => {
                Self::extract_columns_recursive(operand, columns);
            }
            CompiledExpression::Grouped(expr) => {
                Self::extract_columns_recursive(expr, columns);
            }
            _ => {}
        }
    }

    /// Extract all function names from an expression
    pub fn extract_functions(expr: &CompiledExpression) -> Vec<String> {
        let mut functions = Vec::new();
        Self::extract_functions_recursive(expr, &mut functions);
        functions.sort();
        functions.dedup();
        functions
    }

    fn extract_functions_recursive(expr: &CompiledExpression, functions: &mut Vec<String>) {
        match expr {
            CompiledExpression::FunctionCall { name, args } => {
                functions.push(name.clone());
                for arg in args {
                    Self::extract_functions_recursive(arg, functions);
                }
            }
            CompiledExpression::Binary { left, right, .. } => {
                Self::extract_functions_recursive(left, functions);
                Self::extract_functions_recursive(right, functions);
            }
            CompiledExpression::Unary { operand, .. } => {
                Self::extract_functions_recursive(operand, functions);
            }
            CompiledExpression::When {
                branches,
                else_expr,
            } => {
                for branch in branches {
                    Self::extract_functions_recursive(&branch.condition, functions);
                    Self::extract_functions_recursive(&branch.value, functions);
                }
                if let Some(else_expr) = else_expr {
                    Self::extract_functions_recursive(else_expr, functions);
                }
            }
            CompiledExpression::Array(exprs) => {
                for expr in exprs {
                    Self::extract_functions_recursive(expr, functions);
                }
            }
            CompiledExpression::IsNull(operand) | CompiledExpression::IsNotNull(operand) => {
                Self::extract_functions_recursive(operand, functions);
            }
            CompiledExpression::Grouped(expr) => {
                Self::extract_functions_recursive(expr, functions);
            }
            _ => {}
        }
    }

    /// Check if an expression is a simple column reference
    pub fn is_simple_column(expr: &CompiledExpression) -> bool {
        matches!(
            expr,
            CompiledExpression::Identifier(_) | CompiledExpression::DotPath(_)
        )
    }

    /// Check if an expression contains any function calls
    pub fn contains_functions(expr: &CompiledExpression) -> bool {
        !Self::extract_functions(expr).is_empty()
    }

    /// Check if an expression is a constant (literal value)
    pub fn is_constant(expr: &CompiledExpression) -> bool {
        matches!(expr, CompiledExpression::Literal(_))
    }

    /// Format a literal value as a string representation
    /// Returns (formatted_string, optional_type_hint)
    pub fn format_literal(value: &Value) -> (String, Option<String>) {
        let formatted = match value {
            Value::Null => "NULL".to_string(),
            Value::Boolean(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
            Value::String(s) => format!("'{}'", s),
            Value::Int(n) => n.to_string(),
            Value::UInt(n) => n.to_string(),
            Value::Float(n) => n.to_string(),
            Value::Decimal(n) => n.to_string(),
            Value::Date(d) => format!("'{}'", d),
            Value::Time { value, .. } => format!("'{}'", value),
            Value::Timestamp { value, .. } => format!("'{}'", value),
            Value::Uuid(u) => format!("'{}'", u),
            Value::Json(j) => format!("'{}'", j),
            _ => "NULL".to_string(),
        };

        (formatted, None)
    }

    /// Get the complexity score of an expression (number of nodes)
    pub fn complexity_score(expr: &CompiledExpression) -> usize {
        match expr {
            CompiledExpression::Literal(_) | CompiledExpression::Identifier(_) => 1,
            CompiledExpression::DotPath(segments) => segments.len(),
            CompiledExpression::Binary { left, right, .. } => {
                1 + Self::complexity_score(left) + Self::complexity_score(right)
            }
            CompiledExpression::Unary { operand, .. } => 1 + Self::complexity_score(operand),
            CompiledExpression::FunctionCall { args, .. } => {
                1 + args.iter().map(Self::complexity_score).sum::<usize>()
            }
            CompiledExpression::When {
                branches,
                else_expr,
            } => {
                let branch_score: usize = branches
                    .iter()
                    .map(|b| {
                        Self::complexity_score(&b.condition) + Self::complexity_score(&b.value)
                    })
                    .sum();
                let else_score = else_expr
                    .as_ref()
                    .map(|e| Self::complexity_score(e))
                    .unwrap_or(0);
                1 + branch_score + else_score
            }
            CompiledExpression::Array(exprs) => {
                1 + exprs.iter().map(Self::complexity_score).sum::<usize>()
            }
            CompiledExpression::IsNull(operand) | CompiledExpression::IsNotNull(operand) => {
                1 + Self::complexity_score(operand)
            }
            CompiledExpression::Grouped(expr) => Self::complexity_score(expr),
        }
    }

    /// Convert expression to SQL-like string representation
    pub fn to_string(expr: &CompiledExpression) -> String {
        match expr {
            CompiledExpression::Literal(value) => Self::format_literal(value).0,
            CompiledExpression::Identifier(name) => name.clone(),
            CompiledExpression::DotPath(segments) => segments.join("."),

            CompiledExpression::Binary { left, op, right } => {
                let left_str = Self::to_string(left);
                let right_str = Self::to_string(right);
                let op_str = Self::binary_op_to_string(op);

                // Add parentheses for nested binary operations to preserve precedence
                let left_formatted = if matches!(**left, CompiledExpression::Binary { .. }) {
                    format!("({})", left_str)
                } else {
                    left_str
                };
                let right_formatted = if matches!(**right, CompiledExpression::Binary { .. }) {
                    format!("({})", right_str)
                } else {
                    right_str
                };

                format!("{} {} {}", left_formatted, op_str, right_formatted)
            }

            CompiledExpression::Unary { op, operand } => {
                let operand_str = Self::to_string(operand);
                let op_str = Self::unary_op_to_string(op);
                format!("{} {}", op_str, operand_str)
            }

            CompiledExpression::FunctionCall { name, args } => {
                let args_str = args
                    .iter()
                    .map(Self::to_string)
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{}({})", name.to_uppercase(), args_str)
            }

            CompiledExpression::When {
                branches,
                else_expr,
            } => {
                let mut result = String::from("CASE");

                for branch in branches {
                    let cond = Self::to_string(&branch.condition);
                    let val = Self::to_string(&branch.value);
                    result.push_str(&format!(" WHEN {} THEN {}", cond, val));
                }

                if let Some(else_val) = else_expr {
                    result.push_str(&format!(" ELSE {}", Self::to_string(else_val)));
                }

                result.push_str(" END");
                result
            }

            CompiledExpression::Array(exprs) => {
                let items = exprs
                    .iter()
                    .map(Self::to_string)
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("[{}]", items)
            }

            CompiledExpression::IsNull(operand) => {
                format!("{} IS NULL", Self::to_string(operand))
            }

            CompiledExpression::IsNotNull(operand) => {
                format!("{} IS NOT NULL", Self::to_string(operand))
            }

            CompiledExpression::Grouped(expr) => {
                format!("({})", Self::to_string(expr))
            }
        }
    }

    /// Convert binary operator to SQL string
    fn binary_op_to_string(op: &BinaryOp) -> &'static str {
        match op {
            BinaryOp::Add => "+",
            BinaryOp::Subtract => "-",
            BinaryOp::Multiply => "*",
            BinaryOp::Divide => "/",
            BinaryOp::Modulo => "%",
            BinaryOp::Equal => "=",
            BinaryOp::NotEqual => "!=",
            BinaryOp::GreaterThan => ">",
            BinaryOp::LessThan => "<",
            BinaryOp::GreaterOrEqual => ">=",
            BinaryOp::LessOrEqual => "<=",
            BinaryOp::And => "AND",
            BinaryOp::Or => "OR",
        }
    }

    /// Convert unary operator to SQL string
    fn unary_op_to_string(op: &UnaryOp) -> &'static str {
        match op {
            UnaryOp::Not => "NOT",
            UnaryOp::Negate => "-",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use model::execution::expr::BinaryOp;

    #[test]
    fn test_extract_columns_simple() {
        let expr = CompiledExpression::Identifier("name".to_string());
        let columns = ExpressionAnalyzer::extract_columns(&expr);
        assert_eq!(columns, vec!["name"]);
    }

    #[test]
    fn test_extract_columns_dotpath() {
        let expr = CompiledExpression::DotPath(vec!["users".to_string(), "email".to_string()]);
        let columns = ExpressionAnalyzer::extract_columns(&expr);
        assert_eq!(columns, vec!["users.email"]);
    }

    #[test]
    fn test_extract_columns_binary() {
        let expr = CompiledExpression::Binary {
            left: Box::new(CompiledExpression::Identifier("price".to_string())),
            op: BinaryOp::Multiply,
            right: Box::new(CompiledExpression::Literal(Value::Float(1.1))),
        };
        let columns = ExpressionAnalyzer::extract_columns(&expr);
        assert_eq!(columns, vec!["price"]);
    }

    #[test]
    fn test_extract_functions() {
        let expr = CompiledExpression::FunctionCall {
            name: "upper".to_string(),
            args: vec![CompiledExpression::Identifier("name".to_string())],
        };
        let functions = ExpressionAnalyzer::extract_functions(&expr);
        assert_eq!(functions, vec!["upper"]);
    }

    #[test]
    fn test_is_simple_column() {
        let expr = CompiledExpression::Identifier("name".to_string());
        assert!(ExpressionAnalyzer::is_simple_column(&expr));

        let expr = CompiledExpression::Literal(Value::Int(42));
        assert!(!ExpressionAnalyzer::is_simple_column(&expr));
    }

    #[test]
    fn test_complexity_score() {
        let expr = CompiledExpression::Identifier("name".to_string());
        assert_eq!(ExpressionAnalyzer::complexity_score(&expr), 1);

        let expr = CompiledExpression::Binary {
            left: Box::new(CompiledExpression::Identifier("a".to_string())),
            op: BinaryOp::Add,
            right: Box::new(CompiledExpression::Identifier("b".to_string())),
        };
        assert_eq!(ExpressionAnalyzer::complexity_score(&expr), 3);
    }

    #[test]
    fn test_to_string_identifier() {
        let expr = CompiledExpression::Identifier("user_id".to_string());
        assert_eq!(ExpressionAnalyzer::to_string(&expr), "user_id");
    }

    #[test]
    fn test_to_string_dotpath() {
        let expr = CompiledExpression::DotPath(vec!["users".to_string(), "email".to_string()]);
        assert_eq!(ExpressionAnalyzer::to_string(&expr), "users.email");
    }

    #[test]
    fn test_to_string_literal() {
        let expr = CompiledExpression::Literal(Value::String("hello".to_string()));
        assert_eq!(ExpressionAnalyzer::to_string(&expr), "'hello'");

        let expr = CompiledExpression::Literal(Value::Int(42));
        assert_eq!(ExpressionAnalyzer::to_string(&expr), "42");

        let expr = CompiledExpression::Literal(Value::Boolean(true));
        assert_eq!(ExpressionAnalyzer::to_string(&expr), "TRUE");

        let expr = CompiledExpression::Literal(Value::Null);
        assert_eq!(ExpressionAnalyzer::to_string(&expr), "NULL");
    }

    #[test]
    fn test_to_string_binary() {
        let expr = CompiledExpression::Binary {
            left: Box::new(CompiledExpression::Identifier("price".to_string())),
            op: BinaryOp::Multiply,
            right: Box::new(CompiledExpression::Literal(Value::Float(1.1))),
        };
        assert_eq!(ExpressionAnalyzer::to_string(&expr), "price * 1.1");
    }

    #[test]
    fn test_to_string_binary_nested() {
        // (a + b) * c
        let expr = CompiledExpression::Binary {
            left: Box::new(CompiledExpression::Binary {
                left: Box::new(CompiledExpression::Identifier("a".to_string())),
                op: BinaryOp::Add,
                right: Box::new(CompiledExpression::Identifier("b".to_string())),
            }),
            op: BinaryOp::Multiply,
            right: Box::new(CompiledExpression::Identifier("c".to_string())),
        };
        assert_eq!(ExpressionAnalyzer::to_string(&expr), "(a + b) * c");
    }

    #[test]
    fn test_to_string_function() {
        let expr = CompiledExpression::FunctionCall {
            name: "upper".to_string(),
            args: vec![CompiledExpression::Identifier("name".to_string())],
        };
        assert_eq!(ExpressionAnalyzer::to_string(&expr), "UPPER(name)");

        let expr = CompiledExpression::FunctionCall {
            name: "coalesce".to_string(),
            args: vec![
                CompiledExpression::Identifier("email".to_string()),
                CompiledExpression::Literal(Value::String("N/A".to_string())),
            ],
        };
        assert_eq!(
            ExpressionAnalyzer::to_string(&expr),
            "COALESCE(email, 'N/A')"
        );
    }

    #[test]
    fn test_to_string_when() {
        use model::execution::expr::WhenBranch;

        let expr = CompiledExpression::When {
            branches: vec![WhenBranch {
                condition: CompiledExpression::Binary {
                    left: Box::new(CompiledExpression::Identifier("status".to_string())),
                    op: BinaryOp::Equal,
                    right: Box::new(CompiledExpression::Literal(Value::String(
                        "active".to_string(),
                    ))),
                },
                value: CompiledExpression::Literal(Value::String("ACTIVE".to_string())),
            }],
            else_expr: Some(Box::new(CompiledExpression::Literal(Value::String(
                "INACTIVE".to_string(),
            )))),
        };
        assert_eq!(
            ExpressionAnalyzer::to_string(&expr),
            "CASE WHEN status = 'active' THEN 'ACTIVE' ELSE 'INACTIVE' END"
        );
    }

    #[test]
    fn test_to_string_is_null() {
        let expr = CompiledExpression::IsNull(Box::new(CompiledExpression::Identifier(
            "email".to_string(),
        )));
        assert_eq!(ExpressionAnalyzer::to_string(&expr), "email IS NULL");
    }

    #[test]
    fn test_to_string_is_not_null() {
        let expr = CompiledExpression::IsNotNull(Box::new(CompiledExpression::Identifier(
            "email".to_string(),
        )));
        assert_eq!(ExpressionAnalyzer::to_string(&expr), "email IS NOT NULL");
    }

    #[test]
    fn test_to_string_grouped() {
        let expr = CompiledExpression::Grouped(Box::new(CompiledExpression::Binary {
            left: Box::new(CompiledExpression::Identifier("a".to_string())),
            op: BinaryOp::Add,
            right: Box::new(CompiledExpression::Identifier("b".to_string())),
        }));
        assert_eq!(ExpressionAnalyzer::to_string(&expr), "(a + b)");
    }
}
