pub mod ast;
pub mod context;
pub mod error;
pub mod eval;
pub mod functions;
pub mod inference;
pub mod types;

pub use ast::eval_ast_expression;
pub use context::EvalContext;
pub use error::{ExpressionError, Result};
pub use eval::Evaluator;
pub use functions::FunctionRegistry;
pub use inference::infer_expression_type;
pub use types::{parse_env_as_type, value_to_string};
