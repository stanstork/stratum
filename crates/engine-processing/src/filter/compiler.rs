use smql_syntax::ast::filter::FilterExpression;

/// A trait for compiling filter expressions into a specific format.
pub trait FilterCompiler {
    /// The type of filter that this compiler produces.
    type Filter;

    /// Compile the AST into a filter.
    fn compile(expr: &FilterExpression) -> Self::Filter;
}
