use crate::{
    ast::{
        attribute::Attribute,
        block::{ConnectionBlock, DefineBlock},
        doc::SmqlDocument,
        dotpath::DotPath,
        expr::{Expression, ExpressionKind, WhenBranch},
        ident::Identifier,
        literal::Literal,
        operator::BinaryOperator,
        pipeline::{
            AfterBlock, BeforeBlock, FieldMapping, FromBlock, JoinClause, NestedBlock,
            PaginateBlock, PipelineBlock, SelectBlock, SettingsBlock, ToBlock, WhereClause,
            WithBlock,
        },
        span::Span,
        validation::{
            FailedRowsBlock, OnErrorBlock, RetryBlock, ValidateBlock, ValidationBody,
            ValidationCheck, ValidationKind,
        },
    },
    errors::BuildError,
    parser::{Rule, SmqlParser},
};
use pest::{
    Parser,
    iterators::{Pair, Pairs},
};

pub type BuildResult<T> = Result<T, BuildError>;

/// Parse SMQL text into a typed AST
pub fn parse(input: &str) -> BuildResult<SmqlDocument> {
    let pairs = SmqlParser::parse(Rule::program, input).map_err(|e| BuildError {
        message: format!("Syntax error: {}", e),
        line: 1,
        column: 1,
    })?;

    build_document(pairs)
}

fn build_document(mut pairs: Pairs<Rule>) -> BuildResult<SmqlDocument> {
    let program = pairs.next().ok_or_else(|| BuildError {
        message: "Empty input".to_string(),
        line: 1,
        column: 1,
    })?;

    let span = pair_to_span(&program);
    let mut define_block = None;
    let mut connections = Vec::new();
    let mut pipelines = Vec::new();

    for pair in program.into_inner() {
        match pair.as_rule() {
            Rule::define_block => {
                define_block = Some(build_define_block(pair)?);
            }
            Rule::connection_block => {
                connections.push(build_connection_block(pair)?);
            }
            Rule::pipeline_block => {
                pipelines.push(build_pipeline_block(pair)?);
            }
            Rule::EOI => {}
            _ => {}
        }
    }

    Ok(SmqlDocument {
        define_block,
        connections,
        pipelines,
        span,
    })
}

fn pair_to_span(pair: &Pair<Rule>) -> crate::ast::span::Span {
    let (line, col) = pair.line_col();
    let span_pest = pair.as_span();
    Span::new(span_pest.start(), span_pest.end(), line, col)
}

fn build_define_block(pair: Pair<Rule>) -> BuildResult<DefineBlock> {
    let span = pair_to_span(&pair);
    let mut attributes = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::attribute {
            attributes.push(build_attribute(inner)?);
        }
    }

    Ok(DefineBlock { attributes, span })
}

fn build_connection_block(pair: Pair<Rule>) -> BuildResult<ConnectionBlock> {
    let span = pair_to_span(&pair);
    let mut name = String::new();
    let mut attributes = Vec::new();
    let mut nested_blocks = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::lit_string => {
                name = parse_string_literal(inner.as_str());
            }
            Rule::attribute => {
                attributes.push(build_attribute(inner)?);
            }
            Rule::nested_block => {
                nested_blocks.push(build_nested_block(inner)?);
            }
            _ => {}
        }
    }

    Ok(ConnectionBlock {
        name,
        attributes,
        nested_blocks,
        span,
    })
}

fn build_pipeline_block(pair: Pair<Rule>) -> BuildResult<PipelineBlock> {
    let span = pair_to_span(&pair);
    let mut name = String::new();
    let mut after = None;
    let mut from = None;
    let mut to = None;
    let mut where_clauses = Vec::new();
    let mut with_block = None;
    let mut select_block = None;
    let mut validate_block = None;
    let mut on_error_block = None;
    let mut paginate_block = None;
    let mut before_block = None;
    let mut after_block = None;
    let mut settings_block = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::lit_string => {
                name = parse_string_literal(inner.as_str());
            }
            Rule::attribute => {
                after = Some(build_after_dependencies(inner)?);
            }
            Rule::from_block => {
                from = Some(build_from_block(inner)?);
            }
            Rule::to_block => {
                to = Some(build_to_block(inner)?);
            }
            Rule::where_block => {
                where_clauses.push(build_where_clause(inner)?);
            }
            Rule::with_block => {
                with_block = Some(build_with_block(inner)?);
            }
            Rule::select_block => {
                select_block = Some(build_select_block(inner)?);
            }
            Rule::validate_block => {
                validate_block = Some(build_validate_block(inner)?);
            }
            Rule::on_error_block => {
                on_error_block = Some(build_on_error_block(inner)?);
            }
            Rule::paginate_block => {
                paginate_block = Some(build_paginate_block(inner)?);
            }
            Rule::before_block => {
                before_block = Some(build_before_block(inner)?);
            }
            Rule::after_block => {
                after_block = Some(build_after_block(inner)?);
            }
            Rule::settings_block => {
                settings_block = Some(build_settings_block(inner)?);
            }
            _ => {}
        }
    }

    Ok(PipelineBlock {
        name,
        description: None,
        after,
        from,
        to,
        where_clauses,
        with_block,
        select_block,
        validate_block,
        on_error_block,
        paginate_block,
        before_block,
        after_block,
        settings_block,
        span,
    })
}

fn build_nested_block(pair: Pair<Rule>) -> BuildResult<NestedBlock> {
    let span = pair_to_span(&pair);
    let mut name = Identifier::new("", span);
    let mut attributes = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::ident => {
                name = Identifier::new(inner.as_str(), pair_to_span(&inner));
            }
            Rule::attribute => {
                attributes.push(build_attribute(inner)?);
            }
            _ => {}
        }
    }

    Ok(NestedBlock {
        kind: name.to_string(),
        attributes,
        span,
    })
}

fn build_after_dependencies(pair: Pair<Rule>) -> BuildResult<Vec<Expression>> {
    let mut deps = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::expression {
            deps.push(build_expression(inner)?);
        }
    }

    Ok(deps)
}

fn build_from_block(pair: Pair<Rule>) -> BuildResult<FromBlock> {
    let span = pair_to_span(&pair);
    let mut attributes = Vec::new();
    let mut nested_blocks = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::attribute => {
                attributes.push(build_attribute(inner)?);
            }
            Rule::nested_block => {
                nested_blocks.push(build_nested_block(inner)?);
            }
            _ => {}
        }
    }

    Ok(FromBlock {
        attributes,
        nested_blocks,
        span,
    })
}

fn build_to_block(pair: Pair<Rule>) -> BuildResult<ToBlock> {
    let span = pair_to_span(&pair);
    let mut attributes = Vec::new();
    let mut nested_blocks = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::attribute => {
                attributes.push(build_attribute(inner)?);
            }
            Rule::nested_block => {
                nested_blocks.push(build_nested_block(inner)?);
            }
            _ => {}
        }
    }

    Ok(ToBlock {
        attributes,
        nested_blocks,
        span,
    })
}

fn build_where_clause(pair: Pair<Rule>) -> BuildResult<WhereClause> {
    let span = pair_to_span(&pair);
    let mut label = None;
    let mut conditions = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::lit_string => {
                label = Some(parse_string_literal(inner.as_str()));
            }
            Rule::expression => {
                conditions.push(build_expression(inner)?);
            }
            _ => {}
        }
    }

    Ok(WhereClause {
        label,
        conditions,
        span,
    })
}

fn build_with_block(pair: Pair<Rule>) -> BuildResult<WithBlock> {
    let span = pair_to_span(&pair);
    let mut joins = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::join_clause {
            joins.push(build_join_clause(inner)?);
        }
    }

    Ok(WithBlock { joins, span })
}

fn build_join_clause(pair: Pair<Rule>) -> BuildResult<JoinClause> {
    let span = pair_to_span(&pair);
    let mut alias = Identifier::new("", span);
    let mut table = Identifier::new("", span);
    let mut condition = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::ident => {
                // First ident is alias, second is table
                if alias.name.is_empty() {
                    alias = Identifier::new(inner.as_str(), pair_to_span(&inner));
                } else {
                    table = Identifier::new(inner.as_str(), pair_to_span(&inner));
                }
            }
            Rule::expression => {
                condition = Some(build_expression(inner)?);
            }
            _ => {} // Skip keywords like kw_from, kw_where
        }
    }

    Ok(JoinClause {
        alias,
        table,
        condition,
        span,
    })
}

fn build_select_block(pair: Pair<Rule>) -> BuildResult<SelectBlock> {
    let span = pair_to_span(&pair);
    let mut fields = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::field_mapping {
            fields.push(build_field_mapping(inner)?);
        }
    }

    Ok(SelectBlock { fields, span })
}

fn build_field_mapping(pair: Pair<Rule>) -> BuildResult<FieldMapping> {
    let span = pair_to_span(&pair);
    let mut name = Identifier::new("", span);
    let mut value = Expression::new(ExpressionKind::Literal(Literal::Null), span);

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::ident => {
                name = Identifier::new(inner.as_str(), pair_to_span(&inner));
            }
            Rule::expression => {
                value = build_expression(inner)?;
            }
            _ => {}
        }
    }

    Ok(FieldMapping { name, value, span })
}

fn build_validate_block(pair: Pair<Rule>) -> BuildResult<ValidateBlock> {
    let span = pair_to_span(&pair);
    let mut checks = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::validation_check {
            checks.push(build_validation_check(inner)?);
        }
    }

    Ok(ValidateBlock { checks, span })
}

fn build_validation_check(pair: Pair<Rule>) -> BuildResult<ValidationCheck> {
    let span = pair_to_span(&pair);
    let mut kind = ValidationKind::Assert;
    let mut label = String::new();
    let mut check = None;
    let mut message = None;
    let mut action = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::kw_assert => kind = ValidationKind::Assert,
            Rule::kw_warn => kind = ValidationKind::Warn,
            Rule::lit_string => {
                label = parse_string_literal(inner.as_str());
            }
            Rule::validation_body => {
                let mut pairs = inner.into_inner();
                while let Some(pair) = pairs.next() {
                    if pair.as_rule() == Rule::op_eq
                        && let Some(value_pair) = pairs.next()
                    {
                        match value_pair.as_rule() {
                            Rule::expression => {
                                check = Some(build_expression(value_pair)?);
                            }
                            Rule::lit_string => {
                                message = Some(parse_string_literal(value_pair.as_str()));
                            }
                            Rule::ident => {
                                action = Some(value_pair.as_str().to_string());
                            }
                            _ => {}
                        }
                    }
                }
            }
            _ => {}
        }
    }

    Ok(ValidationCheck {
        kind,
        label,
        body: ValidationBody {
            check: check.unwrap_or_else(|| {
                Expression::new(ExpressionKind::Literal(Literal::Boolean(true)), span)
            }),
            message: message.unwrap_or_default(),
            action,
        },
        span,
    })
}

fn build_on_error_block(pair: Pair<Rule>) -> BuildResult<OnErrorBlock> {
    let span = pair_to_span(&pair);
    let mut retry = None;
    let mut failed_rows = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::retry_block => {
                retry = Some(build_retry_block(inner)?);
            }
            Rule::failed_rows_block => {
                failed_rows = Some(build_failed_rows_block(inner)?);
            }
            _ => {}
        }
    }

    Ok(OnErrorBlock {
        retry,
        failed_rows,
        span,
    })
}

fn build_retry_block(pair: Pair<Rule>) -> BuildResult<RetryBlock> {
    let span = pair_to_span(&pair);
    let mut attributes = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::attribute {
            attributes.push(build_attribute(inner)?);
        }
    }

    Ok(RetryBlock { attributes, span })
}

fn build_failed_rows_block(pair: Pair<Rule>) -> BuildResult<FailedRowsBlock> {
    let span = pair_to_span(&pair);
    let mut attributes = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::attribute {
            attributes.push(build_attribute(inner)?);
        }
    }

    Ok(FailedRowsBlock { attributes, span })
}

fn build_paginate_block(pair: Pair<Rule>) -> BuildResult<PaginateBlock> {
    let span = pair_to_span(&pair);
    let mut attributes = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::attribute {
            attributes.push(build_attribute(inner)?);
        }
    }

    Ok(PaginateBlock { attributes, span })
}

fn build_before_block(pair: Pair<Rule>) -> BuildResult<BeforeBlock> {
    let span = pair_to_span(&pair);
    let mut sql = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::sql_attr {
            // sql_attr = { "sql" ~ op_eq ~ array_literal }
            for sql_inner in inner.into_inner() {
                if sql_inner.as_rule() == Rule::array_literal {
                    // Build the array expression and extract string literals
                    let array_expr = build_array_literal(sql_inner, span)?;
                    if let ExpressionKind::Array(elements) = array_expr.kind {
                        for elem in elements {
                            if let ExpressionKind::Literal(Literal::String(s)) = elem.kind {
                                sql.push(s);
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(BeforeBlock { sql, span })
}

fn build_after_block(pair: Pair<Rule>) -> BuildResult<AfterBlock> {
    let span = pair_to_span(&pair);
    let mut sql = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::sql_attr {
            // sql_attr = { "sql" ~ op_eq ~ array_literal }
            for sql_inner in inner.into_inner() {
                if sql_inner.as_rule() == Rule::array_literal {
                    // Build the array expression and extract string literals
                    let array_expr = build_array_literal(sql_inner, span)?;
                    if let ExpressionKind::Array(elements) = array_expr.kind {
                        for elem in elements {
                            if let ExpressionKind::Literal(Literal::String(s)) = elem.kind {
                                sql.push(s);
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(AfterBlock { sql, span })
}

fn build_settings_block(pair: Pair<Rule>) -> BuildResult<SettingsBlock> {
    let span = pair_to_span(&pair);
    let mut attributes = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::attribute {
            attributes.push(build_attribute(inner)?);
        }
    }

    Ok(SettingsBlock { attributes, span })
}

fn build_attribute(pair: Pair<Rule>) -> BuildResult<Attribute> {
    let span = pair_to_span(&pair);
    let mut key = Identifier::new("", span);
    let mut value = Expression::new(ExpressionKind::Literal(Literal::Null), span);

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::ident => {
                key = Identifier::new(inner.as_str(), pair_to_span(&inner));
            }
            Rule::expression => {
                value = build_expression(inner)?;
            }
            _ => {}
        }
    }

    Ok(Attribute { key, value, span })
}

fn build_expression(pair: Pair<Rule>) -> BuildResult<Expression> {
    let span = pair_to_span(&pair);
    build_expression_inner(pair, span)
}

fn build_expression_inner(pair: Pair<Rule>, span: Span) -> BuildResult<Expression> {
    match pair.as_rule() {
        Rule::expression => {
            // Unwrap the top-level expression rule
            let inner = pair.into_inner().next().unwrap();
            build_expression_inner(inner, span)
        }
        Rule::logical_or => build_binary_expression(pair, span),
        Rule::logical_and => build_binary_expression(pair, span),
        Rule::equality => build_binary_expression(pair, span),
        Rule::comparison => build_binary_expression(pair, span),
        Rule::additive => build_binary_expression(pair, span),
        Rule::multiplicative => build_binary_expression(pair, span),
        Rule::is_null_check => build_is_null_check(pair, span),
        Rule::primary => build_primary_expression(pair, span),
        _ => Err(BuildError {
            message: format!("Unexpected rule in expression: {:?}", pair.as_rule()),
            line: span.line,
            column: span.column,
        }),
    }
}

fn build_binary_expression(pair: Pair<Rule>, span: Span) -> BuildResult<Expression> {
    let mut inner = pair.into_inner();
    let left = inner.next().unwrap();

    // Check if there's an operator
    if let Some(op_pair) = inner.next() {
        let operator = match op_pair.as_str() {
            "||" => BinaryOperator::Or,
            "&&" => BinaryOperator::And,
            "==" => BinaryOperator::Equal,
            "!=" => BinaryOperator::NotEqual,
            ">" => BinaryOperator::GreaterThan,
            "<" => BinaryOperator::LessThan,
            ">=" => BinaryOperator::GreaterOrEqual,
            "<=" => BinaryOperator::LessOrEqual,
            "+" => BinaryOperator::Add,
            "-" => BinaryOperator::Subtract,
            "*" => BinaryOperator::Multiply,
            "/" => BinaryOperator::Divide,
            "%" => BinaryOperator::Modulo,
            op => {
                return Err(BuildError {
                    message: format!("Unknown operator: {}", op),
                    line: span.line,
                    column: span.column,
                });
            }
        };

        let right = inner.next().unwrap();

        Ok(Expression::new(
            ExpressionKind::Binary {
                left: Box::new(build_expression_inner(left, span)?),
                operator,
                right: Box::new(build_expression_inner(right, span)?),
            },
            span,
        ))
    } else {
        // No operator, just recurse
        build_expression_inner(left, span)
    }
}

fn build_primary_expression(pair: Pair<Rule>, span: Span) -> BuildResult<Expression> {
    let inner = pair.into_inner().next().unwrap();
    build_primary_inner(inner, span)
}

fn build_dot_notation(pair: Pair<Rule>, span: Span) -> BuildResult<Expression> {
    // dotted_ident is an atomic rule (@), so we need to parse the string manually
    let segments: Vec<String> = pair.as_str().split('.').map(|s| s.to_string()).collect();

    Ok(Expression::new(
        ExpressionKind::DotNotation(DotPath::new(segments, span)),
        span,
    ))
}

fn build_function_call(pair: Pair<Rule>, span: Span) -> BuildResult<Expression> {
    let mut name = String::new();
    let mut arguments = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::ident => {
                name = inner.as_str().to_string();
            }
            Rule::expression => {
                arguments.push(build_expression_inner(inner, span)?);
            }
            _ => {}
        }
    }

    Ok(Expression::new(
        ExpressionKind::FunctionCall { name, arguments },
        span,
    ))
}

fn build_array_literal(pair: Pair<Rule>, span: Span) -> BuildResult<Expression> {
    let mut elements = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::expression {
            elements.push(build_expression_inner(inner, span)?);
        }
    }

    Ok(Expression::new(ExpressionKind::Array(elements), span))
}

fn build_when_expression(pair: Pair<Rule>, span: Span) -> BuildResult<Expression> {
    let mut branches = Vec::new();
    let mut else_value = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::when_branch => {
                branches.push(build_when_branch(inner)?);
            }
            Rule::expression => {
                else_value = Some(Box::new(build_expression_inner(inner, span)?));
            }
            _ => {}
        }
    }

    Ok(Expression::new(
        ExpressionKind::WhenExpression {
            branches,
            else_value,
        },
        span,
    ))
}

fn build_when_branch(pair: Pair<Rule>) -> BuildResult<crate::ast::expr::WhenBranch> {
    let span = pair_to_span(&pair);
    let mut condition = Expression::new(ExpressionKind::Literal(Literal::Null), span);
    let mut value = Expression::new(ExpressionKind::Literal(Literal::Null), span);

    let inner = pair.into_inner();

    for item in inner {
        if item.as_rule() == Rule::expression {
            // First expression is condition, second is value
            if matches!(condition.kind, ExpressionKind::Literal(Literal::Null)) {
                condition = build_expression_inner(item, span)?;
            } else {
                value = build_expression_inner(item, span)?;
            }
        }
    }

    Ok(WhenBranch {
        condition,
        value,
        span,
    })
}

fn build_is_null_check(pair: Pair<Rule>, span: Span) -> BuildResult<Expression> {
    let mut inner = pair.into_inner().peekable();

    // Since primary is a silent rule, we need to handle grouped expressions specially
    // For grouped expressions: lparen ~ expression ~ rparen
    // For other primaries: just the token itself
    let primary_expr = if inner.peek().map(|p| p.as_rule()) == Some(Rule::lparen) {
        // Skip lparen
        inner.next();
        // Get the expression
        let expr = inner.next().unwrap();
        let result = build_expression_inner(expr, span)?;
        // Skip rparen
        inner.next();
        result
    } else {
        // Regular primary expression
        let primary = inner.next().unwrap();
        build_primary_inner(primary, span)?
    };

    // Check if there are "is null" or "is not null" keywords following
    let mut has_is = false;
    let mut has_not = false;
    let mut has_null = false;

    for token in inner {
        match token.as_rule() {
            Rule::kw_is => has_is = true,
            Rule::kw_not => has_not = true,
            Rule::kw_null => has_null = true,
            _ => {}
        }
    }

    // Wrap it based on what keywords we found
    if has_is && has_not && has_null {
        // "primary is not null"
        Ok(Expression::new(
            ExpressionKind::IsNotNull(Box::new(primary_expr)),
            span,
        ))
    } else if has_is && has_null {
        // "primary is null"
        Ok(Expression::new(
            ExpressionKind::IsNull(Box::new(primary_expr)),
            span,
        ))
    } else {
        Ok(primary_expr)
    }
}

fn build_primary_inner(pair: Pair<Rule>, span: Span) -> BuildResult<Expression> {
    match pair.as_rule() {
        Rule::lit_number => {
            let num = pair.as_str().parse::<f64>().map_err(|_| BuildError {
                message: format!("Invalid number: {}", pair.as_str()),
                line: span.line,
                column: span.column,
            })?;
            Ok(Expression::new(
                ExpressionKind::Literal(Literal::Number(num)),
                span,
            ))
        }
        Rule::lit_string => {
            let s = parse_string_literal(pair.as_str());
            Ok(Expression::new(
                ExpressionKind::Literal(Literal::String(s)),
                span,
            ))
        }
        Rule::lit_boolean => {
            let b = pair.as_str() == "true";
            Ok(Expression::new(
                ExpressionKind::Literal(Literal::Boolean(b)),
                span,
            ))
        }
        Rule::kw_null => Ok(Expression::new(
            ExpressionKind::Literal(Literal::Null),
            span,
        )),
        Rule::ident => {
            let name = pair.as_str().to_string();
            Ok(Expression::new(ExpressionKind::Identifier(name), span))
        }
        Rule::dotted_ident => Ok(build_dot_notation(pair, span)?),
        Rule::fn_call => Ok(build_function_call(pair, span)?),
        Rule::array_literal => Ok(build_array_literal(pair, span)?),
        Rule::when_expr => Ok(build_when_expression(pair, span)?),
        Rule::expression => {
            // Grouped expression - just build the inner expression
            // (the grouping is represented by context, not by wrapping)
            build_expression_inner(pair, span)
        }
        _ => Err(BuildError {
            message: format!("Unexpected primary expression: {:?}", pair.as_rule()),
            line: span.line,
            column: span.column,
        }),
    }
}

fn parse_string_literal(s: &str) -> String {
    // Remove quotes and unescape
    let s = s.trim_matches('"');
    s.replace("\\n", "\n")
        .replace("\\t", "\t")
        .replace("\\r", "\r")
        .replace("\\\"", "\"")
        .replace("\\\\", "\\")
}
