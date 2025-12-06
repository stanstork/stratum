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
            AfterBlock, BeforeBlock, FromBlock, NestedBlock, PaginateBlock, PipelineBlock,
            SelectBlock, SettingsBlock, ToBlock, WhereClause, WithBlock,
        },
        span::Span,
        validation::{OnErrorBlock, ValidateBlock},
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
        match inner.as_rule() {
            Rule::attribute => {
                attributes.push(build_attribute(inner)?);
            }
            _ => {}
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
    let mut name = Identifier::new("", span.clone());
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
    todo!()
}

fn build_from_block(pair: Pair<Rule>) -> BuildResult<FromBlock> {
    todo!()
}

fn build_to_block(pair: Pair<Rule>) -> BuildResult<ToBlock> {
    todo!()
}

fn build_where_clause(pair: Pair<Rule>) -> BuildResult<WhereClause> {
    todo!()
}

fn build_with_block(pair: Pair<Rule>) -> BuildResult<WithBlock> {
    todo!()
}

fn build_select_block(pair: Pair<Rule>) -> BuildResult<SelectBlock> {
    todo!()
}

fn build_validate_block(pair: Pair<Rule>) -> BuildResult<ValidateBlock> {
    todo!()
}

fn build_on_error_block(pair: Pair<Rule>) -> BuildResult<OnErrorBlock> {
    todo!()
}

fn build_paginate_block(pair: Pair<Rule>) -> BuildResult<PaginateBlock> {
    todo!()
}

fn build_before_block(pair: Pair<Rule>) -> BuildResult<BeforeBlock> {
    todo!()
}

fn build_after_block(pair: Pair<Rule>) -> BuildResult<AfterBlock> {
    todo!()
}

fn build_settings_block(pair: Pair<Rule>) -> BuildResult<SettingsBlock> {
    todo!()
}

fn build_attribute(pair: Pair<Rule>) -> BuildResult<Attribute> {
    let span = pair_to_span(&pair);
    let mut key = Identifier::new("", span.clone());
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
            let inner = pair.into_inner().next().unwrap();
            build_expression_inner(inner, span)
        }
        Rule::logical_or => build_binary_expression(pair, span),
        Rule::logical_and => build_binary_expression(pair, span),
        Rule::equality => build_binary_expression(pair, span),
        Rule::comparison => build_binary_expression(pair, span),
        Rule::additive => build_binary_expression(pair, span),
        Rule::multiplicative => build_binary_expression(pair, span),
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
        build_expression_inner(left, span)
    }
}

fn build_primary_expression(pair: Pair<Rule>, span: Span) -> BuildResult<Expression> {
    let inner = pair.into_inner().next().unwrap();

    match inner.as_rule() {
        Rule::lit_number => {
            let num = inner.as_str().parse::<f64>().map_err(|e| BuildError {
                message: format!("Invalid number literal: {}", e),
                line: span.line,
                column: span.column,
            })?;
            Ok(Expression::new(
                ExpressionKind::Literal(Literal::Number(num)),
                span,
            ))
        }
        Rule::lit_string => {
            let s = parse_string_literal(inner.as_str());
            Ok(Expression::new(
                ExpressionKind::Literal(Literal::String(s)),
                span,
            ))
        }
        Rule::lit_boolean => {
            let b = inner.as_str() == "true";
            Ok(Expression::new(
                ExpressionKind::Literal(Literal::Boolean(b)),
                span,
            ))
        }
        Rule::lit_null => Ok(Expression::new(
            ExpressionKind::Literal(Literal::Null),
            span,
        )),
        Rule::ident => {
            let name = inner.as_str().to_string();
            Ok(Expression::new(ExpressionKind::Identifier(name), span))
        }
        Rule::dotted_ident => Ok(build_dot_notation(inner, span)?),
        Rule::fn_call => Ok(build_function_call(inner, span)?),
        Rule::array_literal => Ok(build_array_literal(inner, span)?),
        Rule::when_expr => Ok(build_when_expression(inner, span)?),
        Rule::is_null_check => {
            let operand_pair = inner.into_inner().next().unwrap();
            let operand = build_expression_inner(operand_pair, span.clone())?;
            Ok(Expression::new(
                ExpressionKind::IsNull(Box::new(operand)),
                span,
            ))
        }
        Rule::expression => {
            // Grouped expression
            let expr = build_expression_inner(inner, span)?;
            Ok(Expression::new(
                ExpressionKind::Grouped(Box::new(expr)),
                span,
            ))
        }
        _ => Err(BuildError {
            message: format!("Unexpected primary expression: {:?}", inner.as_rule()),
            line: span.line,
            column: span.column,
        }),
    }
}

fn build_dot_notation(pair: Pair<Rule>, span: Span) -> BuildResult<Expression> {
    let mut segments = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::ident {
            segments.push(inner.as_str().to_string());
        }
    }

    Ok(Expression::new(
        ExpressionKind::DotNotation(DotPath::new(segments, span.clone())),
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
            elements.push(build_expression_inner(inner, span.clone())?);
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
                else_value = Some(Box::new(build_expression_inner(inner, span.clone())?));
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

    let mut inner = pair.into_inner();

    if let Some(cond_pair) = inner.next() {
        condition = build_expression_inner(cond_pair, span.clone())?;
    }

    if let Some(val_pair) = inner.next() {
        value = build_expression_inner(val_pair, span.clone())?;
    }

    Ok(WhenBranch {
        condition,
        value,
        span,
    })
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
