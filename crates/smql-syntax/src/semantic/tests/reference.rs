use crate::{
    ast::{
        attribute::Attribute,
        block::{ConnectionBlock, DefineBlock},
        doc::SmqlDocument,
        expr::{Expression, ExpressionKind},
        operator::BinaryOperator,
        pipeline::{FromBlock, PipelineBlock, ToBlock, WhereClause},
    },
    errors::ValidationIssueKind,
    semantic::validator::validate,
};

use super::helpers::*;

#[test]
fn test_undefined_connection_reference() {
    let doc = SmqlDocument {
        define_block: None,
        connections: vec![ConnectionBlock {
            name: "db1".to_string(),
            attributes: vec![
                Attribute {
                    key: ident("driver", span(2, 3)),
                    value: string_lit("mysql", span(2, 12)),
                    span: span(2, 3),
                },
                Attribute {
                    key: ident("url", span(3, 3)),
                    value: string_lit("localhost", span(3, 9)),
                    span: span(3, 3),
                },
            ],
            nested_blocks: vec![],
            span: span(1, 1),
        }],
        pipelines: vec![PipelineBlock {
            name: "sync".to_string(),
            after: None,
            from: Some(FromBlock {
                attributes: vec![Attribute {
                    key: ident("connection", span(8, 5)),
                    value: dot_notation(&["connection", "nonexistent"], span(8, 18)),
                    span: span(8, 5),
                }],
                nested_blocks: vec![],
                span: span(7, 3),
            }),
            to: Some(ToBlock {
                attributes: vec![Attribute {
                    key: ident("connection", span(11, 5)),
                    value: dot_notation(&["connection", "db1"], span(11, 18)),
                    span: span(11, 5),
                }],
                nested_blocks: vec![],
                span: span(10, 3),
            }),
            where_clauses: vec![],
            with_block: None,
            select_block: None,
            validate_block: None,
            on_error_block: None,
            paginate_block: None,
            before_block: None,
            after_block: None,
            settings_block: None,
            span: span(6, 1),
        }],
        span: span(1, 1),
    };

    let result = validate(&doc);

    assert!(result.has_errors());
    let undefined_errors: Vec<_> = result
        .errors
        .iter()
        .filter(|e| matches!(e.kind, ValidationIssueKind::UndefinedConnection { .. }))
        .collect();
    assert_eq!(undefined_errors.len(), 1);
}

#[test]
fn test_undefined_pipeline_reference() {
    let doc = SmqlDocument {
        define_block: None,
        connections: vec![],
        pipelines: vec![PipelineBlock {
            name: "sync1".to_string(),
            after: Some(vec![dot_notation(
                &["pipeline", "nonexistent"],
                span(2, 12),
            )]),
            from: None,
            to: None,
            where_clauses: vec![],
            with_block: None,
            select_block: None,
            validate_block: None,
            on_error_block: None,
            paginate_block: None,
            before_block: None,
            after_block: None,
            settings_block: None,
            span: span(1, 1),
        }],
        span: span(1, 1),
    };

    let result = validate(&doc);

    assert!(result.has_errors());
    let undefined_errors: Vec<_> = result
        .errors
        .iter()
        .filter(|e| matches!(e.kind, ValidationIssueKind::UndefinedPipeline { .. }))
        .collect();
    assert_eq!(undefined_errors.len(), 1);
}

#[test]
fn test_undefined_define_constant() {
    let doc = SmqlDocument {
        define_block: Some(DefineBlock {
            attributes: vec![Attribute {
                key: ident("tax_rate", span(2, 3)),
                value: string_lit("1.4", span(2, 15)),
                span: span(2, 3),
            }],
            span: span(1, 1),
        }),
        connections: vec![],
        pipelines: vec![PipelineBlock {
            name: "sync".to_string(),
            after: None,
            from: None,
            to: None,
            where_clauses: vec![WhereClause {
                label: None,
                conditions: vec![Expression::new(
                    ExpressionKind::Binary {
                        left: Box::new(dot_notation(&["orders", "total"], span(7, 5))),
                        operator: BinaryOperator::Multiply,
                        right: Box::new(dot_notation(&["define", "nonexistent"], span(7, 21))),
                    },
                    span(7, 5),
                )],
                span: span(6, 3),
            }],
            with_block: None,
            select_block: None,
            validate_block: None,
            on_error_block: None,
            paginate_block: None,
            before_block: None,
            after_block: None,
            settings_block: None,
            span: span(5, 1),
        }],
        span: span(1, 1),
    };

    let result = validate(&doc);

    assert!(result.has_errors());
    let undefined_errors: Vec<_> = result
        .errors
        .iter()
        .filter(|e| matches!(e.kind, ValidationIssueKind::UndefinedDefineConstant { .. }))
        .collect();
    assert_eq!(undefined_errors.len(), 1);
}
