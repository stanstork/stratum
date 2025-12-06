//! Tests that combine parsing with semantic validation

use smql_syntax::builder::parse;
use smql_syntax::semantic::validator::validate;

#[test]
fn test_parse_and_validate() {
    let input = r#"
        define {
            tax_rate = 1.4
        }

        connection "db" {
            driver = "mysql"
            url = "localhost"
        }

        pipeline "sync" {
            from {
                connection = connection.db
            }
            to {
                connection = connection.db
            }

            select {
                total_with_tax = orders.total * define.tax_rate
            }
        }
    "#;

    let doc = parse(input).expect("Parse failed");
    let result = validate(&doc);

    assert!(result.is_valid(), "Validation failed: {:?}", result);
    assert_eq!(result.errors.len(), 0);
    assert_eq!(result.warnings.len(), 0);
}

#[test]
fn test_parse_error_undefined_connection() {
    let input = r#"
        pipeline "sync" {
            from {
                connection = connection.nonexistent
            }
            to {
                connection = connection.db
            }
        }
    "#;

    let doc = parse(input).expect("Parse should succeed");
    let result = validate(&doc);

    // Should have errors about undefined connection
    assert!(result.has_errors());
    assert!(result.errors.iter().any(|e| matches!(
        e.kind,
        smql_syntax::errors::ValidationIssueKind::UndefinedConnection { .. }
    )));
}

#[test]
fn test_parse_error_duplicate_names() {
    let input = r#"
        connection "db" {
            driver = "mysql"
            url = "localhost"
        }

        connection "db" {
            driver = "postgres"
            url = "localhost"
        }
    "#;

    let doc = parse(input).expect("Parse should succeed");
    let result = validate(&doc);

    // Should have error about duplicate connection
    assert!(result.has_errors());
}

#[test]
fn test_parse_error_missing_required_fields() {
    let input = r#"
        connection "db" {
            // Missing driver and url
        }
    "#;

    let doc = parse(input).expect("Parse should succeed");
    let result = validate(&doc);

    // Should have errors about missing fields
    assert!(result.has_errors());
    assert!(result.errors.len() >= 2); // Missing driver and url
}
