//! Tests for block structure parsing

use smql_syntax::builder::parse;

#[test]
fn test_parse_simple_define() {
    let input = r#"
        define {
            tax_rate = 1.4
        }
    "#;

    let result = parse(input);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let doc = result.unwrap();
    assert!(doc.define_block.is_some());

    let define = doc.define_block.unwrap();
    assert_eq!(define.attributes.len(), 1);
    assert_eq!(define.attributes[0].key.name, "tax_rate");
}

#[test]
fn test_parse_connection() {
    let input = r#"
        connection "mysql_prod" {
            driver = "mysql"
            url = "localhost:3306"
        }
    "#;

    let result = parse(input);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let doc = result.unwrap();
    assert_eq!(doc.connections.len(), 1);

    let conn = &doc.connections[0];
    assert_eq!(conn.name, "mysql_prod");
    assert_eq!(conn.attributes.len(), 2);
}

#[test]
fn test_parse_pipeline() {
    let input = r#"
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
        }
    "#;

    let result = parse(input);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let doc = result.unwrap();
    assert_eq!(doc.connections.len(), 1);
    assert_eq!(doc.pipelines.len(), 1);

    let pipeline = &doc.pipelines[0];
    assert_eq!(pipeline.name, "sync");
    assert!(pipeline.from.is_some());
    assert!(pipeline.to.is_some());
}

#[test]
fn test_parse_validate_block() {
    let input = r#"
        pipeline "sync" {
            from { connection = connection.db }
            to { connection = connection.db }

            validate {
                assert "customer_id_not_null" {
                  check = customers.id is not null
                  message = "Customer ID cannot be null"
                  action = skip
                }

                warn "missing_email" {
                  check = customers.email is not null
                  message = "Customer email is missing"
                }
            }

        }
    "#;

    let result = parse(input);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let doc = result.unwrap();
    let pipeline = &doc.pipelines[0];
    assert!(pipeline.validate_block.is_some());

    let validate = pipeline.validate_block.as_ref().unwrap();
    assert_eq!(validate.checks.len(), 2);
}

#[test]
fn test_parse_nested_blocks() {
    let input = r#"
        connection "db" {
            driver = "mysql"
            url = "localhost"

            pool {
                max_size = 20
                min_size = 5
            }

            retry {
                max_attempts = 3
            }
        }
    "#;

    let result = parse(input);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let doc = result.unwrap();
    let conn = &doc.connections[0];
    assert_eq!(conn.nested_blocks.len(), 2);
}

#[test]
fn test_parse_join_clause() {
    let input = r#"
        pipeline "sync" {
            from { connection = connection.db }
            to { connection = connection.db }

            with {
              orders from orders where customers.id == orders.customer_id
              products from products where orders.product_id == products.id
            }
        }
    "#;

    let result = parse(input);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let doc = result.unwrap();
    let pipeline = &doc.pipelines[0];
    assert!(pipeline.with_block.is_some());

    let with_block = pipeline.with_block.as_ref().unwrap();
    assert_eq!(with_block.joins.len(), 2);
}

#[test]
fn test_parse_on_error_block() {
    let input = r#"
        pipeline "sync" {
            from { connection = connection.db }
            to { connection = connection.db }

            on_error {
                retry {
                    max_attempts = 3
                    backoff = "exponential"
                }

                failed_rows {
                    table = "failed_rows"
                }
            }
        }
    "#;

    let result = parse(input);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let doc = result.unwrap();
    let pipeline = &doc.pipelines[0];
    assert!(pipeline.on_error_block.is_some());

    let on_error = pipeline.on_error_block.as_ref().unwrap();
    assert!(on_error.retry.is_some());
    assert!(on_error.failed_rows.is_some());
}

#[test]
fn test_parse_with_comments() {
    let input = r#"
        // This is a comment
        define {
            tax_rate = 1.4  // inline comment
        }

        /* Block comment
           spanning multiple lines */
        connection "db" {
            driver = "mysql"
            url = "localhost"
        }
    "#;

    let result = parse(input);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
}
