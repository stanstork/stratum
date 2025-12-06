//! Tests for expression parsing and AST building

use smql_syntax::builder::parse;

#[test]
fn test_parse_expressions() {
    let input = r#"
        define {
            a = 1 + 2
            b = 3 * 4
            c = (5 + 6) * 7
            d = true && false
            e = x > 10
        }
    "#;

    let result = parse(input);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let doc = result.unwrap();
    let define = doc.define_block.unwrap();
    assert_eq!(define.attributes.len(), 5);
}

#[test]
fn test_parse_function_calls() {
    let input = r#"
        connection "db" {
            url = env("DB_URL")
            created = now()
            date = date("2024-01-01")
        }
    "#;

    let result = parse(input);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let doc = result.unwrap();
    let conn = &doc.connections[0];
    assert_eq!(conn.attributes.len(), 3);
}

#[test]
fn test_parse_array_literals() {
    let input = r#"
        define {
            numbers = [1, 2, 3]
            strings = ["a", "b", "c"]
            empty = []
        }
    "#;

    let result = parse(input);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let doc = result.unwrap();
    let define = doc.define_block.unwrap();
    assert_eq!(define.attributes.len(), 3);
}

#[test]
fn test_parse_when_expression() {
    let input = r#"
        define {
            status = when {
                count > 100 then "high"
                count > 50 then "medium"
                else "low"
            }
        }
    "#;

    let result = parse(input);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let doc = result.unwrap();
    let define = doc.define_block.unwrap();
    assert_eq!(define.attributes.len(), 1);
}
