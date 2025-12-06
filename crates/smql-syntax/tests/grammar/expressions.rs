//! Grammar tests for expression parsing (literals, operators, functions)

use pest::Parser;
use smql_syntax::parser::{Rule, SmqlParser};

#[test]
fn test_parse_literals() {
    let inputs = vec![
        r#"define { str = "hello" }"#,
        r#"define { num = 42 }"#,
        r#"define { float = 3.14 }"#,
        r#"define { bool_true = true }"#,
        r#"define { bool_false = false }"#,
        r#"define { nul = null }"#,
    ];

    for input in inputs {
        let result = SmqlParser::parse(Rule::program, input);
        assert!(result.is_ok(), "Failed to parse: {}", input);
    }
}

#[test]
fn test_parse_dot_notation() {
    let input = r#"
define {
  conn_ref = connection.mysql_prod
  col_ref = customers.email
  nested = define.tax_rate
}
"#;
    let result = SmqlParser::parse(Rule::program, input);
    assert!(result.is_ok());
}

#[test]
fn test_parse_function_calls() {
    let input = r#"
define {
  env_var = env("DB_HOST")
  timestamp = now()
  date_val = date("2024-01-01")
  upper_val = upper(customers.name)
}
"#;
    let result = SmqlParser::parse(Rule::program, input);
    assert!(result.is_ok());
}

#[test]
fn test_parse_array_literals() {
    let input = r#"
define {
  numbers = [1, 2, 3]
  strings = ["a", "b", "c"]
  mixed = [1, "two", true, null]
  empty = []
}
"#;
    let result = SmqlParser::parse(Rule::program, input);
    assert!(result.is_ok());
}

#[test]
fn test_parse_when_expression() {
    let input = r#"
pipeline "test" {
  select {
    tier = when {
      orders.total > 10000 then "enterprise"
      orders.total > 1000 then "business"
      else "standard"
    }
  }
}
"#;
    let result = SmqlParser::parse(Rule::program, input);
    assert!(result.is_ok());
}

#[test]
fn test_parse_binary_operations() {
    let input = r#"
define {
  comparison = value > 100
  equality = status == "active"
  inequality = status != "inactive"
  logical_and = age > 18 && age < 65
  logical_or = role == "admin" || role == "owner"
  arithmetic = total * 1.4
}
"#;
    let result = SmqlParser::parse(Rule::program, input);
    assert!(result.is_ok());
}

#[test]
fn test_parse_is_null_check() {
    let input = r#"
pipeline "test" {
  where {
    users.email is not null
    users.phone is null
  }
}
"#;
    let result = SmqlParser::parse(Rule::program, input);
    assert!(result.is_ok());
}

#[test]
fn test_parse_complex_expression() {
    let input = r#"
define {
  complex = (age > 18 && age < 65) || status == "admin"
  nested = ((a + b) * c) / d
}
"#;
    let result = SmqlParser::parse(Rule::program, input);
    assert!(result.is_ok());
}

#[test]
fn test_parse_invalid_syntax() {
    let inputs = vec![
        "define connection test { }", // Wrong: should be connection "test"
        "pipeline test { }",          // Wrong: missing quotes on name
        "define { invalid @#$ }",     // Wrong: invalid characters
    ];

    for input in inputs {
        let result = SmqlParser::parse(Rule::program, input);
        assert!(result.is_err(), "Should fail: {}", input);
    }
}
