//! Grammar tests for block structures (define, connection, pipeline blocks)

use pest::Parser;
use smql_syntax::parser::{Rule, SmqlParser};

#[test]
fn test_parse_empty_program() {
    let input = "";
    let result = SmqlParser::parse(Rule::program, input);
    assert!(result.is_ok());
}

#[test]
fn test_parse_define_block_simple() {
    let input = r#"
define {
  tax_rate = 1.4
  cutoff_date = "2024-01-01"
}
"#;
    let result = SmqlParser::parse(Rule::program, input);
    assert!(result.is_ok());
}

#[test]
fn test_parse_connection_block() {
    let input = r#"
connection "mysql_prod" {
  driver = "mysql"
  url = env("DB_URL")
  port = 5432
}
"#;
    let result = SmqlParser::parse(Rule::program, input);
    assert!(result.is_ok());
}

#[test]
fn test_parse_connection_with_nested_block() {
    let input = r#"
connection "mysql_prod" {
  driver = "mysql"
  url = "localhost:3306"
  
  pool {
    max_size = 20
    min_size = 5
  }
}
"#;
    let result = SmqlParser::parse(Rule::program, input);
    assert!(result.is_ok());
}

#[test]
fn test_parse_pipeline_simple() {
    let input = r#"
pipeline "copy_customers" {
  from {
    connection = connection.mysql_prod
    table = "customers"
  }
  
  to {
    connection = connection.warehouse
    table = "customers_backup"
  }
}
"#;
    let result = SmqlParser::parse(Rule::program, input);
    assert!(result.is_ok());
}

#[test]
fn test_parse_where_clause_no_label() {
    let input = r#"
pipeline "test" {
  where {
    status == "active"
    created_at > "2024-01-01"
  }
}
"#;
    let result = SmqlParser::parse(Rule::program, input);
    assert!(result.is_ok());
}

#[test]
fn test_parse_where_clause_with_label() {
    let input = r#"
pipeline "test" {
  where "active_only" {
    status == "active"
  }
}
"#;
    let result = SmqlParser::parse(Rule::program, input);
    assert!(result.is_ok());
}

#[test]
fn test_parse_with_block_joins() {
    let input = r#"
pipeline "test" {
  with {
    users from users where users.id == orders.user_id
    products from products where products.id == items.product_id
  }
}
"#;
    let result = SmqlParser::parse(Rule::program, input);
    assert!(result.is_ok());
}

#[test]
fn test_parse_before_after_blocks() {
    let input = r#"
pipeline "test" {
  before {
    sql = ["DROP INDEX IF EXISTS idx_test", "ALTER TABLE test DISABLE TRIGGER ALL"]
  }
  
  after {
    sql = ["CREATE INDEX idx_test ON test(id)", "ALTER TABLE test ENABLE TRIGGER ALL"]
  }
}
"#;
    let result = SmqlParser::parse(Rule::program, input);
    assert!(result.is_ok());
}

#[test]
fn test_parse_on_error_block() {
    let input = r#"
pipeline "test" {
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
    let result = SmqlParser::parse(Rule::program, input);
    assert!(result.is_ok());
}

#[test]
fn test_parse_paginate_block() {
    let input = r#"
pipeline "test" {
  paginate {
    using = "timestamp"
    column = orders.updated_at
    tiebreaker = orders.id
  }
}
"#;
    let result = SmqlParser::parse(Rule::program, input);
    assert!(result.is_ok());
}

#[test]
fn test_parse_settings_block() {
    let input = r#"
pipeline "test" {
  settings {
    batch_size = 1000
    workers = 4
    checkpoint = every_batch
  }
}
"#;
    let result = SmqlParser::parse(Rule::program, input);
    assert!(result.is_ok());
}

#[test]
fn test_comments_are_ignored() {
    let input = r#"
// Line comment
define {
  tax_rate = 1.4  // inline comment
}

/* Block comment
   spanning multiple lines */
connection "db" {
  driver = "mysql"
}
"#;
    let result = SmqlParser::parse(Rule::program, input);
    assert!(result.is_ok());
}
