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

#[test]
fn test_failed_rows_with_table_nested_block_with_schema() {
    let input = r#"
pipeline "test" {
  on_error {
    failed_rows {
      table {
        connection = connection.warehouse
        schema     = "dlq"
        table      = "failed_orders"
      }
    }
  }
}
"#;
    let result = SmqlParser::parse(Rule::program, input);
    assert!(result.is_ok());
}

#[test]
fn test_failed_rows_with_table_nested_block_without_schema() {
    let input = r#"
pipeline "test" {
  on_error {
    failed_rows {
      table {
        connection = connection.error_db
        table      = "failed_rows"
      }
    }
  }
}
"#;
    let result = SmqlParser::parse(Rule::program, input);
    assert!(result.is_ok());
}

#[test]
fn test_failed_rows_with_file_nested_block_explicit_format() {
    let input = r#"
pipeline "test" {
  on_error {
    failed_rows {
      file {
        path   = "/data/errors/failed_rows.csv"
        format = "csv"
      }
    }
  }
}
"#;
    let result = SmqlParser::parse(Rule::program, input);
    assert!(result.is_ok());
}

#[test]
fn test_failed_rows_with_file_nested_block_json() {
    let input = r#"
pipeline "test" {
  on_error {
    failed_rows {
      file {
        path   = "/var/log/stratum/errors.json"
        format = "json"
      }
    }
  }
}
"#;
    let result = SmqlParser::parse(Rule::program, input);
    assert!(result.is_ok());
}

#[test]
fn test_failed_rows_with_file_nested_block_parquet() {
    let input = r#"
pipeline "test" {
  on_error {
    failed_rows {
      file {
        path   = "/data/dlq/failed.parquet"
        format = "parquet"
      }
    }
  }
}
"#;
    let result = SmqlParser::parse(Rule::program, input);
    assert!(result.is_ok());
}

#[test]
fn test_failed_rows_with_action_and_nested_block() {
    let input = r#"
pipeline "test" {
  on_error {
    failed_rows {
      action = "save_to_table"

      table {
        connection = connection.warehouse
        schema     = "errors"
        table      = "pipeline_failures"
      }
    }
  }
}
"#;
    let result = SmqlParser::parse(Rule::program, input);
    assert!(result.is_ok());
}

#[test]
fn test_failed_rows_with_old_style_attributes() {
    let input = r#"
pipeline "test" {
  on_error {
    failed_rows {
      action = "skip"
      destination = connection.error_db.failed_rows
    }
  }
}
"#;
    let result = SmqlParser::parse(Rule::program, input);
    assert!(result.is_ok());
}

#[test]
fn test_failed_rows_multiple_formats_in_different_pipelines() {
    let input = r#"
pipeline "pipeline1" {
  on_error {
    failed_rows {
      table {
        connection = connection.db1
        table      = "errors"
      }
    }
  }
}

pipeline "pipeline2" {
  on_error {
    failed_rows {
      file {
        path = "/logs/pipeline2_errors.json"
      }
    }
  }
}
"#;
    let result = SmqlParser::parse(Rule::program, input);
    assert!(result.is_ok());
}

#[test]
fn test_on_error_with_retry_and_failed_rows() {
    let input = r#"
pipeline "test" {
  on_error {
    retry {
      max_attempts = 5
      delay_ms     = 1000
    }

    failed_rows {
      action = "log"

      file {
        path   = "/data/errors/{pipeline_name}_{date}.parquet"
        format = "parquet"
      }
    }
  }
}
"#;
    let result = SmqlParser::parse(Rule::program, input);
    assert!(result.is_ok());
}
