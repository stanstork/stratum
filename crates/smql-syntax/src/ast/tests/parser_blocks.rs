use crate::parser::{Rule, SmqlParser};
use pest::Parser;

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

  pool {
    max_size = 20
    timeout = "60s"
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
  description = "Copy customers table"

  from {
    connection = connection.mysql_prod
    table = "customers"
  }

  to {
    connection = connection.warehouse_pg
    table = "customers_copy"
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
    total > 100
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
fn test_parse_select_block() {
    let input = r#"
pipeline "test" {
  select {
    customer_id = customers.id
    customer_name = customers.name
    loaded_at = now()
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
    products from products where products.id == order_items.product_id
  }
}
"#;
    let result = SmqlParser::parse(Rule::program, input);
    assert!(result.is_ok());
}

#[test]
fn test_parse_validate_block() {
    let input = r#"
pipeline "test" {
  validate {
    assert "positive_total" {
      check = orders.total >= 0
      message = "Total must be positive"
      action = skip
    }

    warn "missing_email" {
      check = users.email is not null
      message = "Email is missing"
    }
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
      backoff = "5s"
    }

    failed_rows {
      table = "failed_orders"
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
    timezone = "UTC"
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
    sql = [
      "ALTER TABLE fact_orders DISABLE TRIGGER ALL",
      "DROP INDEX IF EXISTS idx_orders_customer"
    ]
  }

  after {
    sql = [
      "CREATE INDEX idx_orders_customer ON fact_orders(customer_id)",
      "ANALYZE fact_orders"
    ]
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
    batch_size = env("batch_size")
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
  /* Block comment */
  tax_rate = 1.4  // Inline comment
}
"#;
    let result = SmqlParser::parse(Rule::program, input);
    assert!(result.is_ok());
}
