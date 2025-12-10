//! Tests for complete, comprehensive SMQL documents

use smql_syntax::builder::parse;
use smql_syntax::semantic::validator::validate;

#[test]
fn test_parse_complete_example_config() {
    let input = r#"
// Computed values
define {
  tax_rate = 1.4
  cutoff_date = "2024-01-01"
  active_status = "active"
}

// Data source
connection "mysql_prod" {
  driver = "mysql"
  url = env("SOURCE_DB_URL")

  pool {
    max_size = env("DB_POOL_SIZE", 20)
  }
}

connection "warehouse_pg" {
  driver = "postgres"
  url = env("DEST_DB")

  pool {
    max_size = 50
    timeout = "60s"
  }
}

// Simple pipeline
pipeline "copy_customers" {
  description = "Mirror customers table"

  from {
    connection = connection.mysql_prod
    table = "customers"
  }

  to {
    connection = connection.warehouse_pg
    table = "customers_copy"
    mode = "replace"
  }

  where "active_only" {
    customers.status == define.active_status
  }

  select {
    customer_id = customers.id
    customer_name = customers.name
    customer_email = lower(customers.email)
    loaded_at = now()
  }
}

// Complex pipeline with joins
pipeline "orders_denormalized" {
  description = "Create wide analytics table"

  after = [pipeline.copy_customers]

  from {
    connection = connection.mysql_prod
    table = "orders"
  }

  to {
    connection = connection.warehouse_pg
    table = "fact_orders"
    mode = "append"
  }

  where "valid_orders" {
    orders.status == define.active_status
    orders.total > 100
    orders.created_at >= define.cutoff_date
  }

  with {
    users from users where users.id == orders.user_id
    products from products where products.id == order_items.product_id
    regions from regions where regions.id == orders.region_id
  }

  select {
    order_id = orders.id
    customer_id = orders.user_id
    product_id = order_items.product_id
    customer_name = users.name
    product_name = products.name
    region_name = regions.name
    order_total = orders.total
    order_tax = orders.total * define.tax_rate
    order_date = date(orders.created_at)

    tier = when {
      orders.total > 10000 then "enterprise"
      orders.total > 1000 then "business"
      else "standard"
    }

    synced_at = now()
  }

  validate {
    assert "positive_total" {
      check = orders.total >= 0
      message = "Order total cannot be negative"
      action = skip
    }

    warn "missing_email" {
      check = users.email is not null
      message = "Customer email is missing"
    }
  }

  on_error {
    retry {
      max_attempts = 3
      backoff = "5s"
    }

    failed_rows {
      table = "failed_orders"
    }
  }

  paginate {
    using = "timestamp"
    column = orders.updated_at
    tiebreaker = orders.id
    timezone = "UTC"
  }

  before {
    sql = [
      "ALTER TABLE fact_orders DISABLE TRIGGER ALL",
      "DROP INDEX IF EXISTS idx_orders_customer"
    ]
  }

  after {
    sql = [
      "CREATE INDEX IF NOT EXISTS idx_orders_customer ON fact_orders(customer_id)",
      "CREATE INDEX IF NOT EXISTS idx_orders_date ON fact_orders(order_date)",
      "ALTER TABLE fact_orders ENABLE TRIGGER ALL",
      "ANALYZE fact_orders"
    ]
  }

  settings {
    batch_size = env("batch_size")
    workers = 4
    checkpoint = every_batch
  }
}
"#;

    let result = parse(input);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let doc = result.unwrap();

    // Verify define block
    assert!(doc.define_block.is_some());
    let define = doc.define_block.as_ref().unwrap();
    assert_eq!(define.attributes.len(), 3);

    // Verify connections
    assert_eq!(doc.connections.len(), 2);
    assert_eq!(doc.connections[0].name, "mysql_prod");
    assert_eq!(doc.connections[1].name, "warehouse_pg");

    // Verify pipelines
    assert_eq!(doc.pipelines.len(), 2);
    assert_eq!(doc.pipelines[0].name, "copy_customers");
    assert_eq!(doc.pipelines[1].name, "orders_denormalized");

    // Verify first pipeline
    let pipeline1 = &doc.pipelines[0];
    assert!(pipeline1.from.is_some());
    assert!(pipeline1.to.is_some());
    assert_eq!(pipeline1.where_clauses.len(), 1);
    assert!(pipeline1.select_block.is_some());

    // Verify second pipeline with all features
    let pipeline2 = &doc.pipelines[1];
    assert!(pipeline2.from.is_some());
    assert!(pipeline2.to.is_some());
    assert_eq!(pipeline2.where_clauses.len(), 1);
    assert!(pipeline2.with_block.is_some());
    assert!(pipeline2.select_block.is_some());
    assert!(pipeline2.validate_block.is_some());
    assert!(pipeline2.on_error_block.is_some());
    assert!(pipeline2.paginate_block.is_some());
    assert!(pipeline2.before_block.is_some());
    assert!(pipeline2.after_block.is_some());
    assert!(pipeline2.settings_block.is_some());

    // Verify with block has 3 joins
    let with_block = pipeline2.with_block.as_ref().unwrap();
    assert_eq!(with_block.joins.len(), 3);

    // Verify validate block has 2 checks
    let validate_block = pipeline2.validate_block.as_ref().unwrap();
    assert_eq!(validate_block.checks.len(), 2);

    // Verify before/after SQL statements
    let before = pipeline2.before_block.as_ref().unwrap();
    assert_eq!(before.sql.len(), 2);

    let after = pipeline2.after_block.as_ref().unwrap();
    assert_eq!(after.sql.len(), 4);

    // Validate the document
    let validation_result = validate(&doc);
    assert!(
        validation_result.is_valid(),
        "Validation failed: {:?}",
        validation_result
    );
}

#[test]
fn test_parse_complete_document() {
    let input = r#"
        define {
            tax_rate = 1.4
            cutoff_date = "2024-01-01"
        }

        connection "mysql_prod" {
            driver = "mysql"
            url = env("DB_URL")

            pool {
                max_size = 20
            }
        }

        pipeline "copy_customers" {
            from {
                connection = connection.mysql_prod
                table = "customers"
            }

            to {
                connection = connection.mysql_prod
                table = "customers_backup"
            }

            where {
                created_at > define.cutoff_date
            }

            select {
                id = customers.id
                name = customers.name
                total_with_tax = customers.total * define.tax_rate
            }
        }
    "#;

    let result = parse(input);
    assert!(result.is_ok(), "Parse failed: {:?}", result.err());

    let doc = result.unwrap();

    // Check define block
    assert!(doc.define_block.is_some());
    let define = doc.define_block.unwrap();
    assert_eq!(define.attributes.len(), 2);

    // Check connections
    assert_eq!(doc.connections.len(), 1);
    let conn = &doc.connections[0];
    assert_eq!(conn.name, "mysql_prod");
    assert_eq!(conn.nested_blocks.len(), 1);

    // Check pipelines
    assert_eq!(doc.pipelines.len(), 1);
    let pipeline = &doc.pipelines[0];
    assert_eq!(pipeline.name, "copy_customers");
    assert!(pipeline.from.is_some());
    assert!(pipeline.to.is_some());
    assert_eq!(pipeline.where_clauses.len(), 1);
    assert!(pipeline.select_block.is_some());
}
