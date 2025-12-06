use crate::parser::{Rule, SmqlParser};
use pest::Parser;

#[test]
fn test_parse_complete_config() {
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
    and orders.total > 100
    and orders.created_at >= define.cutoff_date
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

    let result = SmqlParser::parse(Rule::program, input);
    assert!(result.is_ok(), "Failed to parse complete config");
}
