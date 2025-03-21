#[cfg(test)]
mod tests {
    use crate::parser::parse;

    fn assert_parses(input: &str) {
        match parse(input) {
            Ok(_) => assert!(true),
            Err(e) => panic!("Failed to parse valid input: {:?}", e),
        }
    }

    #[test]
    fn test_parse_migrate() {
        let input = r#"
            CONNECTIONS (
                SOURCE MYSQL "mysql://user:password@localhost:3306/db",
                DESTINATION POSTGRES "postgres://user:password@localhost:5432/db"
            );

            MIGRATE orders TO orders_collection
            WITH SETTINGS (
                INFER_SCHEMA = TRUE,
                CREATE_TABLE = TRUE
            );
        "#;
        assert_parses(input);
    }

    #[test]
    fn test_parse_map_with_functions() {
        let input = r#"
            CONNECTIONS (
                SOURCE MYSQL "mysql://user:password@localhost:3306/db",
                DESTINATION POSTGRES "postgres://user:password@localhost:5432/db"
            );

            MIGRATE orders TO orders_collection
            WITH SETTINGS (
                INFER_SCHEMA = TRUE,
                CREATE_TABLE = TRUE
            );

            MAP (
                COALESCE(products_a[id], products_b[id]) -> product_id,
                CONCAT(products_a[name], " / ", products_b[name]) -> product_name,
                ROUND(products_a[price] * 1.2, 2) -> price_with_tax
            );
        "#;
        assert_parses(input);
    }

    #[test]
    fn test_parse_filter() {
        let input = r#"
            CONNECTIONS (
                SOURCE MYSQL "mysql://user:password@localhost:3306/db",
                DESTINATION POSTGRES "postgres://user:password@localhost:5432/db"
            );

            MIGRATE orders TO orders_collection
            WITH SETTINGS (
                INFER_SCHEMA = TRUE,
                CREATE_TABLE = TRUE
            );

            FILTER (
                price > 100,
                status = "paid"
            );
        "#;
        assert_parses(input);
    }

    #[test]
    fn test_parse_aggregate() {
        let input = r#"
            CONNECTIONS (
                SOURCE MYSQL "mysql://user:password@localhost:3306/db",
                DESTINATION POSTGRES "postgres://user:password@localhost:5432/db"
            );

            MIGRATE orders TO orders_collection
            WITH SETTINGS (
                INFER_SCHEMA = TRUE,
                CREATE_TABLE = TRUE
            );

            AGGREGATE (
                SUM(price) -> total_price,
                COUNT(*) -> order_count,
                AVG(discount) -> avg_discount
            );
        "#;
        assert_parses(input);
    }

    #[test]
    fn test_parse_complex_migrate_with_multiple_sources() {
        let input = r#"
            CONNECTIONS (
                SOURCE MYSQL "mysql://user:password@localhost:3306/db",
                DESTINATION POSTGRES "postgres://user:password@localhost:5432/db"
            );

            MIGRATE orders TO orders_collection
            WITH SETTINGS (
                INFER_SCHEMA = TRUE,
                CREATE_TABLE = TRUE
            );

            MAP (
                COALESCE(products_a[id], products_b[id]) -> product_id,
                COALESCE(products_a[name], products_b[name]) -> product_name,
                COALESCE(products_a[price], products_b[price]) -> product_price
            );
        "#;
        assert_parses(input);
    }

    #[test]
    fn test_parse_nested_function_calls() {
        let input = r#"
            CONNECTIONS (
                SOURCE MYSQL "mysql://user:password@localhost:3306/db",
                DESTINATION POSTGRES "postgres://user:password@localhost:5432/db"
            );

            MIGRATE orders TO orders_collection
            WITH SETTINGS (
                INFER_SCHEMA = TRUE,
                CREATE_TABLE = TRUE
            );

            MAP (
                ROUND(SUM(products_a[price]) * 1.2, 2) -> total_price_adjusted,
                UPPER(CONCAT(products_a[name], " ", products_b[name])) -> full_product_name
            );
        "#;
        assert_parses(input);
    }

    #[test]
    fn test_parse_large_config() {
        let input = r#"
            CONNECTIONS (
                SOURCE MYSQL "mysql://user:password@source_db",
                DESTINATION POSTGRES "postgres://user:password@dest_db"
            );

            MIGRATE users TO customers
            WITH SETTINGS (
                INFER_SCHEMA = TRUE,
                CREATE_TABLE = TRUE,
                BATCH_SIZE = 500
            );

            FILTER (
                age > 18,
                status = "active"
            );

            LOAD orders FROM order_table USING order_id;

            MAP (
                COALESCE(users[id], orders[user_id]) -> customer_id,
                CONCAT(users[first_name], " ", users[last_name]) -> full_name,
                ROUND(users[balance] * 1.05, 2) -> adjusted_balance,
                orders[status] -> order_status
            );

            AGGREGATE (
                SUM(orders[total]) -> total_revenue,
                COUNT(users[id]) -> active_users
            );
        "#;
        assert_parses(input);
    }

    #[test]
    fn test_parse_mixed_case_and_whitespace() {
        let input = r#"
                connections (
                    source mysql "mysql://user:pass@db",
                    destination postgres "postgres://user:pass@db"
                );

                migrate   Orders   TO customers   WITH SETTINGS ( infer_schema = true   );
                
                filter (  age   >= 18  ,   status =  "active"   );

                map ( 
                    Coalesce(Users[id], Orders[user_id]) -> CustomerId,
                    ROUND( Users[balance] * 1.1 , 2 ) -> adjusted_balance
                );
        "#;
        assert_parses(input);
    }

    #[test]
    fn test_parse_complex_nested_expressions() {
        let input = r#"
            CONNECTIONS (
                SOURCE MYSQL "mysql://user:password@localhost:3306/db",
                DESTINATION POSTGRES "postgres://user:password@localhost:5432/db"
            );

            MIGRATE orders TO orders_collection
            WITH SETTINGS (
                INFER_SCHEMA = TRUE,
                CREATE_TABLE = TRUE
            );

            MAP (
                ROUND(AVG(COALESCE(products_a[price], products_b[price]) * 1.2), 2) -> adjusted_price
            );
        "#;
        assert_parses(input);
    }

    #[test]
    fn test_parse_multiple_statements() {
        let input = r#"
            CONNECTIONS (
                SOURCE MYSQL "mysql://source_db",
                DESTINATION POSTGRES "postgres://dest_db"
            );

            MIGRATE orders TO transactions;

            FILTER (
                status = "shipped",
                total > 100
            );

            MAP (
                orders[id] -> transaction_id,
                orders[user_id] -> customer_id,
                orders[total] * 1.1 -> total_with_tax
            );

            AGGREGATE (
                SUM(orders[total]) -> total_revenue,
                COUNT(orders[id]) -> order_count
            );
        "#;
        assert_parses(input);
    }
}
