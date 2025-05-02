#[cfg(test)]
mod tests {
    use crate::parser::parse;

    fn assert_parses(input: &str) {
        match parse(input) {
            Ok(plan) => {
                println!("Parsed successfully: {:#?}", plan);
                assert!(true, "Parsed successfully");
            }
            Err(e) => panic!("Failed to parse valid input: {:?}", e),
        }
    }

    #[test]
    fn test_migrate() {
        let config = r#"
            CONNECTIONS (
                SOURCE(MYSQL, "mysql://user:password@localhost:3306/db"),
                DESTINATION(POSTGRES, "postgres://user:password@localhost:5432/db")
            );

            MIGRATE (
                SOURCE(TABLE, orders) -> DEST(TABLE, orders_flat) [
                    SETTINGS (
                        INFER_SCHEMA           = TRUE,
                        IGNORE_CONSTRAINTS     = TRUE,
                        CREATE_MISSING_COLUMNS = TRUE,
                        COPY_COLUMNS           = MAP_ONLY
                    ),

                    FILTER(
                        AND(
                            orders[status] = "active",
                            orders[total]  > 400,
                            users[id]      < 4
                        )
                    ),

                    LOAD(
                        TABLES(users, order_items, products),
                        MATCH(
                            ON(users[id]             -> orders[user_id]),
                            ON(order_items[order_id] -> orders[id]),
                            ON(products[product_id]  -> order_items[id])
                        )
                    ),

                    MAP(
                        users[name]                         -> user_name,
                        users[email]                        -> user_email,
                        order_items[price]                  -> order_price,
                        products[name]                      -> product_name,
                        products[price]                     -> product_price,
                        order_items[price] * 1.4            -> order_price_with_tax,
                        CONCAT(users[name], products[name]) -> concat_lookup_test
                    )
                ],

                SOURCES(TABLE, [products_a, products_b]) -> DEST(TABLE, products) [
                    SETTINGS (
                        INFER_SCHEMA = FALSE,
                        COPY_COLUMNS = ALL
                    ),

                    FILTER(
                        OR(
                            products_a[status] = "active",
                            products_b[status] = "active"
                        )
                    ),

                    MAP(
                        products_a[id]                               -> id,
                        COALESCE(products_a[name], products_b[name]) -> name,
                        products_a[price]                            -> price_a,
                        products_b[price]                            -> price_b
                    )
                ],

                SOURCE(TABLE, invoices) -> DEST(TABLE, statement) [
                    SETTINGS (
                        INFER_SCHEMA = FALSE,
                        COPY_COLUMNS = ALL
                    ),

                    FILTER(
                        invoices[date] >= "2024-01-01"
                    )
                ],

                SOURCE(API, "https://api.example.com/invoices") -> DEST(FILE, "/tmp/invoices.json") [
                    FILTER(
                        invoices[date] >= "2024-01-01"
                    )
                ]
            )
            WITH SETTINGS (
                CREATE_MISSING_TABLES = TRUE,
                BATCH_SIZE            = 1000
            );

        "#;
        assert_parses(config);
    }
}
