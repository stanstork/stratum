/// Helper for navigating complex EXPLAIN JSON structures
pub struct ExplainParser;

impl ExplainParser {
    /// Extract estimated rows from PostgreSQL EXPLAIN output
    /// For JOINs: returns the root plan's estimated output rows (after filters + joins)
    /// For single table: returns the scan node's estimated rows
    pub fn extract_pg(explain: &serde_json::Value) -> Option<f64> {
        explain
            .as_array()
            .and_then(|arr| arr.first())
            .and_then(|root| root.get("Plan"))
            .and_then(|plan| {
                // For JOINs, the top-level Plan Rows represents the final output
                // This is the correct selectivity metric
                plan["Plan Rows"].as_f64()
            })
    }

    /// Extract estimated rows from MySQL EXPLAIN output
    /// For JOINs: MySQL doesn't provide final row estimates directly in EXPLAIN FORMAT=JSON
    /// We need to look at the outermost table's rows_examined_per_scan or calculate from nested_loop
    pub fn extract_mysql(explain: &serde_json::Value, table: &str) -> Option<f64> {
        let qb = &explain["query_block"];

        // Single table case
        if let Some(rows) = qb["table"]["rows_examined_per_scan"].as_f64() {
            return Some(rows);
        }

        if let Some(nl) = qb["nested_loop"].as_array() {
            // Try to find the source table specifically
            for item in nl {
                if let Some(table_map) = item["table"].as_object()
                    && table_map.get("table_name").and_then(|v| v.as_str()) == Some(table)
                {
                    return table_map
                        .get("rows_examined_per_scan")
                        .and_then(|v| v.as_f64());
                }
            }

            // If source table not found, use the first table's rows (driving table)
            if let Some(first_item) = nl.first()
                && let Some(rows) = first_item["table"]["rows_examined_per_scan"].as_f64()
            {
                return Some(rows);
            }
        }

        None
    }
}
