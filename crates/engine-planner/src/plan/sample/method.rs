use serde::Serialize;

/// Method for sampling rows from the source table
#[derive(Serialize, Debug, Clone, Default, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SamplingMethod {
    /// Take first N rows (fastest, deterministic)
    /// Uses: LIMIT N
    #[default]
    First,

    /// Take random N rows (varied sample)
    /// Uses: ORDER BY RANDOM() LIMIT N (PostgreSQL)
    /// Uses: ORDER BY RAND() LIMIT N (MySQL)
    Random,

    /// Stratified sampling across groups (representative sample)
    /// Uses: TABLESAMPLE BERNOULLI (PostgreSQL)
    /// Falls back to Random for other databases
    Stratified,

    /// Sample by specific ID values (reproducible)
    /// Uses: WHERE id IN (...)
    /// Requires ID column to be specified
    ById,
}

impl SamplingMethod {
    /// Returns SQL clause for this sampling method
    pub fn to_sql(&self, driver: &str, sample_size: usize) -> String {
        match self {
            SamplingMethod::First => {
                format!("LIMIT {}", sample_size)
            }

            SamplingMethod::Random => match driver {
                "postgres" => format!("ORDER BY RANDOM() LIMIT {}", sample_size),
                "mysql" => format!("ORDER BY RAND() LIMIT {}", sample_size),
                "sqlite" => format!("ORDER BY RANDOM() LIMIT {}", sample_size),
                _ => format!("LIMIT {}", sample_size), // Fallback to First
            },

            SamplingMethod::Stratified => match driver {
                "postgres" => {
                    // Sample approximately sample_size rows
                    // BERNOULLI samples each row independently with probability p
                    // For small tables, this might return fewer than sample_size rows
                    format!("TABLESAMPLE BERNOULLI(10) LIMIT {}", sample_size)
                }
                _ => {
                    // Fallback to random for other databases
                    Self::Random.to_sql(driver, sample_size)
                }
            },

            SamplingMethod::ById => {
                // This requires IDs to be provided separately
                // The sample collector will handle the WHERE clause
                String::new()
            }
        }
    }

    /// Human-readable description
    pub fn description(&self) -> &'static str {
        match self {
            SamplingMethod::First => "First N rows (fastest, deterministic)",
            SamplingMethod::Random => "Random N rows (varied sample)",
            SamplingMethod::Stratified => "Stratified sampling (representative)",
            SamplingMethod::ById => "Specific IDs (reproducible)",
        }
    }

    /// Parse from string (for CLI flags)
    pub fn from_name(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "first" => Some(SamplingMethod::First),
            "random" => Some(SamplingMethod::Random),
            "stratified" => Some(SamplingMethod::Stratified),
            "by_id" | "byid" | "id" => Some(SamplingMethod::ById),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_first_method_sql() {
        let method = SamplingMethod::First;
        assert_eq!(method.to_sql("postgres", 5), "LIMIT 5");
        assert_eq!(method.to_sql("mysql", 10), "LIMIT 10");
    }

    #[test]
    fn test_random_method_sql() {
        let method = SamplingMethod::Random;
        assert_eq!(method.to_sql("postgres", 5), "ORDER BY RANDOM() LIMIT 5");
        assert_eq!(method.to_sql("mysql", 5), "ORDER BY RAND() LIMIT 5");
        assert_eq!(method.to_sql("sqlite", 5), "ORDER BY RANDOM() LIMIT 5");
    }

    #[test]
    fn test_stratified_method_sql() {
        let method = SamplingMethod::Stratified;
        assert!(method.to_sql("postgres", 5).contains("TABLESAMPLE"));
        // Fallback to random for other databases
        assert!(method.to_sql("mysql", 5).contains("RAND"));
    }

    #[test]
    fn test_from_str() {
        assert_eq!(
            SamplingMethod::from_name("first"),
            Some(SamplingMethod::First)
        );
        assert_eq!(
            SamplingMethod::from_name("random"),
            Some(SamplingMethod::Random)
        );
        assert_eq!(
            SamplingMethod::from_name("stratified"),
            Some(SamplingMethod::Stratified)
        );
        assert_eq!(
            SamplingMethod::from_name("by_id"),
            Some(SamplingMethod::ById)
        );
        assert_eq!(SamplingMethod::from_name("id"), Some(SamplingMethod::ById));
        assert_eq!(SamplingMethod::from_name("invalid"), None);
    }
}
