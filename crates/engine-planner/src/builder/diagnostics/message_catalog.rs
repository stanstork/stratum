use toml::Value;

pub struct MessageCatalog {
    data: Value,
    missing: &'static str,
}

impl MessageCatalog {
    pub fn from_toml(content: &'static str, missing: &'static str) -> Self {
        let data = toml::from_str(content).expect("Failed to parse message catalog");
        Self { data, missing }
    }

    pub fn get_message(&self, category: &str, key: &str) -> String {
        self.data
            .get(category)
            .and_then(|cat| cat.get(key))
            .and_then(|v| v.as_str())
            .unwrap_or(self.missing)
            .to_string()
    }

    pub fn get_str(&self, category: &str, key: &str) -> Option<&str> {
        self.data
            .get(category)
            .and_then(|cat| cat.get(key))
            .and_then(|v| v.as_str())
    }
}

pub struct ThresholdCatalog {
    data: Value,
}

impl ThresholdCatalog {
    pub fn from_toml(content: &'static str) -> Self {
        let data = toml::from_str(content).expect("Failed to parse threshold catalog");
        Self { data }
    }

    pub fn get_f64(&self, category: &str, key: &str, default: f64) -> f64 {
        self.data
            .get(category)
            .and_then(|cat| cat.get(key))
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse().ok())
            .unwrap_or(default)
    }
}
