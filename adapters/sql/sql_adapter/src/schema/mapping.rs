use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct NameMap {
    forward: HashMap<String, String>, // old_name → new_name
    reverse: HashMap<String, String>, // new_name → old_name
}

impl NameMap {
    pub fn new(map: HashMap<String, String>) -> Self {
        let mut forward = HashMap::new();
        let mut reverse = HashMap::new();

        for (k, v) in map.into_iter() {
            let k_lower = k.to_ascii_lowercase();
            let v_lower = v.to_ascii_lowercase();

            forward.insert(k_lower.clone(), v_lower.clone());
            reverse.insert(v_lower, k_lower);
        }

        Self { forward, reverse }
    }

    /// Resolve old → new (default direction)
    pub fn resolve(&self, name: &str) -> String {
        let lower = name.to_ascii_lowercase();
        self.forward
            .get(&lower)
            .cloned()
            .unwrap_or_else(|| name.to_string())
    }

    /// Reverse resolve new → old
    pub fn reverse_resolve(&self, name: &str) -> String {
        let lower = name.to_ascii_lowercase();
        self.reverse
            .get(&lower)
            .cloned()
            .unwrap_or_else(|| name.to_string())
    }
}
