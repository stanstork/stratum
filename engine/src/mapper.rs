use std::collections::HashMap;

pub struct TableNameMap<'a> {
    map: &'a HashMap<String, String>,
}

impl<'a> TableNameMap<'a> {
    pub fn new(map: &'a HashMap<String, String>) -> Self {
        Self { map }
    }

    pub fn resolve(&self, name: &str) -> String {
        self.map
            .get(name)
            .cloned()
            .unwrap_or_else(|| name.to_string())
    }
}
