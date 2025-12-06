use crate::ast::span::Span;
use std::collections::{HashMap, HashSet};

/// Symbol table for semantic analysis
#[derive(Debug, Clone)]
pub struct SymbolTable {
    // Top-level declarations
    pub connections: HashMap<String, Span>,
    pub pipelines: HashMap<String, Span>,
    pub define_constants: HashMap<String, Span>,

    // Usage tracking for warnings
    pub used_connections: HashSet<String>,
    pub used_pipelines: HashSet<String>,
    pub used_define_constants: HashSet<String>,
}

impl SymbolTable {
    pub fn new() -> Self {
        SymbolTable {
            connections: HashMap::new(),
            pipelines: HashMap::new(),
            define_constants: HashMap::new(),
            used_connections: HashSet::new(),
            used_pipelines: HashSet::new(),
            used_define_constants: HashSet::new(),
        }
    }

    pub fn add_connection(&mut self, name: String, span: Span) -> Option<Span> {
        self.connections.insert(name, span)
    }

    pub fn add_pipeline(&mut self, name: String, span: Span) -> Option<Span> {
        self.pipelines.insert(name, span)
    }

    pub fn add_define_constant(&mut self, name: String, span: Span) -> Option<Span> {
        self.define_constants.insert(name, span)
    }

    pub fn mark_connection_used(&mut self, name: &str) {
        self.used_connections.insert(name.to_string());
    }

    pub fn mark_pipeline_used(&mut self, name: &str) {
        self.used_pipelines.insert(name.to_string());
    }

    pub fn mark_define_constant_used(&mut self, name: &str) {
        self.used_define_constants.insert(name.to_string());
    }

    pub fn get_unused_connections(&self) -> Vec<String> {
        self.connections
            .keys()
            .filter(|name| !self.used_connections.contains(*name))
            .cloned()
            .collect()
    }

    pub fn get_unused_define_constants(&self) -> Vec<String> {
        self.define_constants
            .keys()
            .filter(|name| !self.used_define_constants.contains(*name))
            .cloned()
            .collect()
    }
}

impl Default for SymbolTable {
    fn default() -> Self {
        Self::new()
    }
}
