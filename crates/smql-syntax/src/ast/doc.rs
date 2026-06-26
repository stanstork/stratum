use crate::ast::{
    block::{ConnectionBlock, DefineBlock, ExecutionBlock, PluginBlock},
    pipeline::PipelineBlock,
    span::Span,
};
use serde::{Deserialize, Serialize};

/// Root document containing all top-level declarations
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SmqlDocument {
    pub define_block: Option<DefineBlock>,
    pub execution_block: Option<ExecutionBlock>,
    pub connections: Vec<ConnectionBlock>,
    pub pipelines: Vec<PipelineBlock>,
    pub plugins: Vec<PluginBlock>,
    pub span: Span,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_document_structure() {
        let span = Span::new(0, 100, 1, 1);
        let doc = SmqlDocument {
            define_block: None,
            execution_block: None,
            connections: vec![],
            pipelines: vec![],
            plugins: vec![],
            span,
        };

        assert!(doc.define_block.is_none());
        assert!(doc.execution_block.is_none());
        assert_eq!(doc.connections.len(), 0);
        assert_eq!(doc.pipelines.len(), 0);
        assert_eq!(doc.plugins.len(), 0);
    }
}
