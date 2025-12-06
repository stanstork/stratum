use crate::ast::{
    attribute::Attribute,
    block::{ConnectionBlock, DefineBlock},
    doc::SmqlDocument,
    expr::Expression,
    pipeline::PipelineBlock,
};

/// Visitor trait for AST traversal
pub trait AstVisitor {
    fn visit_document(&mut self, doc: &SmqlDocument) {
        if let Some(define) = &doc.define_block {
            self.visit_define_block(define);
        }
        for conn in &doc.connections {
            self.visit_connection_block(conn);
        }
        for pipeline in &doc.pipelines {
            self.visit_pipeline_block(pipeline);
        }
    }

    fn visit_define_block(&mut self, _block: &DefineBlock) {}
    fn visit_connection_block(&mut self, _block: &ConnectionBlock) {}
    fn visit_pipeline_block(&mut self, _block: &PipelineBlock) {}
    fn visit_expression(&mut self, _expr: &Expression) {}
    fn visit_attribute(&mut self, _attr: &Attribute) {}
}
