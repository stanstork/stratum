use thiserror::Error;

/// Errors produced while compiling a JavaScript plugin to WASM.
#[derive(Debug, Error)]
pub enum CompileError {
    /// Bundling the JS (esbuild) failed.
    #[error("bundling failed: {0}")]
    Bundle(String),

    /// Running the bundle in QuickJS or reading its metadata failed.
    #[error("metadata extraction failed: {0}")]
    Metadata(String),

    /// Parsing or rewriting the runtime WASM failed.
    #[error("runtime patching failed: {0}")]
    Patch(String),

    /// Reading the runtime blob or writing the output failed.
    #[error("io error: {0}")]
    Io(String),
}
