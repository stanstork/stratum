use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct ShutdownSignal {
    /// Graceful pause - drain current batch, save state, exit cleanly
    pub pause: CancellationToken,
    /// Hard cancel - abort as fast as possible
    pub cancel: CancellationToken,
}

impl ShutdownSignal {
    /// Create a new ShutdownSignal with fresh tokens.
    pub fn new() -> Self {
        Self {
            pause: CancellationToken::new(),
            cancel: CancellationToken::new(),
        }
    }
}

impl Default for ShutdownSignal {
    fn default() -> Self {
        Self::new()
    }
}
