use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio::signal;
use tokio_util::sync::CancellationToken;
use tracing::info;

/// Shutdown coordinator that listens for SIGINT and SIGTERM signals
/// and triggers a graceful shutdown of the migration system.
#[derive(Clone)]
pub struct ShutdownCoordinator {
    cancel_token: CancellationToken,
    shutdown_requested: Arc<AtomicBool>,
}

impl ShutdownCoordinator {
    pub fn new(cancel_token: CancellationToken) -> Self {
        Self {
            cancel_token,
            shutdown_requested: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn register_handlers(&self) {
        let cancel_token = self.cancel_token.clone();
        let shutdown_flag = self.shutdown_requested.clone();

        tokio::spawn(async move {
            let ctrl_c = async {
                signal::ctrl_c()
                    .await
                    .expect("Failed to install SIGINT handler");
            };

            #[cfg(unix)]
            let terminate = async {
                signal::unix::signal(signal::unix::SignalKind::terminate())
                    .expect("Failed to install SIGTERM handler")
                    .recv()
                    .await;
            };

            #[cfg(not(unix))]
            let terminate = std::future::pending::<()>();

            tokio::select! {
                _ = ctrl_c => {
                    info!("Received SIGINT (Ctrl+C), initiating graceful shutdown");
                }
                _ = terminate => {
                    info!("Received SIGTERM, initiating graceful shutdown");
                }
            }

            // Mark that shutdown has been requested
            shutdown_flag.store(true, Ordering::SeqCst);

            // Trigger cancellation
            cancel_token.cancel();

            info!("Shutdown signal broadcasted to all actors");
        });
    }

    pub fn is_shutdown_requested(&self) -> bool {
        self.shutdown_requested.load(Ordering::SeqCst)
    }

    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel_token.clone()
    }
}

/// Exit codes for the CLI application.
#[derive(Debug, Clone, Copy)]
pub enum ExitCode {
    Success = 0,
    GeneralError = 1,
    ShutdownRequested = 130, // Standard exit code for SIGINT
}

impl ExitCode {
    pub fn as_i32(self) -> i32 {
        self as i32
    }
}
