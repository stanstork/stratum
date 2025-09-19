use crate::error::MigrationError;
use serde::Serialize;
use tracing::{error, info, warn};

#[derive(Debug, Clone, Serialize)]
pub struct MetricsReport {
    records_processed: u64,
    bytes_transferred: u64,
    status: String,
}

impl MetricsReport {
    pub fn new(records_processed: u64, bytes_transferred: u64, status: String) -> Self {
        MetricsReport {
            records_processed,
            bytes_transferred,
            status,
        }
    }
}

/// Sends the final report to the configured callback URL.
pub async fn send_report(report: MetricsReport) -> Result<(), MigrationError> {
    let callback_url = match std::env::var("REPORT_CALLBACK_URL") {
        Ok(url) => url,
        Err(_) => {
            error!("CALLBACK_URL environment variable not set. Cannot send report.");
            return Err(MigrationError::MissingCallbackUrl);
        }
    };

    let auth_token = match std::env::var("AUTH_TOKEN") {
        Ok(token) => token,
        Err(_) => {
            error!("AUTH_TOKEN environment variable not set. Cannot send authenticated report.");
            return Err(MigrationError::MissingAuthToken);
        }
    };

    let client = reqwest::Client::new();
    let mut attempts = 0;
    let max_attempts = 5; // Maximum number of retry attempts. TODO: Move to env
    let mut backoff = 100; // Initial backoff duration in milliseconds. TODO: Move to env

    while attempts < max_attempts {
        info!("Attempt {} to send final report...", attempts);
        let response = client
            .post(&callback_url)
            .bearer_auth(&auth_token)
            .json(&report)
            .send()
            .await;

        match response {
            Ok(resp) if resp.status().is_success() => {
                info!("Report sent successfully");
                return Ok(());
            }
            Ok(resp) => {
                warn!("Attempt {} failed with status: {}", attempts, resp.status());
            }
            Err(err) => {
                warn!("Attempt {} failed with error: {}", attempts, err);
            }
        }

        attempts += 1;
        backoff *= 2; // Exponential backoff
        info!("Retrying in {} milliseconds...", backoff);
        tokio::time::sleep(tokio::time::Duration::from_millis(backoff)).await;
    }

    Err(MigrationError::ReportFailed)
}
