use crate::report::dry_run::DryRunReport;
use serde::Serialize;

#[derive(Serialize, Debug, Default, Clone)]
pub struct SummaryReport {
    pub dry_run_report: Option<DryRunReport>,
}
