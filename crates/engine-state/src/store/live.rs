use crate::models::RunState;
use std::cmp::Reverse;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

/// Suffix for a run's published status file.
const LIVE_SUFFIX: &str = ".run.json";

#[inline]
pub fn live_run_path(dir: &Path, run_id: &str) -> PathBuf {
    dir.join(format!("{run_id}{LIVE_SUFFIX}"))
}

/// Publish `state` for readers that cannot take the database lock.
pub fn write_live_run(dir: &Path, state: &RunState) -> io::Result<()> {
    fs::create_dir_all(dir)?;

    let path = live_run_path(dir, &state.run_id);
    let tmp = dir.join(format!("{}{LIVE_SUFFIX}.tmp", state.run_id));

    let bytes = serde_json::to_vec_pretty(state).map_err(io::Error::other)?;
    fs::write(&tmp, bytes)?;
    fs::rename(&tmp, &path)
}

/// Remove a run's published status file, if present.
pub fn remove_live_run(dir: &Path, run_id: &str) {
    let _ = fs::remove_file(live_run_path(dir, run_id));
}

/// Read the published status for `run_id`.
pub fn read_live_run(dir: &Path, run_id: &str) -> Option<RunState> {
    let bytes = fs::read(live_run_path(dir, run_id)).ok()?;
    serde_json::from_slice(&bytes).ok()
}

/// Every published run status in `dir`, newest first. Unreadable files are
/// skipped rather than reported.
pub fn list_live_runs(dir: &Path) -> Vec<RunState> {
    let Ok(entries) = fs::read_dir(dir) else {
        return Vec::new();
    };

    let mut runs: Vec<_> = entries
        .filter_map(Result::ok)
        .filter(|e| {
            // Ensure we don't try to read a directory named `foo.run.json`
            e.file_type().is_ok_and(|ft| ft.is_file())
                && e.file_name()
                    .to_str()
                    .is_some_and(|n| n.ends_with(LIVE_SUFFIX))
        })
        .filter_map(|e| fs::read(e.path()).ok())
        .filter_map(|bytes| serde_json::from_slice::<RunState>(&bytes).ok())
        .collect();

    runs.sort_unstable_by_key(|r| Reverse(r.started_at));
    runs
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{RunState, RunStatus};
    use chrono::Utc;

    fn run(id: &str) -> RunState {
        RunState {
            run_id: id.to_string(),
            config_path: "migration.ppl".to_string(),
            config_hash: "hash".to_string(),
            status: RunStatus::Running,
            started_at: Utc::now(),
            total_pipelines: 2,
            pipelines: Vec::new(),
        }
    }

    #[test]
    fn roundtrips_a_published_run() {
        let dir = tempfile::tempdir().unwrap();
        write_live_run(dir.path(), &run("abc")).unwrap();

        let loaded = read_live_run(dir.path(), "abc").expect("published run");
        assert_eq!(loaded.run_id, "abc");
        assert_eq!(loaded.total_pipelines, 2);
    }

    #[test]
    fn lists_only_published_runs_and_skips_junk() {
        let dir = tempfile::tempdir().unwrap();
        write_live_run(dir.path(), &run("one")).unwrap();
        write_live_run(dir.path(), &run("two")).unwrap();
        fs::write(dir.path().join("conf"), b"not json").unwrap();
        fs::write(dir.path().join("bad.run.json"), b"{").unwrap();

        let runs = list_live_runs(dir.path());
        assert_eq!(runs.len(), 2);
    }

    #[test]
    fn removes_a_published_run() {
        let dir = tempfile::tempdir().unwrap();
        write_live_run(dir.path(), &run("gone")).unwrap();
        remove_live_run(dir.path(), "gone");

        assert!(read_live_run(dir.path(), "gone").is_none());
    }

    #[test]
    fn missing_file_reads_as_none() {
        let dir = tempfile::tempdir().unwrap();
        assert!(read_live_run(dir.path(), "nope").is_none());
        assert!(list_live_runs(dir.path()).is_empty());
    }
}
