use crate::harness::{db::Dbms, direction::Direction};

/// A single `connection` block pointing at a read-only source fixture.
pub fn source_connection(name: &str, dbms: Dbms) -> String {
    connection(name, dbms.driver(), dbms.source_url())
}

/// A single `connection` block pointing at a scratch destination.
pub fn dest_connection(name: &str, dbms: Dbms) -> String {
    connection(name, dbms.driver(), dbms.dest_url())
}

fn connection(name: &str, driver: &str, url: &str) -> String {
    format!("connection \"{name}\" {{ driver = \"{driver}\" url = \"{url}\" }}\n")
}

/// Prepend the `src` and `dst` connections for `dir` to a pipeline body.
pub fn render(dir: Direction, body: &str) -> String {
    format!(
        "{}{}\n{body}\n",
        source_connection("src", dir.src),
        dest_connection("dst", dir.dst),
    )
}

/// Connections for the single-direction feature tests: Sakila (MySQL) source and
/// the `testdb` PostgreSQL destination, named `src` and `dst`.
pub fn feature_smql(body: &str) -> String {
    render(Direction::MYSQL_TO_POSTGRES, body)
}

/// Only the MySQL `src` connection; the body declares its own destination
/// (a WASM sink, a file, ...).
pub fn source_smql(body: &str) -> String {
    format!("{}\n{body}\n", source_connection("src", Dbms::MySql))
}

/// Only the PostgreSQL `dst` connection; the body declares its own source.
pub fn dest_smql(body: &str) -> String {
    format!("{}\n{body}\n", dest_connection("dst", Dbms::Postgres))
}

/// Read a pipeline body from `configs/<name>`.
///
/// Config files contain pipelines only. Any `define` or `connection` block left in
/// one is stripped, so a stale file cannot smuggle a hard-coded URL back in.
pub fn body(name: &str) -> String {
    let path = format!("{}/configs/{}", env!("CARGO_MANIFEST_DIR"), name);
    let raw = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read config '{path}': {e}"));
    strip_preamble(&raw)
}

/// Remove top-level `define { ... }` and `connection "..." { ... }` blocks.
///
/// Only blocks that begin a line are considered, so `connection = connection.src`
/// inside a pipeline is left alone.
fn strip_preamble(smql: &str) -> String {
    let mut out = Vec::new();
    let mut depth = 0usize;

    for line in smql.lines() {
        if depth == 0 {
            let starts_block = line.starts_with("define") || line.starts_with("connection ");
            if starts_block {
                depth += braces(line);
                continue;
            }
            out.push(line);
        } else {
            depth = depth.saturating_add(open(line)).saturating_sub(close(line));
        }
    }

    out.join("\n").trim_start().to_string()
}

fn open(line: &str) -> usize {
    line.matches('{').count()
}
fn close(line: &str) -> usize {
    line.matches('}').count()
}
/// Net open braces on a block's opening line (0 when it is a one-liner).
fn braces(line: &str) -> usize {
    open(line).saturating_sub(close(line))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_define_and_connection_blocks() {
        let raw = "define {\n    mysql_url = env(\"MYSQL_URL\", \"mysql://x\")\n}\n\n\
                   connection \"src\" { driver = \"mysql\"    url = define.mysql_url }\n\
                   connection \"dst\" { driver = \"postgres\" url = define.postgres_url }\n\n\
                   pipeline \"p\" {\n    from { connection = connection.src table = \"t\" }\n}\n";
        let body = strip_preamble(raw);
        assert!(!body.contains("define"));
        assert!(!body.contains("mysql://"));
        // `connection = connection.src` inside the pipeline must survive.
        assert!(body.contains("connection = connection.src"));
        assert!(body.starts_with("pipeline \"p\""));
    }

    #[test]
    fn strips_multiline_connection_blocks() {
        let raw = "connection \"src\" {\n    driver = \"mysql\"\n    url = \"mysql://x\"\n}\n\npipeline \"p\" {}\n";
        let body = strip_preamble(raw);
        assert!(!body.contains("mysql://"));
        assert!(body.starts_with("pipeline"));
    }

    #[test]
    fn render_injects_both_connections() {
        let out = render(Direction::MYSQL_TO_POSTGRES, "pipeline \"p\" {}");
        assert!(out.contains(r#"connection "src" { driver = "mysql""#));
        assert!(out.contains(r#"connection "dst" { driver = "postgres""#));
        assert!(out.contains("pipeline \"p\""));
    }
}

#[cfg(test)]
mod invariants {
    /// No config may name a driver or a connection URL: connections come from the
    /// harness so the same body can run in any direction. Guards against a config
    /// quietly reintroducing them.
    #[test]
    fn configs_declare_no_connections() {
        let root = concat!(env!("CARGO_MANIFEST_DIR"), "/configs");
        let mut checked = 0;
        for group in std::fs::read_dir(root).expect("configs/") {
            let group = group.expect("dir entry").path();
            if !group.is_dir() {
                continue;
            }
            for file in std::fs::read_dir(&group).expect("config group") {
                let path = file.expect("dir entry").path();
                if path.extension().and_then(|e| e.to_str()) != Some("smql") {
                    continue;
                }
                let text = std::fs::read_to_string(&path).expect("read config");
                for needle in ["mysql://", "postgres://", "driver ="] {
                    assert!(
                        !text.contains(needle),
                        "{} must not contain {needle:?}; connections come from the harness",
                        path.display()
                    );
                }
                checked += 1;
            }
        }
        assert!(
            checked > 30,
            "expected to check every config, saw {checked}"
        );
    }
}
