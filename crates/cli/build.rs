use std::path::PathBuf;
use std::process::Command;

/// Embed the JS plugin runtime WASM so the CLI can auto-compile `.js` plugins with no extra setup.
fn embed_js_runtime() {
    let manifest = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    let candidates = [
        std::env::var_os("STRATUM_JS_RUNTIME").map(PathBuf::from),
        Some(manifest.join("../engine-wasm/src/tests/fixtures/stratum-plugin-js-runtime.wasm")),
    ];

    let out = PathBuf::from(std::env::var("OUT_DIR").unwrap()).join("js_runtime.wasm");
    let mut embedded = false;
    for src in candidates.into_iter().flatten() {
        if src.is_file() {
            std::fs::copy(&src, &out).expect("copy embedded JS runtime");
            println!("cargo:rerun-if-changed={}", src.display());
            embedded = true;
            break;
        }
    }
    if !embedded {
        std::fs::write(&out, []).expect("write empty JS runtime placeholder");
    }
    println!("cargo:rerun-if-env-changed=STRATUM_JS_RUNTIME");
}

fn main() {
    embed_js_runtime();

    // Get git commit hash
    let git_hash = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .and_then(|output| {
            if output.status.success() {
                String::from_utf8(output.stdout).ok()
            } else {
                None
            }
        })
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());

    // Get git branch
    let git_branch = Command::new("git")
        .args(["rev-parse", "--abbrev-ref", "HEAD"])
        .output()
        .ok()
        .and_then(|output| {
            if output.status.success() {
                String::from_utf8(output.stdout).ok()
            } else {
                None
            }
        })
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());

    // Get build timestamp in human-readable format
    let build_timestamp = chrono::Utc::now()
        .format("%Y-%m-%d %H:%M:%S UTC")
        .to_string();

    // Get Rust version
    let rustc_version = Command::new("rustc")
        .arg("--version")
        .output()
        .ok()
        .and_then(|output| {
            if output.status.success() {
                String::from_utf8(output.stdout).ok()
            } else {
                None
            }
        })
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());

    // Set environment variables for the build
    println!("cargo:rustc-env=GIT_HASH={}", git_hash);
    println!("cargo:rustc-env=GIT_BRANCH={}", git_branch);
    println!("cargo:rustc-env=BUILD_TIMESTAMP={}", build_timestamp);
    println!("cargo:rustc-env=RUSTC_VERSION={}", rustc_version);

    // Rerun if git state changes
    println!("cargo:rerun-if-changed=../../.git/HEAD");
    println!("cargo:rerun-if-changed=../../.git/refs");
}
