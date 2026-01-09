/// Builds the extended version string with git and build information
pub fn build_version_string() -> &'static str {
    concat!(
        env!("CARGO_PKG_VERSION"),
        "\nGit commit:  ",
        env!("GIT_HASH"),
        " (",
        env!("GIT_BRANCH"),
        ")",
        "\nBuild date:  ",
        env!("BUILD_TIMESTAMP"),
        "\nRust:        ",
        env!("RUSTC_VERSION")
    )
}

/// Prints detailed version information to stdout
pub fn print() {
    let version = env!("CARGO_PKG_VERSION");
    let git_hash = env!("GIT_HASH");
    let git_branch = env!("GIT_BRANCH");
    let build_timestamp = env!("BUILD_TIMESTAMP");
    let rustc_version = env!("RUSTC_VERSION");

    println!("stratum {}", version);
    println!("Git commit:  {} ({})", git_hash, git_branch);
    println!("Build date:  {}", build_timestamp);
    println!("Rust:        {}", rustc_version);
}
