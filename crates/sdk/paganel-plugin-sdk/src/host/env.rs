/// Read a granted environment variable, or `None` if it is unset or not granted.
pub fn env_get(name: &str) -> Option<String> {
    std::env::var(name).ok()
}
