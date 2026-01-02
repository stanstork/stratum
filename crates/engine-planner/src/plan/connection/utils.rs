/// Mask sensitive parts of database URL for safe display/logging.
/// Replaces passwords with "****" while keeping useful connection info.
///
/// Example: `postgres://user:password@host:5432/db`
///       -> `postgres://user:****@host:5432/db`
pub fn mask_url(url: &str) -> String {
    // postgres://user:password@host:5432/db -> postgres://user:****@host:5432/db
    if let Ok(parsed) = url::Url::parse(url) {
        let mut masked = parsed.clone();
        if parsed.password().is_some() {
            masked.set_password(Some("****")).ok();
        }
        masked.to_string()
    } else {
        // Fallback: mask anything after :// and before @
        let re = regex::Regex::new(r"://([^:]+):([^@]+)@").unwrap();
        re.replace(url, "://$1:****@").to_string()
    }
}
