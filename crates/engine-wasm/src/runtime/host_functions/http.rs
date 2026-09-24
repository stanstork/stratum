use std::sync::OnceLock;

use super::{PluginState, link_err, read_guest_bytes, read_guest_string, write_guest_bytes};
use crate::error::WasmError;
use tracing::warn;
use wasmtime::{Caller, Linker};

/// Per-request wall-clock timeout for plugin HTTP calls.
const HTTP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Cap on the response body a plugin may pull back into guest memory.
const HTTP_MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;

/// Returns a shared, globally reused ureq Agent to enable connection pooling.
fn http_agent() -> &'static ureq::Agent {
    static AGENT: OnceLock<ureq::Agent> = OnceLock::new();
    AGENT.get_or_init(|| ureq::AgentBuilder::new().timeout(HTTP_TIMEOUT).build())
}

/// Numeric method codes, matching the guest SDK's `HttpMethod` enum.
fn http_method_name(code: u32) -> Option<&'static str> {
    match code {
        0 => Some("GET"),
        1 => Some("POST"),
        2 => Some("PUT"),
        3 => Some("DELETE"),
        4 => Some("PATCH"),
        _ => None,
    }
}

/// Extract the host (no port, no userinfo) from an absolute URL, cheaply.
fn url_host(url: &str) -> Option<&str> {
    let after_scheme = url.split("://").nth(1)?;
    // Authority ends at the first path/query/fragment delimiter.
    let authority = after_scheme.split(['/', '?', '#']).next()?;
    // Drop any `user:pass@` prefix.
    let host_port = authority.rsplit('@').next()?;
    if host_port.is_empty() {
        return None;
    }
    // Bracketed IPv6 literal: `[::1]:8080` -> `::1`.
    if let Some(rest) = host_port.strip_prefix('[') {
        return rest.split(']').next().filter(|h| !h.is_empty());
    }
    // Strip `:port` for host:port (IPv4 / hostname).
    Some(host_port.split(':').next().unwrap_or(host_port))
}

/// Allowlist check.
fn host_allowed(url: &str, allowlist: &[String]) -> bool {
    if allowlist.is_empty() {
        return true;
    }
    match url_host(url) {
        Some(host) => allowlist
            .iter()
            .any(|allowed| allowed.eq_ignore_ascii_case(host)),
        None => false,
    }
}

/// SSRF guard: refuse requests whose host is a link-local address.
fn is_blocked_http_host(url: &str) -> bool {
    use std::net::{Ipv4Addr, Ipv6Addr};
    let Some(host) = url_host(url) else {
        // Unparseable/relative URL: refuse rather than guess.
        return true;
    };

    if let Ok(v4) = host.parse::<Ipv4Addr>() {
        return v4.is_link_local();
    }

    if let Ok(v6) = host.parse::<Ipv6Addr>() {
        // fe80::/10 link-local, plus IPv4-mapped link-local.
        let seg = v6.segments();
        let link_local = (seg[0] & 0xffc0) == 0xfe80;
        let mapped_link_local = v6.to_ipv4().map(|v4| v4.is_link_local()).unwrap_or(false);
        return link_local || mapped_link_local;
    }

    false
}

pub(super) fn link(linker: &mut Linker<PluginState>) -> Result<(), WasmError> {
    linker
        .func_wrap(
            "paganel",
            "http_request",
            |mut caller: Caller<'_, PluginState>,
             method: u32,
             url_ptr: u32,
             url_len: u32,
             body_ptr: u32,
             body_len: u32|
             -> u64 {
                if !caller.data().capabilities.http_client {
                    return 0;
                }

                let Some(method_name) = http_method_name(method) else {
                    warn!(plugin = %caller.data().plugin_name, method, "plugin http_request: unknown method code");
                    return 0;
                };

                let Some(url) = read_guest_string(&mut caller, url_ptr, url_len) else {
                    return 0;
                };

                let body = if body_len == 0 {
                    Vec::new()
                } else {
                    match read_guest_bytes(&mut caller, body_ptr, body_len) {
                        Some(b) => b,
                        None => return 0,
                    }
                };

                let plugin = caller.data().plugin_name.clone();
                let allowlist = caller.data().capabilities.http_allowed_hosts.clone();

                if is_blocked_http_host(&url) {
                    warn!(plugin = %plugin, url = %url, "plugin http_request blocked (link-local/SSRF-guarded or unparseable host)");
                    return 0;
                }

                if !host_allowed(&url, &allowlist) {
                    warn!(plugin = %plugin, url = %url, "plugin http_request blocked (host not in allow_http_hosts)");
                    return 0;
                }

                match perform_http_request(method_name, &url, &body) {
                    Some((status, response_body)) => {
                        let mut framed = Vec::with_capacity(2 + response_body.len());
                        framed.extend_from_slice(&status.to_le_bytes());
                        framed.extend_from_slice(&response_body);
                        write_guest_bytes(&mut caller, &framed).unwrap_or(0)
                    }
                    None => {
                        warn!(plugin = %plugin, %method_name, url = %url, "plugin http_request failed");
                        0
                    }
                }
            },
        )
        .map_err(|e| link_err("http_request", e))?;
    Ok(())
}

/// Perform a blocking HTTP request and return `(status, body)`.
fn perform_http_request(method: &str, url: &str, body: &[u8]) -> Option<(u16, Vec<u8>)> {
    let request = http_agent().request(method, url);
    let send_result = if body.is_empty() {
        request.call()
    } else {
        request.send_bytes(body)
    };

    let response = match send_result {
        Ok(resp) => resp,
        Err(ureq::Error::Status(_, resp)) => resp,
        Err(ureq::Error::Transport(_)) => return None,
    };

    let status = response.status();
    let body = read_capped_body(response)?;
    Some((status, body))
}

/// Read a response body, refusing anything larger than `HTTP_MAX_RESPONSE_BYTES`.
fn read_capped_body(response: ureq::Response) -> Option<Vec<u8>> {
    use std::io::Read;

    // Read one byte past the cap so "exactly at the cap" and "over it" differ.
    let mut buf = Vec::new();
    response
        .into_reader()
        .take(HTTP_MAX_RESPONSE_BYTES as u64 + 1)
        .read_to_end(&mut buf)
        .ok()?;

    if buf.len() > HTTP_MAX_RESPONSE_BYTES {
        return None;
    }
    Some(buf)
}

#[cfg(test)]
mod tests {
    use super::{is_blocked_http_host, url_host};
    use crate::runtime::host_functions::test_harness::instantiate;
    use crate::runtime::limits::HostCapabilities;

    #[test]
    fn url_host_extracts_host_without_port_or_userinfo() {
        assert_eq!(url_host("http://example.com/path"), Some("example.com"));
        assert_eq!(url_host("https://example.com:8443/x"), Some("example.com"));
        assert_eq!(
            url_host("http://user:pass@host.internal:80/"),
            Some("host.internal")
        );
        assert_eq!(
            url_host("http://169.254.169.254/latest/meta-data"),
            Some("169.254.169.254")
        );
        assert_eq!(url_host("http://[::1]:8080/"), Some("::1"));
        assert_eq!(url_host("not-a-url"), None);
    }

    #[test]
    fn ssrf_guard_blocks_link_local_but_allows_normal_hosts() {
        // Cloud metadata endpoint and other link-local addresses are blocked.
        assert!(is_blocked_http_host(
            "http://169.254.169.254/latest/meta-data/"
        ));
        assert!(is_blocked_http_host("http://169.254.0.1/"));
        assert!(is_blocked_http_host("http://[fe80::1]/"));
        // Relative/garbage URLs are refused rather than guessed at.
        assert!(is_blocked_http_host("/relative/path"));
        // Public hosts, loopback, and private ranges (valid internal APIs) pass.
        assert!(!is_blocked_http_host("https://api.example.com/v1"));
        assert!(!is_blocked_http_host("http://127.0.0.1:8080/"));
        assert!(!is_blocked_http_host("http://10.0.0.5/internal"));
    }

    /// Build a guest that GETs `url` and returns the packed (ptr,len) of the body.
    fn http_get_wat(url: &str) -> String {
        format!(
            r#"
            (module
              (import "paganel" "http_request"
                (func $http (param i32 i32 i32 i32 i32) (result i64)))
              (memory (export "memory") 1)
              (global $bump (mut i32) (i32.const 2048))
              (func (export "__paganel_alloc") (param $len i32) (result i32)
                (local $p i32)
                (local.set $p (global.get $bump))
                (global.set $bump (i32.add (global.get $bump) (local.get $len)))
                (local.get $p))
              (data (i32.const 16) "{url}")
              (func (export "run") (result i64)
                (call $http (i32.const 0) (i32.const 16) (i32.const {len})
                            (i32.const 0) (i32.const 0))))
            "#,
            url = url,
            len = url.len(),
        )
    }

    fn http_caps(http: bool, allow: &[&str]) -> HostCapabilities {
        HostCapabilities {
            http_client: http,
            http_allowed_hosts: allow.iter().map(|s| s.to_string()).collect(),
            ..HostCapabilities::default()
        }
    }

    /// Spawn a one-shot loopback HTTP/1.1 server returning `response` verbatim,
    /// and return its address plus the join handle.
    fn serve_once(response: &'static [u8]) -> (std::net::SocketAddr, std::thread::JoinHandle<()>) {
        use std::io::{Read, Write};
        use std::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        let handle = std::thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buf = [0u8; 1024];
                let _ = stream.read(&mut buf); // consume the request
                let _ = stream.write_all(response);
                let _ = stream.flush();
            }
        });
        (addr, handle)
    }

    /// Run an `http_get_wat` guest against `caps` and decode the framed reply
    /// (`[status u16 LE][body]`), or `None` if the host returned the 0 sentinel.
    fn run_http(url: &str, caps: HostCapabilities) -> Option<(u16, Vec<u8>)> {
        let wat = http_get_wat(url);
        let (mut store, instance) = instantiate(&wat, caps);
        let run = instance
            .get_typed_func::<(), i64>(&mut store, "run")
            .expect("run export");
        let packed = run.call(&mut store, ()).expect("call run") as u64;
        if packed == 0 {
            return None;
        }
        let ptr = (packed >> 32) as u32;
        let len = (packed & 0xFFFF_FFFF) as u32;
        let memory = instance.get_memory(&mut store, "memory").expect("memory");
        let mut raw = vec![0u8; len as usize];
        memory
            .read(&store, ptr as usize, &mut raw)
            .expect("read framed");
        assert!(
            raw.len() >= 2,
            "framed reply must carry a 2-byte status prefix"
        );
        let status = u16::from_le_bytes([raw[0], raw[1]]);
        Some((status, raw[2..].to_vec()))
    }

    #[test]
    fn http_returns_zero_when_capability_denied() {
        let out = run_http("http://127.0.0.1:1/", http_caps(false, &[]));
        assert!(out.is_none(), "denied http must return the 0 sentinel");
    }

    #[test]
    fn http_blocks_link_local_even_when_capability_granted() {
        // Capability granted, but the SSRF guard must refuse the metadata IP
        // before any network call is attempted.
        let out = run_http(
            "http://169.254.169.254/latest/meta-data/",
            http_caps(true, &[]),
        );
        assert!(out.is_none(), "link-local must be blocked");
    }

    #[test]
    fn http_get_round_trips_status_and_body() {
        let (addr, server) =
            serve_once(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello");
        let out = run_http(&format!("http://{addr}/"), http_caps(true, &[]));
        server.join().ok();

        let (status, body) = out.expect("expected a response");
        assert_eq!(status, 200, "status should round-trip");
        assert_eq!(body, b"hello", "body should round-trip into guest memory");
    }

    #[test]
    fn http_passes_through_non_success_status_and_body() {
        let (addr, server) = serve_once(
            b"HTTP/1.1 404 Not Found\r\nContent-Length: 9\r\nConnection: close\r\n\r\nnot found",
        );
        let out = run_http(&format!("http://{addr}/"), http_caps(true, &[]));
        server.join().ok();

        let (status, body) = out.expect("non-2xx should still return a response");
        assert_eq!(status, 404, "real error status must reach the plugin");
        assert_eq!(body, b"not found");
    }

    #[test]
    fn http_allowlist_permits_listed_host() {
        let (addr, server) =
            serve_once(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok");
        // Allowlist the loopback host explicitly.
        let out = run_http(&format!("http://{addr}/"), http_caps(true, &["127.0.0.1"]));
        server.join().ok();
        assert_eq!(out.expect("allowed").0, 200);
    }

    #[test]
    fn http_allowlist_blocks_unlisted_host() {
        // Host not in the allowlist: refused before any connection is attempted,
        // so no server is needed.
        let out = run_http("http://127.0.0.1:9/", http_caps(true, &["example.com"]));
        assert!(out.is_none(), "unlisted host must be blocked");
    }
}
