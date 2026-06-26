use crate::error::{PluginError, PluginResult};

/// HTTP method codes matching the host ABI.
#[repr(u32)]
pub enum HttpMethod {
    Get = 0,
    Post = 1,
    Put = 2,
    Delete = 3,
    Patch = 4,
}

pub struct HttpResponse {
    pub status: u32,
    pub body: Vec<u8>,
}

#[cfg(target_arch = "wasm32")]
#[link(wasm_import_module = "stratum")]
unsafe extern "C" {
    // (method, url_ptr, url_len, body_ptr, body_len) -> packed (status_hi16 | body_ptr_hi | body_len_lo)
    fn http_request(method: u32, url_ptr: u32, url_len: u32, body_ptr: u32, body_len: u32) -> u64;
}

#[cfg(not(target_arch = "wasm32"))]
unsafe fn http_request(_m: u32, _up: u32, _ul: u32, _bp: u32, _bl: u32) -> u64 {
    0
}

pub fn http_get(url: &str) -> PluginResult<HttpResponse> {
    request(HttpMethod::Get, url, &[])
}

pub fn http_post(url: &str, body: &[u8]) -> PluginResult<HttpResponse> {
    request(HttpMethod::Post, url, body)
}

fn request(method: HttpMethod, url: &str, body: &[u8]) -> PluginResult<HttpResponse> {
    let url_bytes = url.as_bytes();
    let packed = unsafe {
        http_request(
            method as u32,
            url_bytes.as_ptr() as u32,
            url_bytes.len() as u32,
            body.as_ptr() as u32,
            body.len() as u32,
        )
    };

    if packed == 0 {
        return Err(PluginError::capability_denied("http_client"));
    }

    // Decode packed value. Wire format defined by host.
    let (resp_ptr, resp_len) = crate::runtime::pack::unpack(packed);
    let body = unsafe { crate::runtime::abi::read_from_guest(resp_ptr, resp_len) };
    Ok(HttpResponse { status: 200, body })
}
