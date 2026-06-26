use crate::abi::{RUNTIME, pack};
use crate::bootstrap;
use rquickjs::Function;

pub fn call_role(role: &str, ptr: u32, len: u32) -> u64 {
    let rt = match RUNTIME.get() {
        Some(cell) => &cell.0,
        None => return bootstrap::error_envelope("runtime not initialized"),
    };

    let input_bytes = unsafe { bootstrap::read_from_guest(ptr, len) };
    let input_str = match std::str::from_utf8(&input_bytes) {
        Ok(s) => s,
        Err(_) => return bootstrap::error_envelope("input not utf-8"),
    };

    let result = rt.context.with(|ctx| -> Result<String, String> {
        let dispatch: Function = rt
            .dispatch_fn
            .clone()
            .restore(&ctx)
            .map_err(|e| e.to_string())?;
        let s: String = dispatch
            .call((role, input_str))
            .map_err(|e| format!("js: {e}"))?;
        Ok(s)
    });

    match result {
        Ok(s) => {
            let (p, l) = unsafe { bootstrap::write_to_guest(s.as_bytes()) };
            pack(p, l)
        }
        Err(msg) => bootstrap::error_envelope(&msg),
    }
}

pub fn call_lifecycle(hook: &str, ptr: u32, len: u32) -> u32 {
    // Sink prepare/finalize. Invoke the SDK's __stratum_lifecycle, which no-ops
    // when the plugin didn't register that hook. 0 = ok, 1 = error.
    let rt = match RUNTIME.get() {
        Some(cell) => &cell.0,
        None => return 1,
    };

    let input_bytes = unsafe { bootstrap::read_from_guest(ptr, len) };
    let input_str = String::from_utf8_lossy(&input_bytes).into_owned();

    let result = rt.context.with(|ctx| -> Result<(), String> {
        let lifecycle: Function = match ctx.globals().get("__stratum_lifecycle") {
            Ok(f) => f,
            Err(_) => return Ok(()),
        };
        let _: String = lifecycle
            .call((hook, input_str.as_str()))
            .map_err(|e| format!("js: {e}"))?;
        Ok(())
    });

    match result {
        Ok(()) => 0,
        Err(_) => 1,
    }
}
