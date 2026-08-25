use super::{PluginState, link_err, read_guest_string};
use crate::error::WasmError;
use tracing::info;
use wasmtime::{Caller, Linker};

pub(super) fn link(linker: &mut Linker<PluginState>) -> Result<(), WasmError> {
    linker
        .func_wrap(
            "stratum",
            "metric_counter",
            |mut caller: Caller<'_, PluginState>, name_ptr: u32, name_len: u32, value: i64| {
                if !caller.data().capabilities.metrics {
                    return;
                }

                if let Some(name) = read_guest_string(&mut caller, name_ptr, name_len) {
                    let plugin = caller.data().plugin_name.clone();
                    info!(target: "plugin::metrics", plugin = %plugin, metric = %name, kind = "counter", value, "plugin metric");
                }
            },
        )
        .map_err(|e| link_err("metric_counter", e))?;

    linker
        .func_wrap(
            "stratum",
            "metric_gauge",
            |mut caller: Caller<'_, PluginState>, name_ptr: u32, name_len: u32, value: f64| {
                if !caller.data().capabilities.metrics {
                    return;
                }

                if let Some(name) = read_guest_string(&mut caller, name_ptr, name_len) {
                    let plugin = caller.data().plugin_name.clone();
                    info!(target: "plugin::metrics", plugin = %plugin, metric = %name, kind = "gauge", value, "plugin metric");
                }
            },
        )
        .map_err(|e| link_err("metric_gauge", e))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::runtime::host_functions::test_harness::{caps, instantiate};

    const METRIC_WAT: &str = r#"
        (module
          (import "stratum" "metric_counter" (func $mc (param i32 i32 i64)))
          (memory (export "memory") 1)
          (data (i32.const 16) "rows")
          (func (export "run")
            (call $mc (i32.const 16) (i32.const 4) (i64.const 5))))
    "#;

    #[test]
    fn metric_counter_is_inert_but_traps_free_regardless_of_capability() {
        for granted in [true, false] {
            let (mut store, instance) = instantiate(METRIC_WAT, caps(false, granted));
            let run = instance
                .get_typed_func::<(), ()>(&mut store, "run")
                .expect("run export");
            run.call(&mut store, ())
                .expect("metric call must never trap");
        }
    }
}
