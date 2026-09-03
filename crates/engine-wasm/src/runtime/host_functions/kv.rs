use super::{PluginState, link_err, read_guest_bytes, write_guest_bytes};
use crate::error::WasmError;
use wasmtime::{Caller, Linker};

pub(super) fn link(linker: &mut Linker<PluginState>) -> Result<(), WasmError> {
    linker
        .func_wrap(
            "paganel",
            "kv_get",
            |mut caller: Caller<'_, PluginState>, key_ptr: u32, key_len: u32| -> u64 {
                if !caller.data().capabilities.key_value_store {
                    return 0;
                }

                let Some(key) = read_guest_bytes(&mut caller, key_ptr, key_len) else {
                    return 0;
                };
                let Some(value) = caller.data().kv.get(&key).cloned() else {
                    return 0;
                };

                write_guest_bytes(&mut caller, &value).unwrap_or(0)
            },
        )
        .map_err(|e| link_err("kv_get", e))?;

    linker
        .func_wrap(
            "paganel",
            "kv_set",
            |mut caller: Caller<'_, PluginState>,
             key_ptr: u32,
             key_len: u32,
             val_ptr: u32,
             val_len: u32| {
                if !caller.data().capabilities.key_value_store {
                    return;
                }

                let Some(key) = read_guest_bytes(&mut caller, key_ptr, key_len) else {
                    return;
                };
                let Some(value) = read_guest_bytes(&mut caller, val_ptr, val_len) else {
                    return;
                };

                caller.data_mut().kv.insert(key, value);
            },
        )
        .map_err(|e| link_err("kv_set", e))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::runtime::host_functions::test_harness::{caps, instantiate};

    /// A guest that exports a bump allocator + memory, sets `k`=`vv`, then reads
    /// `k` back. `run` returns the packed (ptr,len) that `kv_get` handed back.
    const KV_WAT: &str = r#"
        (module
          (import "paganel" "kv_set" (func $kv_set (param i32 i32 i32 i32)))
          (import "paganel" "kv_get" (func $kv_get (param i32 i32) (result i64)))
          (memory (export "memory") 1)
          (global $bump (mut i32) (i32.const 1024))
          (func (export "__paganel_alloc") (param $len i32) (result i32)
            (local $p i32)
            (local.set $p (global.get $bump))
            (global.set $bump (i32.add (global.get $bump) (local.get $len)))
            (local.get $p))
          (data (i32.const 16) "k")
          (data (i32.const 32) "vv")
          (func (export "run") (result i64)
            (call $kv_set (i32.const 16) (i32.const 1) (i32.const 32) (i32.const 2))
            (call $kv_get (i32.const 16) (i32.const 1))))
    "#;

    #[test]
    fn kv_round_trips_a_value_through_guest_memory() {
        let (mut store, instance) = instantiate(KV_WAT, caps(true, false));
        let run = instance
            .get_typed_func::<(), i64>(&mut store, "run")
            .expect("run export");
        let packed = run.call(&mut store, ()).expect("call run") as u64;

        let ptr = (packed >> 32) as u32;
        let len = (packed & 0xFFFF_FFFF) as u32;
        assert_eq!(len, 2, "value length should round-trip");

        let memory = instance.get_memory(&mut store, "memory").expect("memory");
        let mut buf = [0u8; 2];
        memory
            .read(&store, ptr as usize, &mut buf)
            .expect("read value");
        assert_eq!(&buf, b"vv", "value bytes should round-trip");
    }

    #[test]
    fn kv_get_returns_zero_when_capability_denied() {
        let (mut store, instance) = instantiate(KV_WAT, caps(false, false));
        let run = instance
            .get_typed_func::<(), i64>(&mut store, "run")
            .expect("run export");
        let packed = run.call(&mut store, ()).expect("call run");
        assert_eq!(packed, 0, "denied kv must return the 0 sentinel");
    }

    #[test]
    fn kv_get_returns_zero_for_absent_key() {
        // Set nothing; get a key that was never stored.
        let wat = r#"
            (module
              (import "paganel" "kv_get" (func $kv_get (param i32 i32) (result i64)))
              (memory (export "memory") 1)
              (func (export "__paganel_alloc") (param i32) (result i32) (i32.const 1024))
              (data (i32.const 16) "missing")
              (func (export "run") (result i64)
                (call $kv_get (i32.const 16) (i32.const 7))))
        "#;
        let (mut store, instance) = instantiate(wat, caps(true, false));
        let run = instance
            .get_typed_func::<(), i64>(&mut store, "run")
            .expect("run export");
        assert_eq!(run.call(&mut store, ()).expect("call run"), 0);
    }
}
