use stratum_plugin_sdk::{PluginInput, PluginResult, stratum_transform};

/// Burns CPU forever so the host's fuel limit trips. Used by
/// `test_fuel_exhaustion` to verify the runtime kills runaway plugins.
#[stratum_transform(
    name = "test_infinite_loop",
    version = "1.0.0",
    output = "f64",
    input = []
)]
fn spin(_input: PluginInput) -> PluginResult<f64> {
    loop {
        std::hint::spin_loop();
    }
}
