use paganel_plugin_sdk::{PluginInput, PluginResult, paganel_transform};

/// Burns CPU forever so the host's fuel limit trips. Used by
/// `test_fuel_exhaustion` to verify the runtime kills runaway plugins.
#[paganel_transform(
    name = "test_infinite_loop",
    version = "1.0.0",
    output = "f64",
    input = []
)]
fn spin(_inputs: Vec<PluginInput>) -> PluginResult<Vec<f64>> {
    loop {
        std::hint::spin_loop();
    }
}
