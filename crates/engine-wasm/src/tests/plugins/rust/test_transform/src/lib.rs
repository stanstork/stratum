use stratum_plugin_sdk::{PluginInput, PluginResult, stratum_transform};

#[stratum_transform(
    name = "test_transform",
    version = "1.0.0",
    output = "f64",
    input = [
        { name = "a", type = "f64", nullable = false },
        { name = "b", type = "f64", nullable = false },
    ]
)]
fn add(input: PluginInput) -> PluginResult<f64> {
    let a = input.get_f64("a")?;
    let b = input.get_f64("b")?;
    Ok(a + b)
}
