use paganel_plugin_sdk::{PluginInput, PluginResult, paganel_transform};

#[paganel_transform(
    name = "test_transform",
    version = "1.0.0",
    output = "f64",
    input = [
        { name = "a", type = "f64", nullable = false },
        { name = "b", type = "f64", nullable = false },
    ]
)]
fn add(inputs: Vec<PluginInput>) -> PluginResult<Vec<f64>> {
    inputs
        .iter()
        .map(|input| {
            let a = input.get_f64("a")?;
            let b = input.get_f64("b")?;
            Ok(a + b)
        })
        .collect()
}
