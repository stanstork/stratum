use stratum_plugin_sdk::{PluginInput, PluginResult, Value, stratum_transform};

#[stratum_transform(
    name = "test_echo",
    version = "1.0.0",
    output = "json",
    input = [
        { name = "x", type = "json", nullable = true },
    ]
)]
fn echo(inputs: Vec<PluginInput>) -> PluginResult<Vec<Value>> {
    Ok(inputs
        .iter()
        .map(|input| input.get("x").cloned().unwrap_or(Value::Null))
        .collect())
}
