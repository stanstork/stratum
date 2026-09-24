use paganel_plugin_sdk::{FilterDecision, PluginInput, PluginResult, paganel_filter};

#[paganel_filter(
    name = "test_filter",
    version = "1.0.0",
    input = [
        { name = "value", type = "i64", nullable = false },
    ]
)]
fn positive_only(inputs: Vec<PluginInput>) -> PluginResult<Vec<FilterDecision>> {
    inputs
        .iter()
        .map(|input| {
            let value = input.get_i64("value")?;
            Ok(if value > 0 {
                FilterDecision::pass()
            } else {
                FilterDecision::reject("value must be positive")
            })
        })
        .collect()
}
