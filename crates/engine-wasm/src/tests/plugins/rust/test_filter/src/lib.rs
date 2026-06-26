use stratum_plugin_sdk::{FilterDecision, PluginInput, PluginResult, stratum_filter};

#[stratum_filter(
    name = "test_filter",
    version = "1.0.0",
    input = [
        { name = "value", type = "i64", nullable = false },
    ]
)]
fn positive_only(input: PluginInput) -> PluginResult<FilterDecision> {
    let value = input.get_i64("value")?;
    if value > 0 {
        Ok(FilterDecision::pass())
    } else {
        Ok(FilterDecision::reject("value must be positive"))
    }
}
