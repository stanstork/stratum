use crate::{commands::PluginCmd, error::CliError};
use engine_processing::EnvContext;
use std::sync::Arc;
use stratum_plugin_compiler::{CompileOpts, compile_to_file};

mod inspect;
mod shared;
mod test;
mod validate;

pub async fn run(cmd: &PluginCmd, env: Arc<EnvContext>) -> Result<(), CliError> {
    match cmd {
        PluginCmd::Compile {
            input,
            output,
            minify,
            esbuild_path,
            runtime_wasm,
        } => {
            let opts = CompileOpts {
                minify: *minify,
                esbuild_path: esbuild_path.clone(),
                runtime_wasm: runtime_wasm.clone(),
                ..Default::default()
            };
            let compiled = compile_to_file(input, output, &opts)?;
            println!(
                "compiled {} ({}, {} bytes)",
                output.display(),
                compiled.role,
                compiled.wasm.len()
            );
            Ok(())
        }
        PluginCmd::Inspect { path, json } => inspect::run(path, *json),
        PluginCmd::Validate { config, json } => validate::run(config.clone(), *json, env).await,
        PluginCmd::Test {
            path,
            mode,
            input,
            cursor,
            config_json,
            json,
        } => test::run(
            path,
            mode.as_deref(),
            input.as_deref(),
            cursor.as_deref(),
            config_json.as_deref(),
            *json,
        ),
    }
}
