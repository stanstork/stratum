#!/usr/bin/env bash
# crates/engine-wasm/src/tests/build_fixtures.sh
# Compiles the Rust and JavaScript test plugins to WASM. Run once before tests.
#
#   ./build_fixtures.sh           # build both Rust and JS fixtures
#   ./build_fixtures.sh rust      # build only the Rust fixtures
#   ./build_fixtures.sh js        # build only the JS fixtures
#
# The JS branch shells out to `npx esbuild` (downloaded on demand) and to the
# `stratum plugin compile` CLI, which patches each bundle into a pre-built JS
# runtime WASM.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"
FIXTURES_DIR="$SCRIPT_DIR/fixtures"
RUST_PLUGINS_DIR="$SCRIPT_DIR/plugins/rust"
JS_PLUGINS_DIR="$SCRIPT_DIR/plugins/js"
JS_SDK_DIR="$REPO_ROOT/crates/sdk/stratum-plugin-sdk-js"
RUNTIME_WASM="$FIXTURES_DIR/stratum-plugin-js-runtime.wasm"

TARGET="${1:-all}"

mkdir -p "$FIXTURES_DIR"

build_rust() {
    echo "==> Building Rust plugin fixtures"
    for plugin_dir in "$RUST_PLUGINS_DIR"/test_*; do
        plugin_name=$(basename "$plugin_dir")
        echo "Building $plugin_name..."
        (cd "$plugin_dir" && cargo build --target wasm32-wasip1 --release)
        cp "$RUST_PLUGINS_DIR/target/wasm32-wasip1/release/${plugin_name}.wasm" "$FIXTURES_DIR/"
    done
}

build_js() {
    echo "==> Building JS plugin fixtures"

    command -v npx >/dev/null 2>&1 || {
        echo "error: npx not found on PATH (install Node.js to build JS fixtures)" >&2
        exit 1
    }

    # 1. Pre-build the JS runtime WASM the compiler patches into. The crate is a
    #    cdylib, so the artifact is stratum_plugin_js_runtime.wasm. It is excluded
    #    from the root workspace (wasm-only, see Cargo.toml), so build it by its
    #    manifest path; its artifacts land in the crate's own target dir.
    echo "Building JS runtime (stratum-plugin-js-runtime)..."
    JS_RUNTIME_DIR="$REPO_ROOT/crates/sdk/stratum-plugin-js-runtime"
    cargo build --manifest-path "$JS_RUNTIME_DIR/Cargo.toml" --target wasm32-wasip1 --release
    mkdir -p "$(dirname "$RUNTIME_WASM")"
    cp "$JS_RUNTIME_DIR/target/wasm32-wasip1/release/stratum_plugin_js_runtime.wasm" "$RUNTIME_WASM"

    # 2. Make `@stratum/plugin-sdk` resolvable by esbuild. esbuild walks up from
    #    the plugin file looking for node_modules, so symlink the SDK package in
    #    next to the plugins.
    mkdir -p "$JS_PLUGINS_DIR/node_modules/@stratum"
    ln -sfn "$JS_SDK_DIR" "$JS_PLUGINS_DIR/node_modules/@stratum/plugin-sdk"

    # 3. A thin wrapper so the compiler can invoke esbuild via npx (single
    #    binary path; npx --yes fetches esbuild on first use).
    ESBUILD_WRAPPER="$SCRIPT_DIR/.esbuild-npx.sh"
    cat > "$ESBUILD_WRAPPER" <<'EOF'
#!/usr/bin/env bash
exec npx --yes esbuild "$@"
EOF
    chmod +x "$ESBUILD_WRAPPER"

    # 4. Compile each plugin into its own patched runtime copy. Fixtures are
    #    suffixed _js so they sit beside the Rust fixtures without colliding.
    for js in "$JS_PLUGINS_DIR"/test_*.js; do
        name=$(basename "$js" .js)
        echo "Compiling ${name} (js)..."
        cargo run -p cli --release -- plugin compile "$js" \
            -o "$FIXTURES_DIR/${name}_js.wasm" \
            --esbuild-path "$ESBUILD_WRAPPER" \
            --runtime-wasm "$RUNTIME_WASM"
    done
}

case "$TARGET" in
    rust) build_rust ;;
    js)   build_js ;;
    all)  build_rust; build_js ;;
    *)
        echo "usage: $0 [rust|js|all]" >&2
        exit 1
        ;;
esac

echo "All requested test fixtures built in $FIXTURES_DIR/"
