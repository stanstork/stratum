#!/usr/bin/env bash
# Regenerate assets/plan.png from the REAL output of `pag plan`.

set -euo pipefail

cd "$(dirname "$0")/.."                   # repo root
CONFIG="examples/configs/schema.ppl"      # rich DAG: renames, excludes, joins, estimates
FONT="Adwaita Mono"
ANSI="$(mktemp)"; TRIM="$(mktemp)"; CAT="$(mktemp)"; SVG="$(mktemp --suffix=.svg)"
trap 'rm -f "$ANSI" "$TRIM" "$CAT" "$SVG"' EXIT

# 1. Run `pag plan` in a real PTY (tmux) and capture the SETTLED screen: the
#    progress spinner has cleared - exactly what a human sees - with color kept.
tmux kill-session -t pagplan 2>/dev/null || true
tmux new-session -d -s pagplan -x 130 -y 50
tmux send-keys -t pagplan 'export PATH="$PWD/target/release:$PATH"; clear' Enter
sleep 1
tmux send-keys -t pagplan "pag plan -c $CONFIG -e .env; echo __PLAN_DONE__" Enter
for _ in $(seq 1 30); do
    tmux capture-pane -t pagplan -p | grep -q __PLAN_DONE__ && break
    sleep 1
done
tmux capture-pane -t pagplan -e -p > "$ANSI"
tmux kill-session -t pagplan 2>/dev/null || true

# 2. Keep only the plan output. The command-echo line contains "echo", the
#    output header ("pag plan · <config>") does not - so start capture there.
awk '/pag plan/ && !/echo/ {f=1} f {print} /Ready to apply/ {exit}' "$ANSI" > "$TRIM"

# 3. Re-theme to VHS's "Atom" palette so this screenshot matches the demo GIF
#    (rendered by VHS with `Set Theme "Atom"`). Palette-only remap - the text is
#    untouched, exactly as an Atom-themed terminal would render it. Hex values
#    are VHS's Atom theme (themes.json): bg #161719, fg #c5c8c6, red #fd5ff1,
#    green #87c38a, cyan #85befd.
printf '\033[38;2;197;200;198m' > "$CAT"
perl -pe '
    s/\x1b\[36m/\x1b[38;2;133;190;253m/g;               # cyan  -> #85befd
    s/\x1b\[32m/\x1b[38;2;135;195;138m/g;               # green -> #87c38a
    s/\x1b\[31m/\x1b[38;2;253;95;241m/g;                # red   -> #fd5ff1
    s/\x1b\[39m/\x1b[38;2;197;200;198m/g;               # default fg -> #c5c8c6
    s/\x1b\[0;1m/\x1b[0m\x1b[38;2;197;200;198m\x1b[1m/g;
    s/\x1b\[0m/\x1b[0m\x1b[38;2;197;200;198m/g;          # restore fg after each reset
' "$TRIM" >> "$CAT"

# 4. freeze -> styled SVG (references Adwaita Mono) -> rasterize to PNG.
freeze --execute "cat $CAT" --font.family "$FONT" --background "#161719" -o "$SVG"
magick -density 200 -background none "$SVG" assets/plan.png

echo "wrote assets/plan.png ($(magick identify -format '%wx%h' assets/plan.png))"
