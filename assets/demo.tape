Output assets/demo.gif

Set Shell bash
Set FontFamily "Adwaita Mono"
Set FontSize 18
Set Padding 25
Set Width 1460
Set Height 600
Set Theme { "background": "#161719", "foreground": "#c5c8c6", "black": "#6e747e", "red": "#fd5ff1", "green": "#87c38a", "yellow": "#ffd7b1", "blue": "#85befd", "magenta": "#b9b6fc", "cyan": "#85befd", "white": "#e0e0e0", "brightBlack": "#6e747e", "brightRed": "#fd5ff1", "brightGreen": "#94fa36", "brightYellow": "#f5ffa8", "brightBlue": "#96cbfe", "brightMagenta": "#b9b6fc", "brightCyan": "#85befd", "brightWhite": "#e0e0e0", "selection": "#444444", "cursor": "#d0d0d0" }
Set TypingSpeed 45ms

# --- hidden setup: clean prompt, put `pag` on PATH, reset dest + state ---
Hide
Type `export PS1="$ "`
Enter
Type `export PATH="$PWD/target/release:$PATH"`
Enter
Type "docker compose exec -T postgres psql -U user -d testdb -c 'DROP TABLE IF EXISTS actor, payment CASCADE' >/dev/null 2>&1; rm -rf ~/.paganel; clear"
Enter
Sleep 3s
Show

# --- 1. migrate Sakila actor + payment, writing integrity receipts ---
Type "pag apply --pretty -c examples/configs/merkle.ppl -e .env --integrity"
Enter
Sleep 4s

# --- 2. verify: destination matches the receipts ---
Type "pag verify --pretty -c examples/configs/merkle.ppl -e .env"
Enter
Sleep 3s

# --- 3. tamper: change one non-key column of one destination row ---
Type `docker compose exec -T postgres psql -U user -d testdb -c "UPDATE actor SET last_name = 'TAMPERED' WHERE actor_id = 5"`
Enter
Sleep 2s

# --- 4. verify again: fails and names the exact divergent row ---
Type "pag verify --pretty -c examples/configs/merkle.ppl -e .env"
Enter
Sleep 6s
