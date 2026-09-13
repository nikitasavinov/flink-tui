# Run from the repository root after `docker compose up --build -d` and `make build`.
# Record with: vhs scripts/demo.tape
Output "docs/demo.gif"

Set Shell "bash"
Set Width 1600
Set Height 1000
Set FontSize 18
Set FontFamily "Menlo"
Set Theme "Catppuccin Mocha"
Set Padding 16
Set Margin 16
Set MarginFill "#0B1020"
Set BorderRadius 10
Set WindowBar Colorful
Set Framerate 15
Set TypingSpeed 65ms
Set CursorBlink false
Set WaitTimeout 30s

# Keep terminal colors enabled when recording from an environment using NO_COLOR.
Env NO_COLOR ""
Env COLORTERM "truecolor"
Env COLORFGBG "15;0"

Hide
Type@1ms "./bin/flink-tui --endpoint http://localhost:8081 --sql-endpoint '' --refresh 2s"
Enter
Wait+Screen /Flink TUI Demo/
Sleep 1s
Show

# 1. Cluster overview: every running job at a glance.
Sleep 2500ms

# 2. Open the demo job and zoom into its branching graph.
Type "/Flink TUI Demo"
Sleep 300ms
Enter
Wait+Screen /parallelism x/
Sleep 2s
Type "+"
Sleep 1800ms
Screenshot "docs/demo.png"

# 3. Deep dive: checkpoint history, then per-operator statistics.
Ctrl+n
Type "cp"
Wait+Screen /CHECKPOINTS/
Sleep 2400ms
Down
Enter
Wait+Screen /OPERATORS/
Sleep 2400ms

# 4. Deep dive: JobManager JVM health and cluster configuration.
Ctrl+p
Wait+Screen /COMMAND PALETTE/
Type "jm"
Sleep 400ms
Enter
Wait+Screen /JOB MANAGER/
Sleep 3s

# Close on the cluster overview with the job filter cleared.
Type "1"
Type "/"
Ctrl+w
Escape
Sleep 2300ms
Hide
Ctrl+c
