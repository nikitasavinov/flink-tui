# User guide

The full reference for starting Flink TUI, navigating the cluster, and using
each screen. For installation and a local demo, start with the
[README](../README.md#get-started).

- [Command-line options](#command-line-options)
- [Authentication and TLS](#authentication-and-tls)
- [Navigation and global keys](#navigation-and-global-keys)
- [Command palette and sidebar](#command-palette-and-sidebar)
- [Jobs and operators](#jobs-and-operators)
- [Checkpoints](#checkpoints)
- [Exceptions](#exceptions)
- [TaskManagers and JobManager](#taskmanagers-and-jobmanager)
- [Logs, threads, and profiling](#logs-threads-and-profiling)
- [Job actions](#job-actions)
- [SQL workbench](#sql-workbench)
- [Investigation walkthroughs](#investigation-walkthroughs)
- [Troubleshooting and limits](#troubleshooting-and-limits)
- [Development commands](#development-commands)

Keys are case-sensitive: `F` means `Shift+f`, while `f` is lowercase.
`Ctrl+P` means holding Control and pressing P. Palette aliases such as
`cp` are typed inside the TUI; they are not shell subcommands.

## Command-line options

Run `flink-tui` once it is installed, or `flink-tui` from the repository
root after `make build`. See [Get started](../README.md#get-started) for both
paths. Flink TUI takes flags, with no subcommands or positional arguments.

Connect to a cluster without SQL:

```sh
flink-tui --endpoint http://localhost:8081 --sql-endpoint ''
```

Connect to both the JobManager and SQL Gateway:

```sh
flink-tui \
  --endpoint http://localhost:8081 \
  --sql-endpoint http://localhost:8083
```

Open a particular job and refresh once per second. Replace the example Job ID
with one from your cluster:

```sh
flink-tui \
  --endpoint http://localhost:8081 \
  --sql-endpoint '' \
  --job 0123456789abcdef0123456789abcdef \
  --refresh 1s
```

| Flag | Default | Behavior |
| --- | --- | --- |
| `--endpoint URL` | `http://localhost:8081` | JobManager REST API |
| `--sql-endpoint URL` | `http://localhost:8083` | SQL Gateway; an empty string disables the workbench |
| `--job ID` | Empty | Open this job's graph instead of Cluster Overview |
| `--refresh DURATION` | `3s` | Periodic refresh interval; at least `250ms` |
| `--help` | — | Print all options and exit |

Durations use Go notation, such as `500ms`, `1s`, or `1m`.
There is no configuration file; connection settings come from these flags and
the authentication environment variables below.

### Why there are two endpoints

The JobManager REST API provides jobs, metrics, checkpoints, and process
diagnostics. The [SQL Gateway](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/interfaces/sql-gateway/overview/)
is a separate service with its own API for sessions, statements, and results.

Only the SQL workbench needs a Gateway. Its host, credentials, and TLS settings
are independent of the JobManager's. Configure the Gateway itself to submit
work to the intended cluster; setting `--endpoint` in Flink TUI does not
configure the Gateway.

Endpoints may include a reverse-proxy base path, such as
`https://flink.example.com/cluster-a`. Credentials, query strings, and
fragments in endpoint URLs are rejected. Use the authentication options below.

## Authentication and TLS

JobManager and SQL Gateway credentials are configured separately. SQL does not
inherit the JobManager's credentials.

| Setting | JobManager flag | SQL Gateway flag |
| --- | --- | --- |
| Basic auth username | `--username` | `--sql-username` |
| Password file | `--password-file` | `--sql-password-file` |
| Bearer token file | `--bearer-token-file` | `--sql-bearer-token-file` |
| PEM CA bundle | `--ca-cert` | `--sql-ca-cert` |
| mTLS client certificate | `--client-cert` | `--sql-client-cert` |
| mTLS client key | `--client-key` | `--sql-client-key` |
| Disable TLS verification | `--insecure-skip-verify` | `--sql-insecure-skip-verify` |

Each setting except the verification toggle also has an environment variable:

| Setting | JobManager environment | SQL Gateway environment |
| --- | --- | --- |
| Username | `FLINK_TUI_USERNAME` | `FLINK_TUI_SQL_USERNAME` |
| Password value | `FLINK_TUI_PASSWORD` | `FLINK_TUI_SQL_PASSWORD` |
| Bearer token value | `FLINK_TUI_BEARER_TOKEN` | `FLINK_TUI_SQL_BEARER_TOKEN` |
| CA bundle path | `FLINK_TUI_CA_CERT` | `FLINK_TUI_SQL_CA_CERT` |
| Client certificate path | `FLINK_TUI_CLIENT_CERT` | `FLINK_TUI_SQL_CLIENT_CERT` |
| Client key path | `FLINK_TUI_CLIENT_KEY` | `FLINK_TUI_SQL_CLIENT_KEY` |

Password and token environment variables hold the **secret itself**. Their
flags take **file paths** instead. A supplied file overrides the matching
environment variable; trailing line endings are removed from its contents.
The other flags override their matching environment defaults.

Basic authentication needs a username and cannot be combined with a bearer
token. Mutual TLS requires both a client certificate and key. TLS verification
is enabled by default; the insecure flags disable it and are unsafe for
normal use.

Basic authentication:

```sh
flink-tui \
  --endpoint https://flink.example.com \
  --username operator \
  --password-file /path/to/password \
  --sql-endpoint ''
```

A bearer token with a private CA:

```sh
flink-tui \
  --endpoint https://flink.example.com \
  --bearer-token-file /path/to/token \
  --ca-cert /path/to/ca.pem \
  --sql-endpoint ''
```

Mutual TLS:

```sh
flink-tui \
  --endpoint https://flink.example.com \
  --ca-cert /path/to/ca.pem \
  --client-cert /path/to/client.pem \
  --client-key /path/to/client-key.pem \
  --sql-endpoint ''
```

For a separately authenticated Gateway, add `--sql-endpoint` and its
`--sql-*` credentials to the same command.

## Navigation and global keys

These keys apply while navigating content. Search fields, the command palette,
SQL insert mode, and confirmation dialogs consume their own keys. Close the
current input or overlay with `Esc` before using normal navigation shortcuts.
`Ctrl+C` always quits.

| Key | Action |
| --- | --- |
| `Ctrl+P` or `:` | Open the command palette |
| `Ctrl+N` or `Ctrl+B` | Toggle focus between navigation and content |
| `Ctrl+G` | Show or hide the navigation sidebar |
| `Left` | Focus navigation on list and document screens; graphs use it for movement |
| `q` or `Esc` | Return along the navigation trail, after closing local input or zoom |
| `Backspace` | Move to the containing scope; flame graphs use it to zoom out |
| `1` | Return to Cluster Overview and park the current investigation |
| `Ctrl+O` | Resume the parked investigation, when one exists |
| `g` | Open the current job graph; on a profiler report, return to the Profiler |
| `<` / `>` | Inspect the previous or next comparable object in the same view |
| `r` | Refresh the current screen |
| `Space` | Pause or resume periodic refresh |
| `Ctrl+E` | Show full details of the current error |
| `m` | Toggle mouse capture |
| `?` | Open help for the current screen |
| `Ctrl+C` | Quit |

Prefer `Ctrl+N` if your terminal multiplexer uses `Ctrl+B`.
Navigation focus and visibility shortcuts also work while entering a filter
or editing SQL, but are locked during job-action confirmation.

### Going back, going up, and returning home

`q` and `Esc` retrace the route you took, including palette jumps.
For example, going from a subtask to a thread dump and back restores the
subtask view. At Cluster Overview, these keys stay on Overview;
use `Ctrl+C` to exit.

`Backspace` moves to the containing scope instead of retracing your route:
operator views lead back toward their job; process diagnostics lead back to
the owning TaskManager or JobManager. This clears the previous route trail.
In a flame graph, `Backspace` only zooms out.

`1` parks the current trail so you can glance at Overview and use
`Ctrl+O` to resume. Opening another destination from Overview starts a new
investigation and replaces the parked one.

### Comparing jobs, vertices, checkpoints, and processes

`<` and `>` change the object without adding another step to the back
history. What changes depends on the current screen:

| Current screen | Previous / next object |
| --- | --- |
| Job Graph, checkpoint history or summary, Timeline, Diagnostics, Exceptions, Job Configuration, Job Actions | Job in the current Overview tab and filter |
| Subtasks, Vertex Flame Graph, Metrics, Accumulators | Vertex in graph order |
| Checkpoint Operators or Checkpoint Subtasks | Checkpoint, keeping the selected operator where possible |
| Task Manager Detail | TaskManager |
| Process logs, stdout, thread dumps, Profiler | Process: loaded TaskManagers and the JobManager |
| Profile Flame Graph | Another process's Profiler screen |

Stepping to another vertex in Metrics clears the tracked metric selection.
Static documents, such as an accumulator value or an opened stack trace,
do not support process stepping.

### Filters and search

Most lists use `/` to start filtering. Type to narrow the rows, use
`Backspace` to edit, and `Esc` to leave the field **without clearing the
filter**. Most list filters support `Ctrl+W` to clear the query.

Metrics, document search, and thread-dump filters use `Backspace` instead
of `Ctrl+W`. These fields retain the query when reopened.

`Enter` both accepts the filter and opens the selected row on Overview,
Diagnostics, Timeline, checkpoint history and operators, Exceptions, and
Accumulators. On Subtasks, Metrics, configuration, and thread-dump filters,
it only leaves the field. In documents, it jumps to the next match.

### Mouse, help, and refresh

Mouse capture starts **off**, allowing normal terminal text selection and
copying. Press `m` to enable row selection, scrolling, and graph interaction.
Press it again to give mouse input back to the terminal.

In help, use arrows or `j` / `k`, `PgUp` / `PgDn`, and
`Home` / `End` to scroll; `?`, `q`, or `Esc` closes it.
Close error details with `Ctrl+E`, `e`, `q`, `Esc`, or `Backspace`.

Logs, documents, and thread dumps are snapshots: use `r` to reload them.
Pausing periodic refresh does not cancel SQL execution or an accepted
checkpoint, savepoint, stop, or cancel request.

## Command palette and sidebar

Press `Ctrl+P` or `:`, type a screen name, alias, or object name, select
with arrows or `Ctrl+J` / `Ctrl+K`, then press `Enter`.
`Backspace` edits the query; `Esc` closes the palette.
`q` is ordinary query text here.

The palette searches loaded jobs, vertices of the selected job, and loaded
TaskManagers. Job and vertex commands become available when their context
exists; SQL appears only when a Gateway is configured.

| Alias | Destination | Other search terms |
| --- | --- | --- |
| `ov` | Cluster Overview | `1` |
| `g` | Job Graph | — |
| `st` | Subtasks | — |
| `fl` | Vertex Flame Graph | `flame` |
| `cp` | Checkpoints | — |
| `tl` | Timeline | `timeline` |
| `dx` | Diagnostics | `backpressure`, `watermark` |
| `mx` | Metrics | — |
| `acc` | Accumulators | `accumulators` |
| `ex` | Exceptions | — |
| `cfg` | Job Configuration | — |
| `act` | Job Actions | `savepoint`, `cancel` |
| `tm` | Task Managers | — |
| `jm` | Job Manager | — |
| `sq` | SQL Workbench | `sql` |
| `lg` | Current process log, opened at the tail | `logs`, `tail` |
| `td` | Thread Dump | `threads`, `dump` |
| `pr` | Process Profiler | `profiler` |

Aliases are navigation commands, not direct job mutations.
For example, `savepoint` opens Job Actions so you can select and review an
action.

Combine a destination with an object to open a specific view directly:

| Palette query | Result |
| --- | --- |
| `cp Flink TUI Checkpoints` | Checkpoints for the playground's checkpoint job |
| `mx Decode Orders` | Metrics for a matching vertex in the selected job |
| `lg jobmanager` | Current JobManager log |
| `td jobmanager` | JobManager thread dump |
| `pr jobmanager` | JobManager Profiler |
| `det worker-id-fragment` | Details for a matching TaskManager; `tmd` also works |

Use names or ID fragments from your own cluster. Incompatible destination
and object combinations show why they cannot be opened.

The palette also includes **Open Data Skew Diagnostics**, **Refresh Now**,
**Pause Refresh / Resume Refresh**, **Focus / Unfocus Navigation**,
**Show / Hide Navigation**, **Enable / Disable Mouse Capture**, and, on a
job graph, **Toggle Graph Map**.

### Using the sidebar

Press `Ctrl+N` to focus navigation. Use arrows and `Home` / `End`
to select a destination, then `Enter` to open it. Alternatively, type its
visible alias: a completed alias opens immediately, without `Enter`.

Aliases take precedence over letter-based movement, so use arrows here;
`j` can begin the `jm` alias. `Right`, `Esc`, or `q` returns
focus to content.

Contextual branches also expose `ops` for Checkpoint Operators, `cst`
for Checkpoint Subtasks, `det` for Task Manager Detail, `doc` for the
open Document, and `pf` for Profile Flame Graph. These refer to the current
drill-down and are only available when that context exists.

On narrow terminals, focused navigation becomes a full-screen picker.
`Ctrl+G` controls persistent sidebar visibility independently of focus.

## Jobs and operators

For the tables below, “move rows” means `Up` / `Down` or `k` /
`j`; “page” means `PgUp` / `PgDn`.

### Cluster Overview

Overview lists jobs and cluster capacity.

| Key | Action |
| --- | --- |
| Move rows; page | Select a job |
| `Enter` | Open the selected job |
| `Tab` or `v` | Cycle Active, Completed, and All tabs |
| `Shift+Tab` | Cycle tabs backward |
| `/` | Filter by job name, full ID, state, or type |

The first opening of a job leads to its graph. When returning to a job,
the TUI can restore the last job view. The Overview tab and filter also
determine which jobs `<` / `>` compare.

### Job Graph

| Key | Action |
| --- | --- |
| `Left` / `h`; `Right` / `l` | Select an upstream or downstream vertex |
| `Up` / `k`; `Down` / `j` | Select a sibling or nearby vertex |
| `Tab` / `Shift+Tab` | Next / previous vertex in graph order |
| `Enter` | Open the selected vertex's subtasks |
| `F` | Open its live vertex flame graph |
| `v` | Open its Metrics explorer |
| `u` | Open its Accumulators |
| `+` or `=`; `-` or `_` | Zoom in / out |
| `f` | Fit the graph |
| `0` | Reset to the detailed zoom level |
| `z` | Toggle the minimap |
| `Shift` + arrow | Pan |
| `<` / `>` | Previous / next **job** |

Zoom levels are detailed (100%), compact (70%), and topology (40%).
A newly opened graph fits the viewport and selects its hottest vertex;
refreshing preserves subsequent manual pan and zoom.

With mouse capture enabled, click a vertex to select it and drag empty space
to pan. The wheel pans; `Shift` + wheel pans horizontally, and
`Ctrl` + wheel zooms at the pointer when the terminal forwards that modifier.
Click or drag in the minimap to move around the graph.

### Subtasks

Inspect each parallel subtask's state, worker, busy time, backpressure,
watermark, and I/O.

| Key | Action |
| --- | --- |
| Move rows; page | Select a subtask |
| `Enter` | Return to the Job Graph |
| `d` | Open the selected subtask's TaskManager thread dump |
| `s` | Open the sort editor |
| `v` | Toggle I/O rates and cumulative counts |
| `/` | Filter subtasks |
| `<` / `>` | Previous / next vertex |

The thread-dump jump tries to select the matching execution thread. If no
matching thread is available, it shows the full worker dump.

In the sort editor, use `Left` / `Right` or `h` / `l` to choose
a column. Set ascending order with `a`, `Up`, or `k`; set descending
order with `d`, `Down`, or `j`. Close with `Enter`, `Esc`, or
`s`; changes apply immediately.

| Sort shortcut | Column |
| --- | --- |
| `#` or `0` | Subtask index |
| `r` | State |
| `b` | Busy time |
| `p` | Backpressure |
| `i` / `o` | Input / output records |
| `I` / `O` | Input / output bytes |
| `w` | Watermark |
| `t` | TaskManager |

Idle time is available by cycling columns. With mouse capture enabled,
clicking a column header selects or reverses its sort.

### Diagnostics and Timeline

Diagnostics compares vertices across **Backpressure**, **Data Skew**, and
**Metrics** pages. Timeline shows vertex execution attempts and durations.

| Key | Diagnostics | Timeline |
| --- | --- | --- |
| Move rows; page | Select a vertex | Select a vertex |
| `Enter` | Open subtasks | Open subtasks |
| `g` | Open the selected vertex in the graph | Open the selected vertex in the graph |
| `/` | Filter vertices | Filter vertices |
| `[` / `]` | Change diagnostic page | — |
| `b` / `d` / `v` | Backpressure / Data Skew / Metrics | — |
| `<` / `>` | Previous / next job | Previous / next job |

### Metrics

Select exposed metrics and chart their values over time.

| Key | Action |
| --- | --- |
| Move rows; page; `Home` / `End` | Select a metric |
| `Enter` | Track or untrack the selected metric, up to four charts |
| `/` | Search metric names; `Enter` accepts the search |
| `s` | Switch between an aggregate and individual subtask series |
| `a` | Cycle SUM, AVG, MAX, MIN in aggregate mode |
| `[` / `]` | Change the history window: 1, 5, or 15 minutes |
| `x` | Untrack all metrics |
| `<` / `>` | Previous / next vertex; clears the tracked selection |

History accumulates while you track metrics in this view. Choosing a longer
window does not fetch historical data from a metrics database.

### Accumulators and Job Configuration

Accumulators shows user accumulator values for a vertex and its subtasks.
Job Configuration shows the job's execution and user configuration.

Both support row movement, paging, `Home` / `End`, and `/` filtering.
In Accumulators, `Enter` opens the full value in a document and `<` /
`>` changes vertex. In Job Configuration, `Enter` accepts the filter
and `<` / `>` changes job.

Configuration values with secret-looking keys are masked. Review any
configuration or log text before sharing it; masking is not a guarantee that
all sensitive values are recognized.

## Checkpoints

Open `cp` from the palette with a job selected.

### History and summary

| Key | Action |
| --- | --- |
| `Tab` or `v` | Switch History / Summary |
| Move rows; page | Select a checkpoint in History |
| `Home` / `End` | Newest / oldest retained checkpoint |
| `Enter` | Open the selected checkpoint's operators from History |
| `/` | Filter History |
| `<` / `>` | Previous / next job |

Summary shows aggregate statistics; History exposes individual attempts.
Only checkpoints retained by the cluster are available.

### Checkpoint Operators

| Key | Action |
| --- | --- |
| Move rows; page; `Home` / `End` | Select an operator |
| `Enter` | Open checkpoint subtasks for the selected operator |
| `s` | Cycle Diagnosis, Duration, State Size, Acknowledgement, Processed Data sorting |
| `i` | Show or hide effective checkpoint configuration |
| `g` | Open the selected operator in the job graph |
| `/` | Filter operators |
| `<` / `>` | Previous / next checkpoint |

### Checkpoint Subtasks

| Key | Action |
| --- | --- |
| Move rows; page; `Home` / `End` | Select a subtask |
| `Enter` | Return to Checkpoint Operators |
| `s` | Cycle Diagnosis, Duration, State Size, Alignment, Start Delay sorting |
| `g` | Open the operator in the job graph |
| `<` / `>` | Previous / next checkpoint |

This view has no text filter. Compare duration, alignment, start delay, and
state size across subtasks to locate the phase or worker contributing to a
slow checkpoint.

## Exceptions

Open `ex` with a job selected to inspect failure incidents and stack traces.

| Key | Action |
| --- | --- |
| Move rows; page; `Home` / `End` | Select an incident |
| `[` / `]` | Select a root or concurrent exception within the incident |
| `s` | Cycle recency, task, exception type, and recurrence sorting |
| `/` | Filter exception names, traces, tasks, locations, and labels |
| `Enter` or `g` | Open the affected vertex in the graph |
| `T` | Open the affected TaskManager |
| `d` | Open that TaskManager's thread dump |
| `l` | Open that TaskManager's current log at its tail |
| `L` or `+` | Request more retained incidents |
| `Ctrl+U` / `Ctrl+D` | Scroll the trace preview up / down |
| `<` / `>` | Previous / next job |

Process jumps need a worker identity in the exception. A current thread dump
may no longer contain the failed attempt's thread. Loading more incidents
cannot recover exceptions already expired from Flink's retention.

## TaskManagers and JobManager

Open `tm` for workers or `jm` for the JobManager.

### Task Managers

Move rows or page through the worker list; `Enter` opens worker details.
The detail page scrolls with arrows, `j` / `k`, page keys, and
`Home` / `End`.

These shortcuts work on the selected worker in either the list or detail page:

| Key | Action |
| --- | --- |
| `l` | Open the current log at its tail |
| `L` | Browse available log files |
| `x` | Open stdout |
| `d` | Open a thread dump |
| `p` | Open Process Profiler |
| `Tab` or `9` | Open Job Manager |

On Task Manager Detail, `<` / `>` steps between workers.

### Job Manager

The page includes JVM health and a configuration table. Move rows, page, or
use `Home` / `End` to navigate the table.

| Key | Action |
| --- | --- |
| `/` | Filter configuration |
| `L` | Switch configuration / log-file list |
| `Enter` | Open the selected file when the log-file list is visible |
| `l` | Open the current log at its tail |
| `x` | Open stdout |
| `d` | Open a thread dump |
| `p` | Open Process Profiler |
| `Tab` or `8` | Open Task Managers |

## Logs, threads, and profiling

Process diagnostics apply to the selected TaskManager or JobManager.
Use `<` / `>` to compare processes without returning to the worker list.

### Log files

From a worker, press `L` for its file list. On Job Manager, `L` switches
to its file list.

| Key | Action |
| --- | --- |
| Move rows; page; `Home` / `End` | Select a log file |
| `Enter` | Open the selected file |
| `x` | Open stdout |
| `d` | Open a thread dump |
| `p` | Open Process Profiler |
| `r` | Reload the file list |

Use lowercase `l` on the process page, or `lg` in the palette, to open
the current log directly at its tail.

### Documents: logs, stdout, and full values

| Key | Action |
| --- | --- |
| `Up` / `Down` or `k` / `j` | Scroll vertically |
| `PgUp` / `PgDn` or `Ctrl+U` / `Ctrl+D` | Page up / down |
| `Home` / `End` | First / last line |
| `h` / `l` | Scroll horizontally |
| `0` | Reset horizontal scroll |
| `/` | Search text |
| `Enter` while searching | Find the next match |
| `n` / `N` | Next / previous match |
| `r` | Reload a remote log or stdout document |

The `Left` arrow focuses navigation; use `h` for horizontal scrolling.
Logs are fetched snapshots, not a continuous tail. Full accumulator values
and opened stack traces are static documents.

### Thread Dump

| Key | Action |
| --- | --- |
| Move rows; page; `Home` / `End` | Select a thread |
| `Ctrl+U` / `Ctrl+D` | Scroll the selected thread's stack preview |
| `Enter` | Open its full stack in a document |
| `/` | Filter thread names and stacks |
| `p` | Open Process Profiler |
| `r` | Request a new dump |

Filtering manually replaces any automatic thread focus from a subtask or
exception jump. Refreshes are manual.

### Process Profiler

Open `pr` or press `p` from a process page. The cluster must enable
`rest.profiling.enabled: true`.

| Key | Action |
| --- | --- |
| `[` / `]` | Choose CPU, LOCK, WALL, ALLOC, or ITIMER mode |
| `-` or `_`; `+` or `=` | Shorter / longer capture: 3, 5, 10, 30, or 60 seconds |
| `p` or `n` | Start a capture with the displayed mode and duration |
| Move rows; page; `Home` / `End` | Select a profiler run |
| `Enter` | Open a finished HTML report as a terminal flame graph |
| `l` / `x` / `d` | Current log / stdout / thread dump |
| `r` | Refresh profiler history |

Starting a capture acts immediately; there is no separate `y` confirmation.
Captures target the selected process.

### Flame graphs

Live vertex flame graphs sample a running vertex. Profiler flame graphs
display a completed process capture. Both use these controls:

| Key | Action |
| --- | --- |
| `Up` / `k`; `Down` / `j` | Parent / child frame |
| `Left` / `h`; `Right` / `l` | Previous / next sibling frame |
| `Tab` / `Shift+Tab` | Next / previous frame in display order |
| `PgUp` / `PgDn` | Move by five frames |
| `Enter` | Zoom into the selected frame |
| `Backspace` | Zoom out |
| `Home` or `0` | Reset to the root |
| `q` or `Esc` | Zoom out first; return to the previous screen once at the root |

With mouse capture enabled, click to select a frame and click it again to
zoom in. The wheel moves between frames.

**Live Vertex Flame Graph** requires `rest.flamegraph.enabled: true`.
Open it with `F` on a job graph or `fl` in the palette:

| Key | Action |
| --- | --- |
| `[` / `]` | Change sample type: On CPU, Off CPU, Mixed |
| `s` / `S` | Next / previous scope: all subtasks or one subtask |
| `r` | Request fresh samples |
| `g` | Return to the job graph |
| `<` / `>` | Previous / next vertex |

The default sample type is Mixed. Initial samples can take several seconds.

**Profile Flame Graph** uses `g` to return to Process Profiler.
`<` / `>` opens another process's Profiler; a completed report is not
transferred to that process.

## Job actions

Open `act`, `savepoint`, or `cancel` in the palette with a job selected.
Move with arrows or `j` / `k`; `Home` / `End` selects the first /
last action.

| Action | Effect |
| --- | --- |
| Configured checkpoint | Trigger a checkpoint using the configured checkpoint type |
| Full checkpoint | Trigger a full checkpoint |
| Savepoint | Trigger a canonical savepoint while the job keeps running |
| Stop with savepoint | Take a savepoint and stop without draining |
| Stop, drain, and savepoint | Advance to the maximum watermark, fire event-time timers, take a savepoint, and stop |
| Cancel | Cancel immediately without a final savepoint; in-flight work may be lost |

Availability depends on job state; stop-with-savepoint actions require a
streaming job. Savepoints use the cluster's configured default directory,
`execution.checkpointing.savepoint-dir`; the TUI has no target-directory
editor.

To run an action:

1. Select an available action and press `Enter`.
2. Review the displayed action and target job.
3. Press `y` to execute, or `n`, `q`, or `Esc` to dismiss.

Navigation and selection are locked during confirmation so the target cannot
change. The TUI displays request progress, failures, and returned savepoint
paths. Quitting does not undo a request already accepted by Flink.

These confirmations apply to Job Actions. SQL execution and profiler captures
use their own direct execution keys.

## SQL workbench

Configure `--sql-endpoint`, then open `sq` or `sql` from the palette.
The workbench opens in **NORMAL** mode with `SELECT 1 AS answer;` in the
editor.

### Run a first query

1. Press `i` to enter **INSERT** mode.
2. Press `Ctrl+L` to clear the editor, then type or paste:

   ```sql
   SELECT 1 AS answer;
   ```

3. Press `F5` or `Ctrl+Enter` to execute.
4. Press `Tab` to leave insert mode and focus results.
5. Press `Ctrl+X` to cancel an operation that is still running.

Pasting SQL inserts text; it does not execute the statement. Submitted SQL
runs against the configured Gateway without a separate confirmation dialog.

### Editor and result controls

| Key | NORMAL mode | INSERT mode |
| --- | --- | --- |
| `i`, `e`, or `Enter` | Enter INSERT mode | Insert text / newline |
| `Esc` | Go back through normal navigation | Return to NORMAL mode |
| `Tab` | Switch editor / results focus | Return to NORMAL mode and focus results |
| `F5` or `Ctrl+Enter` | Execute the editor contents | Execute the editor contents |
| `Ctrl+X` | Cancel the active operation | Cancel the active operation |
| Arrow keys | Move in the focused area; `Left` focuses navigation | Move the text cursor |
| `Home` / `End` | Line boundaries in editor; first / last result in results | Start / end of the current line |
| `Backspace` / `Delete` | `Backspace` goes to the containing scope | Delete before / after the cursor |
| `Ctrl+L` | — | Clear the editor |
| `r` | Refresh session/result state or retry a failed result page | Insert `r` |

With results focused in NORMAL mode, `j` / `k` and page keys navigate
rows. `Enter`, `i`, or `e` returns to editing. In INSERT mode,
printable navigation shortcuts such as `q`, `?`, and `Space` become
SQL text. Press `Esc` first to use them as TUI commands.

Clicking the editor enters INSERT mode when mouse capture is enabled.
Clicking or scrolling results focuses the result table.

### Operations and results

One statement operation can be active at a time. Cancel a running operation
before submitting another. If its handle has not arrived yet, cancellation is
queued until it becomes available.

Streaming results retain the latest **500 rows**, with Flink changelog kinds
such as `+I`, `-U`, `+U`, and `-D` where applicable.
There is no query history, completion, or export.

`r` does not rerun the SQL statement. Pausing periodic screen refresh does
not cancel the query. If the Gateway expires the session, restart the TUI.
Quitting attempts to close the operation and session.

## Investigation walkthroughs

### Find a backpressured operator's worker thread

1. Open a job from Overview.
2. Open `dx` in the palette and use `b` for Backpressure.
3. Select a busy or backpressured vertex, then press `Enter` for Subtasks.
4. Press `s`, then `p` and `d` to sort by backpressure descending;
   press `Enter` to leave the sort editor.
5. Select a subtask and press `d` to inspect its worker's thread dump.
6. Use `q` to return along the same trail. For a process capture, press
   `p` in the thread dump to open Profiler, review mode and duration, then
   press `p` again to start.

### Follow a failure to its log

1. Open the job and then `ex` in the palette.
2. Select an incident; use `[` / `]` to inspect concurrent exceptions.
3. Press `l` to open the affected worker's current log.
4. Use `/` to search for an exception or task name and `n` / `N`
   to move between matches.
5. Return with `q`; use `g` to locate the affected operator in the graph
   or `T` to inspect the worker.

### Compare slow checkpoints

1. Open `cp` for the job and select a slow checkpoint in History.
2. Press `Enter` to inspect operators.
3. Press `s` to select Duration sorting, then open an operator with
   `Enter`.
4. Compare subtask duration, alignment, and start delay; use `s` to sort
   by the phase of interest.
5. Use `<` / `>` to compare neighboring checkpoints for the same operator.
6. Press `g` to inspect that operator in the running job graph.

## Troubleshooting and limits

| Symptom | What to check |
| --- | --- |
| Connection refused or disconnected | Verify `--endpoint`, the REST port, network access, and credentials. Open `Ctrl+E` for the full error. |
| Job views work but SQL fails | Check the separate Gateway address and `--sql-*` credentials. Disable SQL with `--sql-endpoint ''` if unused. |
| Certificate validation fails | Supply the appropriate CA with `--ca-cert` or `--sql-ca-cert`; check the hostname and certificate validity. |
| A destination is unavailable | Select the required job, vertex, checkpoint, or process first. |
| A shortcut inserts text | Leave the filter, palette, or SQL INSERT mode with `Esc`. |
| Terminal copying stops working | Press `m` in normal navigation to disable mouse capture. |
| Logs or threads do not update | These are snapshots; press `r` to reload. |
| Some values remain visible after a failed refresh | They are the last known values; check the stale or degraded status and error details. |
| A vertex flame graph is unavailable or initially empty | Enable `rest.flamegraph.enabled` and allow time for initial samples from a running vertex. |
| A profiler capture is unavailable | Enable `rest.profiling.enabled` on the cluster. |
| A savepoint fails for a missing directory | Configure the cluster's default savepoint directory. |
| A log, stdout, or profiler report exceeds 16 MiB | Download it directly from Flink; the TUI rejects larger downloads. |
| SQL session expired | Restart the TUI to create a fresh session. |
| SQL rows disappear from the beginning | Only the latest 500 rows are retained. |

Compatibility is tested against **Flink 1.20.5 and 2.3.0 on Java 17**.
Other versions are untested. Job submission and JAR uploads are handled
outside the TUI, for example through the Flink Web UI.

## Development commands

Run these from the repository root:

| Command | Purpose |
| --- | --- |
| `make build` | Build `bin/flink-tui` |
| `make tui` | Run from source with default connection options |
| `make up` | Build and start the local playground |
| `make status` | Show playground container status, including exited submitters |
| `make down` | Stop and remove playground containers and networks |
| `make test` | Run Go tests |
| `make test-race` | Run Go tests with the race detector |
| `make lint` | Run the pinned lint tool |
| `make test-e2e` | Run the isolated Docker integration suite |
| `FLINK_VERSION=1.20.5 make test-e2e` | Run that suite against Flink 1.20.5 |
| `make test-topology` | Check live topology rendering against the playground on port 8081 |
| `vhs scripts/demo.tape` | Record the demo; requires VHS, a built binary, and the running playground |

The isolated E2E suite uses ports 18081 and 18083 and removes its temporary
cluster afterward. See the [playground instructions](../README.md#try-the-playground)
for starting the demo jobs.

`make test` and `make test-race` include real HTTPS connection tests for both
clients: basic and bearer authentication, private CAs, mutual TLS, certificate
rejection, and the explicit insecure option. Certificates are generated in
temporary directories; the tests use loopback ports and need no Docker cluster.

`make test-e2e` also checks live Flink through authenticated HTTPS reverse
proxies, with separate REST and SQL Gateway credentials, CAs, and URL prefixes.
It exercises SQL session creation, execution, result pagination, and cleanup.
