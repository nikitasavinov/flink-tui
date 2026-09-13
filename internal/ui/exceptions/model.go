package exceptions

import (
	"fmt"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/nikitasavinov/flink-tui/internal/flink"
	processmodule "github.com/nikitasavinov/flink-tui/internal/ui/processdiag"
	shellmodule "github.com/nikitasavinov/flink-tui/internal/ui/shell"
)

const headerHeight = 3

// Intent asks the root coordinator to perform cross-feature navigation.
type Intent uint8

const (
	IntentNone Intent = iota
	IntentGraph
	IntentOverview
	IntentReloadSnapshot
	IntentTaskManager
	IntentThreadDump
	IntentLogs
)

// Context contains job and viewport state owned by the shell.
type Context struct {
	Snapshot   flink.Snapshot
	BodyHeight int
}

// Result combines a command with navigation and operator feedback.
type Result struct {
	Command       tea.Cmd
	Intent        Intent
	Selected      string
	TaskManagerID string
	Process       flink.ProcessRef
	Focus         processmodule.Focus
	Notice        string
}

// State is a detached snapshot for shell policy and tests.
type State struct {
	Cursor          int
	VariantCursor   int
	DetailOffset    int
	Sort            uint8
	Query           string
	SearchOpen      bool
	Limit           int
	LoadingMore     bool
	Selected        string
	SelectedFailure flink.JobException
}

// Model owns exception incident interaction and presentation state.
type Model struct {
	context  Context
	snapshot flink.Snapshot
	selected string

	exceptionCursor        int
	exceptionVariantCursor int
	exceptionDetailOffset  int
	exceptionSort          exceptionSort
	exceptionFilter        shellmodule.QueryInput
	exceptionLimit         int
	exceptionLoadingMore   bool

	pendingIntent        Intent
	pendingTaskManagerID string
	pendingProcess       flink.ProcessRef
	pendingFocus         processmodule.Focus
	pendingNotice        string
}

// New creates an empty exception explorer.
func New() Model {
	return Model{exceptionSort: exceptionSortRecent, exceptionLimit: flink.DefaultExceptionLimit}
}

// Sync updates the current snapshot without treating it as a completed fetch.
func (m *Model) Sync(value Context) {
	m.context, m.snapshot = value, value.Snapshot
}

// SnapshotApplied synchronizes snapshot content and constrains selection.
// Request completion is acknowledged separately with its requested limit.
func (m *Model) SnapshotApplied(value Context) {
	m.Sync(value)
	m.syncExceptionSelection()
}

// RefreshFinished clears incremental loading when a reply covers the requested
// history limit. An older in-flight refresh must not complete a newer request.
func (m *Model) RefreshFinished(limit int) {
	if limit >= m.exceptionLimit {
		m.exceptionLoadingMore = false
	}
}

// Reset drops all state tied to the previous job.
func (m *Model) Reset() { *m = New() }

// Open prepares the exception view for interaction.
func (m *Model) Open(value Context) {
	m.Sync(value)
	if value.Snapshot.JobID == "" {
		return
	}
	m.syncExceptionSelection()
}

// HandleKey reduces keyboard input.
func (m *Model) HandleKey(key string) Result {
	m.clearPending()
	command := m.handleExceptionKey(key)
	return m.result(command)
}

// HandleClick reduces mouse input.
func (m *Model) HandleClick(event tea.Mouse) Result {
	m.clearPending()
	command := m.handleExceptionMouseClick(event)
	return m.result(command)
}

// Move changes the selected incident.
func (m *Model) Move(delta int) { m.moveExceptionSelection(delta) }

// Render draws exception incidents and the selected trace.
func (m Model) Render(width, height int) string { return m.renderExceptions(width, height) }

// CapturesKeys reports whether search owns keyboard input.
func (m Model) CapturesKeys() bool { return m.exceptionFilter.Active() }

// Limit is the requested Flink exception-history size.
func (m Model) Limit() int { return m.exceptionLimit }

// State returns detached interaction state.
func (m Model) State() State {
	selected, _ := m.selectedException()
	return State{
		Cursor: m.exceptionCursor, VariantCursor: m.exceptionVariantCursor, DetailOffset: m.exceptionDetailOffset,
		Sort: uint8(m.exceptionSort), Query: m.exceptionFilter.Value(), SearchOpen: m.exceptionFilter.Active(),
		Limit: m.exceptionLimit, LoadingMore: m.exceptionLoadingMore, Selected: m.selected, SelectedFailure: selected,
	}
}

// RestoreState replaces interaction state while retaining the current context.
func (m *Model) RestoreState(state State) {
	m.exceptionCursor, m.exceptionVariantCursor, m.exceptionDetailOffset = state.Cursor, state.VariantCursor, state.DetailOffset
	m.exceptionSort = exceptionSort(state.Sort)
	m.exceptionFilter.Restore(shellmodule.QueryState{Value: state.Query, Open: state.SearchOpen})
	m.exceptionLimit, m.exceptionLoadingMore, m.selected = state.Limit, state.LoadingMore, state.Selected
	if m.exceptionLimit <= 0 {
		m.exceptionLimit = flink.DefaultExceptionLimit
	}
	m.syncExceptionSelection()
}

// Incidents returns the current filtered and sorted incident roots.
func (m Model) Incidents() []flink.JobException { return m.filteredExceptionIncidents() }

// Variants returns a root incident and its concurrent failures.
func Variants(root flink.JobException) []flink.JobException { return exceptionVariants(root) }

// TaskDisplay returns the operator label used by the view.
func TaskDisplay(taskName string) string { return exceptionTaskDisplay(taskName) }

// NormalizeOperator returns the graph-matching form of an exception task name.
func NormalizeOperator(taskName string) string { return normalizedExceptionOperator(taskName) }

func (m *Model) clearPending() {
	m.pendingIntent, m.pendingTaskManagerID = IntentNone, ""
	m.pendingProcess, m.pendingFocus, m.pendingNotice = flink.ProcessRef{}, processmodule.Focus{}, ""
}

func (m Model) result(command tea.Cmd) Result {
	return Result{
		Command: command, Intent: m.pendingIntent, Selected: m.selected,
		TaskManagerID: m.pendingTaskManagerID, Process: m.pendingProcess,
		Focus: m.pendingFocus, Notice: m.pendingNotice,
	}
}

func (m *Model) setNotice(value string) { m.pendingNotice = value }
func (m *Model) fetchSnapshot() tea.Cmd { m.pendingIntent = IntentReloadSnapshot; return nil }

func (m *Model) openTaskManagerByID(id string) tea.Cmd {
	m.pendingIntent, m.pendingTaskManagerID = IntentTaskManager, id
	return nil
}

func (m *Model) openThreadDumpWithFocus(process flink.ProcessRef, focus processmodule.Focus) tea.Cmd {
	m.pendingIntent, m.pendingProcess, m.pendingFocus = IntentThreadDump, process, focus
	return nil
}

func (m *Model) openProcessLogs(process flink.ProcessRef) tea.Cmd {
	m.pendingIntent, m.pendingProcess = IntentLogs, process
	return nil
}

func (m Model) bodyHeight() int {
	if m.context.BodyHeight > 0 {
		return m.context.BodyHeight
	}
	return 24
}

func humanDuration(value time.Duration) string {
	if value <= 0 {
		return "-"
	}
	value = value.Round(time.Second)
	if value < time.Minute {
		return fmt.Sprintf("%ds", int(value.Seconds()))
	}
	if value < time.Hour {
		return fmt.Sprintf("%dm%02ds", int(value.Minutes()), int(value.Seconds())%60)
	}
	return fmt.Sprintf("%dh%02dm", int(value.Hours()), int(value.Minutes())%60)
}
