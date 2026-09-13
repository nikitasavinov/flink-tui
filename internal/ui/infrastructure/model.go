package infrastructure

import (
	"context"
	"slices"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/nikitasavinov/flink-tui/internal/flink"
	"github.com/nikitasavinov/flink-tui/internal/ui/shared"
	shellmodule "github.com/nikitasavinov/flink-tui/internal/ui/shell"
)

const requestTimeout = 8 * time.Second

// View identifies an infrastructure-owned screen.
type View uint8

const (
	ViewTaskManagers View = iota
	ViewTaskManagerDetail
	ViewJobManager
)

// Intent asks the parent application to navigate outside this module.
type Intent uint8

const (
	IntentNone Intent = iota
	IntentOverview
	IntentBack
	IntentLogs
	IntentLogFiles
	IntentStdout
	IntentThreadDump
	IntentProfiler
	IntentDocument
)

// Result combines an optional command with a cross-feature navigation intent.
type Result struct {
	Command   tea.Cmd
	Intent    Intent
	Process   flink.ProcessRef
	Log       flink.LogFile
	BackToken int
	Notice    string
}

// Message is the sealed family of infrastructure refresh replies.
type Message interface {
	infrastructureMessage()
}

type refreshMsg struct {
	infrastructure flink.Infrastructure
	err            error
	generation     uint64
}

func (refreshMsg) infrastructureMessage() {}

// State is a detached diagnostic snapshot for shell integration and tests.
type State struct {
	View                   View
	Infrastructure         flink.Infrastructure
	Busy                   bool
	Err                    error
	Generation             uint64
	PendingTargetID        string
	BackToken              int
	BackLabel              string
	JobManagerLogsOpen     bool
	TaskManagerIndex       int
	JobManagerLogIndex     int
	JobManagerConfigIndex  int
	JobManagerConfigQuery  string
	JobManagerConfigSearch bool
	DetailOffset           int
}

// Model owns all mutable infrastructure screen state.
type Model struct {
	client      *flink.Client
	parent      context.Context
	formatError func(error) string

	view           View
	infrastructure flink.Infrastructure
	busy           bool
	err            error
	generation     uint64

	taskManagerSelection shared.Cursor
	detailOffset         int
	targetID             string
	backToken            int
	backLabel            string
	jobManagerConfig     shared.Cursor
	jobManagerFilter     shellmodule.QueryInput
	jobManagerLog        shared.Cursor
	jobManagerLogsOpen   bool
}

// New creates an infrastructure module tied to the application's request tree.
func New(client *flink.Client, parent context.Context, formatError func(error) string) Model {
	return Model{
		client:      client,
		parent:      parent,
		formatError: formatError,
		view:        ViewTaskManagers,
		backLabel:   "Task Managers",
	}
}

// State returns a detached copy of module-owned data.
func (m Model) State() State {
	return State{
		View: m.view, Infrastructure: cloneInfrastructure(m.infrastructure),
		Busy: m.busy, Err: m.err, Generation: m.generation, PendingTargetID: m.targetID,
		BackToken: m.backToken, BackLabel: m.backLabel,
		JobManagerLogsOpen: m.jobManagerLogsOpen,
		TaskManagerIndex:   m.taskManagerSelection.Index(), JobManagerLogIndex: m.jobManagerLog.Index(),
		JobManagerConfigIndex: m.jobManagerConfig.Index(),
		JobManagerConfigQuery: m.jobManagerFilter.Value(), JobManagerConfigSearch: m.jobManagerFilter.Active(),
		DetailOffset: m.detailOffset,
	}
}

// Restore hydrates a previously fetched infrastructure snapshot. It is useful
// for preserving the last-good view when rebuilding an application model.
func (m *Model) Restore(value flink.Infrastructure) {
	m.infrastructure = cloneInfrastructure(value)
	m.constrainSelections()
}

// RestoreState hydrates a complete module snapshot, including presentation
// state. It is intended for application reconstruction and deterministic tests.
func (m *Model) RestoreState(state State) {
	m.view = state.View
	m.infrastructure = cloneInfrastructure(state.Infrastructure)
	m.busy = state.Busy
	m.err = state.Err
	m.generation = state.Generation
	m.targetID = state.PendingTargetID
	m.backToken = state.BackToken
	m.backLabel = shared.Fallback(state.BackLabel, "Task Managers")
	m.jobManagerLogsOpen = state.JobManagerLogsOpen
	m.jobManagerFilter.Restore(shellmodule.QueryState{Value: state.JobManagerConfigQuery, Open: state.JobManagerConfigSearch})
	m.taskManagerSelection.Set(state.TaskManagerIndex, len(m.infrastructure.TaskManagers))
	m.jobManagerLog.Set(state.JobManagerLogIndex, len(m.infrastructure.JobManager.Logs))
	m.jobManagerConfig.Set(state.JobManagerConfigIndex, len(m.filteredJobManagerConfiguration()))
	m.detailOffset = max(0, state.DetailOffset)
}

// View returns the active infrastructure-owned screen.
func (m Model) View() View { return m.view }

// Activate switches between infrastructure-owned views without fetching.
func (m *Model) Activate(view View) { m.view = view }

// SetBackTarget defines where process and detail actions return after the
// TaskManager list is entered directly by the application shell.
func (m *Model) SetBackTarget(backToken int, backLabel string) {
	m.backToken = backToken
	m.backLabel = shared.Fallback(backLabel, "Task Managers")
}

// Error returns the current infrastructure request error.
func (m Model) Error() error { return m.err }

// CapturesKeys reports whether JobManager configuration filtering owns input.
func (m Model) CapturesKeys() bool {
	return m.view == ViewJobManager && !m.jobManagerLogsOpen && m.jobManagerFilter.Active()
}

// SelectedTaskManager returns the currently selected manager.
func (m Model) SelectedTaskManager() (flink.TaskManager, bool) {
	rows := m.infrastructure.TaskManagers
	index := m.taskManagerSelection.Index()
	if index < 0 || index >= len(rows) {
		return flink.TaskManager{}, false
	}
	return rows[index], true
}

// SelectTaskManagerPeer selects an adjacent worker while retaining the detail
// view. The selection wraps so operators can scan every worker in place.
func (m *Model) SelectTaskManagerPeer(delta int) (flink.TaskManager, flink.TaskManager, bool) {
	rows := m.infrastructure.TaskManagers
	if len(rows) < 2 || delta == 0 {
		return flink.TaskManager{}, flink.TaskManager{}, false
	}
	current := m.taskManagerSelection.Index()
	if current < 0 || current >= len(rows) {
		return flink.TaskManager{}, flink.TaskManager{}, false
	}
	next := (current + delta%len(rows) + len(rows)) % len(rows)
	m.taskManagerSelection.Set(next, len(rows))
	m.targetID = ""
	m.detailOffset = 0
	m.view = ViewTaskManagerDetail
	return rows[current], rows[next], next != current
}

// SelectedTaskManagerPosition reports the selected worker's one-based
// position in the same stable order rendered by the Task Managers table.
func (m Model) SelectedTaskManagerPosition() (int, int, bool) {
	index := m.taskManagerSelection.Index()
	if index < 0 || index >= len(m.infrastructure.TaskManagers) {
		return 0, len(m.infrastructure.TaskManagers), false
	}
	return index + 1, len(m.infrastructure.TaskManagers), true
}

// SelectTaskManagerByID keeps infrastructure context aligned when another
// screen moves laterally to a different worker. It does not activate a view or
// start a fetch.
func (m *Model) SelectTaskManagerByID(id string) bool {
	return m.selectTaskManagerByID(strings.TrimSpace(id))
}

// SelectedJobManagerLog returns the selected JobManager log metadata.
func (m Model) SelectedJobManagerLog() (flink.LogFile, bool) {
	logs := m.infrastructure.JobManager.Logs
	index := m.jobManagerLog.Index()
	if index < 0 || index >= len(logs) {
		return flink.LogFile{}, false
	}
	return logs[index], true
}

// OpenTaskManagers activates the TaskManager list and refreshes it.
func (m *Model) OpenTaskManagers(backToken int) tea.Cmd {
	m.view = ViewTaskManagers
	m.targetID = ""
	m.backToken = backToken
	m.backLabel = "Task Managers"
	return m.Refresh()
}

// OpenTaskManagerByID resolves a TaskManager immediately when cached, or after
// the refresh reply otherwise.
func (m *Model) OpenTaskManagerByID(taskManagerID string, backToken int, backLabel string) Result {
	taskManagerID = strings.TrimSpace(taskManagerID)
	if taskManagerID == "" {
		return Result{Notice: "TaskManager ID is unavailable."}
	}
	m.backToken = backToken
	m.backLabel = shared.Fallback(backLabel, "Task Managers")
	m.targetID = taskManagerID
	if m.selectTaskManagerByID(taskManagerID) {
		m.view = ViewTaskManagerDetail
		m.detailOffset = 0
	} else {
		m.view = ViewTaskManagers
	}
	return Result{Command: m.Refresh()}
}

// OpenJobManager activates the JobManager screen and refreshes it.
func (m *Model) OpenJobManager() tea.Cmd {
	m.view = ViewJobManager
	return m.Refresh()
}

// Refresh marks the current view busy and starts a new generation.
func (m *Model) Refresh() tea.Cmd {
	if m.busy {
		return nil
	}
	m.busy = true
	m.err = nil
	return m.fetch()
}

// Poll retains the last-good data and lets a slow request finish before another
// tick starts work. Advancing the generation on every tick would starve replies.
func (m *Model) Poll() tea.Cmd {
	if m.busy {
		return nil
	}
	m.busy = true
	return m.fetch()
}

func (m *Model) fetch() tea.Cmd {
	m.generation++
	client := m.client
	generation := m.generation
	parent := m.parent
	return func() tea.Msg {
		if client == nil {
			return refreshMsg{err: context.Canceled, generation: generation}
		}
		if parent == nil {
			parent = context.Background()
		}
		ctx, cancel := context.WithTimeout(parent, requestTimeout)
		defer cancel()
		value, err := client.Infrastructure(ctx)
		return refreshMsg{infrastructure: value, err: err, generation: generation}
	}
}

// Apply reduces a refresh reply. active prevents a late reply from changing
// the parent application's current screen while still accepting cache data.
func (m *Model) Apply(message Message, active bool) Result {
	reply, ok := message.(refreshMsg)
	if !ok || reply.generation != m.generation {
		return Result{}
	}
	m.busy = false
	m.err = reply.err
	if reply.err == nil {
		m.infrastructure = reply.infrastructure
		m.constrainSelections()
	}
	if m.targetID == "" {
		return Result{}
	}
	target := m.targetID
	m.targetID = ""
	if !active {
		return Result{}
	}
	if reply.err == nil && m.selectTaskManagerByID(target) {
		m.view = ViewTaskManagerDetail
		m.detailOffset = 0
		return Result{}
	}
	return Result{
		Intent: IntentBack, BackToken: m.backToken,
		Notice: "TaskManager " + target + " is no longer registered.",
	}
}

// HandleKey handles input for the active infrastructure view.
func (m *Model) HandleKey(key string, bodyHeight, contentWidth int) Result {
	switch m.view {
	case ViewTaskManagers:
		return m.handleTaskManagerKey(key, bodyHeight)
	case ViewTaskManagerDetail:
		return m.handleTaskManagerDetailKey(key, bodyHeight, contentWidth)
	case ViewJobManager:
		return m.handleJobManagerKey(key, bodyHeight)
	default:
		return Result{}
	}
}

func (m *Model) handleTaskManagerKey(key string, bodyHeight int) Result {
	switch key {
	case "up", "k":
		m.moveTaskManagerSelection(-1)
	case "down", "j":
		m.moveTaskManagerSelection(1)
	case "pgup":
		m.moveTaskManagerSelection(-max(1, taskManagerRowsAvailable(bodyHeight)))
	case "pgdown":
		m.moveTaskManagerSelection(max(1, taskManagerRowsAvailable(bodyHeight)))
	case "enter":
		if len(m.infrastructure.TaskManagers) > 0 {
			m.view = ViewTaskManagerDetail
			m.detailOffset = 0
			m.backLabel = "Task Managers"
		}
	case "l", "L", "x", "d", "p":
		if manager, ok := m.SelectedTaskManager(); ok {
			return processIntent(key, flink.TaskManagerProcess(manager.ID), m.backToken)
		}
	case "tab", "9":
		m.view = ViewJobManager
	case "esc":
		return Result{Intent: IntentOverview}
	}
	return Result{}
}

func (m *Model) handleTaskManagerDetailKey(key string, bodyHeight, contentWidth int) Result {
	switch key {
	case "up", "k":
		m.moveTaskManagerDetail(-1, bodyHeight, contentWidth)
	case "down", "j":
		m.moveTaskManagerDetail(1, bodyHeight, contentWidth)
	case "pgup":
		m.moveTaskManagerDetail(-max(1, detailViewportHeight(bodyHeight)), bodyHeight, contentWidth)
	case "pgdown":
		m.moveTaskManagerDetail(max(1, detailViewportHeight(bodyHeight)), bodyHeight, contentWidth)
	case "home":
		m.detailOffset = 0
	case "end":
		m.detailOffset = m.detailMaxOffset(bodyHeight, contentWidth)
	case "esc", "backspace":
		return Result{Intent: IntentBack, BackToken: m.backToken}
	case "tab", "9":
		m.view = ViewJobManager
	case "l", "L", "x", "d", "p":
		if manager, ok := m.SelectedTaskManager(); ok {
			return processIntent(key, flink.TaskManagerProcess(manager.ID), m.backToken)
		}
	}
	return Result{}
}

func (m *Model) handleJobManagerKey(key string, bodyHeight int) Result {
	if m.jobManagerFilter.Active() {
		m.handleJobManagerConfigSearch(key)
		return Result{}
	}
	switch key {
	case "up", "k":
		m.moveJobManagerSelection(-1)
	case "down", "j":
		m.moveJobManagerSelection(1)
	case "pgup":
		m.moveJobManagerSelection(-max(1, jobManagerRowsAvailable(bodyHeight)))
	case "pgdown":
		m.moveJobManagerSelection(max(1, jobManagerRowsAvailable(bodyHeight)))
	case "home":
		m.setJobManagerCursorToStart()
	case "end":
		m.setJobManagerCursorToEnd()
	case "l":
		return processIntent(key, flink.JobManagerProcess(), m.backToken)
	case "L":
		m.jobManagerLogsOpen = !m.jobManagerLogsOpen
		m.jobManagerFilter.Restore(shellmodule.QueryState{Value: m.jobManagerFilter.Value(), Open: false})
	case "enter":
		if m.jobManagerLogsOpen {
			if log, ok := m.SelectedJobManagerLog(); ok {
				return Result{Intent: IntentDocument, Process: flink.JobManagerProcess(), Log: log, BackToken: m.backToken}
			}
		}
	case "x", "d", "p":
		return processIntent(key, flink.JobManagerProcess(), m.backToken)
	case "tab", "8":
		m.view = ViewTaskManagers
	case "/":
		if !m.jobManagerLogsOpen {
			m.jobManagerFilter.Restore(shellmodule.QueryState{Value: m.jobManagerFilter.Value(), Open: true})
		}
	case "esc":
		return Result{Intent: IntentOverview}
	}
	return Result{}
}

func (m *Model) handleJobManagerConfigSearch(key string) {
	m.jobManagerFilter.HandleKey(key)
	m.jobManagerConfig.Set(0, 0)
}

// HandleClick applies a body-relative mouse click.
func (m *Model) HandleClick(event tea.Mouse, bodyY, bodyHeight int) Result {
	if event.Button != tea.MouseLeft {
		return Result{}
	}
	if m.view == ViewTaskManagerDetail {
		if bodyY >= 0 && bodyY <= 1 {
			return Result{Intent: IntentBack, BackToken: m.backToken}
		}
		return Result{}
	}
	if m.view == ViewTaskManagers {
		position := bodyY - taskManagerBodyRowStart
		if position < 0 || position >= taskManagerRowsAvailable(bodyHeight) {
			return Result{}
		}
		index := m.taskManagerWindowStart(bodyHeight) + position
		if index >= 0 && index < len(m.infrastructure.TaskManagers) {
			m.taskManagerSelection.Set(index, len(m.infrastructure.TaskManagers))
			m.view = ViewTaskManagerDetail
			m.detailOffset = 0
			m.backLabel = "Task Managers"
		}
		return Result{}
	}
	position := bodyY - jobManagerBodyRowStart
	if position < 0 || position >= jobManagerRowsAvailable(bodyHeight) {
		return Result{}
	}
	if m.jobManagerLogsOpen {
		index := m.jobManagerLogWindowStart(bodyHeight) + position
		m.jobManagerLog.Set(index, len(m.infrastructure.JobManager.Logs))
		return Result{}
	}
	index := m.jobManagerConfigWindowStart(bodyHeight) + position
	m.jobManagerConfig.Set(index, len(m.filteredJobManagerConfiguration()))
	return Result{}
}

// HandleWheel moves the selection or detail viewport for the active view.
func (m *Model) HandleWheel(delta, bodyHeight, contentWidth int) {
	switch m.view {
	case ViewTaskManagers:
		m.moveTaskManagerSelection(delta)
	case ViewTaskManagerDetail:
		m.moveTaskManagerDetail(delta, bodyHeight, contentWidth)
	case ViewJobManager:
		m.moveJobManagerSelection(delta)
	}
}

func processIntent(key string, process flink.ProcessRef, backToken int) Result {
	intent := IntentNone
	switch key {
	case "l":
		intent = IntentLogs
	case "L":
		intent = IntentLogFiles
	case "x":
		intent = IntentStdout
	case "d":
		intent = IntentThreadDump
	case "p":
		intent = IntentProfiler
	}
	return Result{Intent: intent, Process: process, BackToken: backToken}
}

func (m *Model) selectTaskManagerByID(id string) bool {
	for index, manager := range m.infrastructure.TaskManagers {
		if manager.ID == id {
			m.taskManagerSelection.Set(index, len(m.infrastructure.TaskManagers))
			return true
		}
	}
	return false
}

func (m *Model) moveTaskManagerSelection(delta int) {
	m.taskManagerSelection.Move(delta, len(m.infrastructure.TaskManagers))
}

func (m *Model) moveJobManagerSelection(delta int) {
	if m.jobManagerLogsOpen {
		m.jobManagerLog.Move(delta, len(m.infrastructure.JobManager.Logs))
		return
	}
	m.jobManagerConfig.Move(delta, len(m.filteredJobManagerConfiguration()))
}

func (m *Model) setJobManagerCursorToStart() {
	if m.jobManagerLogsOpen {
		m.jobManagerLog.Set(0, len(m.infrastructure.JobManager.Logs))
		return
	}
	m.jobManagerConfig.Set(0, len(m.filteredJobManagerConfiguration()))
}

func (m *Model) setJobManagerCursorToEnd() {
	if m.jobManagerLogsOpen {
		m.jobManagerLog.Set(len(m.infrastructure.JobManager.Logs)-1, len(m.infrastructure.JobManager.Logs))
		return
	}
	rows := m.filteredJobManagerConfiguration()
	m.jobManagerConfig.Set(len(rows)-1, len(rows))
}

func (m *Model) constrainSelections() {
	m.taskManagerSelection.Constrain(len(m.infrastructure.TaskManagers))
	m.jobManagerConfig.Constrain(len(m.filteredJobManagerConfiguration()))
	m.jobManagerLog.Constrain(len(m.infrastructure.JobManager.Logs))
}

func cloneInfrastructure(value flink.Infrastructure) flink.Infrastructure {
	value.TaskManagers = slices.Clone(value.TaskManagers)
	for index := range value.TaskManagers {
		value.TaskManagers[index].GarbageCollectors = slices.Clone(value.TaskManagers[index].GarbageCollectors)
		value.TaskManagers[index].Allocations = slices.Clone(value.TaskManagers[index].Allocations)
	}
	value.JobManager.Configuration = slices.Clone(value.JobManager.Configuration)
	value.JobManager.Logs = slices.Clone(value.JobManager.Logs)
	return value
}
