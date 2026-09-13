package profiler

import (
	"context"
	"fmt"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/nikitasavinov/flink-tui/internal/flink"
	"github.com/nikitasavinov/flink-tui/internal/ui/shared"
)

const (
	headerHeight                 = 3
	footerHeight                 = 1
	defaultRequestTimeout        = 8 * time.Second
	profilerReportRequestTimeout = 20 * time.Second
	profilerCaptureGracePeriod   = 15 * time.Second
)

// Intent asks the shell to leave profiler history or open a captured report.
type Intent uint8

const (
	IntentNone Intent = iota
	IntentBack
	IntentLogs
	IntentStdout
	IntentThreadDump
	IntentReport
)

// Result combines module work with cross-feature navigation.
type Result struct {
	Command    tea.Cmd
	Intent     Intent
	BackToken  int
	Process    flink.ProcessRef
	ReportName string
	Graph      flink.FlameGraph
	Notice     string
}

// Message is the sealed family of profiler REST replies.
type Message interface {
	profilerMessage()
}

type listMsg struct {
	entries    []flink.Profiling
	err        error
	process    flink.ProcessRef
	generation uint64
	requestID  uint64
}

func (listMsg) profilerMessage() {}

type listRequest struct {
	process    flink.ProcessRef
	generation uint64
}

type startMsg struct {
	entry      flink.Profiling
	err        error
	process    flink.ProcessRef
	generation uint64
}

func (startMsg) profilerMessage() {}

type downloadMsg struct {
	filename   string
	graph      flink.FlameGraph
	err        error
	process    flink.ProcessRef
	generation uint64
}

func (downloadMsg) profilerMessage() {}

// State is a detached profiler snapshot for shell policy and tests.
type State struct {
	Process         flink.ProcessRef
	Generation      uint64
	BackToken       int
	Entries         []flink.Profiling
	Selection       int
	Mode            flink.ProfilerMode
	Duration        time.Duration
	Busy            bool
	Action          string
	Err             error
	Message         string
	MessageStatus   string
	PendingAt       time.Time
	ReportName      string
	PendingRunCount int
}

// Model owns one process profiler workspace.
type Model struct {
	client *flink.Client
	parent context.Context

	process                 flink.ProcessRef
	observabilityGeneration uint64
	profilerBackMode        int
	profilerEntries         []flink.Profiling
	profilerSelection       shared.Cursor
	profilerMode            flink.ProfilerMode
	profilerDuration        time.Duration
	profilerBusy            bool
	profilerAction          string
	profilerErr             error
	profilerMessage         string
	profilerMessageStatus   string
	profilerPendingAt       time.Time
	profilerPendingRuns     map[profilerRunIdentity]int
	profilerReportName      string
	listRequests            shared.RequestGate[listRequest]
	width, height           int
	pendingIntent           Intent
	pendingProcess          flink.ProcessRef
	pendingNotice           string
}

// New creates an empty profiler workspace.
func New(client *flink.Client, parent context.Context) Model {
	return Model{client: client, parent: parent, profilerMode: flink.ProfilerITimer, profilerDuration: 5 * time.Second}
}

// SetViewport updates shell-owned layout dimensions.
func (m *Model) SetViewport(width, height int) { m.width, m.height = width, height }

// Open resets state for a process and fetches its profiler history.
func (m *Model) Open(process flink.ProcessRef, backToken int) tea.Cmd {
	return m.openProfiler(process, backToken)
}

// Retarget opens profiler history for a peer JVM while retaining the chosen
// profiling mode and duration. Captures and reports never transfer.
func (m *Model) Retarget(process flink.ProcessRef) tea.Cmd {
	return m.openProfiler(process, m.profilerBackMode)
}

// Reset clears process-scoped profiler state while retaining dependencies.
func (m *Model) Reset() {
	client, parent := m.client, m.parent
	generation, requests := m.observabilityGeneration+1, m.listRequests
	requests.Reset()
	*m = New(client, parent)
	m.observabilityGeneration, m.listRequests = generation, requests
}

// State returns a detached snapshot.
func (m Model) State() State {
	return State{
		Process: m.process, Generation: m.observabilityGeneration,
		BackToken: m.profilerBackMode, Entries: append([]flink.Profiling(nil), m.profilerEntries...),
		Selection: m.profilerSelection.Index(), Mode: m.profilerMode, Duration: m.profilerDuration,
		Busy: m.profilerBusy, Action: m.profilerAction, Err: m.profilerErr,
		Message: m.profilerMessage, MessageStatus: m.profilerMessageStatus,
		PendingAt: m.profilerPendingAt, ReportName: m.profilerReportName,
		PendingRunCount: len(m.profilerPendingRuns),
	}
}

// RestoreState replaces state while retaining dependencies.
func (m *Model) RestoreState(state State) {
	m.process = state.Process
	m.observabilityGeneration = state.Generation
	m.profilerBackMode = state.BackToken
	m.profilerEntries = append([]flink.Profiling(nil), state.Entries...)
	m.profilerSelection.Set(state.Selection, len(m.profilerEntries))
	m.profilerMode = state.Mode
	if !containsProfilerMode(m.profilerMode) {
		m.profilerMode = flink.ProfilerITimer
	}
	m.profilerDuration = state.Duration
	if !containsProfilerDuration(m.profilerDuration) {
		m.profilerDuration = 5 * time.Second
	}
	m.profilerBusy = state.Busy
	m.profilerAction = state.Action
	m.profilerErr = state.Err
	m.profilerMessage = state.Message
	m.profilerMessageStatus = state.MessageStatus
	m.profilerPendingAt = state.PendingAt
	m.profilerReportName = state.ReportName
}

// HandleKey reduces profiler keyboard input.
func (m *Model) HandleKey(key string) Result {
	m.pendingIntent, m.pendingProcess, m.pendingNotice = IntentNone, flink.ProcessRef{}, ""
	command := m.handleProfilerKey(key)
	result := Result{Command: command, Intent: m.pendingIntent, Process: m.pendingProcess, Notice: m.pendingNotice}
	if result.Intent == IntentBack {
		result.BackToken = m.profilerBackMode
	}
	return result
}

// HandleClick reduces an absolute terminal mouse click.
func (m *Model) HandleClick(event tea.Mouse) Result {
	m.pendingIntent, m.pendingProcess, m.pendingNotice = IntentNone, flink.ProcessRef{}, ""
	command := m.handleProfilerMouseClick(event)
	return Result{Command: command, Intent: m.pendingIntent, Process: m.pendingProcess, Notice: m.pendingNotice}
}

// HandleWheel moves through profiler history.
func (m *Model) HandleWheel(delta int) { m.moveProfilerSelection(delta) }

// Refresh starts a history fetch and clears the previous request error.
func (m *Model) Refresh() tea.Cmd {
	m.profilerBusy = true
	m.profilerErr = nil
	return m.fetchProfilerList()
}

// Poll fetches current history without clearing the visible list.
func (m *Model) Poll() tea.Cmd { return m.fetchProfilerList() }

// Apply reduces one profiler reply. active prevents a late report download from
// stealing navigation after the operator leaves the profiler screen.
func (m *Model) Apply(message Message, active bool) Result {
	switch message := message.(type) {
	case listMsg:
		if !m.listRequests.Finish(message.requestID) || message.generation != m.observabilityGeneration || message.process != m.process {
			return Result{}
		}
		m.profilerBusy = false
		m.profilerErr = message.err
		if message.err == nil {
			m.profilerEntries = message.entries
			m.profilerSelection.Constrain(len(m.profilerEntries))
			m.completePendingProfiler(message.entries)
		}
	case startMsg:
		return m.applyStart(message)
	case downloadMsg:
		return m.applyDownload(message, active)
	}
	return Result{}
}

// Render draws profiler history.
func (m Model) Render(width, height int) string { return m.renderProfiler(width, height) }

// Error returns the current profiler request error.
func (m Model) Error() error { return m.profilerErr }

// Process returns the selected JVM process.
func (m Model) Process() flink.ProcessRef { return m.process }

func (m *Model) applyStart(message startMsg) Result {
	if message.generation != m.observabilityGeneration || message.process != m.process {
		return Result{}
	}
	m.profilerErr = message.err
	if message.err != nil {
		m.profilerAction = ""
		m.profilerPendingAt = time.Time{}
		m.profilerPendingRuns = nil
		m.profilerMessage = ""
		m.profilerMessageStatus = ""
		return Result{}
	}
	m.profilerMessageStatus = message.entry.Status
	m.profilerMessage = fmt.Sprintf("%s %s", profilerModeLabel(message.entry.Mode), strings.ToLower(shared.Fallback(message.entry.Message, message.entry.Status)))
	if message.entry.Status == "RUNNING" {
		m.profilerAction = "waiting"
		m.profilerPendingAt = message.entry.TriggeredAt
	} else {
		m.profilerAction = ""
		m.profilerPendingAt = time.Time{}
		m.profilerPendingRuns = nil
	}
	return Result{Command: m.fetchProfilerList()}
}

func (m *Model) applyDownload(message downloadMsg, active bool) Result {
	if message.generation != m.observabilityGeneration || message.process != m.process {
		return Result{}
	}
	m.profilerAction = ""
	m.profilerErr = message.err
	if message.err != nil {
		return Result{}
	}
	m.profilerReportName = message.filename
	m.profilerMessageStatus = "FINISHED"
	m.profilerMessage = "Opened " + message.filename + " in the terminal"
	result := Result{Process: message.process, ReportName: message.filename, Graph: message.graph}
	if !active {
		result.Notice = "Profiler report is ready; return to Profiler to open it."
		return result
	}
	result.Intent = IntentReport
	result.Command = tea.ClearScreen
	return result
}

func (m *Model) openProcessLogs(process flink.ProcessRef) tea.Cmd {
	m.pendingIntent, m.pendingProcess = IntentLogs, process
	return nil
}

func (m *Model) openStdout(process flink.ProcessRef) tea.Cmd {
	m.pendingIntent, m.pendingProcess = IntentStdout, process
	return nil
}

func (m *Model) openThreadDump(process flink.ProcessRef) tea.Cmd {
	m.pendingIntent, m.pendingProcess = IntentThreadDump, process
	return nil
}

func (m *Model) setNotice(message string) { m.pendingNotice = message }

func (m Model) requestTimeout(timeout time.Duration) (context.Context, context.CancelFunc) {
	parent := m.parent
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, timeout)
}

func (m Model) bodyHeight() int              { return max(3, m.height-headerHeight-footerHeight) }
func (m Model) contentWidthAt(width int) int { return width }
