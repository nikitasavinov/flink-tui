package processdiag

import (
	"context"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/nikitasavinov/flink-tui/internal/flink"
	"github.com/nikitasavinov/flink-tui/internal/ui/shared"
	shellmodule "github.com/nikitasavinov/flink-tui/internal/ui/shell"
)

const (
	headerHeight          = 3
	footerHeight          = 1
	defaultRequestTimeout = 8 * time.Second
	longRequestTimeout    = 20 * time.Second
)

type screenMode int

const (
	modeProcessLogs screenMode = -1 - iota
	modeDocument
	modeThreadDump
	modeProfiler
)

// View identifies a screen owned by process diagnostics.
type View uint8

const (
	ViewLogs View = iota
	ViewDocument
	ViewThreadDump
)

// Intent asks the application shell to leave this module.
type Intent uint8

const (
	IntentNone Intent = iota
	IntentBack
	IntentProfiler
)

// Focus identifies the execution thread corresponding to a selected subtask.
type Focus struct {
	Label   string
	Prefix  string
	Matched bool
}

// NewFocus creates a subtask execution-thread focus.
func NewFocus(prefix, label string) Focus {
	return Focus{Prefix: prefix, Label: label}
}

type threadFocusState = Focus

// Source identifies the backing content for a document.
type Source = documentSource

const (
	SourceStatic     = documentStatic
	SourceLog        = documentLog
	SourceCurrentLog = documentCurrentLog
	SourceStdout     = documentStdout
)

// Result combines an optional command with a cross-feature navigation intent.
type Result struct {
	Command   tea.Cmd
	Intent    Intent
	BackToken int
	Process   flink.ProcessRef
	View      View
}

// Message is the sealed family of process-observability REST replies.
type Message interface {
	processMessage()
}

type processLogsMsg struct {
	logs       []flink.LogFile
	err        error
	process    flink.ProcessRef
	generation uint64
}

func (processLogsMsg) processMessage() {}

type documentMsg struct {
	title      string
	content    string
	err        error
	generation uint64
}

func (documentMsg) processMessage() {}

type threadDumpMsg struct {
	threads    []flink.ThreadInfo
	err        error
	process    flink.ProcessRef
	generation uint64
}

func (threadDumpMsg) processMessage() {}

// State is a detached snapshot for shell composition and tests.
type State struct {
	View               View
	Process            flink.ProcessRef
	LogBackToken       int
	ThreadBackToken    int
	Logs               []flink.LogFile
	LogIndex           int
	LogsBusy           bool
	LogsErr            error
	DocumentTitle      string
	DocumentLines      []string
	DocumentOffsetY    int
	DocumentOffsetX    int
	DocumentBackToken  int
	DocumentSource     Source
	DocumentProcess    flink.ProcessRef
	DocumentFilename   string
	DocumentLoading    bool
	DocumentErr        error
	DocumentQuery      string
	DocumentSearch     bool
	DocumentTailOnLoad bool
	Threads            []flink.ThreadInfo
	ThreadCursor       int
	ThreadStackOffset  int
	ThreadQuery        string
	ThreadSearch       bool
	ThreadFocus        Focus
	ThreadBusy         bool
	ThreadErr          error
	Generation         uint64
	BodyHeight         int
	PendingIntent      Intent
}

// Model owns process diagnostic navigation, loading, and presentation state.
type Model struct {
	client *flink.Client
	parent context.Context

	mode   screenMode
	height int

	process                 flink.ProcessRef
	processLogBackMode      screenMode
	threadBackMode          screenMode
	processLogs             []flink.LogFile
	processLogSelection     shared.Cursor
	processBusy             bool
	processErr              error
	document                documentState
	threadDump              []flink.ThreadInfo
	threadCursor            int
	threadStackOffset       int
	threadFilter            shellmodule.QueryInput
	threadFocus             threadFocusState
	threadBusy              bool
	threadErr               error
	observabilityGeneration uint64
	logsGeneration          uint64
	documentGeneration      uint64
	threadGeneration        uint64

	pendingIntent Intent
}

// New creates an empty process diagnostics workspace.
func New(client *flink.Client, parent context.Context) Model {
	return Model{client: client, parent: parent, mode: modeProcessLogs}
}

// Activate selects a process-diagnostic screen without fetching.
func (m *Model) Activate(view View) {
	switch view {
	case ViewDocument:
		m.mode = modeDocument
	case ViewThreadDump:
		m.mode = modeThreadDump
	default:
		m.mode = modeProcessLogs
	}
}

// CurrentView reports the active module-owned screen.
func (m Model) CurrentView() View {
	switch m.mode {
	case modeDocument:
		return ViewDocument
	case modeThreadDump:
		return ViewThreadDump
	default:
		return ViewLogs
	}
}

// OpenLogs opens and refreshes a process log listing.
func (m *Model) OpenLogs(process flink.ProcessRef, backToken int) tea.Cmd {
	return m.openProcessLogs(process, screenMode(backToken))
}

// OpenLog opens a full log document.
func (m *Model) OpenLog(process flink.ProcessRef, log flink.LogFile, backToken int) tea.Cmd {
	return m.openLogDocument(process, log, screenMode(backToken))
}

// OpenCurrentLog opens the process's active log directly and positions the
// document at its tail when the first response arrives.
func (m *Model) OpenCurrentLog(process flink.ProcessRef, backToken, bodyHeight int) tea.Cmd {
	if bodyHeight > 0 {
		m.height = bodyHeight + headerHeight + footerHeight
	}
	return m.openCurrentLog(process, screenMode(backToken))
}

// OpenStdout opens a process stdout document.
func (m *Model) OpenStdout(process flink.ProcessRef, backToken int) tea.Cmd {
	return m.openStdout(process, screenMode(backToken))
}

// OpenStatic opens already-available text without a REST request.
func (m *Model) OpenStatic(title, content string, backToken int) {
	m.openStaticDocument(title, content, screenMode(backToken))
}

// OpenThreadDump opens a process thread dump.
func (m *Model) OpenThreadDump(process flink.ProcessRef, backToken int, focus Focus) tea.Cmd {
	return m.openThreadDumpWithFocus(process, screenMode(backToken), focus)
}

// RetargetLogs opens the same log catalog for another Flink process.
func (m *Model) RetargetLogs(process flink.ProcessRef) tea.Cmd {
	return m.openProcessLogs(process, m.processLogBackMode)
}

// RetargetDocument opens the equivalent process-backed document on another
// JVM. The search term transfers, while offsets and the active match reset
// because the content is different. Static documents have no process peer.
func (m *Model) RetargetDocument(process flink.ProcessRef) (tea.Cmd, bool) {
	query := m.document.filter.Value()
	backMode := m.document.backMode
	filename := m.document.filename
	source := m.document.source
	var command tea.Cmd
	switch source {
	case documentLog:
		command = m.openLogDocument(process, flink.LogFile{Name: filename}, backMode)
	case documentCurrentLog:
		command = m.openCurrentLog(process, backMode)
	case documentStdout:
		command = m.openStdout(process, backMode)
	default:
		return nil, false
	}
	m.process = process
	m.document.filter.Restore(shellmodule.QueryState{Value: query})
	m.processLogs = nil
	m.processLogSelection.Set(0, 0)
	m.processBusy = true
	m.processErr = nil
	return tea.Batch(command, m.fetchProcessLogs()), true
}

// RetargetThreadDump fetches another JVM's threads with the same filter. The
// selected thread and stack offset reset because thread identities differ.
func (m *Model) RetargetThreadDump(process flink.ProcessRef) tea.Cmd {
	query := m.threadFilter.Value()
	command := m.openThreadDump(process, m.threadBackMode)
	m.threadFilter.Restore(shellmodule.QueryState{Value: query})
	m.processLogs = nil
	m.processLogSelection.Set(0, 0)
	m.processBusy = true
	m.processErr = nil
	return tea.Batch(command, m.fetchProcessLogs())
}

// HandleKey processes input for the active view.
func (m *Model) HandleKey(key string, bodyHeight int) Result {
	if bodyHeight > 0 {
		m.height = bodyHeight + headerHeight + footerHeight
	}
	m.pendingIntent = IntentNone
	var command tea.Cmd
	switch m.mode {
	case modeDocument:
		command = m.handleDocumentKey(key)
	case modeThreadDump:
		command = m.handleThreadDumpKey(key)
	default:
		command = m.handleProcessLogKey(key)
	}
	return m.result(command)
}

// HandleClick processes an application-coordinate mouse click.
func (m *Model) HandleClick(event tea.Mouse, bodyHeight int) Result {
	if bodyHeight > 0 {
		m.height = bodyHeight + headerHeight + footerHeight
	}
	m.pendingIntent = IntentNone
	var command tea.Cmd
	switch m.mode {
	case modeThreadDump:
		m.handleThreadMouseClick(event)
	case modeProcessLogs:
		command = m.handleProcessLogMouseClick(event)
	}
	return m.result(command)
}

// HandleWheel scrolls or moves selection in the active view.
func (m *Model) HandleWheel(delta, bodyHeight int) {
	if bodyHeight > 0 {
		m.height = bodyHeight + headerHeight + footerHeight
	}
	switch m.mode {
	case modeDocument:
		m.moveDocumentVertical(delta)
	case modeThreadDump:
		m.moveThreadSelection(delta)
	default:
		m.moveProcessLogSelection(delta)
	}
}

// Refresh reloads remote content for the active view. Static documents are
// intentionally immutable.
func (m *Model) Refresh() tea.Cmd {
	switch m.mode {
	case modeDocument:
		if m.document.source == documentStatic {
			return nil
		}
		m.document.loading = true
		m.document.err = nil
		m.observabilityGeneration++
		return m.fetchDocument()
	case modeThreadDump:
		m.threadBusy = true
		m.threadErr = nil
		m.observabilityGeneration++
		return m.fetchThreadDump()
	default:
		m.processBusy = true
		m.processErr = nil
		m.observabilityGeneration++
		return m.fetchProcessLogs()
	}
}

// Apply reduces a process diagnostic REST reply.
func (m *Model) Apply(message Message) tea.Cmd {
	switch message := message.(type) {
	case processLogsMsg:
		if message.generation != m.logsGeneration || message.process != m.process {
			return nil
		}
		m.processBusy = false
		m.processErr = message.err
		if message.err == nil {
			m.processLogs = append([]flink.LogFile(nil), message.logs...)
			m.processLogSelection.Constrain(len(m.processLogs))
		}
	case documentMsg:
		if message.generation != m.documentGeneration || message.title != m.document.title {
			return nil
		}
		m.document.loading = false
		m.document.err = message.err
		if message.err == nil {
			m.document.lines = documentLines(message.content)
			if m.document.tailOnLoad {
				m.document.offsetY = max(0, len(m.document.lines)-m.documentRowsAvailable())
				m.document.tailOnLoad = false
			} else {
				m.document.offsetY = shared.Clamp(m.document.offsetY, 0, max(0, len(m.document.lines)-m.documentRowsAvailable()))
			}
		}
	case threadDumpMsg:
		if message.generation != m.threadGeneration || message.process != m.process {
			return nil
		}
		m.threadBusy = false
		m.threadErr = message.err
		if message.err == nil {
			m.threadDump = prioritizeThreads(message.threads)
			if m.threadFilter.Value() == "" && m.threadFocus.Prefix != "" {
				m.focusThreadDump()
			} else {
				m.threadCursor = shared.Clamp(m.threadCursor, 0, max(0, len(m.filteredThreads())-1))
				m.threadStackOffset = 0
			}
		}
	}
	return nil
}

// Render renders the active process-diagnostic view.
func (m Model) Render(width, height int) string {
	m.height = height + headerHeight + footerHeight
	switch m.mode {
	case modeDocument:
		return m.renderDocument(width, height)
	case modeThreadDump:
		return m.renderThreadDump(width, height)
	default:
		return m.renderProcessLogs(width, height)
	}
}

// SelectedThread returns the current filtered thread selection.
func (m Model) SelectedThread() (flink.ThreadInfo, bool) { return m.selectedThread() }

// State returns a detached snapshot.
func (m Model) State() State {
	return State{
		View: m.CurrentView(), Process: m.process,
		LogBackToken: int(m.processLogBackMode), ThreadBackToken: int(m.threadBackMode),
		Logs: append([]flink.LogFile(nil), m.processLogs...), LogIndex: m.processLogSelection.Index(),
		LogsBusy: m.processBusy, LogsErr: m.processErr,
		DocumentTitle: m.document.title, DocumentLines: append([]string(nil), m.document.lines...),
		DocumentOffsetY: m.document.offsetY, DocumentOffsetX: m.document.offsetX,
		DocumentBackToken: int(m.document.backMode), DocumentSource: m.document.source,
		DocumentProcess: m.document.process, DocumentFilename: m.document.filename,
		DocumentLoading: m.document.loading, DocumentErr: m.document.err,
		DocumentQuery: m.document.filter.Value(), DocumentSearch: m.document.filter.Active(),
		DocumentTailOnLoad: m.document.tailOnLoad,
		Threads:            append([]flink.ThreadInfo(nil), m.threadDump...), ThreadCursor: m.threadCursor,
		ThreadStackOffset: m.threadStackOffset, ThreadQuery: m.threadFilter.Value(),
		ThreadSearch: m.threadFilter.Active(), ThreadFocus: m.threadFocus,
		ThreadBusy: m.threadBusy, ThreadErr: m.threadErr,
		Generation: m.observabilityGeneration,
		BodyHeight: max(3, m.height-headerHeight-footerHeight), PendingIntent: m.pendingIntent,
	}
}

// RestoreState hydrates a complete detached state snapshot.
func (m *Model) RestoreState(state State) {
	m.Activate(state.View)
	m.process = state.Process
	m.processLogBackMode = screenMode(state.LogBackToken)
	m.threadBackMode = screenMode(state.ThreadBackToken)
	m.processLogs = append([]flink.LogFile(nil), state.Logs...)
	m.processLogSelection.Set(state.LogIndex, len(m.processLogs))
	m.processBusy = state.LogsBusy
	m.processErr = state.LogsErr
	m.document = documentState{
		title: state.DocumentTitle, lines: append([]string(nil), state.DocumentLines...),
		offsetY: state.DocumentOffsetY, offsetX: state.DocumentOffsetX,
		backMode: screenMode(state.DocumentBackToken), source: state.DocumentSource,
		process: state.DocumentProcess, filename: state.DocumentFilename,
		loading: state.DocumentLoading, err: state.DocumentErr,
		tailOnLoad: state.DocumentTailOnLoad,
	}
	m.document.filter.Restore(shellmodule.QueryState{Value: state.DocumentQuery, Open: state.DocumentSearch})
	selectedThreadName := ""
	if state.ThreadCursor >= 0 && state.ThreadCursor < len(state.Threads) {
		selectedThreadName = state.Threads[state.ThreadCursor].Name
	}
	m.threadDump = prioritizeThreads(state.Threads)
	m.threadFilter.Restore(shellmodule.QueryState{Value: state.ThreadQuery, Open: state.ThreadSearch})
	m.threadCursor = 0
	for index, thread := range m.filteredThreads() {
		if thread.Name == selectedThreadName {
			m.threadCursor = index
			break
		}
	}
	m.threadStackOffset = max(0, state.ThreadStackOffset)
	m.threadFocus = state.ThreadFocus
	m.threadBusy = state.ThreadBusy
	m.threadErr = state.ThreadErr
	m.observabilityGeneration = state.Generation
	m.logsGeneration, m.documentGeneration, m.threadGeneration = state.Generation, state.Generation, state.Generation
	m.height = max(3, state.BodyHeight) + headerHeight + footerHeight
	m.pendingIntent = state.PendingIntent
	if m.threadFilter.Value() == "" && m.threadFocus.Prefix != "" && len(m.threadDump) > 0 && !m.threadFocus.Matched {
		m.focusThreadDump()
	}
}

func (m Model) result(command tea.Cmd) Result {
	result := Result{Command: command, View: m.CurrentView(), Process: m.process, Intent: m.pendingIntent}
	if m.mode >= 0 {
		result.Intent = IntentBack
		result.BackToken = int(m.mode)
	}
	return result
}

func (m *Model) openProfiler(process flink.ProcessRef, _ screenMode) tea.Cmd {
	m.process = process
	m.mode = modeProfiler
	m.pendingIntent = IntentProfiler
	return func() tea.Msg { return nil }
}

func (m Model) requestTimeout(timeout time.Duration) (context.Context, context.CancelFunc) {
	parent := m.parent
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, timeout)
}

func (m Model) bodyHeight() int {
	return max(3, m.height-headerHeight-footerHeight)
}

func sanitizeLine(value string) string {
	value = strings.Map(func(char rune) rune {
		switch char {
		case '\n', '\r', 0:
			return ' '
		default:
			if char < 0x20 && char != '\t' {
				return ' '
			}
			return char
		}
	}, value)
	return value
}
