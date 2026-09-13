package sqlworkbench

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/nikitasavinov/flink-tui/internal/flink"
	"github.com/nikitasavinov/flink-tui/internal/ui/shared"
)

const (
	defaultStatement       = "SELECT 1 AS answer;"
	maxRows                = 500
	pollInterval           = 350 * time.Millisecond
	requestTimeout         = 10 * time.Second
	executeRequestTimeout  = 15 * time.Second
	shutdownRequestTimeout = 1500 * time.Millisecond
)

// Focus identifies which half of the workbench owns keyboard input.
type Focus uint8

const (
	FocusEditor Focus = iota
	FocusResults
)

// Intent asks the parent application to perform cross-screen behavior. The
// workbench never imports or mutates the root UI coordinator.
type Intent uint8

const (
	IntentNone Intent = iota
	IntentOverview
	IntentPalette
)

// KeyResult combines a workbench command with an optional application intent.
type KeyResult struct {
	Command tea.Cmd
	Intent  Intent
}

// Message is a sealed family of asynchronous workbench replies. The parent UI
// routes these messages back to Model.Apply without knowing their payloads.
type Message interface {
	sqlWorkbenchMessage()
}

type sessionMsg struct {
	handle     string
	info       flink.SQLGatewayInfo
	err        error
	generation uint64
}

func (sessionMsg) sqlWorkbenchMessage() {}

type executeMsg struct {
	operation string
	err       error
	session   string
	// Retain a completed handle when cleanup fails so another run retries it.
	previousResultType string
}

func (executeMsg) sqlWorkbenchMessage() {}

type resultMsg struct {
	result       flink.SQLResult
	err          error
	session      string
	operation    string
	requestedURI string
}

func (resultMsg) sqlWorkbenchMessage() {}

type pollMsg struct {
	session   string
	operation string
	nextURI   string
}

func (pollMsg) sqlWorkbenchMessage() {}

type cancelMsg struct {
	status    string
	err       error
	session   string
	operation string
}

func (cancelMsg) sqlWorkbenchMessage() {}

// State is a read-only snapshot for shell integration and black-box tests.
// Slice fields are cloned so callers cannot mutate workbench-owned state.
type State struct {
	Configured  bool
	Endpoint    string
	Focus       Focus
	Editing     bool
	Text        string
	Session     string
	SessionBusy bool
	Info        flink.SQLGatewayInfo
	Busy        bool
	Err         error
	Operation   string
	ResultType  string
	ResultKind  string
	JobID       string
	Columns     []flink.SQLColumn
	Rows        []flink.SQLRow
	NextURI     string
}

// Model owns all mutable SQL workbench state.
type Model struct {
	client      *flink.SQLGatewayClient
	parent      context.Context
	formatError func(error) string

	focus   Focus
	editing bool
	text    string
	cursor  int

	session           string
	sessionBusy       bool
	sessionGeneration uint64
	sessionReply      <-chan sessionMsg
	pendingStatement  string
	info              flink.SQLGatewayInfo

	busy       bool
	canceling  bool
	err        error
	operation  string
	resultType string
	resultKind string
	jobID      string
	columns    []flink.SQLColumn
	rows       []flink.SQLRow
	nextURI    string
	selection  shared.Cursor
}

// New creates a workbench tied to the application's cancellable request tree.
func New(client *flink.SQLGatewayClient, parent context.Context, formatError func(error) string) Model {
	return Model{
		client:      client,
		parent:      parent,
		formatError: formatError,
		focus:       FocusEditor,
		text:        defaultStatement,
		cursor:      len([]rune(defaultStatement)),
	}
}

// Configured reports whether a SQL Gateway client is available.
func (m Model) Configured() bool { return m.client != nil }

// State returns a detached view of the current workbench state.
func (m Model) State() State {
	rows := make([]flink.SQLRow, len(m.rows))
	for index, row := range m.rows {
		rows[index] = flink.SQLRow{Kind: row.Kind, Fields: slices.Clone(row.Fields)}
	}
	state := State{
		Configured: m.client != nil,
		Focus:      m.focus, Editing: m.editing, Text: m.text, Session: m.session, SessionBusy: m.sessionBusy,
		Info: m.info, Busy: m.busy, Err: m.err, Operation: m.operation,
		ResultType: m.resultType, ResultKind: m.resultKind, JobID: m.jobID,
		Columns: slices.Clone(m.columns), Rows: rows, NextURI: m.nextURI,
	}
	if m.client != nil {
		state.Endpoint = m.client.Endpoint()
	}
	return state
}

// Error returns the current SQL Gateway or operation error.
func (m Model) Error() error { return m.err }

// CapturesKeys reports whether the editor is visibly in insert mode.
func (m Model) CapturesKeys() bool { return m.editing }

// Open focuses the editor and lazily opens a Gateway session.
func (m *Model) Open() tea.Cmd {
	m.focus = FocusEditor
	m.editing = false
	if m.client == nil {
		m.err = errors.New("SQL Gateway endpoint is not configured")
		return nil
	}
	return m.Refresh()
}

// Refresh opens the session when needed or retries a failed result page without
// executing the statement again. Successful streams follow nextResultUri.
func (m *Model) Refresh() tea.Cmd {
	if m.client == nil || m.sessionBusy {
		return nil
	}
	if m.session != "" {
		if m.operation != "" && m.resultType == "ERROR" && !m.busy && !m.canceling {
			m.busy, m.err, m.resultType = true, nil, "RUNNING"
			return m.fetchResults(m.session, m.operation, m.nextURI)
		}
		return nil
	}
	return m.startSession()
}

func (m *Model) startSession() tea.Cmd {
	m.sessionGeneration++
	m.sessionBusy = true
	m.err = nil
	reply := make(chan sessionMsg, 1)
	m.sessionReply = reply
	return m.openSession(reply)
}

func (m Model) openSession(reply chan<- sessionMsg) tea.Cmd {
	client := m.client
	generation := m.sessionGeneration
	return func() tea.Msg {
		ctx, cancel := m.withTimeout(requestTimeout)
		defer cancel()
		info, infoErr := client.Info(ctx)
		if infoErr != nil {
			message := sessionMsg{err: infoErr, generation: generation}
			reply <- message
			return message
		}
		handle, err := client.OpenSession(ctx, "flink-tui", nil)
		message := sessionMsg{handle: handle, info: info, err: err, generation: generation}
		reply <- message
		return message
	}
}

func (m *Model) executeStatement() tea.Cmd {
	if m.client == nil {
		m.err = errors.New("SQL Gateway endpoint is not configured")
		return nil
	}
	statement := m.text
	if strings.TrimSpace(statement) == "" {
		m.err = errors.New("SQL statement is empty")
		return nil
	}
	if m.busy || m.operationActive() {
		m.err = errors.New("an operation is still active; press ctrl+x to cancel it")
		return nil
	}
	if m.session == "" {
		m.pendingStatement = statement
		m.busy = true
		m.err = nil
		m.resultType = "CONNECTING"
		if !m.sessionBusy {
			return m.startSession()
		}
		return nil
	}
	return m.submitStatement(statement)
}

func (m *Model) submitStatement(statement string) tea.Cmd {
	previousOperation, previousResultType := m.operation, m.resultType
	m.pendingStatement = ""
	m.busy = true
	m.err = nil
	m.operation = ""
	m.resultType = "SUBMITTING"
	m.resultKind = ""
	m.jobID = ""
	m.columns = nil
	m.rows = nil
	m.nextURI = ""
	m.selection.Set(0, 0)
	client := m.client
	session := m.session
	withTimeout := m.withTimeout
	return func() tea.Msg {
		ctx, cancel := withTimeout(executeRequestTimeout)
		defer cancel()
		if previousOperation != "" {
			if err := client.CloseOperation(ctx, session, previousOperation); err != nil {
				return executeMsg{
					err: fmt.Errorf("close previous SQL operation: %w", err), session: session,
					operation: previousOperation, previousResultType: previousResultType,
				}
			}
		}
		operation, err := client.Execute(ctx, session, statement)
		return executeMsg{operation: operation, err: err, session: session}
	}
}

func (m Model) fetchResults(session, operation, nextURI string) tea.Cmd {
	client := m.client
	return func() tea.Msg {
		ctx, cancel := m.withTimeout(requestTimeout)
		defer cancel()
		result, err := client.FetchResults(ctx, session, operation, nextURI)
		return resultMsg{result: result, err: err, session: session, operation: operation, requestedURI: nextURI}
	}
}

func (m *Model) cancelOperation() tea.Cmd {
	if m.pendingStatement != "" && m.operation == "" {
		m.pendingStatement = ""
		m.busy = false
		m.resultType = "CANCELED"
		m.nextURI = ""
		return nil
	}
	if m.busy && m.operation == "" && (m.resultType == "SUBMITTING" || m.resultType == "CANCELING") {
		// The server has not returned an operation handle yet. Remember the
		// request and cancel as soon as the accepted handle becomes available.
		m.canceling = true
		m.resultType = "CANCELING"
		return nil
	}
	if m.client == nil || m.session == "" || m.operation == "" || !m.operationActive() || m.canceling {
		return nil
	}
	return m.startCancellation()
}

func (m *Model) startCancellation() tea.Cmd {
	m.busy = true
	m.canceling = true
	m.err = nil
	client := m.client
	session, operation := m.session, m.operation
	withTimeout := m.withTimeout
	return func() tea.Msg {
		ctx, cancel := withTimeout(requestTimeout)
		defer cancel()
		status, err := client.CancelOperation(ctx, session, operation)
		return cancelMsg{status: status, err: err, session: session, operation: operation}
	}
}

func (m Model) operationActive() bool {
	if m.operation == "" {
		return false
	}
	switch m.resultType {
	case "EOS", "CANCELED", "CLOSED":
		return false
	default:
		// A failed result request does not terminate the remote operation.
		// Keep cancellation available before allowing another statement.
		return true
	}
}

// Close returns a best-effort cleanup command for application shutdown.
func (m Model) Close() tea.Cmd {
	if m.client == nil || (m.session == "" && m.sessionReply == nil) {
		return nil
	}
	client := m.client
	session := m.session
	operation := m.operation
	pendingSession := m.sessionReply
	return func() tea.Msg {
		if session == "" {
			// A successful open may already be queued behind the quit key. Read
			// its handle without waiting for another event-loop update.
			timer := time.NewTimer(shutdownRequestTimeout)
			defer timer.Stop()
			select {
			case reply := <-pendingSession:
				session = reply.handle
			case <-timer.C:
			}
			if session == "" {
				return nil
			}
		}
		if operation != "" {
			ctx, cancel := shutdownContext()
			_ = client.CloseOperation(ctx, session, operation)
			cancel()
		}
		ctx, cancel := shutdownContext()
		_ = client.CloseSession(ctx, session)
		cancel()
		return nil
	}
}

func (m Model) withTimeout(timeout time.Duration) (context.Context, context.CancelFunc) {
	parent := m.parent
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, timeout)
}

// Cleanup deliberately outlives the application's request tree so quit can
// still close the operation and session after regular requests are canceled.
func shutdownContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), shutdownRequestTimeout)
}
