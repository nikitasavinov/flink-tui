package jobops

import (
	"context"
	"errors"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/nikitasavinov/flink-tui/internal/flink"
	"github.com/nikitasavinov/flink-tui/internal/ui/shared"
	shellmodule "github.com/nikitasavinov/flink-tui/internal/ui/shell"
)

const (
	headerHeight          = 3
	defaultRequestTimeout = 8 * time.Second
	longRequestTimeout    = 15 * time.Second
)

// View identifies a screen owned by the job-operations workspace.
type View uint8

const (
	ViewConfiguration View = iota
	ViewActions
)

// Intent asks the application shell to perform cross-feature behavior.
type Intent uint8

const (
	IntentNone Intent = iota
	IntentGraph
	IntentOverview
	IntentRefreshSnapshot
)

// Context is the job-scoped input supplied by the application shell.
type Context struct {
	JobID      string
	JobState   string
	JobType    string
	Scheduler  string
	Generation uint64
	BodyHeight int
}

// Result combines a module command with optional cross-feature navigation.
type Result struct {
	Command tea.Cmd
	Intent  Intent
}

// Message is the sealed family of job-operation REST replies.
type Message interface {
	jobOperationsMessage()
}

type configurationMsg struct {
	configuration flink.JobConfiguration
	err           error
	jobID         string
	generation    uint64
	requestID     uint64
}

type configurationRequest struct {
	jobID      string
	generation uint64
}

func (configurationMsg) jobOperationsMessage() {}

type actionKind uint8

const (
	actionConfiguredCheckpoint actionKind = iota
	actionFullCheckpoint
	actionSavepoint
	actionStopSavepoint
	actionStopDrain
	actionCancel
)

type actionMsg struct {
	kind       actionKind
	triggerID  string
	err        error
	jobID      string
	generation uint64
}

func (actionMsg) jobOperationsMessage() {}

type actionStatusMsg struct {
	operation  flink.AsyncOperation
	err        error
	jobID      string
	triggerID  string
	generation uint64
}

func (actionStatusMsg) jobOperationsMessage() {}

type jobActionClient interface {
	TriggerCheckpoint(context.Context, string, string) (string, error)
	TriggerSavepoint(context.Context, string, string) (string, error)
	StopWithSavepoint(context.Context, string, bool, string) (string, error)
	CancelJob(context.Context, string) error
}

// State is a detached snapshot used by the shell for policy and tests.
type State struct {
	View                View
	Configuration       flink.JobConfiguration
	ConfigurationBusy   bool
	ConfigurationErr    error
	ConfigurationIndex  int
	ActionIndex         int
	ActionConfirm       bool
	ActionBusy          bool
	ActionTriggerID     string
	ActionStatus        string
	ActionLocation      string
	ActionMessage       string
	ActionErr           error
	ActionKind          uint8
	ConfigurationQuery  string
	ConfigurationSearch bool
}

// Model owns all mutable job-operations state.
type Model struct {
	client  *flink.Client
	parent  context.Context
	context Context
	view    View

	configuration         flink.JobConfiguration
	configurationBusy     bool
	configurationErr      error
	configurationRow      shared.Cursor
	configurationFilter   shellmodule.QueryInput
	configurationRequests shared.RequestGate[configurationRequest]

	actionSelection shared.Cursor
	actionConfirm   bool
	actionBusy      bool
	actionPollBusy  bool
	actionKind      actionKind
	actionTriggerID string
	actionStatus    string
	actionLocation  string
	actionMessage   string
	actionErr       error
}

// New creates an empty job-operations workspace.
func New(client *flink.Client, parent context.Context) Model {
	return Model{client: client, parent: parent}
}

// Sync updates immutable shell-owned job context without replacing local UI
// state.
func (m *Model) Sync(value Context) { m.context = value }

// Reset drops all job-scoped state after the selected job changes.
func (m *Model) Reset() {
	client, parent := m.client, m.parent
	requests := m.configurationRequests
	requests.Reset()
	*m = Model{client: client, parent: parent}
	m.configurationRequests = requests
}

// CurrentView reports the active workspace screen.
func (m Model) CurrentView() View { return m.view }

// State returns a detached state snapshot.
func (m Model) State() State {
	return State{
		View: m.view, Configuration: m.configuration,
		ConfigurationBusy: m.configurationBusy, ConfigurationErr: m.configurationErr,
		ConfigurationIndex: m.configurationRow.Index(), ActionIndex: m.actionSelection.Index(),
		ActionConfirm: m.actionConfirm, ActionBusy: m.actionBusy,
		ActionTriggerID: m.actionTriggerID, ActionStatus: m.actionStatus,
		ActionLocation: m.actionLocation, ActionMessage: m.actionMessage,
		ActionErr: m.actionErr, ActionKind: uint8(m.actionKind),
		ConfigurationQuery: m.configurationFilter.Value(), ConfigurationSearch: m.configurationFilter.Active(),
	}
}

// RestoreState replaces local state while retaining dependencies and context.
// It exists for deterministic shell integration tests.
func (m *Model) RestoreState(state State) {
	m.view = state.View
	m.configuration = state.Configuration
	m.configurationBusy = state.ConfigurationBusy
	m.configurationErr = state.ConfigurationErr
	m.configurationRow.Set(state.ConfigurationIndex, len(m.configurationRows()))
	m.actionSelection.Set(state.ActionIndex, len(m.actionRows()))
	m.actionConfirm = state.ActionConfirm
	m.actionBusy = state.ActionBusy
	m.actionTriggerID = state.ActionTriggerID
	m.actionStatus = state.ActionStatus
	m.actionLocation = state.ActionLocation
	m.actionMessage = state.ActionMessage
	m.actionErr = state.ActionErr
	m.actionKind = actionKind(state.ActionKind)
	m.configurationFilter.Restore(shellmodule.QueryState{Value: state.ConfigurationQuery, Open: state.ConfigurationSearch})
	m.configurationRow.Constrain(len(m.configurationRows()))
}

// OpenConfiguration activates configuration and starts a fetch.
func (m *Model) OpenConfiguration(value Context) tea.Cmd {
	m.Sync(value)
	if m.context.JobID == "" {
		return nil
	}
	m.view = ViewConfiguration
	m.configurationBusy = true
	m.configurationErr = nil
	m.configurationRow.Set(0, len(m.configurationRows()))
	return m.fetchConfiguration()
}

// OpenActions activates guarded operations.
func (m *Model) OpenActions(value Context) {
	m.Sync(value)
	if m.context.JobID == "" {
		return
	}
	m.view = ViewActions
	m.actionConfirm = false
	m.actionSelection.Constrain(len(m.actionRows()))
}

// Activate selects a workspace screen before delegated input or rendering.
func (m *Model) Activate(view View) { m.view = view }

// CapturesKeys reports whether global shortcuts must yield to confirmation or
// an in-flight mutation.
func (m Model) CapturesKeys() bool {
	return m.actionConfirm || m.actionBusy || m.view == ViewConfiguration && m.configurationFilter.Active()
}

// Error reports the error owned by the active workspace view.
func (m Model) Error() error {
	if m.view == ViewActions {
		return m.actionErr
	}
	return m.configurationErr
}

// ActionState reports footer-relevant action state.
func (m Model) ActionState() (confirm, busy bool) { return m.actionConfirm, m.actionBusy }

// HandleKey reduces keyboard input for the active view.
func (m *Model) HandleKey(key string) Result {
	if m.view == ViewActions {
		return m.handleActionKey(key)
	}
	return m.handleConfigurationKey(key)
}

// HandleClick reduces an absolute terminal mouse click.
func (m *Model) HandleClick(event tea.Mouse) Result {
	if m.view == ViewActions {
		m.handleActionClick(event)
	} else {
		m.handleConfigurationClick(event)
	}
	return Result{}
}

// HandleWheel moves the active list selection.
func (m *Model) HandleWheel(delta int) {
	if m.view == ViewActions {
		if m.actionConfirm || m.actionBusy {
			return
		}
		m.actionSelection.Move(delta, len(m.actionRows()))
		return
	}
	m.configurationRow.Move(delta, len(m.configurationRows()))
}

// Refresh refreshes the active view. Actions refresh their job snapshot in the
// shell and continue polling any accepted async operation.
func (m *Model) Refresh() Result {
	if m.view == ViewActions {
		return Result{Command: m.Poll(), Intent: IntentRefreshSnapshot}
	}
	m.configurationBusy = true
	m.configurationErr = nil
	return Result{Command: m.fetchConfiguration()}
}

// Poll returns a command only while an async checkpoint/savepoint operation is
// in progress. It intentionally continues while periodic screen refresh is
// paused.
func (m *Model) Poll() tea.Cmd {
	if m.actionTriggerID == "" || m.actionStatus != "IN_PROGRESS" || m.actionPollBusy {
		return nil
	}
	m.actionPollBusy = true
	client := m.client
	jobID := m.context.JobID
	triggerID := m.actionTriggerID
	kind := m.actionKind
	generation := m.context.Generation
	requestTimeout := m.requestTimeout
	return func() tea.Msg {
		ctx, cancel := requestTimeout(defaultRequestTimeout)
		defer cancel()
		var operation flink.AsyncOperation
		var err error
		if kind == actionConfiguredCheckpoint || kind == actionFullCheckpoint {
			operation, err = client.CheckpointOperation(ctx, jobID, triggerID)
		} else {
			operation, err = client.SavepointOperation(ctx, jobID, triggerID)
		}
		return actionStatusMsg{operation: operation, err: err, jobID: jobID, triggerID: triggerID, generation: generation}
	}
}

// Apply reduces one sealed REST reply.
func (m *Model) Apply(message Message) Result {
	switch message := message.(type) {
	case configurationMsg:
		if message.generation != m.context.Generation || message.jobID != m.context.JobID ||
			!m.configurationRequests.Finish(message.requestID) {
			return Result{}
		}
		m.configurationBusy = false
		m.configurationErr = message.err
		if message.err == nil {
			m.configuration = message.configuration
			m.configurationRow.Constrain(len(m.configurationRows()))
		}
	case actionMsg:
		return m.applyAction(message)
	case actionStatusMsg:
		return m.applyActionStatus(message)
	}
	return Result{}
}

func (m *Model) fetchConfiguration() tea.Cmd {
	client := m.client
	jobID := m.context.JobID
	generation := m.context.Generation
	requestID, start := m.configurationRequests.Begin(configurationRequest{jobID: jobID, generation: generation})
	if !start {
		return nil
	}
	requestTimeout := m.requestTimeout
	return func() tea.Msg {
		ctx, cancel := requestTimeout(defaultRequestTimeout)
		defer cancel()
		configuration, err := client.JobConfiguration(ctx, jobID)
		return configurationMsg{configuration: configuration, err: err, jobID: jobID, generation: generation, requestID: requestID}
	}
}

func (m *Model) performAction(kind actionKind) tea.Cmd {
	requestTimeout := m.requestTimeout
	return performJobActionCommand(m.client, m.context.JobID, m.context.Generation, kind, func() (context.Context, context.CancelFunc) {
		return requestTimeout(longRequestTimeout)
	})
}

func performJobActionCommand(client jobActionClient, jobID string, generation uint64, kind actionKind, requestContext func() (context.Context, context.CancelFunc)) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := requestContext()
		defer cancel()
		var triggerID string
		var err error
		switch kind {
		case actionConfiguredCheckpoint:
			triggerID, err = client.TriggerCheckpoint(ctx, jobID, "CONFIGURED")
		case actionFullCheckpoint:
			triggerID, err = client.TriggerCheckpoint(ctx, jobID, "FULL")
		case actionSavepoint:
			triggerID, err = client.TriggerSavepoint(ctx, jobID, "")
		case actionStopSavepoint:
			triggerID, err = client.StopWithSavepoint(ctx, jobID, false, "")
		case actionStopDrain:
			triggerID, err = client.StopWithSavepoint(ctx, jobID, true, "")
		case actionCancel:
			err = client.CancelJob(ctx, jobID)
		default:
			err = errors.New("unknown job action")
		}
		return actionMsg{kind: kind, triggerID: triggerID, err: err, jobID: jobID, generation: generation}
	}
}

func (m *Model) applyAction(message actionMsg) Result {
	if message.generation != m.context.Generation || message.jobID != m.context.JobID || message.kind != m.actionKind {
		return Result{}
	}
	m.actionBusy = false
	m.actionErr = message.err
	if message.err != nil {
		m.actionMessage = ""
		return Result{}
	}
	m.actionTriggerID = message.triggerID
	if message.triggerID == "" {
		m.actionStatus = "COMPLETED"
		m.actionMessage = "Cancel request accepted by Flink."
		return Result{Intent: IntentRefreshSnapshot}
	}
	m.actionStatus = "IN_PROGRESS"
	m.actionMessage = "Operation accepted; polling Flink for completion."
	return Result{Command: m.Poll()}
}

func (m *Model) applyActionStatus(message actionStatusMsg) Result {
	if message.generation != m.context.Generation || message.jobID != m.context.JobID || message.triggerID != m.actionTriggerID {
		return Result{}
	}
	m.actionPollBusy = false
	m.actionBusy = false
	m.actionErr = message.err
	if message.err != nil {
		return Result{}
	}
	m.actionStatus = message.operation.Status
	m.actionLocation = message.operation.Location
	if message.operation.Failure != "" {
		m.actionErr = errors.New(message.operation.Failure)
		m.actionMessage = ""
	} else if message.operation.Status == "COMPLETED" {
		m.actionMessage = "Operation completed successfully."
	}
	if message.operation.Status != "" && message.operation.Status != "IN_PROGRESS" {
		return Result{Intent: IntentRefreshSnapshot}
	}
	return Result{}
}

func (m Model) requestTimeout(timeout time.Duration) (context.Context, context.CancelFunc) {
	parent := m.parent
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, timeout)
}

func (m *Model) handleConfigurationKey(key string) Result {
	if m.configurationFilter.Active() {
		m.configurationFilter.HandleKey(key)
		m.configurationRow.Set(0, len(m.configurationRows()))
		return Result{}
	}
	rows := m.configurationRows()
	switch key {
	case "esc", "g":
		return Result{Intent: IntentGraph}
	case "up", "k":
		m.configurationRow.Move(-1, len(rows))
	case "down", "j":
		m.configurationRow.Move(1, len(rows))
	case "pgup":
		m.configurationRow.Move(-max(1, m.configurationRowsAvailable()), len(rows))
	case "pgdown":
		m.configurationRow.Move(max(1, m.configurationRowsAvailable()), len(rows))
	case "home":
		m.configurationRow.Set(0, len(rows))
	case "end":
		m.configurationRow.Set(max(0, len(rows)-1), len(rows))
	case "/":
		m.configurationFilter.Open()
	}
	return Result{}
}

func (m *Model) handleActionKey(key string) Result {
	if m.actionConfirm {
		switch key {
		case "y":
			return Result{Command: m.executeSelectedAction()}
		case "n", "q", "esc":
			m.actionConfirm = false
		}
		return Result{}
	}
	if m.actionBusy {
		if key == "esc" {
			return Result{Intent: IntentGraph}
		}
		return Result{}
	}
	rows := m.actionRows()
	switch key {
	case "esc", "g":
		return Result{Intent: IntentGraph}
	case "up", "k":
		m.actionSelection.Move(-1, len(rows))
	case "down", "j":
		m.actionSelection.Move(1, len(rows))
	case "home":
		m.actionSelection.Set(0, len(rows))
	case "end":
		m.actionSelection.Set(max(0, len(rows)-1), len(rows))
	case "enter":
		index := m.actionSelection.Index()
		if index >= 0 && index < len(rows) && rows[index].enabled {
			m.actionConfirm = true
		}
	}
	return Result{}
}

func (m *Model) executeSelectedAction() tea.Cmd {
	rows := m.actionRows()
	index := m.actionSelection.Index()
	if index < 0 || index >= len(rows) || !rows[index].enabled {
		m.actionConfirm = false
		return nil
	}
	action := rows[index]
	m.actionConfirm = false
	m.actionBusy = true
	m.actionErr = nil
	m.actionMessage = "Sending " + action.label + "..."
	m.actionTriggerID = ""
	m.actionStatus = ""
	m.actionLocation = ""
	m.actionPollBusy = false
	m.actionKind = action.kind
	return m.performAction(action.kind)
}

func jobStateActive(state string) bool {
	switch strings.ToUpper(state) {
	case "RUNNING", "CREATED", "RESTARTING", "RECONCILING", "INITIALIZING":
		return true
	default:
		return false
	}
}

func (m Model) configurationRowsAvailable() int {
	return max(1, m.context.BodyHeight-3-3)
}

func (m Model) configurationWindowStart() int {
	return shared.WindowStart(m.configurationRow.Index(), m.configurationRowsAvailable(), len(m.configurationRows()))
}

func (m *Model) handleConfigurationClick(event tea.Mouse) {
	if event.Button != tea.MouseLeft {
		return
	}
	position := event.Y - headerHeight - 3
	if position < 0 || position >= m.configurationRowsAvailable() {
		return
	}
	index := m.configurationWindowStart() + position
	if index >= 0 && index < len(m.configurationRows()) {
		m.configurationRow.Set(index, len(m.configurationRows()))
	}
}

func (m *Model) handleActionClick(event tea.Mouse) {
	if event.Button != tea.MouseLeft || m.actionConfirm || m.actionBusy {
		return
	}
	position := event.Y - headerHeight - 3
	rows := m.actionRows()
	if position >= 0 && position < len(rows) {
		m.actionSelection.Set(position, len(rows))
	}
}

func boolLabel(value bool) string {
	if value {
		return "true"
	}
	return "false"
}

func sensitiveConfigurationKey(key string) bool {
	key = strings.ToLower(key)
	for _, fragment := range []string{"password", "secret", "token", "credential", "access.key", "private.key", "sasl.jaas"} {
		if strings.Contains(key, fragment) {
			return true
		}
	}
	return false
}
