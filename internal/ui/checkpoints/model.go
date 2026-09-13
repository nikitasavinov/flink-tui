package checkpoints

import (
	"context"
	"fmt"
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
)

// View identifies a screen owned by the checkpoint workspace.
type View uint8

const (
	ViewHistory View = iota
	ViewOperators
	ViewSubtasks
)

// Intent asks the application shell to leave the checkpoint workspace.
type Intent uint8

const (
	IntentNone Intent = iota
	IntentGraph
	IntentOverview
)

// Context is the job-scoped data consumed by the workspace.
type Context struct {
	JobID      string
	Summary    flink.CheckpointSummary
	Nodes      []flink.Node
	Order      []string
	UpdatedAt  time.Time
	RefreshErr error
	Generation uint64
	BodyHeight int
}

// Result combines a module command with optional cross-screen navigation.
type Result struct {
	Command tea.Cmd
	Intent  Intent
	Vertex  string
	View    View
	Notice  string
	Peer    *PeerChange
	// PeerFallback reports that a subtask comparison had to collapse to its
	// operator parent because the target checkpoint could not support it.
	PeerFallback bool
}

// PeerChange describes one lateral checkpoint comparison without coupling the
// checkpoint workspace to the shell's notice presentation.
type PeerChange struct {
	PreviousID       int64
	ID               int64
	PreviousPosition int
	Position         int
	Total            int
}

// Message is the sealed family of checkpoint REST replies.
type Message interface {
	checkpointMessage()
}

type checkpointDetailMsg struct {
	details      flink.CheckpointDetails
	config       flink.CheckpointConfig
	err          error
	configErr    error
	jobID        string
	checkpointID int64
	generation   uint64
	requestID    uint64
}

func (checkpointDetailMsg) checkpointMessage() {}

type checkpointSubtasksMsg struct {
	details      flink.CheckpointSubtaskDetails
	err          error
	jobID        string
	checkpointID int64
	vertexID     string
	generation   uint64
	requestID    uint64
}

func (checkpointSubtasksMsg) checkpointMessage() {}

type snapshotState struct {
	JobID       string
	Nodes       []flink.Node
	Checkpoints flink.CheckpointSummary
	UpdatedAt   time.Time
}

type checkpointRequestKey struct {
	jobID        string
	generation   uint64
	checkpointID int64
	vertexID     string
}

type layoutState struct {
	Order []string
	Rects map[string]struct{}
}

// State is a detached snapshot for shell policy, error display, and tests.
type State struct {
	View             View
	Page             checkpointPage
	Cursor           int
	SelectedID       int64
	Detail           flink.CheckpointDetails
	Config           flink.CheckpointConfig
	DetailID         int64
	DetailBusy       bool
	DetailErr        error
	ConfigErr        error
	OperatorCursor   int
	OperatorSelected string
	OperatorSort     checkpointOperatorSort
	ConfigOpen       bool
	Subtasks         flink.CheckpointSubtaskDetails
	SubtasksBusy     bool
	SubtasksErr      error
	SubtaskVertex    string
	SubtaskCursor    int
	SubtaskSelected  int
	SubtaskSort      checkpointSubtaskSort
	PendingIntent    Intent
	PendingVertex    string
	PeerSubtasks     bool
	PeerVertex       string
	Generation       uint64
	JobID            string
	Summary          flink.CheckpointSummary
	Nodes            []flink.Node
	Order            []string
	UpdatedAt        time.Time
	RefreshErr       error
	BodyHeight       int
	HistoryQuery     string
	HistorySearch    bool
	OperatorQuery    string
	OperatorSearch   bool
}

// Model owns all checkpoint workspace state.
type Model struct {
	client *flink.Client
	parent context.Context

	view           View
	snapshot       snapshotState
	layout         layoutState
	height         int
	err            error
	generation     uint64
	detailRequest  shared.RequestGate[checkpointRequestKey]
	subtaskRequest shared.RequestGate[checkpointRequestKey]

	pendingIntent Intent
	pendingVertex string

	checkpointCursor           int
	checkpointSelectedID       int64
	checkpointPage             checkpointPage
	checkpointDetail           flink.CheckpointDetails
	checkpointConfig           flink.CheckpointConfig
	checkpointDetailID         int64
	checkpointDetailBusy       bool
	checkpointDetailErr        error
	checkpointConfigErr        error
	checkpointOperatorCursor   int
	checkpointOperatorSelected string
	checkpointOperatorSort     checkpointOperatorSort
	checkpointConfigOpen       bool
	checkpointSubtasks         flink.CheckpointSubtaskDetails
	checkpointSubtasksBusy     bool
	checkpointSubtasksErr      error
	checkpointSubtaskVertex    string
	checkpointSubtaskCursor    int
	checkpointSubtaskSelected  int
	checkpointSubtaskSort      checkpointSubtaskSort
	checkpointPeerSubtasks     bool
	checkpointPeerVertex       string
	checkpointHistoryFilter    shellmodule.QueryInput
	checkpointOperatorFilter   shellmodule.QueryInput
}

// New creates an empty checkpoint workspace.
func New(client *flink.Client, parent context.Context) Model {
	return Model{
		client:                    client,
		parent:                    parent,
		view:                      ViewHistory,
		checkpointSubtaskSelected: -1,
	}
}

// Sync updates job data while preserving identity-pinned selections.
func (m *Model) Sync(value Context) {
	if m.snapshot.JobID != "" && value.JobID != m.snapshot.JobID {
		m.Reset()
	}
	m.snapshot = snapshotState{
		JobID:       value.JobID,
		Nodes:       append([]flink.Node(nil), value.Nodes...),
		Checkpoints: cloneCheckpointSummary(value.Summary),
		UpdatedAt:   value.UpdatedAt,
	}
	m.layout.Order = append([]string(nil), value.Order...)
	m.layout.Rects = make(map[string]struct{}, len(value.Nodes))
	for _, node := range value.Nodes {
		m.layout.Rects[node.ID] = struct{}{}
	}
	m.err = value.RefreshErr
	m.generation = value.Generation
	if value.BodyHeight > 0 {
		m.height = value.BodyHeight
	}
	m.syncCheckpointSelection()
}

// Reset drops all job-scoped checkpoint state.
func (m *Model) Reset() {
	client, parent := m.client, m.parent
	detailRequest, subtaskRequest := m.detailRequest, m.subtaskRequest
	detailRequest.Reset()
	subtaskRequest.Reset()
	*m = New(client, parent)
	m.detailRequest, m.subtaskRequest = detailRequest, subtaskRequest
}

// Open activates the checkpoint history screen.
func (m *Model) Open(value Context) {
	m.Sync(value)
	m.openCheckpoints()
}

// Activate selects a checkpoint-owned screen without fetching.
func (m *Model) Activate(view View) {
	switch view {
	case ViewOperators:
		m.view = ViewOperators
	case ViewSubtasks:
		m.view = ViewSubtasks
	default:
		m.view = ViewHistory
	}
}

// CurrentView reports the active checkpoint-owned screen.
func (m Model) CurrentView() View { return m.view }

// CapturesKeys reports whether a checkpoint table filter owns printable input.
func (m Model) CapturesKeys() bool {
	switch m.view {
	case ViewHistory:
		return m.checkpointHistoryFilter.Active()
	case ViewOperators:
		return m.checkpointOperatorFilter.Active()
	default:
		return false
	}
}

// HandleKey processes input for the active checkpoint view.
func (m *Model) HandleKey(key string, bodyHeight int) Result {
	if bodyHeight > 0 {
		m.height = bodyHeight
	}
	m.pendingIntent, m.pendingVertex = IntentNone, ""
	var command tea.Cmd
	switch m.view {
	case ViewOperators:
		command = m.handleCheckpointOperatorKey(key)
	case ViewSubtasks:
		command = m.handleCheckpointSubtaskKey(key)
	default:
		command = m.handleCheckpointKey(key)
	}
	return m.result(command)
}

// HandleClick processes an application-coordinate mouse click.
func (m *Model) HandleClick(event tea.Mouse, bodyHeight int) Result {
	if bodyHeight > 0 {
		m.height = bodyHeight
	}
	m.pendingIntent, m.pendingVertex = IntentNone, ""
	switch m.view {
	case ViewOperators:
		m.handleCheckpointOperatorMouseClick(event)
	case ViewSubtasks:
		m.handleCheckpointSubtaskMouseClick(event)
	default:
		m.handleCheckpointMouseClick(event)
	}
	return m.result(nil)
}

// HandleWheel moves the active selection.
func (m *Model) HandleWheel(delta, bodyHeight int) {
	if bodyHeight > 0 {
		m.height = bodyHeight
	}
	switch m.view {
	case ViewOperators:
		m.moveCheckpointOperatorSelection(delta)
	case ViewSubtasks:
		m.moveCheckpointSubtaskSelection(delta)
	default:
		m.moveCheckpointSelection(delta)
	}
}

// Refresh reloads detail data for diagnostic screens. History refresh remains
// owned by the parent snapshot pipeline.
func (m *Model) Refresh() tea.Cmd {
	switch m.view {
	case ViewOperators:
		m.checkpointDetailBusy = true
		m.checkpointDetailErr = nil
		m.checkpointConfigErr = nil
		return m.fetchCheckpointDetail()
	case ViewSubtasks:
		m.checkpointSubtasksBusy = true
		m.checkpointSubtasksErr = nil
		return m.fetchCheckpointSubtasks()
	default:
		return nil
	}
}

// RefreshOperators reloads checkpoint detail and configuration regardless of
// which checkpoint-owned screen the shell is currently composing.
func (m *Model) RefreshOperators() tea.Cmd {
	m.checkpointDetailBusy = true
	m.checkpointDetailErr = nil
	m.checkpointConfigErr = nil
	return m.fetchCheckpointDetail()
}

// RefreshSubtasks reloads the selected operator's subtask checkpoint data.
func (m *Model) RefreshSubtasks() tea.Cmd {
	m.checkpointSubtasksBusy = true
	m.checkpointSubtasksErr = nil
	return m.fetchCheckpointSubtasks()
}

// Apply reduces a checkpoint REST reply.
func (m *Model) Apply(message Message) Result {
	// Navigation intents belong to the input event that produced them. A late
	// reply must not replay a prior graph/overview jump, even when it is stale.
	m.pendingIntent, m.pendingVertex = IntentNone, ""
	result := Result{}
	switch message := message.(type) {
	case checkpointDetailMsg:
		if message.generation != m.generation || message.jobID != m.snapshot.JobID || message.checkpointID != m.checkpointDetailID ||
			!m.detailRequest.Finish(message.requestID) {
			return m.result(nil)
		}
		m.checkpointDetailBusy = false
		m.checkpointDetailErr = message.err
		m.checkpointConfigErr = message.configErr
		if message.err == nil {
			m.checkpointDetail = message.details
			m.syncCheckpointOperatorSelection()
		}
		if message.configErr == nil {
			m.checkpointConfig = message.config
		}
		if m.checkpointPeerSubtasks {
			m.checkpointPeerSubtasks = false
			vertexID := m.checkpointPeerVertex
			m.checkpointPeerVertex = ""
			if message.err != nil {
				m.view = ViewOperators
				result.Notice = fmt.Sprintf("Could not compare checkpoint #%d subtasks; showing operators.", m.checkpointDetailID)
				result.PeerFallback = true
				break
			}
			if !checkpointContainsOperator(message.details, vertexID) {
				m.view = ViewOperators
				result.Notice = fmt.Sprintf("Checkpoint #%d has no %s operator data; showing operators.",
					m.checkpointDetailID, m.nodeName(vertexID))
				result.PeerFallback = true
				break
			}
			m.view = ViewSubtasks
			m.checkpointSubtaskVertex = vertexID
			m.checkpointSubtasks = flink.CheckpointSubtaskDetails{}
			m.checkpointSubtasksBusy = true
			m.checkpointSubtasksErr = nil
			result.Command = m.fetchCheckpointSubtasks()
		}
	case checkpointSubtasksMsg:
		if message.generation != m.generation || message.jobID != m.snapshot.JobID ||
			message.checkpointID != m.checkpointDetailID || message.vertexID != m.checkpointSubtaskVertex ||
			!m.subtaskRequest.Finish(message.requestID) {
			return m.result(nil)
		}
		m.checkpointSubtasksBusy = false
		m.checkpointSubtasksErr = message.err
		if message.err == nil {
			m.checkpointSubtasks = message.details
			m.syncCheckpointSubtaskSelection()
		}
	}
	base := m.result(result.Command)
	base.Notice = result.Notice
	base.PeerFallback = result.PeerFallback
	return base
}

// Render renders the active checkpoint-owned screen.
func (m Model) Render(width, height int) string {
	m.height = height
	switch m.view {
	case ViewOperators:
		return m.renderCheckpointOperators(width, height)
	case ViewSubtasks:
		return m.renderCheckpointSubtasks(width, height)
	default:
		return m.renderCheckpoints(width, height)
	}
}

// State returns a detached workspace snapshot.
func (m Model) State() State {
	return State{
		View: m.CurrentView(), Page: m.checkpointPage, Cursor: m.checkpointCursor,
		SelectedID: m.checkpointSelectedID, Detail: cloneCheckpointDetails(m.checkpointDetail),
		Config: m.checkpointConfig, DetailID: m.checkpointDetailID, DetailBusy: m.checkpointDetailBusy,
		DetailErr: m.checkpointDetailErr, ConfigErr: m.checkpointConfigErr,
		OperatorCursor: m.checkpointOperatorCursor, OperatorSelected: m.checkpointOperatorSelected,
		OperatorSort: m.checkpointOperatorSort, ConfigOpen: m.checkpointConfigOpen,
		Subtasks: cloneCheckpointSubtasks(m.checkpointSubtasks), SubtasksBusy: m.checkpointSubtasksBusy,
		SubtasksErr: m.checkpointSubtasksErr, SubtaskVertex: m.checkpointSubtaskVertex,
		SubtaskCursor: m.checkpointSubtaskCursor, SubtaskSelected: m.checkpointSubtaskSelected,
		SubtaskSort: m.checkpointSubtaskSort, PendingIntent: m.pendingIntent, PendingVertex: m.pendingVertex,
		PeerSubtasks: m.checkpointPeerSubtasks, PeerVertex: m.checkpointPeerVertex,
		Generation: m.generation, JobID: m.snapshot.JobID,
		Summary: cloneCheckpointSummary(m.snapshot.Checkpoints), Nodes: append([]flink.Node(nil), m.snapshot.Nodes...),
		Order: append([]string(nil), m.layout.Order...), UpdatedAt: m.snapshot.UpdatedAt,
		RefreshErr: m.err, BodyHeight: m.height,
		HistoryQuery: m.checkpointHistoryFilter.Value(), HistorySearch: m.checkpointHistoryFilter.Active(),
		OperatorQuery: m.checkpointOperatorFilter.Value(), OperatorSearch: m.checkpointOperatorFilter.Active(),
	}
}

// RestoreState hydrates a complete detached workspace snapshot.
func (m *Model) RestoreState(state State) {
	m.Sync(Context{
		JobID: state.JobID, Summary: state.Summary, Nodes: state.Nodes, Order: state.Order,
		UpdatedAt: state.UpdatedAt, RefreshErr: state.RefreshErr,
		Generation: state.Generation, BodyHeight: state.BodyHeight,
	})
	m.Activate(state.View)
	m.checkpointPage = state.Page
	m.checkpointCursor = state.Cursor
	m.checkpointSelectedID = state.SelectedID
	m.checkpointDetail = cloneCheckpointDetails(state.Detail)
	m.checkpointConfig = state.Config
	m.checkpointDetailID = state.DetailID
	m.checkpointDetailBusy = state.DetailBusy
	m.checkpointDetailErr = state.DetailErr
	m.checkpointConfigErr = state.ConfigErr
	m.checkpointOperatorCursor = state.OperatorCursor
	m.checkpointOperatorSelected = state.OperatorSelected
	m.checkpointOperatorSort = state.OperatorSort
	m.checkpointConfigOpen = state.ConfigOpen
	m.checkpointSubtasks = cloneCheckpointSubtasks(state.Subtasks)
	m.checkpointSubtasksBusy = state.SubtasksBusy
	m.checkpointSubtasksErr = state.SubtasksErr
	m.checkpointSubtaskVertex = state.SubtaskVertex
	m.checkpointSubtaskCursor = state.SubtaskCursor
	m.checkpointSubtaskSelected = state.SubtaskSelected
	m.checkpointSubtaskSort = state.SubtaskSort
	m.checkpointPeerSubtasks = state.PeerSubtasks
	m.checkpointPeerVertex = state.PeerVertex
	m.pendingIntent = state.PendingIntent
	m.pendingVertex = state.PendingVertex
	m.checkpointHistoryFilter.Restore(shellmodule.QueryState{Value: state.HistoryQuery, Open: state.HistorySearch})
	m.checkpointOperatorFilter.Restore(shellmodule.QueryState{Value: state.OperatorQuery, Open: state.OperatorSearch})
}

func (m Model) result(command tea.Cmd) Result {
	return Result{Command: command, View: m.CurrentView(), Intent: m.pendingIntent, Vertex: m.pendingVertex}
}

func checkpointContainsOperator(details flink.CheckpointDetails, vertexID string) bool {
	for _, operator := range details.Operators {
		if operator.VertexID == vertexID {
			return true
		}
	}
	return false
}

func (m Model) requestTimeout(timeout time.Duration) (context.Context, context.CancelFunc) {
	parent := m.parent
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, timeout)
}

func (m Model) bodyHeight() int { return max(3, m.height) }

func (m Model) nodeName(id string) string {
	for _, node := range m.snapshot.Nodes {
		if node.ID == id {
			return node.Name
		}
	}
	return shared.ShortID(id)
}

func cloneCheckpointSummary(value flink.CheckpointSummary) flink.CheckpointSummary {
	value.History = append([]flink.Checkpoint(nil), value.History...)
	if value.LatestFailed != nil {
		latest := *value.LatestFailed
		value.LatestFailed = &latest
	}
	return value
}

func cloneCheckpointDetails(value flink.CheckpointDetails) flink.CheckpointDetails {
	value.Operators = append([]flink.CheckpointOperator(nil), value.Operators...)
	return value
}

func cloneCheckpointSubtasks(value flink.CheckpointSubtaskDetails) flink.CheckpointSubtaskDetails {
	value.Subtasks = append([]flink.CheckpointSubtask(nil), value.Subtasks...)
	return value
}

// Health summarizes the newest checkpoint attempt for the shell's compact
// health line. Lifetime failures do not make a newer successful attempt red.
func Health(summary flink.CheckpointSummary, now time.Time) (string, bool) {
	if summary.Total == 0 && len(summary.History) == 0 {
		return "checkpoints none", false
	}
	if len(summary.History) > 0 {
		latest := summary.History[0]
		status := strings.ToUpper(strings.TrimSpace(latest.Status))
		label := fmt.Sprintf("checkpoint #%d", latest.ID)
		unhealthy := false
		switch status {
		case "COMPLETED":
			if latest.Duration > 0 {
				label += " " + humanLatency(latest.Duration)
			} else {
				label += " completed"
			}
			if size := max(latest.CheckpointedSize, latest.StateSize); size > 0 {
				label += " " + shared.HumanBytes(size)
			}
		case "FAILED":
			label += " failed"
			unhealthy = true
		case "IN_PROGRESS":
			label += " running"
		case "":
			label += " unknown"
			unhealthy = true
		default:
			label += " " + strings.ToLower(status)
			unhealthy = status != "COMPLETED"
		}
		eventAt := latest.CompletedAt
		if eventAt.IsZero() {
			eventAt = latest.TriggeredAt
		}
		if !eventAt.IsZero() {
			label += " " + shortAge(max(time.Duration(0), now.Sub(eventAt))) + " ago"
		}
		return label, unhealthy
	}
	if summary.LatestID == 0 {
		return fmt.Sprintf("checkpoints %d", summary.Total), summary.InProgress > 0
	}
	label := fmt.Sprintf("checkpoint #%d %s", summary.LatestID, humanLatency(summary.LatestDuration))
	if summary.LatestSize > 0 {
		label += " " + shared.HumanBytes(summary.LatestSize)
	}
	if !summary.LatestCompletedAt.IsZero() {
		label += " " + shortAge(max(time.Duration(0), now.Sub(summary.LatestCompletedAt))) + " ago"
	}
	if summary.InProgress > 0 {
		label += fmt.Sprintf(" (%d running)", summary.InProgress)
	}
	return label, false
}

func checkpointHealth(summary flink.CheckpointSummary, now time.Time) (string, bool) {
	return Health(summary, now)
}

func shortAge(age time.Duration) string {
	if age < 0 {
		age = 0
	}
	age = age.Round(time.Second)
	if age < time.Minute {
		return fmt.Sprintf("%ds", int(age.Seconds()))
	}
	if age < time.Hour {
		return fmt.Sprintf("%dm%02ds", int(age.Minutes()), int(age.Seconds())%60)
	}
	return fmt.Sprintf("%dh%02dm", int(age.Hours()), int(age.Minutes())%60)
}
