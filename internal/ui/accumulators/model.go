package accumulators

import (
	"context"
	"fmt"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/nikitasavinov/flink-tui/internal/flink"
	"github.com/nikitasavinov/flink-tui/internal/ui/shared"
	shellmodule "github.com/nikitasavinov/flink-tui/internal/ui/shell"
)

const (
	bodyRowStart = 3
	headerHeight = 3
	requestLimit = 8 * time.Second
)

// Context contains shell-owned job and viewport state.
type Context struct {
	Snapshot     flink.Snapshot
	Selected     string
	Generation   uint64
	BodyHeight   int
	ContentWidth int
}

// Intent asks the shell to perform cross-feature navigation.
type Intent uint8

const (
	IntentNone Intent = iota
	IntentGraph
	IntentOverview
	IntentDocument
)

// Result combines a command with a navigation request.
type Result struct {
	Command tea.Cmd
	Intent  Intent
	Title   string
	Content string
}

// Message is the sealed family of accumulator replies.
type Message interface{ accumulatorMessage() }

type reply struct {
	values     flink.VertexAccumulators
	err        error
	jobID      string
	vertexID   string
	generation uint64
	requestID  uint64
}

func (reply) accumulatorMessage() {}

type requestKey struct {
	jobID, vertexID string
	generation      uint64
}

type row struct {
	scope    string
	name     string
	typeName string
	value    string
}

// State is a detached snapshot for shell policy and tests.
type State struct {
	Values     flink.VertexAccumulators
	Rows       int
	Selection  int
	Vertex     string
	Busy       bool
	Err        error
	Query      string
	SearchOpen bool
}

// Model owns accumulator request, selection, and rendering state.
type Model struct {
	client   *flink.Client
	parent   context.Context
	context  Context
	values   flink.VertexAccumulators
	rows     []row
	cursor   shared.Cursor
	vertex   string
	busy     bool
	err      error
	filter   shellmodule.QueryInput
	requests shared.RequestGate[requestKey]
}

// New creates an empty accumulator explorer.
func New(client *flink.Client, parent context.Context) Model {
	return Model{client: client, parent: parent}
}

// Sync updates shell-owned context without replacing explorer state.
func (m *Model) Sync(value Context) { m.context = value }

// Reset drops state tied to the previous job.
func (m *Model) Reset() {
	client, parent := m.client, m.parent
	m.requests.Reset()
	requests := m.requests
	*m = New(client, parent)
	m.requests = requests
}

// Open selects the current vertex and starts loading its accumulators.
func (m *Model) Open(value Context) tea.Cmd {
	m.Sync(value)
	if value.Snapshot.JobID == "" || value.Selected == "" {
		return nil
	}
	if m.vertex != value.Selected || m.values.JobID != value.Snapshot.JobID {
		m.values, m.rows = flink.VertexAccumulators{}, nil
		m.cursor.Set(0, 0)
	}
	m.vertex, m.busy, m.err = value.Selected, true, nil
	return m.fetch()
}

// Refresh reloads the current vertex.
func (m *Model) Refresh() tea.Cmd {
	if m.context.Snapshot.JobID == "" || m.vertex == "" {
		return nil
	}
	m.busy, m.err = true, nil
	return m.fetch()
}

// Poll reloads the current vertex while retaining last-good content.
func (m *Model) Poll() tea.Cmd {
	if m.context.Snapshot.JobID == "" || m.vertex == "" {
		return nil
	}
	return m.fetch()
}

// Apply reduces an identity-checked accumulator reply.
func (m *Model) Apply(message Message) tea.Cmd {
	value, ok := message.(reply)
	if !ok || value.generation != m.context.Generation || value.jobID != m.context.Snapshot.JobID || value.vertexID != m.vertex {
		return nil
	}
	if !m.requests.Finish(value.requestID) {
		return nil
	}
	m.busy, m.err = false, value.err
	if value.err == nil {
		selected := m.selectedIdentity()
		m.values = value.values
		m.rows = flatten(value.values)
		m.restoreSelection(selected)
	}
	return nil
}

// HandleKey reduces keyboard input.
func (m *Model) HandleKey(key string) Result {
	if m.filter.Active() {
		selected := m.selectedIdentity()
		action := m.filter.HandleKey(key)
		m.restoreSelection(selected)
		if action == shellmodule.QueryConfirmed {
			if row, ok := m.selected(); ok {
				return Result{Intent: IntentDocument, Title: "ACCUMULATOR  " + row.scope + " / " + row.name, Content: row.value}
			}
		}
		return Result{}
	}
	switch key {
	case "esc", "g":
		return Result{Intent: IntentGraph}
	case "up", "k":
		m.Move(-1)
	case "down", "j":
		m.Move(1)
	case "pgup":
		m.Move(-max(1, m.rowsAvailable()))
	case "pgdown":
		m.Move(max(1, m.rowsAvailable()))
	case "home":
		m.cursor.Set(0, len(m.rows))
	case "end":
		m.cursor.Set(max(0, len(m.rows)-1), len(m.rows))
	case "enter":
		if selected, ok := m.selected(); ok {
			return Result{Intent: IntentDocument, Title: "ACCUMULATOR  " + selected.scope + " / " + selected.name, Content: selected.value}
		}
	case "/":
		m.filter.Open()
	}
	return Result{}
}

// HandleClick selects a visible row.
func (m *Model) HandleClick(event tea.Mouse) {
	if event.Button != tea.MouseLeft {
		return
	}
	position := event.Y - headerHeight - bodyRowStart
	if position < 0 || position >= m.rowsAvailable() {
		return
	}
	index := m.windowStart() + position
	rows := m.filteredRows()
	if index >= 0 && index < len(rows) {
		m.cursor.Set(index, len(rows))
	}
}

// Move changes the selected accumulator row.
func (m *Model) Move(delta int) { m.cursor.Move(delta, len(m.filteredRows())) }

// CapturesKeys reports whether the accumulator filter owns printable input.
func (m Model) CapturesKeys() bool { return m.filter.Active() }

// Error returns the last request error.
func (m Model) Error() error { return m.err }

// State returns a detached state snapshot.
func (m Model) State() State {
	return State{Values: m.values, Rows: len(m.filteredRows()), Selection: m.cursor.Index(), Vertex: m.vertex, Busy: m.busy, Err: m.err, Query: m.filter.Value(), SearchOpen: m.filter.Active()}
}

// RestoreState replaces state while retaining dependencies and context.
func (m *Model) RestoreState(state State) {
	m.values, m.rows = state.Values, flatten(state.Values)
	m.filter.Restore(shellmodule.QueryState{Value: state.Query, Open: state.SearchOpen})
	m.cursor.Set(state.Selection, len(m.filteredRows()))
	m.vertex, m.busy, m.err = state.Vertex, state.Busy, state.Err
}

func (m *Model) fetch() tea.Cmd {
	client := m.client
	jobID, vertexID, generation := m.context.Snapshot.JobID, m.vertex, m.context.Generation
	requestID, start := m.requests.Begin(requestKey{jobID: jobID, vertexID: vertexID, generation: generation})
	if !start {
		return nil
	}
	m.busy = true
	parent := m.parent
	if parent == nil {
		parent = context.Background()
	}
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(parent, requestLimit)
		defer cancel()
		values, err := client.Accumulators(ctx, jobID, vertexID)
		return reply{values: values, err: err, jobID: jobID, vertexID: vertexID, generation: generation, requestID: requestID}
	}
}

func flatten(values flink.VertexAccumulators) []row {
	rows := make([]row, 0, len(values.Vertex))
	for _, value := range values.Vertex {
		rows = append(rows, row{scope: "VERTEX", name: value.Name, typeName: value.Type, value: value.Value})
	}
	for _, subtask := range values.Subtasks {
		for _, value := range subtask.Accumulators {
			rows = append(rows, row{scope: fmt.Sprintf("#%d", subtask.Subtask), name: value.Name, typeName: value.Type, value: value.Value})
		}
	}
	return rows
}

func (m Model) selected() (row, bool) {
	rows := m.filteredRows()
	index := m.cursor.Index()
	if index < 0 || index >= len(rows) {
		return row{}, false
	}
	return rows[index], true
}

func (m Model) filteredRows() []row {
	query := strings.ToLower(strings.TrimSpace(m.filter.Value()))
	if query == "" {
		return m.rows
	}
	rows := make([]row, 0, len(m.rows))
	for _, value := range m.rows {
		if strings.Contains(strings.ToLower(value.name), query) {
			rows = append(rows, value)
		}
	}
	return rows
}

func (m Model) selectedIdentity() string {
	value, ok := m.selected()
	if !ok {
		return ""
	}
	return value.scope + "\x00" + value.name
}

func (m *Model) restoreSelection(identity string) {
	rows := m.filteredRows()
	if identity != "" {
		for index, value := range rows {
			if value.scope+"\x00"+value.name == identity {
				m.cursor.Set(index, len(rows))
				return
			}
		}
	}
	m.cursor.Set(0, len(rows))
}

func (m Model) rowsAvailable() int { return max(1, m.bodyHeight()-bodyRowStart-3) }
func (m Model) windowStart() int {
	return shared.WindowStart(m.cursor.Index(), m.rowsAvailable(), len(m.filteredRows()))
}
func (m Model) bodyHeight() int {
	if m.context.BodyHeight > 0 {
		return m.context.BodyHeight
	}
	return 20
}

func (m Model) nodeName() string {
	for _, node := range m.context.Snapshot.Nodes {
		if node.ID == m.vertex {
			return node.Name
		}
	}
	return shared.ShortID(m.vertex)
}

// Render draws the accumulator explorer.
func (m Model) Render(width, height int) string {
	rows := m.filteredRows()
	title := fmt.Sprintf(" ACCUMULATORS  %s  |  %d/%d values", m.nodeName(), len(rows), len(m.rows))
	if m.busy {
		title += "  |  refreshing..."
	}
	status := " Vertex and per-subtask user accumulators"
	statusStyle := lipgloss.NewStyle().Foreground(shared.C("#94A3B8"))
	if !m.values.UpdatedAt.IsZero() {
		status += "  |  updated " + m.values.UpdatedAt.Format("15:04:05")
	}
	if m.err != nil {
		status = " Could not load accumulators: " + shared.ErrorText(m.err)
		statusStyle = statusStyle.Foreground(shared.C("#FB7185"))
	}
	if m.filter.Active() {
		status = " /" + m.filter.Value() + "|  filter accumulator name"
		statusStyle = statusStyle.Foreground(shared.C("#E2E8F0")).Background(shared.C("#111827"))
	} else if m.filter.Value() != "" {
		status += "  |  filter /" + m.filter.Value() + "/"
	}
	columns := "   SCOPE      NAME                                  TYPE                    VALUE"
	lines := []string{
		lipgloss.NewStyle().Bold(true).Foreground(shared.C("#E2E8F0")).Render(shared.Truncate(title, width)),
		statusStyle.Render(shared.Truncate(status, width)),
		lipgloss.NewStyle().Bold(true).Foreground(shared.C("#94A3B8")).Background(shared.C("#111827")).Render(shared.PadRight(shared.Truncate(columns, width), width)),
	}
	start, end := m.windowStart(), min(len(rows), m.windowStart()+m.rowsAvailable())
	for index := start; index < end; index++ {
		lines = append(lines, renderRow(rows[index], index == m.cursor.Index(), width))
	}
	if len(m.rows) == 0 && !m.busy && m.err == nil {
		lines = append(lines, shared.Truncate(" No user accumulators are currently reported.", width), shared.Truncate(" The Flink accumulator endpoints are available; these jobs simply publish none.", width))
	} else if len(rows) == 0 && !m.busy && m.err == nil {
		lines = append(lines, shared.Truncate(" No accumulators match filter /"+m.filter.Value()+"/. Press / to replace it or Ctrl+W while editing to clear.", width))
	}
	for len(lines) < height-3 {
		lines = append(lines, "")
	}
	separator := lipgloss.NewStyle().Foreground(shared.C("#334155")).Render(strings.Repeat("-", width))
	detailOne, detailTwo := m.renderDetail(width)
	lines = append(lines, separator, detailOne, detailTwo)
	return shared.FitLines(lines, width, height)
}

func renderRow(value row, selected bool, width int) string {
	marker := " "
	if selected {
		marker = ">"
	}
	nameWidth, typeWidth := max(16, min(38, width/3)), max(10, min(22, width/5))
	line := fmt.Sprintf(" %s %-8s  %s  %s  %s", marker, shared.Truncate(value.scope, 8), shared.PadRight(shared.Truncate(value.name, nameWidth), nameWidth), shared.PadRight(shared.Truncate(value.typeName, typeWidth), typeWidth), value.value)
	line = shared.PadRight(shared.Truncate(line, width), width)
	if selected {
		return lipgloss.NewStyle().Foreground(shared.C("#FFFFFF")).Background(shared.C("#6D28D9")).Bold(true).Render(line)
	}
	return lipgloss.NewStyle().Foreground(shared.C("#CBD5E1")).Render(line)
}

func (m Model) renderDetail(width int) (string, string) {
	selected, ok := m.selected()
	if !ok {
		return " No accumulator selected.", " enter opens the complete value  |  esc returns to graph"
	}
	first := fmt.Sprintf(" %s / %s  |  type %s", selected.scope, selected.name, shared.Fallback(selected.typeName, "unknown"))
	second := " " + selected.value + "  |  enter opens the complete value"
	return shared.Truncate(first, width), shared.Truncate(second, width)
}
