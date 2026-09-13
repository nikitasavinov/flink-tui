package flamegraph

import (
	"context"
	"fmt"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/nikitasavinov/flink-tui/internal/flink"
	"github.com/nikitasavinov/flink-tui/internal/ui/shared"
)

const (
	headerHeight          = 3
	footerHeight          = 1
	defaultRequestTimeout = 8 * time.Second
)

// View selects the live vertex sampler or an immutable profiler report.
type View uint8

const (
	ViewVertex View = iota
	ViewProfilerReport
)

// Intent asks the application shell to leave the flame-graph workspace.
type Intent uint8

const (
	IntentNone Intent = iota
	IntentGraph
	IntentProfiler
)

// Context contains shell-owned viewport and selected-job data.
type Context struct {
	JobID        string
	Nodes        []flink.Node
	Selected     string
	Generation   uint64
	Width        int
	Height       int
	ContentWidth int
}

// Result combines an optional sampling command with cross-feature navigation.
type Result struct {
	Command tea.Cmd
	Intent  Intent
}

// Message is the sealed family of vertex flame-graph replies.
type Message interface {
	flameGraphMessage()
}

type sampleMsg struct {
	graph      flink.FlameGraph
	err        error
	jobID      string
	vertexID   string
	typeName   flink.FlameGraphType
	subtask    int
	generation uint64
	requestID  uint64
}

func (sampleMsg) flameGraphMessage() {}

type sampleRequest struct {
	jobID, vertexID string
	typeName        flink.FlameGraphType
	subtask         int
	generation      uint64
}

type flameGraphViewState struct {
	flameGraph         flink.FlameGraph
	flameGraphSelected string
	flameGraphFocus    string
}

// State is a detached snapshot for shell policy and integration tests.
type State struct {
	View           View
	Vertex         string
	Type           flink.FlameGraphType
	Subtask        int
	Busy           bool
	Err            error
	LiveGraph      flink.FlameGraph
	LiveSelected   string
	LiveFocus      string
	ReportGraph    flink.FlameGraph
	ReportSelected string
	ReportFocus    string
	ReportName     string
	Process        flink.ProcessRef
}

// Model owns the two isolated graph views and live request lifecycle.
type Model struct {
	client  *flink.Client
	parent  context.Context
	context Context
	view    View
	intent  Intent

	snapshot flink.Snapshot
	selected string
	width    int
	height   int

	flameGraphViewState
	flameGraphType    flink.FlameGraphType
	flameGraphSubtask int
	flameGraphVertex  string
	flameGraphBusy    bool
	flameGraphErr     error
	sampleRequests    shared.RequestGate[sampleRequest]

	process            flink.ProcessRef
	profilerReportName string
	profilerFlameGraph flameGraphViewState
}

// New creates an empty flame-graph workspace.
func New(client *flink.Client, parent context.Context) Model {
	return Model{client: client, parent: parent, flameGraphType: flink.FlameGraphFull, flameGraphSubtask: -1}
}

// Sync updates immutable shell-owned context.
func (m *Model) Sync(value Context) {
	m.context = value
	m.snapshot = flink.Snapshot{JobID: value.JobID, Nodes: value.Nodes}
	m.selected = value.Selected
	m.width = value.Width
	m.height = value.Height
}

// Reset drops live job-scoped state while retaining an independently captured
// profiler report.
func (m *Model) Reset() {
	m.sampleRequests.Reset()
	m.flameGraphViewState = flameGraphViewState{}
	m.flameGraphType = flink.FlameGraphFull
	m.flameGraphSubtask = -1
	m.flameGraphVertex = ""
	m.flameGraphBusy = false
	m.flameGraphErr = nil
}

// Activate selects the graph used by delegated input and rendering.
func (m *Model) Activate(view View) {
	if view == ViewProfilerReport {
		m.view = ViewProfilerReport
		return
	}
	m.view = ViewVertex
}

// CurrentView reports the active graph view.
func (m Model) CurrentView() View { return m.view }

// OpenVertex starts sampling the currently selected vertex.
func (m *Model) OpenVertex(value Context) tea.Cmd {
	m.Sync(value)
	m.Activate(ViewVertex)
	return m.openFlameGraph()
}

// LoadProfilerReport replaces only the immutable profiler report.
func (m *Model) LoadProfilerReport(process flink.ProcessRef, name string, graph flink.FlameGraph) {
	m.process = process
	m.profilerReportName = name
	m.profilerFlameGraph = flameGraphViewState{flameGraph: graph}
	syncFlameGraphViewSelection(&m.profilerFlameGraph)
}

// ClearProfilerReport discards captured profiler content.
func (m *Model) ClearProfilerReport() {
	m.profilerReportName = ""
	m.profilerFlameGraph = flameGraphViewState{}
}

// State returns a detached snapshot.
func (m Model) State() State {
	return State{
		View: m.view, Vertex: m.flameGraphVertex, Type: m.flameGraphType,
		Subtask: m.flameGraphSubtask, Busy: m.flameGraphBusy, Err: m.flameGraphErr,
		LiveGraph: m.flameGraph, LiveSelected: m.flameGraphSelected, LiveFocus: m.flameGraphFocus,
		ReportGraph:    m.profilerFlameGraph.flameGraph,
		ReportSelected: m.profilerFlameGraph.flameGraphSelected,
		ReportFocus:    m.profilerFlameGraph.flameGraphFocus,
		ReportName:     m.profilerReportName, Process: m.process,
	}
}

// RestoreState replaces state while retaining dependencies and context.
func (m *Model) RestoreState(state State) {
	m.Activate(state.View)
	m.flameGraphVertex = state.Vertex
	m.flameGraphType = state.Type
	if m.flameGraphType == "" {
		m.flameGraphType = flink.FlameGraphFull
	}
	m.flameGraphSubtask = state.Subtask
	m.flameGraphBusy = state.Busy
	m.flameGraphErr = state.Err
	m.flameGraphViewState = flameGraphViewState{
		flameGraph: state.LiveGraph, flameGraphSelected: state.LiveSelected, flameGraphFocus: state.LiveFocus,
	}
	m.profilerFlameGraph = flameGraphViewState{
		flameGraph: state.ReportGraph, flameGraphSelected: state.ReportSelected, flameGraphFocus: state.ReportFocus,
	}
	m.profilerReportName = state.ReportName
	m.process = state.Process
}

// HandleKey reduces keyboard input for the active graph.
func (m *Model) HandleKey(key string) Result {
	m.Activate(m.view)
	m.intent = IntentNone
	command := m.handleFlameGraphKey(key)
	return Result{Command: command, Intent: m.intent}
}

// HandleClick reduces an absolute terminal mouse click.
func (m *Model) HandleClick(event tea.Mouse) tea.Cmd {
	m.Activate(m.view)
	return m.handleFlameGraphMouseClick(event)
}

// HandleWheel moves through visible stack frames.
func (m *Model) HandleWheel(delta int) { m.moveFlameGraphLinear(delta) }

// Refresh clears and refetches a live graph. Captured reports are immutable.
func (m *Model) Refresh() tea.Cmd {
	if m.view == ViewProfilerReport {
		return nil
	}
	m.flameGraphBusy = true
	m.flameGraphErr = nil
	return m.fetchFlameGraph()
}

// Poll requests the current live sampling state without clearing the tree.
func (m *Model) Poll() tea.Cmd {
	if m.view == ViewProfilerReport {
		return nil
	}
	return m.fetchFlameGraph()
}

// Apply reduces one live sampling reply.
func (m *Model) Apply(message Message) tea.Cmd {
	sample, ok := message.(sampleMsg)
	if !ok || !m.sampleRequests.Finish(sample.requestID) || sample.generation != m.context.Generation || sample.jobID != m.context.JobID ||
		sample.vertexID != m.flameGraphVertex || sample.typeName != m.flameGraphType || sample.subtask != m.flameGraphSubtask {
		return nil
	}
	m.flameGraphBusy = false
	m.flameGraphErr = sample.err
	if sample.err == nil {
		m.flameGraph = sample.graph
		syncFlameGraphViewSelection(&m.flameGraphViewState)
	}
	return nil
}

// Render draws the active graph.
func (m Model) Render(width, height int) string {
	m.width = max(m.width, width)
	m.Activate(m.view)
	return m.renderFlameGraph(width, height)
}

// Error reports the live sampler error; immutable reports have no request
// error.
func (m Model) Error() error {
	if m.view == ViewProfilerReport {
		return nil
	}
	return m.flameGraphErr
}

func (m *Model) fetchFlameGraph() tea.Cmd {
	client := m.client
	jobID := m.context.JobID
	vertexID := m.flameGraphVertex
	typeName := m.flameGraphType
	subtask := m.flameGraphSubtask
	generation := m.context.Generation
	requestID, start := m.sampleRequests.Begin(sampleRequest{jobID, vertexID, typeName, subtask, generation})
	if !start {
		return nil
	}
	requestTimeout := m.requestTimeout
	return func() tea.Msg {
		ctx, cancel := requestTimeout(defaultRequestTimeout)
		defer cancel()
		graph, err := client.VertexFlameGraph(ctx, jobID, vertexID, typeName, subtask)
		return sampleMsg{graph: graph, err: err, jobID: jobID, vertexID: vertexID, typeName: typeName, subtask: subtask, generation: generation, requestID: requestID}
	}
}

func (m Model) requestTimeout(timeout time.Duration) (context.Context, context.CancelFunc) {
	parent := m.parent
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, timeout)
}

func (m Model) contentWidthAt(width int) int {
	if m.context.ContentWidth > 0 {
		return m.context.ContentWidth
	}
	return width
}

func (m Model) nodeName(id string) string {
	for _, node := range m.snapshot.Nodes {
		if node.ID == id {
			return node.Name
		}
	}
	return id
}

func humanCount(value float64) string {
	absolute := value
	if absolute < 0 {
		absolute = -absolute
	}
	switch {
	case absolute >= 1_000_000_000:
		return fmt.Sprintf("%.1fB", value/1_000_000_000)
	case absolute >= 1_000_000:
		return fmt.Sprintf("%.1fM", value/1_000_000)
	case absolute >= 1_000:
		return fmt.Sprintf("%.1fK", value/1_000)
	default:
		return fmt.Sprintf("%.0f", value)
	}
}
