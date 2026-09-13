package jobdetail

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/nikitasavinov/flink-tui/internal/flink"
	"github.com/nikitasavinov/flink-tui/internal/graph"
	processmodule "github.com/nikitasavinov/flink-tui/internal/ui/processdiag"
	"github.com/nikitasavinov/flink-tui/internal/ui/shared"
	shellmodule "github.com/nikitasavinov/flink-tui/internal/ui/shell"
)

const (
	headerHeight          = 3
	footerHeight          = 1
	defaultRequestTimeout = 8 * time.Second
	nodeWidth             = 29
	nodeHeight            = 5
)

// View identifies a job-exploration screen.
type View uint8

const (
	ViewSubtasks View = iota
	ViewDiagnostics
	ViewTimeline
)

// Page identifies a diagnostics overview perspective.
type Page = diagnosticPage

const (
	PageBackpressure = diagnosticBackpressure
	PageSkew         = diagnosticSkew
	PageMetrics      = diagnosticMetrics
)

// Intent asks the shell to leave this workspace.
type Intent uint8

const (
	IntentNone Intent = iota
	IntentGraph
	IntentOverview
	IntentCheckpoints
	IntentThreadDump
)

// Context contains immutable shell-owned job and viewport data.
type Context struct {
	Snapshot     flink.Snapshot
	Selected     string
	Order        []string
	Generation   uint64
	BodyHeight   int
	ContentWidth int
	Width        int
	History      map[string][]flink.Metrics
}

// Result combines a module command with cross-feature navigation.
type Result struct {
	Command  tea.Cmd
	Intent   Intent
	View     View
	Selected string
	Process  flink.ProcessRef
	Focus    processmodule.Focus
	Notice   string
}

// Message is the sealed family of subtask diagnostic replies.
type Message interface {
	jobDetailMessage()
}

type diagnosticsMsg struct {
	diagnostics flink.VertexDiagnostics
	err         error
	jobID       string
	vertexID    string
	generation  uint64
	requestID   uint64
}

func (diagnosticsMsg) jobDetailMessage() {}

type requestKey struct {
	jobID, vertexID string
	generation      uint64
}

type subtaskSort uint8

const (
	sortSubtaskIndex subtaskSort = iota
	sortSubtaskState
	sortSubtaskBackpressure
	sortSubtaskBusy
	sortSubtaskIdle
	sortSubtaskInput
	sortSubtaskOutput
	sortSubtaskBytesInput
	sortSubtaskBytesOutput
	sortSubtaskWatermark
	sortSubtaskTaskManager
)

// State is a detached snapshot for shell policy and tests.
type State struct {
	View                  View
	Selected              string
	Diagnostics           flink.VertexDiagnostics
	DiagnosticsOpen       string
	DiagnosticsBusy       bool
	DiagnosticsErr        error
	SelectedSubtask       int
	SubtaskSort           uint8
	SubtaskSortDescending bool
	SubtaskSortOpen       bool
	SubtaskTotals         bool
	Page                  Page
	SubtaskQuery          string
	SubtaskSearch         bool
	DiagnosticQuery       string
	DiagnosticSearch      bool
	TimelineQuery         string
	TimelineSearch        bool
}

// Model owns all mutable job-exploration state.
type Model struct {
	client  *flink.Client
	parent  context.Context
	context Context
	view    View

	snapshot flink.Snapshot
	layout   graph.Layout
	selected string
	width    int

	diagnostics           flink.VertexDiagnostics
	diagnosticsOpen       string
	diagnosticsBusy       bool
	diagnosticsErr        error
	requests              shared.RequestGate[requestKey]
	selectedSubtask       int
	subtaskSort           subtaskSort
	subtaskSortDescending bool
	subtaskSortOpen       bool
	subtaskTotals         bool
	diagnosticPage        diagnosticPage
	subtaskFilter         shellmodule.QueryInput
	diagnosticFilter      shellmodule.QueryInput
	timelineFilter        shellmodule.QueryInput

	pendingIntent  Intent
	pendingProcess flink.ProcessRef
	pendingFocus   processmodule.Focus
	pendingNotice  string
}

// New creates an empty job-exploration workspace.
func New(client *flink.Client, parent context.Context) Model {
	return Model{client: client, parent: parent, selectedSubtask: -1}
}

// Sync updates shell-owned context without replacing local drill-down state.
func (m *Model) Sync(value Context) {
	m.context = value
	m.snapshot = value.Snapshot
	m.layout.Order = append([]string(nil), value.Order...)
	m.selected = value.Selected
	m.width = value.Width
}

// Reset drops state tied to the previous job.
func (m *Model) Reset() {
	client, parent := m.client, m.parent
	m.requests.Reset()
	requests := m.requests
	*m = New(client, parent)
	m.requests = requests
}

// OpenSubtasks opens the selected vertex and starts a detail fetch.
func (m *Model) OpenSubtasks(value Context) tea.Cmd {
	m.Sync(value)
	m.view = ViewSubtasks
	return m.openDiagnostics()
}

// OpenDiagnostics opens a job-wide diagnostic page.
func (m *Model) OpenDiagnostics(value Context, page Page) {
	m.Sync(value)
	m.view = ViewDiagnostics
	m.openDiagnosticOverview(page)
}

// OpenTimeline opens the execution timeline.
func (m *Model) OpenTimeline(value Context) {
	m.Sync(value)
	m.view = ViewTimeline
	m.openTimeline()
}

// Activate selects the view for delegated input and rendering.
func (m *Model) Activate(view View) {
	switch view {
	case ViewDiagnostics:
		m.view = ViewDiagnostics
	case ViewTimeline:
		m.view = ViewTimeline
	default:
		m.view = ViewSubtasks
	}
}

// CurrentView reports the active workspace view.
func (m Model) CurrentView() View { return m.view }

// CapturesKeys reports whether the subtask sort picker owns keyboard input.
func (m Model) CapturesKeys() bool {
	switch m.view {
	case ViewDiagnostics:
		return m.diagnosticFilter.Active()
	case ViewTimeline:
		return m.timelineFilter.Active()
	default:
		return m.subtaskSortOpen || m.subtaskFilter.Active()
	}
}

// HandleKey reduces input for the active view.
func (m *Model) HandleKey(key string) Result {
	m.pendingIntent, m.pendingProcess, m.pendingFocus, m.pendingNotice = IntentNone, flink.ProcessRef{}, processmodule.Focus{}, ""
	m.Activate(m.view)
	var command tea.Cmd
	switch m.view {
	case ViewDiagnostics:
		command = m.handleDiagnosticOverviewKey(key)
	case ViewTimeline:
		command = m.handleTimelineKey(key)
	default:
		command = m.handleSubtaskKey(key)
	}
	return m.result(command)
}

// HandleClick reduces an absolute terminal mouse click.
func (m *Model) HandleClick(event tea.Mouse) Result {
	m.pendingIntent, m.pendingProcess, m.pendingFocus, m.pendingNotice = IntentNone, flink.ProcessRef{}, processmodule.Focus{}, ""
	var command tea.Cmd
	switch m.view {
	case ViewDiagnostics:
		m.handleDiagnosticOverviewMouseClick(event)
	case ViewTimeline:
		m.handleTimelineMouseClick(event)
	default:
		command = m.handleSubtaskMouseClick(event)
	}
	return m.result(command)
}

// HandleWheel moves the active vertex or subtask selection.
func (m *Model) HandleWheel(delta int) {
	switch m.view {
	case ViewSubtasks:
		m.moveSubtaskSelection(delta)
	case ViewTimeline:
		m.moveTimelineSelection(delta)
	default:
		m.moveDiagnosticSelection(delta)
	}
}

// Refresh requests the selected vertex's subtask diagnostics.
func (m *Model) Refresh() tea.Cmd {
	if m.diagnosticsOpen == "" {
		return nil
	}
	m.diagnosticsBusy = true
	m.diagnosticsErr = nil
	return m.fetchDiagnostics()
}

// Poll requests subtask diagnostics without clearing the last-good table.
func (m *Model) Poll() tea.Cmd {
	if m.diagnosticsOpen == "" {
		return nil
	}
	return m.fetchDiagnostics()
}

// Apply reduces a generation- and identity-checked diagnostic reply.
func (m *Model) Apply(message Message) tea.Cmd {
	reply, ok := message.(diagnosticsMsg)
	if !ok || reply.generation != m.context.Generation || reply.jobID != m.snapshot.JobID || reply.vertexID != m.diagnosticsOpen {
		return nil
	}
	if !m.requests.Finish(reply.requestID) {
		return nil
	}
	m.diagnosticsBusy = false
	m.diagnosticsErr = reply.err
	if reply.err == nil {
		m.diagnostics = reply.diagnostics
		m.ensureSubtaskSelection()
	}
	return nil
}

// Render draws the active view.
func (m Model) Render(width, height int) string {
	switch m.view {
	case ViewDiagnostics:
		return m.renderDiagnosticOverview(width, height)
	case ViewTimeline:
		return m.renderTimeline(width, height)
	default:
		return m.renderSubtasks(width, height)
	}
}

// State returns a detached snapshot.
func (m Model) State() State {
	return State{
		View: m.view, Selected: m.selected, Diagnostics: m.diagnostics,
		DiagnosticsOpen: m.diagnosticsOpen, DiagnosticsBusy: m.diagnosticsBusy, DiagnosticsErr: m.diagnosticsErr,
		SelectedSubtask: m.selectedSubtask, SubtaskSort: uint8(m.subtaskSort),
		SubtaskSortDescending: m.subtaskSortDescending, SubtaskSortOpen: m.subtaskSortOpen,
		SubtaskTotals: m.subtaskTotals, Page: m.diagnosticPage,
		SubtaskQuery: m.subtaskFilter.Value(), SubtaskSearch: m.subtaskFilter.Active(),
		DiagnosticQuery: m.diagnosticFilter.Value(), DiagnosticSearch: m.diagnosticFilter.Active(),
		TimelineQuery: m.timelineFilter.Value(), TimelineSearch: m.timelineFilter.Active(),
	}
}

// RestoreState replaces local state while retaining dependencies and context.
func (m *Model) RestoreState(state State) {
	m.Activate(state.View)
	m.selected = state.Selected
	m.diagnostics = state.Diagnostics
	m.diagnosticsOpen = state.DiagnosticsOpen
	m.diagnosticsBusy = state.DiagnosticsBusy
	m.diagnosticsErr = state.DiagnosticsErr
	m.selectedSubtask = state.SelectedSubtask
	m.subtaskSort = subtaskSort(state.SubtaskSort)
	m.subtaskSortDescending = state.SubtaskSortDescending
	m.subtaskSortOpen = state.SubtaskSortOpen
	m.subtaskTotals = state.SubtaskTotals
	m.diagnosticPage = state.Page
	m.subtaskFilter.Restore(shellmodule.QueryState{Value: state.SubtaskQuery, Open: state.SubtaskSearch})
	m.diagnosticFilter.Restore(shellmodule.QueryState{Value: state.DiagnosticQuery, Open: state.DiagnosticSearch})
	m.timelineFilter.Restore(shellmodule.QueryState{Value: state.TimelineQuery, Open: state.TimelineSearch})
}

// Error returns the subtask detail request error.
func (m Model) Error() error { return m.diagnosticsErr }

func (m *Model) result(command tea.Cmd) Result {
	return Result{
		Command: command, Intent: m.pendingIntent, View: m.view, Selected: m.selected,
		Process: m.pendingProcess, Focus: m.pendingFocus, Notice: m.pendingNotice,
	}
}

func (m *Model) fetchDiagnostics() tea.Cmd {
	client := m.client
	jobID := m.snapshot.JobID
	vertexID := m.diagnosticsOpen
	generation := m.context.Generation
	requestID, start := m.requests.Begin(requestKey{jobID: jobID, vertexID: vertexID, generation: generation})
	if !start {
		return nil
	}
	m.diagnosticsBusy = true
	parent := m.parent
	if parent == nil {
		parent = context.Background()
	}
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(parent, defaultRequestTimeout)
		defer cancel()
		diagnostics, err := client.VertexDiagnostics(ctx, jobID, vertexID)
		return diagnosticsMsg{diagnostics: diagnostics, err: err, jobID: jobID, vertexID: vertexID, generation: generation, requestID: requestID}
	}
}

func (m *Model) openThreadDumpWithFocus(process flink.ProcessRef, focus processmodule.Focus) tea.Cmd {
	m.pendingIntent, m.pendingProcess, m.pendingFocus = IntentThreadDump, process, focus
	return nil
}

func (m *Model) setNotice(message string) { m.pendingNotice = message }

func (m Model) bodyHeight() int {
	if m.context.BodyHeight > 0 {
		return m.context.BodyHeight
	}
	return 20
}

func (m Model) contentWidthAt(width int) int {
	if m.context.ContentWidth > 0 {
		return m.context.ContentWidth
	}
	return width
}

func (m Model) selectedNode() (flink.Node, bool) { return m.nodeByID(m.selected) }

func (m Model) nodeByID(id string) (flink.Node, bool) {
	for _, node := range m.snapshot.Nodes {
		if node.ID == id {
			return node, true
		}
	}
	return flink.Node{}, false
}

func (m Model) nodeName(id string) string {
	if node, ok := m.nodeByID(id); ok {
		return node.Name
	}
	return shared.ShortID(id)
}

type metricSample struct{ metrics flink.Metrics }

func (m Model) nodeHistory(vertexID string) []metricSample {
	values := m.context.History[vertexID]
	result := make([]metricSample, len(values))
	for index, metrics := range values {
		result[index] = metricSample{metrics: metrics}
	}
	return result
}

func metricValues(samples []metricSample, value func(flink.Metrics) float64) []float64 {
	result := make([]float64, len(samples))
	for index, sample := range samples {
		result[index] = value(sample.metrics)
	}
	return result
}

func sparklineDynamic(values []float64, width int) string {
	if len(values) == 0 || width <= 0 {
		return strings.Repeat(" ", max(0, width))
	}
	minimum, maximum := values[0], values[0]
	for _, value := range values[1:] {
		minimum, maximum = min(minimum, value), max(maximum, value)
	}
	return sparkline(values, width, minimum, maximum)
}

func sparklineFixed(values []float64, width int, maximum float64) string {
	return sparkline(values, width, 0, maximum)
}

func sparkline(values []float64, width int, minimum, maximum float64) string {
	blocks := []rune("▁▂▃▄▅▆▇█")
	if len(values) == 0 || width <= 0 {
		return strings.Repeat(" ", max(0, width))
	}
	values = downsample(values, width)
	var result strings.Builder
	for _, value := range values {
		position := 0
		if maximum > minimum {
			position = int(math.Round((value - minimum) / (maximum - minimum) * float64(len(blocks)-1)))
		}
		position = shared.Clamp(position, 0, len(blocks)-1)
		result.WriteRune(blocks[position])
	}
	for result.Len() < width {
		result.WriteRune(' ')
	}
	return result.String()
}

func downsample(values []float64, width int) []float64 {
	if len(values) <= width {
		return append([]float64(nil), values...)
	}
	result := make([]float64, width)
	for index := range result {
		source := int(float64(index) / float64(max(1, width-1)) * float64(len(values)-1))
		result[index] = values[source]
	}
	return result
}

func pressureLevel(metrics flink.Metrics) string {
	switch {
	case metrics.BackpressurePercent >= 50:
		return "HIGH"
	case metrics.BackpressurePercent >= 10:
		return "LOW"
	default:
		return "OK"
	}
}

func rate(value float64) string {
	if value < 0 {
		return "-"
	}
	return shared.HumanCount(value)
}

func humanMetricBytes(value float64) string {
	if value <= 0 {
		return "0B"
	}
	units := []string{"B", "KiB", "MiB", "GiB", "TiB"}
	unit := 0
	for value >= 1024 && unit < len(units)-1 {
		value /= 1024
		unit++
	}
	if value >= 100 || unit == 0 {
		return fmt.Sprintf("%.0f%s", value, units[unit])
	}
	return fmt.Sprintf("%.1f%s", value, units[unit])
}
