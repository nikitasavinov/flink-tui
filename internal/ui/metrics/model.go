package metrics

import (
	"context"
	"fmt"
	"maps"
	"math"
	"slices"
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

// Intent asks the root coordinator to leave the metric explorer.
type Intent uint8

const (
	IntentNone Intent = iota
	IntentGraph
	IntentOverview
)

// Context contains immutable job and viewport state owned by the shell.
type Context struct {
	Snapshot     flink.Snapshot
	Selected     string
	Generation   uint64
	BodyHeight   int
	ContentWidth int
}

// Result combines an asynchronous command with navigation intent.
type Result struct {
	Command tea.Cmd
	Intent  Intent
}

// Message is the sealed family of custom-metric replies.
type Message interface{ metricMessage() }

type metricCatalogMsg struct {
	names      []string
	err        error
	jobID      string
	vertexID   string
	generation uint64
	requestID  uint64
}

func (metricCatalogMsg) metricMessage() {}

type metricValuesMsg struct {
	values      map[string]float64
	err         error
	jobID       string
	vertexID    string
	subtask     int
	aggregation flink.MetricAggregation
	tracked     []string
	requested   []string
	at          time.Time
	generation  uint64
	requestID   uint64
}

func (metricValuesMsg) metricMessage() {}

type catalogRequest struct {
	jobID, vertexID string
	generation      uint64
}

type valuesRequest struct {
	catalogRequest
	subtask            int
	aggregation        flink.MetricAggregation
	tracked, requested string
}

// State is a detached snapshot for shell policy and tests.
type State struct {
	Names       []string
	Selection   int
	Tracked     []string
	Values      map[string]float64
	Aggregation flink.MetricAggregation
	Scope       int
	Window      time.Duration
	Vertex      string
	Query       string
	SearchOpen  bool
	Busy        bool
	Err         error
	UpdatedAt   time.Time
	HistorySize int
}

// Model owns metric discovery, sampling, selection, and chart history.
type Model struct {
	client  *flink.Client
	parent  context.Context
	context Context
	intent  Intent

	snapshot          flink.Snapshot
	selected          string
	generation        uint64
	metricNames       []string
	metricSelection   shared.Cursor
	metricTracked     []string
	metricValues      map[string]float64
	metricHistory     map[string][]customMetricSample
	metricAggregation flink.MetricAggregation
	metricScope       int
	metricWindow      time.Duration
	metricVertex      string
	metricFilter      shellmodule.QueryInput
	metricBusy        bool
	metricErr         error
	metricUpdatedAt   time.Time
	catalogRequests   shared.RequestGate[catalogRequest]
	valuesRequests    shared.RequestGate[valuesRequest]
}

// New creates an empty metric explorer.
func New(client *flink.Client, parent context.Context) Model {
	return Model{
		client: client, parent: parent,
		metricValues: make(map[string]float64), metricHistory: make(map[string][]customMetricSample),
		metricAggregation: flink.MetricSum, metricScope: -1, metricWindow: 5 * time.Minute,
	}
}

// Sync updates shell-owned context without replacing explorer state.
func (m *Model) Sync(value Context) {
	m.context, m.snapshot = value, value.Snapshot
	m.selected, m.generation = value.Selected, value.Generation
}

// Reset drops state tied to the previous job.
func (m *Model) Reset() {
	client, parent := m.client, m.parent
	catalog, values := m.catalogRequests, m.valuesRequests
	catalog.Reset()
	values.Reset()
	*m = New(client, parent)
	m.catalogRequests, m.valuesRequests = catalog, values
}

// Open selects the current vertex and starts catalog discovery.
func (m *Model) Open(value Context) tea.Cmd {
	m.Sync(value)
	if value.Snapshot.JobID == "" || value.Selected == "" {
		return nil
	}
	m.intent = IntentNone
	if m.metricVertex != value.Selected {
		m.metricVertex = value.Selected
		m.metricNames, m.metricTracked = nil, nil
		m.metricValues = make(map[string]float64)
		m.metricSelection.Set(0, 0)
		m.metricScope = -1
		m.metricFilter.Reset()
	}
	m.metricBusy, m.metricErr = true, nil
	return m.fetchMetricCatalog()
}

// HandleKey reduces keyboard input and returns any cross-feature intent.
func (m *Model) HandleKey(key string) Result {
	m.intent = IntentNone
	command := m.handleMetricExplorerKey(key)
	return Result{Command: command, Intent: m.intent}
}

// HandleClick selects a visible catalog row.
func (m *Model) HandleClick(event tea.Mouse) { m.handleMetricMouseClick(event) }

// Move changes catalog selection.
func (m *Model) Move(delta int) { m.moveMetricSelection(delta) }

// Refresh reloads catalog and currently requested values.
func (m *Model) Refresh() tea.Cmd {
	m.metricBusy, m.metricErr = true, nil
	return tea.Batch(m.fetchMetricCatalog(), m.fetchCustomMetricValues())
}

// Poll refreshes values while retaining the catalog and last-good charts.
func (m *Model) Poll() tea.Cmd { return m.fetchCustomMetricValues() }

// RefreshWindow updates values after viewport or selection changes.
func (m *Model) RefreshWindow() tea.Cmd { return m.refreshMetricWindow() }

// Apply reduces an identity-checked metric reply.
func (m *Model) Apply(message Message) tea.Cmd {
	switch value := message.(type) {
	case metricCatalogMsg:
		if !m.catalogRequests.Finish(value.requestID) || value.generation != m.context.Generation || value.jobID != m.snapshot.JobID || value.vertexID != m.metricVertex {
			return nil
		}
		m.metricErr = value.err
		if value.err != nil {
			m.metricBusy = false
			return nil
		}
		m.metricNames = value.names
		m.metricSelection.Constrain(len(m.filteredMetricNames()))
		m.metricTracked = availableTrackedMetrics(m.metricTracked, value.names)
		if len(m.metricTracked) == 0 {
			m.metricTracked = defaultTrackedMetrics(value.names)
		}
		m.metricBusy = len(m.metricRequestNames()) > 0
		return m.fetchCustomMetricValues()
	case metricValuesMsg:
		if !m.valuesRequests.Finish(value.requestID) || value.generation != m.context.Generation || value.jobID != m.snapshot.JobID || value.vertexID != m.metricVertex ||
			value.subtask != m.metricScope || value.aggregation != m.metricAggregation || !slices.Equal(value.tracked, m.metricTracked) ||
			!slices.Equal(value.requested, m.metricRequestNames()) {
			return nil
		}
		m.metricBusy, m.metricErr = false, value.err
		if value.err == nil {
			m.metricValues, m.metricUpdatedAt = value.values, value.at
			m.recordCustomMetricValues(value)
		}
	}
	return nil
}

// Render draws the explorer.
func (m Model) Render(width, height int) string { return m.renderMetricExplorer(width, height) }

// CapturesKeys reports whether search owns keyboard input.
func (m Model) CapturesKeys() bool { return m.metricFilter.Active() }

// Error returns the most recent request or selection error.
func (m Model) Error() error { return m.metricErr }

// State returns a detached snapshot.
func (m Model) State() State {
	historySize := 0
	for _, samples := range m.metricHistory {
		historySize += len(samples)
	}
	return State{
		Names: append([]string(nil), m.metricNames...), Selection: m.metricSelection.Index(),
		Tracked: append([]string(nil), m.metricTracked...), Values: maps.Clone(m.metricValues),
		Aggregation: m.metricAggregation, Scope: m.metricScope, Window: m.metricWindow,
		Vertex: m.metricVertex, Query: m.metricFilter.Value(), SearchOpen: m.metricFilter.Active(),
		Busy: m.metricBusy, Err: m.metricErr, UpdatedAt: m.metricUpdatedAt, HistorySize: historySize,
	}
}

// RestoreState replaces public state while retaining dependencies and context.
func (m *Model) RestoreState(state State) {
	m.metricNames = append([]string(nil), state.Names...)
	m.metricSelection.Set(state.Selection, len(m.metricNames))
	m.metricTracked = append([]string(nil), state.Tracked...)
	m.metricValues = maps.Clone(state.Values)
	m.metricAggregation, m.metricScope, m.metricWindow = state.Aggregation, state.Scope, state.Window
	if m.metricAggregation == "" {
		m.metricAggregation = flink.MetricSum
	}
	if m.metricWindow == 0 {
		m.metricWindow = 5 * time.Minute
	}
	m.metricVertex = state.Vertex
	m.metricFilter.Restore(shellmodule.QueryState{Value: state.Query, Open: state.SearchOpen})
	m.metricBusy, m.metricErr, m.metricUpdatedAt = state.Busy, state.Err, state.UpdatedAt
}

func (m Model) requestTimeout(timeout time.Duration) (context.Context, context.CancelFunc) {
	parent := m.parent
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, timeout)
}

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

func (m Model) bodyHeight() int {
	if m.context.BodyHeight > 0 {
		return m.context.BodyHeight
	}
	return 20
}

func humanMetricBytes(value float64) string {
	units := []string{"B", "KiB", "MiB", "GiB", "TiB"}
	unit := 0
	for value >= 1024 && unit < len(units)-1 {
		value /= 1024
		unit++
	}
	if unit == 0 {
		return fmt.Sprintf("%.0f%s", value, units[unit])
	}
	return fmt.Sprintf("%.1f%s", value, units[unit])
}

func sparklineDynamic(values []float64, width int) string {
	if len(values) == 0 || width <= 0 {
		return ""
	}
	minimum, maximum := values[0], values[0]
	for _, value := range values[1:] {
		minimum, maximum = math.Min(minimum, value), math.Max(maximum, value)
	}
	blocks := []rune("▁▂▃▄▅▆▇█")
	if len(values) > width {
		reduced := make([]float64, width)
		for index := range reduced {
			reduced[index] = values[int(float64(index)/float64(max(1, width-1))*float64(len(values)-1))]
		}
		values = reduced
	}
	var result strings.Builder
	for _, value := range values {
		position := 0
		if maximum > minimum {
			position = int(math.Round((value - minimum) / (maximum - minimum) * float64(len(blocks)-1)))
		}
		result.WriteRune(blocks[shared.Clamp(position, 0, len(blocks)-1)])
	}
	return result.String()
}
