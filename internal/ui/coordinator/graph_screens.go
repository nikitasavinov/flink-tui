package coordinator

import (
	"strings"

	tea "charm.land/bubbletea/v2"

	"github.com/nikitasavinov/flink-tui/internal/flink"
	"github.com/nikitasavinov/flink-tui/internal/graph"
	accumulatormodule "github.com/nikitasavinov/flink-tui/internal/ui/accumulators"
	exceptionmodule "github.com/nikitasavinov/flink-tui/internal/ui/exceptions"
	flamegraphmodule "github.com/nikitasavinov/flink-tui/internal/ui/flamegraph"
	jobgraphmodule "github.com/nikitasavinov/flink-tui/internal/ui/jobgraph"
	metricmodule "github.com/nikitasavinov/flink-tui/internal/ui/metrics"
	"github.com/nikitasavinov/flink-tui/internal/ui/shared"
)

const (
	mousePanX                 = 8
	mousePanY                 = 3
	metricChangePulseDuration = jobgraphmodule.ChangePulseDuration
)

type graphZoom = jobgraphmodule.Zoom

const (
	graphZoomDetailed = jobgraphmodule.ZoomDetailed
	graphZoomCompact  = jobgraphmodule.ZoomCompact
	graphZoomTopology = jobgraphmodule.ZoomTopology
)

func (m Model) renderGraphScreen(width, height int) string {
	graphHeight := max(3, height-inspectorHeight)
	var graphView string
	if len(m.snapshot.Nodes) == 0 {
		message := "Connecting to " + m.client.Endpoint() + " ..."
		if m.err != nil {
			message = "Waiting for Flink: " + conciseError(m.err)
		}
		graphView = centerText(message, width, graphHeight)
	} else {
		graphView = m.renderGraph(width, graphHeight)
	}
	// Short terminals retain the graph and the inspector's first rows without
	// pushing the shell's navigation hints below the visible terminal.
	return shared.FitLines(strings.Split(graphView+"\n"+m.renderInspector(width), "\n"), width, height)
}

func (m Model) renderInspector(width int) string {
	node, ok := m.selectedNode()
	return jobgraphmodule.RenderInspector(jobgraphmodule.InspectorScene{
		Node: node, HasNode: ok, Nodes: m.snapshot.Nodes,
		Children: m.layout.Children[node.ID], Samples: m.graphTelemetry.Samples(node.ID),
	}, width, inspectorHeight)
}

func (m *Model) handleGraphKey(key string) tea.Cmd {
	origin := m.routeCrumb()
	switch key {
	case "+", "=":
		m.changeGraphZoom(-1)
		return nil
	case "-", "_":
		m.changeGraphZoom(1)
		return nil
	case "f":
		m.fitGraph()
		return nil
	case "F":
		return m.finishDrill(origin, m.openFlameGraph())
	case "u":
		return m.finishDrill(origin, m.openAccumulators())
	case "v":
		return m.finishDrill(origin, m.openMetricExplorer())
	case "0":
		m.resetGraphZoom()
		return nil
	}

	selectionChanged := false
	switch key {
	case "tab":
		m.selectRelative(1)
		selectionChanged = true
	case "shift+tab":
		m.selectRelative(-1)
		selectionChanged = true
	case "left", "h":
		m.selectConnected(m.layout.Parents[m.selected])
		selectionChanged = true
	case "right", "l":
		m.selectConnected(m.layout.Children[m.selected])
		selectionChanged = true
	case "up", "k":
		m.selectSibling(-1)
		selectionChanged = true
	case "down", "j":
		m.selectSibling(1)
		selectionChanged = true
	case "enter":
		return m.finishDrill(origin, m.openDiagnostics())
	case "esc":
		return m.openJobPicker()
	case "z":
		m.toggleMinimap()
	case "shift+left":
		m.pan(-4, 0)
	case "shift+right":
		m.pan(4, 0)
	case "shift+up":
		m.pan(0, -2)
	case "shift+down":
		m.pan(0, 2)
	}
	if selectionChanged {
		m.centerSelection()
	}
	return nil
}

func (m Model) graphZoomLabel() string { return m.graphViewport.ZoomLabel() }

func (m Model) buildGraphLayout(nodes []flink.Node) graph.Layout {
	return m.graphViewport.BuildLayout(nodes)
}

func (m *Model) changeGraphZoom(delta int) {
	m.layout = m.graphViewport.ChangeZoom(delta, m.snapshot.Nodes, m.layout, m.selected, m.graphWidth(), m.graphHeight())
}

func (m *Model) resetGraphZoom() {
	m.layout = m.graphViewport.ResetZoom(m.snapshot.Nodes, m.layout, m.selected, m.graphWidth(), m.graphHeight())
}

func (m *Model) fitGraph() {
	m.layout = m.graphViewport.Fit(m.snapshot.Nodes, m.layout, m.selected, m.graphWidth(), m.graphHeight())
}

func (m *Model) autoFitGraphIfReady() bool {
	if m.width <= 0 || m.height <= 0 {
		return false
	}
	layout, fitted := m.graphViewport.AutoFit(m.snapshot.Nodes, m.layout, m.selected, m.graphWidth(), m.graphHeight())
	m.layout = layout
	return fitted
}

func (m *Model) centerSelection() {
	m.graphViewport.Center(m.layout, m.selected, m.graphWidth(), m.graphHeight())
}

func (m *Model) pan(deltaX, deltaY int) {
	m.graphViewport.Pan(m.layout, m.graphWidth(), m.graphHeight(), deltaX, deltaY)
}

func (m Model) renderGraph(width, height int) string {
	viewport := m.graphViewport.State()
	return jobgraphmodule.Render(jobgraphmodule.Scene{
		Nodes: m.snapshot.Nodes, Layout: m.layout, Selected: m.selected,
		OffsetX: viewport.OffsetX, OffsetY: viewport.OffsetY,
		Zoom: viewport.Zoom, Changes: m.graphTelemetry.Changes(),
		ShowMinimap: m.graphViewport.MinimapVisible(len(m.snapshot.Nodes) > 0, m.graphWidth()),
	}, width, height)
}

func (m *Model) handleMouseClick(event tea.Mouse) {
	m.selected = m.graphViewport.Click(event, m.layout, m.selected, m.graphWidth(), m.graphHeight(), graphScreenTop)
}

func (m *Model) handleMouseMotion(event tea.Mouse) {
	m.graphViewport.Motion(event, m.layout, m.graphWidth(), m.graphHeight(), graphScreenTop)
}

func (m *Model) handleMouseRelease(_ tea.Mouse) {
	m.graphViewport.Release(m.graphWidth())
	// Reclaim the map column after a narrow-screen map interaction.
	m.pan(0, 0)
}

func (m *Model) handleMouseWheel(event tea.Mouse) {
	m.layout = m.graphViewport.Wheel(event, m.snapshot.Nodes, m.layout, m.graphWidth(), m.graphHeight(), graphScreenTop)
}

func wheelDelta(event tea.Mouse) int {
	switch event.Button {
	case tea.MouseWheelUp, tea.MouseWheelLeft:
		return -1
	case tea.MouseWheelDown, tea.MouseWheelRight:
		return 1
	default:
		return 0
	}
}

func (m *Model) recordHistory(snapshot flink.Snapshot) {
	m.graphTelemetry.Record(flink.Snapshot{}, snapshot)
}

func (m Model) nodeHistory(vertexID string) []jobgraphmodule.Sample {
	return m.graphTelemetry.Samples(vertexID)
}

func (m *Model) configureExceptions() {
	m.exceptions = exceptionmodule.New()
	m.syncExceptionContext()
}

func (m Model) exceptionContext() exceptionmodule.Context {
	return exceptionmodule.Context{Snapshot: m.snapshot, BodyHeight: m.bodyHeight()}
}

func (m *Model) syncExceptionContext() { m.exceptions.Sync(m.exceptionContext()) }

func (m *Model) openExceptions() {
	if m.snapshot.JobID == "" {
		return
	}
	m.exceptions.Open(m.exceptionContext())
	m.mode = modeExceptions
}

func (m *Model) applyExceptionResult(result exceptionmodule.Result) tea.Cmd {
	origin := m.routeCrumb()
	if result.Notice != "" {
		m.setNotice(result.Notice)
	}
	if result.Selected != "" {
		if _, ok := m.layout.Rects[result.Selected]; ok {
			m.selected = result.Selected
		}
	}
	var command tea.Cmd
	switch result.Intent {
	case exceptionmodule.IntentGraph:
		m.mode = modeGraph
		m.centerSelection()
	case exceptionmodule.IntentOverview:
		command = m.openJobPicker()
	case exceptionmodule.IntentReloadSnapshot:
		command = m.fetchSnapshot()
	case exceptionmodule.IntentTaskManager:
		command = m.openTaskManagerByID(result.TaskManagerID, modeExceptions)
	case exceptionmodule.IntentThreadDump:
		command = m.openThreadDumpWithFocus(result.Process, modeExceptions, result.Focus)
	case exceptionmodule.IntentLogs:
		command = m.openCurrentProcessLog(result.Process, modeExceptions)
	}
	if result.Intent == exceptionmodule.IntentTaskManager || result.Intent == exceptionmodule.IntentThreadDump || result.Intent == exceptionmodule.IntentLogs {
		m.pushDrill(origin)
	}
	return tea.Batch(result.Command, command)
}

func (m *Model) handleExceptionKey(key string) tea.Cmd {
	m.syncExceptionContext()
	origin := m.routeCrumb()
	command := m.applyExceptionResult(m.exceptions.HandleKey(key))
	if key == "enter" {
		m.pushDrill(origin)
	}
	return command
}

func (m *Model) handleExceptionMouseClick(event tea.Mouse) tea.Cmd {
	m.syncExceptionContext()
	origin := m.routeCrumb()
	result := m.exceptions.HandleClick(event)
	command := m.applyExceptionResult(result)
	if result.Intent == exceptionmodule.IntentGraph {
		m.pushDrill(origin)
	}
	return command
}

func (m *Model) moveExceptionSelection(delta int) { m.exceptions.Move(delta) }

func (m Model) renderExceptions(width, height int) string {
	m.exceptions.Sync(m.exceptionContext())
	return m.exceptions.Render(width, height)
}

func (m Model) filteredExceptionIncidents() []flink.JobException {
	m.exceptions.Sync(m.exceptionContext())
	return m.exceptions.Incidents()
}

func (m *Model) configureMetrics() {
	m.metrics = metricmodule.New(m.client, m.requests.context)
	m.syncMetricContext()
}

func (m Model) metricContext() metricmodule.Context {
	return metricmodule.Context{
		Snapshot: m.snapshot, Selected: m.selected, Generation: m.generation,
		BodyHeight: m.bodyHeight(), ContentWidth: m.contentWidthAt(max(40, m.width)),
	}
}

func (m *Model) syncMetricContext() { m.metrics.Sync(m.metricContext()) }

func (m *Model) openMetricExplorer() tea.Cmd {
	if m.snapshot.JobID == "" || m.selected == "" {
		return nil
	}
	m.mode = modeMetricExplorer
	return m.metrics.Open(m.metricContext())
}

func (m *Model) fetchCustomMetricValues() tea.Cmd {
	m.metrics.Sync(m.metricContext())
	return m.metrics.Poll()
}

func (m *Model) handleMetricExplorerKey(key string) tea.Cmd {
	m.syncMetricContext()
	result := m.metrics.HandleKey(key)
	switch result.Intent {
	case metricmodule.IntentGraph:
		m.mode = modeGraph
		m.centerSelection()
	case metricmodule.IntentOverview:
		return tea.Batch(result.Command, m.openJobPicker())
	}
	return result.Command
}

func (m *Model) moveMetricSelection(delta int) { m.metrics.Move(delta) }

func (m *Model) refreshMetricWindow() tea.Cmd {
	m.syncMetricContext()
	return m.metrics.RefreshWindow()
}

func (m *Model) handleMetricMouseClick(event tea.Mouse) {
	m.syncMetricContext()
	m.metrics.HandleClick(event)
}

func (m Model) renderMetricExplorer(width, height int) string {
	m.metrics.Sync(m.metricContext())
	return m.metrics.Render(width, height)
}

func (m *Model) configureAccumulators() {
	m.accumulators = accumulatormodule.New(m.client, m.requests.context)
	m.syncAccumulatorContext()
}

func (m Model) accumulatorContext() accumulatormodule.Context {
	return accumulatormodule.Context{
		Snapshot: m.snapshot, Selected: m.selected, Generation: m.generation,
		BodyHeight: m.bodyHeight(), ContentWidth: m.contentWidthAt(max(40, m.width)),
	}
}

func (m *Model) syncAccumulatorContext() { m.accumulators.Sync(m.accumulatorContext()) }

func (m *Model) openAccumulators() tea.Cmd {
	if m.snapshot.JobID == "" || m.selected == "" {
		return nil
	}
	m.mode = modeAccumulators
	return m.accumulators.Open(m.accumulatorContext())
}

func (m *Model) fetchAccumulators() tea.Cmd {
	m.accumulators.Sync(m.accumulatorContext())
	return m.accumulators.Poll()
}

func (m *Model) handleAccumulatorKey(key string) tea.Cmd {
	m.syncAccumulatorContext()
	origin := m.routeCrumb()
	result := m.accumulators.HandleKey(key)
	switch result.Intent {
	case accumulatormodule.IntentGraph:
		m.mode = modeGraph
		m.centerSelection()
	case accumulatormodule.IntentOverview:
		return tea.Batch(result.Command, m.openJobPicker())
	case accumulatormodule.IntentDocument:
		m.openStaticDocument(result.Title, result.Content, modeAccumulators)
		m.pushDrill(origin)
	}
	return result.Command
}

func (m *Model) moveAccumulatorSelection(delta int) { m.accumulators.Move(delta) }

func (m *Model) handleAccumulatorMouseClick(event tea.Mouse) {
	m.syncAccumulatorContext()
	m.accumulators.HandleClick(event)
}

func (m Model) renderAccumulators(width, height int) string {
	m.accumulators.Sync(m.accumulatorContext())
	return m.accumulators.Render(width, height)
}

func (m *Model) configureFlameGraphs() {
	m.flameGraphs = flamegraphmodule.New(m.client, m.requests.context)
	m.syncFlameGraphContext()
}

func (m Model) flameGraphContext() flamegraphmodule.Context {
	return flamegraphmodule.Context{
		JobID: m.snapshot.JobID, Nodes: m.snapshot.Nodes, Selected: m.selected,
		Generation: m.generation, Width: m.width, Height: m.height,
		ContentWidth: m.contentWidthAt(max(40, m.width)),
	}
}

func (m *Model) syncFlameGraphContext() { m.flameGraphs.Sync(m.flameGraphContext()) }

func (m *Model) activateFlameGraph() flamegraphmodule.View {
	view := flamegraphmodule.ViewVertex
	if m.mode == modeProfilerFlameGraph {
		view = flamegraphmodule.ViewProfilerReport
	}
	m.flameGraphs.Activate(view)
	return view
}

func (m *Model) openFlameGraph() tea.Cmd {
	if m.snapshot.JobID == "" || m.selected == "" {
		return nil
	}
	command := m.flameGraphs.OpenVertex(m.flameGraphContext())
	m.mode = modeFlameGraph
	return command
}

func (m *Model) applyFlameGraphResult(result flamegraphmodule.Result) tea.Cmd {
	switch result.Intent {
	case flamegraphmodule.IntentGraph:
		m.mode = modeGraph
	case flamegraphmodule.IntentProfiler:
		m.mode = modeProfiler
	}
	return result.Command
}

func (m *Model) handleFlameGraphKey(key string) tea.Cmd {
	m.syncFlameGraphContext()
	m.activateFlameGraph()
	return m.applyFlameGraphResult(m.flameGraphs.HandleKey(key))
}

func (m *Model) handleFlameGraphMouseClick(event tea.Mouse) tea.Cmd {
	m.syncFlameGraphContext()
	m.activateFlameGraph()
	return m.flameGraphs.HandleClick(event)
}

func (m *Model) moveFlameGraphLinear(delta int) {
	m.activateFlameGraph()
	m.flameGraphs.HandleWheel(delta)
}

func (m *Model) fetchFlameGraph() tea.Cmd {
	m.flameGraphs.Sync(m.flameGraphContext())
	m.flameGraphs.Activate(flamegraphmodule.ViewVertex)
	return m.flameGraphs.Poll()
}

func (m Model) renderFlameGraph(width, height int) string {
	m.flameGraphs.Sync(m.flameGraphContext())
	view := flamegraphmodule.ViewVertex
	if m.mode == modeProfilerFlameGraph {
		view = flamegraphmodule.ViewProfilerReport
	}
	m.flameGraphs.Activate(view)
	return m.flameGraphs.Render(width, height)
}
