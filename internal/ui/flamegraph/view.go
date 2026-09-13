package flamegraph

import (
	"cmp"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/nikitasavinov/flink-tui/internal/flink"
	"github.com/nikitasavinov/flink-tui/internal/ui/shared"
)

const (
	flameGraphControlRow    = 2
	flameGraphFirstFrameRow = 4
	flameGraphInspectorRows = 3
	flameGraphAxisWidth     = 4
)

type flameFrame struct {
	Path   string
	Parent string
	Depth  int
	Start  int64
	Node   flink.FlameGraphNode
}

type flameGraphView struct {
	frames     []flameFrame
	firstDepth int
	rowCount   int
	graphX     int
	graphWidth int
	total      int64
}

type flameControlHit struct {
	kind       string
	start, end int
}

func (m *Model) openFlameGraph() tea.Cmd {
	if m.snapshot.JobID == "" || m.selected == "" {
		return nil
	}
	if m.flameGraphType != flink.FlameGraphOnCPU &&
		m.flameGraphType != flink.FlameGraphOffCPU &&
		m.flameGraphType != flink.FlameGraphFull {
		m.flameGraphType = flink.FlameGraphFull
	}
	if m.flameGraphVertex != m.selected {
		m.flameGraphVertex = m.selected
		m.flameGraphSubtask = -1
		m.flameGraphSelected = ""
		m.flameGraphFocus = ""
		m.flameGraph = flink.FlameGraph{}
	}
	m.view = ViewVertex
	m.flameGraphBusy = true
	m.flameGraphErr = nil
	return m.fetchFlameGraph()
}

func (m *Model) handleFlameGraphKey(key string) tea.Cmd {
	profilerReport := m.view == ViewProfilerReport
	state := m.mutableFlameGraph()
	switch key {
	case "esc":
		if state.flameGraphFocus != "" && state.flameGraphFocus != "0" {
			m.zoomOutFlameGraph()
		} else if profilerReport {
			m.intent = IntentProfiler
		} else {
			m.intent = IntentGraph
		}
	case "backspace":
		m.zoomOutFlameGraph()
	case "g":
		if profilerReport {
			m.intent = IntentProfiler
		} else {
			m.intent = IntentGraph
		}
	case "up", "k":
		m.moveFlameGraphParent()
	case "down", "j":
		m.moveFlameGraphChild()
	case "left", "h":
		m.moveFlameGraphSibling(-1)
	case "right", "l":
		m.moveFlameGraphSibling(1)
	case "tab":
		m.moveFlameGraphLinear(1)
	case "shift+tab":
		m.moveFlameGraphLinear(-1)
	case "pgup":
		m.moveFlameGraphLinear(-5)
	case "pgdown":
		m.moveFlameGraphLinear(5)
	case "enter":
		m.zoomSelectedFlameGraph()
	case "home", "0":
		state.flameGraphFocus = "0"
		state.flameGraphSelected = "0"
	case "[":
		if profilerReport {
			return nil
		}
		return m.changeFlameGraphType(-1)
	case "]":
		if profilerReport {
			return nil
		}
		return m.changeFlameGraphType(1)
	case "S":
		if profilerReport {
			return nil
		}
		return m.changeFlameGraphSubtask(-1)
	case "s":
		if profilerReport {
			return nil
		}
		return m.changeFlameGraphSubtask(1)
	}
	return nil
}

func (m *Model) changeFlameGraphType(delta int) tea.Cmd {
	types := []flink.FlameGraphType{
		flink.FlameGraphOnCPU,
		flink.FlameGraphOffCPU,
		flink.FlameGraphFull,
	}
	position := 0
	for index, typeName := range types {
		if typeName == m.flameGraphType {
			position = index
			break
		}
	}
	m.flameGraphType = types[(position+delta+len(types))%len(types)]
	return m.reloadFlameGraph()
}

func (m *Model) setFlameGraphType(typeName flink.FlameGraphType) tea.Cmd {
	if m.flameGraphType == typeName {
		return nil
	}
	m.flameGraphType = typeName
	return m.reloadFlameGraph()
}

func (m *Model) changeFlameGraphSubtask(delta int) tea.Cmd {
	parallelism := m.flameGraphParallelism()
	if parallelism <= 0 {
		return nil
	}
	values := make([]int, 0, parallelism+1)
	values = append(values, -1)
	for subtask := 0; subtask < parallelism; subtask++ {
		values = append(values, subtask)
	}
	position := 0
	for index, subtask := range values {
		if subtask == m.flameGraphSubtask {
			position = index
			break
		}
	}
	m.flameGraphSubtask = values[(position+delta+len(values))%len(values)]
	return m.reloadFlameGraph()
}

func (m *Model) reloadFlameGraph() tea.Cmd {
	m.flameGraph = flink.FlameGraph{}
	m.flameGraphSelected = ""
	m.flameGraphFocus = ""
	m.flameGraphBusy = true
	m.flameGraphErr = nil
	return m.fetchFlameGraph()
}

func (m Model) flameGraphParallelism() int {
	for _, node := range m.snapshot.Nodes {
		if node.ID == m.flameGraphVertex {
			return node.Parallelism
		}
	}
	return 0
}

func (m Model) displayedFlameGraph() flameGraphViewState {
	if m.view == ViewProfilerReport {
		return m.profilerFlameGraph
	}
	return m.flameGraphViewState
}

func (m *Model) mutableFlameGraph() *flameGraphViewState {
	if m.view == ViewProfilerReport {
		return &m.profilerFlameGraph
	}
	return &m.flameGraphViewState
}

func syncFlameGraphViewSelection(state *flameGraphViewState) {
	if !state.flameGraph.Ready() || state.flameGraph.Root.Name == "" {
		state.flameGraphSelected = ""
		state.flameGraphFocus = ""
		return
	}
	if _, ok := flameGraphNodeAt(&state.flameGraph.Root, state.flameGraphFocus); !ok {
		state.flameGraphFocus = "0"
	}
	if _, ok := flameGraphNodeAt(&state.flameGraph.Root, state.flameGraphSelected); !ok ||
		!flamePathContains(state.flameGraphFocus, state.flameGraphSelected) {
		state.flameGraphSelected = state.flameGraphFocus
	}
}

func (m *Model) moveFlameGraphParent() {
	state := m.mutableFlameGraph()
	parent := flamePathParent(state.flameGraphSelected)
	if parent != "" && flamePathContains(state.flameGraphFocus, parent) {
		state.flameGraphSelected = parent
	}
}

func (m *Model) moveFlameGraphChild() {
	state := m.mutableFlameGraph()
	node, ok := flameGraphNodeAt(&state.flameGraph.Root, state.flameGraphSelected)
	if !ok || len(node.Children) == 0 {
		return
	}
	best := 0
	for index := 1; index < len(node.Children); index++ {
		if node.Children[index].Value > node.Children[best].Value {
			best = index
		}
	}
	state.flameGraphSelected = flamePathChild(state.flameGraphSelected, best)
}

func (m *Model) moveFlameGraphSibling(delta int) {
	state := m.mutableFlameGraph()
	parentPath := flamePathParent(state.flameGraphSelected)
	if parentPath == "" {
		return
	}
	parent, ok := flameGraphNodeAt(&state.flameGraph.Root, parentPath)
	if !ok || len(parent.Children) == 0 {
		return
	}
	parts := strings.Split(state.flameGraphSelected, "/")
	position, err := strconv.Atoi(parts[len(parts)-1])
	if err != nil {
		return
	}
	position = shared.Clamp(position+delta, 0, len(parent.Children)-1)
	state.flameGraphSelected = flamePathChild(parentPath, position)
}

func (m *Model) moveFlameGraphLinear(delta int) {
	frames := m.flameGraphFrames()
	if len(frames) == 0 || delta == 0 {
		return
	}
	state := m.mutableFlameGraph()
	position := 0
	for index, frame := range frames {
		if frame.Path == state.flameGraphSelected {
			position = index
			break
		}
	}
	position = shared.Clamp(position+delta, 0, len(frames)-1)
	state.flameGraphSelected = frames[position].Path
}

func (m *Model) zoomSelectedFlameGraph() {
	state := m.mutableFlameGraph()
	if _, ok := flameGraphNodeAt(&state.flameGraph.Root, state.flameGraphSelected); ok {
		state.flameGraphFocus = state.flameGraphSelected
	}
}

func (m *Model) zoomOutFlameGraph() {
	state := m.mutableFlameGraph()
	parent := flamePathParent(state.flameGraphFocus)
	if parent == "" {
		return
	}
	state.flameGraphFocus = parent
	state.flameGraphSelected = parent
}

func (m Model) flameGraphFrames() []flameFrame {
	state := m.displayedFlameGraph()
	if !state.flameGraph.Ready() || state.flameGraph.Root.Name == "" {
		return nil
	}
	focus := state.flameGraphFocus
	if focus == "" {
		focus = "0"
	}
	node, ok := flameGraphNodeAt(&state.flameGraph.Root, focus)
	if !ok {
		return nil
	}
	frames := make([]flameFrame, 0, 32)
	collectFlameFrames(node, focus, "", 0, 0, &frames)
	return frames
}

func collectFlameFrames(
	node *flink.FlameGraphNode,
	path string,
	parent string,
	depth int,
	start int64,
	frames *[]flameFrame,
) {
	*frames = append(*frames, flameFrame{
		Path:   path,
		Parent: parent,
		Depth:  depth,
		Start:  start,
		Node:   *node,
	})
	childStart := start
	for index := range node.Children {
		child := &node.Children[index]
		collectFlameFrames(child, flamePathChild(path, index), path, depth+1, childStart, frames)
		childStart += max(int64(0), child.Value)
	}
}

func flameGraphNodeAt(root *flink.FlameGraphNode, path string) (*flink.FlameGraphNode, bool) {
	if root == nil || path == "" {
		return nil, false
	}
	parts := strings.Split(path, "/")
	if len(parts) == 0 || parts[0] != "0" {
		return nil, false
	}
	node := root
	for _, part := range parts[1:] {
		index, err := strconv.Atoi(part)
		if err != nil || index < 0 || index >= len(node.Children) {
			return nil, false
		}
		node = &node.Children[index]
	}
	return node, true
}

func flamePathChild(path string, index int) string {
	return path + "/" + strconv.Itoa(index)
}

func flamePathParent(path string) string {
	separator := strings.LastIndex(path, "/")
	if separator < 0 {
		return ""
	}
	return path[:separator]
}

func flamePathContains(parent, child string) bool {
	return child == parent || parent != "" && strings.HasPrefix(child, parent+"/")
}

func (m Model) renderFlameGraph(width, height int) string {
	profilerReport := m.view == ViewProfilerReport
	title := ""
	if profilerReport {
		title = fmt.Sprintf(" PROCESS PROFILE FLAME GRAPH  %s  |  %s", m.process.Label(), shared.Fallback(m.profilerReportName, "captured profile"))
	} else {
		name := m.nodeName(m.flameGraphVertex)
		parallelism := m.flameGraphParallelism()
		title = fmt.Sprintf(" VERTEX FLAME GRAPH  %s  x%d", name, parallelism)
	}
	if m.flameGraphBusy && !profilerReport {
		title += "  |  sampling..."
	}
	lines := []string{
		lipgloss.NewStyle().Bold(true).Foreground(shared.C("#E2E8F0")).Render(shared.Truncate(title, width)),
		m.renderFlameGraphStatus(width),
	}
	controls := m.renderProfilerFlameGraphControls(width)
	if !profilerReport {
		controls, _ = m.flameGraphControls(width)
	}
	lines = append(lines, controls)
	separator := lipgloss.NewStyle().Foreground(shared.C("#334155")).Render(strings.Repeat("-", width))
	lines = append(lines, separator)

	view := m.flameGraphView(width, height)
	if len(view.frames) == 0 {
		message := m.flameGraphEmptyMessage()
		lines = append(lines, lipgloss.NewStyle().Foreground(shared.C("#94A3B8")).Render(
			shared.PadRight(shared.Truncate(" "+message, width), width)))
		for row := 1; row < view.rowCount; row++ {
			lines = append(lines, strings.Repeat(" ", width))
		}
	} else {
		for depth := view.firstDepth; depth < view.firstDepth+view.rowCount; depth++ {
			lines = append(lines, m.renderFlameGraphDepth(view, depth, width))
		}
	}
	lines = append(lines, separator)
	lines = append(lines, m.renderSelectedFlameFrame(width)...)
	return shared.FitLines(lines, width, height)
}

func (m Model) renderProfilerFlameGraphControls(width int) string {
	line := " captured async-profiler report  |  arrows navigate  |  enter zoom  |  esc/backspace zoom out  |  home reset"
	return lipgloss.NewStyle().Foreground(shared.C("#94A3B8")).Background(shared.C("#111827")).Render(
		shared.PadRight(shared.Truncate(line, width), width),
	)
}

func (m Model) renderFlameGraphStatus(width int) string {
	state := m.displayedFlameGraph()
	requestErr := m.flameGraphErr
	if m.view == ViewProfilerReport {
		requestErr = nil
	}
	style := lipgloss.NewStyle().Foreground(shared.C("#94A3B8"))
	status := " Starting Flink stack sampling..."
	switch {
	case requestErr != nil:
		status = " Flame graph request failed: " + shared.ErrorText(requestErr)
		style = style.Foreground(shared.C("#FB7185"))
	case state.flameGraph.Disabled():
		status = " Flame graphs are disabled; set rest.flamegraph.enabled: true on the cluster."
		style = style.Foreground(shared.C("#FB7185")).Bold(true)
	case state.flameGraph.Sampling():
		status = " Collecting the first samples; Flink normally needs a few seconds."
		style = style.Foreground(shared.C("#FBBF24"))
	case state.flameGraph.Ready():
		status = fmt.Sprintf(" %s samples  |  captured %s  |  focus %s",
			humanCount(float64(state.flameGraph.Root.Value)),
			state.flameGraph.EndTimestamp.Format("15:04:05"),
			shortFlameFrameName(m.flameGraphFocusName()),
		)
		if state.flameGraph.Root.Value == 0 {
			status = " Sampling completed, but this mode captured no stack samples."
		}
	}
	return style.Render(shared.PadRight(shared.Truncate(status, width), width))
}

func (m Model) flameGraphEmptyMessage() string {
	state := m.displayedFlameGraph()
	requestErr := m.flameGraphErr
	busy := m.flameGraphBusy
	if m.view == ViewProfilerReport {
		requestErr = nil
		busy = false
	}
	switch {
	case requestErr != nil:
		return "No flame graph available; press r to retry."
	case state.flameGraph.Disabled():
		return "Cluster flame-graph support is disabled."
	case state.flameGraph.Sampling() || busy:
		return "Waiting for Flink's first stack sample..."
	case state.flameGraph.Ready():
		return "No samples were captured for this type and subtask."
	default:
		return "Starting stack sampling..."
	}
}

func (m Model) flameGraphFocusName() string {
	state := m.displayedFlameGraph()
	path := state.flameGraphFocus
	if path == "" {
		path = "0"
	}
	node, ok := flameGraphNodeAt(&state.flameGraph.Root, path)
	if !ok {
		return "root"
	}
	return node.Name
}

func (m Model) flameGraphView(width, height int) flameGraphView {
	state := m.displayedFlameGraph()
	frames := m.flameGraphFrames()
	rowCount := max(1, height-flameGraphFirstFrameRow-flameGraphInspectorRows)
	view := flameGraphView{
		frames:     frames,
		rowCount:   rowCount,
		graphX:     min(flameGraphAxisWidth, max(0, width-1)),
		graphWidth: max(1, width-min(flameGraphAxisWidth, max(0, width-1))),
		total:      1,
	}
	if len(frames) == 0 {
		return view
	}
	view.total = max(int64(1), frames[0].Node.Value)
	maxDepth := 0
	selectedDepth := 0
	for _, frame := range frames {
		maxDepth = max(maxDepth, frame.Depth)
		if frame.Path == state.flameGraphSelected {
			selectedDepth = frame.Depth
		}
	}
	view.firstDepth = shared.Clamp(selectedDepth-rowCount/2, 0, max(0, maxDepth-rowCount+1))
	return view
}

func (m Model) renderFlameGraphDepth(view flameGraphView, depth, width int) string {
	state := m.displayedFlameGraph()
	rowFrames := make([]flameFrame, 0)
	for _, frame := range view.frames {
		if frame.Depth == depth {
			rowFrames = append(rowFrames, frame)
		}
	}
	slices.SortStableFunc(rowFrames, func(left, right flameFrame) int {
		return cmp.Compare(left.Start, right.Start)
	})
	prefix := lipgloss.NewStyle().Foreground(shared.C("#64748B")).Render(
		shared.PadRight(fmt.Sprintf("%2d ", depth), view.graphX))
	var line strings.Builder
	line.WriteString(prefix)
	cursor := 0
	for _, frame := range rowFrames {
		left, right := flameFrameColumns(frame, view.total, view.graphWidth)
		left = max(left, cursor)
		if left > cursor {
			line.WriteString(strings.Repeat(" ", left-cursor))
		}
		if right <= left {
			continue
		}
		segmentWidth := right - left
		label := ""
		if segmentWidth >= 3 {
			label = shortFlameFrameName(frame.Node.Name)
		}
		segment := shared.PadRight(shared.Clip(" "+label, segmentWidth), segmentWidth)
		line.WriteString(flameFrameStyle(frame, frame.Path == state.flameGraphSelected).Render(segment))
		cursor = right
	}
	if cursor < view.graphWidth {
		line.WriteString(strings.Repeat(" ", view.graphWidth-cursor))
	}
	return line.String()
}

func flameFrameColumns(frame flameFrame, total int64, width int) (int, int) {
	if width <= 0 {
		return 0, 0
	}
	if frame.Depth == 0 && frame.Node.Value <= 0 {
		return 0, width
	}
	left := int(math.Floor(float64(max(int64(0), frame.Start)) / float64(total) * float64(width)))
	right := int(math.Ceil(float64(max(int64(0), frame.Start+frame.Node.Value)) / float64(total) * float64(width)))
	left = shared.Clamp(left, 0, width)
	right = shared.Clamp(right, 0, width)
	if frame.Node.Value > 0 && right == left && left < width {
		right++
	}
	return left, right
}

func flameFrameStyle(frame flameFrame, selected bool) lipgloss.Style {
	if selected {
		return lipgloss.NewStyle().Foreground(shared.C("#111827")).Background(shared.C("#FBBF24")).Bold(true)
	}
	palette := []string{"#6D28D9", "#1D4ED8", "#0E7490", "#047857", "#B45309", "#BE185D"}
	hash := frame.Depth
	for _, value := range []byte(frame.Node.Name) {
		hash = (hash*33 + int(value)) & 0x7fffffff
	}
	return lipgloss.NewStyle().Foreground(shared.C("#F8FAFC")).Background(shared.C(palette[hash%len(palette)]))
}

func shortFlameFrameName(name string) string {
	name = shared.SanitizeLine(name)
	if name == "" || name == "root" {
		return shared.Fallback(name, "root")
	}
	base, suffix := name, ""
	if colon := strings.LastIndex(base, ":"); colon >= 0 {
		suffix = base[colon:]
		base = base[:colon]
	}
	parts := strings.Split(base, ".")
	if len(parts) >= 2 {
		base = parts[len(parts)-2] + "." + parts[len(parts)-1]
	}
	return base + suffix
}

func (m Model) renderSelectedFlameFrame(width int) []string {
	state := m.displayedFlameGraph()
	frames := m.flameGraphFrames()
	var selected flameFrame
	found := false
	for _, frame := range frames {
		if frame.Path == state.flameGraphSelected {
			selected, found = frame, true
			break
		}
	}
	if !found {
		return []string{
			lipgloss.NewStyle().Foreground(shared.C("#64748B")).Render(shared.PadRight(shared.Truncate(" Select a stack frame for details.", width), width)),
			strings.Repeat(" ", width),
		}
	}
	rootTotal := max(int64(1), state.flameGraph.Root.Value)
	focusTotal := rootTotal
	if node, ok := flameGraphNodeAt(&state.flameGraph.Root, state.flameGraphFocus); ok {
		focusTotal = max(int64(1), node.Value)
	}
	lineOne := fmt.Sprintf(" selected  depth %d  |  samples %s  |  root %5.1f%%  |  focus %5.1f%%  |  children %d",
		selected.Depth,
		humanCount(float64(selected.Node.Value)),
		100*float64(selected.Node.Value)/float64(rootTotal),
		100*float64(selected.Node.Value)/float64(focusTotal),
		len(selected.Node.Children),
	)
	lineTwo := " frame     " + selected.Node.Name
	return []string{
		lipgloss.NewStyle().Foreground(shared.C("#FBBF24")).Bold(true).Render(shared.PadRight(shared.Truncate(lineOne, width), width)),
		lipgloss.NewStyle().Foreground(shared.C("#CBD5E1")).Render(shared.PadRight(shared.Truncate(lineTwo, width), width)),
	}
}

func (m Model) flameGraphControls(width int) (string, []flameControlHit) {
	typeLabel := map[flink.FlameGraphType]string{
		flink.FlameGraphOnCPU:  "On-CPU",
		flink.FlameGraphOffCPU: "Off-CPU",
		flink.FlameGraphFull:   "Mixed",
	}
	if width < 64 {
		typeLabel[flink.FlameGraphOnCPU] = "ON"
		typeLabel[flink.FlameGraphOffCPU] = "OFF"
		typeLabel[flink.FlameGraphFull] = "MIX"
	}
	var line strings.Builder
	hits := make([]flameControlHit, 0, 6)
	cursor := 0
	appendPiece := func(raw string, style lipgloss.Style, kind string) {
		if cursor >= width {
			return
		}
		raw = shared.Clip(raw, width-cursor)
		start := cursor
		cursor += shared.DisplayWidth(raw)
		line.WriteString(style.Render(raw))
		if kind != "" && cursor > start {
			hits = append(hits, flameControlHit{kind: kind, start: start, end: cursor})
		}
	}
	dim := lipgloss.NewStyle().Foreground(shared.C("#94A3B8"))
	button := lipgloss.NewStyle().Foreground(shared.C("#CBD5E1")).Background(shared.C("#1E293B"))
	active := lipgloss.NewStyle().Foreground(shared.C("#FFFFFF")).Background(shared.C("#6D28D9")).Bold(true)
	appendPiece(" type ", dim, "")
	for _, typeName := range []flink.FlameGraphType{flink.FlameGraphOnCPU, flink.FlameGraphOffCPU, flink.FlameGraphFull} {
		style := button
		if typeName == m.flameGraphType {
			style = active
		}
		appendPiece(" "+typeLabel[typeName]+" ", style, "type:"+string(typeName))
		appendPiece(" ", dim, "")
	}
	appendPiece("  subtask ", dim, "")
	appendPiece(" < ", button, "subtask:prev")
	scope := "all"
	if m.flameGraphSubtask >= 0 {
		scope = strconv.Itoa(m.flameGraphSubtask)
	}
	appendPiece(" "+scope+" ", active, "subtask:next")
	appendPiece(" > ", button, "subtask:next")
	if cursor < width {
		line.WriteString(strings.Repeat(" ", width-cursor))
	}
	return line.String(), hits
}

func (m *Model) handleFlameGraphMouseClick(event tea.Mouse) tea.Cmd {
	if event.Button != tea.MouseLeft {
		return nil
	}
	width := m.contentWidthAt(max(40, m.width))
	height := max(3, max(14, m.height)-headerHeight-footerHeight)
	bodyY := event.Y - headerHeight
	if bodyY == flameGraphControlRow {
		if m.view == ViewProfilerReport {
			return nil
		}
		_, hits := m.flameGraphControls(width)
		for _, hit := range hits {
			if event.X < hit.start || event.X >= hit.end {
				continue
			}
			switch hit.kind {
			case "type:" + string(flink.FlameGraphOnCPU):
				return m.setFlameGraphType(flink.FlameGraphOnCPU)
			case "type:" + string(flink.FlameGraphOffCPU):
				return m.setFlameGraphType(flink.FlameGraphOffCPU)
			case "type:" + string(flink.FlameGraphFull):
				return m.setFlameGraphType(flink.FlameGraphFull)
			case "subtask:prev":
				return m.changeFlameGraphSubtask(-1)
			case "subtask:next":
				return m.changeFlameGraphSubtask(1)
			}
		}
		return nil
	}
	view := m.flameGraphView(width, height)
	state := m.mutableFlameGraph()
	position := bodyY - flameGraphFirstFrameRow
	if position < 0 || position >= view.rowCount || event.X < view.graphX {
		return nil
	}
	depth := view.firstDepth + position
	graphX := event.X - view.graphX
	for _, frame := range view.frames {
		if frame.Depth != depth {
			continue
		}
		left, right := flameFrameColumns(frame, view.total, view.graphWidth)
		if graphX >= left && graphX < right {
			if state.flameGraphSelected == frame.Path && state.flameGraphFocus != frame.Path {
				state.flameGraphFocus = frame.Path
			} else {
				state.flameGraphSelected = frame.Path
			}
			return nil
		}
	}
	return nil
}
