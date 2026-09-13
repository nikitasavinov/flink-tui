package jobgraph

import (
	"fmt"
	"slices"
	"strings"

	"github.com/nikitasavinov/flink-tui/internal/flink"
	"github.com/nikitasavinov/flink-tui/internal/graph"
	"github.com/nikitasavinov/flink-tui/internal/ui/shared"
)

type edgeBits uint8

type edgeLabel struct {
	x, y     int
	text     string
	selected bool
}

const (
	edgeNorth edgeBits = 1 << iota
	edgeEast
	edgeSouth
	edgeWest
)

func (m Model) renderGraph(width, height int) string {
	if width <= 0 || height <= 0 {
		return ""
	}
	bounds := graphRect(width, height, m.minimapVisible())
	contentWidth := max(bounds.W, m.layout.Width)
	contentHeight := max(height, m.layout.Height)
	canvas := newCanvas(contentWidth, contentHeight)
	m.drawEdges(canvas)
	for _, node := range m.snapshot.Nodes {
		m.drawNode(canvas, node, m.layout.Rects[node.ID], node.ID == m.selected)
	}
	view := canvas.viewport(m.offsetX, m.offsetY, bounds.W, height)
	if minimap, ok := MinimapRect(width, height, m.minimapVisible()); ok {
		// Crop the graph before composing the map column. Nodes, edges, and
		// wide characters cannot spill into the map or the space below it.
		dock := newCanvas(width-bounds.W, height)
		minimap.X -= bounds.W
		m.drawMinimap(dock, minimap, bounds.W, height)
		graphLines := strings.Split(view, "\n")
		mapLines := strings.Split(dock.viewport(0, 0, dock.width, height), "\n")
		for index := range graphLines {
			graphLines[index] += mapLines[index]
		}
		return strings.Join(graphLines, "\n")
	}
	return view
}

func (m Model) drawEdges(canvas *canvas) {
	connections := make(map[[2]int]edgeBits)
	arrows := make(map[[2]int]bool)
	labels := make([]edgeLabel, 0)
	for _, node := range m.snapshot.Nodes {
		target, ok := m.layout.Rects[node.ID]
		if !ok {
			continue
		}
		for inputIndex, input := range node.Inputs {
			source, ok := m.layout.Rects[input.ID]
			if !ok {
				continue
			}
			startX, startY := source.X+source.W, source.CenterY()
			endX := target.X - 2
			endY := edgeTargetY(target, inputIndex, len(node.Inputs))
			midX := startX + (endX-startX)/2 + inputIndex
			addPath(connections, startX, startY, midX, startY)
			addPath(connections, midX, startY, midX, endY)
			addPath(connections, midX, endY, endX, endY)
			arrows[[2]int{target.X - 1, endY}] = true
			if label := edgeStrategyLabel(input.ShipStrategy, m.graphZoom); label != "" {
				labels = append(labels, edgeLabel{
					x:        endX - len([]rune(label)) + 1,
					y:        endY,
					text:     label,
					selected: node.ID == m.selected || input.ID == m.selected,
				})
			}
		}
	}
	for point, bits := range connections {
		canvas.set(point[0], point[1], edgeRune(bits), styleEdge)
	}
	drawEdgeLabels(canvas, labels)
	for point := range arrows {
		canvas.set(point[0], point[1], '>', styleEdge)
	}
}

func edgeTargetY(target graph.Rect, inputIndex, inputCount int) int {
	ports := target.H - 2
	if ports <= 1 || inputCount <= 1 {
		return target.CenterY()
	}
	inputIndex = shared.Clamp(inputIndex, 0, inputCount-1)
	position := (inputIndex*(ports-1) + (inputCount-1)/2) / (inputCount - 1)
	return target.Y + 1 + position
}

func drawEdgeLabels(canvas *canvas, labels []edgeLabel) {
	slices.SortStableFunc(labels, func(left, right edgeLabel) int {
		if left.selected == right.selected {
			return 0
		}
		if left.selected {
			return -1
		}
		return 1
	})
	occupied := make(map[[2]int]bool)
	for _, label := range labels {
		cells := make([][2]int, 0, len([]rune(label.text)))
		collides := false
		for offset := range []rune(label.text) {
			point := [2]int{label.x + offset, label.y}
			if occupied[point] {
				collides = true
				break
			}
			cells = append(cells, point)
		}
		if collides {
			continue
		}
		style := styleEdgeLabel
		if label.selected {
			style = styleMetric
		}
		canvas.text(label.x, label.y, label.text, style)
		for _, point := range cells {
			occupied[point] = true
		}
	}
}

func edgeStrategyLabel(strategy string, zoom graphZoom) string {
	strategy = strings.ToUpper(strings.TrimSpace(strategy))
	if strategy == "" {
		return ""
	}

	var detailed, compact, topology string
	switch {
	case strings.Contains(strategy, "FORWARD"):
		detailed, compact = "FORWARD", "FWD"
	case strings.Contains(strategy, "HASH") || strings.Contains(strategy, "KEY_GROUP"):
		detailed, compact, topology = "HASH", "HASH", "H"
	case strings.Contains(strategy, "REBALANCE"):
		detailed, compact, topology = "REBAL", "REB", "R"
	case strings.Contains(strategy, "BROADCAST"):
		detailed, compact, topology = "BCAST", "BCST", "B"
	case strings.Contains(strategy, "RESCALE"):
		detailed, compact, topology = "RESCALE", "RSCL", "S"
	case strings.Contains(strategy, "SHUFFLE") || strings.Contains(strategy, "RANDOM"):
		detailed, compact, topology = "SHUFFLE", "SHUF", "X"
	case strings.Contains(strategy, "RANGE"):
		detailed, compact, topology = "RANGE", "RNG", "G"
	case strings.Contains(strategy, "GLOBAL"):
		detailed, compact, topology = "GLOBAL", "GLB", "O"
	default:
		cleaned := strings.NewReplacer("_", "", "-", "", " ", "").Replace(strategy)
		detailed = clipNodeLabel(cleaned, 7)
		compact = clipNodeLabel(cleaned, 4)
		topology = clipNodeLabel(cleaned, 1)
	}

	switch zoom {
	case graphZoomCompact:
		return compact
	case graphZoomTopology:
		return topology
	default:
		return detailed
	}
}

func addPath(connections map[[2]int]edgeBits, x1, y1, x2, y2 int) {
	if x1 == x2 && y1 == y2 {
		return
	}
	dx, dy := sign(x2-x1), sign(y2-y1)
	x, y := x1, y1
	for x != x2 || y != y2 {
		nextX, nextY := x+dx, y+dy
		from, to := directionBits(dx, dy)
		connections[[2]int{x, y}] |= from
		connections[[2]int{nextX, nextY}] |= to
		x, y = nextX, nextY
	}
}

func directionBits(dx, dy int) (edgeBits, edgeBits) {
	switch {
	case dx > 0:
		return edgeEast, edgeWest
	case dx < 0:
		return edgeWest, edgeEast
	case dy > 0:
		return edgeSouth, edgeNorth
	default:
		return edgeNorth, edgeSouth
	}
}

func edgeRune(bits edgeBits) rune {
	switch bits {
	case edgeEast, edgeWest:
		return '─'
	case edgeNorth, edgeSouth:
		return '│'
	case edgeEast | edgeWest:
		return '─'
	case edgeNorth | edgeSouth:
		return '│'
	case edgeEast | edgeSouth:
		return '┌'
	case edgeWest | edgeSouth:
		return '┐'
	case edgeEast | edgeNorth:
		return '└'
	case edgeWest | edgeNorth:
		return '┘'
	case edgeNorth | edgeEast | edgeSouth:
		return '├'
	case edgeNorth | edgeWest | edgeSouth:
		return '┤'
	case edgeEast | edgeWest | edgeSouth:
		return '┬'
	case edgeEast | edgeWest | edgeNorth:
		return '┴'
	case edgeNorth | edgeEast | edgeSouth | edgeWest:
		return '┼'
	default:
		return '·'
	}
}

func (m Model) drawNode(canvas *canvas, node flink.Node, rect graph.Rect, selected bool) {
	if rect.H <= 1 {
		m.drawTopologyNode(canvas, node, rect, selected)
		return
	}

	borderStyle := nodeBorderStyle(node)
	if node.State == "FAILED" || node.State == "FAILING" || node.State == "CANCELED" {
		borderStyle = styleFailed
	} else if node.State != "RUNNING" {
		borderStyle = styleDim
	}

	canvas.set(rect.X, rect.Y, '┌', borderStyle)
	canvas.set(rect.X+rect.W-1, rect.Y, '┐', borderStyle)
	canvas.set(rect.X, rect.Y+rect.H-1, '└', borderStyle)
	canvas.set(rect.X+rect.W-1, rect.Y+rect.H-1, '┘', borderStyle)
	for x := rect.X + 1; x < rect.X+rect.W-1; x++ {
		canvas.set(x, rect.Y, '─', borderStyle)
		canvas.set(x, rect.Y+rect.H-1, '─', borderStyle)
	}
	for y := rect.Y + 1; y < rect.Y+rect.H-1; y++ {
		canvas.set(rect.X, y, '│', borderStyle)
		canvas.set(rect.X+rect.W-1, y, '│', borderStyle)
		if selected {
			for x := rect.X + 1; x < rect.X+rect.W-1; x++ {
				canvas.set(x, y, ' ', styleSelected)
			}
		}
	}

	if rect.H < nodeHeight || rect.W < nodeWidth {
		m.drawCompactNodeContent(canvas, node, rect, selected)
		return
	}
	m.drawDetailedNodeContent(canvas, node, rect, selected)
}

func (m Model) drawDetailedNodeContent(canvas *canvas, node flink.Node, rect graph.Rect, selected bool) {
	inner := rect.W - 2
	parallelism := fmt.Sprintf("x%d", node.Parallelism)
	nameWidth := max(1, inner-shared.DisplayWidth(parallelism)-1)
	lineOne := shared.PadRight(shared.Truncate(node.Name, nameWidth), nameWidth) + " " + parallelism
	level := pressureLevel(node.Metrics)
	lineTwo := fmt.Sprintf("%-4s BUSY %3.0f%% BP %-4s %3.0f%%",
		shortState(node.State),
		node.Metrics.BusyPercent,
		level,
		node.Metrics.BackpressurePercent,
	)
	textStyle := styleNormal
	metricStyle := styleMetric
	busyMetricStyle := styleBusy
	change := m.metricChanges[node.ID]
	if selected {
		textStyle, metricStyle = styleSelected, styleSelected
		busyMetricStyle = styleSelectedBusy
	}
	if change.Busy {
		busyMetricStyle = changedMetricStyle(selected)
	}
	canvas.text(rect.X+1, rect.Y+1, shared.PadRight(shared.Truncate(lineOne, inner), inner), textStyle)
	canvas.text(rect.X+1, rect.Y+2, shared.PadRight(shared.Truncate(lineTwo, inner), inner), textStyle)
	canvas.text(rect.X+1+5, rect.Y+2, fmt.Sprintf("BUSY %3.0f%%", node.Metrics.BusyPercent), busyMetricStyle)
	canvas.text(rect.X+1+18, rect.Y+2, shared.PadRight(level, 4), pressureBadgeStyle(level))
	if change.Backpressure {
		canvas.text(rect.X+1+23, rect.Y+2, fmt.Sprintf("%3.0f%%", node.Metrics.BackpressurePercent), changedMetricStyle(selected))
	}
	canvas.text(rect.X+1, rect.Y+3, strings.Repeat(" ", inner), metricStyle)
	x, limit := rect.X+1, rect.X+1+inner
	x = drawNodeText(canvas, x, rect.Y+3, limit, "in ", metricStyle)
	x = drawNodeText(canvas, x, rect.Y+3, limit, rate(node.Metrics.RecordsInPerSecond), metricPulseStyle(change.Input, selected, metricStyle))
	x = drawNodeText(canvas, x, rect.Y+3, limit, " > out ", metricStyle)
	x = drawNodeText(canvas, x, rect.Y+3, limit, rate(node.Metrics.RecordsOutPerSecond), metricPulseStyle(change.Output, selected, metricStyle))
	x = drawNodeText(canvas, x, rect.Y+3, limit, " SK ", metricStyle)
	drawNodeText(canvas, x, rect.Y+3, limit, fmt.Sprintf("%.0f%%", node.Metrics.DataSkewPercent), metricPulseStyle(change.Skew, selected, metricStyle))
}

func (m Model) drawCompactNodeContent(canvas *canvas, node flink.Node, rect graph.Rect, selected bool) {
	inner := rect.W - 2
	parallelism := fmt.Sprintf("x%d", node.Parallelism)
	nameWidth := max(1, inner-shared.DisplayWidth(parallelism)-1)
	lineOne := shared.PadRight(shared.Truncate(node.Name, nameWidth), nameWidth) + " " + parallelism
	level := pressureLevel(node.Metrics)
	lineTwo := fmt.Sprintf("%-4s B%3.0f%% BP %-4s", shortState(node.State), node.Metrics.BusyPercent, level)
	textStyle := styleNormal
	busyStyle := styleBusy
	if selected {
		textStyle = styleSelected
		busyStyle = styleSelectedBusy
	}
	if m.metricChanges[node.ID].Busy {
		busyStyle = changedMetricStyle(selected)
	}
	canvas.text(rect.X+1, rect.Y+1, shared.PadRight(shared.Truncate(lineOne, inner), inner), textStyle)
	canvas.text(rect.X+1, rect.Y+2, shared.PadRight(shared.Truncate(lineTwo, inner), inner), textStyle)
	canvas.text(rect.X+1+5, rect.Y+2, fmt.Sprintf("B%3.0f%%", node.Metrics.BusyPercent), busyStyle)
	canvas.text(rect.X+1+14, rect.Y+2, shared.PadRight(level, min(4, inner-14)), pressureBadgeStyle(level))
}

func (m Model) drawTopologyNode(canvas *canvas, node flink.Node, rect graph.Rect, selected bool) {
	style := nodeBorderStyle(node)
	if node.State == "FAILED" || node.State == "FAILING" || node.State == "CANCELED" {
		style = styleFailed
	} else if node.State != "RUNNING" {
		style = styleDim
	}
	if selected {
		style = styleSelected
	}
	canvas.text(rect.X, rect.Y, shared.PadRight(clipNodeLabel(node.Name, rect.W), rect.W), style)
}

func clipNodeLabel(value string, width int) string {
	return shared.Clip(value, width)
}

func nodeBorderStyle(node flink.Node) styleKind {
	level := pressureLevel(node.Metrics)
	if level == flink.BackpressureHigh || level == flink.BackpressureLow {
		return pressureStyle(level)
	}
	if node.Metrics.BusyPercent >= 70 {
		return styleBusyBorder
	}
	return stylePressureOK
}

func changedMetricStyle(selected bool) styleKind {
	if selected {
		return styleSelectedChanged
	}
	return styleChanged
}

func metricPulseStyle(changed, selected bool, normal styleKind) styleKind {
	if changed {
		return changedMetricStyle(selected)
	}
	return normal
}

func drawNodeText(canvas *canvas, x, y, limit int, value string, style styleKind) int {
	for _, character := range value {
		if x >= limit {
			break
		}
		canvas.set(x, y, character, style)
		x++
	}
	return x
}

func pressureLevel(metrics flink.Metrics) string {
	if metrics.BackpressureLevel != "" {
		return metrics.BackpressureLevel
	}
	return flink.ClassifyBackpressure(metrics.BackpressurePercent)
}

func pressureStyle(level string) styleKind {
	switch level {
	case flink.BackpressureHigh:
		return stylePressureHigh
	case flink.BackpressureLow:
		return stylePressureLow
	default:
		return stylePressureOK
	}
}

func pressureBadgeStyle(level string) styleKind {
	switch level {
	case flink.BackpressureHigh:
		return stylePressureHighBadge
	case flink.BackpressureLow:
		return stylePressureLowBadge
	default:
		return stylePressureOKBadge
	}
}

func shortState(state string) string {
	switch state {
	case "RUNNING":
		return "RUN"
	case "FAILED", "FAILING":
		return "FAIL"
	case "CANCELED", "CANCELING":
		return "CANC"
	case "RESTARTING":
		return "REST"
	case "INITIALIZING":
		return "INIT"
	default:
		return shared.Truncate(state, 4)
	}
}

func sign(value int) int {
	if value < 0 {
		return -1
	}
	if value > 0 {
		return 1
	}
	return 0
}
