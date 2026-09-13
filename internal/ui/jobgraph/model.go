package jobgraph

import (
	"fmt"
	"math"

	"charm.land/lipgloss/v2"

	"github.com/nikitasavinov/flink-tui/internal/flink"
	"github.com/nikitasavinov/flink-tui/internal/graph"
)

const (
	nodeWidth  = 29
	nodeHeight = 5
)

type graphZoom int

const (
	graphZoomDetailed graphZoom = iota
	graphZoomCompact
	graphZoomTopology
)

// Zoom is the semantic graph detail level.
type Zoom = graphZoom

const (
	ZoomDetailed = graphZoomDetailed
	ZoomCompact  = graphZoomCompact
	ZoomTopology = graphZoomTopology
)

// MetricChange selects the live values receiving a short visual pulse.
type MetricChange struct {
	Busy         bool
	Backpressure bool
	Input        bool
	Output       bool
	Skew         bool
}

type metricChange = MetricChange

// Scene is the complete immutable input to one graph render.
type Scene struct {
	Nodes       []flink.Node
	Layout      graph.Layout
	Selected    string
	OffsetX     int
	OffsetY     int
	Zoom        Zoom
	Changes     map[string]MetricChange
	ShowMinimap bool
}

type graphZoomSpec struct {
	label                 string
	nodeWidth, nodeHeight int
	gapX, gapY            int
}

func zoomSpec(level graphZoom) graphZoomSpec {
	switch level {
	case graphZoomCompact:
		return graphZoomSpec{label: "70%", nodeWidth: 21, nodeHeight: 4, gapX: 7, gapY: 2}
	case graphZoomTopology:
		return graphZoomSpec{label: "40%", nodeWidth: 5, nodeHeight: 1, gapX: 4, gapY: 0}
	default:
		return graphZoomSpec{label: "100%", nodeWidth: nodeWidth, nodeHeight: nodeHeight, gapX: 9, gapY: 3}
	}
}

type Model struct {
	snapshot      flink.Snapshot
	layout        graph.Layout
	selected      string
	offsetX       int
	offsetY       int
	graphZoom     graphZoom
	metricChanges map[string]MetricChange
	showMinimap   bool
}

// Render draws a scene and crops it to the requested viewport.
func Render(scene Scene, width, height int) string {
	model := Model{
		snapshot: flink.Snapshot{Nodes: scene.Nodes}, layout: scene.Layout,
		selected: scene.Selected, offsetX: scene.OffsetX, offsetY: scene.OffsetY,
		graphZoom: scene.Zoom, metricChanges: scene.Changes,
		showMinimap: scene.ShowMinimap,
	}
	return model.renderGraph(width, height)
}

// Project maps a graph coordinate into a bounded miniature axis.
func Project(value, total, cells int) int { return project(value, total, cells) }

// Unproject maps a miniature coordinate back into graph space.
func Unproject(value, cells, total int) int { return unproject(value, cells, total) }

// PressureLevel normalizes Flink's optional backpressure classification.
func PressureLevel(metrics flink.Metrics) string { return pressureLevel(metrics) }

// PressureStyle returns the row/border style for a pressure level.
func PressureStyle(level string) lipgloss.Style {
	return terminalStyles()[pressureStyle(level)]
}

// PressureBadgeStyle returns the filled badge style for a pressure level.
func PressureBadgeStyle(level string) lipgloss.Style {
	return terminalStyles()[pressureBadgeStyle(level)]
}

// BusyStyle returns the live busy-metric style.
func BusyStyle() lipgloss.Style { return terminalStyles()[styleBusy] }

// OperationalSeverity scores the strongest live saturation signal on a
// vertex. Busy and backpressure share Flink's 0-100 scale, so the larger value
// answers the operator-facing question "which vertex is hottest right now?".
func OperationalSeverity(metrics flink.Metrics) float64 {
	busy, pressure := metrics.BusyPercent, metrics.BackpressurePercent
	if math.IsNaN(busy) {
		busy = 0
	}
	if math.IsNaN(pressure) {
		pressure = 0
	}
	return max(busy, pressure)
}

// HottestVertexID returns the most saturated vertex, retaining topological
// order as the stable tie-breaker.
func HottestVertexID(nodes []flink.Node, order []string) string {
	byID := make(map[string]flink.Node, len(nodes))
	for _, node := range nodes {
		byID[node.ID] = node
	}
	selected := ""
	severity := -1.0
	for _, id := range order {
		node, ok := byID[id]
		if !ok {
			continue
		}
		candidate := OperationalSeverity(node.Metrics)
		if selected == "" || candidate > severity {
			selected, severity = id, candidate
		}
	}
	if selected != "" {
		return selected
	}
	for _, node := range nodes {
		candidate := OperationalSeverity(node.Metrics)
		if selected == "" || candidate > severity {
			selected, severity = node.ID, candidate
		}
	}
	return selected
}

func rate(value float64) string {
	if value >= 1_000_000 {
		return fmt.Sprintf("%.1fM", value/1_000_000)
	}
	if value >= 1000 {
		return fmt.Sprintf("%.1fk", value/1000)
	}
	if value >= 100 {
		return fmt.Sprintf("%.0f", value)
	}
	return fmt.Sprintf("%.1f", value)
}
