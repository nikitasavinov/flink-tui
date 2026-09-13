package jobgraph

import (
	"strings"
	"testing"

	"github.com/nikitasavinov/flink-tui/internal/flink"
	"github.com/nikitasavinov/flink-tui/internal/graph"
)

func TestHottestVertexUsesStrongestOperationalSignal(t *testing.T) {
	nodes := []flink.Node{
		{ID: "source", Metrics: flink.Metrics{BackpressurePercent: 92.7}},
		{ID: "risk", Metrics: flink.Metrics{BusyPercent: 100}},
		{ID: "sink", Metrics: flink.Metrics{BusyPercent: 3}},
	}
	if got := HottestVertexID(nodes, []string{"source", "risk", "sink"}); got != "risk" {
		t.Fatalf("hottest vertex = %q, want risk", got)
	}
}

func TestDrawNodeShowsBusyAndBackpressure(t *testing.T) {
	node := flink.Node{
		ID:          "risk-features",
		Name:        "Risk Features",
		State:       "RUNNING",
		Parallelism: 2,
		Metrics: flink.Metrics{
			BusyPercent:         1,
			BackpressurePercent: 99.9,
			BackpressureLevel:   flink.BackpressureHigh,
		},
	}
	canvas := newCanvas(nodeWidth, nodeHeight)
	Model{}.drawNode(canvas, node, graph.Rect{W: nodeWidth, H: nodeHeight}, false)

	var metricLine strings.Builder
	for _, cell := range canvas.cells[2][1 : nodeWidth-1] {
		metricLine.WriteRune(cell.r)
	}
	if got := metricLine.String(); !strings.Contains(got, "BUSY   1% BP HIGH 100%") {
		t.Fatalf("metric line %q does not show busy and backpressure", got)
	}
	if got := canvas.cells[0][0].style; got != stylePressureHigh {
		t.Fatalf("high-pressure border style = %v, want %v", got, stylePressureHigh)
	}
	if got := canvas.cells[2][1+18].style; got != stylePressureHighBadge {
		t.Fatalf("high-pressure badge style = %v, want %v", got, stylePressureHighBadge)
	}
}

func TestBusyAndBackpressuredNodesHaveVisibleBorders(t *testing.T) {
	busy := flink.Node{State: "RUNNING", Metrics: flink.Metrics{BusyPercent: 90, BackpressureLevel: flink.BackpressureOK}}
	if got := nodeBorderStyle(busy); got != styleBusyBorder {
		t.Fatalf("busy border = %v, want %v", got, styleBusyBorder)
	}
	backpressured := flink.Node{State: "RUNNING", Metrics: flink.Metrics{BusyPercent: 90, BackpressureLevel: flink.BackpressureHigh}}
	if got := nodeBorderStyle(backpressured); got != stylePressureHigh {
		t.Fatalf("backpressure border = %v, want %v", got, stylePressureHigh)
	}
}

func TestChangedGraphMetricsReceivePulseStyles(t *testing.T) {
	node := flink.Node{
		ID: "source", Name: "Source", State: "RUNNING", Parallelism: 1,
		Metrics: flink.Metrics{BusyPercent: 80, BackpressurePercent: 35, RecordsInPerSecond: 123, RecordsOutPerSecond: 100, DataSkewPercent: 12},
	}
	model := Model{metricChanges: map[string]metricChange{
		"source": {Busy: true, Backpressure: true, Input: true, Output: true, Skew: true},
	}}
	canvas := newCanvas(nodeWidth, nodeHeight)
	model.drawNode(canvas, node, graph.Rect{W: nodeWidth, H: nodeHeight}, false)
	for _, point := range [][2]int{{6, 2}, {24, 2}, {4, 3}, {14, 3}, {21, 3}} {
		if got := canvas.cells[point[1]][point[0]].style; got != styleChanged {
			t.Fatalf("cell (%d,%d) style = %v, want changed", point[0], point[1], got)
		}
	}

	selected := newCanvas(nodeWidth, nodeHeight)
	model.drawNode(selected, node, graph.Rect{W: nodeWidth, H: nodeHeight}, true)
	if got := selected.cells[2][6].style; got != styleSelectedChanged {
		t.Fatalf("selected changed metric style = %v, want %v", got, styleSelectedChanged)
	}
}

func TestCompactNodeKeepsOperationalSignals(t *testing.T) {
	node := flink.Node{
		ID: "risk", Name: "Risk Evaluation", State: "RUNNING", Parallelism: 4,
		Metrics: flink.Metrics{BusyPercent: 91, BackpressureLevel: flink.BackpressureHigh},
	}
	spec := zoomSpec(graphZoomCompact)
	canvas := newCanvas(spec.nodeWidth, spec.nodeHeight)
	Model{graphZoom: graphZoomCompact}.drawNode(canvas, node, graph.Rect{W: spec.nodeWidth, H: spec.nodeHeight}, false)

	var metricLine strings.Builder
	for _, cell := range canvas.cells[2][1 : spec.nodeWidth-1] {
		metricLine.WriteRune(cell.r)
	}
	if got := metricLine.String(); !strings.Contains(got, "RUN  B 91% BP HIGH") {
		t.Fatalf("compact metric line %q lost operational signals", got)
	}
	if got := canvas.cells[0][0].style; got != stylePressureHigh {
		t.Fatalf("compact high-pressure border style = %v, want %v", got, stylePressureHigh)
	}
}

func TestTopologyNodeUsesTinyColoredLabel(t *testing.T) {
	node := flink.Node{
		ID: "risk", Name: "Risk Evaluation", State: "RUNNING",
		Metrics: flink.Metrics{BackpressureLevel: flink.BackpressureHigh},
	}
	spec := zoomSpec(graphZoomTopology)
	canvas := newCanvas(spec.nodeWidth, spec.nodeHeight)
	Model{graphZoom: graphZoomTopology}.drawNode(canvas, node, graph.Rect{W: spec.nodeWidth, H: spec.nodeHeight}, false)

	var label strings.Builder
	for _, cell := range canvas.cells[0] {
		label.WriteRune(cell.r)
	}
	if got := label.String(); got != "Risk " {
		t.Fatalf("topology label = %q, want %q", got, "Risk ")
	}
	for index, cell := range canvas.cells[0] {
		if cell.style != stylePressureHigh {
			t.Fatalf("topology cell %d style = %v, want high pressure", index, cell.style)
		}
	}
}

func TestEdgeStrategyLabelsFollowSemanticZoom(t *testing.T) {
	tests := []struct {
		strategy                    string
		detailed, compact, topology string
	}{
		{strategy: "FORWARD", detailed: "FORWARD", compact: "FWD"},
		{strategy: "HASH", detailed: "HASH", compact: "HASH", topology: "H"},
		{strategy: "REBALANCE", detailed: "REBAL", compact: "REB", topology: "R"},
		{strategy: "BROADCAST", detailed: "BCAST", compact: "BCST", topology: "B"},
		{strategy: "RESCALE", detailed: "RESCALE", compact: "RSCL", topology: "S"},
		{strategy: "RANDOM", detailed: "SHUFFLE", compact: "SHUF", topology: "X"},
	}
	for _, test := range tests {
		t.Run(test.strategy, func(t *testing.T) {
			if got := edgeStrategyLabel(test.strategy, graphZoomDetailed); got != test.detailed {
				t.Fatalf("detailed label = %q, want %q", got, test.detailed)
			}
			if got := edgeStrategyLabel(test.strategy, graphZoomCompact); got != test.compact {
				t.Fatalf("compact label = %q, want %q", got, test.compact)
			}
			if got := edgeStrategyLabel(test.strategy, graphZoomTopology); got != test.topology {
				t.Fatalf("topology label = %q, want %q", got, test.topology)
			}
		})
	}
}

func TestDrawEdgesPlacesStrategyBeforeArrow(t *testing.T) {
	source := flink.Node{ID: "source", Name: "Source"}
	target := flink.Node{
		ID: "target", Name: "Target",
		Inputs: []flink.Input{{ID: source.ID, ShipStrategy: "HASH"}},
	}
	model := Model{
		snapshot: flink.Snapshot{Nodes: []flink.Node{source, target}},
		layout: graph.Layout{Rects: map[string]graph.Rect{
			source.ID: {X: 2, Y: 1, W: nodeWidth, H: nodeHeight},
			target.ID: {X: 40, Y: 1, W: nodeWidth, H: nodeHeight},
		}},
	}
	canvas := newCanvas(80, 10)
	model.drawEdges(canvas)

	row := canvasRow(canvas, target.ID, model.layout.Rects[target.ID].CenterY())
	if !strings.Contains(row, "HASH>") {
		t.Fatalf("edge row %q does not contain HASH>", row)
	}
	labelX := model.layout.Rects[target.ID].X - 2 - len("HASH") + 1
	if got := canvas.cells[model.layout.Rects[target.ID].CenterY()][labelX].style; got != styleEdgeLabel {
		t.Fatalf("edge label style = %v, want %v", got, styleEdgeLabel)
	}
}

func TestMultiInputStrategiesUseSeparateTargetPorts(t *testing.T) {
	left := flink.Node{ID: "left", Name: "Left"}
	right := flink.Node{ID: "right", Name: "Right"}
	target := flink.Node{
		ID: "join", Name: "Join",
		Inputs: []flink.Input{
			{ID: left.ID, ShipStrategy: "HASH"},
			{ID: right.ID, ShipStrategy: "BROADCAST"},
		},
	}
	targetRect := graph.Rect{X: 40, Y: 5, W: nodeWidth, H: nodeHeight}
	model := Model{
		snapshot: flink.Snapshot{Nodes: []flink.Node{left, right, target}},
		layout: graph.Layout{Rects: map[string]graph.Rect{
			left.ID:   {X: 2, Y: 1, W: nodeWidth, H: nodeHeight},
			right.ID:  {X: 2, Y: 9, W: nodeWidth, H: nodeHeight},
			target.ID: targetRect,
		}},
	}
	canvas := newCanvas(80, 16)
	model.drawEdges(canvas)

	firstY := edgeTargetY(targetRect, 0, 2)
	secondY := edgeTargetY(targetRect, 1, 2)
	if firstY == secondY {
		t.Fatal("multi-input edges share a target port")
	}
	if row := canvasRow(canvas, target.ID, firstY); !strings.Contains(row, "HASH>") {
		t.Fatalf("first input row %q does not contain HASH>", row)
	}
	if row := canvasRow(canvas, target.ID, secondY); !strings.Contains(row, "BCAST>") {
		t.Fatalf("second input row %q does not contain BCAST>", row)
	}
}

func canvasRow(canvas *canvas, _ string, y int) string {
	var row strings.Builder
	for _, cell := range canvas.cells[y] {
		row.WriteRune(cell.r)
	}
	return row.String()
}
