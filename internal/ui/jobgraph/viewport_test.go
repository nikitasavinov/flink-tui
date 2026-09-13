package jobgraph

import (
	"testing"

	tea "charm.land/bubbletea/v2"

	"github.com/nikitasavinov/flink-tui/internal/flink"
	"github.com/nikitasavinov/flink-tui/internal/graph"
)

func TestViewportZoomKeepsSelectionAnchored(t *testing.T) {
	nodes := viewportTestChain(4)
	viewport := NewViewport(false)
	layout := viewport.BuildLayout(nodes)
	selected := nodes[2].ID
	viewport.Center(layout, selected, 80, 13)
	oldRect := layout.Rects[selected]
	oldState := viewport.State()
	oldX := oldRect.X + oldRect.W/2 - oldState.OffsetX
	oldY := oldRect.Y + oldRect.H/2 - oldState.OffsetY

	layout = viewport.ChangeZoom(1, nodes, layout, selected, 80, 13)
	state := viewport.State()
	if state.Zoom != ZoomCompact {
		t.Fatalf("zoom = %v, want compact", state.Zoom)
	}
	rect := layout.Rects[selected]
	if rect.W != 21 || rect.H != 4 {
		t.Fatalf("compact node = %dx%d", rect.W, rect.H)
	}
	if got := rect.X + rect.W/2 - state.OffsetX; got != oldX {
		t.Fatalf("selected screen x = %d, want %d", got, oldX)
	}
	if got := rect.Y + rect.H/2 - state.OffsetY; got != oldY {
		t.Fatalf("selected screen y = %d, want %d", got, oldY)
	}
}

func TestViewportFitChoosesMostDetailedLevelThatFits(t *testing.T) {
	nodes := viewportTestChain(8)
	viewport := NewViewport(false)
	layout := viewport.BuildLayout(nodes)
	layout = viewport.Fit(nodes, layout, nodes[3].ID, 80, 13)
	state := viewport.State()
	if state.Zoom != ZoomTopology {
		t.Fatalf("fit zoom = %v, want topology", state.Zoom)
	}
	if layout.Width > 80 || layout.Height > 13 || state.OffsetX != 0 || state.OffsetY != 0 {
		t.Fatalf("fit layout=%dx%d offset=(%d,%d)", layout.Width, layout.Height, state.OffsetX, state.OffsetY)
	}
}

func TestViewportControlWheelZoomsAroundPointerAndNodesRemainClickable(t *testing.T) {
	nodes := viewportTestChain(4)
	viewport := NewViewport(false)
	layout := viewport.BuildLayout(nodes)
	viewport.Wheel(tea.Mouse{X: 40, Y: 8, Button: tea.MouseWheelDown, Mod: tea.ModCtrl}, nodes, layout, 80, 13, 3)
	if viewport.State().Zoom != ZoomCompact {
		t.Fatalf("ctrl+wheel zoom = %v", viewport.State().Zoom)
	}

	state := viewport.State()
	state.Zoom = ZoomTopology
	viewport.RestoreState(state)
	layout = viewport.BuildLayout(nodes)
	target := layout.Rects[nodes[3].ID]
	selected := viewport.Click(tea.Mouse{X: target.X - viewport.State().OffsetX, Y: 3 + target.Y - viewport.State().OffsetY, Button: tea.MouseLeft}, layout, nodes[0].ID, 80, 13, 3)
	if selected != nodes[3].ID {
		t.Fatalf("topology click selected %q", selected)
	}
}

func TestViewportAutoFitsExactlyOnce(t *testing.T) {
	nodes := viewportTestChain(8)
	viewport := NewViewport(true)
	layout := viewport.BuildLayout(nodes)
	if _, fitted := viewport.AutoFit(nodes, layout, nodes[0].ID, 0, 0); fitted {
		t.Fatal("auto-fit ran without a viewport")
	}
	_, fitted := viewport.AutoFit(nodes, layout, nodes[0].ID, 80, 13)
	if !fitted || viewport.State().AutoFitPending || viewport.State().Zoom != ZoomTopology {
		t.Fatalf("first fit = fitted:%t state:%#v", fitted, viewport.State())
	}
	state := viewport.State()
	state.Zoom, state.OffsetX = ZoomCompact, 25
	viewport.RestoreState(state)
	layout = viewport.BuildLayout(nodes)
	if _, fitted = viewport.AutoFit(nodes, layout, nodes[0].ID, 80, 13); fitted || viewport.State().Zoom != ZoomCompact || viewport.State().OffsetX != 25 {
		t.Fatalf("second fit changed manual viewport: fitted:%t state:%#v", fitted, viewport.State())
	}
}

func TestViewportDragHonorsThresholdAndRelease(t *testing.T) {
	viewport := NewViewport(false)
	state := viewport.State()
	state.OffsetX, state.OffsetY = 80, 4
	viewport.RestoreState(state)
	layout := graph.Layout{Order: []string{"node"}, Rects: map[string]graph.Rect{"node": {X: 2, Y: 2, W: 29, H: 5}}, Width: 400, Height: 40}
	selected := viewport.Click(tea.Mouse{X: 60, Y: 13, Button: tea.MouseLeft}, layout, "node", 80, 13, 3)
	if selected != "node" || !viewport.DragActive() {
		t.Fatalf("empty press selected=%q drag=%t", selected, viewport.DragActive())
	}
	viewport.Motion(tea.Mouse{X: 59, Y: 13, Button: tea.MouseLeft}, layout, 80, 13, 3)
	if got := viewport.State(); got.OffsetX != 80 || got.OffsetY != 4 {
		t.Fatalf("one-cell jitter moved viewport: %#v", got)
	}
	viewport.Motion(tea.Mouse{X: 50, Y: 8, Button: tea.MouseLeft}, layout, 80, 13, 3)
	if got := viewport.State(); got.OffsetX != 90 || got.OffsetY != 9 {
		t.Fatalf("drag state = %#v", got)
	}
	viewport.Release(80)
	if viewport.DragActive() {
		t.Fatal("release left drag active")
	}
}

func TestViewportMinimapPolicyAndNarrowRelease(t *testing.T) {
	viewport := NewViewport(false)
	if viewport.MinimapVisible(true, 80) {
		t.Fatal("narrow minimap started visible")
	}
	viewport.ToggleMinimap(80)
	if !viewport.MinimapVisible(true, 80) {
		t.Fatal("narrow minimap did not open")
	}
	layout := graph.Layout{Order: []string{"node"}, Rects: map[string]graph.Rect{"node": {X: 2, Y: 2, W: 29, H: 5}}, Width: 400, Height: 40}
	minimap, ok := viewport.MinimapRect(80, 13, true)
	if !ok {
		t.Fatal("open minimap has no bounds")
	}
	viewport.Click(tea.Mouse{X: minimap.X + minimap.W - 2, Y: 3 + minimap.Y + minimap.H - 2, Button: tea.MouseLeft}, layout, "node", 80, 13, 3)
	if !viewport.DragActive() || viewport.State().OffsetX == 0 {
		t.Fatalf("minimap click state = %#v", viewport.State())
	}
	viewport.Release(80)
	if viewport.MinimapVisible(true, 80) || viewport.DragActive() {
		t.Fatalf("narrow release state = %#v", viewport.State())
	}

	viewport.ToggleMinimap(120)
	if viewport.MinimapVisible(true, 120) {
		t.Fatal("wide minimap hidden toggle was ignored")
	}
}

func viewportTestChain(count int) []flink.Node {
	nodes := make([]flink.Node, count)
	for index := range nodes {
		nodes[index] = flink.Node{ID: string(rune('a' + index)), Name: "Stage", State: "RUNNING"}
		if index > 0 {
			nodes[index].Inputs = []flink.Input{{ID: nodes[index-1].ID}}
		}
	}
	return nodes
}
