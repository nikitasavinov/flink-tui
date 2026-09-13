package jobgraph

import (
	"fmt"
	"strings"
	"testing"

	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/x/ansi"

	"github.com/nikitasavinov/flink-tui/internal/flink"
	"github.com/nikitasavinov/flink-tui/internal/graph"
)

func TestMinimapBoundsLeaveRoomForGraph(t *testing.T) {
	for _, test := range []struct {
		width, height int
		visible       bool
		want          bool
	}{
		{42, 20, true, false},
		{43, 20, true, true},
		{60, 20, true, true},
		{120, 20, true, true},
		{120, 4, true, false},
		{120, 5, true, true},
		{120, 20, false, false},
	} {
		t.Run(fmt.Sprintf("%dx%d-visible-%t", test.width, test.height, test.visible), func(t *testing.T) {
			rect, ok := MinimapRect(test.width, test.height, test.visible)
			if ok != test.want {
				t.Fatalf("map bounds available=%t, want %t: %#v", ok, test.want, rect)
			}
			if !ok {
				return
			}
			if rect.X-1 < 29 || rect.W < 12 || rect.W > 30 || rect.X+rect.W != test.width-1 || rect.Y+rect.H > test.height {
				t.Fatalf("map does not leave a complete detailed card and separate margins: %#v", rect)
			}
		})
	}
}

func TestMinimapDockDoesNotContainGraphContent(t *testing.T) {
	for _, offset := range []struct{ x, y int }{{0, 0}, {17, 8}, {151, 31}} {
		t.Run(fmt.Sprintf("pan-%d-%d", offset.x, offset.y), func(t *testing.T) {
			const width, height = 90, 20
			minimap, ok := MinimapRect(width, height, true)
			if !ok {
				t.Fatal("fixture has no map bounds")
			}
			graphWidth := minimap.X - 1
			node := flink.Node{ID: "wide", Name: "界界界界界界界界", State: "RUNNING"}
			// The label crosses the pane boundary below the map, where an
			// overlay implementation would leave a second piece of graph.
			layout := graph.Layout{
				Order: []string{node.ID},
				Rects: map[string]graph.Rect{node.ID: {X: offset.x + graphWidth - 8, Y: offset.y + minimap.H + 1, W: 29, H: 5}},
				Width: offset.x + width + 40, Height: offset.y + height + 10,
			}
			scene := Scene{Nodes: []flink.Node{node}, Layout: layout, Selected: node.ID, OffsetX: offset.x, OffsetY: offset.y, ShowMinimap: true}
			lines := strings.Split(ansi.Strip(Render(scene, width, height)), "\n")
			scene.ShowMinimap = false
			graphLines := strings.Split(ansi.Strip(Render(scene, graphWidth, height)), "\n")
			if len(lines) != height {
				t.Fatalf("rendered %d rows, want %d", len(lines), height)
			}
			for y, line := range lines {
				if ansi.StringWidth(line) != width {
					t.Fatalf("row %d has width %d, want %d", y, ansi.StringWidth(line), width)
				}
				if left := ansi.Cut(line, 0, graphWidth); left != graphLines[y] {
					t.Fatalf("map composition changed the graph at row %d: %q != %q", y, left, graphLines[y])
				}
				if ansi.Cut(line, graphWidth, graphWidth+1) != " " || ansi.Cut(line, width-1, width) != " " {
					t.Fatalf("graph or wide character entered a map margin at row %d: %q", y, line)
				}
				if y >= minimap.Y+minimap.H && strings.TrimSpace(ansi.Cut(line, minimap.X, width)) != "" {
					t.Fatalf("graph leaked into the map column below its frame at row %d: %q", y, line)
				}
			}
			if ansi.Cut(lines[minimap.Y], minimap.X, minimap.X+1) != "┌" ||
				ansi.Cut(lines[minimap.Y+minimap.H-1], minimap.X+minimap.W-1, minimap.X+minimap.W) != "┘" {
				t.Fatal("panning or a wide label displaced the map frame")
			}
		})
	}
}

func TestMinimapFitUsesOnlyUnobscuredGraphWidth(t *testing.T) {
	for _, automatic := range []bool{false, true} {
		t.Run(fmt.Sprintf("automatic-%t", automatic), func(t *testing.T) {
			const width, height = 120, 20
			nodes := viewportTestChain(3)
			viewport := NewViewport(automatic)
			layout := viewport.BuildLayout(nodes)
			minimap, _ := viewport.MinimapRect(width, height, true)
			if layout.Width > width || layout.Width <= minimap.X-1 {
				t.Fatal("fixture must fit the full panel but need zooming beside the map")
			}
			if automatic {
				var fitted bool
				layout, fitted = viewport.AutoFit(nodes, layout, nodes[2].ID, width, height)
				if !fitted {
					t.Fatal("initial fit did not run")
				}
			} else {
				layout = viewport.Fit(nodes, layout, nodes[2].ID, width, height)
			}
			if viewport.State().Zoom != ZoomCompact || layout.Width > minimap.X-1 || viewport.State().OffsetX != 0 {
				t.Fatalf("fit would hide vertices under the map: zoom=%v width=%d map=%#v pan=%d", viewport.State().Zoom, layout.Width, minimap, viewport.State().OffsetX)
			}
		})
	}
}

func TestMinimapLeavesRightmostVertexReachable(t *testing.T) {
	const width, height, screenTop = 120, 20, 3
	nodes := viewportTestChain(5)
	nodes[len(nodes)-1].Name = "終端ノード"
	viewport := NewViewport(false)
	layout := viewport.BuildLayout(nodes)
	minimap, _ := viewport.MinimapRect(width, height, true)
	graphWidth := minimap.X - 1
	last := nodes[len(nodes)-1]
	viewport.Center(layout, last.ID, width, height)
	state := viewport.State()
	lastRect := layout.Rects[last.ID]
	if lastRect.X-state.OffsetX < 0 || lastRect.X+lastRect.W-state.OffsetX > graphWidth {
		t.Fatalf("centering leaves the last card behind the map: rect=%#v offset=%d graph width=%d", lastRect, state.OffsetX, graphWidth)
	}
	viewport.Pan(layout, width, height, layout.Width, 0)
	state = viewport.State()
	if state.OffsetX != layout.Width-graphWidth {
		t.Fatalf("rightmost pan=%d, want %d", state.OffsetX, layout.Width-graphWidth)
	}
	selected := viewport.Click(tea.Mouse{
		X: lastRect.X + lastRect.W/2 - state.OffsetX, Y: screenTop + lastRect.CenterY() - state.OffsetY, Button: tea.MouseLeft,
	}, layout, nodes[0].ID, width, height, screenTop)
	if selected != last.ID {
		t.Fatalf("visible last card click selected %q", selected)
	}
	state = viewport.State()
	view := ansi.Strip(Render(Scene{Nodes: nodes, Layout: layout, Selected: selected, OffsetX: state.OffsetX, OffsetY: state.OffsetY, ShowMinimap: true}, width, height))
	if !strings.Contains(view, last.Name) {
		t.Fatal("selected last card's Unicode label is not visible beside the map")
	}
}

func TestMinimapDockDoesNotHitHiddenVerticesOrZoomGraph(t *testing.T) {
	const width, height, screenTop = 120, 20, 3
	viewport := NewViewport(false)
	minimap, _ := viewport.MinimapRect(width, height, true)
	layout := graph.Layout{
		Order: []string{"visible", "hidden"},
		Rects: map[string]graph.Rect{
			"visible": {X: 2, Y: 1, W: 29, H: 5},
			"hidden":  {X: minimap.X + 2, Y: minimap.H + 2, W: 5, H: 1},
		},
		Width: 400, Height: 100,
	}
	event := tea.Mouse{X: minimap.X + 3, Y: screenTop + minimap.H + 2, Button: tea.MouseLeft}
	if selected := viewport.Click(event, layout, "visible", width, height, screenTop); selected != "visible" || viewport.DragActive() {
		t.Fatalf("click below the map reached hidden graph content: selected=%q state=%#v", selected, viewport.State())
	}
	event.Button = tea.MouseWheelDown
	viewport.Wheel(event, nil, layout, width, height, screenTop)
	if viewport.State().OffsetY != 0 {
		t.Fatal("wheel below the map panned the hidden graph")
	}
	event.Y, event.Mod = screenTop+minimap.Y+2, tea.ModCtrl
	viewport.Wheel(event, nil, layout, width, height, screenTop)
	if viewport.State().Zoom != ZoomDetailed {
		t.Fatal("control wheel over the map used map coordinates to zoom the graph")
	}
}

func TestMinimapDragProjectsIntoReducedGraphViewport(t *testing.T) {
	const width, height, screenTop = 120, 20, 3
	viewport := NewViewport(false)
	layout := graph.Layout{Order: []string{"node"}, Rects: map[string]graph.Rect{"node": {X: 2, Y: 1, W: 29, H: 5}}, Width: 400, Height: 100}
	minimap, _ := viewport.MinimapRect(width, height, true)
	graphWidth := minimap.X - 1
	innerX := (minimap.W - 2) / 2
	event := tea.Mouse{X: minimap.X + 1 + innerX, Y: screenTop + minimap.Y + 3, Button: tea.MouseLeft}
	viewport.Click(event, layout, "node", width, height, screenTop)
	want := Unproject(innerX, minimap.W-2, layout.Width) - graphWidth/2
	if !viewport.DragActive() || viewport.State().OffsetX != want {
		t.Fatalf("map click centered the wrong viewport: state=%#v want x=%d", viewport.State(), want)
	}
	event.X = minimap.X + minimap.W - 2
	viewport.Motion(event, layout, width, height, screenTop)
	if viewport.State().OffsetX != layout.Width-graphWidth {
		t.Fatalf("map drag cannot reach the graph's right edge: %#v", viewport.State())
	}
	viewport.Release(width)
	if viewport.DragActive() || !viewport.MinimapVisible(true, width) {
		t.Fatal("wide map release left a drag active or hid the dock")
	}
}
