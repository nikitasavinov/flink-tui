package coordinator

import (
	"testing"

	tea "charm.land/bubbletea/v2"
)

func TestMinimapToggleKeepsSelectionInGraphArea(t *testing.T) {
	for _, width := range []int{80, 140} {
		model := interactionTestModel(t)
		model.mode = modeGraph
		model.width = width
		model.selected = "risk"
		setTestNavigation(&model, navigationHidden, false)
		state := model.graphViewport.State()
		state.MinimapHidden = true
		model.graphViewport.RestoreState(state)
		model.centerSelection()
		originalX := model.graphViewport.State().OffsetX

		model.handleGraphKey("z")
		minimap, ok := model.graphViewport.MinimapRect(model.graphWidth(), model.graphHeight(), true)
		if !ok {
			t.Fatalf("width %d: z did not open the minimap", width)
		}
		selected := model.layout.Rects[model.selected]
		x := selected.X - model.graphViewport.State().OffsetX
		if x < 0 || x+selected.W > minimap.X-1 {
			t.Fatalf("width %d: opening map clipped selected node at x=%d width=%d; graph ends at %d", width, x, selected.W, minimap.X-1)
		}
		if model.graphViewport.State().OffsetX == originalX {
			t.Fatalf("width %d: map did not recenter the selected node", width)
		}

		model.handleGraphKey("z")
		if model.graphViewport.State().OffsetX != originalX {
			t.Fatalf("width %d: closing map did not restore selection center", width)
		}
	}
}

func TestNarrowMinimapReleaseReclaimsGraphWidth(t *testing.T) {
	model := interactionTestModel(t)
	model.mode = modeGraph
	setTestNavigation(&model, navigationHidden, false)
	model.handleGraphKey("z")
	minimap, ok := model.graphViewport.MinimapRect(model.graphWidth(), model.graphHeight(), true)
	if !ok {
		t.Fatal("z did not open the minimap")
	}
	model.handleMouseClick(tea.Mouse{
		X: minimap.X + minimap.W - 2, Y: graphScreenTop + minimap.Y + 1,
		Button: tea.MouseLeft,
	})
	model.handleMouseRelease(tea.Mouse{})
	state := model.graphViewport.State()
	if state.MinimapOpen || state.DragActive {
		t.Fatal("release did not close the narrow minimap and end dragging")
	}
	if want := model.layout.Width - model.graphWidth(); state.OffsetX != want {
		t.Fatalf("right-edge offset after closing map = %d, want %d without empty trailing columns", state.OffsetX, want)
	}
}
