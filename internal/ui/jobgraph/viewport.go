package jobgraph

import (
	tea "charm.land/bubbletea/v2"

	"github.com/nikitasavinov/flink-tui/internal/flink"
	"github.com/nikitasavinov/flink-tui/internal/graph"
	"github.com/nikitasavinov/flink-tui/internal/ui/shared"
)

const (
	mousePanX          = 8
	mousePanY          = 3
	mouseDragThreshold = 2
)

type dragKind uint8

const (
	dragNone dragKind = iota
	dragGraph
	dragMinimap
)

type dragState struct {
	kind                         dragKind
	startX, startY               int
	originOffsetX, originOffsetY int
	moved                        bool
}

// ViewportState is a detached graph viewport snapshot.
type ViewportState struct {
	OffsetX        int
	OffsetY        int
	Zoom           Zoom
	AutoFitPending bool
	MinimapOpen    bool
	MinimapHidden  bool
	DragActive     bool
}

// Viewport owns graph zoom, pan, minimap, and drag state.
type Viewport struct {
	offsetX, offsetY int
	zoom             graphZoom
	autoFitPending   bool
	minimapOpen      bool
	minimapHidden    bool
	drag             dragState
}

// NewViewport creates a detailed viewport and optionally requests first-load fitting.
func NewViewport(autoFitPending bool) Viewport {
	return Viewport{zoom: graphZoomDetailed, autoFitPending: autoFitPending}
}

// Reset clears pan and zoom for another job and requests first-load fitting.
func (viewport *Viewport) Reset() { *viewport = NewViewport(true) }

// State returns a detached viewport snapshot.
func (viewport Viewport) State() ViewportState {
	return ViewportState{
		OffsetX: viewport.offsetX, OffsetY: viewport.offsetY, Zoom: viewport.zoom,
		AutoFitPending: viewport.autoFitPending, MinimapOpen: viewport.minimapOpen,
		MinimapHidden: viewport.minimapHidden, DragActive: viewport.drag.kind != dragNone,
	}
}

// RestoreState replaces viewport state. Active drags are intentionally not synthesized.
func (viewport *Viewport) RestoreState(state ViewportState) {
	viewport.offsetX, viewport.offsetY, viewport.zoom = state.OffsetX, state.OffsetY, state.Zoom
	viewport.autoFitPending, viewport.minimapOpen, viewport.minimapHidden = state.AutoFitPending, state.MinimapOpen, state.MinimapHidden
	if !state.DragActive {
		viewport.drag = dragState{}
	}
}

// ZoomLabel returns the semantic zoom percentage shown in the footer.
func (viewport Viewport) ZoomLabel() string { return zoomSpec(viewport.zoom).label }

// BuildLayout lays out nodes at the current semantic zoom.
func (viewport Viewport) BuildLayout(nodes []flink.Node) graph.Layout {
	spec := zoomSpec(viewport.zoom)
	return graph.BuildWithSpacing(nodes, spec.nodeWidth, spec.nodeHeight, spec.gapX, spec.gapY)
}

// BuildLayoutAt lays out nodes at an explicit semantic zoom.
func BuildLayoutAt(nodes []flink.Node, level Zoom) graph.Layout {
	spec := zoomSpec(level)
	return graph.BuildWithSpacing(nodes, spec.nodeWidth, spec.nodeHeight, spec.gapX, spec.gapY)
}

// ChangeZoom changes detail while keeping the selected vertex anchored.
func (viewport *Viewport) ChangeZoom(delta int, nodes []flink.Node, layout graph.Layout, selected string, width, height int) graph.Layout {
	target := graphZoom(shared.Clamp(int(viewport.zoom)+delta, int(graphZoomDetailed), int(graphZoomTopology)))
	return viewport.setZoomAroundSelection(target, nodes, layout, selected, width, height)
}

// ChangeZoomAt changes detail around a pointer location.
func (viewport *Viewport) ChangeZoomAt(delta, screenX, screenY int, nodes []flink.Node, layout graph.Layout, width, height int) graph.Layout {
	if !rectContains(viewport.graphRect(width, height, len(nodes) > 0), screenX, screenY) {
		return layout
	}
	target := graphZoom(shared.Clamp(int(viewport.zoom)+delta, int(graphZoomDetailed), int(graphZoomTopology)))
	if target == viewport.zoom {
		return layout
	}
	oldCanvasX, oldCanvasY := viewport.offsetX+screenX, viewport.offsetY+screenY
	oldWidth, oldHeight := layout.Width, layout.Height
	viewport.zoom = target
	layout = viewport.BuildLayout(nodes)
	viewport.offsetX = project(oldCanvasX, oldWidth, layout.Width) - screenX
	viewport.offsetY = project(oldCanvasY, oldHeight, layout.Height) - screenY
	viewport.pan(layout, width, height, 0, 0)
	return layout
}

// ResetZoom returns to the detailed view around selection.
func (viewport *Viewport) ResetZoom(nodes []flink.Node, layout graph.Layout, selected string, width, height int) graph.Layout {
	return viewport.setZoomAroundSelection(graphZoomDetailed, nodes, layout, selected, width, height)
}

func (viewport *Viewport) setZoomAroundSelection(target graphZoom, nodes []flink.Node, layout graph.Layout, selected string, width, height int) graph.Layout {
	if target == viewport.zoom {
		return layout
	}
	oldRect, anchored := layout.Rects[selected]
	bounds := viewport.graphRect(width, height, len(nodes) > 0)
	screenX, screenY := bounds.W/2, bounds.H/2
	if anchored {
		screenX = oldRect.X + oldRect.W/2 - viewport.offsetX
		screenY = oldRect.Y + oldRect.H/2 - viewport.offsetY
	}
	viewport.zoom = target
	layout = viewport.BuildLayout(nodes)
	if newRect, ok := layout.Rects[selected]; ok && anchored {
		viewport.offsetX = newRect.X + newRect.W/2 - screenX
		viewport.offsetY = newRect.Y + newRect.H/2 - screenY
		viewport.pan(layout, width, height, 0, 0)
	} else {
		viewport.Center(layout, selected, width, height)
	}
	return layout
}

// Fit chooses the most detailed level that fits the viewport.
func (viewport *Viewport) Fit(nodes []flink.Node, layout graph.Layout, selected string, width, height int) graph.Layout {
	bounds := viewport.graphRect(width, height, len(nodes) > 0)
	target := graphZoomTopology
	for _, candidate := range []graphZoom{graphZoomDetailed, graphZoomCompact, graphZoomTopology} {
		candidateLayout := BuildLayoutAt(nodes, candidate)
		if candidateLayout.Width <= bounds.W && candidateLayout.Height <= bounds.H {
			target = candidate
			break
		}
	}
	layout = viewport.setZoomAroundSelection(target, nodes, layout, selected, width, height)
	if layout.Width <= bounds.W && layout.Height <= bounds.H {
		viewport.offsetX, viewport.offsetY = 0, 0
	} else {
		viewport.Center(layout, selected, width, height)
	}
	return layout
}

// AutoFit performs the requested first-load fit exactly once.
func (viewport *Viewport) AutoFit(nodes []flink.Node, layout graph.Layout, selected string, width, height int) (graph.Layout, bool) {
	if !viewport.autoFitPending || width <= 0 || height <= 0 || len(nodes) == 0 {
		return layout, false
	}
	layout = viewport.Fit(nodes, layout, selected, width, height)
	viewport.autoFitPending = false
	return layout, true
}

// Center centers selection and constrains pan to graph bounds.
func (viewport *Viewport) Center(layout graph.Layout, selected string, width, height int) {
	rect, ok := layout.Rects[selected]
	if !ok || width <= 0 || height <= 0 {
		return
	}
	bounds := viewport.graphRect(width, height, len(layout.Order) > 0)
	viewport.offsetX = rect.X + rect.W/2 - bounds.W/2
	viewport.offsetY = rect.Y + rect.H/2 - bounds.H/2
	viewport.pan(layout, width, height, 0, 0)
}

// Pan shifts and clamps the viewport.
func (viewport *Viewport) Pan(layout graph.Layout, width, height, deltaX, deltaY int) {
	viewport.pan(layout, width, height, deltaX, deltaY)
}

func (viewport *Viewport) pan(layout graph.Layout, width, height, deltaX, deltaY int) {
	bounds := viewport.graphRect(width, height, len(layout.Order) > 0)
	viewport.offsetX = shared.Clamp(viewport.offsetX+deltaX, 0, max(0, layout.Width-bounds.W))
	viewport.offsetY = shared.Clamp(viewport.offsetY+deltaY, 0, max(0, layout.Height-bounds.H))
}

// MinimapVisible applies wide-screen default and narrow-screen explicit-open policy.
func (viewport Viewport) MinimapVisible(hasNodes bool, width int) bool {
	if !hasNodes {
		return false
	}
	if width >= wideMinimapThreshold {
		return !viewport.minimapHidden
	}
	return viewport.minimapOpen
}

// ToggleMinimap toggles the visibility policy for the current width.
func (viewport *Viewport) ToggleMinimap(width int) {
	viewport.CancelDrag()
	if width >= wideMinimapThreshold {
		viewport.minimapHidden = !viewport.minimapHidden
	} else {
		viewport.minimapOpen = !viewport.minimapOpen
	}
}

// MinimapRect returns current minimap bounds.
func (viewport Viewport) MinimapRect(width, height int, hasNodes bool) (graph.Rect, bool) {
	return MinimapRect(width, height, viewport.MinimapVisible(hasNodes, width))
}

func (viewport Viewport) graphRect(width, height int, hasNodes bool) graph.Rect {
	return graphRect(width, height, viewport.MinimapVisible(hasNodes, width))
}

// Click selects a vertex or starts graph/minimap dragging.
func (viewport *Viewport) Click(event tea.Mouse, layout graph.Layout, selected string, width, height, screenTop int) string {
	if event.Button != tea.MouseLeft || width <= 0 || height <= 0 {
		return selected
	}
	if minimap, ok := viewport.MinimapRect(width, height, len(layout.Order) > 0); ok {
		screenRect := minimap
		screenRect.Y += screenTop
		if rectContains(screenRect, event.X, event.Y) {
			viewport.drag = dragState{kind: dragMinimap, startX: event.X, startY: event.Y, originOffsetX: viewport.offsetX, originOffsetY: viewport.offsetY}
			viewport.jumpFromMinimap(event.X, event.Y-screenTop, minimap, layout, width, height)
			return selected
		}
	}
	graphY := event.Y - screenTop
	if !rectContains(viewport.graphRect(width, height, len(layout.Order) > 0), event.X, graphY) {
		return selected
	}
	canvasX, canvasY := event.X+viewport.offsetX, graphY+viewport.offsetY
	for _, id := range layout.Order {
		if rectContains(layout.Rects[id], canvasX, canvasY) {
			viewport.Center(layout, id, width, height)
			return id
		}
	}
	viewport.drag = dragState{kind: dragGraph, startX: event.X, startY: event.Y, originOffsetX: viewport.offsetX, originOffsetY: viewport.offsetY}
	return selected
}

// Motion continues an active graph or minimap drag.
func (viewport *Viewport) Motion(event tea.Mouse, layout graph.Layout, width, height, screenTop int) {
	if viewport.drag.kind == dragNone {
		return
	}
	deltaX, deltaY := event.X-viewport.drag.startX, event.Y-viewport.drag.startY
	if !viewport.drag.moved && abs(deltaX) < mouseDragThreshold && abs(deltaY) < mouseDragThreshold {
		return
	}
	viewport.drag.moved = true
	switch viewport.drag.kind {
	case dragGraph:
		viewport.offsetX, viewport.offsetY = viewport.drag.originOffsetX-deltaX, viewport.drag.originOffsetY-deltaY
		viewport.pan(layout, width, height, 0, 0)
	case dragMinimap:
		if minimap, ok := viewport.MinimapRect(width, height, len(layout.Order) > 0); ok {
			viewport.jumpFromMinimap(event.X, event.Y-screenTop, minimap, layout, width, height)
		}
	}
}

// Release ends dragging and closes a narrow minimap.
func (viewport *Viewport) Release(width int) {
	kind := viewport.drag.kind
	viewport.drag = dragState{}
	if kind == dragMinimap && width < wideMinimapThreshold {
		viewport.minimapOpen = false
	}
}

// DragActive reports whether motion/release events belong to the graph.
func (viewport Viewport) DragActive() bool { return viewport.drag.kind != dragNone }

// CancelDrag discards a stale press.
func (viewport *Viewport) CancelDrag() { viewport.drag = dragState{} }

// Wheel pans or zooms the graph.
func (viewport *Viewport) Wheel(event tea.Mouse, nodes []flink.Node, layout graph.Layout, width, height, screenTop int) graph.Layout {
	graphY := event.Y - screenTop
	if !rectContains(viewport.graphRect(width, height, len(layout.Order) > 0), event.X, graphY) {
		return layout
	}
	if event.Mod.Contains(tea.ModCtrl) {
		switch event.Button {
		case tea.MouseWheelUp:
			return viewport.ChangeZoomAt(-1, event.X, graphY, nodes, layout, width, height)
		case tea.MouseWheelDown:
			return viewport.ChangeZoomAt(1, event.X, graphY, nodes, layout, width, height)
		}
		return layout
	}
	horizontal := event.Mod.Contains(tea.ModShift) || event.Button == tea.MouseWheelLeft || event.Button == tea.MouseWheelRight
	if horizontal {
		switch event.Button {
		case tea.MouseWheelUp, tea.MouseWheelLeft:
			viewport.pan(layout, width, height, -mousePanX, 0)
		case tea.MouseWheelDown, tea.MouseWheelRight:
			viewport.pan(layout, width, height, mousePanX, 0)
		}
		return layout
	}
	switch event.Button {
	case tea.MouseWheelUp:
		viewport.pan(layout, width, height, 0, -mousePanY)
	case tea.MouseWheelDown:
		viewport.pan(layout, width, height, 0, mousePanY)
	}
	return layout
}

func (viewport *Viewport) jumpFromMinimap(screenX, graphY int, minimap graph.Rect, layout graph.Layout, width, height int) {
	innerWidth, innerHeight := minimap.W-2, minimap.H-2
	if innerWidth <= 0 || innerHeight <= 0 {
		return
	}
	relativeX := shared.Clamp(screenX-minimap.X-1, 0, innerWidth-1)
	relativeY := shared.Clamp(graphY-minimap.Y-1, 0, innerHeight-1)
	bounds := viewport.graphRect(width, height, len(layout.Order) > 0)
	viewport.offsetX = unproject(relativeX, innerWidth, layout.Width) - bounds.W/2
	viewport.offsetY = unproject(relativeY, innerHeight, layout.Height) - bounds.H/2
	viewport.pan(layout, width, height, 0, 0)
}

func rectContains(rect graph.Rect, x, y int) bool {
	return x >= rect.X && x < rect.X+rect.W && y >= rect.Y && y < rect.Y+rect.H
}

func abs(value int) int {
	if value < 0 {
		return -value
	}
	return value
}
