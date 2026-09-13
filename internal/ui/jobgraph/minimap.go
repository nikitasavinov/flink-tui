package jobgraph

import (
	"math"

	"github.com/nikitasavinov/flink-tui/internal/flink"
	"github.com/nikitasavinov/flink-tui/internal/graph"
	"github.com/nikitasavinov/flink-tui/internal/ui/shared"
)

const (
	wideMinimapThreshold = 90
	minimapWidth         = 30
	minimapHeight        = 9
)

func (m Model) minimapVisible() bool {
	return m.showMinimap && len(m.snapshot.Nodes) > 0
}

// MinimapRect returns the map bounds within the graph screen. The map has its
// own column, leaving room for a detailed node and a one-cell separator.
func MinimapRect(width, height int, visible bool) (graph.Rect, bool) {
	if !visible || height < 5 {
		return graph.Rect{}, false
	}
	mapWidth := min(minimapWidth, width-nodeWidth-2)
	mapHeight := min(minimapHeight, height)
	if mapWidth < 12 || mapHeight < 5 {
		return graph.Rect{}, false
	}
	return graph.Rect{X: width - mapWidth - 1, Y: 0, W: mapWidth, H: mapHeight}, true
}

func graphRect(width, height int, showMinimap bool) graph.Rect {
	rect := graph.Rect{W: max(0, width), H: max(0, height)}
	if minimap, ok := MinimapRect(width, height, showMinimap); ok {
		rect.W = minimap.X - 1
	}
	return rect
}

func (m Model) drawMinimap(canvas *canvas, rect graph.Rect, viewWidth, viewHeight int) {
	for y := rect.Y; y < rect.Y+rect.H; y++ {
		for x := rect.X; x < rect.X+rect.W; x++ {
			canvas.set(x, y, ' ', styleMinimapBackground)
		}
	}
	drawMinimapFrame(canvas, rect)

	innerX, innerY := rect.X+1, rect.Y+1
	innerWidth, innerHeight := rect.W-2, rect.H-2
	if innerWidth <= 0 || innerHeight <= 0 {
		return
	}

	point := func(nodeRect graph.Rect) (int, int) {
		x := project(nodeRect.X+nodeRect.W/2, m.layout.Width, innerWidth)
		y := project(nodeRect.Y+nodeRect.H/2, m.layout.Height, innerHeight)
		return innerX + x, innerY + y
	}

	connections := make(map[[2]int]edgeBits)
	for _, node := range m.snapshot.Nodes {
		targetRect, targetOK := m.layout.Rects[node.ID]
		if !targetOK {
			continue
		}
		targetX, targetY := point(targetRect)
		for _, input := range node.Inputs {
			sourceRect, sourceOK := m.layout.Rects[input.ID]
			if !sourceOK {
				continue
			}
			sourceX, sourceY := point(sourceRect)
			middleX := sourceX + (targetX-sourceX)/2
			addPath(connections, sourceX, sourceY, middleX, sourceY)
			addPath(connections, middleX, sourceY, middleX, targetY)
			addPath(connections, middleX, targetY, targetX, targetY)
		}
	}
	for point, bits := range connections {
		if point[0] >= innerX && point[0] < innerX+innerWidth &&
			point[1] >= innerY && point[1] < innerY+innerHeight {
			canvas.set(point[0], point[1], edgeRune(bits), styleMinimapEdge)
		}
	}

	viewportLeft := innerX + project(m.offsetX, m.layout.Width, innerWidth)
	viewportRight := innerX + project(m.offsetX+viewWidth-1, m.layout.Width, innerWidth)
	viewportTop := innerY + project(m.offsetY, m.layout.Height, innerHeight)
	viewportBottom := innerY + project(m.offsetY+viewHeight-1, m.layout.Height, innerHeight)
	drawMinimapViewport(canvas, viewportLeft, viewportTop, viewportRight, viewportBottom)

	for _, node := range m.snapshot.Nodes {
		nodeRect, ok := m.layout.Rects[node.ID]
		if !ok {
			continue
		}
		x, y := point(nodeRect)
		style := minimapNodeStyle(node, node.ID == m.selected)
		runeValue := '•'
		if node.ID == m.selected {
			runeValue = '◆'
		}
		canvas.set(x, y, runeValue, style)
	}
	canvas.text(rect.X+2, rect.Y, " MAP ", styleMinimapFrame)
}

func drawMinimapFrame(canvas *canvas, rect graph.Rect) {
	canvas.set(rect.X, rect.Y, '┌', styleMinimapFrame)
	canvas.set(rect.X+rect.W-1, rect.Y, '┐', styleMinimapFrame)
	canvas.set(rect.X, rect.Y+rect.H-1, '└', styleMinimapFrame)
	canvas.set(rect.X+rect.W-1, rect.Y+rect.H-1, '┘', styleMinimapFrame)
	for x := rect.X + 1; x < rect.X+rect.W-1; x++ {
		canvas.set(x, rect.Y, '─', styleMinimapFrame)
		canvas.set(x, rect.Y+rect.H-1, '─', styleMinimapFrame)
	}
	for y := rect.Y + 1; y < rect.Y+rect.H-1; y++ {
		canvas.set(rect.X, y, '│', styleMinimapFrame)
		canvas.set(rect.X+rect.W-1, y, '│', styleMinimapFrame)
	}
}

func drawMinimapViewport(canvas *canvas, left, top, right, bottom int) {
	if left > right {
		left, right = right, left
	}
	if top > bottom {
		top, bottom = bottom, top
	}
	if left == right && top == bottom {
		canvas.set(left, top, '▣', styleMinimapViewport)
		return
	}
	if left == right {
		for y := top; y <= bottom; y++ {
			canvas.set(left, y, '┃', styleMinimapViewport)
		}
		return
	}
	if top == bottom {
		for x := left; x <= right; x++ {
			canvas.set(x, top, '━', styleMinimapViewport)
		}
		return
	}
	canvas.set(left, top, '┏', styleMinimapViewport)
	canvas.set(right, top, '┓', styleMinimapViewport)
	canvas.set(left, bottom, '┗', styleMinimapViewport)
	canvas.set(right, bottom, '┛', styleMinimapViewport)
	for x := left + 1; x < right; x++ {
		canvas.set(x, top, '━', styleMinimapViewport)
		canvas.set(x, bottom, '━', styleMinimapViewport)
	}
	for y := top + 1; y < bottom; y++ {
		canvas.set(left, y, '┃', styleMinimapViewport)
		canvas.set(right, y, '┃', styleMinimapViewport)
	}
}

func minimapNodeStyle(node flink.Node, selected bool) styleKind {
	if selected {
		return styleMinimapSelected
	}
	switch pressureLevel(node.Metrics) {
	case flink.BackpressureHigh:
		return styleMinimapHigh
	case flink.BackpressureLow:
		return styleMinimapLow
	default:
		return styleMinimapNode
	}
}

func project(value, total, cells int) int {
	if total <= 1 || cells <= 1 {
		return 0
	}
	value = shared.Clamp(value, 0, total-1)
	return int(math.Round(float64(value) * float64(cells-1) / float64(total-1)))
}

func unproject(value, cells, total int) int {
	if cells <= 1 || total <= 1 {
		return 0
	}
	value = shared.Clamp(value, 0, cells-1)
	return int(math.Round(float64(value) * float64(total-1) / float64(cells-1)))
}
