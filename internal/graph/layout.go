package graph

import (
	"cmp"
	"math"
	"slices"

	"github.com/nikitasavinov/flink-tui/internal/flink"
)

type Rect struct {
	X int
	Y int
	W int
	H int
}

func (rect Rect) CenterY() int { return rect.Y + rect.H/2 }

type Layout struct {
	Rects    map[string]Rect
	Parents  map[string][]string
	Children map[string][]string
	Layers   [][]string
	Order    []string
	Width    int
	Height   int
}

func Build(nodes []flink.Node, nodeWidth, nodeHeight int) Layout {
	return BuildWithSpacing(nodes, nodeWidth, nodeHeight, 9, 3)
}

// BuildWithSpacing lays out the graph with caller-controlled node geometry.
// Build retains the original detailed-card spacing for existing callers.
func BuildWithSpacing(nodes []flink.Node, nodeWidth, nodeHeight, gapX, gapY int) Layout {
	const (
		paddingX = 2
		paddingY = 1
	)
	nodeWidth = max(1, nodeWidth)
	nodeHeight = max(1, nodeHeight)
	gapX = max(0, gapX)
	gapY = max(0, gapY)

	known := make(map[string]bool, len(nodes))
	name := make(map[string]string, len(nodes))
	original := make(map[string]int, len(nodes))
	parents := make(map[string][]string, len(nodes))
	children := make(map[string][]string, len(nodes))
	indegree := make(map[string]int, len(nodes))
	for index, node := range nodes {
		known[node.ID] = true
		name[node.ID] = node.Name
		original[node.ID] = index
		indegree[node.ID] = 0
	}
	for _, node := range nodes {
		for _, input := range node.Inputs {
			if !known[input.ID] {
				continue
			}
			parents[node.ID] = append(parents[node.ID], input.ID)
			children[input.ID] = append(children[input.ID], node.ID)
			indegree[node.ID]++
		}
	}
	for id := range known {
		sortIDs(parents[id], name, original)
		sortIDs(children[id], name, original)
	}

	queue := make([]string, 0, len(nodes))
	for id, degree := range indegree {
		if degree == 0 {
			queue = append(queue, id)
		}
	}
	sortIDs(queue, name, original)
	rank := make(map[string]int, len(nodes))
	processed := make(map[string]bool, len(nodes))
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		processed[id] = true
		for _, child := range children[id] {
			rank[child] = max(rank[child], rank[id]+1)
			indegree[child]--
			if indegree[child] == 0 {
				queue = append(queue, child)
				sortIDs(queue, name, original)
			}
		}
	}

	maxRank := 0
	for _, node := range nodes {
		if !processed[node.ID] {
			rank[node.ID] = maxRank + 1
		}
		maxRank = max(maxRank, rank[node.ID])
	}
	layers := make([][]string, maxRank+1)
	for _, node := range nodes {
		layers[rank[node.ID]] = append(layers[rank[node.ID]], node.ID)
	}
	for index := range layers {
		sortIDs(layers[index], name, original)
	}

	// A small barycentric sweep keeps branches close to their parents.
	positions := make(map[string]int, len(nodes))
	for layerIndex, layer := range layers {
		if layerIndex > 0 {
			slices.SortStableFunc(layer, func(leftID, rightID string) int {
				left := barycenter(parents[leftID], positions)
				right := barycenter(parents[rightID], positions)
				if left == right {
					return cmp.Compare(name[leftID], name[rightID])
				}
				return cmp.Compare(left, right)
			})
		}
		for position, id := range layer {
			positions[id] = position
		}
	}

	maxLayerSize := 1
	for _, layer := range layers {
		maxLayerSize = max(maxLayerSize, len(layer))
	}
	height := paddingY*2 + maxLayerSize*nodeHeight + (maxLayerSize-1)*gapY
	width := paddingX*2 + len(layers)*nodeWidth + max(0, len(layers)-1)*gapX
	rects := make(map[string]Rect, len(nodes))
	order := make([]string, 0, len(nodes))
	for layerIndex, layer := range layers {
		layerHeight := len(layer)*nodeHeight + max(0, len(layer)-1)*gapY
		startY := (height - layerHeight) / 2
		for position, id := range layer {
			rects[id] = Rect{
				X: paddingX + layerIndex*(nodeWidth+gapX),
				Y: startY + position*(nodeHeight+gapY),
				W: nodeWidth,
				H: nodeHeight,
			}
			order = append(order, id)
		}
	}

	return Layout{
		Rects:    rects,
		Parents:  parents,
		Children: children,
		Layers:   layers,
		Order:    order,
		Width:    width,
		Height:   height,
	}
}

func sortIDs(ids []string, names map[string]string, original map[string]int) {
	slices.SortStableFunc(ids, func(leftID, rightID string) int {
		if names[leftID] == names[rightID] {
			return cmp.Compare(original[leftID], original[rightID])
		}
		return cmp.Compare(names[leftID], names[rightID])
	})
}

func barycenter(ids []string, positions map[string]int) float64 {
	if len(ids) == 0 {
		return math.MaxFloat64
	}
	total := 0
	for _, id := range ids {
		total += positions[id]
	}
	return float64(total) / float64(len(ids))
}
