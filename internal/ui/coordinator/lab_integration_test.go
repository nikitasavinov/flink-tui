package coordinator

import (
	"charm.land/lipgloss/v2"
	"context"
	"github.com/nikitasavinov/flink-tui/internal/flink"
	"github.com/nikitasavinov/flink-tui/internal/graph"
	jobgraphmodule "github.com/nikitasavinov/flink-tui/internal/ui/jobgraph"
	"os"
	"strings"
	"testing"
	"time"
)

const (
	flameLabJobName      = "Flink TUI Flame Lab"
	flameLabWorkloadName = "03 Complex Flame Workload"
)

// TestE2EFlameLabProducesComplexCallTree verifies the Docker fixture itself,
// not a canned API response. It intentionally checks structural markers rather
// than exact sample counts because Flink's thread sampling is nondeterministic.
func TestE2EFlameLabProducesComplexCallTree(t *testing.T) {
	endpoint := os.Getenv("FLINK_TUI_E2E_ENDPOINT")
	if endpoint == "" {
		t.Skip("set FLINK_TUI_E2E_ENDPOINT or run make test-e2e")
	}

	client, err := flink.NewClient(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	jobID := waitForRunningFlameLab(t, client)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	snapshot, err := client.Snapshot(ctx, jobID)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Nodes) != 5 {
		t.Fatalf("Flame Lab has %d vertices, want 5", len(snapshot.Nodes))
	}
	vertexID := ""
	for _, node := range snapshot.Nodes {
		if node.Name == flameLabWorkloadName {
			vertexID = node.ID
			if node.Parallelism != 4 {
				t.Fatalf("%s parallelism = %d, want 4", flameLabWorkloadName, node.Parallelism)
			}
			break
		}
	}
	if vertexID == "" {
		t.Fatalf("vertex %q not found in %#v", flameLabWorkloadName, snapshot.Nodes)
	}

	graph := waitForFlameLabGraph(t, client, jobID, vertexID)
	if graph.Root.Value < 100 {
		t.Fatalf("Mixed flame graph has only %d aggregate samples", graph.Root.Value)
	}
	if depth := flameTreeDepth(graph.Root); depth < 18 {
		t.Fatalf("Mixed flame graph depth = %d, want at least 18", depth)
	}
	for _, marker := range []string{
		"ComplexFlameWorkload.map",
		"runCpuPipeline",
		"simulateMarketScenarios",
		"runContentionPipeline",
		"waitForSharedModel",
		"LockSupport.park",
	} {
		if !flameTreeContains(graph.Root, marker) {
			t.Fatalf("Mixed flame graph is missing %q", marker)
		}
	}
}

func waitForRunningFlameLab(t *testing.T, client *flink.Client) string {
	t.Helper()
	deadline := time.Now().Add(3 * time.Minute)
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		jobs, err := client.Jobs(ctx)
		cancel()
		lastErr = err
		if err == nil {
			for _, job := range jobs {
				if job.Name == flameLabJobName && job.State == "RUNNING" {
					return job.ID
				}
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("running %q job not found: %v", flameLabJobName, lastErr)
	return ""
}

func waitForFlameLabGraph(
	t *testing.T,
	client *flink.Client,
	jobID string,
	vertexID string,
) flink.FlameGraph {
	t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		graph, err := client.VertexFlameGraph(ctx, jobID, vertexID, flink.FlameGraphFull, -1)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		if graph.Disabled() {
			t.Fatal("Docker playground has rest.flamegraph.enabled disabled")
		}
		if graph.Ready() {
			return graph
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatal("timed out waiting for the Flame Lab Mixed flame graph")
	return flink.FlameGraph{}
}

func flameTreeContains(node flink.FlameGraphNode, marker string) bool {
	if strings.Contains(node.Name, marker) {
		return true
	}
	for _, child := range node.Children {
		if flameTreeContains(child, marker) {
			return true
		}
	}
	return false
}

func flameTreeDepth(node flink.FlameGraphNode) int {
	depth := 1
	for _, child := range node.Children {
		depth = max(depth, 1+flameTreeDepth(child))
	}
	return depth
}

const topologyLabJobName = "Flink TUI Topology Lab"

// TestTopologyLabRenderingIntegration validates the physical graph returned by
// Flink, then sends that exact snapshot through the TUI layout and renderer.
// It is opt-in so the normal unit suite remains hermetic.
func TestTopologyLabRenderingIntegration(t *testing.T) {
	endpoint := os.Getenv("FLINK_TUI_INTEGRATION_ENDPOINT")
	if endpoint == "" {
		t.Skip("set FLINK_TUI_INTEGRATION_ENDPOINT to run against the Docker playground")
	}

	client, err := flink.NewClient(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	jobs, err := client.Jobs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	jobID := ""
	for _, job := range jobs {
		if job.Name == topologyLabJobName && job.State == "RUNNING" {
			jobID = job.ID
			break
		}
	}
	if jobID == "" {
		t.Fatalf("running %q job not found", topologyLabJobName)
	}

	snapshot, err := client.Snapshot(ctx, jobID)
	if err != nil {
		t.Fatal(err)
	}
	assertTopologyLabShape(t, snapshot.Nodes)

	layout := graph.Build(snapshot.Nodes, nodeWidth, nodeHeight)
	if len(layout.Layers) != 8 {
		t.Fatalf("layout has %d ranks, want 8", len(layout.Layers))
	}
	if layout.Width <= 80 || layout.Height <= 13 {
		t.Fatalf("fixture no longer exercises two-axis scrolling: %dx%d", layout.Width, layout.Height)
	}
	assertHasCrossLayerEdge(t, snapshot.Nodes, layout)

	model := Model{
		client:         client,
		snapshot:       snapshot,
		layout:         layout,
		selected:       layout.Order[0],
		width:          80,
		height:         24,
		mouseEnabled:   true,
		graphViewport:  jobgraphmodule.NewViewport(false),
		graphTelemetry: jobgraphmodule.NewTelemetry(),
	}
	viewport := model.graphViewport.State()
	viewport.MinimapOpen = true
	model.graphViewport.RestoreState(viewport)
	model.recordHistory(snapshot)
	assertGraphViewportFits(t, model, 80, model.graphHeight())

	viewport = model.graphViewport.State()
	viewport.OffsetX = max(0, layout.Width-80)
	viewport.OffsetY = max(0, layout.Height-model.graphHeight())
	model.graphViewport.RestoreState(viewport)
	assertGraphViewportFits(t, model, 80, model.graphHeight())

	for _, level := range []graphZoom{graphZoomDetailed, graphZoomCompact, graphZoomTopology} {
		viewport = model.graphViewport.State()
		viewport.Zoom = level
		model.graphViewport.RestoreState(viewport)
		model.layout = model.buildGraphLayout(snapshot.Nodes)
		model.centerSelection()
		assertGraphViewportFits(t, model, 80, model.graphHeight())
	}
	viewport = model.graphViewport.State()
	viewport.Zoom = graphZoomDetailed
	// The complete topology needs the full 80-column graph area. The map
	// occupies its own column when open, as exercised by the frames above.
	viewport.MinimapOpen = false
	model.graphViewport.RestoreState(viewport)
	model.layout = model.buildGraphLayout(snapshot.Nodes)
	model.fitGraph()
	if zoom := model.graphViewport.State().Zoom; zoom != graphZoomTopology {
		t.Fatalf("80-column fit chose zoom %v, want topology", zoom)
	}
	if model.layout.Width > 80 || model.layout.Height > model.graphHeight() {
		t.Fatalf("topology fit is %dx%d for viewport 80x%d",
			model.layout.Width, model.layout.Height, model.graphHeight())
	}

	var flameGraph flink.FlameGraph
	for {
		flameGraph, err = client.VertexFlameGraph(ctx, snapshot.JobID, model.selected, flink.FlameGraphFull, -1)
		if err != nil {
			t.Fatal(err)
		}
		if flameGraph.Disabled() {
			t.Fatal("Docker playground has rest.flamegraph.enabled disabled")
		}
		if flameGraph.Ready() {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("waiting for vertex flame graph: %v", ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
	model.mode = modeFlameGraph
	flameState := model.flameGraphs.State()
	flameState.Vertex = model.selected
	flameState.Type = flink.FlameGraphFull
	flameState.Subtask = -1
	flameState.LiveGraph = flameGraph
	flameState.LiveSelected = "0"
	flameState.LiveFocus = "0"
	model.flameGraphs.RestoreState(flameState)
	assertFlameGraphViewportFits(t, model, 80, 20)
}

func assertTopologyLabShape(t *testing.T, nodes []flink.Node) {
	t.Helper()
	if len(nodes) != 35 {
		t.Fatalf("physical vertex count = %d, want 35", len(nodes))
	}
	children := make(map[string]int, len(nodes))
	roots, joins, maxFanIn := 0, 0, 0
	for _, node := range nodes {
		if len(node.Inputs) == 0 {
			roots++
		}
		if len(node.Inputs) > 1 {
			joins++
		}
		maxFanIn = max(maxFanIn, len(node.Inputs))
		for _, input := range node.Inputs {
			children[input.ID]++
		}
	}
	leaves, maxFanOut := 0, 0
	for _, node := range nodes {
		if children[node.ID] == 0 {
			leaves++
		}
		maxFanOut = max(maxFanOut, children[node.ID])
	}
	if roots != 5 || leaves != 8 || joins != 10 || maxFanIn < 2 || maxFanOut < 4 {
		t.Fatalf("shape roots=%d leaves=%d joins=%d maxFanIn=%d maxFanOut=%d",
			roots, leaves, joins, maxFanIn, maxFanOut)
	}
}

func assertHasCrossLayerEdge(t *testing.T, nodes []flink.Node, layout graph.Layout) {
	t.Helper()
	rank := make(map[string]int, len(nodes))
	for layerIndex, layer := range layout.Layers {
		for _, id := range layer {
			rank[id] = layerIndex
		}
	}
	for _, node := range nodes {
		for _, input := range node.Inputs {
			if rank[node.ID]-rank[input.ID] > 1 {
				return
			}
		}
	}
	t.Fatal("fixture has no edge spanning multiple layout ranks")
}

func assertGraphViewportFits(t *testing.T, model Model, width, height int) {
	t.Helper()
	rendered := model.renderGraph(width, height)
	lines := strings.Split(rendered, "\n")
	if len(lines) != height {
		t.Fatalf("graph rendered %d lines, want %d", len(lines), height)
	}
	for index, line := range lines {
		if got := lipgloss.Width(line); got != width {
			t.Fatalf("graph line %d is %d cells wide, want %d", index, got, width)
		}
	}
}

func assertFlameGraphViewportFits(t *testing.T, model Model, width, height int) {
	t.Helper()
	rendered := model.renderFlameGraph(width, height)
	if !strings.Contains(rendered, "FLAME GRAPH") || !strings.Contains(rendered, "root") {
		t.Fatalf("live flame graph did not render sampled frames:\n%s", rendered)
	}
	lines := strings.Split(rendered, "\n")
	if len(lines) != height {
		t.Fatalf("flame graph rendered %d lines, want %d", len(lines), height)
	}
	for index, line := range lines {
		if got := lipgloss.Width(line); got > width {
			t.Fatalf("flame graph line %d is %d cells wide, max %d", index, got, width)
		}
	}
}
