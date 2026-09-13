package jobgraph

import (
	"fmt"
	"image/color"
	"math"
	"strings"
	"time"

	"charm.land/lipgloss/v2"

	"github.com/nikitasavinov/flink-tui/internal/flink"
	"github.com/nikitasavinov/flink-tui/internal/ui/shared"
)

// InspectorScene is the immutable data required by the graph's selected-node
// inspector. The coordinator supplies topology and telemetry without exposing
// mutable application state to the renderer.
type InspectorScene struct {
	Node     flink.Node
	HasNode  bool
	Nodes    []flink.Node
	Children []string
	Samples  []Sample
}

// RenderInspector renders the fixed-height details pane below a job graph.
func RenderInspector(scene InspectorScene, width, height int) string {
	separator := lipgloss.NewStyle().Foreground(shared.C("#334155")).Render(strings.Repeat("-", width))
	if !scene.HasNode {
		return separator + strings.Repeat("\n", max(0, height-1))
	}
	node := scene.Node
	lines := []string{
		separator,
		renderInspectorTitle(node, width),
		shared.Truncate(fmt.Sprintf(" flow    in %s rec/s    out %s rec/s", rate(node.Metrics.RecordsInPerSecond), rate(node.Metrics.RecordsOutPerSecond)), width),
		shared.Truncate(fmt.Sprintf(" total   in %s rec / %s    out %s rec / %s",
			shared.HumanCount(node.Metrics.RecordsIn), humanMetricBytes(node.Metrics.BytesIn),
			shared.HumanCount(node.Metrics.RecordsOut), humanMetricBytes(node.Metrics.BytesOut)), width),
		renderTimeSplit(node, width),
		shared.Truncate(fmt.Sprintf(" signal  watermark %-9s    input skew %5.1f%%", formatWatermark(node.Metrics), node.Metrics.DataSkewPercent), width),
		shared.Truncate(renderTrend(scene.Samples, width), width),
		shared.Truncate(" edges   "+edgeSummary(scene), width),
	}
	if len(lines) > height {
		lines = lines[:height]
	}
	for len(lines) < height {
		lines = append(lines, "")
	}
	return strings.Join(lines, "\n")
}

func renderInspectorTitle(node flink.Node, width int) string {
	suffix := fmt.Sprintf("  parallelism x%d  id %s", node.Parallelism, shared.ShortID(node.ID))
	if width < 64 {
		suffix = fmt.Sprintf("  x%d  %s", node.Parallelism, shared.ShortID(node.ID))
	}
	nameWidth := max(1, width-1-2-shared.DisplayWidth(node.State)-shared.DisplayWidth(suffix))
	name := shared.Truncate(node.Name, nameWidth)
	nameStyle := lipgloss.NewStyle().Bold(true).Foreground(shared.C("#E2E8F0"))
	stateStyle := lipgloss.NewStyle().Bold(true).Foreground(stateColor(node.State))
	return " " + nameStyle.Render(name) + "  " + stateStyle.Render(node.State) + suffix
}

func renderTimeSplit(node flink.Node, width int) string {
	level := pressureLevel(node.Metrics)
	busyValue := BusyStyle().Render(fmt.Sprintf("%5.1f%%", node.Metrics.BusyPercent))
	badge := PressureBadgeStyle(level).Render(" " + level + " ")
	if width < 68 {
		return fmt.Sprintf(" time B %s BP %s %3.0f%% I %3.0f%%",
			busyValue, badge, node.Metrics.BackpressurePercent, node.Metrics.IdlePercent)
	}
	return fmt.Sprintf(" time    busy %s    backpressure %s %5.1f%%    idle %5.1f%%",
		busyValue, badge, node.Metrics.BackpressurePercent, node.Metrics.IdlePercent)
}

func renderTrend(samples []Sample, width int) string {
	barWidth := 12
	if width < 90 {
		barWidth = 8
	}
	if width < 64 {
		barWidth = 6
	}
	busy := SparklineFixed(sampleValues(samples, func(metrics flink.Metrics) float64 { return metrics.BusyPercent }), barWidth, 100)
	pressure := SparklineFixed(sampleValues(samples, func(metrics flink.Metrics) float64 { return metrics.BackpressurePercent }), barWidth, 100)
	if width < 64 {
		return fmt.Sprintf(" trend  busy %s  bp %s  60s", busy, pressure)
	}
	input := SparklineDynamic(sampleValues(samples, func(metrics flink.Metrics) float64 { return metrics.RecordsInPerSecond }), barWidth)
	return fmt.Sprintf(" trend  busy %s  bp %s  in %s  60s", busy, pressure, input)
}

func sampleValues(samples []Sample, value func(flink.Metrics) float64) []float64 {
	values := make([]float64, len(samples))
	for index, sample := range samples {
		values[index] = value(sample.Metrics)
	}
	return values
}

func edgeSummary(scene InspectorScene) string {
	names := make(map[string]string, len(scene.Nodes))
	for _, node := range scene.Nodes {
		names[node.ID] = node.Name
	}
	nodeName := func(id string) string {
		if name, ok := names[id]; ok {
			return name
		}
		return shared.ShortID(id)
	}
	parts := make([]string, 0, len(scene.Node.Inputs)+1)
	for _, input := range flink.SortedInputs(scene.Node.Inputs) {
		strategy := input.ShipStrategy
		if strategy == "" {
			strategy = "INPUT"
		}
		parts = append(parts, strategy+" from "+nodeName(input.ID))
	}
	if len(scene.Children) > 0 {
		children := make([]string, 0, len(scene.Children))
		for _, child := range scene.Children {
			children = append(children, nodeName(child))
		}
		parts = append(parts, "to "+strings.Join(children, ", "))
	}
	if len(parts) == 0 {
		return "no connected vertices"
	}
	return strings.Join(parts, "  |  ")
}

func formatWatermark(metrics flink.Metrics) string {
	if !metrics.WatermarkKnown {
		return "-"
	}
	return time.UnixMilli(metrics.LowWatermark).Format("15:04:05")
}

func humanMetricBytes(value float64) string {
	if value <= 0 {
		return "0B"
	}
	if value >= float64(math.MaxInt64) {
		return ">8EiB"
	}
	units := []string{"B", "KiB", "MiB", "GiB", "TiB"}
	amount := float64(int64(value))
	unit := 0
	for amount >= 1024 && unit < len(units)-1 {
		amount /= 1024
		unit++
	}
	if amount >= 100 || unit == 0 {
		return fmt.Sprintf("%.0f%s", amount, units[unit])
	}
	return fmt.Sprintf("%.1f%s", amount, units[unit])
}

func stateColor(state string) color.Color {
	if state == "FAILED" || state == "FAILING" || state == "CANCELED" {
		return shared.C("#FB7185")
	}
	if state == "RESTARTING" {
		return shared.C("#FBBF24")
	}
	if state == "RUNNING" {
		return shared.C("#34D399")
	}
	return shared.C("#94A3B8")
}
