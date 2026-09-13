package jobgraph

import (
	"strings"
	"testing"
	"time"

	"github.com/nikitasavinov/flink-tui/internal/flink"
)

func TestInspectorRendersFlowTotalsSignalsTrendsAndEdges(t *testing.T) {
	watermark := time.Date(2026, 8, 29, 12, 30, 0, 0, time.UTC)
	source := flink.Node{ID: "source", Name: "Source"}
	node := flink.Node{
		ID: "map", Name: "Risk Map", State: "RUNNING", Parallelism: 4,
		Inputs: []flink.Input{{ID: source.ID, ShipStrategy: "HASH"}},
		Metrics: flink.Metrics{
			RecordsInPerSecond: 12.5, RecordsOutPerSecond: 11.5,
			RecordsIn: 12_300, RecordsOut: 12_000,
			BytesIn: 1024 * 1024, BytesOut: 512 * 1024,
			BusyPercent: 75, BackpressurePercent: 20, IdlePercent: 5,
			WatermarkKnown: true, LowWatermark: watermark.UnixMilli(),
			DataSkewPercent: 12.5,
		},
	}
	sink := flink.Node{ID: "sink", Name: "Sink"}
	rendered := RenderInspector(InspectorScene{
		Node: node, HasNode: true, Nodes: []flink.Node{source, node, sink}, Children: []string{sink.ID},
		Samples: []Sample{{Metrics: flink.Metrics{BusyPercent: 40}}, {Metrics: node.Metrics}},
	}, 120, 8)
	for _, expected := range []string{"Risk Map", "parallelism x4", "12.3k rec", "1.0MiB", "watermark " + watermark.Local().Format("15:04:05"), "trend  busy", "HASH from Source", "to Sink"} {
		if !strings.Contains(rendered, expected) {
			t.Fatalf("inspector missing %q:\n%s", expected, rendered)
		}
	}
}
