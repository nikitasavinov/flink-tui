package jobgraph

import (
	"testing"
	"time"

	"github.com/nikitasavinov/flink-tui/internal/flink"
)

func TestTelemetryRetainsSixtySecondsAndDetectsVisibleChanges(t *testing.T) {
	now := time.Now()
	telemetry := NewTelemetry()
	old := flink.Snapshot{JobID: "job", UpdatedAt: now.Add(-70 * time.Second), Nodes: []flink.Node{{ID: "source", Metrics: flink.Metrics{BusyPercent: 10}}}}
	telemetry.Record(flink.Snapshot{}, old)
	current := flink.Snapshot{JobID: "job", UpdatedAt: now, Nodes: []flink.Node{{ID: "source", Metrics: flink.Metrics{BusyPercent: 80}}}}
	if !telemetry.Record(old, current) {
		t.Fatal("busy change was not detected")
	}
	samples := telemetry.Samples("source")
	if len(samples) != 1 || samples[0].Metrics.BusyPercent != 80 {
		t.Fatalf("samples = %#v, want only latest", samples)
	}
	if !telemetry.Changes()["source"].Busy {
		t.Fatalf("changes = %#v", telemetry.Changes())
	}
}
