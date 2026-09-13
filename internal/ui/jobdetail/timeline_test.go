package jobdetail

import (
	"strings"
	"testing"
	"time"

	"github.com/nikitasavinov/flink-tui/internal/flink"
	"github.com/nikitasavinov/flink-tui/internal/graph"
)

func TestTimelineScalesToCurrentVertexRangeAfterRestart(t *testing.T) {
	model := timelineTestModel()
	now := time.Date(2026, time.August, 25, 12, 0, 0, 0, time.UTC)
	model.snapshot.StartedAt = now.Add(-23 * time.Hour)
	model.snapshot.Duration = 23 * time.Hour
	model.snapshot.UpdatedAt = now
	model.snapshot.Nodes = []flink.Node{
		{ID: "source", Name: "Source", State: "RUNNING", StartedAt: now.Add(-20 * time.Minute), Duration: 20 * time.Minute},
		{ID: "sink", Name: "Sink", State: "RUNNING", StartedAt: now.Add(-10 * time.Minute), Duration: 10 * time.Minute},
	}
	model.layout = graph.Build(model.snapshot.Nodes, nodeWidth, nodeHeight)
	model.selected = "source"

	axisStart, axisEnd := model.timelineVertexRange()
	if axisStart != now.Add(-20*time.Minute) || axisEnd != now {
		t.Fatalf("vertex range = %s -> %s, want last 20 minutes", axisStart, axisEnd)
	}
	source := model.timelineBar(model.snapshot.Nodes[0], 20)
	if source != strings.Repeat("█", 20) {
		t.Fatalf("source bar = %q, want full vertex range", source)
	}
	sink := model.timelineBar(model.snapshot.Nodes[1], 20)
	if sink != strings.Repeat("·", 10)+strings.Repeat("█", 10) {
		t.Fatalf("sink bar = %q, want second half of vertex range", sink)
	}
	columns := model.renderTimelineColumns(120)
	if !strings.Contains(columns, "VERTEX RANGE 20m00s") || strings.Contains(columns, "JOB SPAN") {
		t.Fatalf("timeline columns do not describe vertex scaling: %q", columns)
	}
}

func TestTimelineRangeUsesSnapshotTimeWhenVertexDurationIsUnavailable(t *testing.T) {
	model := timelineTestModel()
	started := time.Date(2026, time.August, 25, 12, 0, 0, 0, time.UTC)
	model.snapshot.UpdatedAt = started.Add(5 * time.Minute)
	model.snapshot.Nodes = []flink.Node{{ID: "source", StartedAt: started, State: "RUNNING"}}

	axisStart, axisEnd := model.timelineVertexRange()
	if axisStart != started || axisEnd != model.snapshot.UpdatedAt {
		t.Fatalf("fallback vertex range = %s -> %s", axisStart, axisEnd)
	}
	if bar := model.timelineBar(model.snapshot.Nodes[0], 10); bar != strings.Repeat("█", 10) {
		t.Fatalf("running vertex bar = %q, want full fallback range", bar)
	}
}

func timelineTestModel() Model {
	model := New(nil, nil)
	model.snapshot = flink.Snapshot{JobID: "job", JobName: "test"}
	model.layout = graph.Build(model.snapshot.Nodes, nodeWidth, nodeHeight)
	model.width = 120
	return model
}
