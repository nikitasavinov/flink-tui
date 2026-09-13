package metrics

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nikitasavinov/flink-tui/internal/flink"
)

func TestMetricBrowserRequestsAndRendersVisibleValues(t *testing.T) {
	model := metricInteractionTestModel(t)
	model.metricVertex = "source"
	model.metricScope = -1
	model.metricAggregation = flink.MetricSum
	model.metricNames = []string{
		"tracked", "visibleGauge", "metric02", "metric03", "metric04",
		"metric05", "metric06", "metric07", "metric08", "metric09",
	}
	model.metricTracked = []string{"tracked"}
	model.metricValues = make(map[string]float64)

	requested := model.metricRequestNames()
	if !slices.Contains(requested, "visibleGauge") {
		t.Fatalf("visible untracked metric not requested: %#v", requested)
	}
	_ = model.Apply(metricValuesMsg{
		values: map[string]float64{
			"tracked":      1,
			"visibleGauge": 42.5,
			"metric02":     2,
			"metric03":     3,
			"metric04":     4,
		},
		jobID:       model.snapshot.JobID,
		vertexID:    model.metricVertex,
		subtask:     model.metricScope,
		aggregation: model.metricAggregation,
		tracked:     append([]string(nil), model.metricTracked...),
		requested:   requested,
		at:          time.Now(),
		generation:  model.generation,
	})
	row := model.renderMetricRow("visibleGauge", false, 80)
	if !strings.Contains(row, "42.50") || strings.HasSuffix(strings.TrimSpace(row), "-") {
		t.Fatalf("visible metric row has no current value: %q", row)
	}

	model.metricSelection.Set(8, 9)
	laterWindow := model.metricRequestNames()
	if !slices.Contains(laterWindow, "metric09") || !slices.Contains(laterWindow, "tracked") {
		t.Fatalf("scrolled request = %#v, want visible tail and tracked series", laterWindow)
	}
}
