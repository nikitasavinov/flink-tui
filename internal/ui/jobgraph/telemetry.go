package jobgraph

import (
	"maps"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/nikitasavinov/flink-tui/internal/flink"
)

const (
	HistoryWindow = time.Minute
	// ChangePulseDuration is how long updated graph metrics remain highlighted.
	ChangePulseDuration = 900 * time.Millisecond
)

// Sample is one timestamped vertex metric observation.
type Sample struct {
	At      time.Time
	Metrics flink.Metrics
}

// Telemetry owns graph-local history and short-lived metric-change pulses.
type Telemetry struct {
	history map[string][]Sample
	changes map[string]MetricChange
}

// NewTelemetry creates an empty graph telemetry store.
func NewTelemetry() Telemetry { return Telemetry{history: make(map[string][]Sample)} }

// Reset clears history and visible changes for a newly selected job.
func (telemetry *Telemetry) Reset() { *telemetry = NewTelemetry() }

// Record stores the latest snapshot and reports whether any displayed metric changed.
func (telemetry *Telemetry) Record(previous, current flink.Snapshot) bool {
	telemetry.changes = detectChanges(previous, current)
	if telemetry.history == nil {
		telemetry.history = make(map[string][]Sample)
	}
	cutoff := current.UpdatedAt.Add(-HistoryWindow)
	for _, node := range current.Nodes {
		samples := telemetry.history[node.ID]
		if len(samples) > 0 && samples[len(samples)-1].At.Equal(current.UpdatedAt) {
			samples[len(samples)-1].Metrics = node.Metrics
		} else {
			samples = append(samples, Sample{At: current.UpdatedAt, Metrics: node.Metrics})
		}
		first := 0
		for first < len(samples) && samples[first].At.Before(cutoff) {
			first++
		}
		telemetry.history[node.ID] = slices.Clone(samples[first:])
	}
	return len(telemetry.changes) > 0
}

// ClearChanges ends the current render pulse without dropping history.
func (telemetry *Telemetry) ClearChanges() { telemetry.changes = nil }

// Changes returns a detached copy of the current pulse fields.
func (telemetry Telemetry) Changes() map[string]MetricChange {
	return maps.Clone(telemetry.changes)
}

// Samples returns a detached vertex history.
func (telemetry Telemetry) Samples(vertexID string) []Sample {
	return slices.Clone(telemetry.history[vertexID])
}

// History returns metric-only histories for diagnostic child screens.
func (telemetry Telemetry) History() map[string][]flink.Metrics {
	result := make(map[string][]flink.Metrics, len(telemetry.history))
	for id, samples := range telemetry.history {
		values := make([]flink.Metrics, len(samples))
		for index, sample := range samples {
			values[index] = sample.Metrics
		}
		result[id] = values
	}
	return result
}

func detectChanges(previous, current flink.Snapshot) map[string]MetricChange {
	if previous.JobID == "" || previous.JobID != current.JobID {
		return nil
	}
	before := make(map[string]flink.Metrics, len(previous.Nodes))
	for _, node := range previous.Nodes {
		before[node.ID] = node.Metrics
	}
	changes := make(map[string]MetricChange)
	for _, node := range current.Nodes {
		old, ok := before[node.ID]
		if !ok {
			continue
		}
		change := MetricChange{
			Busy:         math.Round(old.BusyPercent) != math.Round(node.Metrics.BusyPercent),
			Backpressure: math.Round(old.BackpressurePercent) != math.Round(node.Metrics.BackpressurePercent),
			Input:        rate(old.RecordsInPerSecond) != rate(node.Metrics.RecordsInPerSecond),
			Output:       rate(old.RecordsOutPerSecond) != rate(node.Metrics.RecordsOutPerSecond),
			Skew:         math.Round(old.DataSkewPercent) != math.Round(node.Metrics.DataSkewPercent),
		}
		if change.Busy || change.Backpressure || change.Input || change.Output || change.Skew {
			changes[node.ID] = change
		}
	}
	if len(changes) == 0 {
		return nil
	}
	return changes
}

// SparklineFixed renders values against a fixed zero-to-maximum scale.
func SparklineFixed(values []float64, width int, maximum float64) string {
	return sparkline(values, width, 0, maximum, false)
}

// SparklineDynamic renders values against their observed range.
func SparklineDynamic(values []float64, width int) string {
	if len(values) == 0 || width <= 0 {
		return ""
	}
	minimum, maximum := values[0], values[0]
	for _, value := range values[1:] {
		minimum, maximum = math.Min(minimum, value), math.Max(maximum, value)
	}
	return sparkline(values, width, minimum, maximum, true)
}

func sparkline(values []float64, width int, minimum, maximum float64, centerFlat bool) string {
	if width <= 0 {
		return ""
	}
	if len(values) == 0 {
		return strings.Repeat("·", width)
	}
	runes := []rune("▁▂▃▄▅▆▇█")
	selected := downsample(values, width)
	var result strings.Builder
	if len(selected) < width {
		result.WriteString(strings.Repeat("·", width-len(selected)))
	}
	flat := maximum <= minimum
	for _, value := range selected {
		level := 0
		if flat {
			if centerFlat {
				level = 3
			} else if maximum > 0 {
				level = len(runes) - 1
			}
		} else {
			ratio := math.Max(0, math.Min(1, (value-minimum)/(maximum-minimum)))
			level = int(math.Round(ratio * float64(len(runes)-1)))
		}
		result.WriteRune(runes[level])
	}
	return result.String()
}

func downsample(values []float64, width int) []float64 {
	if width == 1 {
		return []float64{values[len(values)-1]}
	}
	if len(values) <= width {
		return slices.Clone(values)
	}
	result := make([]float64, width)
	for index := range result {
		position := int(math.Round(float64(index) * float64(len(values)-1) / float64(width-1)))
		result[index] = values[position]
	}
	return result
}
