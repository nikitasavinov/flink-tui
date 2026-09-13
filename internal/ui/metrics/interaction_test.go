package metrics

import (
	"math"
	"reflect"
	"slices"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/nikitasavinov/flink-tui/internal/flink"
	shellmodule "github.com/nikitasavinov/flink-tui/internal/ui/shell"
)

func TestOpenMetricExplorerInitializesOnlyWhenVertexChanges(t *testing.T) {
	model := metricInteractionTestModel(t)
	model.metricVertex = "old"
	model.metricNames = []string{"old.metric"}
	model.metricTracked = []string{"old.metric"}
	model.metricValues = map[string]float64{"old.metric": 1}
	model.metricSelection.Set(4, 5)
	model.metricScope = 2
	model.metricFilter.Restore(shellmodule.QueryState{Value: "old", Open: true})

	command := model.Open(model.context)
	if command == nil || model.metricVertex != "source" || !model.metricBusy {
		t.Fatalf("metric explorer open = vertex:%q busy:%t command:%v", model.metricVertex, model.metricBusy, command)
	}
	if len(model.metricNames) != 0 || len(model.metricTracked) != 0 || len(model.metricValues) != 0 ||
		model.metricSelection.Index() != 0 || model.metricScope != -1 || model.metricFilter.Value() != "" || model.metricFilter.Active() {
		t.Fatalf("new vertex metric state was not reset: %#v", model)
	}

	model.metricNames = []string{"keep.metric"}
	model.metricTracked = []string{"keep.metric"}
	if command := model.Open(model.context); command != nil || !reflect.DeepEqual(model.metricNames, []string{"keep.metric"}) ||
		!reflect.DeepEqual(model.metricTracked, []string{"keep.metric"}) {
		t.Fatalf("same vertex did not preserve catalog: names=%#v tracked=%#v command=%v", model.metricNames, model.metricTracked, command)
	}

	model.context.Snapshot.JobID = ""
	if command := model.Open(model.context); command != nil {
		t.Fatal("metric explorer opened without a job")
	}
}

func TestMetricCatalogDefaultAndAvailabilitySelection(t *testing.T) {
	tests := []struct {
		names []string
		want  []string
	}{
		{names: []string{"custom", "numRecordsOutPerSecond", "numRecordsInPerSecond"}, want: []string{"numRecordsInPerSecond", "numRecordsOutPerSecond"}},
		{names: []string{"custom", "other"}, want: []string{"custom"}},
		{names: nil, want: []string{}},
	}
	for _, test := range tests {
		if got := defaultTrackedMetrics(test.names); !reflect.DeepEqual(got, test.want) {
			t.Errorf("default tracked for %#v = %#v, want %#v", test.names, got, test.want)
		}
	}
	if got := availableTrackedMetrics([]string{"gone", "keep", "also"}, []string{"also", "keep"}); !reflect.DeepEqual(got, []string{"keep", "also"}) {
		t.Fatalf("available tracked = %#v", got)
	}
}

func TestMetricExplorerKeyboardAndSearch(t *testing.T) {
	tests := []struct {
		name        string
		key         string
		prepare     func(*Model)
		assert      func(*testing.T, Model)
		wantCommand bool
	}{
		{name: "graph", key: "esc", assert: func(t *testing.T, m Model) {
			if m.intent != IntentGraph {
				t.Fatalf("intent = %d, want graph", m.intent)
			}
		}},
		{name: "up", key: "up", prepare: func(m *Model) { m.metricSelection.Set(1, 2) }, wantCommand: true, assert: assertMetricCursor(0)},
		{name: "down", key: "down", wantCommand: true, assert: assertMetricCursor(1)},
		{name: "page up", key: "pgup", prepare: func(m *Model) { m.metricSelection.Set(3, 4) }, wantCommand: true, assert: assertMetricCursor(0)},
		{name: "page down", key: "pgdown", wantCommand: true, assert: assertMetricCursor(3)},
		{name: "home", key: "home", prepare: func(m *Model) { m.metricSelection.Set(3, 4) }, wantCommand: true, assert: assertMetricCursor(0)},
		{name: "end", key: "end", wantCommand: true, assert: assertMetricCursor(3)},
		{name: "track", key: "enter", wantCommand: true, assert: func(t *testing.T, m Model) {
			if !slices.Contains(m.metricTracked, "alpha") {
				t.Fatalf("tracked metrics = %#v", m.metricTracked)
			}
		}},
		{name: "search", key: "/", assert: func(t *testing.T, m Model) {
			if !m.metricFilter.Active() {
				t.Fatal("metric search did not open")
			}
		}},
		{name: "scope", key: "s", wantCommand: true, assert: func(t *testing.T, m Model) {
			if m.metricScope != 0 {
				t.Fatalf("metric scope = %d, want subtask 0", m.metricScope)
			}
		}},
		{name: "aggregation", key: "a", wantCommand: true, assert: func(t *testing.T, m Model) {
			if m.metricAggregation != flink.MetricAvg {
				t.Fatalf("metric aggregation = %q, want avg", m.metricAggregation)
			}
		}},
		{name: "shorter history", key: "[", prepare: func(m *Model) { m.metricWindow = 5 * time.Minute }, assert: assertMetricWindow(time.Minute)},
		{name: "longer history", key: "]", prepare: func(m *Model) { m.metricWindow = 5 * time.Minute }, assert: assertMetricWindow(15 * time.Minute)},
		{name: "clear", key: "x", prepare: func(m *Model) { m.metricTracked = []string{"alpha"} }, wantCommand: true, assert: func(t *testing.T, m Model) {
			if len(m.metricTracked) != 0 || m.metricValues == nil {
				t.Fatalf("cleared metric state = tracked:%#v values:%#v", m.metricTracked, m.metricValues)
			}
		}},
		{name: "orphan overview key is inert", key: "o", assert: func(t *testing.T, m Model) {
			if m.intent != IntentNone {
				t.Fatalf("intent = %d, want none", m.intent)
			}
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			model := metricInteractionTestModel(t)
			if test.prepare != nil {
				test.prepare(&model)
			}
			command := model.handleMetricExplorerKey(test.key)
			if (command != nil) != test.wantCommand {
				t.Fatalf("command presence = %t, want %t", command != nil, test.wantCommand)
			}
			test.assert(t, model)
		})
	}

	model := metricInteractionTestModel(t)
	model.handleMetricExplorerKey("/")
	model.metricSelection.Set(2, 3)
	model.handleMetricExplorerKey("b")
	model.handleMetricExplorerKey("space")
	model.handleMetricExplorerKey("e")
	if model.metricFilter.Value() != "b e" || model.metricSelection.Index() != 0 {
		t.Fatalf("metric search input = query:%q cursor:%d", model.metricFilter.Value(), model.metricSelection.Index())
	}
	model.handleMetricExplorerKey("backspace")
	if model.metricFilter.Value() != "b " {
		t.Fatalf("metric search backspace = %q", model.metricFilter.Value())
	}
	model.handleMetricExplorerKey("enter")
	if model.metricFilter.Active() {
		t.Fatal("enter did not close metric search")
	}
	model.handleMetricExplorerKey("/")
	model.handleMetricExplorerKey("é")
	model.handleMetricExplorerKey("ctrl+w")
	if model.metricFilter.Value() != "b é" {
		t.Fatalf("reopened metric search = %q", model.metricFilter.Value())
	}
	model.handleMetricExplorerKey("backspace")
	if model.metricFilter.Value() != "b " {
		t.Fatalf("Unicode backspace = %q", model.metricFilter.Value())
	}
	model.handleMetricExplorerKey("esc")
	if model.metricFilter.Active() {
		t.Fatal("escape did not close metric search")
	}
}

func TestMetricSelectionLimitsScopesAndMouseBounds(t *testing.T) {
	model := metricInteractionTestModel(t)
	model.moveMetricSelection(0)
	model.metricNames = nil
	model.moveMetricSelection(1)
	if model.metricSelection.Index() != 0 {
		t.Fatal("empty metric selection moved")
	}

	model.metricNames = []string{"alpha"}
	model.metricSelection.Set(4, 5)
	if command := model.toggleSelectedMetric(); command != nil {
		t.Fatal("invalid metric cursor toggled a metric")
	}
	model.metricSelection.Set(0, 1)
	model.metricTracked = []string{"alpha"}
	if command := model.toggleSelectedMetric(); command == nil || len(model.metricTracked) != 0 {
		t.Fatalf("tracked metric removal = tracked:%#v command:%v", model.metricTracked, command)
	}
	model.metricTracked = []string{"one", "two", "three", "four"}
	if command := model.toggleSelectedMetric(); command != nil || model.metricErr == nil {
		t.Fatalf("metric limit = command:%v err:%v", command, model.metricErr)
	}

	model = metricInteractionTestModel(t)
	for _, want := range []int{0, 1, 2, -1} {
		model.cycleMetricScope()
		if model.metricScope != want {
			t.Fatalf("metric scope = %d, want %d", model.metricScope, want)
		}
	}
	model.snapshot.Nodes[0].Parallelism = 0
	model.metricScope = 1
	model.cycleMetricScope()
	if model.metricScope != -1 {
		t.Fatalf("unknown parallelism scope = %d, want vertex", model.metricScope)
	}

	model.metricAggregation = flink.MetricAggregation("invalid")
	model.cycleMetricAggregation()
	if model.metricAggregation != flink.MetricAvg {
		t.Fatalf("invalid aggregation cycled to %q, want avg", model.metricAggregation)
	}
	model.metricWindow = 2 * time.Minute
	model.cycleMetricWindow(1)
	if model.metricWindow != 15*time.Minute {
		t.Fatalf("unknown window cycled to %s, want 15m", model.metricWindow)
	}

	model.metricNames = []string{"alpha", "beta", "gamma", "delta"}
	model.metricSelection.Set(0, 1)
	model.handleMetricMouseClick(tea.Mouse{Y: headerHeight + 4, Button: tea.MouseRight})
	model.handleMetricMouseClick(tea.Mouse{Y: 0, Button: tea.MouseLeft})
	if model.metricSelection.Index() != 0 {
		t.Fatal("invalid metric click changed selection")
	}
	model.handleMetricMouseClick(tea.Mouse{Y: headerHeight + 3 + 2, Button: tea.MouseLeft})
	if model.metricSelection.Index() != 2 {
		t.Fatalf("metric click cursor = %d, want 2", model.metricSelection.Index())
	}
}

func TestCustomMetricFormattingCoversOperationalValueClasses(t *testing.T) {
	tests := []struct {
		name  string
		value float64
		want  string
	}{
		{name: "gauge", value: math.NaN(), want: "N/A"},
		{name: "gauge", value: math.Inf(1), want: "N/A"},
		{name: "numBytesIn", value: 2048, want: "2.0KiB"},
		{name: "numBytesOutPerSecond", value: -1024, want: "-1.0KiB/s"},
		{name: "numBytesBuffers", value: 2048, want: "2.0k"},
		{name: "large", value: 1_500_000_000, want: "1.5e+09"},
		{name: "thousands", value: 12_345, want: "12.3k"},
		{name: "hundreds", value: 123, want: "123"},
		{name: "ones", value: 1.25, want: "1.25"},
		{name: "fraction", value: 0.125, want: "0.125"},
	}
	for _, test := range tests {
		if got := formatCustomMetricValue(test.name, test.value); got != test.want {
			t.Errorf("format %s %v = %q, want %q", test.name, test.value, got, test.want)
		}
	}
	if got := metricWindowLabel(90 * time.Second); got != "1m30s" {
		t.Fatalf("90-second window label = %q", got)
	}
}

func assertMetricCursor(want int) func(*testing.T, Model) {
	return func(t *testing.T, model Model) {
		if model.metricSelection.Index() != want {
			t.Fatalf("metric cursor = %d, want %d", model.metricSelection.Index(), want)
		}
	}
}

func assertMetricWindow(want time.Duration) func(*testing.T, Model) {
	return func(t *testing.T, model Model) {
		if model.metricWindow != want {
			t.Fatalf("metric window = %s, want %s", model.metricWindow, want)
		}
	}
}

func metricInteractionTestModel(t *testing.T) Model {
	t.Helper()
	model := New(nil, nil)
	model.snapshot = flink.Snapshot{JobID: "job", Nodes: []flink.Node{{ID: "source", Name: "Source", Parallelism: 3}}}
	model.selected = "source"
	model.generation = 1
	model.context = Context{Snapshot: model.snapshot, Selected: model.selected, Generation: model.generation, BodyHeight: 11, ContentWidth: 100}
	model.metricVertex = "source"
	model.metricScope = -1
	model.metricAggregation = flink.MetricSum
	model.metricWindow = 5 * time.Minute
	model.metricNames = []string{"alpha", "beta", "gamma", "delta"}
	model.metricValues = make(map[string]float64)
	return model
}
