package jobdetail

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nikitasavinov/flink-tui/internal/flink"
)

func TestDiagnosticsOrderVerticesWorstFirst(t *testing.T) {
	nodes := []flink.Node{
		{ID: "source", Name: "Source", Metrics: flink.Metrics{BusyPercent: 20, BackpressurePercent: 92.7, DataSkewPercent: 10}},
		{ID: "risk", Name: "Risk", Metrics: flink.Metrics{BusyPercent: 100, DataSkewPercent: 80}},
		{ID: "sink", Name: "Sink", Metrics: flink.Metrics{BusyPercent: 5, DataSkewPercent: 2}},
	}
	model := New(nil, nil)
	context := Context{Snapshot: flink.Snapshot{JobID: "job", Nodes: nodes}, Selected: "source", Order: []string{"source", "risk", "sink"}, BodyHeight: 20}
	model.OpenDiagnostics(context, PageBackpressure)
	if got := model.diagnosticOrder(); !slices.Equal(got, []string{"risk", "source", "sink"}) {
		t.Fatalf("backpressure order = %#v", got)
	}
	model.diagnosticPage = diagnosticSkew
	if got := model.diagnosticOrder(); !slices.Equal(got, []string{"risk", "source", "sink"}) {
		t.Fatalf("skew order = %#v", got)
	}
	if rendered := model.Render(100, 18); !strings.Contains(rendered, "Worst first") {
		t.Fatalf("diagnostics does not disclose sort order:\n%s", rendered)
	}
}

func TestDiagnosticsFilterNarrowsConfirmsPersistsAndExplainsNoMatches(t *testing.T) {
	model := filterTestModel()
	model.OpenDiagnostics(model.context, PageBackpressure)
	model.HandleKey("/")
	for _, key := range []string{"r", "i", "s", "k"} {
		model.HandleKey(key)
	}
	if !model.State().DiagnosticSearch || !slices.Equal(model.diagnosticOrder(), []string{"risk"}) {
		t.Fatalf("diagnostic filter state=%#v order=%v", model.State(), model.diagnosticOrder())
	}
	result := model.HandleKey("enter")
	if model.State().DiagnosticSearch || result.View != ViewSubtasks || result.Command == nil {
		t.Fatalf("filtered enter = view:%v open:%t command nil:%t", result.View, model.State().DiagnosticSearch, result.Command == nil)
	}

	model.OpenDiagnostics(model.context, PageBackpressure)
	if rendered := model.Render(100, 18); !strings.Contains(rendered, "filter /risk/") {
		t.Fatalf("confirmed filter chip missing:\n%s", rendered)
	}
	model.HandleKey("/")
	for _, key := range []string{"z", "z", "z"} {
		model.HandleKey(key)
	}
	model.HandleKey("esc")
	if rendered := model.Render(100, 18); !strings.Contains(rendered, "No vertices match filter /zzz/") {
		t.Fatalf("zero-match diagnostics unexplained:\n%s", rendered)
	}
}

func TestSubtaskFilterMatchesStateIndexAndTaskManager(t *testing.T) {
	model := filterTestModel()
	model.OpenSubtasks(model.context)
	state := model.State()
	state.View = ViewSubtasks
	state.Selected = "risk"
	state.DiagnosticsOpen = "risk"
	state.DiagnosticsBusy = false
	state.Diagnostics = flink.VertexDiagnostics{
		Name: "Risk Score", Parallelism: 2, UpdatedAt: time.Now(),
		Subtasks: []flink.Subtask{
			{Index: 0, State: "RUNNING", TaskManagerID: "tm-blue", Endpoint: "blue.local:1234"},
			{Index: 1, State: "FAILED", TaskManagerID: "tm-red", Endpoint: "red.local:1234"},
		},
	}
	model.RestoreState(state)

	model.HandleKey("/")
	for _, key := range []string{"r", "e", "d", ".", "l", "o", "c", "a", "l"} {
		model.HandleKey(key)
	}
	if rows := model.sortedSubtasks(); len(rows) != 1 || rows[0].Index != 1 {
		t.Fatalf("TaskManager filter rows = %#v", rows)
	}
	model.HandleKey("esc")
	if rendered := model.Render(100, 18); !strings.Contains(rendered, "filter /red.local/") {
		t.Fatalf("subtask filter chip missing:\n%s", rendered)
	}
	model.HandleKey("/")
	for _, key := range []string{"9", "9"} {
		model.HandleKey(key)
	}
	model.HandleKey("esc")
	if rendered := model.Render(100, 18); !strings.Contains(rendered, "No subtasks match filter /99/") {
		t.Fatalf("zero-match subtasks unexplained:\n%s", rendered)
	}
}

func TestTimelineFilterNarrowsAndEnterOpensSelectedVertex(t *testing.T) {
	model := filterTestModel()
	model.OpenTimeline(model.context)
	model.HandleKey("/")
	for _, key := range []string{"s", "i", "n", "k"} {
		model.HandleKey(key)
	}
	if !slices.Equal(model.timelineOrder(), []string{"sink"}) || model.State().Selected != "sink" {
		t.Fatalf("timeline filter order=%v selected=%q", model.timelineOrder(), model.State().Selected)
	}
	result := model.HandleKey("enter")
	if result.View != ViewSubtasks || result.Command == nil {
		t.Fatalf("timeline filtered enter = view:%v command nil:%t", result.View, result.Command == nil)
	}

	model.OpenTimeline(model.context)
	model.HandleKey("/")
	for _, key := range []string{"n", "o", "n", "e"} {
		model.HandleKey(key)
	}
	model.HandleKey("esc")
	if rendered := model.Render(100, 18); !strings.Contains(rendered, "No vertices match filter /none/") {
		t.Fatalf("zero-match timeline unexplained:\n%s", rendered)
	}
}

func filterTestModel() Model {
	model := New(nil, nil)
	model.context = Context{
		Snapshot: flink.Snapshot{
			JobID: "job", JobState: "RUNNING", UpdatedAt: time.Now(),
			Nodes: []flink.Node{
				{ID: "source", Name: "Orders Source", State: "RUNNING", Metrics: flink.Metrics{IdlePercent: 90}},
				{ID: "risk", Name: "Risk Score", State: "RUNNING", Metrics: flink.Metrics{BusyPercent: 100, BackpressurePercent: 95}},
				{ID: "sink", Name: "Audit Sink", State: "RUNNING", Metrics: flink.Metrics{BusyPercent: 20}},
			},
		},
		Selected: "source", Order: []string{"source", "risk", "sink"}, BodyHeight: 20, Width: 100, ContentWidth: 100,
	}
	model.Sync(model.context)
	return model
}
