package accumulators

import (
	"strings"
	"testing"
	"time"

	"github.com/nikitasavinov/flink-tui/internal/flink"
)

func TestAccumulatorExplorerFlattensValuesAndOpensDocument(t *testing.T) {
	model := New(nil, nil)
	model.Sync(Context{
		Snapshot: flink.Snapshot{JobID: "job", Nodes: []flink.Node{{ID: "source", Name: "Source"}}},
		Selected: "source", Generation: 1, BodyHeight: 20,
	})
	model.RestoreState(State{
		Vertex: "source", Selection: 1,
		Values: flink.VertexAccumulators{
			JobID: "job", VertexID: "source", UpdatedAt: time.Now(),
			Vertex:   []flink.UserAccumulator{{Name: "rows", Type: "LongCounter", Value: "123"}},
			Subtasks: []flink.SubtaskAccumulators{{Subtask: 0, Accumulators: []flink.UserAccumulator{{Name: "payload", Type: "String", Value: "line one\nline two"}}}},
		},
	})
	rendered := model.Render(90, 20)
	for _, expected := range []string{"ACCUMULATORS", "Source", "#0", "payload"} {
		if !strings.Contains(rendered, expected) {
			t.Fatalf("view missing %q:\n%s", expected, rendered)
		}
	}
	result := model.HandleKey("enter")
	if result.Intent != IntentDocument || result.Title != "ACCUMULATOR  #0 / payload" || result.Content != "line one\nline two" {
		t.Fatalf("document result = %#v", result)
	}
}

func TestAccumulatorReplyChecksJobVertexAndGeneration(t *testing.T) {
	model := New(nil, nil)
	model.Sync(Context{Snapshot: flink.Snapshot{JobID: "job"}, Selected: "source", Generation: 2})
	model.vertex, model.busy = "source", true
	values := flink.VertexAccumulators{JobID: "job", VertexID: "source", Vertex: []flink.UserAccumulator{{Name: "rows", Value: "7"}}}

	model.Apply(reply{values: values, jobID: "job", vertexID: "source", generation: 1})
	if model.State().Rows != 0 || !model.State().Busy {
		t.Fatalf("stale reply changed state: %#v", model.State())
	}
	model.Apply(reply{values: values, jobID: "job", vertexID: "source", generation: 2})
	if model.State().Rows != 1 || model.State().Busy {
		t.Fatalf("current reply state = %#v", model.State())
	}
}

func TestAccumulatorFilterNarrowsOpensAndExplainsNoMatches(t *testing.T) {
	model := New(nil, nil)
	model.Sync(Context{
		Snapshot: flink.Snapshot{JobID: "job", Nodes: []flink.Node{{ID: "source", Name: "Source"}}},
		Selected: "source", BodyHeight: 20,
	})
	model.RestoreState(State{
		Vertex: "source",
		Values: flink.VertexAccumulators{
			JobID: "job", VertexID: "source",
			Vertex: []flink.UserAccumulator{
				{Name: "rows", Type: "LongCounter", Value: "123"},
				{Name: "failures", Type: "LongCounter", Value: "2"},
			},
		},
	})
	model.HandleKey("/")
	for _, key := range []string{"f", "a", "i", "l"} {
		model.HandleKey(key)
	}
	if state := model.State(); state.Rows != 1 || !state.SearchOpen {
		t.Fatalf("accumulator filter state = %#v", state)
	}
	result := model.HandleKey("enter")
	if result.Intent != IntentDocument || !strings.Contains(result.Title, "failures") {
		t.Fatalf("filtered accumulator enter = %#v", result)
	}
	if rendered := model.Render(90, 20); !strings.Contains(rendered, "filter /fail/") {
		t.Fatalf("accumulator filter chip missing:\n%s", rendered)
	}

	model.HandleKey("/")
	for _, key := range []string{"n", "o", "n", "e"} {
		model.HandleKey(key)
	}
	model.HandleKey("esc")
	if rendered := model.Render(90, 20); !strings.Contains(rendered, "No accumulators match filter /none/") {
		t.Fatalf("accumulator zero match unexplained:\n%s", rendered)
	}
}
