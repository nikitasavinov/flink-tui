package checkpoints

import (
	"testing"

	"github.com/nikitasavinov/flink-tui/internal/flink"
)

func TestRestoreStateAcrossJobsKeepsRestoredPresentation(t *testing.T) {
	model := New(nil, nil)
	model.Sync(Context{
		JobID:   "old-job",
		Summary: flink.CheckpointSummary{History: []flink.Checkpoint{{ID: 1}}},
	})
	state := State{
		View: ViewOperators, Page: PageStatistics,
		JobID: "new-job", DetailID: 42, ConfigOpen: true,
		OperatorSelected: "risk", OperatorCursor: 2,
		Summary: flink.CheckpointSummary{History: []flink.Checkpoint{{ID: 42}}},
		Nodes:   []flink.Node{{ID: "risk", Name: "Risk"}},
		Order:   []string{"risk"}, SubtaskSelected: -1,
	}
	model.RestoreState(state)
	actual := model.State()
	if actual.JobID != "new-job" || actual.View != ViewOperators || actual.Page != PageStatistics ||
		actual.DetailID != 42 || !actual.ConfigOpen || actual.OperatorSelected != "risk" {
		t.Fatalf("restored state was reset during job switch: %#v", actual)
	}
}

func TestAsyncReplyDoesNotReplayNavigationIntent(t *testing.T) {
	for _, stale := range []bool{false, true} {
		model := New(nil, nil)
		model.Sync(Context{JobID: "job", Generation: 1})
		result := model.HandleKey("esc", 20)
		if result.Intent != IntentGraph {
			t.Fatal("fixture did not produce a graph intent")
		}
		generation := uint64(1)
		if stale {
			generation = 0
		}
		result = model.Apply(checkpointDetailMsg{jobID: "job", generation: generation})
		if result.Intent != IntentNone || result.Vertex != "" {
			t.Fatalf("stale=%t reply replayed navigation: %#v", stale, result)
		}
	}
}
