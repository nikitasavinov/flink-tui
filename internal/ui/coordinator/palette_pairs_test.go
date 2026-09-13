package coordinator

import (
	"strings"
	"testing"

	"github.com/nikitasavinov/flink-tui/internal/flink"
)

func TestPaletteCombinesVertexAndDestinationInEitherOrder(t *testing.T) {
	for _, query := range []string{"conv st", "st conv"} {
		model := parityTestModel(t)
		model.snapshot.Nodes[1].Name = "Convert Currency"
		model.layout = model.buildGraphLayout(model.snapshot.Nodes)
		model.selected = "source"
		openPaletteWithQuery(&model, query)

		results := model.filteredPaletteCommands()
		if len(results) == 0 || results[0].Label != "Subtasks · Convert Currency" || results[0].Unavailable != "" {
			t.Fatalf("query %q results = %#v", query, results)
		}
		if command := model.handlePaletteKey("enter"); command == nil || model.mode != modeSubtasks || model.selected != "risk" {
			t.Fatalf("query %q opened mode=%d selected=%q command=%v", query, model.mode, model.selected, command)
		}
	}
}

func TestPaletteCombinesJobAndDestinationAcrossJobLoad(t *testing.T) {
	model := parityTestModel(t)
	overview := model.jobList.State()
	overview.Jobs = []flink.JobSummary{{ID: "demo", Name: "Flink TUI Demo", State: "RUNNING"}}
	model.jobList.RestoreState(overview)
	openPaletteWithQuery(&model, "demo cp")

	results := model.filteredPaletteCommands()
	if len(results) == 0 || results[0].Label != "Checkpoints · Flink TUI Demo" {
		t.Fatalf("job + destination results = %#v", results)
	}
	command := model.handlePaletteKey("enter")
	if command == nil || model.preferredJobID != "demo" || model.navigation.State().DeferredTarget != navigationCheckpoints {
		t.Fatalf("job pair opened preferred=%q deferred=%d command=%v", model.preferredJobID, model.navigation.State().DeferredTarget, command)
	}

	model.applySnapshot(snapshotMsg{
		generation: model.generation,
		snapshot:   flink.Snapshot{JobID: "demo", JobName: "Flink TUI Demo", Nodes: []flink.Node{{ID: "hot", Name: "Hot", State: "RUNNING"}}},
	})
	if model.mode != modeCheckpoints || model.selected != "hot" {
		t.Fatalf("loaded pair mode=%d selected=%q", model.mode, model.selected)
	}
}

func TestPaletteJobAndVertexDestinationChoosesHottestWhenSelectionIsMissing(t *testing.T) {
	model := parityTestModel(t)
	model.snapshot.Nodes[0].Metrics.BusyPercent = 10
	model.snapshot.Nodes[1].Metrics.BusyPercent = 100
	model.layout = model.buildGraphLayout(model.snapshot.Nodes)
	model.selected = ""
	overview := model.jobList.State()
	overview.Jobs = []flink.JobSummary{{ID: model.snapshot.JobID, Name: model.snapshot.JobName, State: "RUNNING"}}
	model.jobList.RestoreState(overview)
	openPaletteWithQuery(&model, "test st")

	results := model.filteredPaletteCommands()
	if len(results) == 0 || results[0].Label != "Subtasks · "+model.snapshot.JobName {
		t.Fatalf("same-job pair results = %#v", results)
	}
	if command := model.handlePaletteKey("enter"); command == nil || model.mode != modeSubtasks || model.selected != "risk" {
		t.Fatalf("same-job pair = mode:%s selected:%q command:%v", screenModeName(model.mode), model.selected, command)
	}
}

func TestPaletteShowsAndExplainsImpossibleObjectDestinationPair(t *testing.T) {
	model := parityTestModel(t)
	model.mode = modeTaskManagers
	openPaletteWithQuery(&model, "tm:1 st")

	results := model.filteredPaletteCommands()
	if len(results) == 0 || results[0].Label != "Subtasks · tm:1" || !strings.Contains(results[0].Unavailable, "TaskManager") {
		t.Fatalf("impossible pair results = %#v", results)
	}
	before := model.mode
	model.handlePaletteKey("enter")
	if model.mode != before || !strings.Contains(model.activeNotice(), "requires job context") {
		t.Fatalf("impossible pair mode=%d notice=%q", model.mode, model.activeNotice())
	}
}

func openPaletteWithQuery(model *Model, query string) {
	state := model.palette.State()
	state.Open, state.Query, state.Cursor = true, query, 0
	model.palette.RestoreState(state)
}
