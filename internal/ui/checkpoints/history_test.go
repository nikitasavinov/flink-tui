package checkpoints

import (
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/nikitasavinov/flink-tui/internal/flink"
)

func TestCheckpointWorkspaceOpensAndRendersDetails(t *testing.T) {
	model := checkpointTestModel(t)

	model.openCheckpoints()
	opened := model
	if opened.CurrentView() != ViewHistory {
		t.Fatalf("checkpoint workspace view = %v", opened.CurrentView())
	}

	rendered := opened.Render(120, opened.height)
	for _, expected := range []string{
		"CHECKPOINTS",
		"42",
		"latest failed #40",
		"storage unavailable",
		"12ms",
		"acknowledged 10/10",
		"state 43.8KiB",
	} {
		if !strings.Contains(rendered, expected) {
			t.Fatalf("checkpoint view missing %q:\n%s", expected, rendered)
		}
	}
	if got := strings.Count(rendered, "\n") + 1; got != opened.height {
		t.Fatalf("rendered lines = %d, want %d", got, opened.height)
	}
}

func TestCheckpointSummaryShowsFullPopulationPercentiles(t *testing.T) {
	model := checkpointTestModel(t)
	model.openCheckpoints()
	model.handleCheckpointKey("tab")
	if model.checkpointPage != checkpointStatistics {
		t.Fatal("tab did not open checkpoint summary")
	}
	rendered := model.renderCheckpoints(120, 21)
	for _, expected := range []string{
		"Summary", "P50", "P90", "P95", "P99", "P99.9", "159ms", "43.8KiB",
		"41 completed checkpoints", "3 recent records retained by Flink",
	} {
		if !strings.Contains(rendered, expected) {
			t.Fatalf("checkpoint summary missing %q:\n%s", expected, rendered)
		}
	}

	model.handleCheckpointMouseClick(tea.Mouse{X: 1, Y: headerHeight + 1, Button: tea.MouseLeft})
	if model.checkpointPage != checkpointHistory {
		t.Fatal("History tab click did not restore checkpoint history")
	}
	model.handleCheckpointMouseClick(tea.Mouse{X: 11, Y: headerHeight + 1, Button: tea.MouseLeft})
	if model.checkpointPage != checkpointStatistics {
		t.Fatal("Summary tab click did not reopen checkpoint statistics")
	}
}

func TestCheckpointSelectionStaysPinnedAcrossRefresh(t *testing.T) {
	model := checkpointTestModel(t)
	model.openCheckpoints()
	model.moveCheckpointSelection(1)
	if model.checkpointSelectedID != 41 {
		t.Fatalf("selected checkpoint = %d, want 41", model.checkpointSelectedID)
	}

	next := model.snapshot
	next.UpdatedAt = next.UpdatedAt.Add(3 * time.Second)
	next.Checkpoints.History = append([]flink.Checkpoint{{ID: 43, Status: "COMPLETED"}}, next.Checkpoints.History...)
	model.Sync(Context{
		JobID: next.JobID, Summary: next.Checkpoints, Nodes: next.Nodes,
		Order: model.layout.Order, UpdatedAt: next.UpdatedAt,
		Generation: model.generation, BodyHeight: model.height,
	})
	refreshed := model
	selected, ok := refreshed.selectedCheckpoint()
	if !ok || selected.ID != 41 || refreshed.checkpointCursor != 2 {
		t.Fatalf("selection after refresh = checkpoint %#v cursor %d, want #41 at 2", selected, refreshed.checkpointCursor)
	}

	refreshed.handleCheckpointKey("home")
	if refreshed.checkpointSelectedID != 0 || refreshed.checkpointCursor != 0 {
		t.Fatalf("home = selected %d cursor %d, want latest-follow mode", refreshed.checkpointSelectedID, refreshed.checkpointCursor)
	}
}

func TestLatestFailedCheckpointCauseStaysVisibleOutsideRecentHistory(t *testing.T) {
	model := checkpointTestModel(t)
	model.openCheckpoints()
	model.snapshot.Checkpoints.History = model.snapshot.Checkpoints.History[:2]
	model.snapshot.Checkpoints.LatestFailed = &flink.Checkpoint{
		ID: 4380, Status: "FAILED", Failure: "Checkpoint Coordinator is suspending.",
	}

	rendered := model.renderCheckpoints(120, 21)
	for _, expected := range []string{"latest failed #4380", "Checkpoint Coordinator is suspending."} {
		if !strings.Contains(rendered, expected) {
			t.Fatalf("latest failed checkpoint banner missing %q:\n%s", expected, rendered)
		}
	}

	model.snapshot.Checkpoints.History = append(model.snapshot.Checkpoints.History, flink.Checkpoint{
		ID: 4380, Status: "FAILED", Failure: "Checkpoint Coordinator is suspending.",
	})
	model.checkpointCursor = 2
	model.checkpointSelectedID = 4380
	rendered = model.renderCheckpoints(120, 21)
	if !strings.Contains(rendered, "cause  Checkpoint Coordinator is suspending.") {
		t.Fatalf("selected failed checkpoint does not show cause:\n%s", rendered)
	}
}

func TestFailedCheckpointCountExplainsExpiredRESTHistory(t *testing.T) {
	model := checkpointTestModel(t)
	model.openCheckpoints()
	model.snapshot.Checkpoints.LatestFailed = nil
	model.snapshot.Checkpoints.History = model.snapshot.Checkpoints.History[:2]

	rendered := model.renderCheckpoints(120, 21)
	if !strings.Contains(rendered, "1 failed checkpoint aged out of REST history.") {
		t.Fatalf("expired checkpoint failure is silent:\n%s", rendered)
	}
	if got := agedOutFailedCheckpoints(model.snapshot.Checkpoints); got != 1 {
		t.Fatalf("aged-out failures = %d, want 1", got)
	}
}

func TestCheckpointHistoryFilterCoversOnlyRetainedRecords(t *testing.T) {
	model := checkpointTestModel(t)
	model.openCheckpoints()
	model.HandleKey("/", model.height)
	for _, key := range []string{"f", "a", "i", "l", "e", "d"} {
		model.HandleKey(key, model.height)
	}
	if history := model.filteredCheckpointHistory(); len(history) != 1 || history[0].ID != 40 {
		t.Fatalf("failed checkpoint filter = %#v", history)
	}
	result := model.HandleKey("enter", model.height)
	if model.CurrentView() != ViewOperators || result.Command == nil {
		t.Fatalf("filtered checkpoint enter = view:%v command nil:%t", model.CurrentView(), result.Command == nil)
	}

	model.Activate(ViewHistory)
	model.HandleKey("/", model.height)
	for _, key := range []string{"9", "9", "9"} {
		model.HandleKey(key, model.height)
	}
	model.HandleKey("esc", model.height)
	rendered := model.Render(120, model.height)
	if !strings.Contains(rendered, "No retained checkpoints match filter /999/") || !strings.Contains(rendered, "3 retained records") {
		t.Fatalf("checkpoint zero match does not explain REST retention:\n%s", rendered)
	}
}

func TestCheckpointOperatorFilterNarrowsAndDrills(t *testing.T) {
	model := checkpointTestModel(t)
	state := model.State()
	state.View = ViewOperators
	state.DetailID = 42
	state.Detail = flink.CheckpointDetails{
		JobID: "job", Checkpoint: flink.Checkpoint{ID: 42, Status: "COMPLETED"},
		Operators: []flink.CheckpointOperator{
			{VertexID: "source", Status: "COMPLETED", Subtasks: 2, AcknowledgedSubtasks: 2},
			{VertexID: "risk", Status: "COMPLETED", Subtasks: 2, AcknowledgedSubtasks: 2},
		},
	}
	model.RestoreState(state)
	model.HandleKey("/", model.height)
	for _, key := range []string{"r", "i", "s", "k"} {
		model.HandleKey(key, model.height)
	}
	if operators := model.sortedCheckpointOperators(); len(operators) != 1 || operators[0].VertexID != "risk" {
		t.Fatalf("checkpoint operator filter = %#v", operators)
	}
	result := model.HandleKey("enter", model.height)
	if model.CurrentView() != ViewSubtasks || result.Command == nil || model.State().SubtaskVertex != "risk" {
		t.Fatalf("filtered operator enter = view:%v vertex:%q command nil:%t", model.CurrentView(), model.State().SubtaskVertex, result.Command == nil)
	}

	model.Activate(ViewOperators)
	model.HandleKey("/", model.height)
	for _, key := range []string{"m", "i", "s", "s", "i", "n", "g"} {
		model.HandleKey(key, model.height)
	}
	model.HandleKey("esc", model.height)
	if rendered := model.Render(120, model.height); !strings.Contains(rendered, "No checkpoint operators match filter /missing/") {
		t.Fatalf("checkpoint operator zero match unexplained:\n%s", rendered)
	}
}

func TestCheckpointDurationDistinguishesZeroFromUnavailable(t *testing.T) {
	if got := humanCheckpointDuration(0); got != "0ms" {
		t.Fatalf("zero checkpoint duration = %q, want 0ms", got)
	}
	if got := humanCheckpointDuration(-time.Millisecond); got != "-" {
		t.Fatalf("unavailable checkpoint duration = %q, want em dash", got)
	}
}

func TestCheckpointHealthUsesNewestAttemptInsteadOfLifetimeFailures(t *testing.T) {
	now := time.Date(2026, time.August, 24, 12, 0, 0, 0, time.UTC)
	summary := flink.CheckpointSummary{
		Total:     268,
		Completed: 267,
		Failed:    1,
		History: []flink.Checkpoint{
			{ID: 268, Status: "COMPLETED", CompletedAt: now.Add(-5 * time.Second), Duration: 80 * time.Millisecond},
			{ID: 12, Status: "FAILED", CompletedAt: now.Add(-2 * time.Hour)},
		},
	}
	label, unhealthy := checkpointHealth(summary, now)
	if unhealthy || strings.Contains(label, "failed") || !strings.Contains(label, "checkpoint #268") || !strings.Contains(label, "5s ago") {
		t.Fatalf("healthy latest checkpoint = %q unhealthy=%t", label, unhealthy)
	}

	summary.History = append([]flink.Checkpoint{{
		ID: 269, Status: "FAILED", CompletedAt: now.Add(-7 * time.Second),
	}}, summary.History...)
	label, unhealthy = checkpointHealth(summary, now)
	if !unhealthy || !strings.Contains(label, "checkpoint #269 failed") || !strings.Contains(label, "7s ago") {
		t.Fatalf("failed latest checkpoint = %q unhealthy=%t", label, unhealthy)
	}
}

func TestCheckpointOperatorAndSubtaskDrilldown(t *testing.T) {
	model := checkpointTestModel(t)
	model.openCheckpoints()

	result := model.HandleKey("enter", model.height)
	operators := model
	if operators.CurrentView() != ViewOperators || operators.checkpointDetailID != 42 || result.Command == nil {
		t.Fatalf("operator drilldown = view %v checkpoint %d command nil %t",
			operators.CurrentView(), operators.checkpointDetailID, result.Command == nil)
	}

	details := flink.CheckpointDetails{
		JobID:      "job",
		Checkpoint: flink.Checkpoint{ID: 42, Status: "IN_PROGRESS"},
		Operators: []flink.CheckpointOperator{
			{VertexID: "source", Status: "COMPLETED", Duration: 10 * time.Millisecond, Subtasks: 2, AcknowledgedSubtasks: 2},
			{VertexID: "risk", Status: "IN_PROGRESS", Duration: 250 * time.Millisecond, StateSize: 4096, ProcessedData: 512, Subtasks: 2, AcknowledgedSubtasks: 1},
		},
		UpdatedAt: time.Now(),
	}
	config := flink.CheckpointConfig{
		Mode:                        "exactly_once",
		Interval:                    3 * time.Second,
		Timeout:                     20 * time.Second,
		MinimumPause:                time.Second,
		MaximumConcurrent:           1,
		StateBackend:                "HashMapStateBackend",
		CheckpointStorage:           "JobManagerCheckpointStorage",
		CheckpointsAfterTasksFinish: true,
	}
	detailRequestID, _ := operators.detailRequest.Begin(checkpointRequestKey{jobID: "job", generation: operators.generation, checkpointID: 42})
	operators.Apply(checkpointDetailMsg{
		details: details, config: config, jobID: "job", checkpointID: 42, generation: operators.generation, requestID: detailRequestID,
	})
	selectedOperator, ok := operators.selectedCheckpointOperator()
	if !ok || selectedOperator.VertexID != "risk" {
		t.Fatalf("diagnostic operator selection = %#v, want risk first", selectedOperator)
	}
	rendered := operators.Render(120, operators.height)
	for _, expected := range []string{"OPERATORS", "sort DIAGNOSIS", "MISSING 1 ACK", "HashMapStateBackend", "DURATION OUTLIER"} {
		if !strings.Contains(rendered, expected) {
			t.Fatalf("operator view missing %q:\n%s", expected, rendered)
		}
	}

	operators.handleCheckpointOperatorKey("i")
	rendered = operators.Render(120, operators.height)
	for _, expected := range []string{"minimum pause 1.0s", "max concurrent 1", "after finished tasks on"} {
		if !strings.Contains(rendered, expected) {
			t.Fatalf("checkpoint config missing %q:\n%s", expected, rendered)
		}
	}
	operators.checkpointConfigOpen = false

	result = operators.HandleKey("enter", operators.height)
	if operators.CurrentView() != ViewSubtasks || operators.checkpointSubtaskVertex != "risk" || result.Command == nil {
		t.Fatalf("subtask drilldown = view %v vertex %q command nil %t",
			operators.CurrentView(), operators.checkpointSubtaskVertex, result.Command == nil)
	}
	subtaskDetails := flink.CheckpointSubtaskDetails{
		JobID:        "job",
		CheckpointID: 42,
		VertexID:     "risk",
		Operator:     details.Operators[1],
		Summary: flink.CheckpointSubtaskSummary{
			EndToEndDuration: flink.CheckpointDistribution{Min: 10, Average: 15, Max: 20},
			StateSize:        flink.CheckpointDistribution{Min: 1024, Average: 2048, Max: 4096},
			StartDelay:       flink.CheckpointDistribution{Min: 1, Average: 3, Max: 5},
		},
		Subtasks: []flink.CheckpointSubtask{
			{Index: 0, Status: "completed", Duration: 20 * time.Millisecond, StateSize: 4096, SyncDuration: 2 * time.Millisecond, AsyncDuration: 6 * time.Millisecond, AlignmentDuration: 4 * time.Millisecond, StartDelay: 5 * time.Millisecond},
			{Index: 1, Status: "pending_or_failed", Aborted: true},
		},
	}
	subtaskRequestID, _ := operators.subtaskRequest.Begin(checkpointRequestKey{jobID: "job", generation: operators.generation, checkpointID: 42, vertexID: "risk"})
	operators.Apply(checkpointSubtasksMsg{
		details: subtaskDetails, jobID: "job", checkpointID: 42, vertexID: "risk", generation: operators.generation, requestID: subtaskRequestID,
	})
	subtasks := operators
	selectedSubtask, ok := subtasks.selectedCheckpointSubtask()
	if !ok || selectedSubtask.Index != 1 {
		t.Fatalf("diagnostic subtask selection = %#v, want pending subtask first", selectedSubtask)
	}
	rendered = subtasks.Render(120, subtasks.height)
	for _, expected := range []string{"SUBTASKS", "PENDING_OR_", "ABORTED", "min/avg/max"} {
		if !strings.Contains(rendered, expected) {
			t.Fatalf("subtask view missing %q:\n%s", expected, rendered)
		}
	}

	result = subtasks.HandleKey("g", subtasks.height)
	if result.Intent != IntentGraph || result.Vertex != "risk" {
		t.Fatalf("graph jump = intent %v vertex %q, want risk graph vertex", result.Intent, result.Vertex)
	}
}

func checkpointTestModel(t *testing.T) Model {
	t.Helper()
	client, err := flink.NewClient("http://localhost:8081")
	if err != nil {
		t.Fatal(err)
	}
	model := New(client, nil)
	model.height = 25
	model.snapshot.JobID = "job"
	model.snapshot.Nodes = []flink.Node{
		{ID: "source", Name: "Source", State: "RUNNING"},
		{ID: "risk", Name: "Risk", State: "RUNNING", Inputs: []flink.Input{{ID: "source"}}},
		{ID: "sink", Name: "Sink", State: "RUNNING", Inputs: []flink.Input{{ID: "risk"}}},
	}
	model.layout.Order = []string{"source", "risk", "sink"}
	model.layout.Rects = map[string]struct{}{"source": {}, "risk": {}, "sink": {}}
	model.snapshot.UpdatedAt = time.Date(2026, time.August, 22, 12, 30, 0, 0, time.Local)
	model.snapshot.Checkpoints = flink.CheckpointSummary{
		Total:     42,
		Completed: 41,
		Failed:    1,
		Statistics: flink.CheckpointStatistics{
			EndToEndDuration: flink.CheckpointDistribution{Min: 2, Average: 11, Max: 176, P50: 10, P90: 17, P95: 20, P99: 31.78, P999: 159.095},
			CheckpointedSize: flink.CheckpointDistribution{Min: 8_332, Average: 44_529, Max: 44_864, P50: 44_864, P90: 44_864, P95: 44_864, P99: 44_864, P999: 44_864},
			StateSize:        flink.CheckpointDistribution{Min: 8_332, Average: 44_529, Max: 44_864, P50: 44_864, P90: 44_864, P95: 44_864, P99: 44_864, P999: 44_864},
			ProcessedData:    flink.CheckpointDistribution{Min: 0, Average: 100, Max: 423, P50: 62.5, P90: 252, P95: 302.85, P99: 372, P999: 418.814},
		},
		LatestID:   42,
		LatestSize: 44_864,
		LatestFailed: &flink.Checkpoint{
			ID: 40, Status: "FAILED", Failure: "storage unavailable",
		},
		History: []flink.Checkpoint{
			{
				ID:                   42,
				Status:               "COMPLETED",
				Type:                 "CHECKPOINT",
				TriggeredAt:          model.snapshot.UpdatedAt.Add(-12 * time.Millisecond),
				CompletedAt:          model.snapshot.UpdatedAt,
				Duration:             12 * time.Millisecond,
				CheckpointedSize:     44_864,
				StateSize:            44_864,
				ProcessedData:        512,
				Subtasks:             10,
				AcknowledgedSubtasks: 10,
			},
			{ID: 41, Status: "COMPLETED", Duration: 9 * time.Millisecond},
			{ID: 40, Status: "FAILED", Duration: 20 * time.Millisecond, Failure: "storage unavailable"},
		},
	}
	return model
}
