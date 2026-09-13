package checkpoints

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/nikitasavinov/flink-tui/internal/flink"
)

func TestCheckpointDiagnosticSortingPreservesIdentity(t *testing.T) {
	model := checkpointDiagnosticTestModel(t)
	operatorSorts := []struct {
		sort checkpointOperatorSort
		want string
	}{
		{sortCheckpointOperatorDiagnosis, "risk"},
		{sortCheckpointOperatorDuration, "risk"},
		{sortCheckpointOperatorState, "sink"},
		{sortCheckpointOperatorAcknowledgement, "risk"},
		{sortCheckpointOperatorProcessed, "unknown"},
	}
	for _, test := range operatorSorts {
		model.checkpointOperatorSort = test.sort
		if got := model.sortedCheckpointOperators()[0].VertexID; got != test.want {
			t.Errorf("operator sort %v first = %q, want %q", test.sort, got, test.want)
		}
	}

	model.checkpointOperatorSelected = "sink"
	model.checkpointOperatorSort = sortCheckpointOperatorProcessed
	model.syncCheckpointOperatorSelection()
	if selected, ok := model.selectedCheckpointOperator(); !ok || selected.VertexID != "sink" {
		t.Fatalf("operator identity after sort = %#v, ok=%t", selected, ok)
	}

	subtaskSorts := []struct {
		sort checkpointSubtaskSort
		want int
	}{
		{sortCheckpointSubtaskDiagnosis, 3},
		{sortCheckpointSubtaskDuration, 2},
		{sortCheckpointSubtaskState, 1},
		{sortCheckpointSubtaskAlignment, 1},
		{sortCheckpointSubtaskStartDelay, 2},
	}
	for _, test := range subtaskSorts {
		model.checkpointSubtaskSort = test.sort
		if got := model.sortedCheckpointSubtasks()[0].Index; got != test.want {
			t.Errorf("subtask sort %v first = %d, want %d", test.sort, got, test.want)
		}
	}

	model.checkpointSubtaskSelected = 0
	model.checkpointSubtaskSort = sortCheckpointSubtaskState
	model.syncCheckpointSubtaskSelection()
	if selected, ok := model.selectedCheckpointSubtask(); !ok || selected.Index != 0 {
		t.Fatalf("subtask identity after sort = %#v, ok=%t", selected, ok)
	}
}

func TestCheckpointPeerStepPreservesDiagnosticScope(t *testing.T) {
	model := checkpointDiagnosticTestModel(t)
	model.checkpointOperatorSort = sortCheckpointOperatorState
	model.checkpointOperatorSelected = "risk"

	result := model.StepPeer(1)
	if result.Command == nil || result.Peer == nil || result.Peer.PreviousID != 42 || result.Peer.ID != 41 ||
		result.Peer.PreviousPosition != 1 || result.Peer.Position != 2 || result.Peer.Total != 3 {
		t.Fatalf("operator peer result = %#v command nil=%t", result.Peer, result.Command == nil)
	}
	if model.CurrentView() != ViewOperators || model.checkpointDetailID != 41 ||
		model.checkpointSelectedID != 41 || model.checkpointOperatorSort != sortCheckpointOperatorState ||
		model.checkpointOperatorSelected != "risk" {
		t.Fatalf("operator peer state = view:%d detail:%d selected:%d sort:%d operator:%q",
			model.CurrentView(), model.checkpointDetailID, model.checkpointSelectedID,
			model.checkpointOperatorSort, model.checkpointOperatorSelected)
	}

	model.Activate(ViewSubtasks)
	model.checkpointSubtaskSort = sortCheckpointSubtaskAlignment
	model.checkpointSubtaskVertex = "risk"
	result = model.StepPeer(1)
	if result.Peer == nil || result.Peer.ID != 40 || model.CurrentView() != ViewSubtasks {
		t.Fatalf("subtask peer = %#v view=%d", result.Peer, model.CurrentView())
	}
	requestID, _ := model.detailRequest.Begin(checkpointRequestKey{jobID: "job", generation: model.generation, checkpointID: 40})
	apply := model.Apply(checkpointDetailMsg{
		details: flink.CheckpointDetails{
			JobID: "job", Checkpoint: flink.Checkpoint{ID: 40, Status: "IN_PROGRESS"},
			Operators: []flink.CheckpointOperator{{VertexID: "risk", Status: "IN_PROGRESS", AcknowledgedSubtasks: 0}},
		},
		jobID: "job", checkpointID: 40, generation: model.generation, requestID: requestID,
	})
	if apply.Command == nil || apply.View != ViewSubtasks || model.checkpointSubtaskVertex != "risk" ||
		model.checkpointSubtaskSort != sortCheckpointSubtaskAlignment {
		t.Fatalf("subtask peer apply = command nil:%t view:%d vertex:%q sort:%d notice:%q",
			apply.Command == nil, apply.View, model.checkpointSubtaskVertex, model.checkpointSubtaskSort, apply.Notice)
	}
}

func TestCheckpointSubtaskPeerFallsBackWhenOperatorIsAbsent(t *testing.T) {
	model := checkpointDiagnosticTestModel(t)
	model.Activate(ViewSubtasks)
	model.checkpointSubtaskVertex = "risk"
	result := model.StepPeer(1)
	if result.Command == nil {
		t.Fatal("peer step returned no detail command")
	}
	requestID, _ := model.detailRequest.Begin(checkpointRequestKey{jobID: "job", generation: model.generation, checkpointID: 41})
	apply := model.Apply(checkpointDetailMsg{
		details: flink.CheckpointDetails{
			JobID: "job", Checkpoint: flink.Checkpoint{ID: 41, Status: "FAILED"},
			Operators: []flink.CheckpointOperator{{VertexID: "source", Status: "FAILED"}},
		},
		jobID: "job", checkpointID: 41, generation: model.generation, requestID: requestID,
	})
	if apply.View != ViewOperators || !apply.PeerFallback || !strings.Contains(apply.Notice, "no Risk operator data") {
		t.Fatalf("missing operator fallback = view:%d notice:%q", apply.View, apply.Notice)
	}
}

func TestCheckpointDurationMarkerRequiresARealOutlier(t *testing.T) {
	for _, test := range []struct {
		name        string
		candidate   time.Duration
		durations   []time.Duration
		wantOutlier bool
	}{
		{name: "healthy tie", candidate: 4 * time.Millisecond, durations: []time.Duration{4 * time.Millisecond, 4 * time.Millisecond, 4 * time.Millisecond, 4 * time.Millisecond}},
		{name: "tiny unique maximum", candidate: 5 * time.Millisecond, durations: []time.Duration{4 * time.Millisecond, 4 * time.Millisecond, 5 * time.Millisecond}},
		{name: "material straggler", candidate: 900 * time.Millisecond, durations: []time.Duration{80 * time.Millisecond, 100 * time.Millisecond, 900 * time.Millisecond}, wantOutlier: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := checkpointDurationOutlier(test.candidate, test.durations); got != test.wantOutlier {
				t.Fatalf("checkpointDurationOutlier() = %t, want %t", got, test.wantOutlier)
			}
		})
	}
}

func TestCheckpointDiagnosticKeyboardNavigation(t *testing.T) {
	operatorTests := []struct {
		name        string
		key         string
		prepare     func(*Model)
		assert      func(*testing.T, Model)
		wantCommand bool
		wantView    View
		wantIntent  Intent
		wantVertex  string
	}{
		{name: "back", key: "esc", wantView: ViewHistory},
		{name: "subtasks", key: "enter", wantCommand: true, wantView: ViewSubtasks, assert: func(t *testing.T, m Model) {
			if m.checkpointSubtaskVertex != "risk" {
				t.Fatalf("subtask drilldown vertex = %q", m.checkpointSubtaskVertex)
			}
		}},
		{name: "up", key: "up", wantView: ViewOperators, prepare: func(m *Model) { m.moveCheckpointOperatorTo(1) }, assert: assertOperatorCursor(0)},
		{name: "down", key: "down", wantView: ViewOperators, assert: assertOperatorCursor(1)},
		{name: "page up", key: "pgup", wantView: ViewOperators, prepare: func(m *Model) { m.moveCheckpointOperatorTo(3) }, assert: assertOperatorCursor(0)},
		{name: "page down", key: "pgdown", wantView: ViewOperators, assert: assertOperatorCursor(3)},
		{name: "home", key: "home", wantView: ViewOperators, prepare: func(m *Model) { m.moveCheckpointOperatorTo(3) }, assert: assertOperatorCursor(0)},
		{name: "end", key: "end", wantView: ViewOperators, assert: assertOperatorCursor(3)},
		{name: "sort", key: "s", wantView: ViewOperators, assert: func(t *testing.T, m Model) {
			if m.checkpointOperatorSort != sortCheckpointOperatorDuration {
				t.Fatalf("operator sort = %v, want duration", m.checkpointOperatorSort)
			}
		}},
		{name: "config", key: "i", wantView: ViewOperators, assert: func(t *testing.T, m Model) {
			if !m.checkpointConfigOpen {
				t.Fatal("configuration panel did not open")
			}
		}},
		{name: "graph", key: "g", wantView: ViewOperators, wantIntent: IntentGraph, wantVertex: "risk"},
		{name: "orphan overview key is inert", key: "o", wantView: ViewOperators},
	}
	for _, test := range operatorTests {
		t.Run("operator/"+test.name, func(t *testing.T) {
			model := checkpointDiagnosticTestModel(t)
			if test.prepare != nil {
				test.prepare(&model)
			}
			result := model.HandleKey(test.key, model.height)
			if (result.Command != nil) != test.wantCommand {
				t.Fatalf("command presence = %t, want %t", result.Command != nil, test.wantCommand)
			}
			assertCheckpointResult(t, result, test.wantView, test.wantIntent, test.wantVertex)
			if test.assert != nil {
				test.assert(t, model)
			}
		})
	}

	subtaskTests := []struct {
		name        string
		key         string
		prepare     func(*Model)
		assert      func(*testing.T, Model)
		wantCommand bool
		wantView    View
		wantIntent  Intent
		wantVertex  string
	}{
		{name: "operators", key: "esc", wantView: ViewOperators},
		{name: "orphan history key is inert", key: "c", wantView: ViewSubtasks},
		{name: "up", key: "up", wantView: ViewSubtasks, prepare: func(m *Model) { m.moveCheckpointSubtaskTo(1) }, assert: assertSubtaskCursor(0)},
		{name: "down", key: "down", wantView: ViewSubtasks, assert: assertSubtaskCursor(1)},
		{name: "page up", key: "pgup", wantView: ViewSubtasks, prepare: func(m *Model) { m.moveCheckpointSubtaskTo(3) }, assert: assertSubtaskCursor(0)},
		{name: "page down", key: "pgdown", wantView: ViewSubtasks, assert: assertSubtaskCursor(3)},
		{name: "home", key: "home", wantView: ViewSubtasks, prepare: func(m *Model) { m.moveCheckpointSubtaskTo(3) }, assert: assertSubtaskCursor(0)},
		{name: "end", key: "end", wantView: ViewSubtasks, assert: assertSubtaskCursor(3)},
		{name: "sort", key: "s", wantView: ViewSubtasks, assert: func(t *testing.T, m Model) {
			if m.checkpointSubtaskSort != sortCheckpointSubtaskDuration {
				t.Fatalf("subtask sort = %v, want duration", m.checkpointSubtaskSort)
			}
		}},
		{name: "graph", key: "g", wantView: ViewSubtasks, wantIntent: IntentGraph, wantVertex: "risk"},
		{name: "orphan overview key is inert", key: "o", wantView: ViewSubtasks},
	}
	for _, test := range subtaskTests {
		t.Run("subtask/"+test.name, func(t *testing.T) {
			model := checkpointDiagnosticTestModel(t)
			model.Activate(ViewSubtasks)
			if test.prepare != nil {
				test.prepare(&model)
			}
			result := model.HandleKey(test.key, model.height)
			if (result.Command != nil) != test.wantCommand {
				t.Fatalf("command presence = %t, want %t", result.Command != nil, test.wantCommand)
			}
			assertCheckpointResult(t, result, test.wantView, test.wantIntent, test.wantVertex)
			if test.assert != nil {
				test.assert(t, model)
			}
		})
	}
}

func TestCheckpointDiagnosticSelectionAndMouseBounds(t *testing.T) {
	model := checkpointDiagnosticTestModel(t)
	model.moveCheckpointOperatorSelection(0)
	model.moveCheckpointOperatorSelection(1)
	if model.checkpointOperatorCursor != 1 {
		t.Fatalf("operator cursor = %d, want 1", model.checkpointOperatorCursor)
	}
	model.handleCheckpointOperatorMouseClick(tea.Mouse{Y: headerHeight + checkpointDiagnosticBodyRowStart + 2, Button: tea.MouseRight})
	model.handleCheckpointOperatorMouseClick(tea.Mouse{Y: 0, Button: tea.MouseLeft})
	if model.checkpointOperatorCursor != 1 {
		t.Fatal("invalid operator click changed selection")
	}
	model.handleCheckpointOperatorMouseClick(tea.Mouse{Y: headerHeight + checkpointDiagnosticBodyRowStart + 2, Button: tea.MouseLeft})
	if model.checkpointOperatorCursor != 2 {
		t.Fatalf("operator click cursor = %d, want 2", model.checkpointOperatorCursor)
	}

	model.moveCheckpointSubtaskSelection(0)
	model.moveCheckpointSubtaskSelection(1)
	if model.checkpointSubtaskCursor != 1 {
		t.Fatalf("subtask cursor = %d, want 1", model.checkpointSubtaskCursor)
	}
	model.handleCheckpointSubtaskMouseClick(tea.Mouse{Y: headerHeight + checkpointDiagnosticBodyRowStart + 2, Button: tea.MouseRight})
	model.handleCheckpointSubtaskMouseClick(tea.Mouse{Y: 0, Button: tea.MouseLeft})
	if model.checkpointSubtaskCursor != 1 {
		t.Fatal("invalid subtask click changed selection")
	}
	model.handleCheckpointSubtaskMouseClick(tea.Mouse{Y: headerHeight + checkpointDiagnosticBodyRowStart + 2, Button: tea.MouseLeft})
	if model.checkpointSubtaskCursor != 2 {
		t.Fatalf("subtask click cursor = %d, want 2", model.checkpointSubtaskCursor)
	}

	empty := checkpointTestModel(t)
	empty.checkpointOperatorCursor, empty.checkpointOperatorSelected = 9, "missing"
	empty.syncCheckpointOperatorSelection()
	empty.moveCheckpointOperatorTo(1)
	if empty.checkpointOperatorCursor != 0 || empty.checkpointOperatorSelected != "" {
		t.Fatalf("empty operator selection = cursor:%d selected:%q", empty.checkpointOperatorCursor, empty.checkpointOperatorSelected)
	}
	empty.checkpointSubtaskCursor, empty.checkpointSubtaskSelected = 9, 9
	empty.syncCheckpointSubtaskSelection()
	empty.moveCheckpointSubtaskTo(1)
	if empty.checkpointSubtaskCursor != 0 || empty.checkpointSubtaskSelected != -1 {
		t.Fatalf("empty subtask selection = cursor:%d selected:%d", empty.checkpointSubtaskCursor, empty.checkpointSubtaskSelected)
	}
}

func TestCheckpointDiagnosticErrorsAndResponsiveRows(t *testing.T) {
	model := checkpointDiagnosticTestModel(t)
	for _, width := range []int{70, 90, 120} {
		operatorRow := model.renderCheckpointOperatorRow(model.checkpointDetail.Operators[1], true, width)
		subtaskRow := model.renderCheckpointSubtaskRow(model.checkpointSubtasks.Subtasks[1], true, width)
		if !strings.Contains(operatorRow, "Risk") || !strings.Contains(subtaskRow, "PENDING") {
			t.Fatalf("width %d rows missing content:\n%s\n%s", width, operatorRow, subtaskRow)
		}
	}

	model.checkpointDetailErr = errors.New("details unavailable")
	if status := model.renderCheckpointDiagnosticStatus(120); !strings.Contains(status, "Detail refresh failed") {
		t.Fatalf("detail error status = %q", status)
	}
	model.checkpointDetailErr = nil
	model.checkpointDetail.Checkpoint.Failure = "storage failed"
	if status := model.renderCheckpointDiagnosticStatus(120); !strings.Contains(status, "Failure: storage failed") {
		t.Fatalf("checkpoint failure status = %q", status)
	}
	model.checkpointDetail.Checkpoint.Failure = ""
	model.checkpointConfigErr = errors.New("config unavailable")
	if status := model.renderCheckpointDiagnosticStatus(160); !strings.Contains(status, "config unavailable") {
		t.Fatalf("config error status = %q", status)
	}
	if lines := model.renderCheckpointConfig(120); !strings.Contains(lines[0], "config unavailable") || lines[1] != "" || lines[2] != "" {
		t.Fatalf("config error detail = %#v", lines)
	}

	model.checkpointSubtasksErr = errors.New("subtasks unavailable")
	if status := model.renderCheckpointSubtaskStatus(120); !strings.Contains(status, "Subtask refresh failed") {
		t.Fatalf("subtask error status = %q", status)
	}
	model.checkpointSubtasksErr = nil
	model.checkpointConfigErr = nil
	model.checkpointConfig.ExternalizationEnabled = true
	if lines := model.renderCheckpointConfig(160); !strings.Contains(lines[1], "retain on cancellation") {
		t.Fatalf("retained externalization = %#v", lines)
	}
	model.checkpointConfig.DeleteExternalizedOnCancellation = true
	if lines := model.renderCheckpointConfig(160); !strings.Contains(lines[1], "delete on cancellation") {
		t.Fatalf("deleted externalization = %#v", lines)
	}
}

func TestCheckpointDiagnosticRanksAndLabels(t *testing.T) {
	operatorRanks := map[string]int{
		"FAILED": 3, "canceled": 3, "CANCELLED": 3, "in_progress": 2,
		"pending": 2, "completed": 0, "unknown": 1,
	}
	for status, want := range operatorRanks {
		if got := checkpointStatusRank(status); got != want {
			t.Errorf("status rank %q = %d, want %d", status, got, want)
		}
	}
	subtaskRanks := []struct {
		subtask flink.CheckpointSubtask
		want    int
	}{
		{flink.CheckpointSubtask{Aborted: true}, 4},
		{flink.CheckpointSubtask{Status: "failed"}, 3},
		{flink.CheckpointSubtask{Status: "pending_or_failed"}, 3},
		{flink.CheckpointSubtask{Status: "pending"}, 2},
		{flink.CheckpointSubtask{Status: "in_progress"}, 2},
		{flink.CheckpointSubtask{Status: "completed"}, 0},
		{flink.CheckpointSubtask{Status: "unknown"}, 1},
	}
	for _, test := range subtaskRanks {
		if got := checkpointSubtaskRank(test.subtask); got != test.want {
			t.Errorf("subtask rank %#v = %d, want %d", test.subtask, got, test.want)
		}
	}

	model := checkpointDiagnosticTestModel(t)
	operatorLabels := []string{"DIAGNOSIS", "DURATION", "STATE SIZE", "ACKNOWLEDGEMENT", "PROCESSED DATA"}
	for index, want := range operatorLabels {
		model.checkpointOperatorSort = checkpointOperatorSort(index)
		if got := model.checkpointOperatorSortLabel(); got != want {
			t.Errorf("operator sort label %d = %q, want %q", index, got, want)
		}
	}
	subtaskLabels := []string{"DIAGNOSIS", "DURATION", "STATE SIZE", "ALIGNMENT", "START DELAY"}
	for index, want := range subtaskLabels {
		model.checkpointSubtaskSort = checkpointSubtaskSort(index)
		if got := model.checkpointSubtaskSortLabel(); got != want {
			t.Errorf("subtask sort label %d = %q, want %q", index, got, want)
		}
	}
}

func TestCheckpointDiagnosticNoSelectionAndUnknownGraphVertex(t *testing.T) {
	model := checkpointTestModel(t)
	model.snapshot.Checkpoints.History = nil
	if command := model.openCheckpointDetail(); command != nil {
		t.Fatal("missing checkpoint opened detail")
	}
	if command := model.openCheckpointSubtasks(); command != nil {
		t.Fatal("missing operator opened subtasks")
	}
	model.jumpCheckpointOperatorToGraph()
	model.jumpCheckpointVertexToGraph("missing")
	if model.pendingIntent == IntentGraph || model.pendingVertex == "missing" {
		t.Fatal("unknown vertex opened in graph")
	}
	if got := model.renderSelectedCheckpointOperator(80)[0]; !strings.Contains(got, "Select an operator") {
		t.Fatalf("empty operator detail = %q", got)
	}
	if got := model.renderSelectedCheckpointSubtask(80)[0]; !strings.Contains(got, "Select a subtask") {
		t.Fatalf("empty subtask detail = %q", got)
	}
}

func assertCheckpointResult(t *testing.T, result Result, wantView View, wantIntent Intent, wantVertex string) {
	t.Helper()
	if result.View != wantView || result.Intent != wantIntent || result.Vertex != wantVertex {
		t.Fatalf("result = view:%d intent:%d vertex:%q, want view:%d intent:%d vertex:%q",
			result.View, result.Intent, result.Vertex, wantView, wantIntent, wantVertex)
	}
}

func assertOperatorCursor(want int) func(*testing.T, Model) {
	return func(t *testing.T, model Model) {
		if model.checkpointOperatorCursor != want {
			t.Fatalf("operator cursor = %d, want %d", model.checkpointOperatorCursor, want)
		}
	}
}

func assertSubtaskCursor(want int) func(*testing.T, Model) {
	return func(t *testing.T, model Model) {
		if model.checkpointSubtaskCursor != want {
			t.Fatalf("subtask cursor = %d, want %d", model.checkpointSubtaskCursor, want)
		}
	}
}

func checkpointDiagnosticTestModel(t *testing.T) Model {
	t.Helper()
	model := checkpointTestModel(t)
	now := time.Date(2026, time.August, 28, 12, 0, 0, 0, time.UTC)
	model.Activate(ViewOperators)
	model.checkpointDetailID = 42
	model.checkpointDetail = flink.CheckpointDetails{
		JobID: "job", Checkpoint: flink.Checkpoint{ID: 42, Status: "IN_PROGRESS"},
		Operators: []flink.CheckpointOperator{
			{VertexID: "source", Status: "COMPLETED", LatestAcknowledgedAt: now, Duration: 10 * time.Millisecond, StateSize: 100, ProcessedData: 400, Subtasks: 2, AcknowledgedSubtasks: 2},
			{VertexID: "risk", Status: "IN_PROGRESS", LatestAcknowledgedAt: now.Add(time.Second), Duration: 50 * time.Millisecond, StateSize: 200, ProcessedData: 300, Subtasks: 2, AcknowledgedSubtasks: 1},
			{VertexID: "sink", Status: "COMPLETED", LatestAcknowledgedAt: now.Add(2 * time.Second), Duration: 30 * time.Millisecond, StateSize: 900, ProcessedData: 200, Subtasks: 2, AcknowledgedSubtasks: 2},
			{VertexID: "unknown", Status: "FAILED", LatestAcknowledgedAt: now.Add(3 * time.Second), Duration: 20 * time.Millisecond, StateSize: 500, ProcessedData: 1000, Subtasks: 2, AcknowledgedSubtasks: 2},
		},
	}
	model.checkpointConfig = flink.CheckpointConfig{Mode: "exactly_once", Interval: 3 * time.Second, Timeout: 20 * time.Second}
	model.checkpointSubtaskVertex = "risk"
	model.checkpointSubtasks = flink.CheckpointSubtaskDetails{
		JobID: "job", CheckpointID: 42, VertexID: "risk", Operator: model.checkpointDetail.Operators[1],
		Subtasks: []flink.CheckpointSubtask{
			{Index: 0, Status: "completed", Duration: 10 * time.Millisecond, StateSize: 100, AlignmentDuration: time.Millisecond, AlignmentProcessed: 10, StartDelay: 2 * time.Millisecond},
			{Index: 1, Status: "pending", Duration: 20 * time.Millisecond, StateSize: 900, AlignmentDuration: 5 * time.Millisecond, AlignmentProcessed: 20, StartDelay: time.Millisecond, Unaligned: true},
			{Index: 2, Status: "completed", Duration: 50 * time.Millisecond, StateSize: 200, AlignmentDuration: 2 * time.Millisecond, AlignmentProcessed: 100, StartDelay: 30 * time.Millisecond},
			{Index: 3, Status: "failed", Aborted: true},
		},
	}
	model.checkpointSubtaskSelected = -1
	model.syncCheckpointOperatorSelection()
	model.syncCheckpointSubtaskSelection()
	if operators := operatorIDs(model.sortedCheckpointOperators()); !reflect.DeepEqual(operators, []string{"risk", "unknown", "sink", "source"}) {
		t.Fatalf("fixture operator order = %#v", operators)
	}
	return model
}

func operatorIDs(operators []flink.CheckpointOperator) []string {
	result := make([]string, len(operators))
	for index, operator := range operators {
		result[index] = operator.VertexID
	}
	return result
}
