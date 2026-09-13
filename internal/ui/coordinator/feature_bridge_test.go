package coordinator

import (
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/nikitasavinov/flink-tui/internal/flink"
	"github.com/nikitasavinov/flink-tui/internal/ui/jobgraph"
	processmodule "github.com/nikitasavinov/flink-tui/internal/ui/processdiag"
)

func TestMetricHistoryKeepsOnlySixtySeconds(t *testing.T) {
	model := interactionTestModel(t)
	now := time.Now()
	node := model.snapshot.Nodes[0]
	node.Metrics.BusyPercent = 10
	model.recordHistory(flink.Snapshot{UpdatedAt: now.Add(-70 * time.Second), Nodes: []flink.Node{node}})
	node.Metrics.BusyPercent = 80
	model.recordHistory(flink.Snapshot{UpdatedAt: now, Nodes: []flink.Node{node}})

	samples := model.nodeHistory(node.ID)
	if len(samples) != 1 || samples[0].Metrics.BusyPercent != 80 {
		t.Fatalf("history = %#v, want only latest sample", samples)
	}
	if got := len([]rune(jobgraph.SparklineFixed([]float64{0, 50, 100}, 8, 100))); got != 8 {
		t.Fatalf("sparkline width = %d, want 8", got)
	}
}

func TestInitialGraphSnapshotSelectsHottestVertex(t *testing.T) {
	model := parityTestModel(t)
	model.snapshot = flink.Snapshot{}
	model.selected = ""
	next := flink.Snapshot{JobID: "hot-job", JobName: "Incident", Nodes: []flink.Node{
		{ID: "source", Name: "Orders Source", Metrics: flink.Metrics{BackpressurePercent: 92.7}},
		{ID: "risk", Name: "Risk Score", Inputs: []flink.Input{{ID: "source"}}, Metrics: flink.Metrics{BusyPercent: 100}},
		{ID: "sink", Name: "Sink", Inputs: []flink.Input{{ID: "risk"}}, Metrics: flink.Metrics{BusyPercent: 4}},
	}}
	model.applySnapshot(snapshotMsg{snapshot: next, generation: model.generation})
	if model.selected != "risk" {
		t.Fatalf("initial graph selected %q, want hottest risk vertex", model.selected)
	}
}

func TestJobPickerSwitchesJobsInPlace(t *testing.T) {
	model := interactionTestModel(t)
	updated, command := model.Update(tea.KeyPressMsg(tea.Key{Code: '1', Text: "1"}))
	opened := updated.(Model)
	if opened.mode != modeJobs || command == nil {
		t.Fatalf("job picker mode = %v, command nil = %t", opened.mode, command == nil)
	}

	state := opened.jobList.State()
	state.Jobs = []flink.JobSummary{
		{ID: "job", Name: "Current", State: "RUNNING"},
		{ID: "other", Name: "Completed", State: "FINISHED"},
	}
	opened.jobList.RestoreState(state)
	loaded := opened
	updated, _ = loaded.Update(tea.KeyPressMsg(tea.Key{Code: tea.KeyTab}))
	loaded = updated.(Model)
	updated, command = loaded.Update(tea.KeyPressMsg(tea.Key{Code: tea.KeyEnter}))
	switched := updated.(Model)
	if switched.mode != modeGraph || switched.preferredJobID != "other" || switched.generation != 1 || command == nil {
		t.Fatalf("switched model = mode %v job %q generation %d command nil %t",
			switched.mode, switched.preferredJobID, switched.generation, command == nil)
	}
}

func TestNewModelStartsAtClusterOverviewUnlessDeepLinked(t *testing.T) {
	client, err := flink.NewClient("http://localhost:8081")
	if err != nil {
		t.Fatal(err)
	}
	home := NewModel(client, "", 3*time.Second)
	if home.mode != modeJobs || !home.jobList.State().Loading || home.loading {
		t.Fatalf("default model = mode %v jobsLoading %t loading %t", home.mode, home.jobList.State().Loading, home.loading)
	}

	direct := NewModel(client, "job-id", 3*time.Second)
	if direct.mode != modeGraph || !direct.loading || direct.jobList.State().Loading {
		t.Fatalf("deep-linked model = mode %v jobsLoading %t loading %t", direct.mode, direct.jobList.State().Loading, direct.loading)
	}
}

func TestClusterOverviewRendersCapacityJobsAndNavigation(t *testing.T) {
	model := interactionTestModel(t)
	model.mode = modeJobs
	model.width = 120
	model.height = 24
	overview := model.jobList.State()
	overview.Jobs = []flink.JobSummary{
		{ID: "job", Name: "Current", State: "RUNNING", RunningTasks: 4, TotalTasks: 4},
		{ID: "other", Name: "Checkpoint Fixture", State: "RUNNING", RunningTasks: 10, TotalTasks: 10},
	}
	overview.Cluster = flink.ClusterOverview{
		TaskManagers:   1,
		SlotsTotal:     16,
		SlotsAvailable: 12,
		JobsRunning:    2,
		FlinkVersion:   "2.3.0",
		FlinkCommit:    "abc1234",
		UpdatedAt:      time.Now(),
	}
	overview.UpdatedAt = overview.Cluster.UpdatedAt
	model.jobList.RestoreState(overview)
	rendered := model.render()
	for _, expected := range []string{
		"Cluster Overview",
		"Flink 2.3.0",
		"task managers 1",
		"slots 4/16 used",
		"jobs 2 running",
		"Checkpoint Fixture",
	} {
		if !strings.Contains(rendered, expected) {
			t.Fatalf("overview missing %q:\n%s", expected, rendered)
		}
	}
	if got := strings.Count(rendered, "\n") + 1; got != model.height {
		t.Fatalf("rendered lines = %d, want %d", got, model.height)
	}

	model.mode = modeGraph
	updated, command := model.Update(tea.KeyPressMsg(tea.Key{Code: tea.KeyEscape}))
	home := updated.(Model)
	if home.mode != modeJobs || command == nil {
		t.Fatalf("escape = mode %v command nil %t, want overview fetch", home.mode, command == nil)
	}
}

func TestSubtaskDrilldownAndSort(t *testing.T) {
	model := interactionTestModel(t)
	updated, command := model.Update(tea.KeyPressMsg(tea.Key{Code: tea.KeyEnter}))
	opened := updated.(Model)
	state := opened.jobDetails.State()
	if opened.mode != modeSubtasks || state.DiagnosticsOpen != "source" || command == nil {
		t.Fatalf("drilldown = mode %v vertex %q command nil %t", opened.mode, state.DiagnosticsOpen, command == nil)
	}

	diagnostics := flink.VertexDiagnostics{
		JobID:    "job",
		VertexID: "source",
		Name:     "Source",
		Subtasks: []flink.Subtask{
			{Index: 0, State: "RUNNING", Metrics: flink.Metrics{BackpressurePercent: 10, BusyPercent: 90}},
			{Index: 1, State: "RUNNING", Metrics: flink.Metrics{BackpressurePercent: 80, BusyPercent: 20}},
		},
	}
	state.Diagnostics = diagnostics
	state.DiagnosticsBusy = false
	state.SelectedSubtask = 0
	opened.jobDetails.RestoreState(state)
	loaded := opened
	_ = loaded.handleSubtaskKey("s")
	_ = loaded.handleSubtaskKey("p")
	state = loaded.jobDetails.State()
	if state.SubtaskSort != 2 || !state.SubtaskSortDescending {
		t.Fatalf("backpressure sort = %d descending=%t", state.SubtaskSort, state.SubtaskSortDescending)
	}
	loaded.moveSubtaskSelection(1)
	if selected := loaded.jobDetails.State().SelectedSubtask; selected != 0 {
		t.Fatalf("selected subtask = %d, want 0 after moving in sorted order", selected)
	}
}

func TestSubtaskThreadDumpJumpFocusesExecutionThread(t *testing.T) {
	model := interactionTestModel(t)
	model.mode = modeSubtasks
	state := model.jobDetails.State()
	state.DiagnosticsOpen = "risk"
	state.Diagnostics = flink.VertexDiagnostics{
		JobID:         "job",
		VertexID:      "risk",
		Name:          "Risk Score",
		ExecutionName: "Risk Score: Writer",
		Parallelism:   4,
		Subtasks: []flink.Subtask{
			{Index: 1, State: "RUNNING", TaskManagerID: "tm-b", Endpoint: "tm-b:123"},
		},
	}
	state.SelectedSubtask = 1
	model.jobDetails.RestoreState(state)

	updated, command := model.Update(tea.KeyPressMsg(tea.Key{Code: 'd', Text: "d"}))
	opened := updated.(Model)
	processState := opened.processDiagnostics.State()
	if command == nil || opened.mode != modeThreadDump || processState.ThreadBackToken != int(modeSubtasks) {
		t.Fatalf("thread dump jump = mode %d back %d command nil %t", opened.mode, processState.ThreadBackToken, command == nil)
	}
	if processState.Process != flink.TaskManagerProcess("tm-b") {
		t.Fatalf("thread dump process = %#v", processState.Process)
	}
	if processState.ThreadFocus.Prefix != "Risk Score: Writer (2/" || processState.ThreadFocus.Label != "subtask #1" {
		t.Fatalf("thread focus = %#v", processState.ThreadFocus)
	}

	processState.Threads = []flink.ThreadInfo{
		{Name: "main", Stack: "\"main\" Id=1 WAITING"},
		{Name: "Risk Score: Writer (2/4)#0", Stack: "\"Risk Score: Writer (2/4)#0\" Id=2 RUNNABLE"},
		{Name: "Legacy Source Thread - Risk Score: Writer (2/4)#1", Stack: "\"Legacy Source Thread - Risk Score: Writer (2/4)#1\" Id=3 RUNNABLE"},
	}
	processState.ThreadBusy = false
	opened.processDiagnostics.RestoreState(processState)
	focused := opened
	thread, ok := focused.selectedThread()
	processState = focused.processDiagnostics.State()
	if !ok || thread.Name != "Legacy Source Thread - Risk Score: Writer (2/4)#1" || !processState.ThreadFocus.Matched {
		t.Fatalf("focused thread = %#v ok=%t focus=%#v", thread, ok, processState.ThreadFocus)
	}
	rendered := focused.renderThreadDump(140, 24)
	for _, expected := range []string{"subtask #1", "focused execution thread Risk Score: Writer (2/", "Legacy Source Thread - Risk Score"} {
		if !strings.Contains(rendered, expected) {
			t.Fatalf("focused thread dump missing %q:\n%s", expected, rendered)
		}
	}

	updated, _ = focused.Update(tea.KeyPressMsg(tea.Key{Code: tea.KeyEscape}))
	if returned := updated.(Model); returned.mode != modeSubtasks {
		t.Fatalf("thread dump returned to mode %d, want subtasks", returned.mode)
	}
}

func TestSubtaskThreadDumpJumpHasMouseCellAndSafeFallbacks(t *testing.T) {
	model := interactionTestModel(t)
	model.mode = modeSubtasks
	model.width = 160
	model.height = 30
	setTestNavigation(&model, navigationHidden, false)
	state := model.jobDetails.State()
	state.DiagnosticsOpen = "source"
	state.Diagnostics = flink.VertexDiagnostics{
		Name: "Busy Map", Parallelism: 2,
		Subtasks: []flink.Subtask{{Index: 0, State: "RUNNING", TaskManagerID: "tm-a", Endpoint: "tm-a:123"}},
	}
	state.SelectedSubtask = 0
	model.jobDetails.RestoreState(state)
	if rendered := model.renderSubtasks(160, 26); !strings.Contains(rendered, "[dump]") {
		t.Fatalf("subtask table has no dump cell:\n%s", rendered)
	}

	command := model.handleSubtaskMouseClick(tea.Mouse{
		X: 159, Y: headerHeight + 4, Button: tea.MouseLeft,
	})
	processState := model.processDiagnostics.State()
	if command == nil || model.mode != modeThreadDump || processState.ThreadFocus.Prefix != "Busy Map (1/" {
		t.Fatalf("mouse dump jump = mode %d command nil %t focus %#v", model.mode, command == nil, processState.ThreadFocus)
	}

	processState.Threads = []flink.ThreadInfo{{Name: "main", Stack: "\"main\" Id=1 WAITING"}}
	processState.ThreadBusy = false
	model.processDiagnostics.RestoreState(processState)
	processState = model.processDiagnostics.State()
	if processState.ThreadFocus.Matched || processState.ThreadCursor != 0 {
		t.Fatalf("unmatched focus = %#v cursor %d", processState.ThreadFocus, processState.ThreadCursor)
	}
	if rendered := model.renderThreadDump(140, 20); !strings.Contains(rendered, "no thread matched Busy Map (1/") {
		t.Fatalf("unmatched thread fallback is not visible:\n%s", rendered)
	}

	model.mode = modeSubtasks
	state = model.jobDetails.State()
	state.Diagnostics.Subtasks[0].TaskManagerID = ""
	model.jobDetails.RestoreState(state)
	processState.ThreadFocus = processmodule.Focus{}
	model.processDiagnostics.RestoreState(processState)
	command = model.handleSubtaskKey("d")
	if command != nil || model.mode != modeSubtasks || !strings.Contains(model.activeNotice(), "no TaskManager assignment") {
		t.Fatalf("unassigned subtask jump = mode %d command=%v notice=%q", model.mode, command, model.activeNotice())
	}

	state = model.jobDetails.State()
	state.Diagnostics.Subtasks[0].TaskManagerID = "(unassigned)"
	model.jobDetails.RestoreState(state)
	model.notice = ""
	command = model.handleSubtaskKey("d")
	if command != nil || model.mode != modeSubtasks || !strings.Contains(model.activeNotice(), "no TaskManager assignment") {
		t.Fatalf("Flink unassigned sentinel jump = mode %d command=%v notice=%q", model.mode, command, model.activeNotice())
	}
	if rendered := model.renderSubtasks(140, 20); strings.Contains(rendered, "[dump]") {
		t.Fatalf("Flink unassigned sentinel rendered a dump action:\n%s", rendered)
	}
}

func TestNarrowDiagnosticsKeepsDiagnosisAndDropsIdle(t *testing.T) {
	model := interactionTestModel(t)
	model.openDiagnosticOverview(diagnosticBackpressure)
	node := flink.Node{
		ID:   "risk",
		Name: "A deliberately long operator name",
		Metrics: flink.Metrics{
			BusyPercent:         40,
			BackpressurePercent: 85,
			BackpressureLevel:   flink.BackpressureHigh,
			IdlePercent:         2,
		},
	}
	model.snapshot.Nodes = []flink.Node{node}
	model.selected = node.ID
	model.syncJobDetailContext()
	rendered := model.renderDiagnosticOverview(80, 20)
	if !strings.Contains(rendered, "DIAGNOSIS") || strings.Contains(rendered, "IDLE") {
		t.Fatalf("narrow diagnostics = %q", rendered)
	}
	if !strings.Contains(rendered, "downstream pressure") {
		t.Fatalf("narrow view hid diagnosis: %q", rendered)
	}
}

func TestGraphRendersHealthSignalsAndHistory(t *testing.T) {
	model := interactionTestModel(t)
	model.width = 120
	model.height = 30
	model.snapshot.JobState = "RUNNING"
	model.snapshot.Checkpoints = flink.CheckpointSummary{Total: 4, Completed: 4, LatestID: 9, LatestDuration: 1200 * time.Millisecond, LatestSize: 2048}
	model.snapshot.Exceptions = flink.ExceptionSummary{Count: 1, Latest: "boom"}
	model.snapshot.UpdatedAt = time.Now()
	model.snapshot.Nodes[0].Metrics = flink.Metrics{
		BusyPercent:         55,
		BackpressurePercent: 20,
		BackpressureLevel:   flink.BackpressureLow,
		DataSkewPercent:     12.5,
		LowWatermark:        time.Now().UnixMilli(),
		WatermarkKnown:      true,
	}
	model.recordHistory(model.snapshot)

	rendered := model.render()
	for _, expected := range []string{"checkpoint #9", "exceptions 1: boom", "input skew  12.5%", "trend  busy"} {
		if !strings.Contains(rendered, expected) {
			t.Fatalf("render missing %q:\n%s", expected, rendered)
		}
	}
	if got := strings.Count(rendered, "\n") + 1; got != model.height {
		t.Fatalf("rendered lines = %d, want %d", got, model.height)
	}
}

func TestStaleSnapshotIsIgnoredAfterJobSwitch(t *testing.T) {
	model := interactionTestModel(t)
	model.generation = 2
	updated, _ := model.Update(snapshotMsg{
		generation: 1,
		snapshot:   flink.Snapshot{JobID: "old", JobName: "Old"},
	})
	got := updated.(Model)
	if got.snapshot.JobID != "job" {
		t.Fatalf("stale snapshot changed job to %q", got.snapshot.JobID)
	}
}

func TestSnapshotChangesPulseUntilClearMessage(t *testing.T) {
	model := interactionTestModel(t)
	next := model.snapshot
	next.Nodes = append([]flink.Node(nil), model.snapshot.Nodes...)
	next.Nodes[0].Metrics.BusyPercent = 82
	next.Nodes[0].Metrics.RecordsInPerSecond = 1234
	next.UpdatedAt = time.Now()

	updated, command := model.Update(snapshotMsg{snapshot: next, generation: model.generation})
	changed := updated.(Model)
	if command == nil {
		t.Fatal("changed metrics did not schedule a pulse clear")
	}
	if change := changed.graphTelemetry.Changes()["source"]; !change.Busy || !change.Input {
		t.Fatalf("source changes = %#v, want busy and input", change)
	}

	updated, _ = changed.Update(metricPulseClearMsg{generation: changed.metricPulseGeneration})
	if got := updated.(Model).graphTelemetry.Changes(); len(got) != 0 {
		t.Fatalf("metric pulse was not cleared: %#v", got)
	}
}

func TestCumulativeCountersRenderForVertexAndSubtasks(t *testing.T) {
	model := parityTestModel(t)
	model.snapshot.Nodes[0].Metrics.RecordsIn = 12_345
	model.snapshot.Nodes[0].Metrics.RecordsOut = 12_000
	model.snapshot.Nodes[0].Metrics.BytesIn = 1_048_576
	model.snapshot.Nodes[0].Metrics.BytesOut = 524_288
	inspector := model.renderInspector(100)
	for _, expected := range []string{"total", "12.3k rec", "1.0MiB", "12.0k rec", "512KiB"} {
		if !strings.Contains(inspector, expected) {
			t.Fatalf("inspector missing %q:\n%s", expected, inspector)
		}
	}

	model.mode = modeSubtasks
	detailState := model.jobDetails.State()
	detailState.Diagnostics = flink.VertexDiagnostics{
		Name: "Source", Parallelism: 1,
		Subtasks: []flink.Subtask{{
			Index: 0, State: "RUNNING", Metrics: model.snapshot.Nodes[0].Metrics,
		}},
	}
	detailState.SelectedSubtask = 0
	model.jobDetails.RestoreState(detailState)
	model = updateWithKey(model, tea.Key{Code: 'v', Text: "v"})
	if !model.jobDetails.State().SubtaskTotals {
		t.Fatal("v did not switch the subtask table to cumulative counters")
	}
	rendered := model.renderSubtasks(100, 20)
	for _, expected := range []string{"RECORDS IN", "BYTES OUT", "12.3k", "512KiB"} {
		if !strings.Contains(rendered, expected) {
			t.Fatalf("subtask totals missing %q:\n%s", expected, rendered)
		}
	}
}

func TestCustomMetricExplorerTracksFiltersAndChartsAnyMetric(t *testing.T) {
	model := parityTestModel(t)
	model.mode = modeMetricExplorer
	metricState := model.metrics.State()
	metricState.Vertex = "source"
	metricState.Names = []string{"mailboxLatencyMs_p99", "numRecordsInPerSecond", "user.custom.gauge"}
	metricState.Tracked = []string{"numRecordsInPerSecond"}
	metricState.Values = map[string]float64{"numRecordsInPerSecond": 42.5}
	metricState.Aggregation = flink.MetricSum
	metricState.Scope = -1
	metricState.Window = 5 * time.Minute
	model.metrics.RestoreState(metricState)
	rendered := model.renderMetricExplorer(110, 22)
	for _, expected := range []string{"CUSTOM METRICS", "mailboxLatencyMs_p99", "numRecordsInPerSecond", "42.50", "CHARTS"} {
		if !strings.Contains(rendered, expected) {
			t.Fatalf("metric explorer missing %q:\n%s", expected, rendered)
		}
	}

	metricState = model.metrics.State()
	metricState.Selection = 2
	model.metrics.RestoreState(metricState)
	updated, command := model.Update(tea.KeyPressMsg(tea.Key{Code: tea.KeyEnter}))
	model = updated.(Model)
	metricState = model.metrics.State()
	if command == nil || !stringsSliceContains(metricState.Tracked, "user.custom.gauge") {
		t.Fatalf("enter did not track custom metric: %#v, command=%v", metricState.Tracked, command)
	}
	metricState.SearchOpen = true
	model.metrics.RestoreState(metricState)
	updated, _ = model.Update(tea.KeyPressMsg(tea.Key{Code: 'q', Text: "q"}))
	model = updated.(Model)
	if query := model.metrics.State().Query; model.mode != modeMetricExplorer || query != "q" {
		t.Fatalf("search input was treated as a global command: mode=%d query=%q", model.mode, query)
	}
}

func TestAccumulatorDrilldownOpensCompleteValue(t *testing.T) {
	model := parityTestModel(t)
	model.mode = modeAccumulators
	accumulatorState := model.accumulators.State()
	accumulatorState.Vertex = "source"
	accumulatorState.Values = flink.VertexAccumulators{
		JobID: "job", VertexID: "source", UpdatedAt: time.Now(),
		Vertex: []flink.UserAccumulator{{Name: "rows", Type: "LongCounter", Value: "123"}},
		Subtasks: []flink.SubtaskAccumulators{{
			Subtask: 0, Accumulators: []flink.UserAccumulator{{Name: "payload", Type: "String", Value: "line one\nline two"}},
		}},
	}
	accumulatorState.Selection = 1
	model.accumulators.RestoreState(accumulatorState)
	rendered := model.renderAccumulators(90, 20)
	if !strings.Contains(rendered, "#0") || !strings.Contains(rendered, "payload") {
		t.Fatalf("accumulator rows missing:\n%s", rendered)
	}
	model = updateWithKey(model, tea.Key{Code: tea.KeyEnter})
	processState := model.processDiagnostics.State()
	if model.mode != modeDocument || processState.DocumentBackToken != int(modeAccumulators) || len(processState.DocumentLines) != 2 {
		t.Fatalf("accumulator document = %#v", processState)
	}
}

func TestTaskManagerLogsContentStdoutAndThreadDumpNavigation(t *testing.T) {
	model := parityTestModel(t)
	model.mode = modeTaskManagerDetail
	updated, command := model.Update(tea.KeyPressMsg(tea.Key{Code: 'l', Text: "l"}))
	model = updated.(Model)
	processState := model.processDiagnostics.State()
	if model.mode != modeDocument || processState.DocumentSource != processmodule.SourceCurrentLog ||
		processState.DocumentProcess.Kind != flink.ProcessTaskManager || !processState.DocumentTailOnLoad || command == nil {
		t.Fatalf("TaskManager logs open = mode %d process %#v command=%v", model.mode, processState.Process, command)
	}
	processState.DocumentLines = []string{"first", "second"}
	processState.DocumentLoading = false
	processState.DocumentTailOnLoad = false
	model.processDiagnostics.RestoreState(processState)
	if len(model.processDiagnostics.State().DocumentLines) != 2 || model.processDiagnostics.State().DocumentLines[1] != "second" {
		t.Fatalf("document lines = %#v", model.processDiagnostics.State().DocumentLines)
	}
	model = updateWithKey(model, tea.Key{Code: tea.KeyEscape})
	if model.mode != modeTaskManagerDetail {
		t.Fatalf("log document returned to mode %d", model.mode)
	}
	updated, command = model.Update(tea.KeyPressMsg(tea.Key{Code: 'd', Text: "d"}))
	model = updated.(Model)
	if model.mode != modeThreadDump || command == nil {
		t.Fatalf("thread dump open = mode %d command=%v", model.mode, command)
	}
	processState = model.processDiagnostics.State()
	processState.Threads = []flink.ThreadInfo{{Name: "main", Stack: "\"main\" Id=1 RUNNABLE\n\tat Main.run"}}
	processState.ThreadBusy = false
	model.processDiagnostics.RestoreState(processState)
	rendered := model.renderThreadDump(100, 20)
	if !strings.Contains(rendered, "main") || !strings.Contains(rendered, "RUNNABLE") || !strings.Contains(rendered, "Main.run") {
		t.Fatalf("thread dump missing detail:\n%s", rendered)
	}
	model = updateWithKey(model, tea.Key{Code: tea.KeyEnter})
	processState = model.processDiagnostics.State()
	if model.mode != modeDocument || processState.DocumentBackToken >= 0 {
		t.Fatalf("full thread stack document = %#v", processState)
	}
	model = updateWithKey(model, tea.Key{Code: tea.KeyEscape})
	if model.mode != modeThreadDump {
		t.Fatalf("thread document returned to mode %d", model.mode)
	}
}

func stringsSliceContains(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}

func TestParityViewsRenderWithinResponsiveShell(t *testing.T) {
	model := parityTestModel(t)
	tests := []struct {
		mode screenMode
		want string
	}{
		{mode: modeDiagnostics, want: "DIAGNOSTICS"},
		{mode: modeTimeline, want: "TIMELINE"},
		{mode: modeExceptions, want: "EXCEPTION INCIDENTS"},
		{mode: modeJobConfig, want: "JOB CONFIGURATION"},
		{mode: modeActions, want: "JOB ACTIONS"},
		{mode: modeTaskManagers, want: "TASK MANAGERS"},
		{mode: modeTaskManagerDetail, want: "TASK MANAGER DETAIL"},
		{mode: modeJobManager, want: "JOB MANAGER"},
		{mode: modeMetricExplorer, want: "CUSTOM METRICS"},
		{mode: modeAccumulators, want: "ACCUMULATORS"},
		{mode: modeProcessLogs, want: "LOGS"},
		{mode: modeDocument, want: "lines"},
		{mode: modeThreadDump, want: "THREAD DUMP"},
		{mode: modeProfiler, want: "PROFILER"},
	}
	for _, width := range []int{60, 80, 140} {
		for _, test := range tests {
			model.width = width
			model.height = 24
			model.mode = test.mode
			rendered := model.render()
			if !strings.Contains(rendered, test.want) {
				t.Fatalf("mode %d width %d missing %q", test.mode, width, test.want)
			}
			lines := strings.Split(rendered, "\n")
			if len(lines) != model.height {
				t.Fatalf("mode %d width %d rendered %d lines", test.mode, width, len(lines))
			}
			for index, line := range lines {
				if got := lipgloss.Width(line); got > width {
					t.Fatalf("mode %d width %d line %d is %d cells", test.mode, width, index, got)
				}
			}
		}
	}
}

func TestDiagnosticPagesPreserveVertexAndOpenSubtasks(t *testing.T) {
	model := parityTestModel(t)
	model.openDiagnosticOverview(diagnosticBackpressure)
	model = updateWithKey(model, tea.Key{Code: ']', Text: "]"})
	if page := model.jobDetails.State().Page; page != diagnosticSkew || model.mode != modeDiagnostics {
		t.Fatalf("diagnostic page = %d mode = %d", page, model.mode)
	}
	model = updateWithKey(model, tea.Key{Code: tea.KeyUp})
	if model.selected != "risk" {
		t.Fatalf("selected = %q, want risk", model.selected)
	}
	updated, command := model.Update(tea.KeyPressMsg(tea.Key{Code: tea.KeyEnter}))
	opened := updated.(Model)
	detailState := opened.jobDetails.State()
	if opened.mode != modeSubtasks || detailState.DiagnosticsOpen != "risk" || command == nil {
		t.Fatalf("subtasks open produced mode=%d vertex=%q command=%v", opened.mode, detailState.DiagnosticsOpen, command)
	}
}

func TestJobConfigurationRedactsSensitiveValues(t *testing.T) {
	model := parityTestModel(t)
	model.mode = modeJobConfig
	rendered := model.renderJobConfiguration(100, 20)
	if strings.Contains(rendered, "very-secret") || !strings.Contains(rendered, "redacted") {
		t.Fatalf("sensitive value was not redacted:\n%s", rendered)
	}
}

func TestEveryJobActionRequiresConfirmation(t *testing.T) {
	model := parityTestModel(t)
	model.openActions()
	updated, command := model.Update(tea.KeyPressMsg(tea.Key{Code: tea.KeyEnter}))
	confirming := updated.(Model)
	confirmingState := confirming.jobOperations.State()
	if command != nil || !confirmingState.ActionConfirm || confirmingState.ActionBusy {
		t.Fatalf("enter produced confirm=%t busy=%t command=%v", confirmingState.ActionConfirm, confirmingState.ActionBusy, command)
	}
	updated, command = confirming.Update(tea.KeyPressMsg(tea.Key{Code: 'y', Text: "y"}))
	executing := updated.(Model)
	executingState := executing.jobOperations.State()
	if command == nil || executingState.ActionConfirm || !executingState.ActionBusy {
		t.Fatalf("y produced confirm=%t busy=%t command=%v", executingState.ActionConfirm, executingState.ActionBusy, command)
	}
}

func TestJobManagerTogglesBetweenConfigurationAndLogs(t *testing.T) {
	model := parityTestModel(t)
	model.mode = modeJobManager
	model = updateWithKey(model, tea.Key{Code: 'L', Text: "L"})
	if !model.infrastructure.State().JobManagerLogsOpen {
		t.Fatal("L did not open JobManager logs")
	}
	rendered := model.renderJobManager(100, 20)
	if !strings.Contains(rendered, "JOBMANAGER LOG FILE") || !strings.Contains(rendered, "jobmanager.log") {
		t.Fatalf("log view missing metadata:\n%s", rendered)
	}
	model = updateWithKey(model, tea.Key{Code: 'L', Text: "L"})
	if model.infrastructure.State().JobManagerLogsOpen {
		t.Fatal("second L did not return to configuration")
	}
}

func TestTaskManagerCanBeExploredByKeyboardAndMouse(t *testing.T) {
	model := parityTestModel(t)
	model.mode = modeTaskManagers
	model.width = 80
	model.height = 24

	model = updateWithKey(model, tea.Key{Code: tea.KeyEnter})
	if model.mode != modeTaskManagerDetail {
		t.Fatalf("enter opened mode %d, want TaskManager detail", model.mode)
	}
	for _, expected := range []string{"TASK MANAGER DETAIL", "IDENTITY AND CAPACITY", "MEMORY"} {
		if rendered := model.render(); !strings.Contains(rendered, expected) {
			t.Fatalf("TaskManager detail missing %q:\n%s", expected, rendered)
		}
	}
	updated, _ := model.Update(tea.MouseWheelMsg{X: 40, Y: headerHeight + 10, Button: tea.MouseWheelDown})
	model = updated.(Model)
	if model.infrastructure.State().DetailOffset == 0 {
		t.Fatal("mouse wheel did not scroll TaskManager detail")
	}
	model = updateWithKey(model, tea.Key{Code: tea.KeyEnd})
	if model.infrastructure.State().DetailOffset == 0 || !strings.Contains(model.render(), "ALLOCATIONS") {
		t.Fatalf("TaskManager detail did not scroll to allocations:\n%s", model.render())
	}
	model = updateWithKey(model, tea.Key{Code: tea.KeyEscape})
	if model.mode != modeTaskManagers {
		t.Fatalf("escape returned to mode %d, want TaskManagers", model.mode)
	}

	updated, _ = model.Update(tea.MouseClickMsg{
		X: 2, Y: headerHeight + taskManagerBodyRowStart, Button: tea.MouseLeft,
	})
	if got := updated.(Model).mode; got != modeTaskManagerDetail {
		t.Fatalf("mouse click opened mode %d, want TaskManager detail", got)
	}
}

func parityTestModel(t *testing.T) Model {
	model := interactionTestModel(t)
	now := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	model.snapshot.JobState = "RUNNING"
	model.snapshot.JobType = "STREAMING"
	model.snapshot.Scheduler = "Default"
	model.snapshot.StartedAt = now.Add(-time.Hour)
	model.snapshot.Duration = time.Hour
	model.snapshot.UpdatedAt = now
	model.snapshot.Transitions = []flink.StateTransition{{State: "CREATED", At: now.Add(-time.Hour)}, {State: "RUNNING", At: now.Add(-time.Hour + time.Second)}}
	for index := range model.snapshot.Nodes {
		model.snapshot.Nodes[index].Parallelism = 2
		model.snapshot.Nodes[index].MaxParallelism = 128
		model.snapshot.Nodes[index].StartedAt = now.Add(-time.Hour + time.Duration(index)*time.Second)
		model.snapshot.Nodes[index].Duration = time.Hour - time.Duration(index)*time.Second
		model.snapshot.Nodes[index].Metrics = flink.Metrics{
			RecordsInPerSecond: float64(100 + index*10), RecordsOutPerSecond: float64(90 + index*10),
			BusyPercent: float64(20 + index*20), BackpressurePercent: float64(index * 30),
			IdlePercent: float64(80 - index*20), DataSkewPercent: float64(index * 12),
		}
	}
	model.snapshot.Exceptions = flink.ExceptionSummary{
		Count: 1,
		Entries: []flink.JobException{{
			Name: "java.lang.IllegalStateException", Stacktrace: "java.lang.IllegalStateException: bad state\n  at Job.run(Job.java:12)", At: now,
		}},
	}
	operations := model.jobOperations.State()
	operations.Configuration = flink.JobConfiguration{
		RestartStrategy: "fixed-delay", Parallelism: 2, UpdatedAt: now,
		User: []flink.ConfigurationEntry{{Key: "api.password", Value: "very-secret"}, {Key: "pipeline.name", Value: "Test"}},
	}
	model.jobOperations.RestoreState(operations)
	model.infrastructure.Restore(flink.Infrastructure{
		UpdatedAt: now,
		TaskManagers: []flink.TaskManager{{
			ID: "tm:1", Path: "127.0.0.1:6122", Slots: 4, FreeSlots: 1, AssignedTasks: 3, AssignedTasksKnown: true, CPUCores: 8,
			HeapUsed: 3000, HeapCommitted: 3500, HeapMax: 4000, ShuffleUsed: 500, ShuffleTotal: 1000,
			Allocations: []flink.TaskManagerAllocation{{JobID: "job", AssignedTasks: 3, AssignedTasksKnown: true}},
		}},
		JobManager: flink.JobManager{
			JVMVersion: "OpenJDK 17", Architecture: "aarch64", CPUPercent: 12.5,
			HeapUsed: 1000, HeapMax: 4000, Threads: 42, TaskManagers: 1, RunningJobs: 1,
			SlotsAvailable: 1, SlotsTotal: 4,
			Configuration: []flink.ConfigurationEntry{{Key: "parallelism.default", Value: "2"}},
			Logs:          []flink.LogFile{{Name: "jobmanager.log", Size: 4096, ModifiedAt: now}},
		},
	})
	model.recordHistory(model.snapshot)
	return model
}
