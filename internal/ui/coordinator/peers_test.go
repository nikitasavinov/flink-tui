package coordinator

import (
	"strings"
	"testing"

	tea "charm.land/bubbletea/v2"

	"github.com/nikitasavinov/flink-tui/internal/flink"
	checkpointmodule "github.com/nikitasavinov/flink-tui/internal/ui/checkpoints"
	inframodule "github.com/nikitasavinov/flink-tui/internal/ui/infrastructure"
	"github.com/nikitasavinov/flink-tui/internal/ui/shared"
)

func TestSubtasksPeerStepPreservesSortAndHistory(t *testing.T) {
	model := interactionTestModel(t)
	model.configureJobDetails()
	model.mode = modeSubtasks
	model.selected = "source"
	state := model.jobDetails.State()
	state.SubtaskSort = 2
	state.SubtaskSortDescending = true
	model.jobDetails.RestoreState(state)
	model.history.Push(routeCrumb{mode: modeDiagnostics, selected: "source"})
	historyLength := model.history.Len()

	updated, command := model.Update(tea.KeyPressMsg(tea.Key{Code: '>', Text: ">"}))
	model = updated.(Model)
	state = model.jobDetails.State()
	if command == nil || model.mode != modeSubtasks || model.selected != "risk" {
		t.Fatalf("peer step = mode %d selected %q command nil %t", model.mode, model.selected, command == nil)
	}
	if state.SubtaskSort != 2 || !state.SubtaskSortDescending {
		t.Fatalf("sort changed to column=%d descending=%t", state.SubtaskSort, state.SubtaskSortDescending)
	}
	if model.history.Len() != historyLength {
		t.Fatalf("peer step grew history from %d to %d", historyLength, model.history.Len())
	}
	if notice := model.activeNotice(); !strings.Contains(notice, "Source → Risk") || !strings.Contains(notice, "(2/3)") {
		t.Fatalf("peer notice = %q", notice)
	}
}

func TestPeerNoticeReportsPositionWrapAndFits(t *testing.T) {
	model := interactionTestModel(t)
	model.width = 80
	model.setPeerNotice(
		peerDescriptor{name: strings.Repeat("previous-vertex-", 5), position: 30, total: 30},
		peerDescriptor{name: strings.Repeat("next-vertex-", 6), position: 1, total: 30},
		1,
	)
	notice := model.activeNotice()
	if !strings.Contains(notice, "(1/30, wrapped)") {
		t.Fatalf("wrap notice = %q", notice)
	}
	if shared.DisplayWidth(" "+notice) > 80 {
		t.Fatalf("notice width = %d: %q", shared.DisplayWidth(" "+notice), notice)
	}
}

func TestTaskManagerPeerNoticeUsesTableIdentity(t *testing.T) {
	model := interactionTestModel(t)
	model.configureInfrastructure()
	state := model.infrastructure.State()
	state.View = 1
	state.Infrastructure.TaskManagers = []flink.TaskManager{
		{ID: "172.22.0.3:42213-408ff3", Path: "pekko.tcp://flink@172.22.0.3:42213/user/rpc/taskmanager_0"},
		{ID: "172.22.0.4:35507-ded791", Path: "pekko.tcp://flink@172.22.0.4:35507/user/rpc/taskmanager_0"},
	}
	model.infrastructure.RestoreState(state)
	model.mode = modeTaskManagerDetail

	_ = model.stepTaskManagerPeer(1)
	notice := model.activeNotice()
	if !strings.Contains(notice, "172.22.0.3:42213-408ff3 → 172.22.0.4:35507-ded791") ||
		!strings.Contains(notice, "(2/2)") || strings.Contains(notice, "pekko.tcp") {
		t.Fatalf("TaskManager peer notice = %q", notice)
	}
}

func TestProcessPeerStepPreservesDocumentQueryAndHistory(t *testing.T) {
	model := interactionTestModel(t)
	model.configureInfrastructure()
	infra := model.infrastructure.State()
	infra.Infrastructure.TaskManagers = []flink.TaskManager{
		{ID: "tm-1", Path: "pekko.tcp://flink@tm-1/user/rpc/taskmanager_0"},
		{ID: "tm-2", Path: "pekko.tcp://flink@tm-2/user/rpc/taskmanager_0"},
	}
	model.infrastructure.RestoreState(infra)
	model.configureProcessDiagnostics()
	_ = model.openCurrentProcessLog(flink.TaskManagerProcess("tm-1"), modeTaskManagerDetail)
	processState := model.processDiagnostics.State()
	processState.DocumentQuery = "checkpoint timeout"
	processState.DocumentSearch = false
	processState.DocumentOffsetY = 30
	model.processDiagnostics.RestoreState(processState)
	model.mode = modeDocument
	model.history.Push(routeCrumb{mode: modeTaskManagerDetail})
	wantHistory := model.history.Len()

	updated, command := model.Update(tea.KeyPressMsg(tea.Key{Code: '>', Text: ">"}))
	model = updated.(Model)
	state := model.processDiagnostics.State()
	if command == nil || model.mode != modeDocument || state.DocumentProcess != flink.TaskManagerProcess("tm-2") ||
		state.DocumentQuery != "checkpoint timeout" || state.DocumentSearch || state.DocumentOffsetY != 0 {
		t.Fatalf("document process peer = command nil:%t mode:%d state:%#v", command == nil, model.mode, state)
	}
	if model.history.Len() != wantHistory {
		t.Fatalf("process peer grew history to %d, want %d", model.history.Len(), wantHistory)
	}
	if notice := model.activeNotice(); !strings.Contains(notice, "tm-1 → tm-2") || !strings.Contains(notice, "(2/3)") {
		t.Fatalf("process notice = %q", notice)
	}

	updated, _ = model.Update(tea.KeyPressMsg(tea.Key{Code: '>', Text: ">"}))
	model = updated.(Model)
	if process := model.processDiagnostics.State().DocumentProcess; process.Kind != flink.ProcessJobManager {
		t.Fatalf("last TaskManager did not step to JobManager: %#v", process)
	}
	if notice := model.activeNotice(); !strings.Contains(notice, "tm-2 → JobManager") || !strings.Contains(notice, "(3/3)") {
		t.Fatalf("JobManager process notice = %q", notice)
	}
}

func TestProcessPeerStepPreservesThreadFilter(t *testing.T) {
	model := interactionTestModel(t)
	model.configureInfrastructure()
	infra := model.infrastructure.State()
	infra.Infrastructure.TaskManagers = []flink.TaskManager{{ID: "tm-1"}, {ID: "tm-2"}}
	model.infrastructure.RestoreState(infra)
	model.configureProcessDiagnostics()
	_ = model.openThreadDump(flink.TaskManagerProcess("tm-1"), modeTaskManagerDetail)
	state := model.processDiagnostics.State()
	state.ThreadQuery = "RUNNABLE checkpoint"
	state.ThreadSearch = false
	state.ThreadCursor = 9
	state.ThreadStackOffset = 12
	model.processDiagnostics.RestoreState(state)
	model.mode = modeThreadDump
	historyLength := model.history.Len()

	updated, command := model.Update(tea.KeyPressMsg(tea.Key{Code: '>', Text: ">"}))
	model = updated.(Model)
	state = model.processDiagnostics.State()
	if command == nil || state.Process != flink.TaskManagerProcess("tm-2") ||
		state.ThreadQuery != "RUNNABLE checkpoint" || state.ThreadCursor != 0 || state.ThreadStackOffset != 0 ||
		model.history.Len() != historyLength {
		t.Fatalf("thread process peer = command nil:%t state:%#v history:%d/%d",
			command == nil, state, model.history.Len(), historyLength)
	}
}

func TestSingleTaskManagerDetailExplainsWorkerPeerIsUnavailable(t *testing.T) {
	model := interactionTestModel(t)
	model.configureInfrastructure()
	infra := model.infrastructure.State()
	infra.Infrastructure.TaskManagers = []flink.TaskManager{{ID: "only-tm"}}
	infra.View = inframodule.ViewTaskManagerDetail
	model.infrastructure.RestoreState(infra)
	model.mode = modeTaskManagerDetail

	if command := model.stepPeer(1); command != nil || !strings.Contains(model.activeNotice(), "No other TaskManager") {
		t.Fatalf("single worker peer = command:%v notice:%q", command, model.activeNotice())
	}
}

func TestStaticDocumentRefusesProcessPeerStep(t *testing.T) {
	model := interactionTestModel(t)
	model.configureProcessDiagnostics()
	model.openStaticDocument("Accumulator", "value", modeAccumulators)
	updated, command := model.Update(tea.KeyPressMsg(tea.Key{Code: '>', Text: ">"}))
	model = updated.(Model)
	if command != nil || !strings.Contains(model.activeNotice(), "not backed by a Flink process") {
		t.Fatalf("static document peer = command:%v notice:%q", command, model.activeNotice())
	}
}

func TestProfilerReportPeerOpensPeerProfilerWithoutTransferringReport(t *testing.T) {
	model := interactionTestModel(t)
	model.configureInfrastructure()
	infra := model.infrastructure.State()
	infra.Infrastructure.TaskManagers = []flink.TaskManager{{ID: "tm-1"}, {ID: "tm-2"}}
	model.infrastructure.RestoreState(infra)
	model.configureProcessDiagnostics()
	model.configureProfiler()
	model.configureFlameGraphs()
	first := flink.TaskManagerProcess("tm-1")
	_ = model.profiler.Open(first, int(modeTaskManagerDetail))
	model.flameGraphs.LoadProfilerReport(first, "profile.html", flink.FlameGraph{
		EndTimestampMillis: 1, Root: flink.FlameGraphNode{Name: "root", Value: 1},
	})
	model.mode = modeProfilerFlameGraph
	model.history.Push(routeCrumb{mode: modeProfiler})

	updated, command := model.Update(tea.KeyPressMsg(tea.Key{Code: '>', Text: ">"}))
	model = updated.(Model)
	state := model.flameGraphs.State()
	if command == nil || model.mode != modeProfiler || model.profiler.Process() != flink.TaskManagerProcess("tm-2") ||
		state.ReportName != "" || state.ReportGraph.Ready() || model.history.Len() != 0 {
		t.Fatalf("report peer = command nil:%t mode:%d process:%#v report:%q ready:%t history:%d",
			command == nil, model.mode, model.profiler.Process(), state.ReportName, state.ReportGraph.Ready(), model.history.Len())
	}
	if notice := model.activeNotice(); !strings.Contains(notice, "report not transferred; opened profiler") {
		t.Fatalf("report peer notice = %q", notice)
	}
}

func TestCheckpointPeerStepKeepsScreenAfterJobLoads(t *testing.T) {
	model := interactionTestModel(t)
	nodes := append([]flink.Node(nil), model.snapshot.Nodes...)
	model.configureJobList()
	state := model.jobList.State()
	state.Jobs = []flink.JobSummary{
		{ID: "job", Name: "Orders", State: "RUNNING"},
		{ID: "other", Name: "Payments", State: "RUNNING"},
	}
	model.jobList.RestoreState(state)
	model.mode = modeCheckpoints
	model.history.Push(routeCrumb{mode: modeGraph, selected: "source"})
	historyLength := model.history.Len()

	updated, command := model.Update(tea.KeyPressMsg(tea.Key{Code: '>', Text: ">"}))
	model = updated.(Model)
	if command == nil || model.mode != modeGraph || model.preferredJobID != "other" {
		t.Fatalf("job step first half = mode %d preferred %q command nil %t", model.mode, model.preferredJobID, command == nil)
	}
	_ = model.applySnapshot(snapshotMsg{
		snapshot:   flink.Snapshot{JobID: "other", JobName: "Payments", JobState: "RUNNING", Nodes: nodes},
		generation: model.generation,
	})
	if model.mode != modeCheckpoints || model.history.Len() != historyLength {
		t.Fatalf("job step completion = mode %d history %d, want Checkpoints/%d", model.mode, model.history.Len(), historyLength)
	}
	if notice := model.activeNotice(); !strings.Contains(notice, "Orders → Payments") {
		t.Fatalf("peer notice = %q", notice)
	}
}

func TestCheckpointDetailPeerStepDoesNotGrowRouteHistory(t *testing.T) {
	model := interactionTestModel(t)
	model.mode = modeCheckpointOperators
	model.snapshot.Checkpoints.History = []flink.Checkpoint{{ID: 42}, {ID: 41}, {ID: 40}}
	model.configureCheckpoints()
	state := model.checkpoints.State()
	state.View = checkpointmodule.ViewOperators
	state.DetailID = 42
	state.SelectedID = 42
	model.checkpoints.RestoreState(state)
	model.history.Push(routeCrumb{mode: modeCheckpoints})
	wantHistory := model.history.Len()

	updated, command := model.Update(tea.KeyPressMsg(tea.Key{Code: '>', Text: ">"}))
	model = updated.(Model)
	if command == nil || model.mode != modeCheckpointOperators || model.checkpoints.State().DetailID != 41 {
		t.Fatalf("checkpoint detail peer = command nil:%t mode:%d detail:%d",
			command == nil, model.mode, model.checkpoints.State().DetailID)
	}
	if model.history.Len() != wantHistory {
		t.Fatalf("checkpoint peer grew history to %d, want %d", model.history.Len(), wantHistory)
	}
	if notice := model.activeNotice(); !strings.Contains(notice, "#42 → #41") || !strings.Contains(notice, "(2/3)") {
		t.Fatalf("checkpoint peer notice = %q", notice)
	}
}

func TestPeerStepOnOverviewExplainsItIsUnavailable(t *testing.T) {
	model := interactionTestModel(t)
	model.mode = modeJobs
	updated, command := model.Update(tea.KeyPressMsg(tea.Key{Code: '>', Text: ">"}))
	model = updated.(Model)
	if command != nil || !strings.Contains(model.activeNotice(), "unavailable on Overview") {
		t.Fatalf("overview peer = command %v notice %q", command, model.activeNotice())
	}
}

func TestFlameGraphSStillCyclesSubtasks(t *testing.T) {
	model := interactionTestModel(t)
	model.snapshot.Nodes[0].Parallelism = 2
	model.configureFlameGraphs()
	model.selected = "source"
	_ = model.openFlameGraph()

	if command := model.handleFlameGraphKey("s"); command == nil || model.flameGraphs.State().Subtask != 0 {
		t.Fatalf("s produced command nil %t subtask %d", command == nil, model.flameGraphs.State().Subtask)
	}
	if command := model.handleFlameGraphKey("S"); command == nil || model.flameGraphs.State().Subtask != -1 {
		t.Fatalf("S produced command nil %t subtask %d", command == nil, model.flameGraphs.State().Subtask)
	}
}
