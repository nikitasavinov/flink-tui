package coordinator

import (
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/x/ansi"

	"github.com/nikitasavinov/flink-tui/internal/flink"
	flamegraphmodule "github.com/nikitasavinov/flink-tui/internal/ui/flamegraph"
)

func TestScopeUpWalksContainmentAndAbandonsTheExitedBranch(t *testing.T) {
	model := interactionTestModel(t)
	model.configureJobList()
	model.configureInfrastructure()
	model.configureProcessDiagnostics()
	model.infrastructure.Restore(flink.Infrastructure{
		TaskManagers: []flink.TaskManager{{ID: "tm-risk", Slots: 16}},
		UpdatedAt:    time.Now(),
	})
	model.selected = "risk"
	model.snapshot.Nodes[1].Name = "Risk Score"
	model.history.Push(routeCrumb{mode: modeGraph, selected: "source", jump: true})
	model.history.Push(routeCrumb{mode: modeCheckpoints, selected: "source", jump: true})
	model.history.Push(routeCrumb{mode: modeDiagnostics, selected: "risk"})
	model.history.Push(routeCrumb{mode: modeSubtasks, selected: "risk"})
	_ = model.openThreadDump(flink.TaskManagerProcess("tm-risk"), modeSubtasks)

	model = updateWithKey(model, tea.Key{Code: tea.KeyBackspace})
	manager, ok := model.selectedTaskManager()
	if model.mode != modeTaskManagerDetail || !ok || manager.ID != "tm-risk" {
		t.Fatalf("first scope-up = mode:%s manager:%#v found:%t", screenModeName(model.mode), manager, ok)
	}
	if model.history.Len() != 0 {
		t.Fatalf("scope-up retained %d obsolete route crumbs", model.history.Len())
	}

	model = updateWithKey(model, tea.Key{Code: tea.KeyBackspace})
	if model.mode != modeTaskManagers {
		t.Fatalf("second scope-up = %s, want Task Managers", screenModeName(model.mode))
	}
	model = updateWithKey(model, tea.Key{Code: tea.KeyBackspace})
	if model.mode != modeJobs {
		t.Fatalf("third scope-up = %s, want Overview", screenModeName(model.mode))
	}
	model = updateWithKey(model, tea.Key{Code: tea.KeyBackspace})
	if model.mode != modeJobs || !strings.Contains(model.activeNotice(), "top-level scope") {
		t.Fatalf("scope-up at root = mode:%s notice:%q", screenModeName(model.mode), model.activeNotice())
	}
}

func TestBackStillReplaysTheRouteFromAProcessDrill(t *testing.T) {
	model := interactionTestModel(t)
	model.configureJobDetails()
	model.configureProcessDiagnostics()
	model.selected = "risk"
	model.history.Push(routeCrumb{mode: modeDiagnostics, selected: "risk"})
	model.history.Push(routeCrumb{mode: modeSubtasks, selected: "risk"})
	_ = model.openThreadDump(flink.TaskManagerProcess("tm-risk"), modeSubtasks)

	model = updateWithKey(model, tea.Key{Code: 'q', Text: "q"})
	if model.mode != modeSubtasks || model.selected != "risk" || model.history.Len() != 1 {
		t.Fatalf("q replay = mode:%s selected:%q history:%d", screenModeName(model.mode), model.selected, model.history.Len())
	}
}

func TestFlameGraphKeepsBackspaceForFrameZoom(t *testing.T) {
	model := interactionTestModel(t)
	model.configureFlameGraphs()
	model.mode = modeFlameGraph
	model.selected = "source"
	model.history.Push(routeCrumb{mode: modeGraph, selected: "source"})
	state := model.flameGraphs.State()
	state.View = flamegraphmodule.ViewVertex
	state.Vertex = "source"
	state.LiveGraph = flink.FlameGraph{
		EndTimestampMillis: 1,
		Root:               flink.FlameGraphNode{Name: "root", Value: 10, Children: []flink.FlameGraphNode{{Name: "child", Value: 10}}},
	}
	state.LiveFocus = "0/0"
	state.LiveSelected = "0/0"
	model.flameGraphs.RestoreState(state)

	model = updateWithKey(model, tea.Key{Code: tea.KeyBackspace})
	if model.mode != modeFlameGraph || model.history.Len() != 1 || model.flameGraphs.State().LiveFocus != "0" {
		t.Fatalf("flame backspace = mode:%s history:%d focus:%q", screenModeName(model.mode), model.history.Len(), model.flameGraphs.State().LiveFocus)
	}
	if strings.Contains(ansi.Strip(model.renderFooter(160)), "⌫ up") {
		t.Fatal("flame graph footer advertises scope-up over its zoom binding")
	}
}

func TestNavigationContextFollowsTheActiveScope(t *testing.T) {
	model := interactionTestModel(t)
	model.width, model.height = 160, 48
	model.snapshot.JobState = "RUNNING"
	model.snapshot.Nodes[1].Name = "Risk Score"
	model.selected = "risk"

	model.mode = modeTaskManagers
	clusterSidebar := ansi.Strip(model.renderNavigation(fullNavigationWidth, 43))
	if strings.Contains(clusterSidebar, "JOB  Test") || strings.Contains(clusterSidebar, "vtx ") {
		t.Fatalf("cluster sidebar leaked job context:\n%s", clusterSidebar)
	}

	model.mode = modeCheckpoints
	jobSidebar := ansi.Strip(model.renderNavigation(fullNavigationWidth, 43))
	for _, expected := range []string{"JOB  Test", "vtx Risk Score"} {
		if !strings.Contains(jobSidebar, expected) {
			t.Fatalf("job sidebar missing %q:\n%s", expected, jobSidebar)
		}
	}

	model.configureInfrastructure()
	model.configureProcessDiagnostics()
	model.infrastructure.Restore(flink.Infrastructure{
		TaskManagers: []flink.TaskManager{{ID: "172.22.0.3:43539-ded791", Slots: 16}},
		UpdatedAt:    time.Now(),
	})
	_ = model.openThreadDump(flink.TaskManagerProcess("172.22.0.3:43539-ded791"), modeSubtasks)
	processSidebar := ansi.Strip(model.renderNavigation(fullNavigationWidth, 43))
	for _, expected := range []string{"tm 172.22.0.3", "slots 16"} {
		if !strings.Contains(processSidebar, expected) {
			t.Fatalf("process sidebar missing %q:\n%s", expected, processSidebar)
		}
	}
	if strings.Contains(processSidebar, "JOB  Test") || strings.Contains(processSidebar, "vtx ") {
		t.Fatalf("process sidebar leaked job context:\n%s", processSidebar)
	}

	model.mode = modeTaskManagers
	model = updateWithKey(model, tea.Key{Code: 'g', Text: "g"})
	if model.mode != modeGraph || model.snapshot.JobID != "job" {
		t.Fatalf("g from cluster = mode:%s job:%q", screenModeName(model.mode), model.snapshot.JobID)
	}
}

func TestProcessBreadcrumbKeepsTheObjectInsteadOfThePreviousScreen(t *testing.T) {
	model := interactionTestModel(t)
	model.configureProcessDiagnostics()
	model.snapshot.Nodes[1].Name = "Risk Score"
	model.selected = "risk"
	model.history.Push(routeCrumb{mode: modeSubtasks, selected: "risk"})
	_ = model.openThreadDump(flink.TaskManagerProcess("tm-risk"), modeSubtasks)

	breadcrumb := model.breadcrumb("Thread Dump")
	for _, expected := range []string{"Test", "Risk Score", "Thread Dump"} {
		if !strings.Contains(breadcrumb, expected) {
			t.Fatalf("breadcrumb missing %q: %q", expected, breadcrumb)
		}
	}
	if strings.Contains(breadcrumb, "Subtasks") {
		t.Fatalf("breadcrumb replays a screen instead of objects: %q", breadcrumb)
	}
	if fitted := fitBreadcrumb("An extremely long production job name › Risk Score › Thread Dump", 30); !strings.Contains(fitted, "Risk Score") || !strings.Contains(fitted, "Thread Dump") {
		t.Fatalf("narrow breadcrumb dropped its last object: %q", fitted)
	}
}

func TestProcessDocumentScopeComesFromItsSourceNotItsCallingScreen(t *testing.T) {
	model := interactionTestModel(t)
	model.configureInfrastructure()
	model.configureProcessDiagnostics()
	model.infrastructure.Restore(flink.Infrastructure{
		TaskManagers: []flink.TaskManager{{ID: "tm-risk", Slots: 8}},
		UpdatedAt:    time.Now(),
	})
	_ = model.openCurrentProcessLog(flink.TaskManagerProcess("tm-risk"), modeExceptions)
	if model.mode != modeDocument || !documentUsesInfrastructure(model) || model.activeScope() != scopeProcess {
		t.Fatalf("process document = mode:%s infrastructure:%t scope:%d", screenModeName(model.mode), documentUsesInfrastructure(model), model.activeScope())
	}
	context := model.navigationContext()
	if context.ProcessLabel != "tm tm-risk" || context.JobName != "" || !context.HasProcessSlots || context.ProcessSlots != 8 {
		t.Fatalf("process document context = %#v", context)
	}

	model.openStaticDocument("Accumulator", "value", modeAccumulators)
	if documentUsesInfrastructure(model) || model.activeScope() != scopeVertex {
		t.Fatalf("static document was classified as process-backed: scope=%d", model.activeScope())
	}
}
