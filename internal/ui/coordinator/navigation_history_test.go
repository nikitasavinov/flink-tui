package coordinator

import (
	"strings"
	"testing"

	tea "charm.land/bubbletea/v2"

	"github.com/nikitasavinov/flink-tui/internal/flink"
	exceptionmodule "github.com/nikitasavinov/flink-tui/internal/ui/exceptions"
	inframodule "github.com/nikitasavinov/flink-tui/internal/ui/infrastructure"
	processmodule "github.com/nikitasavinov/flink-tui/internal/ui/processdiag"
)

func TestSubtaskDrillReturnsToCallingRankedList(t *testing.T) {
	tests := []struct {
		name string
		open func(*Model)
		key  func(*Model) tea.Cmd
		want screenMode
	}{
		{
			name: "diagnostics",
			open: func(model *Model) { model.openDiagnosticOverview(diagnosticBackpressure) },
			key:  func(model *Model) tea.Cmd { return model.handleDiagnosticOverviewKey("enter") },
			want: modeDiagnostics,
		},
		{
			name: "timeline",
			open: func(model *Model) { model.openTimeline() },
			key:  func(model *Model) tea.Cmd { return model.handleTimelineKey("enter") },
			want: modeTimeline,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			model := interactionTestModel(t)
			model.configureJobDetails()
			model.selected = "risk"
			test.open(&model)
			if command := test.key(&model); command == nil || model.mode != modeSubtasks || model.history.Len() != 1 {
				t.Fatalf("drill = mode %d history %d command %v", model.mode, model.history.Len(), command)
			}

			for _, key := range []tea.Key{{Code: 'q', Text: "q"}, {Code: tea.KeyEscape}} {
				candidate := model
				updated, _ := candidate.Update(tea.KeyPressMsg(key))
				candidate = updated.(Model)
				if candidate.mode != test.want || candidate.selected != "risk" || candidate.history.Len() != 0 {
					t.Fatalf("back %q = mode %d selected %q history %d, want mode %d and restored row", key.String(), candidate.mode, candidate.selected, candidate.history.Len(), test.want)
				}
			}
		})
	}
}

func TestExceptionTaskManagerDrillReturnsToException(t *testing.T) {
	model := interactionTestModel(t)
	model.configureInfrastructure()
	model.infrastructure.Restore(flink.Infrastructure{TaskManagers: []flink.TaskManager{{ID: "tm-risk"}}})
	model.mode = modeExceptions

	command := model.applyExceptionResult(exceptionmodule.Result{
		Intent: exceptionmodule.IntentTaskManager, TaskManagerID: "tm-risk",
	})
	if model.mode != modeTaskManagerDetail || model.history.Len() != 1 {
		t.Fatalf("task-manager drill = mode %d history %d command %v", model.mode, model.history.Len(), command)
	}

	updated, _ := model.Update(tea.KeyPressMsg(tea.Key{Code: 'q', Text: "q"}))
	model = updated.(Model)
	if model.mode != modeExceptions || model.history.Len() != 0 {
		t.Fatalf("task-manager back = mode %d history %d, want Exceptions", model.mode, model.history.Len())
	}
}

func TestExceptionVertexDrillReturnsToException(t *testing.T) {
	model := interactionTestModel(t)
	model.snapshot.Exceptions = flink.ExceptionSummary{Entries: []flink.JobException{{
		Name: "boom", TaskName: "Risk (1/1)", TaskManagerID: "tm-risk",
	}}}
	model.configureExceptions()
	model.openExceptions()

	_ = model.handleExceptionKey("enter")
	if model.mode != modeGraph || model.selected != "risk" || model.history.Len() != 1 {
		t.Fatalf("vertex drill = mode %d selected %q history %d", model.mode, model.selected, model.history.Len())
	}
	updated, _ := model.Update(tea.KeyPressMsg(tea.Key{Code: 'q', Text: "q"}))
	model = updated.(Model)
	if model.mode != modeExceptions || model.history.Len() != 0 {
		t.Fatalf("vertex back = mode %d history %d, want Exceptions", model.mode, model.history.Len())
	}
}

func TestRoutePopReconcilesInfrastructureBeforeDelayedPoll(t *testing.T) {
	model := interactionTestModel(t)
	model.configureInfrastructure()
	model.configureProcessDiagnostics()
	model.infrastructure.Activate(inframodule.ViewTaskManagerDetail)
	model.processDiagnostics.Activate(processmodule.ViewThreadDump)
	model.history.Push(routeCrumb{mode: modeTaskManagers})
	model.history.Push(routeCrumb{mode: modeTaskManagerDetail})
	model.mode = modeThreadDump

	if !model.popRouteHistory() || model.mode != modeTaskManagerDetail ||
		model.infrastructure.View() != inframodule.ViewTaskManagerDetail {
		t.Fatalf("first pop = mode %d infrastructure view %d", model.mode, model.infrastructure.View())
	}
	if !model.popRouteHistory() || model.mode != modeTaskManagers ||
		model.infrastructure.View() != inframodule.ViewTaskManagers {
		t.Fatalf("second pop = mode %d infrastructure view %d", model.mode, model.infrastructure.View())
	}

	// A poll that was already in flight must not restore the old detail view.
	_ = model.applyInfrastructureResult(inframodule.Result{})
	if model.mode != modeTaskManagers {
		t.Fatalf("delayed infrastructure poll restored mode %d, want Task Managers", model.mode)
	}
}

func TestRoutePopReconcilesProcessDiagnosticsBeforeDelayedPoll(t *testing.T) {
	model := interactionTestModel(t)
	model.configureProcessDiagnostics()
	model.processDiagnostics.Activate(processmodule.ViewThreadDump)
	model.history.Push(routeCrumb{mode: modeProcessLogs})
	model.history.Push(routeCrumb{mode: modeDocument})
	model.mode = modeThreadDump

	if !model.popRouteHistory() || model.mode != modeDocument ||
		model.processDiagnostics.CurrentView() != processmodule.ViewDocument {
		t.Fatalf("first pop = mode %d process view %d", model.mode, model.processDiagnostics.CurrentView())
	}
	if !model.popRouteHistory() || model.mode != modeProcessLogs ||
		model.processDiagnostics.CurrentView() != processmodule.ViewLogs {
		t.Fatalf("second pop = mode %d process view %d", model.mode, model.processDiagnostics.CurrentView())
	}

	_ = model.applyProcessDiagnosticResult(processmodule.Result{})
	if model.mode != modeProcessLogs {
		t.Fatalf("delayed process poll restored mode %d, want Logs", model.mode)
	}
}

func TestExplicitDestinationJumpRecordsItsOrigin(t *testing.T) {
	// A jump is a place the operator came from, not a fresh start. Back must
	// walk an incident trail out however it was walked in.
	model := interactionTestModel(t)
	model.history.Push(routeCrumb{mode: modeDiagnostics, selected: "risk"})
	model.mode = modeSubtasks
	_ = model.navigate(navigationTimeline)
	if model.mode != modeTimeline || model.history.Len() != 2 {
		t.Fatalf("jump = mode %d history %d, want Timeline over a recorded Subtasks origin", model.mode, model.history.Len())
	}
	if !model.popRouteHistory() || model.mode != modeSubtasks {
		t.Fatalf("first back = mode %d, want the jump origin", model.mode)
	}
	if !model.popRouteHistory() || model.mode != modeDiagnostics {
		t.Fatalf("second back = mode %d, want the earlier drill origin", model.mode)
	}
}

func TestOverviewParksAndResumesTheTrail(t *testing.T) {
	// Home remains a one-press exit, while one explicit resume restores the
	// suspended investigation without turning Overview into another crumb.
	model := interactionTestModel(t)
	model.configureJobList()
	model.history.Push(routeCrumb{mode: modeDiagnostics, selected: "risk"})
	model.history.Push(routeCrumb{mode: modeSubtasks, selected: "risk"})
	model.mode = modeTimeline
	model.selected = "risk"

	_ = model.navigate(navigationOverview)
	if model.mode != modeJobs || model.history.Len() != 0 || !model.parked.active ||
		model.parked.leaf.mode != modeTimeline {
		t.Fatalf("home = mode %d history %d parked=%#v", model.mode, model.history.Len(), model.parked)
	}
	footer := model.renderFooter(160)
	if !strings.Contains(footer, "ctrl+o resume Timeline") {
		t.Fatalf("parked footer = %q", footer)
	}
	model.help.Show()
	if help := model.renderShellBody(160, 40); !strings.Contains(help, "ctrl+o") || !strings.Contains(help, "resume Timeline") {
		t.Fatalf("parked help omitted resume:\n%s", help)
	}
	model.help.Close()

	updated, command := model.Update(tea.KeyPressMsg(tea.Key{Code: 'o', Mod: tea.ModCtrl}))
	model = updated.(Model)
	if command == nil || model.mode != modeTimeline || model.selected != "risk" || model.history.Len() != 2 || model.parked.active {
		t.Fatalf("resume = command nil:%t mode:%d selected:%q history:%d parked:%t",
			command == nil, model.mode, model.selected, model.history.Len(), model.parked.active)
	}
	updated, command = model.Update(tea.KeyPressMsg(tea.Key{Code: 'o', Mod: tea.ModCtrl}))
	model = updated.(Model)
	if command != nil || model.mode != modeTimeline {
		t.Fatalf("second resume = command:%v mode:%d", command, model.mode)
	}
}

func TestNewOverviewNavigationDiscardsParkedTrail(t *testing.T) {
	model := interactionTestModel(t)
	model.configureJobList()
	model.mode = modeTimeline
	model.history.Push(routeCrumb{mode: modeGraph, selected: "source"})
	_ = model.navigate(navigationOverview)
	if !model.parked.active {
		t.Fatal("home did not park route")
	}
	_ = model.navigate(navigationGraph)
	if model.mode != modeGraph || model.parked.active {
		t.Fatalf("new navigation = mode:%d parked:%t", model.mode, model.parked.active)
	}
	updated, command := model.Update(tea.KeyPressMsg(tea.Key{Code: 'o', Mod: tea.ModCtrl}))
	model = updated.(Model)
	if command != nil || model.mode != modeGraph {
		t.Fatalf("discarded resume = command:%v mode:%d", command, model.mode)
	}
}

func TestPressingHomeAgainKeepsParkedTrail(t *testing.T) {
	model := interactionTestModel(t)
	model.configureJobList()
	model.mode = modeDiagnostics
	_ = model.navigate(navigationOverview)
	parked := model.parked
	_ = model.navigate(navigationOverview)
	if model.parked != parked {
		t.Fatalf("second Home changed parked route from %#v to %#v", parked, model.parked)
	}
}

func TestBackFooterAndHelpNameRecordedRoute(t *testing.T) {
	model := interactionTestModel(t)
	model.width, model.height = 160, 48

	for _, test := range []struct {
		name string
		from screenMode
		want string
	}{
		{name: "task managers", from: modeTaskManagers, want: "q/esc Task Managers"},
		{name: "job graph", from: modeGraph, want: "q/esc Job Graph"},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidate := model
			candidate.mode = test.from
			_ = candidate.navigate(navigationCheckpoints)
			if candidate.mode != modeCheckpoints {
				t.Fatalf("navigation landed on mode %d", candidate.mode)
			}
			screen := candidate.activeScreen()
			back := candidate.backDestinationLabel(screen.backFallback)
			footer := screen.footer(candidate, "off", back)
			if !strings.Contains(footer, test.want) {
				t.Fatalf("footer %q does not name %q", footer, test.want)
			}

			candidate.help.Show()
			help := candidate.renderShellBody(candidate.width, candidate.bodyHeight())
			if !strings.Contains(help, "THIS SCREEN") || !strings.Contains(help, test.want) {
				t.Fatalf("help disagrees with route footer:\n%s", help)
			}
		})
	}
}

func TestDeferredOverviewDestinationDoesNotRecordPhantomGraph(t *testing.T) {
	model := interactionTestModel(t)
	model.configureJobList()
	state := model.jobList.State()
	state.Jobs = []flink.JobSummary{
		{ID: "job", Name: "Current", State: "RUNNING"},
		{ID: "other", Name: "Other", State: "RUNNING"},
	}
	state.Selection = 1
	model.jobList.RestoreState(state)
	model.mode = modeJobs

	if command := model.navigate(navigationCheckpoints); command == nil || model.mode != modeGraph {
		t.Fatalf("first half = mode %d command nil %t", model.mode, command == nil)
	}
	_ = model.applySnapshot(snapshotMsg{
		snapshot:   flink.Snapshot{JobID: "other", JobName: "Other", JobState: "RUNNING", Nodes: model.snapshot.Nodes},
		generation: model.generation,
	})
	if model.mode != modeCheckpoints || model.history.Len() != 1 {
		t.Fatalf("continuation = mode %d history %d, want Checkpoints over one crumb", model.mode, model.history.Len())
	}
	crumb, _ := model.history.Peek()
	if crumb.mode != modeJobs {
		t.Fatalf("sole crumb = %s, want Overview", screenModeName(crumb.mode))
	}
	_ = model.handleScreenBack()
	if model.mode != modeJobs || model.history.Len() != 0 {
		t.Fatalf("one back = mode %d history %d, want Overview", model.mode, model.history.Len())
	}
}

func TestDeferredPaletteJobSwitchDoesNotRecordPhantomGraph(t *testing.T) {
	model := interactionTestModel(t)
	model.mode = modeCheckpoints
	if command := model.jumpToPaletteJob("other"); command == nil || model.mode != modeGraph {
		t.Fatalf("first half = mode %d command nil %t", model.mode, command == nil)
	}
	_ = model.applySnapshot(snapshotMsg{
		snapshot:   flink.Snapshot{JobID: "other", JobName: "Other", JobState: "RUNNING", Nodes: model.snapshot.Nodes},
		generation: model.generation,
	})
	if model.mode != modeCheckpoints || model.history.Len() != 0 {
		t.Fatalf("palette continuation = mode %d history %d, want Checkpoints without phantom Graph", model.mode, model.history.Len())
	}
}

func TestEveryScreenHasOneTruthfulSidebarIdentity(t *testing.T) {
	model := interactionTestModel(t)
	for mode := screenMode(0); mode < modeCount; mode++ {
		model.mode = mode
		if mode == modeDocument {
			model.history.Push(routeCrumb{mode: modeAccumulators, selected: "source"})
		}
		active := make([]navigationRow, 0, 1)
		for _, row := range model.navigationRows() {
			if row.Active {
				active = append(active, row)
			}
		}
		if len(active) != 1 || active[0].Target != model.activeScreen().navTarget {
			t.Fatalf("mode %d (%s) active rows = %#v", mode, model.activeScreen().name, active)
		}
		model.history.Clear()
	}
}

func TestSidebarAndPaletteShareCanonicalDestinationMetadata(t *testing.T) {
	model := interactionTestModel(t)
	sqlClient, err := flink.NewSQLGatewayClient("http://localhost:8083")
	if err != nil {
		t.Fatal(err)
	}
	model.configureSQLWorkbench(sqlClient)
	commands := make(map[commandID]paletteCommand)
	for _, command := range model.paletteCommands() {
		commands[command.ID] = command
	}
	for _, row := range model.navigationRows() {
		if row.Group {
			continue
		}
		spec := destination(row.Target)
		if spec.command == "" {
			continue
		}
		command, ok := commands[spec.command]
		if !ok || command.Shortcut != row.Alias || command.Label != "Open "+row.Label {
			t.Fatalf("route %v row=%#v command=%#v found=%t", row.Target, row, command, ok)
		}
	}
}

func TestEveryScreenNameMatchesItsNavigationDestination(t *testing.T) {
	for mode := screenMode(0); mode < modeCount; mode++ {
		screen := screenRegistry[mode]
		spec := destination(screen.navTarget)
		if spec.label == "" || screen.name != spec.label {
			t.Fatalf("mode %d screen name %q, destination label %q", mode, screen.name, spec.label)
		}
	}
}

func TestHeaderBreadcrumbKeepsScreenIdentityAtNarrowWidth(t *testing.T) {
	model := interactionTestModel(t)
	model.mode = modeGraph
	if header := model.renderHeader(80); !strings.Contains(header, "Test") || !strings.Contains(header, "Job Graph") {
		t.Fatalf("graph header has no job/screen breadcrumb:\n%s", header)
	}
	model.mode = modeSubtasks
	model.selected = "risk"
	if header := model.renderHeader(80); !strings.Contains(header, "Risk") || !strings.Contains(header, "Subtasks") {
		t.Fatalf("subtask header has no vertex/screen breadcrumb:\n%s", header)
	}
}

func TestSidebarEchoesAmbiguousTypeaheadPrefix(t *testing.T) {
	model := interactionTestModel(t)
	model.width = 80
	setTestNavigation(&model, navigationShown, false)
	model.focusNavigation()
	model = updateWithKey(model, tea.Key{Code: 'c', Text: "c"})
	footer := model.renderFooter(model.width)
	for _, expected := range []string{"c▮", "cp", "cfg"} {
		if !strings.Contains(footer, expected) {
			t.Fatalf("typeahead footer missing %q: %q", expected, footer)
		}
	}
}

func TestWideFooterRetainsHighValueScreenActions(t *testing.T) {
	model := interactionTestModel(t)
	model.width = 200
	model.mode = modeExceptions
	exceptionFooter := model.renderFooter(model.width)
	for _, expected := range []string{"l log tail", "L more", "ctrl+u/d trace"} {
		if !strings.Contains(exceptionFooter, expected) {
			t.Fatalf("Exceptions footer lost %q: %q", expected, exceptionFooter)
		}
	}

	model.configureInfrastructure()
	model.mode = modeTaskManagers
	model.infrastructure.Activate(inframodule.ViewTaskManagers)
	if footer := model.renderFooter(model.width); !strings.Contains(footer, "tab JobManager") {
		t.Fatalf("Task Managers footer lost JobManager jump: %q", footer)
	}
}

func TestPaletteIndexesLiveJobsAndVertices(t *testing.T) {
	model := interactionTestModel(t)
	overview := model.jobList.State()
	overview.Jobs = []flink.JobSummary{{ID: "other-job", Name: "Payments Pipeline", State: "RUNNING", RunningTasks: 4, TotalTasks: 5}}
	model.jobList.RestoreState(overview)

	state := model.palette.State()
	state.Open, state.Query = true, "payments"
	model.palette.RestoreState(state)
	jobs := model.filteredPaletteCommands()
	if len(jobs) != 1 || jobs[0].ID != commandID(commandJobPrefix+"other-job") {
		t.Fatalf("job palette results = %#v", jobs)
	}

	state.Query = "risk"
	model.palette.RestoreState(state)
	vertices := model.filteredPaletteCommands()
	if len(vertices) != 1 || vertices[0].ID != commandID(commandVertexPrefix+"risk") || !strings.Contains(vertices[0].Description, "vertex") {
		t.Fatalf("vertex palette results = %#v", vertices)
	}
}

func TestPaletteJobAndVertexJumpsPreserveTheCurrentWorkspace(t *testing.T) {
	model := interactionTestModel(t)
	model.mode = modeDiagnostics
	if command := model.jumpToPaletteJob("other-job"); command == nil || model.preferredJobID != "other-job" ||
		model.navigation.State().DeferredTarget != navigationDiagnostics {
		t.Fatalf("job jump = job %q target %v command %v", model.preferredJobID, model.navigation.State().DeferredTarget, command)
	}

	model = interactionTestModel(t)
	model.configureJobDetails()
	model.openDiagnosticOverview(diagnosticBackpressure)
	if command := model.jumpToPaletteVertex("risk"); command != nil || model.mode != modeDiagnostics || model.selected != "risk" {
		t.Fatalf("vertex jump = mode %d selected %q command %v", model.mode, model.selected, command)
	}
}

func TestLogsPaletteAliasTargetsTheSelectedIncidentProcess(t *testing.T) {
	model := interactionTestModel(t)
	model.snapshot.Exceptions = flink.ExceptionSummary{Entries: []flink.JobException{{
		Name: "boom", TaskName: "Risk (1/1)", TaskManagerID: "tm-risk",
	}}}
	model.configureExceptions()
	model.configureProcessDiagnostics()
	model.openExceptions()

	state := model.palette.State()
	state.Open, state.Query = true, "logs"
	model.palette.RestoreState(state)
	commands := model.filteredPaletteCommands()
	if len(commands) != 1 || commands[0].ID != commandProcessLogs {
		t.Fatalf("logs alias = %#v", commands)
	}
	command := model.handlePaletteKey("enter")
	process := model.processDiagnostics.State().DocumentProcess
	if command == nil || model.mode != modeDocument || process != flink.TaskManagerProcess("tm-risk") {
		t.Fatalf("logs command = mode %d process %#v command %v", model.mode, process, command)
	}
}

func TestOverviewIsTheBackStackRoot(t *testing.T) {
	// Overview's feature fallback is the job graph. Escape must not use it, or
	// the two screens ping-pong forever with no way to stop at the root.
	for _, key := range []tea.Key{{Code: tea.KeyEscape}, {Code: 'q', Text: "q"}} {
		model := interactionTestModel(t)
		model.configureJobList()
		model.mode = modeJobs

		updated, _ := model.Update(tea.KeyPressMsg(key))
		model = updated.(Model)
		if model.mode != modeJobs {
			t.Fatalf("%q on Overview = mode %d, want the root to stay put", key.String(), model.mode)
		}
		if !strings.Contains(model.notice, "root screen") {
			t.Fatalf("%q on Overview left notice %q, want a root explanation", key.String(), model.notice)
		}
	}
}

func TestPaletteKeysSurviveAFocusedSidebar(t *testing.T) {
	// ":" and ctrl+p are not sidebar aliases. A focused sidebar swallowing them
	// makes the palette unreachable exactly while an operator hunts a destination.
	for _, key := range []tea.Key{{Code: ':', Text: ":"}, {Code: 'p', Mod: tea.ModCtrl}} {
		model := interactionTestModel(t)
		model.width, model.height = 160, 48
		model.focusNavigation()
		if !model.navigation.Focused() {
			t.Fatal("sidebar did not take focus")
		}

		updated, _ := model.Update(tea.KeyPressMsg(key))
		model = updated.(Model)
		if !model.palette.Open() {
			t.Fatalf("%q with the sidebar focused did not open the palette", key.String())
		}
		if model.navigation.Focused() {
			t.Fatalf("%q opened the palette but left the sidebar focused", key.String())
		}
	}
}

func TestJumpBreaksBreadcrumbScopeButDrillKeepsIt(t *testing.T) {
	// Task Managers are cluster-scoped. Reaching them from a job screen now
	// leaves a crumb behind, and that crumb must not lend them the job's name.
	jumped := interactionTestModel(t)
	jumped.mode = modeGraph
	_ = jumped.navigate(navigationTaskManagers)
	if crumb := jumped.breadcrumb("Task Managers"); strings.Contains(crumb, "Test") {
		t.Fatalf("jump to Task Managers = %q, want no job scope", crumb)
	}

	// A drill is the opposite: the caller's scope is exactly what it inherits,
	// which is what keeps an exception's TaskManager tied to its incident.
	drilled := interactionTestModel(t)
	drilled.history.Push(routeCrumb{mode: modeExceptions, selected: "risk"})
	drilled.mode = modeTaskManagers
	if crumb := drilled.breadcrumb("Task Managers"); !strings.Contains(crumb, "Test") {
		t.Fatalf("drill to Task Managers = %q, want the calling job's scope", crumb)
	}
}
