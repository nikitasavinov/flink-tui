package coordinator

import (
	"fmt"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/x/ansi"

	"github.com/nikitasavinov/flink-tui/internal/flink"
	joblistmodule "github.com/nikitasavinov/flink-tui/internal/ui/joblist"
)

func TestResponsiveShellKeepsEveryRenderInsideTerminal(t *testing.T) {
	for _, width := range []int{60, 80, 120, 140} {
		model := interactionTestModel(t)
		model.width = width
		model.height = 24

		rendered := model.render()
		lines := strings.Split(rendered, "\n")
		if len(lines) != model.height {
			t.Fatalf("width %d rendered %d lines, want %d", width, len(lines), model.height)
		}
		for index, line := range lines {
			if got := lipgloss.Width(line); got > width {
				t.Fatalf("width %d line %d is %d cells wide", width, index, got)
			}
		}
		if width < wideNavigationThreshold && strings.Contains(rendered, "NAVIGATION") {
			t.Fatalf("width %d should hide automatic navigation", width)
		}
		if width >= wideNavigationThreshold && !strings.Contains(rendered, "NAVIGATION") {
			t.Fatalf("width %d should show automatic navigation", width)
		}
	}
}

func TestQIsBackOrCancelAndOnlyCtrlCQuits(t *testing.T) {
	checkpoint := interactionTestModel(t)
	checkpoint.openCheckpoints()
	checkpoint = updateWithKey(checkpoint, tea.Key{Code: 'q', Text: "q"})
	if checkpoint.mode != modeGraph {
		t.Fatalf("q from checkpoints opened mode %d, want graph", checkpoint.mode)
	}

	confirm := interactionTestModel(t)
	confirm.mode = modeActions
	actionState := confirm.jobOperations.State()
	actionState.ActionConfirm = true
	confirm.jobOperations.RestoreState(actionState)
	confirm = updateWithKey(confirm, tea.Key{Code: 'q', Text: "q"})
	if confirm.jobOperations.State().ActionConfirm || confirm.mode != modeActions {
		t.Fatalf("q from confirmation produced mode=%d confirm=%t", confirm.mode, confirm.jobOperations.State().ActionConfirm)
	}

	root := interactionTestModel(t)
	root.mode = modeJobs
	updated, command := root.Update(tea.KeyPressMsg(tea.Key{Code: 'q', Text: "q"}))
	root = updated.(Model)
	if command != nil || root.mode != modeJobs || !strings.Contains(root.activeNotice(), "Ctrl+C") {
		t.Fatalf("q at root produced command=%v mode=%d notice=%q", command, root.mode, root.activeNotice())
	}
	if _, command = root.Update(tea.KeyPressMsg(tea.Key{Code: 'c', Mod: tea.ModCtrl})); command == nil {
		t.Fatal("Ctrl+C did not return the quit command")
	}
}

func TestQuestionMarkOpensUniversalHelp(t *testing.T) {
	model := interactionTestModel(t)
	model.mode = modeJobs
	model = updateWithKey(model, tea.Key{Code: '?', Text: "?"})
	if !model.help.Open() {
		t.Fatal("? did not open help")
	}
	rendered := model.render()
	for _, expected := range []string{"HELP  Overview", "GLOBAL", "THIS SCREEN", "ctrl+c", "/ filter"} {
		if !strings.Contains(rendered, expected) {
			t.Fatalf("help missing %q:\n%s", expected, rendered)
		}
	}
	model = updateWithKey(model, tea.Key{Code: 'q', Text: "q"})
	if model.help.Open() || model.mode != modeJobs {
		t.Fatalf("q closed help=%t mode=%d", !model.help.Open(), model.mode)
	}
}

func TestActionConfirmationFooterDoesNotAdvertiseDeadNavigation(t *testing.T) {
	model := interactionTestModel(t)
	model.mode = modeActions
	state := model.jobOperations.State()
	state.ActionConfirm = true
	model.jobOperations.RestoreState(state)
	footer := model.renderFooter(160)
	for _, expected := range []string{"CONFIRMATION OWNS INPUT", "y execute", "n/q/esc cancel", "ctrl+c quit"} {
		if !strings.Contains(footer, expected) {
			t.Fatalf("confirmation footer missing %q: %q", expected, footer)
		}
	}
	if strings.Contains(footer, "1-9") || strings.Contains(footer, "ctrl+b") || strings.Contains(footer, "ctrl+n") {
		t.Fatalf("confirmation footer advertises disabled navigation: %q", footer)
	}
}

func TestNavigationStaysReachableDuringJobFilter(t *testing.T) {
	model := interactionTestModel(t)
	model.mode = modeJobs
	model.width = 80
	state := model.jobList.State()
	state.SearchOpen = true
	model.jobList.RestoreState(state)

	model = updateWithKey(model, tea.Key{Code: '4', Text: "4"})
	if model.mode != modeJobs || model.navigation.Focused() {
		t.Fatalf("filter 4 produced mode=%d focused=%t, want the filter to keep the digit", model.mode, model.navigation.Focused())
	}

	model = updateWithKey(model, tea.Key{Code: 'n', Mod: tea.ModCtrl})
	if !model.navigation.Focused() {
		t.Fatal("ctrl+n during job filter should focus navigation")
	}
	model = updateWithKey(model, tea.Key{Code: tea.KeyDown})
	model = updateWithKey(model, tea.Key{Code: tea.KeyEnter})
	if model.mode != modeGraph || model.navigation.Focused() {
		t.Fatalf("filter-to-graph produced mode=%d focused=%t", model.mode, model.navigation.Focused())
	}
}

func TestActionConfirmationStillBlocksNavigationToggle(t *testing.T) {
	model := interactionTestModel(t)
	model.mode = modeActions
	state := model.jobOperations.State()
	state.ActionConfirm = true
	model.jobOperations.RestoreState(state)

	model = updateWithKey(model, tea.Key{Code: 'n', Mod: tea.ModCtrl})
	if model.navigation.Focused() {
		t.Fatal("ctrl+n during confirmation should not focus navigation")
	}
	if !model.jobOperations.State().ActionConfirm {
		t.Fatal("ctrl+n dismissed confirmation")
	}

	model = updateWithKey(model, tea.Key{Code: 'g', Mod: tea.ModCtrl})
	if model.navigationVisibleAt(model.width) {
		t.Fatal("ctrl+g during confirmation changed navigation visibility")
	}
	if !model.jobOperations.State().ActionConfirm {
		t.Fatal("ctrl+g dismissed confirmation")
	}
}

func TestCommonTerminalWidthShowsNavigationAndGraphMap(t *testing.T) {
	model := interactionTestModel(t)
	model.mode = modeGraph
	model.width = 120
	model.height = 30

	rendered := model.render()
	for _, label := range []string{"NAVIGATION", " MAP "} {
		if !strings.Contains(rendered, label) {
			t.Fatalf("120-column graph is missing %q:\n%s", label, rendered)
		}
	}
}

func TestNavigationCanBecomeNarrowFullScreenAndCloseAgain(t *testing.T) {
	model := interactionTestModel(t)
	model.width = 60

	model = updateWithKey(model, tea.Key{Code: 'n', Mod: tea.ModCtrl})
	if !model.navigationOnlyAt(model.width) {
		t.Fatal("ctrl+n should open full-screen navigation at narrow widths")
	}
	if !model.navigation.Focused() {
		t.Fatal("opened navigation should take keyboard focus")
	}
	if rendered := model.render(); !strings.Contains(rendered, "NAVIGATION") {
		t.Fatal("open narrow navigation was not rendered")
	}
	if footer := model.renderFooter(model.width); !strings.Contains(footer, "navigation") ||
		!strings.Contains(footer, "enter use") || !strings.Contains(footer, "right/esc") {
		t.Fatalf("focused navigation footer does not explain content handoff: %q", footer)
	}

	model = updateWithKey(model, tea.Key{Code: 'n', Mod: tea.ModCtrl})
	if model.navigationVisibleAt(model.width) {
		t.Fatal("second ctrl+n should close navigation")
	}
}

func TestCtrlNChangesFocusAndCtrlGChangesVisibility(t *testing.T) {
	model := interactionTestModel(t)
	model.width = 80
	setTestNavigation(&model, navigationShown, false)

	model = updateWithKey(model, tea.Key{Code: 'n', Mod: tea.ModCtrl})
	if !model.navigationVisibleAt(model.width) || !model.navigation.Focused() {
		t.Fatalf("first ctrl+n produced visible=%t focused=%t, want both true",
			model.navigationVisibleAt(model.width), model.navigation.Focused())
	}

	model = updateWithKey(model, tea.Key{Code: 'n', Mod: tea.ModCtrl})
	if !model.navigationVisibleAt(model.width) || model.navigation.Focused() {
		t.Fatalf("second ctrl+n produced visible=%t focused=%t, want visible content focus",
			model.navigationVisibleAt(model.width), model.navigation.Focused())
	}

	model = updateWithKey(model, tea.Key{Code: 'g', Mod: tea.ModCtrl})
	if model.navigationVisibleAt(model.width) || model.navigation.Focused() {
		t.Fatalf("ctrl+g hide produced visible=%t focused=%t",
			model.navigationVisibleAt(model.width), model.navigation.Focused())
	}
	model = updateWithKey(model, tea.Key{Code: 'g', Mod: tea.ModCtrl})
	if !model.navigationVisibleAt(model.width) || model.navigation.Focused() {
		t.Fatalf("ctrl+g show produced visible=%t focused=%t, want visible content focus",
			model.navigationVisibleAt(model.width), model.navigation.Focused())
	}

	setTestNavigation(&model, navigationShown, false)
	model = updateWithKey(model, tea.Key{Code: 'b', Mod: tea.ModCtrl})
	if !model.navigation.Focused() {
		t.Fatal("ctrl+b should remain a navigation alias")
	}
}

func TestLeftFocusesMenuFromOverviewAndEnterNavigates(t *testing.T) {
	model := interactionTestModel(t)
	model.mode = modeJobs
	model.width = 80

	model = updateWithKey(model, tea.Key{Code: tea.KeyLeft})
	if !model.navigation.Focused() || !model.navigationVisibleAt(model.width) {
		t.Fatalf("left produced focused=%t visible=%t", model.navigation.Focused(), model.navigationVisibleAt(model.width))
	}
	rows := model.navigationRows()
	if got := rows[model.navigation.State().Cursor].Target; got != navigationOverview {
		t.Fatalf("focused target = %v, want overview", got)
	}

	model = updateWithKey(model, tea.Key{Code: tea.KeyDown})
	rows = model.navigationRows()
	if got := rows[model.navigation.State().Cursor].Target; got != navigationGraph {
		t.Fatalf("down target = %v, want graph", got)
	}
	model = updateWithKey(model, tea.Key{Code: tea.KeyEnter})
	if model.mode != modeGraph || model.navigation.Focused() || model.navigationVisibleAt(model.width) {
		t.Fatalf("enter produced mode=%d focused=%t visible=%t", model.mode, model.navigation.Focused(), model.navigationVisibleAt(model.width))
	}
	model = updateWithKey(model, tea.Key{Code: tea.KeyTab})
	if model.selected != "risk" || model.navigation.Focused() {
		t.Fatalf("graph tab after nav enter selected=%q focused=%t, want risk", model.selected, model.navigation.Focused())
	}
}

func TestWideNavigationStaysVisibleAfterEnterHandsOff(t *testing.T) {
	model := interactionTestModel(t)
	model.mode = modeJobs
	model.width = 120
	setTestNavigation(&model, navigationAuto, true)

	model = updateWithKey(model, tea.Key{Code: tea.KeyDown})
	model = updateWithKey(model, tea.Key{Code: tea.KeyEnter})
	if model.mode != modeGraph || model.navigation.Focused() || !model.navigationVisibleAt(model.width) {
		t.Fatalf("wide enter produced mode=%d focused=%t visible=%t", model.mode, model.navigation.Focused(), model.navigationVisibleAt(model.width))
	}
}

func TestRemovedDigitsDoNotNavigateFromContentOrSidebar(t *testing.T) {
	for key := '2'; key <= '9'; key++ {
		model := interactionTestModel(t)
		model.width = 80
		model = updateWithKey(model, tea.Key{Code: key, Text: string(key)})
		if model.mode != modeGraph {
			t.Fatalf("content %q produced mode=%d, want graph", key, model.mode)
		}

		setTestNavigation(&model, navigationShown, true)
		model = updateWithKey(model, tea.Key{Code: key, Text: string(key)})
		if model.mode != modeGraph || !model.navigation.Focused() {
			t.Fatalf("sidebar %q produced mode=%d focused=%t", key, model.mode, model.navigation.Focused())
		}
	}

	model := interactionTestModel(t)
	model.mode = modeTaskManagers
	model.history.Push(routeCrumb{mode: modeGraph, selected: "source"})
	model = updateWithKey(model, tea.Key{Code: '1', Text: "1"})
	if model.mode != modeJobs || model.history.Len() != 0 {
		t.Fatalf("1 produced mode=%d history=%d, want Overview root", model.mode, model.history.Len())
	}
}

func TestEnterClosesOnlyFullScreenNavigation(t *testing.T) {
	model := interactionTestModel(t)
	model.mode = modeJobs
	model.width = 60
	model = updateWithKey(model, tea.Key{Code: tea.KeyLeft})
	model = updateWithKey(model, tea.Key{Code: tea.KeyDown})
	model = updateWithKey(model, tea.Key{Code: tea.KeyEnter})
	if model.mode != modeGraph || model.navigation.Focused() || model.navigationVisibleAt(model.width) {
		t.Fatalf("full-screen enter produced mode=%d focused=%t visible=%t", model.mode, model.navigation.Focused(), model.navigationVisibleAt(model.width))
	}
}

func TestFreshOverviewCanOpenSelectedJobFromNavigation(t *testing.T) {
	model := interactionTestModel(t)
	model.mode = modeJobs
	model.width = 80
	model.snapshot = flink.Snapshot{}
	model.preferredJobID = ""
	overview := model.jobList.State()
	overview.Jobs = []flink.JobSummary{{ID: "selected-job", Name: "Selected", State: "RUNNING"}}
	overview.Selection = 0
	model.jobList.RestoreState(overview)

	model = updateWithKey(model, tea.Key{Code: tea.KeyLeft})
	model = updateWithKey(model, tea.Key{Code: tea.KeyDown})
	rows := model.navigationRows()
	if row := rows[model.navigation.State().Cursor]; row.Target != navigationGraph || !row.Enabled {
		t.Fatalf("fresh overview selected row = %#v, want enabled Job Graph", row)
	}
	updated, command := model.Update(tea.KeyPressMsg(tea.Key{Code: tea.KeyEnter}))
	opened := updated.(Model)
	if command == nil || opened.mode != modeGraph || opened.preferredJobID != "selected-job" || opened.navigation.Focused() {
		t.Fatalf("enter produced command=%v mode=%d job=%q focused=%t", command, opened.mode, opened.preferredJobID, opened.navigation.Focused())
	}
}

func TestRightReturnsFromNarrowMenuAndGraphLeftKeepsGraphMeaning(t *testing.T) {
	model := interactionTestModel(t)
	model.mode = modeJobs
	model.width = 80
	model = updateWithKey(model, tea.Key{Code: tea.KeyLeft})
	model = updateWithKey(model, tea.Key{Code: tea.KeyRight})
	if model.navigation.Focused() || !model.navigationVisibleAt(model.width) {
		t.Fatalf("right left navigation focused=%t visible=%t", model.navigation.Focused(), model.navigationVisibleAt(model.width))
	}
	model.width = 60
	setTestNavigation(&model, navigationAuto, false)
	model = updateWithKey(model, tea.Key{Code: tea.KeyLeft})
	model = updateWithKey(model, tea.Key{Code: tea.KeyRight})
	if model.navigation.Focused() || model.navigationVisibleAt(model.width) {
		t.Fatalf("right should close full-screen navigation: focused=%t visible=%t", model.navigation.Focused(), model.navigationVisibleAt(model.width))
	}

	model.mode = modeGraph
	model.selected = "risk"
	model.layout.Parents = map[string][]string{"risk": {"source"}}
	model = updateWithKey(model, tea.Key{Code: tea.KeyLeft})
	if model.selected != "source" || model.navigation.Focused() {
		t.Fatalf("graph left selected=%q navigationFocused=%t", model.selected, model.navigation.Focused())
	}
}

func TestSidebarMouseNavigationAndContentTranslation(t *testing.T) {
	model := interactionTestModel(t)
	model.width = 140
	model.height = 24
	viewport := model.graphViewport.State()
	viewport.OffsetX, viewport.OffsetY = 80, 4
	model.graphViewport.RestoreState(viewport)
	checkpointRow := -1
	for index, row := range model.navigationRows() {
		if row.Target == navigationCheckpoints {
			checkpointRow = index
			break
		}
	}
	if checkpointRow < 0 {
		t.Fatal("checkpoint navigation row is missing")
	}

	updated, _ := model.Update(tea.MouseClickMsg{
		X:      2,
		Y:      headerHeight + checkpointRow,
		Button: tea.MouseLeft,
	})
	checkpointModel := updated.(Model)
	if checkpointModel.mode != modeCheckpoints {
		t.Fatalf("sidebar click opened mode %d, want checkpoints", checkpointModel.mode)
	}

	// A click on the shell separator must not activate the row beside it.
	checkpointModel.mode = modeGraph
	updated, _ = checkpointModel.Update(tea.MouseClickMsg{
		X:      checkpointModel.navigationWidthAt(checkpointModel.width),
		Y:      headerHeight + 1,
		Button: tea.MouseLeft,
	})
	if got := updated.(Model).mode; got != modeGraph {
		t.Fatalf("separator click opened mode %d, want graph", got)
	}

	model.mode = modeGraph
	target := model.layout.Rects["risk"]
	viewport = model.graphViewport.State()
	updated, _ = model.Update(tea.MouseClickMsg{
		X:      model.contentStartX() + target.X + 1 - viewport.OffsetX,
		Y:      graphScreenTop + target.Y + 1 - viewport.OffsetY,
		Button: tea.MouseLeft,
	})
	if got := updated.(Model).selected; got != "risk" {
		t.Fatalf("content click with sidebar selected %q, want risk", got)
	}
}

func TestSidebarWheelTakesFocusAndMovesNavigationCursor(t *testing.T) {
	model := interactionTestModel(t)
	model.width = 80
	setTestNavigation(&model, navigationShown, false)

	updated, _ := model.Update(tea.MouseWheelMsg{
		X:      2,
		Y:      headerHeight + 4,
		Button: tea.MouseWheelDown,
	})
	got := updated.(Model)
	if !got.navigation.Focused() {
		t.Fatal("sidebar wheel did not take navigation focus")
	}
	rows := got.navigationRows()
	if target := rows[got.navigation.State().Cursor].Target; target != navigationSubtasks {
		t.Fatalf("sidebar wheel selected target %v, want first expanded graph child", target)
	}
}

func TestCommandPaletteCapturesTextAndExecutesFilteredCommand(t *testing.T) {
	model := interactionTestModel(t)
	model = updateWithKey(model, tea.Key{Code: ':', Text: ":"})
	if !model.palette.Open() {
		t.Fatal(": should open the command palette")
	}

	model = updateWithKey(model, tea.Key{Code: 'q', Text: "q"})
	if state := model.palette.State(); !state.Open || state.Query != "q" {
		t.Fatalf("q in palette produced open=%t query=%q", state.Open, state.Query)
	}

	model = updateWithKey(model, tea.Key{Code: tea.KeyEscape})
	model = updateWithKey(model, tea.Key{Code: ':', Text: ":"})
	for _, character := range "history details" {
		model = updateWithKey(model, tea.Key{Code: character, Text: string(character)})
	}
	commands := model.filteredPaletteCommands()
	if len(commands) != 1 || commands[0].ID != commandCheckpoints {
		t.Fatalf("history/details filter returned %#v", commands)
	}
	model = updateWithKey(model, tea.Key{Code: tea.KeyEnter})
	if model.palette.Open() || model.mode != modeCheckpoints {
		t.Fatalf("palette execution produced open=%t mode=%d", model.palette.Open(), model.mode)
	}
}

func TestCommandPaletteRecognizesOperatorAliases(t *testing.T) {
	model := interactionTestModel(t)
	sqlClient, err := flink.NewSQLGatewayClient("http://localhost:8083")
	if err != nil {
		t.Fatal(err)
	}
	model.configureSQLWorkbench(sqlClient)
	tests := map[string]commandID{
		"logs": commandProcessLogs, "savepoint": commandActions,
		"cancel": commandActions, "profiler": commandProfiler,
		"watermark": commandBackpressure, "sql": commandSQL, "flame": commandFlameGraph,
		"tl": commandTimeline, "acc": commandAccumulators,
		"cfg": commandJobConfig, "act": commandActions,
		"cp": commandCheckpoints, "dx": commandBackpressure, "mx": commandMetrics,
		"tm": commandTaskManagers, "jm": commandJobManager,
	}
	for query, want := range tests {
		state := model.palette.State()
		state.Open, state.Query, state.Cursor = true, query, 0
		model.palette.RestoreState(state)
		filtered := model.filteredPaletteCommands()
		if len(filtered) != 1 || filtered[0].ID != want {
			t.Fatalf("palette alias %q returned %#v, want %q", query, filtered, want)
		}
	}
}

func TestPaletteIndexesAndOpensTaskManagerByID(t *testing.T) {
	model := parityTestModel(t)
	state := model.palette.State()
	state.Open, state.Query, state.Cursor = true, "tm:1", 0
	model.palette.RestoreState(state)

	filtered := model.filteredPaletteCommands()
	wantID := commandID(commandTaskManagerPrefix + "tm:1")
	if len(filtered) == 0 || filtered[0].ID != wantID {
		t.Fatalf("TaskManager ID results = %#v, want %q first", filtered, wantID)
	}
	command := model.handlePaletteKey("enter")
	manager, ok := model.selectedTaskManager()
	if command == nil || model.mode != modeTaskManagerDetail || !ok || manager.ID != "tm:1" {
		t.Fatalf("TaskManager selection = mode:%d manager:%#v found:%t command:%v", model.mode, manager, ok, command)
	}
}

func TestJobScopedPaletteAliasLoadsHighlightedOverviewJob(t *testing.T) {
	model := parityTestModel(t)
	model.mode = modeJobs
	model.snapshot = flink.Snapshot{}
	model.preferredJobID = ""
	overview := model.jobList.State()
	overview.Jobs = []flink.JobSummary{{ID: "selected-job", Name: "Selected", State: "RUNNING"}}
	overview.Selection = 0
	model.jobList.RestoreState(overview)

	palette := model.palette.State()
	palette.Open, palette.Query = true, "savepoint"
	model.palette.RestoreState(palette)
	if filtered := model.filteredPaletteCommands(); len(filtered) != 1 || filtered[0].ID != commandActions {
		t.Fatalf("fresh Overview savepoint alias = %#v", filtered)
	}
	command := model.handlePaletteKey("enter")
	if command == nil || model.mode != modeGraph || model.preferredJobID != "selected-job" ||
		model.navigation.State().DeferredTarget != navigationActions {
		t.Fatalf("deferred action = command:%v mode:%d job:%q target:%d", command, model.mode,
			model.preferredJobID, model.navigation.State().DeferredTarget)
	}

	next := flink.Snapshot{JobID: "selected-job", JobName: "Selected", Nodes: []flink.Node{{ID: "source", Name: "Source"}}}
	model.applySnapshot(snapshotMsg{snapshot: next, generation: model.generation})
	if model.mode != modeActions || model.navigation.State().DeferredTarget != navigationNone {
		t.Fatalf("loaded alias destination = mode:%d deferred:%d", model.mode, model.navigation.State().DeferredTarget)
	}
}

func TestOpeningAnotherJobFromOverviewReturnsToCheckpoints(t *testing.T) {
	model := parityTestModel(t)
	model.mode = modeCheckpoints
	_ = model.openJobPicker()
	overview := model.jobList.State()
	overview.Jobs = []flink.JobSummary{
		{ID: model.snapshot.JobID, Name: "Previously Loaded", State: "RUNNING"},
		{ID: "highlighted-job", Name: "Highlighted", State: "RUNNING"},
	}
	overview.Selection = 1
	model.jobList.RestoreState(overview)

	updated, command := model.Update(tea.KeyPressMsg(tea.Key{Code: tea.KeyEnter}))
	model = updated.(Model)
	if command == nil || model.preferredJobID != "highlighted-job" || model.snapshot.JobID != "" ||
		model.navigation.State().DeferredTarget != navigationCheckpoints {
		t.Fatalf("Overview enter used job=%q snapshot=%q target=%d command=%v", model.preferredJobID,
			model.snapshot.JobID, model.navigation.State().DeferredTarget, command)
	}
	next := flink.Snapshot{JobID: "highlighted-job", JobName: "Highlighted", Nodes: []flink.Node{{ID: "source", Name: "Source"}}}
	model.applySnapshot(snapshotMsg{snapshot: next, generation: model.generation})
	if model.mode != modeCheckpoints {
		t.Fatalf("Overview enter completed in mode %d, want checkpoints", model.mode)
	}
}

func TestOpeningAJobAfterClusterScreenFallsBackToGraph(t *testing.T) {
	model := parityTestModel(t)
	model.mode = modeTaskManagers
	_ = model.openJobPicker()
	overview := model.jobList.State()
	overview.Jobs = []flink.JobSummary{{ID: "highlighted-job", Name: "Highlighted", State: "RUNNING"}}
	overview.Selection = 0
	model.jobList.RestoreState(overview)

	updated, command := model.Update(tea.KeyPressMsg(tea.Key{Code: tea.KeyEnter}))
	model = updated.(Model)
	if command == nil || model.preferredJobID != "highlighted-job" ||
		model.navigation.State().DeferredTarget != navigationNone {
		t.Fatalf("cluster continuation = job:%q target:%d command:%v", model.preferredJobID,
			model.navigation.State().DeferredTarget, command)
	}
	next := flink.Snapshot{JobID: "highlighted-job", JobName: "Highlighted", Nodes: []flink.Node{{ID: "source", Name: "Source"}}}
	model.applySnapshot(snapshotMsg{snapshot: next, generation: model.generation})
	if model.mode != modeGraph {
		t.Fatalf("cluster-screen job switch completed in mode %d, want graph", model.mode)
	}
}

func TestJobPickerContinuationNormalizesScopedDrillDowns(t *testing.T) {
	tests := []struct {
		mode screenMode
		want navigationTarget
	}{
		{modeCheckpoints, navigationCheckpoints},
		{modeCheckpointOperators, navigationCheckpoints},
		{modeCheckpointSubtasks, navigationCheckpoints},
		{modeDiagnostics, navigationDiagnostics},
		{modeTimeline, navigationTimeline},
		{modeExceptions, navigationExceptions},
		{modeJobConfig, navigationJobConfig},
		{modeActions, navigationActions},
		{modeSubtasks, navigationGraph},
		{modeFlameGraph, navigationGraph},
		{modeMetricExplorer, navigationGraph},
		{modeAccumulators, navigationGraph},
		{modeTaskManagers, navigationGraph},
		{modeSQL, navigationGraph},
	}
	for _, test := range tests {
		if got := jobPickerTargetForMode(test.mode); got != test.want {
			t.Errorf("mode %d continuation = %d, want %d", test.mode, got, test.want)
		}
	}
}

func TestGraphUppercaseFOpensSelectedVertexFlameGraph(t *testing.T) {
	model := interactionTestModel(t)
	model.configureFlameGraphs()
	command := model.handleGraphKey("F")
	if model.mode != modeFlameGraph || command == nil || model.flameGraphs.State().Vertex != model.selected {
		t.Fatalf("F produced mode=%d command=%v vertex=%q", model.mode, command, model.flameGraphs.State().Vertex)
	}
	model.width = 120
	if footer := graphScreenFooter(model, "off", "Overview"); !strings.Contains(footer, "F flame for Source") ||
		!strings.Contains(footer, "f fit") || !strings.Contains(footer, ":st/fl/mx/acc go") {
		t.Fatalf("graph footer does not teach vertex views: %q", footer)
	}
	model.width = 80
	model.mode = modeGraph
	if footer := model.renderFooter(80); !strings.Contains(footer, "F flame:Source") ||
		!strings.Contains(footer, "f fit") || !strings.Contains(footer, ":st/fl/mx/acc go") {
		t.Fatalf("80-column graph footer hides vertex views: %q", footer)
	}
}

func TestGraphRejectsOrphanDestinationLettersButEscapeStillBacksOut(t *testing.T) {
	for _, key := range []string{"c", "o"} {
		model := interactionTestModel(t)
		if command := model.handleGraphKey(key); command != nil || model.mode != modeGraph {
			t.Fatalf("graph key %q produced mode=%d command=%v", key, model.mode, command)
		}
	}
	model := interactionTestModel(t)
	if command := model.handleGraphKey("esc"); command == nil || model.mode != modeJobs {
		t.Fatalf("graph escape produced mode=%d command nil=%t", model.mode, command == nil)
	}
}

func TestGraphOpensVertexAccumulatorsAndMetrics(t *testing.T) {
	model := interactionTestModel(t)
	model.configureAccumulators()
	command := model.handleGraphKey("u")
	if model.mode != modeAccumulators || command == nil {
		t.Fatalf("u produced mode=%d command=%v, want accumulators", model.mode, command)
	}

	model = interactionTestModel(t)
	model.configureMetrics()
	command = model.handleGraphKey("v")
	if model.mode != modeMetricExplorer || command == nil {
		t.Fatalf("v produced mode=%d command=%v, want metrics", model.mode, command)
	}
}

func TestVertexViewsHaveTheirOwnSidebarIdentity(t *testing.T) {
	model := interactionTestModel(t)
	want := map[navigationTarget]string{
		navigationSubtasks: "st", navigationFlameGraph: "fl",
		navigationMetrics: "mx", navigationAccumulators: "acc",
	}
	for target, alias := range want {
		found := false
		for _, row := range model.navigationRows() {
			if row.Target == target {
				found = true
				if row.Alias != alias || row.Indent != 1 {
					t.Fatalf("vertex destination %v = %#v, want alias %q and one-level indent", target, row, alias)
				}
			}
		}
		if !found {
			t.Fatalf("vertex destination %v is missing from the expanded graph branch", target)
		}
	}
	for mode, target := range map[screenMode]navigationTarget{
		modeFlameGraph: navigationFlameGraph, modeSubtasks: navigationSubtasks,
		modeMetricExplorer: navigationMetrics, modeAccumulators: navigationAccumulators,
	} {
		model.mode = mode
		for _, row := range model.navigationRows() {
			if row.Active != (row.Target == target) {
				t.Fatalf("mode %d row %q active=%t, want only target %v active", mode, row.Label, row.Active, target)
			}
		}
	}
}

func TestNavigationCatalogShowsCanonicalAliasForEveryDestination(t *testing.T) {
	model := interactionTestModel(t)
	want := map[navigationTarget]string{
		navigationOverview: "ov", navigationGraph: "g", navigationSubtasks: "st",
		navigationFlameGraph: "fl", navigationCheckpoints: "cp", navigationTimeline: "tl",
		navigationDiagnostics: "dx", navigationMetrics: "mx", navigationAccumulators: "acc",
		navigationExceptions: "ex", navigationJobConfig: "cfg", navigationActions: "act",
		navigationTaskManagers: "tm", navigationJobManager: "jm", navigationSQL: "sq",
	}
	seen := make(map[navigationTarget]navigationRow)
	for _, row := range model.navigationRows() {
		if row.Group {
			continue
		}
		seen[row.Target] = row
		if row.Hint() == "" {
			t.Fatalf("destination %q has an empty gutter: %#v", row.Label, row)
		}
	}
	for target, alias := range want {
		row, ok := seen[target]
		if !ok || row.Alias != alias || row.Hint() != alias {
			t.Fatalf("destination %v = %#v, want alias %q", target, row, alias)
		}
	}

	rendered := model.renderNavigation(fullNavigationWidth, 24)
	for _, hint := range []string{
		"ov  Overview", "g   Job Graph", "st    Subtasks", "fl    Vertex Flame G",
		"mx    Metrics", "acc   Accumulators", "cp  Checkpoints", "tl  Timeline",
		"dx  Diagnostics", "ex  Exceptions", "cfg Job Configuration", "act Job Actions",
		"tm  Task Managers", "jm  Job Manager", "sq  SQL Workbench",
	} {
		if !strings.Contains(rendered, hint) {
			t.Fatalf("sidebar missing %q:\n%s", hint, rendered)
		}
	}
	if strings.Contains(rendered, "1 Overview") || strings.Contains(rendered, "4 Checkpoints") {
		t.Fatalf("sidebar still paints digits in the alias column:\n%s", rendered)
	}
}

func TestFocusedNavigationAcceptsDisplayedAliases(t *testing.T) {
	model := interactionTestModel(t)
	model.width = 80
	setTestNavigation(&model, navigationShown, false)
	model.focusNavigation()

	model = updateWithKey(model, tea.Key{Code: 't', Text: "t"})
	if model.mode != modeGraph || !model.navigation.Focused() {
		t.Fatalf("partial t produced mode=%d focused=%t, want to stay on the graph",
			model.mode, model.navigation.Focused())
	}
	model = updateWithKey(model, tea.Key{Code: 'l', Text: "l"})
	if model.mode != modeTimeline || model.navigation.Focused() {
		t.Fatalf("tl produced mode=%d focused=%t, want Timeline with screen focus",
			model.mode, model.navigation.Focused())
	}

	// A destination opens on the alias the sidebar prints, never on a shorter
	// prefix of it. Opening early handed the operator's remaining letters to
	// the screen that had just replaced theirs, so "cfg" opened Job
	// Configuration on "cf" and then let "g" jump straight to the Job Graph.
	model.focusNavigation()
	for _, letter := range []rune{'c', 'f'} {
		model = updateWithKey(model, tea.Key{Code: letter, Text: string(letter)})
		if model.mode != modeTimeline || !model.navigation.Focused() {
			t.Fatalf("partial %q produced mode=%d focused=%t, want to stay on Timeline",
				string(letter), model.mode, model.navigation.Focused())
		}
	}
	model = updateWithKey(model, tea.Key{Code: 'g', Text: "g"})
	if model.mode != modeJobConfig || model.navigation.Focused() {
		t.Fatalf("cfg produced mode=%d focused=%t, want Configuration with screen focus",
			model.mode, model.navigation.Focused())
	}

	model.focusNavigation()
	model = updateWithKey(model, tea.Key{Code: 'd', Text: "d"})
	if model.mode != modeJobConfig || !model.navigation.Focused() {
		t.Fatalf("partial d produced mode=%d focused=%t, want to stay on Configuration",
			model.mode, model.navigation.Focused())
	}
	model = updateWithKey(model, tea.Key{Code: 'x', Text: "x"})
	if model.mode != modeDiagnostics || model.navigation.Focused() {
		t.Fatalf("dx produced mode=%d focused=%t, want Diagnostics with screen focus",
			model.mode, model.navigation.Focused())
	}
}

// TestFocusedNavigationReachesJobManagerAlias covers the one sidebar alias that
// begins with a vim motion letter. Treating "j" as "move down" made the
// documented "jm" unreachable and silently routed the trailing "m" to the
// Metrics alias instead.
func TestFocusedNavigationReachesJobManagerAlias(t *testing.T) {
	model := interactionTestModel(t)
	model.width = 80
	setTestNavigation(&model, navigationShown, false)
	model.focusNavigation()

	model = updateWithKey(model, tea.Key{Code: 'j', Text: "j"})
	if model.mode != modeGraph || !model.navigation.Focused() {
		t.Fatalf("partial j produced mode=%d focused=%t, want to stay on the graph",
			model.mode, model.navigation.Focused())
	}
	model = updateWithKey(model, tea.Key{Code: 'm', Text: "m"})
	if model.mode != modeJobManager || model.navigation.Focused() {
		t.Fatalf("jm produced mode=%d focused=%t, want Job Manager with screen focus",
			model.mode, model.navigation.Focused())
	}
}

func TestOverviewIsTheOnlyNumericNavigationShortcut(t *testing.T) {
	model := interactionTestModel(t)
	got := make([]string, 0, 1)
	for _, row := range model.navigationRows() {
		if row.Shortcut != "" {
			got = append(got, row.Shortcut)
		}
	}
	if strings.Join(got, ",") != "1" {
		t.Fatalf("numeric navigation shortcuts = %v, want only 1", got)
	}
}

func TestGlobalDestinationsDoNotStealContextualSort(t *testing.T) {
	model := interactionTestModel(t)
	model.mode = modeSubtasks

	model = updateWithKey(model, tea.Key{Code: 's', Text: "s"})
	state := model.jobDetails.State()
	if model.mode != modeSubtasks || !state.SubtaskSortOpen {
		t.Fatalf("s produced mode=%d sortOpen=%t", model.mode, state.SubtaskSortOpen)
	}
	model = updateWithKey(model, tea.Key{Code: 'p', Text: "p"})
	model = updateWithKey(model, tea.Key{Code: tea.KeyEnter})
	state = model.jobDetails.State()
	if state.SubtaskSort != 2 || !state.SubtaskSortDescending || state.SubtaskSortOpen {
		t.Fatalf("sort picker produced sort=%d descending=%t open=%t", state.SubtaskSort, state.SubtaskSortDescending, state.SubtaskSortOpen)
	}

	model = updateWithKey(model, tea.Key{Code: '2', Text: "2"})
	if model.mode != modeSubtasks {
		t.Fatalf("2 produced mode=%d, want the current screen to keep the key", model.mode)
	}
}

func TestJobDestinationsRemainAvailableOutsideJobScreens(t *testing.T) {
	for _, mode := range []screenMode{modeJobs, modeTaskManagers, modeJobManager, modeSQL} {
		model := interactionTestModel(t)
		model.mode = mode
		seen := make(map[navigationTarget]navigationRow)
		for _, row := range model.navigationRows() {
			seen[row.Target] = row
		}
		for _, target := range []navigationTarget{
			navigationTimeline,
			navigationDiagnostics,
			navigationExceptions,
			navigationJobConfig,
			navigationActions,
		} {
			row, ok := seen[target]
			if !ok || !row.Enabled {
				t.Fatalf("mode %d destination %v = %#v, want present and enabled", mode, target, row)
			}
		}
	}
}

func TestShortNavigationScrollsToKeyboardAndMouseTargets(t *testing.T) {
	model := interactionTestModel(t)
	sqlClient, err := flink.NewSQLGatewayClient("http://localhost:8083")
	if err != nil {
		t.Fatal(err)
	}
	model.configureSQLWorkbench(sqlClient)
	model.width = 50
	model.height = 14
	setTestNavigation(&model, navigationShown, false)
	model.focusNavigation()
	model.moveNavigationCursorToEdge(true)

	bodyHeight := model.height - headerHeight - footerHeightAt(model.width)
	rendered := model.renderNavigation(model.width, bodyHeight)
	if !strings.Contains(rendered, "> sq  SQL Workbench") || !strings.Contains(rendered, "^ more") {
		t.Fatalf("end of short navigation is not visible with overflow marker:\n%s", rendered)
	}
	if lines := strings.Split(rendered, "\n"); len(lines) != bodyHeight {
		t.Fatalf("short navigation rendered %d lines, want %d", len(lines), bodyHeight)
	}

	workbenchLine := -1
	for index, line := range strings.Split(rendered, "\n") {
		if strings.Contains(line, "SQL Workbench") {
			workbenchLine = index
			break
		}
	}
	if workbenchLine < 0 {
		t.Fatal("could not locate visible Workbench row")
	}
	updated, command := model.Update(tea.MouseClickMsg{
		X:      2,
		Y:      headerHeight + workbenchLine,
		Button: tea.MouseLeft,
	})
	opened := updated.(Model)
	if opened.mode != modeSQL || command == nil {
		t.Fatalf("scrolled Workbench click produced mode=%d command=%v", opened.mode, command)
	}

	model = updateWithKey(model, tea.Key{Code: tea.KeyHome})
	rendered = model.renderNavigation(model.width, bodyHeight)
	if !strings.Contains(rendered, "> ov  Overview") || !strings.Contains(rendered, "v more") {
		t.Fatalf("start of short navigation is not visible with overflow marker:\n%s", rendered)
	}
}

func TestJobsRefreshPreservesCursorByJobIdentity(t *testing.T) {
	model := interactionTestModel(t)
	model.mode = modeJobs
	overview := model.jobList.State()
	overview.Jobs = []flink.JobSummary{
		{ID: "job", Name: "Loaded", State: "RUNNING"},
		{ID: "other", Name: "Other", State: "RUNNING"},
	}
	overview.Selection = 1
	model.jobList.RestoreState(overview)
	overview = model.jobList.State()
	overview.Jobs = []flink.JobSummary{
		{ID: "other", Name: "Other", State: "RUNNING"},
		{ID: "job", Name: "Loaded", State: "RUNNING"},
	}
	overview.Selection = 0
	model.jobList.RestoreState(overview)
	if selected := model.jobIDAtCursor(); selected != "other" {
		t.Fatalf("refresh selected %q, want manually selected job %q", selected, "other")
	}
}

func TestFootersAdvertiseMenuAndBackPaths(t *testing.T) {
	tests := []struct {
		mode screenMode
		want []string
	}{
		{mode: modeJobs, want: []string{"left nav", "g graph"}},
		{mode: modeDiagnostics, want: []string{"left nav", "q/esc Job Graph", "g graph"}},
		{mode: modeTimeline, want: []string{"left nav", "q/esc Job Graph", "g graph"}},
		{mode: modeExceptions, want: []string{"left nav", "q/esc Job Graph"}},
		{mode: modeJobConfig, want: []string{"left nav", "q/esc Job Graph", "g graph"}},
		{mode: modeActions, want: []string{"left nav", "q/esc Job Graph", "g graph"}},
		{mode: modeTaskManagers, want: []string{"left nav", "q/esc Overview"}},
		{mode: modeJobManager, want: []string{"left nav", "q/esc Overview"}},
		{mode: modeProfilerFlameGraph, want: []string{"ctrl+n nav", "q/esc Process Profiler"}},
	}
	for _, test := range tests {
		model := interactionTestModel(t)
		model.mode = test.mode
		footer := model.renderFooter(100)
		for _, expected := range test.want {
			if !strings.Contains(footer, expected) {
				t.Fatalf("mode %d footer missing %q: %q", test.mode, expected, footer)
			}
		}
	}

	graph := interactionTestModel(t)
	graph.width = 120
	if footer := graph.renderFooter(120); !strings.Contains(footer, "ctrl+n focus") || !strings.Contains(footer, "1 home") {
		t.Fatalf("graph footer missing navigation hints: %q", footer)
	}
}

func TestNarrowFooterKeepsGlobalKeysOnTheirOwnRow(t *testing.T) {
	model := interactionTestModel(t)
	model.width, model.height = 80, 30
	model.mode = modeJobs
	footer := model.renderFooter(80)
	lines := strings.Split(footer, "\n")
	if len(lines) != 2 {
		t.Fatalf("narrow footer = %d rows, want 2", len(lines))
	}
	for index, line := range lines {
		if got := lipgloss.Width(line); got != 80 {
			t.Fatalf("narrow footer row %d = %d cells, want 80", index+1, got)
		}
	}
	if !strings.Contains(lines[0], ": go") || !strings.Contains(lines[0], "? help") {
		t.Fatalf("narrow footer row 1 = %q, want the global keys", lines[0])
	}
	if !strings.Contains(lines[0], "^N focus") || !strings.Contains(lines[0], "^G sidebar") {
		t.Fatalf("80-column global footer clipped navigation controls: %q", lines[0])
	}
	if !strings.Contains(lines[1], "left nav") {
		t.Fatalf("narrow footer row 2 = %q, want the screen keys", lines[1])
	}
	veryNarrow := model.renderFooter(60)
	veryNarrowGlobal := strings.Split(veryNarrow, "\n")[0]
	for _, expected := range []string{": go", "? help", "^N nav", "^C quit"} {
		if !strings.Contains(veryNarrowGlobal, expected) {
			t.Fatalf("60-column global footer missing %q: %q", expected, veryNarrowGlobal)
		}
	}

	wide := interactionTestModel(t)
	wide.width, wide.height = 160, 48
	if strings.Contains(wide.renderFooter(160), "\n") {
		t.Fatal("wide footer must stay a single row")
	}
}

func TestPaletteIsTheAdvertisedPrimaryNavigationLanguage(t *testing.T) {
	model := interactionTestModel(t)
	for _, width := range []int{60, 80, 100, 160} {
		global := model.globalFooterKeys(width)
		if !strings.HasPrefix(global, ": go") {
			t.Fatalf("width %d global footer starts %q, want : go", width, global)
		}
		quit := "ctrl+c quit"
		if width < wideFooterThreshold {
			quit = "^C quit"
		}
		if !strings.Contains(global, quit) {
			t.Fatalf("width %d global footer dropped %q: %q", width, quit, global)
		}
	}

	model.palette.Show()
	palette := ansi.Strip(model.renderShellBody(100, 20))
	for _, expected := range []string{"alias", "job", "vertex", "process"} {
		if !strings.Contains(palette, expected) {
			t.Fatalf("palette prompt missing %q:\n%s", expected, palette)
		}
	}

	for mode := screenMode(0); mode < modeCount; mode++ {
		candidate := model
		candidate.palette.Close()
		candidate.mode = mode
		local := candidate.activeScreen().footer(candidate, "off", "Overview")
		if strings.Contains(local, "ctrl+n→") {
			t.Fatalf("%s still advertises the sidebar as a named route: %q", screenModeName(mode), local)
		}
	}
}

func TestSlashRemainsScreenOwnedOutsideFilterTables(t *testing.T) {
	for _, mode := range []screenMode{modeGraph, modeFlameGraph, modeSQL} {
		model := interactionTestModel(t)
		model.mode = mode
		if mode == modeFlameGraph {
			model.configureFlameGraphs()
		}
		before := model
		model = updateWithKey(model, tea.Key{Code: '/', Text: "/"})
		if model.mode != mode {
			t.Fatalf("/ on %s changed mode to %s", screenModeName(mode), screenModeName(model.mode))
		}
		if model.jobDetails.CapturesKeys() || model.checkpoints.CapturesKeys() || model.accumulators.CapturesKeys() || model.jobOperations.CapturesKeys() {
			t.Fatalf("/ on %s opened an unrelated shared table filter (before mode %s)", screenModeName(mode), screenModeName(before.mode))
		}
	}
}

func TestNarrowModalFootersKeepTheReservedSecondRow(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*Model)
	}{
		{name: "notice", setup: func(model *Model) { model.setNotice("Something changed.") }},
		{name: "help", setup: func(model *Model) { model.help.Show() }},
		{name: "palette", setup: func(model *Model) { model.palette.Show() }},
		{name: "error details", setup: func(model *Model) { model.errorDetailOpen = true }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			model := interactionTestModel(t)
			model.width, model.height = 80, 24
			test.setup(&model)
			lines := strings.Split(model.renderFooter(model.width), "\n")
			if len(lines) != footerHeightAt(model.width) {
				t.Fatalf("footer has %d rows, want %d", len(lines), footerHeightAt(model.width))
			}
			for index, line := range lines {
				if got := lipgloss.Width(line); got != model.width {
					t.Fatalf("row %d has %d cells, want %d", index+1, got, model.width)
				}
			}
		})
	}
}

func TestWideJobNavigationFitsSQLAndPersistentContext(t *testing.T) {
	model := interactionTestModel(t)
	model.mode = modeGraph
	model.snapshot.JobState = "RUNNING"
	model.snapshot.Checkpoints = flink.CheckpointSummary{
		Total:   3,
		History: []flink.Checkpoint{{ID: 3, Status: "COMPLETED"}},
	}

	rendered := model.renderNavigation(fullNavigationWidth, 20)
	for _, expected := range []string{"SQL Workbench", "CONTEXT", "Test", "Source", "cp#3"} {
		if !strings.Contains(rendered, expected) {
			t.Fatalf("navigation missing %q:\n%s", expected, rendered)
		}
	}
	if lines := strings.Split(rendered, "\n"); len(lines) != 20 {
		t.Fatalf("navigation rendered %d lines, want 20", len(lines))
	}
}

func TestJobSidebarScopeFollowsHighlightedOverviewRow(t *testing.T) {
	model := interactionTestModel(t)
	model.configureJobList()
	state := model.jobList.State()
	state.Jobs = []flink.JobSummary{
		{ID: "job", Name: "Previously Loaded", State: "RUNNING", Duration: time.Hour},
		{ID: "next", Name: "Highlighted Incident Job", State: "FAILING", Duration: 2 * time.Hour},
	}
	state.Selection = 1
	model.jobList.RestoreState(state)
	model.mode = modeJobs

	rendered := ansi.Strip(model.renderNavigation(fullNavigationWidth, 24))
	if !strings.Contains(rendered, "JOB  Highlighted Inci") {
		t.Fatalf("job scope does not follow Overview cursor:\n%s", rendered)
	}
	if strings.Contains(rendered, "Previously Loaded") || strings.Count(rendered, "Highlighted") != 1 {
		t.Fatalf("job scope is duplicated or stale:\n%s", rendered)
	}
	if context := model.navigationContext(); context.JobName != "Highlighted Incident Job" ||
		context.JobState != "FAILING" || context.VertexName != "" || context.HasCheckpoint {
		t.Fatalf("Overview navigation context = %#v", context)
	}
}

func TestJobSidebarScopeIsBareAndDisabledWithoutSelection(t *testing.T) {
	model := interactionTestModel(t)
	model.configureJobList()
	model.jobList.RestoreState(joblistmodule.State{})
	model.snapshot = flink.Snapshot{}
	model.preferredJobID = ""
	model.mode = modeJobs

	rows := model.navigationRows()
	if rows[2].Label != "JOB" || rows[3].Enabled {
		t.Fatalf("empty job scope = label:%q graph enabled:%t", rows[2].Label, rows[3].Enabled)
	}
}

func TestLongJobScopeDoesNotDisplaceNavigationRows(t *testing.T) {
	model := interactionTestModel(t)
	model.snapshot.JobName = strings.Repeat("Long production job ", 4)
	model.mode = modeGraph
	rendered := ansi.Strip(model.renderNavigation(fullNavigationWidth, 24))
	if !strings.Contains(rendered, "JOB  Long production") || !strings.Contains(rendered, "Job Graph") ||
		!strings.Contains(rendered, "SQL Workbench") {
		t.Fatalf("long job scope displaced navigation:\n%s", rendered)
	}
	for _, line := range strings.Split(rendered, "\n") {
		if lipgloss.Width(line) > fullNavigationWidth {
			t.Fatalf("job scope line width = %d: %q", lipgloss.Width(line), line)
		}
	}
}

func TestVertexContextStaysBesideNavigationAtSupportedWidths(t *testing.T) {
	for _, size := range []struct {
		width  int
		height int
	}{{width: 160, height: 48}, {width: 100, height: 30}} {
		t.Run(fmt.Sprintf("%dx%d", size.width, size.height), func(t *testing.T) {
			model := interactionTestModel(t)
			model.width, model.height = size.width, size.height
			model.mode = modeSubtasks
			model.selected = "risk"
			model.snapshot.Nodes[1].Name = "Decode Orders"
			bodyHeight := model.height - headerHeight - footerHeightAt(model.width)
			lines := strings.Split(ansi.Strip(model.renderNavigation(fullNavigationWidth, bodyHeight)), "\n")

			lastDestination, contextLine := -1, -1
			for index, line := range lines {
				if strings.Contains(line, "SQL Workbench") {
					lastDestination = index
				}
				if strings.Contains(line, "CONTEXT") {
					contextLine = index
				}
			}
			if !strings.Contains(strings.Join(lines, "\n"), "vtx Decode Orders") {
				t.Fatalf("sidebar lost full vertex name:\n%s", strings.Join(lines, "\n"))
			}
			if lastDestination < 0 || contextLine-lastDestination != 2 {
				t.Fatalf("last destination=%d context=%d, want one blank separator:\n%s",
					lastDestination, contextLine, strings.Join(lines, "\n"))
			}
		})
	}
}

func updateWithKey(model Model, key tea.Key) Model {
	updated, _ := model.Update(tea.KeyPressMsg(key))
	return updated.(Model)
}

// A live TaskManager's Pekko path contains "tcp", and every worker used to
// claim the "tm" alias outright. Both outranked the destination an operator
// typed, so the same keystrokes opened different screens depending on whether
// infrastructure had been fetched yet.
func TestPaletteGutterAliasesOutrankLiveObjects(t *testing.T) {
	model := interactionTestModel(t)
	state := model.infrastructure.State()
	state.Infrastructure.TaskManagers = []flink.TaskManager{{
		ID:    "172.22.0.3:43539-ded791",
		Path:  "pekko.tcp://flink@172.22.0.3:43539/user/rpc/taskmanager_0",
		Slots: 16, FreeSlots: 7,
	}}
	model.infrastructure.RestoreState(state)
	model.jobList.RestoreState(joblistmodule.State{Jobs: []flink.JobSummary{
		{ID: "job", Name: "Test", State: "RUNNING"},
	}})

	for alias, want := range map[string]string{
		"cp": "Open Checkpoints",
		"tm": "Open Task Managers",
		"st": "Open Subtasks",
		"fl": "Open Vertex Flame Graph",
		"ex": "Open Exceptions",
		"pr": "Open Process Profiler",
		"g":  "Open Job Graph",
	} {
		model.palette.Show()
		for _, letter := range alias {
			model.palette.HandleKey(string(letter), model.paletteCommands())
		}
		filtered := model.filteredPaletteCommands()
		model.palette.Close()
		if len(filtered) == 0 {
			t.Fatalf("palette %q matched nothing", alias)
		}
		if filtered[0].Label != want {
			t.Fatalf("palette %q ranked %q first, want %q", alias, filtered[0].Label, want)
		}
	}
}
