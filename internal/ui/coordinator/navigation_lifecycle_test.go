package coordinator

import (
	"strings"
	"testing"

	tea "charm.land/bubbletea/v2"

	"github.com/charmbracelet/x/ansi"
	"github.com/nikitasavinov/flink-tui/internal/flink"
)

func TestCompactGraphKeepsFooterInsideTerminal(t *testing.T) {
	for _, width := range []int{40, 60, 80} {
		model := interactionTestModel(t)
		model.width, model.height = width, 14
		lines := strings.Split(model.render(), "\n")
		if len(lines) != model.height {
			t.Fatalf("%dx%d graph rendered %d rows, pushing controls below the terminal", width, model.height, len(lines))
		}
		if !strings.Contains(lines[model.height-2], ": go") {
			t.Fatalf("%d-column graph hid the global controls", width)
		}
		for row, line := range lines {
			if ansi.StringWidth(line) > width {
				t.Fatalf("%d-column graph row %d exceeds terminal width", width, row)
			}
		}
	}
}

func TestPendingVertexRequestDoesNotPreventReopeningScreen(t *testing.T) {
	for _, target := range []navigationTarget{navigationSubtasks, navigationMetrics, navigationAccumulators} {
		t.Run(destination(target).label, func(t *testing.T) {
			model := interactionTestModel(t)
			model.configureJobDetails()
			model.configureMetrics()
			model.configureAccumulators()
			if command := model.navigate(target); command == nil || model.activeNavigationTarget() != target {
				t.Fatal("first visit did not start loading the destination")
			}
			model.handleScreenBack()
			if model.mode != modeGraph {
				t.Fatal("Back did not return to Graph")
			}
			model.navigate(target)
			if model.activeNavigationTarget() != target {
				t.Fatalf("pending request prevented reopening %s", destination(target).label)
			}
		})
	}
}

func TestPaletteFromNarrowNavigationReturnsToVisibleContent(t *testing.T) {
	model := interactionTestModel(t)
	model.width = 60
	model.focusNavigation()
	if !model.navigationOnlyAt(model.width) {
		t.Fatal("fixture did not open the narrow navigation overlay")
	}
	updated, _ := model.Update(tea.KeyPressMsg{Code: ':', Text: ":"})
	model = updated.(Model)
	updated, _ = model.Update(tea.KeyPressMsg{Code: tea.KeyEscape})
	model = updated.(Model)
	if model.palette.Open() || model.navigationOnlyAt(model.width) || model.navigation.Focused() {
		t.Fatal("closing the palette left navigation covering the active content")
	}
	updated, _ = model.Update(tea.KeyPressMsg{Code: tea.KeyTab})
	if updated.(Model).selected != "risk" {
		t.Fatal("keyboard input did not return to the visible graph")
	}
}

func TestWindowResizeKeepsNavigationFocusOnVisiblePane(t *testing.T) {
	for _, test := range []struct {
		name    string
		display navigationDisplay
		focused bool
		width   int
	}{
		{name: "focused auto sidebar shrinks", display: navigationAuto, focused: true, width: 80},
		{name: "split sidebar becomes overlay", display: navigationShown, focused: false, width: 60},
	} {
		t.Run(test.name, func(t *testing.T) {
			model := interactionTestModel(t)
			model.width = 120
			setTestNavigation(&model, test.display, test.focused)
			updated, _ := model.Update(tea.WindowSizeMsg{Width: test.width, Height: 24})
			model = updated.(Model)
			if model.navigation.Focused() != test.focused || model.navigation.VisibleAt(model.width) != test.focused {
				t.Fatal("resize separated keyboard focus from the visible navigation pane")
			}
		})
	}
}

func TestLeavingDeferredJobRoutePreventsLateNavigation(t *testing.T) {
	for _, leave := range []string{"back", "graph", "process palette"} {
		t.Run(leave, func(t *testing.T) {
			model := interactionTestModel(t)
			model.configureJobList()
			state := model.jobList.State()
			state.Jobs = []flink.JobSummary{{ID: "job", State: "RUNNING"}, {ID: "next", State: "RUNNING"}}
			model.jobList.RestoreState(state)
			model.navigate(navigationCheckpoints)
			model.stepPeer(1)
			if model.navigation.State().DeferredTarget != navigationCheckpoints || model.preferredJobID != "next" {
				t.Fatal("fixture did not defer Checkpoints for the peer job")
			}
			wantMode := modeGraph
			switch leave {
			case "back":
				model.handleScreenBack()
			case "graph":
				model.navigate(navigationGraph)
			case "process palette":
				model.configureProcessDiagnostics()
				model.openPaletteProcessAt(paletteObject{kind: paletteJobManager, process: flink.JobManagerProcess()}, navigationThreadDump)
				wantMode = modeThreadDump
			}
			model.applySnapshot(snapshotMsg{generation: model.generation, snapshot: flink.Snapshot{JobID: "next"}})
			if model.mode != wantMode || model.navigation.State().DeferredTarget != navigationNone {
				t.Fatalf("late snapshot reopened the abandoned route: mode=%v", model.mode)
			}
		})
	}
}
