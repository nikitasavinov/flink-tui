package shell

import (
	"testing"

	tea "charm.land/bubbletea/v2"
)

func TestNavigationGeometryAtResponsiveBoundaries(t *testing.T) {
	tests := []struct {
		name                       string
		display                    Display
		width                      int
		visible, only              bool
		navigation, content, start int
	}{
		{name: "auto below wide threshold", display: DisplayAuto, width: 99, content: 99},
		{name: "auto at wide threshold", display: DisplayAuto, width: 100, visible: true, navigation: 25, content: 74, start: 26},
		{name: "shown below overlay cutover", display: DisplayShown, width: 71, visible: true, only: true, navigation: 71},
		{name: "shown at overlay cutover", display: DisplayShown, width: 72, visible: true, navigation: 20, content: 51, start: 21},
		{name: "shown quarter width", display: DisplayShown, width: 99, visible: true, navigation: 24, content: 74, start: 25},
		{name: "shown capped width", display: DisplayShown, width: 200, visible: true, navigation: 25, content: 174, start: 26},
		{name: "explicitly hidden wide", display: DisplayHidden, width: 200, content: 200},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			navigation := Navigation{display: test.display}
			if got := navigation.VisibleAt(test.width); got != test.visible {
				t.Errorf("VisibleAt(%d) = %t, want %t", test.width, got, test.visible)
			}
			if got := navigation.OnlyAt(test.width); got != test.only {
				t.Errorf("OnlyAt(%d) = %t, want %t", test.width, got, test.only)
			}
			if got := navigation.WidthAt(test.width); got != test.navigation {
				t.Errorf("WidthAt(%d) = %d, want %d", test.width, got, test.navigation)
			}
			if got := navigation.ContentWidthAt(test.width); got != test.content {
				t.Errorf("ContentWidthAt(%d) = %d, want %d", test.width, got, test.content)
			}
			if got := navigation.ContentStartX(test.width); got != test.start {
				t.Errorf("ContentStartX(%d) = %d, want %d", test.width, got, test.start)
			}
		})
	}
}

func TestNavigationHitTestBoundaries(t *testing.T) {
	const (
		width        = 80
		bodyHeight   = 5
		headerHeight = 3
	)
	rows := []Row{
		{Label: "JOBS", Group: true, Enabled: true},
		{Label: "Overview", Target: TargetOverview, Enabled: true},
		{Label: "Subtasks", Target: TargetSubtasks},
		{Label: "Graph", Target: TargetGraph, Enabled: true},
	}
	tests := []struct {
		name    string
		display Display
		mouse   tea.Mouse
		want    ClickResult
	}{
		{name: "hidden sidebar", display: DisplayHidden, mouse: tea.Mouse{X: 1, Y: 4, Button: tea.MouseLeft}},
		{name: "non-left button", display: DisplayShown, mouse: tea.Mouse{X: 1, Y: 4, Button: tea.MouseRight}},
		{name: "content column", display: DisplayShown, mouse: tea.Mouse{X: 21, Y: 4, Button: tea.MouseLeft}},
		{name: "separator", display: DisplayShown, mouse: tea.Mouse{X: 20, Y: 4, Button: tea.MouseLeft}, want: ClickResult{Handled: true}},
		{name: "above body", display: DisplayShown, mouse: tea.Mouse{X: 1, Y: 2, Button: tea.MouseLeft}, want: ClickResult{Handled: true}},
		{name: "group", display: DisplayShown, mouse: tea.Mouse{X: 1, Y: 3, Button: tea.MouseLeft}, want: ClickResult{Handled: true}},
		{name: "enabled row", display: DisplayShown, mouse: tea.Mouse{X: 1, Y: 4, Button: tea.MouseLeft}, want: ClickResult{Handled: true, Target: TargetOverview, Enabled: true}},
		{name: "disabled row", display: DisplayShown, mouse: tea.Mouse{X: 1, Y: 5, Button: tea.MouseLeft}, want: ClickResult{Handled: true, Target: TargetSubtasks}},
		{name: "last row", display: DisplayShown, mouse: tea.Mouse{X: 1, Y: 6, Button: tea.MouseLeft}, want: ClickResult{Handled: true, Target: TargetGraph, Enabled: true}},
		{name: "below rows", display: DisplayShown, mouse: tea.Mouse{X: 1, Y: 7, Button: tea.MouseLeft}, want: ClickResult{Handled: true}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			navigation := Navigation{display: test.display}
			if got := navigation.HitTest(test.mouse, width, bodyHeight, headerHeight, rows); got != test.want {
				t.Fatalf("HitTest(%#v) = %#v, want %#v", test.mouse, got, test.want)
			}
		})
	}
}

func TestNavigationHitTestUsesScrolledViewport(t *testing.T) {
	rows := []Row{
		{Target: TargetOverview, Enabled: true},
		{Target: TargetGraph, Enabled: true},
		{Target: TargetSubtasks, Enabled: true},
		{Target: TargetFlameGraph, Enabled: true},
		{Target: TargetCheckpoints, Enabled: true},
		{Target: TargetTimeline, Enabled: true},
		{Target: TargetDiagnostics, Enabled: true},
		{Target: TargetMetrics, Enabled: true},
	}
	navigation := Navigation{display: DisplayShown, focused: true, cursor: 4}
	tests := []struct {
		name string
		y    int
		want ClickResult
	}{
		{name: "upper scroll marker", y: 3, want: ClickResult{Handled: true}},
		{name: "first visible row", y: 4, want: ClickResult{Handled: true, Target: TargetFlameGraph, Enabled: true}},
		{name: "second visible row", y: 5, want: ClickResult{Handled: true, Target: TargetCheckpoints, Enabled: true}},
		{name: "lower scroll marker", y: 6, want: ClickResult{Handled: true}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mouse := tea.Mouse{X: 1, Y: test.y, Button: tea.MouseLeft}
			if got := navigation.HitTest(mouse, 80, 4, 3, rows); got != test.want {
				t.Fatalf("HitTest(y=%d) = %#v, want %#v", test.y, got, test.want)
			}
		})
	}
}

func TestNavigationMoveToEdgeSkipsGroupsAndDisabledRows(t *testing.T) {
	rows := []Row{
		{Label: "start group", Group: true, Enabled: true},
		{Label: "disabled"},
		{Label: "first", Enabled: true},
		{Label: "last", Enabled: true},
		{Label: "disabled"},
		{Label: "end group", Group: true, Enabled: true},
	}
	navigation := Navigation{cursor: 99, prefix: "stale"}
	navigation.MoveToEdge(false, rows)
	if navigation.cursor != 2 || navigation.prefix != "" {
		t.Fatalf("first edge = cursor %d prefix %q", navigation.cursor, navigation.prefix)
	}
	navigation.MoveToEdge(true, rows)
	if navigation.cursor != 3 {
		t.Fatalf("last edge = cursor %d, want 3", navigation.cursor)
	}

	navigation = Navigation{cursor: 7, prefix: "stale"}
	navigation.MoveToEdge(false, []Row{{Group: true, Enabled: true}, {Enabled: false}})
	if navigation.cursor != 7 || navigation.prefix != "" {
		t.Fatalf("no selectable edge changed state to cursor %d prefix %q", navigation.cursor, navigation.prefix)
	}
}
