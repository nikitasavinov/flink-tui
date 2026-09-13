package shell

import (
	"strings"
	"testing"

	"github.com/charmbracelet/x/ansi"
)

func TestNavigationOwnsResponsiveFocusAndSelection(t *testing.T) {
	rows := []Row{
		{Label: "GROUP", Group: true, Enabled: true},
		{Label: "Overview", Target: TargetOverview, Enabled: true, Active: true},
		{Label: "Graph", Target: TargetGraph, Enabled: true},
		{Label: "Disabled", Target: TargetSubtasks},
	}
	var navigation Navigation
	navigation.ToggleFocus(60, rows)
	if !navigation.OnlyAt(60) || !navigation.Focused() {
		t.Fatalf("open state = %#v", navigation.State())
	}
	navigation.Move(1, rows)
	result := navigation.HandleKey("enter", rows)
	if result.Action != KeyNavigate || result.Target != TargetGraph {
		t.Fatalf("enter result = %#v", result)
	}
	navigation.ReturnToContent(60)
	if navigation.VisibleAt(60) || navigation.Focused() {
		t.Fatalf("returned state = %#v", navigation.State())
	}
}

func TestNavigationFocusAndVisibilityAreIndependentInSplitPane(t *testing.T) {
	rows := []Row{{Label: "Overview", Target: TargetOverview, Enabled: true, Active: true}}
	var navigation Navigation

	navigation.ToggleVisibility(80, rows)
	if !navigation.VisibleAt(80) || navigation.Focused() {
		t.Fatalf("show = %#v, want visible without focus", navigation.State())
	}
	navigation.ToggleFocus(80, rows)
	if !navigation.VisibleAt(80) || !navigation.Focused() {
		t.Fatalf("focus = %#v, want visible and focused", navigation.State())
	}
	navigation.ToggleFocus(80, rows)
	if !navigation.VisibleAt(80) || navigation.Focused() {
		t.Fatalf("return to content = %#v, want visible without focus", navigation.State())
	}
	navigation.ToggleVisibility(80, rows)
	if navigation.VisibleAt(80) || navigation.Focused() {
		t.Fatalf("hide = %#v, want hidden and unfocused", navigation.State())
	}
}

func TestNarrowNavigationOverlayTakesFocusAndDismissesWithContent(t *testing.T) {
	rows := []Row{{Label: "Overview", Target: TargetOverview, Enabled: true, Active: true}}
	var navigation Navigation

	navigation.ToggleVisibility(60, rows)
	if !navigation.OnlyAt(60) || !navigation.Focused() {
		t.Fatalf("show narrow overlay = %#v", navigation.State())
	}
	navigation.ToggleFocus(60, rows)
	if navigation.VisibleAt(60) || navigation.Focused() {
		t.Fatalf("return from narrow overlay = %#v", navigation.State())
	}
}

func TestNavigationRendersChildDestinationsIndented(t *testing.T) {
	rendered := ansi.Strip(renderRow(Row{Label: "Vertex Flame Graph", Indent: 1, Enabled: true}, 30, false))
	if !strings.HasPrefix(rendered, "        Vertex Flame Graph") {
		t.Fatalf("indented row = %q", rendered)
	}
}

func TestNavigationRendersAliasInShortcutGutter(t *testing.T) {
	rendered := ansi.Strip(renderRow(Row{Label: "Timeline", Alias: "tl", Enabled: true}, 24, false))
	if !strings.Contains(rendered, "tl  Timeline") {
		t.Fatalf("alias row = %q", rendered)
	}
	numbered := ansi.Strip(renderRow(Row{Label: "Overview", Shortcut: "1", Alias: "ov", Enabled: true}, 24, false))
	if !strings.Contains(numbered, "ov  Overview") || strings.Contains(numbered, "1 Overview") {
		t.Fatalf("numbered row should show the alias, not the digit: %q", numbered)
	}
}

func TestNavigationContextNeverDisplacesDestinationRows(t *testing.T) {
	rows := []Row{
		{Label: "NAVIGATION", Group: true, Enabled: true},
		{Label: "Overview", Alias: "ov", Enabled: true},
		{Label: "Job Graph", Alias: "g", Enabled: true},
		{Label: "Task Managers", Alias: "tm", Enabled: true},
		{Label: "SQL Workbench", Alias: "sq", Enabled: true},
	}
	context := Context{JobName: "Orders", JobState: "RUNNING", VertexName: "Decode Orders"}
	rendered := ansi.Strip((Navigation{}).Render(25, len(rows), rows, context))
	for _, expected := range []string{"Overview", "Job Graph", "Task Managers", "SQL Workbench"} {
		if !strings.Contains(rendered, expected) {
			t.Fatalf("tight navigation displaced %q:\n%s", expected, rendered)
		}
	}
	if strings.Contains(rendered, "CONTEXT") {
		t.Fatalf("context displaced a destination in a full column:\n%s", rendered)
	}
}

func TestCompactContextPreservesVertexAndCheckpoint(t *testing.T) {
	rows := []Row{
		{Label: "NAVIGATION", Group: true, Enabled: true},
		{Label: "Overview", Alias: "ov", Enabled: true},
		{Label: "Job Graph", Alias: "g", Enabled: true},
	}
	context := Context{
		JobName: "Orders", JobState: "RUNNING", VertexName: "Decode Orders",
		HasCheckpoint: true, CheckpointID: 51,
	}
	rendered := ansi.Strip((Navigation{}).Render(25, len(rows)+2, rows, context))
	for _, expected := range []string{"CONTEXT", "vtx Decode Orders", "cp#51"} {
		if !strings.Contains(rendered, expected) {
			t.Fatalf("compact context missing %q:\n%s", expected, rendered)
		}
	}
}

func TestFocusedNavigationTypesAliasPrefix(t *testing.T) {
	rows := []Row{
		{Label: "Timeline", Alias: "tl", Target: TargetTimeline, Enabled: true},
		{Label: "Accumulators", Alias: "acc", Target: TargetAccumulators, Enabled: true},
		{Label: "Actions", Alias: "act", Target: TargetActions, Enabled: true},
		{Label: "Flame", Alias: "flame", Target: TargetFlameGraph, Enabled: true},
	}
	var navigation Navigation
	navigation.Focus(80, rows)

	// A prefix that already narrows to one row still waits for the rest of the
	// alias. Opening on "t" left the operator's "l" to act on Timeline.
	if result := navigation.HandleKey("t", rows); result.Action != KeyNone {
		t.Fatalf("partial t should wait, got %#v", result)
	}
	if rows[navigation.State().Cursor].Target != TargetTimeline {
		t.Fatalf("partial t cursor = %#v", rows[navigation.State().Cursor])
	}
	if result := navigation.HandleKey("l", rows); result.Action != KeyNavigate || result.Target != TargetTimeline {
		t.Fatalf("tl = %#v", result)
	}

	navigation.Focus(80, rows)
	if result := navigation.HandleKey("a", rows); result.Action != KeyNone {
		t.Fatalf("ambiguous a should wait, got %#v", result)
	}
	if rows[navigation.State().Cursor].Target != TargetAccumulators {
		t.Fatalf("ambiguous a cursor = %#v", rows[navigation.State().Cursor])
	}
	if result := navigation.HandleKey("c", rows); result.Action != KeyNone {
		t.Fatalf("ac should stay ambiguous, got %#v", result)
	}
	if result := navigation.HandleKey("t", rows); result.Action != KeyNavigate || result.Target != TargetActions {
		t.Fatalf("act = %#v", result)
	}

	navigation.Focus(80, rows)
	for _, letter := range []string{"f", "l", "a", "m"} {
		if result := navigation.HandleKey(letter, rows); result.Action != KeyNone {
			t.Fatalf("partial %q of flame should wait, got %#v", letter, result)
		}
	}
	if result := navigation.HandleKey("e", rows); result.Action != KeyNavigate || result.Target != TargetFlameGraph {
		t.Fatalf("flame = %#v", result)
	}
}

// TestFocusedNavigationOpensAnAliasThatPrefixesAnother keeps "tm" reachable
// even though "tmd" extends it: an alias always opens on its own spelling.
func TestFocusedNavigationOpensAnAliasThatPrefixesAnother(t *testing.T) {
	rows := []Row{
		{Label: "Task Managers", Alias: "tm", Target: TargetTaskManagers, Enabled: true},
		{Label: "Task Manager Detail", Alias: "tmd", Target: TargetTaskManagerDetail, Enabled: true},
	}
	var navigation Navigation
	navigation.Focus(80, rows)

	if result := navigation.HandleKey("t", rows); result.Action != KeyNone {
		t.Fatalf("partial t should wait, got %#v", result)
	}
	if result := navigation.HandleKey("m", rows); result.Action != KeyNavigate || result.Target != TargetTaskManagers {
		t.Fatalf("tm = %#v", result)
	}
}

// TestFocusedNavigationPrefersAliasesOverVimMotion covers "j", which both moves
// the cursor down and begins the Job Manager alias.
func TestFocusedNavigationPrefersAliasesOverVimMotion(t *testing.T) {
	rows := []Row{
		{Label: "Job Graph", Alias: "g", Target: TargetGraph, Enabled: true},
		{Label: "Job Manager", Alias: "jm", Target: TargetJobManager, Enabled: true},
	}
	var navigation Navigation
	navigation.Focus(80, rows)

	if result := navigation.HandleKey("j", rows); result.Action != KeyNone {
		t.Fatalf("partial j should wait, got %#v", result)
	}
	if result := navigation.HandleKey("m", rows); result.Action != KeyNavigate || result.Target != TargetJobManager {
		t.Fatalf("jm = %#v", result)
	}

	// "k" begins no alias, so it keeps its motion meaning.
	navigation.Focus(80, rows)
	navigation.Move(1, rows)
	if result := navigation.HandleKey("k", rows); result.Action != KeyNone || navigation.State().Cursor != 0 {
		t.Fatalf("k = %#v cursor=%d, want an upward move", result, navigation.State().Cursor)
	}
}

func TestPaletteFiltersAndReturnsTypedCommand(t *testing.T) {
	commands := []Command{
		{ID: "graph", Label: "Open Job Graph", Description: "topology"},
		{ID: "checkpoints", Label: "Open Checkpoints", Description: "history details", Aliases: []string{"savepoint"}},
	}
	var palette Palette
	palette.Show()
	for _, key := range []string{"h", "i", "s", "t", "o", "r", "y"} {
		palette.HandleKey(key, commands)
	}
	filtered := palette.Filter(commands)
	if len(filtered) != 1 || filtered[0].ID != "checkpoints" {
		t.Fatalf("filtered commands = %#v", filtered)
	}
	if id, ok := palette.HandleKey("enter", commands); !ok || id != "checkpoints" {
		t.Fatalf("selection = %q, %t", id, ok)
	}

	palette.Show()
	for _, key := range "savepoint" {
		palette.HandleKey(string(key), commands)
	}
	if filtered := palette.Filter(commands); len(filtered) != 1 || filtered[0].ID != "checkpoints" {
		t.Fatalf("alias filter returned %#v", filtered)
	}
}

func TestPaletteRanksNamesAboveDescriptionsAndObjectsWithinNames(t *testing.T) {
	commands := []Command{
		{ID: "graph", Label: "Open Job Graph", Description: "Explore the selected job topology"},
		{ID: "command-risk", Label: "Risk Score", Description: "Open a diagnostic command"},
		{ID: "topology-job", Label: "Flink TUI Topology Lab", Description: "job"},
		{ID: "risk-vertex", Label: "Risk Score", Description: "vertex", SearchPriority: 100},
	}
	var palette Palette
	palette.Show()

	for _, key := range "topo" {
		palette.HandleKey(string(key), commands)
	}
	filtered := palette.Filter(commands)
	if len(filtered) != 2 || filtered[0].ID != "topology-job" || filtered[1].ID != "graph" {
		t.Fatalf("topology ranking = %#v", filtered)
	}

	palette.Show()
	for _, key := range "risk score" {
		palette.HandleKey(string(key), commands)
	}
	filtered = palette.Filter(commands)
	if len(filtered) != 2 || filtered[0].ID != "risk-vertex" || filtered[1].ID != "command-risk" {
		t.Fatalf("object ranking = %#v", filtered)
	}
}
