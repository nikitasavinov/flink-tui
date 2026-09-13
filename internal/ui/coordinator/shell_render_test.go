package coordinator

import (
	"fmt"
	"strings"
	"syscall"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/x/ansi"

	"github.com/nikitasavinov/flink-tui/internal/flink"
	"github.com/nikitasavinov/flink-tui/internal/graph"
	jobgraphmodule "github.com/nikitasavinov/flink-tui/internal/ui/jobgraph"
	"github.com/nikitasavinov/flink-tui/internal/ui/shared"
	"github.com/nikitasavinov/flink-tui/internal/ui/shell"
	"github.com/nikitasavinov/flink-tui/internal/ui/snapshotstate"
)

func TestMouseCaptureToggleChangesViewMode(t *testing.T) {
	model := interactionTestModel(t)
	view := model.View()
	if got := view.MouseMode; got != tea.MouseModeCellMotion {
		t.Fatalf("initial mouse mode = %v, want cell motion", got)
	}
	if view.OnMouse != nil {
		t.Fatal("mouse callback would duplicate events already delivered to Update")
	}

	updated, _ := model.Update(tea.KeyPressMsg(tea.Key{Code: 'm', Text: "m"}))
	got := updated.(Model)
	if got.mouseEnabled {
		t.Fatal("m should disable mouse capture")
	}
	if mode := got.View().MouseMode; mode != tea.MouseModeNone {
		t.Fatalf("disabled mouse mode = %v, want none", mode)
	}
	if got.View().OnMouse != nil {
		t.Fatal("disabled mouse view still has an OnMouse callback")
	}
}

func TestNewModelLeavesTerminalMouseSelectionAvailable(t *testing.T) {
	client, err := flink.NewClient("http://localhost:8081")
	if err != nil {
		t.Fatal(err)
	}
	model := NewModel(client, "", 3*time.Second)
	if model.mouseEnabled || model.View().MouseMode != tea.MouseModeNone {
		t.Fatalf("new model mouse enabled=%t mode=%v", model.mouseEnabled, model.View().MouseMode)
	}
	updated, _ := model.Update(tea.KeyPressMsg(tea.Key{Code: 'm', Text: "m"}))
	model = updated.(Model)
	if !model.mouseEnabled || model.View().MouseMode != tea.MouseModeCellMotion {
		t.Fatalf("m did not enable app mouse: enabled=%t mode=%v", model.mouseEnabled, model.View().MouseMode)
	}
}

func TestMetricRefreshPreservesManualViewport(t *testing.T) {
	model := interactionTestModel(t)
	nodes := make([]flink.Node, 12)
	for index := range nodes {
		nodes[index] = flink.Node{ID: string(rune('a' + index)), Name: "Stage", State: "RUNNING"}
		if index > 0 {
			previous := string(rune('a' + index - 1))
			nodes[index].Inputs = []flink.Input{{ID: previous}}
		}
	}
	model.snapshot = flink.Snapshot{JobID: "job", JobName: "Test", Nodes: nodes}
	model.layout = model.buildGraphLayout(nodes)
	model.selected = nodes[0].ID
	viewport := model.graphViewport.State()
	viewport.OffsetX = 200
	model.graphViewport.RestoreState(viewport)

	updated, _ := model.Update(snapshotMsg{snapshot: model.snapshot})
	got := updated.(Model).graphViewport.State()
	if got.OffsetX != viewport.OffsetX {
		t.Fatalf("refresh offsetX = %d, want manually panned %d", got.OffsetX, viewport.OffsetX)
	}
}

func interactionTestModel(t *testing.T) Model {
	t.Helper()
	client, err := flink.NewClient("http://localhost:8081")
	if err != nil {
		t.Fatal(err)
	}
	nodes := []flink.Node{
		{ID: "source", Name: "Source", State: "RUNNING"},
		{ID: "risk", Name: "Risk", State: "RUNNING", Inputs: []flink.Input{{ID: "source"}}},
		{ID: "sink", Name: "Sink", State: "RUNNING", Inputs: []flink.Input{{ID: "risk"}}},
	}
	return Model{
		client:         client,
		snapshot:       flink.Snapshot{JobID: "job", JobName: "Test", Nodes: nodes},
		graphViewport:  jobgraphmodule.NewViewport(false),
		graphTelemetry: jobgraphmodule.NewTelemetry(),
		layout: graph.Layout{
			Rects: map[string]graph.Rect{
				"source": {X: 2, Y: 2, W: nodeWidth, H: nodeHeight},
				"risk":   {X: 100, Y: 10, W: nodeWidth, H: nodeHeight},
				"sink":   {X: 360, Y: 30, W: nodeWidth, H: nodeHeight},
			},
			Order:  []string{"source", "risk", "sink"},
			Width:  400,
			Height: 40,
		},
		selected:     "source",
		width:        80,
		height:       24,
		mouseEnabled: true,
	}
}

func setTestNavigation(model *Model, display navigationDisplay, focused bool) {
	state := model.navigation.State()
	state.Display = display
	state.Focused = focused
	model.navigation.RestoreState(state)
}

func setTestNavigationCursor(model *Model, cursor int) {
	state := model.navigation.State()
	state.Cursor = cursor
	model.navigation.RestoreState(state)
}

func TestRefreshBadgeUsesSuccessfulFetchAgeAndCurrentError(t *testing.T) {
	now := time.Date(2026, time.August, 24, 12, 0, 0, 0, time.UTC)
	interval := 3 * time.Second
	tests := []struct {
		name        string
		lastSuccess time.Time
		err         error
		degraded    bool
		paused      bool
		want        string
	}{
		{name: "initial connection failure", err: syscall.ECONNREFUSED, want: "DISCONNECTED"},
		{name: "fresh", lastSuccess: now.Add(-2 * time.Second), want: "LIVE"},
		{name: "recent failed refresh", lastSuccess: now.Add(-12 * time.Second), err: syscall.ECONNREFUSED, want: "STALE 12s"},
		{name: "prolonged failure", lastSuccess: now.Add(-16 * time.Second), err: syscall.ECONNREFUSED, want: "DISCONNECTED"},
		{name: "late refresh", lastSuccess: now.Add(-7 * time.Second), want: "STALE 7s"},
		{name: "partial snapshot", lastSuccess: now.Add(-2 * time.Second), degraded: true, want: "DEGRADED"},
		{name: "paused", lastSuccess: now.Add(-12 * time.Second), paused: true, want: "PAUSED 12s"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			badge := shell.DeriveRefreshBadge(now, test.lastSuccess, interval, test.err, test.degraded, test.paused)
			if badge.Label != test.want {
				t.Fatalf("badge = %q, want %q", badge.Label, test.want)
			}
		})
	}
}

func TestInfrastructureHeaderUsesTheSameWorkersAsItsBody(t *testing.T) {
	model := interactionTestModel(t)
	model.configureInfrastructure()
	model.mode = modeTaskManagers
	model.infrastructure.Restore(flink.Infrastructure{
		TaskManagers: []flink.TaskManager{
			{ID: "tm-1", Slots: 4, FreeSlots: 1},
			{ID: "tm-2", Slots: 4, FreeSlots: 2},
			{ID: "tm-3", Slots: 4, FreeSlots: 3},
		},
		// Simulate the JobManager metric lag observed immediately after scale-up.
		JobManager: flink.JobManager{TaskManagers: 1, SlotsTotal: 4, SlotsAvailable: 1, RunningJobs: 2},
		UpdatedAt:  time.Now(),
	})
	header := model.renderInfrastructureHeader("Task Managers", 120)
	if !strings.Contains(header, "task managers 3") || !strings.Contains(header, "slots 6/12 available") ||
		strings.Contains(header, "task managers 1") {
		t.Fatalf("infrastructure header disagrees with worker list:\n%s", header)
	}
}

func TestInfrastructureHeaderDistinguishesUnfetchedFromFetchedZero(t *testing.T) {
	model := interactionTestModel(t)
	model.configureInfrastructure()
	model.mode = modeTaskManagers

	unfetched := ansi.Strip(model.renderInfrastructureHeader("Task Managers", 120))
	if !strings.Contains(unfetched, "task managers —") || strings.Contains(unfetched, "task managers 0") {
		t.Fatalf("unfetched infrastructure was rendered as fact:\n%s", unfetched)
	}

	model.infrastructure.Restore(flink.Infrastructure{UpdatedAt: time.Now()})
	fetched := ansi.Strip(model.renderInfrastructureHeader("Task Managers", 120))
	for _, expected := range []string{"task managers 0", "slots 0/0 available", "jobs 0"} {
		if !strings.Contains(fetched, expected) {
			t.Fatalf("fetched empty infrastructure missing %q:\n%s", expected, fetched)
		}
	}
}

func TestTransportErrorIsConciseWithFullDetailsBehindE(t *testing.T) {
	transportError := &flink.RequestError{
		Service: "Flink",
		Method:  "GET",
		Path:    "/overview",
		Err:     fmt.Errorf("Get \"http://localhost:9999/overview\": dial tcp [::1]:9999: %w", syscall.ECONNREFUSED),
	}
	if got := conciseError(transportError); got != "connection refused" {
		t.Fatalf("concise error = %q", got)
	}

	model := interactionTestModel(t)
	model.mode = modeGraph
	model.snapshot = flink.Snapshot{}
	model.err = transportError
	model.refreshInterval = 3 * time.Second
	header := model.renderHeader(120)
	for _, expected := range []string{"DISCONNECTED", "connection refused", "e details"} {
		if !strings.Contains(header, expected) {
			t.Fatalf("header missing %q:\n%s", expected, header)
		}
	}
	if strings.Contains(header, "dial tcp") || strings.Contains(header, "Get \"") {
		t.Fatalf("header leaked transport details:\n%s", header)
	}

	updated, _ := model.Update(tea.KeyPressMsg(tea.Key{Code: 'e', Mod: tea.ModCtrl}))
	model = updated.(Model)
	if !model.errorDetailOpen {
		t.Fatal("e did not open error details")
	}
	details := model.renderErrorDetails(120, 20)
	for _, expected := range []string{"ERROR DETAILS", "GET /overview", "dial tcp", "localhost:9999"} {
		if !strings.Contains(details, expected) {
			t.Fatalf("error details missing %q:\n%s", expected, details)
		}
	}
}

func TestPartialSnapshotRetainsLastKnownSubresources(t *testing.T) {
	oldCheckpoint := flink.CheckpointSummary{Total: 8, LatestID: 8}
	oldExceptions := flink.ExceptionSummary{Count: 1, Latest: "boom"}
	previous := flink.Snapshot{
		JobID:       "job",
		Checkpoints: oldCheckpoint,
		Exceptions:  oldExceptions,
		Nodes: []flink.Node{{
			ID:      "source",
			Metrics: flink.Metrics{RecordsOutPerSecond: 123, BusyPercent: 80},
		}},
	}
	next := flink.Snapshot{
		JobID: "job",
		Nodes: []flink.Node{{ID: "source"}},
		Issues: []flink.SnapshotIssue{
			{Kind: flink.SnapshotIssueMetrics, VertexID: "source", Err: syscall.ECONNRESET},
			{Kind: flink.SnapshotIssueCheckpoints, Err: syscall.ECONNRESET},
			{Kind: flink.SnapshotIssueExceptions, Err: syscall.ECONNRESET},
		},
	}
	merged := snapshotstate.Merge(previous, next)
	if merged.Nodes[0].Metrics.RecordsOutPerSecond != 123 || merged.Nodes[0].Metrics.BusyPercent != 80 {
		t.Fatalf("merged metrics = %#v", merged.Nodes[0].Metrics)
	}
	if merged.Checkpoints.LatestID != oldCheckpoint.LatestID || merged.Exceptions.Latest != oldExceptions.Latest {
		t.Fatalf("merged health = checkpoints %#v exceptions %#v", merged.Checkpoints, merged.Exceptions)
	}
}

func TestHomeHeaderDistinguishesPartialFailureFromDisconnection(t *testing.T) {
	model := interactionTestModel(t)
	model.mode = modeJobs
	model.refreshInterval = 3 * time.Second
	overview := model.jobList.State()
	overview.Cluster = flink.ClusterOverview{UpdatedAt: time.Now(), TaskManagers: 1}
	overview.UpdatedAt = overview.Cluster.UpdatedAt
	overview.JobsErr = &flink.RequestError{
		Service: "Flink", Method: "GET", Path: "/jobs/overview",
		StatusCode: 500, Status: "500 Internal Server Error",
	}
	model.jobList.RestoreState(overview)
	header := model.renderHomeHeader(120)
	if !strings.Contains(header, "DEGRADED") || strings.Contains(header, "DISCONNECTED") {
		t.Fatalf("partial home failure status:\n%s", header)
	}
}

func TestSQLCanOpenErrorDetailsWithoutStealingPrintableE(t *testing.T) {
	model := interactionTestModel(t)
	model.mode = modeSQL
	model.sql.Open()
	updated, _ := model.Update(tea.KeyPressMsg(tea.Key{Code: 'e', Mod: tea.ModCtrl}))
	model = updated.(Model)
	if !model.errorDetailOpen {
		t.Fatal("ctrl+e did not open SQL error details")
	}
}

func TestRemovedJobScopedDigitsDoNotClaimMissingContext(t *testing.T) {
	for _, key := range []rune{'5', '7'} {
		model := interactionTestModel(t)
		model.mode = modeJobs
		model.snapshot = flink.Snapshot{}
		model.selected = ""
		updated, command := model.Update(tea.KeyPressMsg(tea.Key{Code: key, Text: string(key)}))
		model = updated.(Model)
		if command != nil || model.mode != modeJobs {
			t.Fatalf("key %q changed mode to %d or returned command %v", key, model.mode, command)
		}
		if notice := model.activeNotice(); notice != "" {
			t.Fatalf("removed key %q notice = %q, want none", key, notice)
		}
	}
}

func TestGraphToDiagnosticsRequestsFullRepaint(t *testing.T) {
	model := interactionTestModel(t)
	model.mode = modeGraph
	model.width = 80
	model.height = 24
	command := model.navigate(navigationDiagnostics)
	if model.mode != modeDiagnostics || command == nil {
		t.Fatalf("diagnostics navigation = mode %d command %v", model.mode, command)
	}
	if messageType := fmt.Sprintf("%T", command()); messageType != "tea.clearScreenMsg" {
		t.Fatalf("navigation repaint message = %s, want tea.clearScreenMsg", messageType)
	}
	rendered := model.render()
	if lines := strings.Count(rendered, "\n") + 1; lines != 24 {
		t.Fatalf("80x24 diagnostics rendered %d lines", lines)
	}
	if strings.Contains(rendered, "input skew  0.0%") {
		t.Fatalf("diagnostics retained graph inspector content:\n%s", rendered)
	}
}

// sqlGatewayJobName is the shape the SQL Gateway gives a job: the statement
// text, newlines and all. Rendering it verbatim used to break every row below
// it on the cluster overview.
const sqlGatewayJobName = "SELECT `t`.`name`, COUNT(*) AS `hits`\nFROM (VALUES ROW('alpha'),\nROW('beta')) AS `t` (`name`)"

func TestSanitizeLineReplacesControlCharacters(t *testing.T) {
	got := shared.SanitizeLine("SELECT a\nFROM t\r\nWHERE\tx")
	want := "SELECT a FROM t  WHERE x"
	if got != want {
		t.Fatalf("sanitizeLine = %q, want %q", got, want)
	}
	if strings.ContainsAny(got, "\n\r\t") {
		t.Fatalf("sanitizeLine left a control character in %q", got)
	}
}

func TestTruncateFlattensMultiLineJobName(t *testing.T) {
	got := shared.Truncate(sqlGatewayJobName, 40)
	if strings.ContainsAny(got, "\n\r") {
		t.Fatalf("truncate leaked a newline: %q", got)
	}
	if width := ansi.StringWidth(got); width > 40 {
		t.Fatalf("truncate width = %d, want <= 40", width)
	}
	if !strings.HasSuffix(got, "...") {
		t.Fatalf("truncate should have ellipsised %q", got)
	}
}

// A short name with a newline still fits its column by width, so the length
// check alone never fires. This is the case the original implementation missed.
func TestTruncateSanitizesEvenWhenItFits(t *testing.T) {
	got := shared.Truncate("a\nb", 40)
	if got != "a b" {
		t.Fatalf("truncate = %q, want %q", got, "a b")
	}
}

func TestTruncateMeasuresDisplayWidthNotRunes(t *testing.T) {
	// Six runes, twelve cells: rune counting would let this overflow a
	// ten-cell column and shift every column to its right.
	const name = "日本語処理系"
	got := shared.Truncate(name, 10)
	if width := ansi.StringWidth(got); width > 10 {
		t.Fatalf("truncate(%q, 10) width = %d, want <= 10", name, width)
	}
	if got == name {
		t.Fatalf("truncate returned the full wide string %q", got)
	}
}

func TestPadRightFillsDisplayWidth(t *testing.T) {
	for _, value := range []string{"ok", "日本", "🚀", "", "a\nb"} {
		if width := ansi.StringWidth(shared.PadRight(value, 12)); width != 12 {
			t.Fatalf("padRight(%q, 12) width = %d, want 12", value, width)
		}
	}
}

// The invariant every fixed-width screen depends on: a value fitted to a column
// occupies exactly that column, and carries nothing that moves the cursor.
func TestColumnInvariant(t *testing.T) {
	values := []string{
		"",
		"Flink TUI Demo",
		sqlGatewayJobName,
		"日本語処理系のジョブ",
		"mixed 日本語 and ascii",
		"emoji 🚀🚀 name",
		"tab\tseparated",
		"esc\x1b[31mred\x1b[0m",
		strings.Repeat("x", 400),
	}
	for _, value := range values {
		for _, width := range []int{1, 2, 3, 4, 5, 12, 40, 67, 200} {
			got := shared.PadRight(shared.Truncate(value, width), width)
			if w := ansi.StringWidth(got); w != width {
				t.Fatalf("padRight(truncate(%q, %d), %d) width = %d, want %d", value, width, width, w, width)
			}
			if strings.ContainsFunc(got, shared.IsLayoutBreaking) {
				t.Fatalf("padRight(truncate(%q, %d), %d) = %q contains a control character", value, width, width, got)
			}
		}
	}
}

func TestClipFillsWidthWithoutEllipsis(t *testing.T) {
	if got := shared.Clip("Risk Score", 4); got != "Risk" {
		t.Fatalf("clip = %q, want %q", got, "Risk")
	}
	// Clipping must not split a wide rune across the column boundary.
	if width := ansi.StringWidth(shared.Clip("日本語", 3)); width > 3 {
		t.Fatalf("clip width = %d, want <= 3", width)
	}
}
