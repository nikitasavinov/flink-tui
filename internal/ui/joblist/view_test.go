package joblist

import (
	"strings"
	"testing"

	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/x/ansi"

	"github.com/nikitasavinov/flink-tui/internal/flink"
	"github.com/nikitasavinov/flink-tui/internal/ui/shared"
)

const sqlGatewayJobName = "SELECT `t`.`name`, COUNT(*) AS `hits`\nFROM (VALUES ROW('alpha'),\nROW('beta')) AS `t` (`name`)"

func TestJobViewsSeparateActiveCompletedAndAll(t *testing.T) {
	model := New(nil, nil)
	model.Sync(Context{BodyHeight: 20})
	model.jobs = []flink.JobSummary{
		{ID: "running", Name: "Streaming Orders", State: "RUNNING"},
		{ID: "finished", Name: "Daily Backfill", State: "FINISHED"},
		{ID: "failed", Name: "Broken Import", State: "FAILED"},
	}
	model.syncJobCursor()

	active := model.Render(100, 16)
	assertJobViewContains(t, active, []string{"Active 1", "Streaming Orders"}, []string{"Daily Backfill", "Broken Import"})

	model.HandleKey("tab")
	if model.jobView != jobViewCompleted {
		t.Fatalf("tab selected view %v, want completed", model.jobView)
	}
	completed := model.Render(100, 16)
	assertJobViewContains(t, completed, []string{"Completed 2", "Daily Backfill", "Broken Import"}, []string{"Streaming Orders"})

	model.HandleKey("tab")
	if model.jobView != jobViewAll {
		t.Fatalf("second tab selected view %v, want all", model.jobView)
	}
	assertJobViewContains(t, model.Render(100, 16), []string{"All 3", "Streaming Orders", "Daily Backfill", "Broken Import"}, nil)
}

func TestCompletedTabMouseAndOpenIntent(t *testing.T) {
	model := New(nil, nil)
	model.Sync(Context{BodyHeight: 20})
	model.jobs = []flink.JobSummary{
		{ID: "running", Name: "Streaming Orders", State: "RUNNING"},
		{ID: "finished", Name: "Daily Backfill", State: "FINISHED"},
	}
	model.syncJobCursor()
	active, _ := model.jobCounts()
	completedTabX := 1 + shared.DisplayWidth(model.jobTabLabel(jobViewActive, active)) + 1
	result := model.HandleClick(tea.Mouse{X: completedTabX + 1, Y: headerHeight + jobTabsRow, Button: tea.MouseLeft})
	if result.Command != nil || model.jobView != jobViewCompleted {
		t.Fatalf("completed tab click produced view=%v result=%#v", model.jobView, result)
	}
	result = model.HandleKey("enter")
	if result.Intent != IntentOpenJob || result.JobID != "finished" {
		t.Fatalf("completed open intent = %#v", result)
	}
}

func TestRefreshPreservesVisibleSelectionByIdentity(t *testing.T) {
	model := New(nil, nil)
	model.jobView = jobViewCompleted
	model.jobsGeneration = 4
	model.jobs = []flink.JobSummary{
		{ID: "old", Name: "Old", State: "FINISHED"},
		{ID: "selected", Name: "Selected", State: "FAILED"},
	}
	model.jobSelection.Set(1, 2)
	model.Apply(refreshMsg{generation: 4, jobs: []flink.JobSummary{
		{ID: "selected", Name: "Selected", State: "FAILED"},
		{ID: "old", Name: "Old", State: "FINISHED"},
	}})
	if got := model.jobIDAtCursor(); got != "selected" {
		t.Fatalf("refresh selected %q, want selected", got)
	}
}

func TestOverviewFilterAndFullJobID(t *testing.T) {
	model := New(nil, nil)
	model.Sync(Context{BodyHeight: 20})
	model.jobs = []flink.JobSummary{
		{ID: "11111111111111111111111111111111", Name: "Orders Pipeline", State: "RUNNING", Type: "STREAMING"},
		{ID: "22222222222222222222222222222222", Name: "Payments Pipeline", State: "RUNNING", Type: "STREAMING"},
	}
	model.syncJobCursor()
	if rendered := ansi.Strip(model.Render(100, 16)); !strings.Contains(rendered, model.jobs[0].ID) {
		t.Fatalf("overview does not expose the complete selected Job ID:\n%s", rendered)
	}

	model.HandleKey("/")
	for _, key := range []string{"p", "a", "y"} {
		model.HandleKey(key)
	}
	if !model.CapturesKeys() || model.State().Query != "pay" {
		t.Fatalf("filter state = %#v", model.State())
	}
	model.HandleKey("enter")
	rendered := ansi.Strip(model.Render(100, 16))
	assertJobViewContains(t, rendered, []string{"Payments Pipeline", model.jobs[1].ID, "Filter /pay/"}, []string{"Orders Pipeline"})
}

func TestReopeningFilterReplacesTheSelectedQueryOnFirstType(t *testing.T) {
	model := New(nil, nil)
	model.Sync(Context{BodyHeight: 20})
	state := model.State()
	state.Query = "flame"
	model.RestoreState(state)

	model.HandleKey("/")
	if state := model.State(); !state.SearchOpen || !state.SearchReplace {
		t.Fatalf("reopened filter is not visibly selected: %#v", state)
	}
	if rendered := ansi.Strip(model.Render(80, 14)); !strings.Contains(rendered, "[flame]") {
		t.Fatalf("selected filter is not visible:\n%s", rendered)
	}
	for _, key := range []string{"d", "e", "m", "o"} {
		model.HandleKey(key)
	}
	if state := model.State(); state.Query != "demo" || state.SearchReplace {
		t.Fatalf("typing appended to old filter: %#v", state)
	}
}

func TestFilterChipExplainsEmptyCompletedTab(t *testing.T) {
	model := New(nil, nil)
	model.Sync(Context{BodyHeight: 20})
	model.jobs = []flink.JobSummary{
		{ID: "running", Name: "Checkpoint Pipeline", State: "RUNNING"},
		{ID: "finished", Name: "SELECT 1 AS answer", State: "FINISHED"},
	}
	state := model.State()
	state.Query = "checkpoint"
	model.RestoreState(state)
	model.jobView = jobViewCompleted

	rendered := ansi.Strip(model.Render(100, 16))
	for _, expected := range []string{"Filter /checkpoint/", "hides all completed jobs", "Ctrl+W clears"} {
		if !strings.Contains(rendered, expected) {
			t.Fatalf("filtered-empty tab missing %q:\n%s", expected, rendered)
		}
	}
}

func TestRefreshIgnoresOutOfOrderReplies(t *testing.T) {
	model := New(nil, nil)
	model.startJobsRefresh()
	olderGeneration := model.jobsGeneration
	model.Apply(refreshMsg{generation: olderGeneration})
	model.startJobsRefresh()
	newerGeneration := model.jobsGeneration
	model.Apply(refreshMsg{generation: newerGeneration, jobs: []flink.JobSummary{{ID: "new", Name: "New", State: "RUNNING"}}})
	model.Apply(refreshMsg{generation: olderGeneration, jobs: []flink.JobSummary{{ID: "old", Name: "Old", State: "RUNNING"}}})
	if len(model.jobs) != 1 || model.jobs[0].ID != "new" {
		t.Fatalf("stale jobs reply replaced latest list: %#v", model.jobs)
	}
}

func TestRenderJobRowKeepsMultiLineNameOnOneRow(t *testing.T) {
	model := Model{}
	job := flink.JobSummary{
		ID: "3855ce73f1a24d0e9c1b7a5e2d4f6081", Name: sqlGatewayJobName,
		State: "FINISHED", Type: "STREAMING", TotalTasks: 2,
	}
	for _, width := range []int{70, 90, 160, 200} {
		row := model.renderJobRow(job, false, width)
		if strings.Contains(row, "\n") {
			t.Fatalf("job row at width %d spans multiple lines: %q", width, row)
		}
		if got := ansi.StringWidth(row); got != width {
			t.Fatalf("job row width at %d = %d, want %d", width, got, width)
		}
	}
}

func assertJobViewContains(t *testing.T, rendered string, present, absent []string) {
	t.Helper()
	for _, value := range present {
		if !strings.Contains(rendered, value) {
			t.Fatalf("job view missing %q:\n%s", value, rendered)
		}
	}
	for _, value := range absent {
		if strings.Contains(rendered, value) {
			t.Fatalf("job view unexpectedly contains %q:\n%s", value, rendered)
		}
	}
}

// TestFilterEnterOpensTheMatchedJob covers the shortest path to a job. Enter
// used to close the filter and nothing more, so "/name enter" left the
// operator on Overview needing a second enter to open the row they had just
// narrowed the list down to.
func TestFilterEnterOpensTheMatchedJob(t *testing.T) {
	model := New(nil, nil)
	model.Sync(Context{BodyHeight: 20})
	model.jobs = []flink.JobSummary{
		{ID: "orders", Name: "Streaming Orders", State: "RUNNING"},
		{ID: "backfill", Name: "Daily Backfill", State: "RUNNING"},
	}
	model.syncJobCursor()

	if result := model.HandleKey("/"); result.Intent != IntentNone {
		t.Fatalf("opening the filter produced %#v", result)
	}
	for _, letter := range "Backf" {
		if result := model.HandleKey(string(letter)); result.Intent != IntentNone {
			t.Fatalf("typing %q produced %#v", string(letter), result)
		}
	}
	result := model.HandleKey("enter")
	if result.Intent != IntentOpenJob || result.JobID != "backfill" {
		t.Fatalf("filter enter = %#v, want the matched job opened", result)
	}
	if model.State().SearchOpen {
		t.Fatal("filter enter left the search box open")
	}
	if model.State().Query != "Backf" {
		t.Fatalf("filter enter changed the query to %q", model.State().Query)
	}
}

// TestFilterEscapeKeepsTheFilterWithoutOpening keeps the dismiss path distinct
// from the confirm path.
func TestFilterEscapeKeepsTheFilterWithoutOpening(t *testing.T) {
	model := New(nil, nil)
	model.Sync(Context{BodyHeight: 20})
	model.jobs = []flink.JobSummary{{ID: "orders", Name: "Streaming Orders", State: "RUNNING"}}
	model.syncJobCursor()

	model.HandleKey("/")
	model.HandleKey("O")
	if result := model.HandleKey("esc"); result.Intent != IntentNone {
		t.Fatalf("filter esc = %#v, want no navigation", result)
	}
	if state := model.State(); state.SearchOpen || state.Query != "O" {
		t.Fatalf("filter esc left state %#v", state)
	}
}
