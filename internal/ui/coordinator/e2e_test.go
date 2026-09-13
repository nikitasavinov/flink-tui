package coordinator

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/x/ansi"

	"github.com/nikitasavinov/flink-tui/internal/flink"
	checkpointmodule "github.com/nikitasavinov/flink-tui/internal/ui/checkpoints"
	"github.com/nikitasavinov/flink-tui/internal/ui/exceptions"
	"github.com/nikitasavinov/flink-tui/internal/ui/jobgraph"
	processmodule "github.com/nikitasavinov/flink-tui/internal/ui/processdiag"
	"github.com/nikitasavinov/flink-tui/internal/ui/sqlworkbench"
)

const (
	e2eWidth           = 160
	e2eHeight          = 48
	e2eJobBodyRowStart = 4
)

type e2eRegion struct {
	minX int
	maxX int
	minY int
	maxY int
}

type e2eDriver struct {
	t         *testing.T
	model     Model
	sqlClient *flink.SQLGatewayClient
	trace     []string
}

func newE2EDriver(t *testing.T, endpoint, sqlEndpoint string) *e2eDriver {
	t.Helper()
	client, err := flink.NewClient(endpoint)
	if err != nil {
		t.Fatal(err)
	}

	var sqlClient *flink.SQLGatewayClient
	if sqlEndpoint != "" {
		sqlClient, err = flink.NewSQLGatewayClient(sqlEndpoint)
		if err != nil {
			t.Fatal(err)
		}
	}

	driver := &e2eDriver{
		t:         t,
		model:     NewModelWithSQLGateway(client, sqlClient, "", time.Hour),
		sqlClient: sqlClient,
	}
	driver.send("resize terminal", tea.WindowSizeMsg{Width: e2eWidth, Height: e2eHeight})
	driver.send("enable mouse capture", tea.KeyPressMsg(tea.Key{Code: 'm'}))
	if !driver.model.mouseEnabled {
		t.Fatal("mouse capture did not enable")
	}
	t.Cleanup(driver.cleanup)
	// Match Init's reserved first overview request without running its long
	// refresh timer in the synchronous headless driver.
	driver.runCommand(driver.model.jobList.InitialPoll(), 0)
	return driver
}

func (d *e2eDriver) cleanup() {
	state := d.model.sql.State()
	if d.sqlClient != nil && state.Session != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		if state.Operation != "" {
			_ = d.sqlClient.CloseOperation(ctx, state.Session, state.Operation)
		}
		_ = d.sqlClient.CloseSession(ctx, state.Session)
		cancel()
	}
	if !d.t.Failed() {
		return
	}

	directory := os.Getenv("FLINK_TUI_E2E_ARTIFACTS")
	if directory == "" {
		directory = filepath.Join("target", "e2e")
	}
	if err := os.MkdirAll(directory, 0o750); err != nil { //nolint:gosec // The test harness intentionally selects its artifact directory.
		d.t.Logf("could not create E2E artifact directory: %v", err)
		return
	}
	name := strings.NewReplacer("/", "-", " ", "-", "\\", "-").Replace(d.t.Name())
	if err := os.WriteFile(filepath.Join(directory, name+"-screen.txt"), []byte(d.screen()+"\n"), 0o600); err != nil { //nolint:gosec // Test artifact path is harness-controlled.
		d.t.Logf("could not save final E2E screen: %v", err)
	}
	trace := strings.Join(d.trace, "\n") + "\n"
	if err := os.WriteFile(filepath.Join(directory, name+"-trace.txt"), []byte(trace), 0o600); err != nil { //nolint:gosec // Test artifact path is harness-controlled.
		d.t.Logf("could not save E2E trace: %v", err)
	}
}

func (d *e2eDriver) waitForPlayground() {
	d.t.Helper()
	d.eventually("two running demo jobs and one TaskManager", 3*time.Minute, func() bool {
		d.runCommand(d.model.fetchJobs(), 0)
		overview := d.model.jobList.State()
		if overview.JobsErr != nil || overview.ClusterErr != nil || overview.Cluster.TaskManagers < 1 {
			return false
		}
		return d.hasRunningJob("Flink TUI Demo") && d.hasRunningJob("Flink TUI Checkpoints")
	})
	d.expectVisible("Flink TUI Demo")
	d.expectVisible("Flink TUI Checkpoints")
}

func (d *e2eDriver) waitForSQLGateway() {
	d.t.Helper()
	if d.sqlClient == nil {
		d.t.Fatal("SQL Gateway client is not configured")
	}
	d.eventually("SQL Gateway readiness", 2*time.Minute, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_, err := d.sqlClient.Info(ctx)
		cancel()
		return err == nil
	})
}

func (d *e2eDriver) hasRunningJob(name string) bool {
	for _, job := range d.model.jobList.State().Jobs {
		if job.Name == name && job.State == "RUNNING" {
			return true
		}
	}
	return false
}

func (d *e2eDriver) eventually(description string, timeout time.Duration, probe func() bool) {
	d.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if probe() {
			d.record("ready: %s", description)
			return
		}
		if time.Now().After(deadline) {
			d.t.Fatalf("timed out waiting for %s\n\n%s", description, d.screen())
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func (d *e2eDriver) clickSidebar(label string) {
	d.t.Helper()
	width := max(40, d.model.width)
	navigationWidth := d.model.navigationWidthAt(width)
	if navigationWidth == 0 {
		d.t.Fatalf("sidebar is not visible while trying to click %q", label)
	}
	d.clickText("sidebar "+label, label, e2eRegion{
		minX: 0,
		maxX: navigationWidth,
		minY: headerHeight,
		maxY: d.model.height - footerHeightAt(d.model.width),
	})
}

func (d *e2eDriver) clickJob(name string) {
	d.t.Helper()
	region := d.contentRegion()
	// Skip the overview title, tabs, selected-job status, and columns. The
	// selected job name also appears in the status line and is not clickable.
	region.minY = headerHeight + e2eJobBodyRowStart
	d.clickText("job "+name, name, region)
}

func (d *e2eDriver) clickContent(text string) {
	d.t.Helper()
	d.clickText("content "+text, text, d.contentRegion())
}

func (d *e2eDriver) graphRegion() e2eRegion {
	region := d.contentRegion()
	region.minY = graphScreenTop
	region.maxY = graphScreenTop + d.model.graphHeight()
	if minimap, ok := d.model.graphViewport.MinimapRect(d.model.graphWidth(), d.model.graphHeight(), len(d.model.snapshot.Nodes) > 0); ok {
		region.maxX = region.minX + minimap.X - 1
	}
	return region
}

func (d *e2eDriver) clickVisibleVertexOtherThan(vertexID string) flink.Node {
	d.t.Helper()
	region := d.graphRegion()
	for _, node := range d.model.snapshot.Nodes {
		if node.ID == vertexID {
			continue
		}
		rect, ok := d.model.layout.Rects[node.ID]
		if !ok {
			continue
		}
		viewport := d.model.graphViewport.State()
		x := d.model.contentStartX() + rect.X - viewport.OffsetX + rect.W/2
		y := graphScreenTop + rect.Y - viewport.OffsetY + rect.H/2
		if x < region.minX || x >= region.maxX || y < region.minY || y >= region.maxY {
			continue
		}
		d.record("click graph vertex %s at %d,%d", node.Name, x, y)
		d.sendMouse(x, y)
		if d.model.selected != node.ID {
			d.t.Fatalf("clicking graph vertex %q selected %q", node.Name, d.model.selected)
		}
		return node
	}
	d.t.Fatalf("no second graph vertex is visible\n\n%s", d.screen())
	return flink.Node{}
}

func (d *e2eDriver) clickText(action, text string, region e2eRegion) {
	d.t.Helper()
	x, y, ok := d.findText(text, region)
	if !ok {
		d.t.Fatalf("could not find clickable text %q for %s\n\n%s", text, action, d.screen())
	}
	d.record("click %s at %d,%d", action, x, y)
	d.sendMouse(x, y)
}

func (d *e2eDriver) findText(text string, region e2eRegion) (int, int, bool) {
	lines := strings.Split(d.screen(), "\n")
	maxY := min(region.maxY, len(lines))
	for y := max(0, region.minY); y < maxY; y++ {
		line := lines[y]
		searchFrom := 0
		for searchFrom <= len(line) {
			relative := strings.Index(line[searchFrom:], text)
			if relative < 0 {
				break
			}
			index := searchFrom + relative
			startX := lipgloss.Width(line[:index])
			endX := startX + max(1, lipgloss.Width(text))
			clickX := startX + (endX-startX)/2
			if clickX >= region.minX && clickX < region.maxX {
				return clickX, y, true
			}
			searchFrom = index + max(1, len(text))
		}
	}
	return 0, 0, false
}

func (d *e2eDriver) contentRegion() e2eRegion {
	return e2eRegion{
		minX: d.model.contentStartX(),
		maxX: d.model.width,
		minY: headerHeight,
		maxY: d.model.height - footerHeightAt(d.model.width),
	}
}

func (d *e2eDriver) sendMouse(x, y int) {
	d.send("mouse click", tea.MouseClickMsg{X: x, Y: y, Button: tea.MouseLeft})
}

func (d *e2eDriver) pressCtrlEnter() {
	d.send("press ctrl+enter", tea.KeyPressMsg(tea.Key{Code: tea.KeyEnter, Mod: tea.ModCtrl}))
}

func (d *e2eDriver) send(action string, message tea.Msg) {
	d.t.Helper()
	d.record("%s (%T)", action, message)
	d.apply(message, 0)
}

func (d *e2eDriver) apply(message tea.Msg, depth int) {
	d.t.Helper()
	if depth > 128 {
		d.t.Fatal("E2E command chain exceeded 128 transitions")
	}
	updated, command := d.model.Update(message)
	model, ok := updated.(Model)
	if !ok {
		d.t.Fatalf("UI returned unexpected model type %T", updated)
	}
	d.model = model
	d.runCommand(command, depth+1)
}

func (d *e2eDriver) runCommand(command tea.Cmd, depth int) {
	d.t.Helper()
	if command == nil {
		return
	}
	message := command()
	if message == nil {
		return
	}
	if batch, ok := message.(tea.BatchMsg); ok {
		for _, child := range batch {
			d.runCommand(child, depth+1)
		}
		return
	}
	d.apply(message, depth+1)
}

func (d *e2eDriver) expectVisible(text string) {
	d.t.Helper()
	if !strings.Contains(d.screen(), text) {
		d.t.Fatalf("expected screen to contain %q\n\n%s", text, d.screen())
	}
}

func (d *e2eDriver) screen() string {
	return ansi.Strip(d.model.View().Content)
}

func (d *e2eDriver) record(format string, values ...any) {
	d.trace = append(d.trace, fmt.Sprintf("%02d  %s", len(d.trace)+1, fmt.Sprintf(format, values...)))
}

func TestE2ECheckpointFailuresAgainstFlink(t *testing.T) {
	endpoint := os.Getenv("FLINK_TUI_E2E_ENDPOINT")
	if endpoint == "" {
		t.Skip("set FLINK_TUI_E2E_ENDPOINT or run make test-e2e")
	}
	driver := newE2EDriver(t, endpoint, "")
	driver.waitForPlayground()
	const jobName = "Flink TUI Checkpoint Failure Lab"
	driver.eventually("running checkpoint failure fixture", time.Minute, func() bool {
		driver.runCommand(driver.model.fetchJobs(), 0)
		for _, job := range driver.model.jobList.State().Jobs {
			if job.Name == jobName && job.State == "RUNNING" && job.RunningTasks > 0 && job.RunningTasks == job.TotalTasks {
				return true
			}
		}
		return false
	})
	driver.clickJob(jobName)
	driver.clickSidebar("Job Actions")
	for _, action := range []string{"Trigger configured checkpoint", "Trigger full checkpoint"} {
		driver.clickContent(action)
		driver.send("review "+action, tea.KeyPressMsg(tea.Key{Code: tea.KeyEnter}))
		if !driver.model.jobOperations.State().ActionConfirm {
			t.Fatalf("%s did not request confirmation", action)
		}
		driver.send("confirm "+action, tea.KeyPressMsg(tea.Key{Code: 'y'}))
		if state := driver.model.jobOperations.State(); state.ActionTriggerID == "" {
			t.Fatalf("%s was not accepted by Flink: %v", action, state.ActionErr)
		}
		driver.eventually(action+" completion", 45*time.Second, func() bool {
			driver.runCommand(driver.model.fetchActionOperationIfNeeded(), 0)
			state := driver.model.jobOperations.State()
			if state.ActionErr != nil && state.ActionStatus != "COMPLETED" {
				t.Fatalf("%s polling failed: %v", action, state.ActionErr)
			}
			return state.ActionStatus == "COMPLETED"
		})
		state := driver.model.jobOperations.State()
		if state.ActionErr == nil || state.ActionMessage != "" {
			t.Fatalf("%s failure was not surfaced: error=%v message=%q", action, state.ActionErr, state.ActionMessage)
		}
		driver.expectVisible("Action failed:")
		if strings.Contains(driver.screen(), "Operation completed successfully.") {
			t.Fatalf("%s failure was rendered as success", action)
		}
		t.Logf("%s: %v", action, state.ActionErr)
	}
}

func TestE2EMouseNavigationAgainstFlink(t *testing.T) {
	endpoint := os.Getenv("FLINK_TUI_E2E_ENDPOINT")
	if endpoint == "" {
		t.Skip("set FLINK_TUI_E2E_ENDPOINT or run make test-e2e")
	}

	driver := newE2EDriver(t, endpoint, "")
	driver.waitForPlayground()

	driver.clickJob("Flink TUI Checkpoints")
	if driver.model.mode != modeGraph || driver.model.snapshot.JobName != "Flink TUI Checkpoints" {
		t.Fatalf("job click opened mode %v and job %q", driver.model.mode, driver.model.snapshot.JobName)
	}
	driver.expectVisible("Flink TUI Checkpoints")

	firstVertex := driver.model.selected
	clicked := driver.clickVisibleVertexOtherThan(firstVertex)
	driver.expectVisible(clicked.Name)
	driver.clickSidebar("Timeline")
	if driver.model.mode != modeTimeline {
		t.Fatalf("Timeline click opened mode %v", driver.model.mode)
	}
	driver.expectVisible("VERTEX RANGE")
	driver.clickSidebar("Job Graph")

	driver.send("open selected vertex", tea.KeyPressMsg(tea.Key{Code: tea.KeyEnter}))
	diagnostics := driver.model.jobDetails.State()
	if driver.model.mode != modeSubtasks || diagnostics.DiagnosticsErr != nil || len(diagnostics.Diagnostics.Subtasks) == 0 {
		t.Fatalf("Subtasks click produced mode=%v rows=%d err=%v",
			driver.model.mode, len(diagnostics.Diagnostics.Subtasks), diagnostics.DiagnosticsErr)
	}
	driver.eventually("assigned subtask for thread dump", 45*time.Second, func() bool {
		driver.runCommand(driver.model.fetchDiagnostics(), 0)
		diagnostics = driver.model.jobDetails.State()
		for _, subtask := range diagnostics.Diagnostics.Subtasks {
			if subtask.TaskManagerID != "" && subtask.TaskManagerID != "(unassigned)" {
				return true
			}
		}
		return false
	})
	driver.expectVisible("SUBTASKS")
	driver.clickContent("[dump]")
	processState := driver.model.processDiagnostics.State()
	if driver.model.mode != modeThreadDump || processState.ThreadErr != nil {
		t.Fatalf("subtask dump click produced mode=%v err=%v", driver.model.mode, processState.ThreadErr)
	}
	if !processState.ThreadFocus.Matched {
		t.Fatalf("subtask execution thread %q was not found in TaskManager dump", processState.ThreadFocus.Prefix)
	}
	thread, ok := driver.model.selectedThread()
	if !ok || !strings.Contains(thread.Name, processState.ThreadFocus.Prefix) {
		t.Fatalf("focused thread = %#v, want prefix %q", thread, processState.ThreadFocus.Prefix)
	}
	driver.expectVisible("focused execution thread")
	driver.send("return from subtask thread dump", tea.KeyPressMsg(tea.Key{Code: tea.KeyEscape}))
	if driver.model.mode != modeSubtasks {
		t.Fatalf("thread dump returned to mode %v, want subtasks", driver.model.mode)
	}

	driver.eventually("checkpoint history", 45*time.Second, func() bool {
		driver.runCommand(driver.model.fetchSnapshot(), 0)
		return len(driver.model.snapshot.Checkpoints.History) > 0
	})
	driver.clickSidebar("Checkpoints")
	if driver.model.mode != modeCheckpoints {
		t.Fatalf("Checkpoints click opened mode %v", driver.model.mode)
	}
	driver.clickContent("Summary")
	checkpointState := driver.model.checkpoints.State()
	if checkpointState.Page != checkpointmodule.PageStatistics {
		t.Fatalf("Summary click left checkpoint page at %v", checkpointState.Page)
	}
	driver.expectVisible("P99.9")

	driver.clickSidebar("Task Managers")
	infrastructureState := driver.model.infrastructure.State()
	if driver.model.mode != modeTaskManagers || infrastructureState.Err != nil || len(infrastructureState.Infrastructure.TaskManagers) == 0 {
		t.Fatalf("Task Managers click produced mode=%v rows=%d err=%v",
			driver.model.mode, len(infrastructureState.Infrastructure.TaskManagers), infrastructureState.Err)
	}
	managerID := infrastructureState.Infrastructure.TaskManagers[0].ID
	managerPrefix := string([]rune(managerID)[:min(12, len([]rune(managerID)))])
	driver.clickContent(managerPrefix)
	if driver.model.mode != modeTaskManagerDetail {
		t.Fatalf("TaskManager row click opened mode %v", driver.model.mode)
	}
	driver.expectVisible("MEMORY MODEL  CONFIG -> LIVE")
	driver.expectVisible("FLINK MEMORY")
	manager, ok := driver.model.selectedTaskManager()
	if !ok || manager.NonHeapCommitted <= 0 || manager.NonHeapMax <= 0 ||
		manager.DirectCount <= 0 || manager.DirectMax <= 0 || len(manager.GarbageCollectors) < 2 {
		t.Fatalf("TaskManager runtime memory was not preserved: %#v", manager)
	}
	driver.expectVisible("direct buffers")
	driver.expectVisible(manager.GarbageCollectors[len(manager.GarbageCollectors)-1].Name)

	driver.clickContent("click this header to return")
	if driver.model.mode != modeTaskManagers {
		t.Fatalf("TaskManager back-header click opened mode %v", driver.model.mode)
	}
	driver.clickSidebar("Overview")
	if driver.model.mode != modeJobs {
		t.Fatalf("Overview click opened mode %v", driver.model.mode)
	}
	driver.expectVisible("Flink TUI Checkpoints")
}

func TestE2ENavigationContractAgainstFlink(t *testing.T) {
	endpoint := os.Getenv("FLINK_TUI_E2E_ENDPOINT")
	if endpoint == "" {
		t.Skip("set FLINK_TUI_E2E_ENDPOINT or run make test-e2e")
	}

	driver := newE2EDriver(t, endpoint, "")
	driver.waitForPlayground()
	driver.send("resize to an 80-column terminal", tea.WindowSizeMsg{Width: 80, Height: 24})
	lines := strings.Split(driver.screen(), "\n")
	if len(lines) != 24 || !strings.Contains(lines[22], ": go") ||
		!strings.Contains(lines[22], "^G sidebar") || !strings.Contains(lines[23], "left nav") {
		t.Fatalf("80-column footer does not preserve global and local rows:\n%s", driver.screen())
	}

	driver.send("focus navigation", tea.KeyPressMsg(tea.Key{Code: 'n', Mod: tea.ModCtrl}))
	if !driver.model.navigation.Focused() || !driver.model.navigationVisibleAt(driver.model.width) {
		t.Fatalf("ctrl+n produced focused=%t visible=%t", driver.model.navigation.Focused(), driver.model.navigationVisibleAt(driver.model.width))
	}
	driver.send("return focus to content", tea.KeyPressMsg(tea.Key{Code: 'n', Mod: tea.ModCtrl}))
	if driver.model.navigation.Focused() || !driver.model.navigationVisibleAt(driver.model.width) {
		t.Fatalf("second ctrl+n produced focused=%t visible=%t", driver.model.navigation.Focused(), driver.model.navigationVisibleAt(driver.model.width))
	}
	driver.send("hide navigation", tea.KeyPressMsg(tea.Key{Code: 'g', Mod: tea.ModCtrl}))
	if driver.model.navigationVisibleAt(driver.model.width) {
		t.Fatal("ctrl+g did not hide navigation")
	}
	driver.send("show navigation", tea.KeyPressMsg(tea.Key{Code: 'g', Mod: tea.ModCtrl}))
	if !driver.model.navigationVisibleAt(driver.model.width) || driver.model.navigation.Focused() {
		t.Fatalf("second ctrl+g produced focused=%t visible=%t", driver.model.navigation.Focused(), driver.model.navigationVisibleAt(driver.model.width))
	}

	driver.send("open palette", tea.KeyPressMsg(tea.Key{Code: ':', Text: ":"}))
	for _, character := range "flame" {
		driver.send("type palette query", tea.KeyPressMsg(tea.Key{Code: character, Text: string(character)}))
	}
	filtered := driver.model.filteredPaletteCommands()
	if len(filtered) < 2 || filtered[0].ID == commandFlameGraph || !strings.Contains(filtered[0].Label, "Flame Lab") {
		t.Fatalf("flame palette ranking = %#v", filtered)
	}
	driver.send("open Flame Lab job", tea.KeyPressMsg(tea.Key{Code: tea.KeyEnter}))
	if driver.model.mode != modeGraph || driver.model.snapshot.JobName != "Flink TUI Flame Lab" {
		t.Fatalf("palette opened mode=%d job=%q", driver.model.mode, driver.model.snapshot.JobName)
	}
	selected, ok := driver.model.selectedNode()
	if !ok {
		t.Fatal("Flame Lab graph has no selected vertex")
	}
	driver.send("open object-and-destination palette", tea.KeyPressMsg(tea.Key{Code: ':', Text: ":"}))
	for _, character := range selected.Name + " st" {
		driver.send("type vertex and destination", tea.KeyPressMsg(tea.Key{Code: character, Text: string(character)}))
	}
	filtered = driver.model.filteredPaletteCommands()
	if len(filtered) == 0 || filtered[0].Label != "Subtasks · "+selected.Name || filtered[0].Unavailable != "" {
		t.Fatalf("vertex + subtasks palette result = %#v", filtered)
	}
	driver.send("open selected vertex subtasks", tea.KeyPressMsg(tea.Key{Code: tea.KeyEnter}))
	if driver.model.mode != modeSubtasks || driver.model.selected != selected.ID {
		t.Fatalf("composite palette opened mode=%d vertex=%q", driver.model.mode, driver.model.selected)
	}
	driver.send("scope up to graph", tea.KeyPressMsg(tea.Key{Code: tea.KeyBackspace}))
	if driver.model.mode != modeGraph || driver.model.selected != selected.ID {
		t.Fatalf("scope up opened mode=%d vertex=%q", driver.model.mode, driver.model.selected)
	}
	driver.send("press removed digit", tea.KeyPressMsg(tea.Key{Code: '5', Text: "5"}))
	if driver.model.mode != modeGraph {
		t.Fatalf("removed digit 5 opened mode %d", driver.model.mode)
	}

	driver.send("open palette", tea.KeyPressMsg(tea.Key{Code: ':', Text: ":"}))
	for _, character := range "cp" {
		driver.send("type exact checkpoint alias", tea.KeyPressMsg(tea.Key{Code: character, Text: string(character)}))
	}
	filtered = driver.model.filteredPaletteCommands()
	if len(filtered) == 0 || filtered[0].ID != commandCheckpoints {
		t.Fatalf("cp palette ranking = %#v", filtered)
	}
	driver.send("open checkpoints", tea.KeyPressMsg(tea.Key{Code: tea.KeyEnter}))
	if driver.model.mode != modeCheckpoints {
		t.Fatalf("cp opened mode %d", driver.model.mode)
	}
	driver.send("return home", tea.KeyPressMsg(tea.Key{Code: '1', Text: "1"}))
	driver.clickJob("Flink TUI Demo")
	if driver.model.mode != modeCheckpoints || driver.model.snapshot.JobName != "Flink TUI Demo" {
		t.Fatalf("cross-job continuation opened mode=%d job=%q", driver.model.mode, driver.model.snapshot.JobName)
	}

	driver.runCommand(driver.model.fetchInfrastructure(), 0)
	managers := driver.model.infrastructure.State().Infrastructure.TaskManagers
	if len(managers) == 0 {
		t.Fatal("Flink returned no TaskManagers")
	}
	managerID := managers[0].ID
	managerPrefix := string([]rune(managerID)[:min(12, len([]rune(managerID)))])
	driver.send("open palette", tea.KeyPressMsg(tea.Key{Code: ':', Text: ":"}))
	for _, character := range managerPrefix {
		driver.send("type TaskManager ID prefix", tea.KeyPressMsg(tea.Key{Code: character, Text: string(character)}))
	}
	filtered = driver.model.filteredPaletteCommands()
	if len(filtered) == 0 || filtered[0].ID != commandID(commandTaskManagerPrefix+managerID) {
		t.Fatalf("TaskManager palette ranking = %#v", filtered)
	}
	driver.send("open TaskManager object", tea.KeyPressMsg(tea.Key{Code: tea.KeyEnter}))
	manager, ok := driver.model.selectedTaskManager()
	if driver.model.mode != modeTaskManagerDetail || !ok || manager.ID != managerID {
		t.Fatalf("TaskManager object opened mode=%d manager=%#v found=%t", driver.model.mode, manager, ok)
	}
}

func TestE2EAutoFitsTopologyLabOnOpen(t *testing.T) {
	endpoint := os.Getenv("FLINK_TUI_E2E_ENDPOINT")
	if endpoint == "" {
		t.Skip("set FLINK_TUI_E2E_ENDPOINT or run make test-e2e")
	}

	driver := newE2EDriver(t, endpoint, "")
	driver.waitForPlayground()
	driver.eventually("running topology lab", 2*time.Minute, func() bool {
		driver.runCommand(driver.model.fetchJobs(), 0)
		return driver.hasRunningJob(topologyLabJobName)
	})
	driver.clickJob(topologyLabJobName)

	if len(driver.model.snapshot.Nodes) != 35 {
		t.Fatalf("topology lab opened with %d vertices, want 35", len(driver.model.snapshot.Nodes))
	}
	viewport := driver.model.graphViewport.State()
	if viewport.AutoFitPending {
		t.Fatal("topology lab auto-fit remained pending after opening")
	}
	expected := graphZoomTopology
	region := driver.graphRegion()
	graphWidth, graphHeight := region.maxX-region.minX, region.maxY-region.minY
	for _, candidate := range []graphZoom{graphZoomDetailed, graphZoomCompact, graphZoomTopology} {
		layout := jobgraph.BuildLayoutAt(driver.model.snapshot.Nodes, candidate)
		if layout.Width <= graphWidth && layout.Height <= graphHeight {
			expected = candidate
			break
		}
	}
	if viewport.Zoom != expected {
		t.Fatalf("topology lab opened at zoom %v, want most detailed fitting zoom %v",
			viewport.Zoom, expected)
	}
	if driver.model.layout.Width > graphWidth || driver.model.layout.Height > graphHeight {
		t.Fatalf("auto-fit layout %dx%d exceeds viewport %dx%d",
			driver.model.layout.Width, driver.model.layout.Height,
			graphWidth, graphHeight)
	}

	firstVertex := driver.model.selected
	clicked := driver.clickVisibleVertexOtherThan(firstVertex)
	driver.expectVisible(clicked.Name)
}

func TestE2EExceptionIncidentsAgainstFlink(t *testing.T) {
	endpoint := os.Getenv("FLINK_TUI_E2E_ENDPOINT")
	if endpoint == "" {
		t.Skip("set FLINK_TUI_E2E_ENDPOINT or run against a cluster with retained exceptions")
	}

	driver := newE2EDriver(t, endpoint, "")
	driver.waitForPlayground()
	fixtureJob := os.Getenv("FLINK_TUI_E2E_EXCEPTION_JOB")
	if fixtureJob != "" {
		driver.eventually("running exception fixture", time.Minute, func() bool {
			driver.runCommand(driver.model.fetchJobs(), 0)
			return driver.hasRunningJob(fixtureJob)
		})
		driver.clickJob(fixtureJob)
		driver.eventually("retained incident and recovered execution", time.Minute, func() bool {
			driver.runCommand(driver.model.fetchSnapshot(), 0)
			if len(driver.model.snapshot.Exceptions.Entries) == 0 || len(driver.model.snapshot.Nodes) == 0 {
				return false
			}
			for _, node := range driver.model.snapshot.Nodes {
				if node.State != "RUNNING" {
					return false
				}
			}
			return true
		})
	} else {
		driver.clickJob("Flink TUI Demo")
	}
	if len(driver.model.snapshot.Exceptions.Entries) == 0 {
		t.Skip("cluster has no retained exception incidents")
	}
	driver.runCommand(driver.model.fetchInfrastructure(), 0)
	registered := make(map[string]bool)
	for _, manager := range driver.model.infrastructure.State().Infrastructure.TaskManagers {
		registered[manager.ID] = true
	}

	driver.clickSidebar("Exceptions")
	incidentIndex, variantIndex := -1, -1
	var selected flink.JobException
	for rootIndex, incident := range driver.model.filteredExceptionIncidents() {
		for failureIndex, failure := range exceptions.Variants(incident) {
			if failure.TaskName != "" && registered[failure.TaskManagerID] {
				incidentIndex, variantIndex, selected = rootIndex, failureIndex, failure
				break
			}
		}
		if incidentIndex >= 0 {
			break
		}
	}
	if incidentIndex < 0 {
		if fixtureJob != "" {
			t.Fatal("exception fixture has no retained task failure on a registered TaskManager")
		}
		t.Skip("retained exceptions do not reference a currently registered TaskManager")
	}
	if fixtureJob == "Flink TUI Exception Lab" && !strings.Contains(selected.Stacktrace, "Intentional Flink TUI E2E incident") {
		t.Fatalf("fixture produced an unexpected task failure: %s", selected.Stacktrace)
	}
	exceptionState := driver.model.exceptions.State()
	exceptionState.Cursor, exceptionState.VariantCursor = incidentIndex, variantIndex
	driver.model.exceptions.RestoreState(exceptionState)
	driver.expectVisible(exceptions.TaskDisplay(selected.TaskName))
	driver.expectVisible(selected.TaskManagerID)

	driver.clickContent("[enter vertex]")
	if driver.model.mode != modeGraph {
		t.Fatalf("exception graph action opened mode %v", driver.model.mode)
	}
	if node, found := driver.model.nodeByID(driver.model.selected); !found || exceptions.NormalizeOperator(node.Name) != exceptions.NormalizeOperator(selected.TaskName) {
		t.Fatalf("exception graph action selected node %#v for task %q", node, selected.TaskName)
	}

	driver.clickSidebar("Exceptions")
	driver.clickContent("[d thread dump]")
	processState := driver.model.processDiagnostics.State()
	if driver.model.mode != modeThreadDump || processState.ThreadErr != nil || processState.Process.TaskManagerID != selected.TaskManagerID {
		t.Fatalf("exception thread action = mode %v process %#v err %v", driver.model.mode, processState.Process, processState.ThreadErr)
	}
	thread, focused := driver.model.selectedThread()
	if !focused || !processState.ThreadFocus.Matched || !strings.Contains(thread.Name, processState.ThreadFocus.Prefix) {
		t.Fatalf("exception thread focus = thread %#v selected %t focus %#v", thread, focused, processState.ThreadFocus)
	}
	driver.send("return from exception thread dump", tea.KeyPressMsg(tea.Key{Code: tea.KeyEscape}))

	driver.clickContent("[T task manager]")
	manager, ok := driver.model.selectedTaskManager()
	if driver.model.mode != modeTaskManagerDetail || !ok || manager.ID != selected.TaskManagerID {
		t.Fatalf("exception TaskManager action = mode %v manager %#v ok %t", driver.model.mode, manager, ok)
	}
	driver.clickContent("click this header to return")
	if driver.model.mode != modeExceptions {
		t.Fatalf("TaskManager detail returned to mode %v, want exceptions", driver.model.mode)
	}

	driver.clickContent("[l current log]")
	processState = driver.model.processDiagnostics.State()
	if driver.model.mode != modeDocument || processState.DocumentSource != processmodule.SourceCurrentLog ||
		processState.DocumentErr != nil || len(processState.DocumentLines) == 0 {
		t.Fatalf("exception current log action = mode %v lines %d err %v", driver.model.mode, len(processState.DocumentLines), processState.DocumentErr)
	}
	driver.send("return from exception logs", tea.KeyPressMsg(tea.Key{Code: tea.KeyEscape}))
	if driver.model.mode != modeExceptions {
		t.Fatalf("logs returned to mode %v, want exceptions", driver.model.mode)
	}
}

func TestE2ESQLWorkbenchMouseAndQuery(t *testing.T) {
	endpoint := os.Getenv("FLINK_TUI_E2E_ENDPOINT")
	if endpoint == "" {
		t.Skip("set FLINK_TUI_E2E_ENDPOINT or run make test-e2e")
	}
	sqlEndpoint := os.Getenv("FLINK_TUI_E2E_SQL_ENDPOINT")
	if sqlEndpoint == "" {
		t.Skip("set FLINK_TUI_E2E_SQL_ENDPOINT or run make test-e2e")
	}

	driver := newE2EDriver(t, endpoint, sqlEndpoint)
	driver.waitForPlayground()
	driver.waitForSQLGateway()

	driver.clickSidebar("Workbench")
	state := driver.model.sql.State()
	if driver.model.mode != modeSQL || state.Session == "" || state.Err != nil {
		t.Fatalf("Workbench click produced mode=%v session=%q err=%v",
			driver.model.mode, state.Session, state.Err)
	}
	driver.expectVisible("SQL WORKBENCH")

	driver.clickContent("RESULTS")
	if focus := driver.model.sql.State().Focus; focus != sqlworkbench.FocusResults {
		t.Fatalf("results click left SQL focus at %v", focus)
	}
	driver.clickContent("SELECT 1 AS answer;")
	if focus := driver.model.sql.State().Focus; focus != sqlworkbench.FocusEditor {
		t.Fatalf("editor click left SQL focus at %v", focus)
	}

	driver.pressCtrlEnter()
	state = driver.model.sql.State()
	if state.Err != nil {
		t.Fatalf("SELECT 1 failed: %v", state.Err)
	}
	if len(state.Rows) == 0 || !strings.Contains(strings.Join(state.Rows[0].Fields, " "), "1") {
		t.Fatalf("SELECT 1 returned %#v", state.Rows)
	}
	driver.expectVisible("+I")
	driver.clickContent("+I")
	if focus := driver.model.sql.State().Focus; focus != sqlworkbench.FocusResults {
		t.Fatalf("result-row click left SQL focus at %v", focus)
	}
}
