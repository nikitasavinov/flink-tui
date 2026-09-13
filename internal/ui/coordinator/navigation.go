package coordinator

import (
	"fmt"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/nikitasavinov/flink-tui/internal/flink"
	checkpointmodule "github.com/nikitasavinov/flink-tui/internal/ui/checkpoints"
	processmodule "github.com/nikitasavinov/flink-tui/internal/ui/processdiag"
	"github.com/nikitasavinov/flink-tui/internal/ui/shared"
	shellmodule "github.com/nikitasavinov/flink-tui/internal/ui/shell"
)

const (
	wideNavigationThreshold = shellmodule.WideNavigationThreshold
	fullNavigationWidth     = shellmodule.FullNavigationWidth
)

type navigationDisplay = shellmodule.Display

const (
	navigationAuto   = shellmodule.DisplayAuto
	navigationShown  = shellmodule.DisplayShown
	navigationHidden = shellmodule.DisplayHidden
)

type navigationTarget = shellmodule.Target

const (
	navigationNone                = shellmodule.TargetNone
	navigationOverview            = shellmodule.TargetOverview
	navigationGraph               = shellmodule.TargetGraph
	navigationSubtasks            = shellmodule.TargetSubtasks
	navigationFlameGraph          = shellmodule.TargetFlameGraph
	navigationCheckpoints         = shellmodule.TargetCheckpoints
	navigationTimeline            = shellmodule.TargetTimeline
	navigationDiagnostics         = shellmodule.TargetDiagnostics
	navigationMetrics             = shellmodule.TargetMetrics
	navigationAccumulators        = shellmodule.TargetAccumulators
	navigationExceptions          = shellmodule.TargetExceptions
	navigationJobConfig           = shellmodule.TargetJobConfig
	navigationActions             = shellmodule.TargetActions
	navigationTaskManagers        = shellmodule.TargetTaskManagers
	navigationJobManager          = shellmodule.TargetJobManager
	navigationSQL                 = shellmodule.TargetSQL
	navigationCheckpointOperators = shellmodule.TargetCheckpointOperators
	navigationCheckpointSubtasks  = shellmodule.TargetCheckpointSubtasks
	navigationTaskManagerDetail   = shellmodule.TargetTaskManagerDetail
	navigationProcessLogs         = shellmodule.TargetProcessLogs
	navigationDocument            = shellmodule.TargetDocument
	navigationThreadDump          = shellmodule.TargetThreadDump
	navigationProfiler            = shellmodule.TargetProfiler
	navigationProfilerFlameGraph  = shellmodule.TargetProfilerFlameGraph
)

type navigationRow = shellmodule.Row
type commandID = shellmodule.CommandID

const (
	commandOverview          commandID = "overview"
	commandGraph             commandID = "graph"
	commandSubtasks          commandID = "subtasks"
	commandFlameGraph        commandID = "flame-graph"
	commandCheckpoints       commandID = "checkpoints"
	commandTimeline          commandID = "timeline"
	commandBackpressure      commandID = "backpressure"
	commandSkew              commandID = "skew"
	commandMetrics           commandID = "metrics"
	commandAccumulators      commandID = "accumulators"
	commandExceptions        commandID = "exceptions"
	commandJobConfig         commandID = "job-config"
	commandActions           commandID = "actions"
	commandTaskManagers      commandID = "task-managers"
	commandJobManager        commandID = "job-manager"
	commandProcessLogs       commandID = "process-logs"
	commandThreadDump        commandID = "thread-dump"
	commandProfiler          commandID = "profiler"
	commandSQL               commandID = "sql"
	commandRefresh           commandID = "refresh"
	commandTogglePause       commandID = "toggle-pause"
	commandToggleMouse       commandID = "toggle-mouse"
	commandFocusNavigation   commandID = "toggle-navigation"
	commandToggleNavVisible  commandID = "toggle-navigation-visibility"
	commandToggleMap         commandID = "toggle-map"
	commandJobPrefix                   = "job:"
	commandVertexPrefix                = "vertex:"
	commandTaskManagerPrefix           = "taskmanager:"
)

type paletteCommand = shellmodule.Command

func (m Model) navigationVisibleAt(width int) bool { return m.navigation.VisibleAt(width) }

func (m Model) navigationOnlyAt(width int) bool { return m.navigation.OnlyAt(width) }

func (m Model) navigationWidthAt(width int) int { return m.navigation.WidthAt(width) }

func (m Model) contentWidthAt(width int) int { return m.navigation.ContentWidthAt(width) }

func (m Model) contentStartX() int { return m.navigation.ContentStartX(max(40, m.width)) }

func (m Model) graphWidth() int {
	width := m.width
	if width <= 0 {
		width = 100
	}
	return max(1, m.contentWidthAt(width))
}

func (m *Model) toggleNavigationFocus() {
	m.navigation.ToggleFocus(max(40, m.width), m.navigationRows())
	if m.mode == modeGraph {
		m.centerSelection()
	}
}

func (m *Model) toggleNavigationVisibility() {
	m.navigation.ToggleVisibility(max(40, m.width), m.navigationRows())
	if m.mode == modeGraph {
		m.centerSelection()
	}
}

func (m Model) locksNavigationChrome() bool {
	return m.mode == modeActions && m.jobOperations.CapturesKeys()
}

func (m Model) jobContextAvailable() bool {
	_, ok := m.navigationJobScope()
	return ok
}

func (m Model) vertexContextAvailable() bool {
	if m.snapshot.JobID != "" {
		return m.selected != ""
	}
	return m.jobContextAvailable()
}

func (m Model) navigationRows() []navigationRow {
	active := m.activeNavigationTarget()
	jobGroup := "JOB"
	if job, ok := m.navigationJobScope(); ok && m.showsJobNavigationContext() {
		jobGroup += "  " + shared.Fallback(job.Name, job.ID)
	}
	rows := []navigationRow{
		{Label: "NAVIGATION", Group: true, Enabled: true},
		m.destinationRow(navigationOverview, 0),
		{Label: jobGroup, Group: true, Enabled: true},
		m.destinationRow(navigationGraph, 0),
	}
	if m.graphBranchExpanded() {
		rows = append(rows,
			m.destinationRow(navigationSubtasks, 1),
			m.destinationRow(navigationFlameGraph, 1),
			m.destinationRow(navigationMetrics, 1),
			m.destinationRow(navigationAccumulators, 1),
		)
		if active == navigationDocument {
			rows = append(rows, m.destinationRow(navigationDocument, 2))
		}
	}
	rows = append(rows, m.destinationRow(navigationCheckpoints, 0))
	switch active {
	case navigationCheckpointOperators:
		rows = append(rows, m.destinationRow(navigationCheckpointOperators, 1))
	case navigationCheckpointSubtasks:
		rows = append(rows,
			m.destinationRow(navigationCheckpointOperators, 1),
			m.destinationRow(navigationCheckpointSubtasks, 2),
		)
	}
	rows = append(rows,
		m.destinationRow(navigationTimeline, 0),
		m.destinationRow(navigationDiagnostics, 0),
		m.destinationRow(navigationExceptions, 0),
		m.destinationRow(navigationJobConfig, 0),
		m.destinationRow(navigationActions, 0),
		navigationRow{Label: "INFRASTRUCTURE", Group: true, Enabled: true},
		m.destinationRow(navigationTaskManagers, 0),
	)
	if active == navigationTaskManagerDetail {
		rows = append(rows, m.destinationRow(navigationTaskManagerDetail, 1))
	}
	if m.processBranchExpanded(flink.ProcessTaskManager) {
		rows = append(rows, m.processNavigationRows()...)
	}
	rows = append(rows, m.destinationRow(navigationJobManager, 0))
	if m.processBranchExpanded(flink.ProcessJobManager) {
		rows = append(rows, m.processNavigationRows()...)
	}
	rows = append(rows, m.destinationRow(navigationSQL, 0))
	return rows
}

func (m Model) processBranchExpanded(kind flink.ProcessKind) bool {
	active := m.activeNavigationTarget()
	if kind == flink.ProcessTaskManager && (active == navigationTaskManagers || active == navigationTaskManagerDetail) {
		return true
	}
	if kind == flink.ProcessJobManager && active == navigationJobManager {
		return true
	}
	switch active {
	case navigationProcessLogs, navigationThreadDump, navigationProfiler, navigationProfilerFlameGraph:
		return m.activeProcess().Kind == kind
	case navigationDocument:
		return documentUsesInfrastructure(m) && m.activeProcess().Kind == kind
	default:
		return false
	}
}

func (m Model) processNavigationRows() []navigationRow {
	rows := []navigationRow{
		m.destinationRow(navigationProcessLogs, 1),
		m.destinationRow(navigationThreadDump, 1),
		m.destinationRow(navigationProfiler, 1),
	}
	switch m.activeNavigationTarget() {
	case navigationDocument:
		rows = append(rows, m.destinationRow(navigationDocument, 2))
	case navigationProfilerFlameGraph:
		rows = append(rows, m.destinationRow(navigationProfilerFlameGraph, 2))
	}
	return rows
}

func (m Model) navigationProcess() (flink.ProcessRef, bool) {
	switch m.mode {
	case modeTaskManagers, modeTaskManagerDetail:
		manager, ok := m.selectedTaskManager()
		if !ok {
			return flink.ProcessRef{}, false
		}
		return flink.TaskManagerProcess(manager.ID), true
	case modeJobManager:
		return flink.JobManagerProcess(), true
	case modeExceptions:
		failure := m.exceptions.State().SelectedFailure
		if strings.TrimSpace(failure.TaskManagerID) != "" {
			return flink.TaskManagerProcess(failure.TaskManagerID), true
		}
	case modeSubtasks:
		state := m.jobDetails.State()
		for _, subtask := range state.Diagnostics.Subtasks {
			if subtask.Index == state.SelectedSubtask && strings.TrimSpace(subtask.TaskManagerID) != "" {
				return flink.TaskManagerProcess(subtask.TaskManagerID), true
			}
		}
	case modeProcessLogs, modeThreadDump, modeProfiler, modeProfilerFlameGraph:
		return m.activeProcess(), true
	case modeDocument:
		if documentUsesInfrastructure(m) {
			return m.activeProcess(), true
		}
	}
	return flink.ProcessRef{}, false
}

func (m Model) activeProcess() flink.ProcessRef {
	switch m.mode {
	case modeProfiler:
		return m.profiler.Process()
	case modeProfilerFlameGraph:
		return m.flameGraphs.State().Process
	case modeDocument:
		state := m.processDiagnostics.State()
		if state.DocumentSource == processmodule.SourceLog || state.DocumentSource == processmodule.SourceCurrentLog || state.DocumentSource == processmodule.SourceStdout {
			return state.DocumentProcess
		}
		return state.Process
	default:
		return m.processDiagnostics.State().Process
	}
}

func (m Model) navigationContext() shellmodule.Context {
	if m.activeScope() == scopeProcess {
		process := m.activeProcess()
		if process.Kind == flink.ProcessTaskManager {
			context := shellmodule.Context{
				ProcessLabel:     "tm " + shared.TaskManagerIdentity(process.TaskManagerID),
				ShowProcessSlots: true,
			}
			for _, manager := range m.infrastructure.State().Infrastructure.TaskManagers {
				if manager.ID == process.TaskManagerID {
					context.ProcessSlots = manager.Slots
					context.HasProcessSlots = true
					break
				}
			}
			return context
		}
		return shellmodule.Context{ProcessLabel: "jm JobManager"}
	}
	if !m.showsJobNavigationContext() {
		return shellmodule.Context{}
	}
	job, ok := m.navigationJobScope()
	if !ok {
		return shellmodule.Context{}
	}
	context := shellmodule.Context{
		JobName:  shared.Fallback(job.Name, job.ID),
		JobState: job.State,
		Uptime:   shared.HumanDuration(job.Duration),
	}
	if job.ID != m.snapshot.JobID {
		return context
	}
	if node, ok := m.selectedNode(); ok {
		context.VertexName = node.Name
	}
	if summary := m.snapshot.Checkpoints; summary.Total > 0 {
		context.HasCheckpoint = true
		context.CheckpointID = summary.LatestID
		if len(summary.History) > 0 {
			context.CheckpointID = summary.History[0].ID
			context.CheckpointStatus = summary.History[0].Status
		} else if summary.InProgress > 0 {
			context.CheckpointStatus = "IN_PROGRESS"
		}
	}
	return context
}

func (m Model) showsJobNavigationContext() bool {
	switch m.activeScope() {
	case scopeOverview, scopeJob, scopeVertex:
		return true
	default:
		return false
	}
}

// navigationJobScope returns the object the job destinations currently act
// on. Overview follows its highlighted row; every other screen follows the
// loaded (or currently loading) job.
func (m Model) navigationJobScope() (flink.JobSummary, bool) {
	if m.mode == modeJobs {
		if job, ok := m.jobAtCursor(); ok {
			return job, true
		}
	}
	if m.snapshot.JobID != "" {
		return flink.JobSummary{
			ID: m.snapshot.JobID, Name: m.snapshot.JobName, State: m.snapshot.JobState,
			StartedAt: m.snapshot.StartedAt, Duration: m.snapshot.Duration,
		}, true
	}
	if m.preferredJobID != "" {
		for _, job := range m.jobList.State().Jobs {
			if job.ID == m.preferredJobID {
				return job, true
			}
		}
	}
	return flink.JobSummary{}, false
}

func (m Model) renderNavigation(width, height int) string {
	return m.navigation.Render(width, height, m.navigationRows(), m.navigationContext())
}

func (m *Model) handleNavigationMouseClick(event tea.Mouse) (bool, tea.Cmd) {
	width := max(40, m.width)
	bodyHeight := max(3, max(14, m.height)-headerHeight-footerHeightAt(width))
	result := m.navigation.HitTest(event, width, bodyHeight, headerHeight, m.navigationRows())
	if !result.Handled {
		return false, nil
	}
	if result.Target == navigationNone {
		return true, nil
	}
	if !result.Enabled {
		if reason := m.navigationUnavailableReason(result.Target); reason != "" {
			m.setNotice(reason)
		}
		return true, nil
	}
	return true, m.navigate(result.Target)
}

func (m *Model) focusNavigation() {
	m.navigation.Focus(max(40, m.width), m.navigationRows())
}

func (m *Model) handleFocusedNavigationKey(key string) tea.Cmd {
	result := m.navigation.HandleKey(key, m.navigationRows())
	switch result.Action {
	case shellmodule.KeyNavigate:
		return m.navigateFromFocusedNavigation(result.Target)
	case shellmodule.KeyUnavailable:
		if reason := m.navigationUnavailableReason(result.Target); reason != "" {
			m.setNotice(reason)
		}
	case shellmodule.KeyReturnToContent:
		m.returnNavigationToContent()
	}
	return nil
}

func (m *Model) handleBack() tea.Cmd {
	return m.handleScreenBack()
}

func (m *Model) navigateFromFocusedNavigation(target navigationTarget) tea.Cmd {
	// Opening a destination is a jump, not a browse: the next arrow key must
	// act on that screen. Wide sidebars stay visible as location context;
	// narrower overlays collapse through DestinationOpened.
	return m.navigate(target)
}

func (m *Model) moveNavigationCursor(delta int) {
	m.navigation.Move(delta, m.navigationRows())
}

func (m *Model) moveNavigationCursorToEdge(last bool) {
	m.navigation.MoveToEdge(last, m.navigationRows())
}

func (m Model) navigationAliasMatches(prefix string) []string {
	matches := make([]string, 0, 3)
	for _, row := range m.navigationRows() {
		if row.Group || row.Alias == "" || !strings.HasPrefix(strings.ToLower(row.Alias), strings.ToLower(prefix)) {
			continue
		}
		matches = append(matches, row.Alias)
	}
	return matches
}

func (m *Model) returnNavigationToContent() {
	m.navigation.ReturnToContent(max(40, m.width))
	if m.mode == modeGraph {
		m.centerSelection()
	}
}

func (m *Model) navigate(target navigationTarget) tea.Cmd {
	return m.navigateRoute(target, true)
}

// navigateWithoutHistory finishes a deferred route that was already initiated
// by the operator. The intermediate graph used while a job loads was never a
// visible destination, so it must not become a second back-stack entry.
func (m *Model) navigateWithoutHistory(target navigationTarget) tea.Cmd {
	return m.navigateRoute(target, false)
}

func (m *Model) navigateRoute(target navigationTarget, recordOrigin bool) (command tea.Cmd) {
	previousMode := m.mode
	origin := m.routeCrumb()
	// A jump is still a place an operator came from. Recording it is what lets
	// a single back key walk an incident trail out the way it was walked in,
	// however the operator moved: sidebar, palette, a mnemonic, or a drill.
	// Overview is the exception because it is home, not a step.
	defer func() {
		if m.mode == previousMode {
			return
		}
		if target == navigationOverview {
			m.parkRouteHistory(origin)
		} else if recordOrigin {
			origin.jump = true
			m.history.Push(origin)
		}
		command = tea.Batch(command, tea.ClearScreen)
	}()
	if reason := m.navigationUnavailableReason(target); reason != "" {
		m.setNotice(reason)
		return nil
	}
	if m.mode == modeJobs && target != navigationOverview {
		m.clearRouteHistory()
	}
	if target == navigationOverview || target == navigationGraph || !jobScopedNavigation(target) {
		m.navigation.ClearDeferredTarget()
	}
	if jobScopedNavigation(target) && m.mode == modeJobs {
		if job, ok := m.jobAtCursor(); ok && job.ID != m.snapshot.JobID {
			if target != navigationGraph {
				m.navigation.DeferTarget(target)
				m.setNotice("Opening " + destination(target).label + " for " + shared.Fallback(job.Name, job.ID) + "…")
			} else {
				m.navigation.ClearDeferredTarget()
			}
			m.navigation.DestinationOpened(m.width)
			return m.switchToJob(job.ID)
		}
	}
	if jobScopedNavigation(target) && m.snapshot.JobID == "" && m.preferredJobID != "" {
		if target != navigationGraph {
			m.navigation.DeferTarget(target)
			m.setNotice("Loading the job, then opening " + destination(target).label + "…")
		}
		if m.loading {
			return nil
		}
		m.mode = modeGraph
		m.loading = true
		m.navigation.DestinationOpened(m.width)
		return m.fetchSnapshot()
	}
	if recordOrigin {
		m.notice = ""
		m.noticeUntil = time.Time{}
	}
	m.navigation.DestinationOpened(m.width)
	switch target {
	case navigationOverview:
		return m.openJobPicker()
	case navigationGraph:
		if m.snapshot.JobID == "" {
			if _, ok := m.jobAtCursor(); ok {
				return m.switchToSelectedJob()
			}
			if m.preferredJobID != "" {
				m.mode = modeGraph
				m.loading = true
				return m.fetchSnapshot()
			}
			return nil
		}
		m.mode = modeGraph
		m.centerSelection()
	case navigationSubtasks:
		return m.openDiagnostics()
	case navigationFlameGraph:
		return m.openFlameGraph()
	case navigationCheckpoints:
		m.openCheckpoints()
	case navigationTimeline:
		m.openTimeline()
	case navigationDiagnostics:
		m.openDiagnosticOverview(diagnosticBackpressure)
	case navigationMetrics:
		return m.openMetricExplorer()
	case navigationAccumulators:
		return m.openAccumulators()
	case navigationExceptions:
		m.openExceptions()
	case navigationJobConfig:
		return m.openJobConfiguration()
	case navigationActions:
		m.openActions()
	case navigationTaskManagers:
		return m.openTaskManagers()
	case navigationJobManager:
		return m.openJobManager()
	case navigationSQL:
		return m.openSQLWorkbench()
	case navigationCheckpointOperators:
		m.checkpoints.Activate(checkpointmodule.ViewOperators)
		m.syncCheckpointMode()
		return m.fetchCheckpointDetail()
	case navigationCheckpointSubtasks:
		m.checkpoints.Activate(checkpointmodule.ViewSubtasks)
		m.syncCheckpointMode()
		return m.fetchCheckpointSubtasks()
	case navigationTaskManagerDetail:
		if manager, ok := m.selectedTaskManager(); ok {
			return m.openTaskManagerByID(manager.ID, modeTaskManagers)
		}
	case navigationProcessLogs:
		if process, ok := m.navigationProcess(); ok {
			return m.openCurrentProcessLog(process, processNavigationBackMode(process, previousMode))
		}
	case navigationThreadDump:
		if process, ok := m.navigationProcess(); ok {
			return m.openThreadDump(process, processNavigationBackMode(process, previousMode))
		}
	case navigationProfiler:
		if process, ok := m.navigationProcess(); ok {
			return m.openProfiler(process, processNavigationBackMode(process, previousMode))
		}
	case navigationDocument, navigationProfilerFlameGraph:
		// Contextual identity rows point at the content that is already open.
		return nil
	}
	return nil
}

func processNavigationBackMode(process flink.ProcessRef, current screenMode) screenMode {
	switch current {
	case modeProcessLogs, modeDocument, modeThreadDump, modeProfiler, modeProfilerFlameGraph:
		if process.Kind == flink.ProcessTaskManager {
			return modeTaskManagerDetail
		}
		return modeJobManager
	default:
		return current
	}
}

func jobScopedNavigation(target navigationTarget) bool {
	switch target {
	case navigationGraph, navigationSubtasks, navigationFlameGraph, navigationCheckpoints,
		navigationTimeline, navigationDiagnostics, navigationMetrics, navigationAccumulators,
		navigationExceptions, navigationJobConfig, navigationActions:
		return true
	default:
		return false
	}
}

// jobPickerTargetForMode keeps comparison screens stable while the operator
// chooses another job. Vertex-scoped views cannot safely transfer because the
// selected vertex may not exist in the new job; checkpoint drill-downs
// deliberately normalize to their job-level history screen.
func jobPickerTargetForMode(mode screenMode) navigationTarget {
	switch mode {
	case modeCheckpoints, modeCheckpointOperators, modeCheckpointSubtasks:
		return navigationCheckpoints
	case modeDiagnostics:
		return navigationDiagnostics
	case modeTimeline:
		return navigationTimeline
	case modeExceptions:
		return navigationExceptions
	case modeJobConfig:
		return navigationJobConfig
	case modeActions:
		return navigationActions
	default:
		return navigationGraph
	}
}

func (m Model) navigationUnavailableReason(target navigationTarget) string {
	switch target {
	case navigationGraph:
		if !m.jobContextAvailable() {
			return "Select a job in Overview first."
		}
	case navigationSubtasks, navigationFlameGraph, navigationMetrics, navigationAccumulators:
		if !m.jobContextAvailable() {
			return "Select and open a job first (Overview, then Enter)."
		}
		if !m.vertexContextAvailable() {
			return "Select a job vertex first."
		}
	case navigationCheckpoints, navigationTimeline, navigationDiagnostics, navigationExceptions, navigationJobConfig, navigationActions:
		if !m.jobContextAvailable() {
			return "Select and open a job first (Overview, then Enter)."
		}
	case navigationCheckpointOperators:
		if m.checkpoints.State().DetailID == 0 {
			return "Open a checkpoint from Checkpoints first."
		}
	case navigationCheckpointSubtasks:
		if m.checkpoints.State().SubtaskVertex == "" {
			return "Open an operator from checkpoint details first."
		}
	case navigationTaskManagerDetail:
		if _, ok := m.selectedTaskManager(); !ok {
			return "Select a TaskManager first."
		}
	case navigationProcessLogs, navigationThreadDump, navigationProfiler:
		if _, ok := m.navigationProcess(); !ok {
			return "Select a JobManager or TaskManager first."
		}
	case navigationDocument:
		if m.mode != modeDocument {
			return "No document is currently open."
		}
	case navigationProfilerFlameGraph:
		if m.mode != modeProfilerFlameGraph {
			return "No profiler flame graph is currently open."
		}
	case navigationSQL:
		if !m.sql.Configured() {
			return "SQL Gateway is not configured."
		}
	}
	return ""
}

func (m *Model) handleGlobalNavigationKey(key string) (bool, tea.Cmd) {
	if key == "ctrl+o" && m.parked.active {
		return true, m.resumeParkedRoute()
	}
	if key == "<" {
		return true, m.stepPeer(-1)
	}
	if key == ">" {
		return true, m.stepPeer(1)
	}
	if key == "g" {
		switch m.mode {
		case modeDiagnostics, modeTimeline, modeExceptions, modeCheckpointOperators, modeCheckpointSubtasks, modeFlameGraph, modeProfilerFlameGraph:
			return true, m.activeScreen().key(m, tea.KeyPressMsg(tea.Key{Code: 'g', Text: "g"}))
		default:
			return true, m.navigate(navigationGraph)
		}
	}
	if key == "1" {
		return true, m.navigate(navigationOverview)
	}
	return false, nil
}

func (m Model) paletteCommands() []paletteCommand {
	commands := []paletteCommand{m.destinationCommand(navigationOverview)}
	if m.jobContextAvailable() {
		for _, target := range []navigationTarget{
			navigationGraph, navigationSubtasks, navigationFlameGraph, navigationCheckpoints,
			navigationTimeline, navigationDiagnostics,
		} {
			commands = append(commands, m.destinationCommand(target))
		}
		commands = append(commands, paletteCommand{ID: commandSkew, Label: "Open Data Skew Diagnostics", Description: "Compare vertex input-rate imbalance"})
		for _, target := range []navigationTarget{
			navigationMetrics, navigationAccumulators, navigationExceptions, navigationJobConfig, navigationActions,
		} {
			commands = append(commands, m.destinationCommand(target))
		}
	}
	commands = append(commands,
		m.destinationCommand(navigationTaskManagers),
		m.destinationCommand(navigationJobManager),
		m.destinationCommand(navigationProcessLogs),
		m.destinationCommand(navigationThreadDump),
		m.destinationCommand(navigationProfiler),
	)
	if m.sql.Configured() {
		commands = append(commands, m.destinationCommand(navigationSQL))
	}
	commands = append(commands, m.palettePairCommands()...)
	commands = append(commands, m.paletteObjectCommands()...)
	commands = append(commands,
		paletteCommand{ID: commandRefresh, Label: "Refresh Now", Shortcut: "r", Description: "Fetch the current view immediately"},
		paletteCommand{ID: commandTogglePause, Label: pauseCommandLabel(m.paused), Shortcut: "space", Description: "Pause or resume periodic refresh"},
		paletteCommand{ID: commandFocusNavigation, Label: "Focus / Unfocus Navigation", Shortcut: "ctrl+n", Description: "Move keyboard focus between the sidebar and content"},
		paletteCommand{ID: commandToggleNavVisible, Label: "Show / Hide Navigation", Shortcut: "ctrl+g", Description: "Toggle persistent sidebar visibility"},
		paletteCommand{ID: commandToggleMouse, Label: mouseCommandLabel(m.mouseEnabled), Shortcut: "m", Description: "Toggle terminal mouse capture"},
	)
	if m.mode == modeGraph && len(m.snapshot.Nodes) > 0 {
		commands = append(commands, paletteCommand{ID: commandToggleMap, Label: "Toggle Graph Map", Shortcut: "z", Description: "Show or hide the graph minimap"})
	}
	return commands
}

func pauseCommandLabel(paused bool) string {
	if paused {
		return "Resume Refresh"
	}
	return "Pause Refresh"
}

func mouseCommandLabel(enabled bool) string {
	if enabled {
		return "Disable Mouse Capture"
	}
	return "Enable Mouse Capture"
}

func (m Model) filteredPaletteCommands() []paletteCommand {
	return m.palette.Filter(m.paletteCommands())
}

func (m *Model) openPalette() {
	// A full-screen navigation overlay must relinquish the body as well as
	// keyboard focus, so closing the palette reveals the active content.
	m.returnNavigationToContent()
	m.palette.Show()
}

func (m *Model) handlePaletteKey(key string) tea.Cmd {
	id, selected := m.palette.HandleKey(key, m.paletteCommands())
	if !selected {
		return nil
	}
	return m.executePaletteCommand(id)
}

func (m *Model) executePaletteCommand(id commandID) tea.Cmd {
	m.palette.Close()
	switch id {
	case commandOverview:
		return m.navigate(navigationOverview)
	case commandGraph:
		return m.navigate(navigationGraph)
	case commandSubtasks:
		return m.navigate(navigationSubtasks)
	case commandFlameGraph:
		return m.navigate(navigationFlameGraph)
	case commandCheckpoints:
		return m.navigate(navigationCheckpoints)
	case commandTimeline:
		return m.navigate(navigationTimeline)
	case commandBackpressure:
		return m.navigate(navigationDiagnostics)
	case commandSkew:
		command := m.navigate(navigationDiagnostics)
		if m.mode == modeDiagnostics {
			m.openDiagnosticOverview(diagnosticSkew)
		}
		return command
	case commandMetrics:
		return m.navigate(navigationMetrics)
	case commandAccumulators:
		return m.navigate(navigationAccumulators)
	case commandExceptions:
		return m.navigate(navigationExceptions)
	case commandJobConfig:
		return m.navigate(navigationJobConfig)
	case commandActions:
		return m.navigate(navigationActions)
	case commandTaskManagers:
		return m.navigate(navigationTaskManagers)
	case commandJobManager:
		return m.navigate(navigationJobManager)
	case commandProcessLogs:
		return m.navigate(navigationProcessLogs)
	case commandThreadDump:
		return m.navigate(navigationThreadDump)
	case commandProfiler:
		return m.navigate(navigationProfiler)
	case commandSQL:
		return m.navigate(navigationSQL)
	case commandRefresh:
		return m.refreshCurrent()
	case commandTogglePause:
		m.togglePause()
	case commandToggleMouse:
		m.mouseEnabled = !m.mouseEnabled
		m.graphViewport.CancelDrag()
	case commandFocusNavigation:
		m.toggleNavigationFocus()
	case commandToggleNavVisible:
		m.toggleNavigationVisibility()
	case commandToggleMap:
		m.toggleMinimap()
	default:
		value := string(id)
		if strings.HasPrefix(value, commandPairPrefix) {
			pair, ok := m.parsePalettePair(id)
			if !ok {
				m.setNotice("That palette result is no longer available.")
				return nil
			}
			return m.executePalettePair(pair)
		}
		if strings.HasPrefix(value, commandJobPrefix) {
			return m.jumpToPaletteJob(strings.TrimPrefix(value, commandJobPrefix))
		}
		if strings.HasPrefix(value, commandVertexPrefix) {
			return m.jumpToPaletteVertex(strings.TrimPrefix(value, commandVertexPrefix))
		}
		if strings.HasPrefix(value, commandTaskManagerPrefix) {
			return m.jumpToPaletteTaskManager(strings.TrimPrefix(value, commandTaskManagerPrefix))
		}
	}
	return nil
}

func (m Model) paletteObjectCommands() []paletteCommand {
	const objectSearchPriority = 100

	jobs := m.jobList.State().Jobs
	taskManagers := m.infrastructure.State().Infrastructure.TaskManagers
	commands := make([]paletteCommand, 0, len(jobs)+len(m.snapshot.Nodes)+len(taskManagers))
	for _, job := range jobs {
		label := shared.Fallback(job.Name, job.ID)
		description := fmt.Sprintf("job · %s · %d/%d tasks", shared.Fallback(job.State, "UNKNOWN"), job.RunningTasks, job.TotalTasks)
		commands = append(commands, paletteCommand{
			ID: commandID(commandJobPrefix + job.ID), Label: label, Description: description,
			Aliases: []string{"job", job.ID}, SearchPriority: objectSearchPriority,
		})
	}
	for _, node := range m.snapshot.Nodes {
		description := "vertex · " + shared.Fallback(m.snapshot.JobName, "selected job")
		if node.Metrics.BackpressurePercent > 0 || node.Metrics.BackpressureLevel != "" {
			description += fmt.Sprintf(" · BP %s %.1f%%", shared.Fallback(node.Metrics.BackpressureLevel, "OBSERVED"), node.Metrics.BackpressurePercent)
		}
		if node.Metrics.BusyPercent > 0 {
			description += fmt.Sprintf(" · busy %.1f%%", node.Metrics.BusyPercent)
		}
		commands = append(commands, paletteCommand{
			ID: commandID(commandVertexPrefix + node.ID), Label: node.Name, Description: description,
			Aliases: []string{"vertex", node.ID}, SearchPriority: objectSearchPriority,
		})
	}
	for _, manager := range taskManagers {
		description := fmt.Sprintf("taskmanager · %d slots · %d free", manager.Slots, manager.FreeSlots)
		commands = append(commands, paletteCommand{
			ID: commandID(commandTaskManagerPrefix + manager.ID), Label: shared.TaskManagerIdentity(manager.ID), Description: description,
			// "tm" stays out: it is the Task Managers destination alias, and a
			// worker that claims it makes the list unreachable from the palette.
			Aliases: []string{"taskmanager", manager.Path}, SearchPriority: objectSearchPriority,
		})
	}
	return commands
}

func (m *Model) jumpToPaletteJob(jobID string) tea.Cmd {
	if jobID == "" {
		return nil
	}
	if jobID == m.snapshot.JobID {
		m.clearRouteHistory()
		return nil
	}
	target := m.paletteJobContinuation()
	if target == navigationGraph {
		m.navigation.ClearDeferredTarget()
	} else {
		m.navigation.DeferTarget(target)
	}
	return m.switchToJob(jobID)
}

func (m Model) paletteJobContinuation() navigationTarget {
	switch m.mode {
	case modeSubtasks:
		return navigationSubtasks
	case modeFlameGraph:
		return navigationFlameGraph
	case modeCheckpoints, modeCheckpointOperators, modeCheckpointSubtasks:
		return navigationCheckpoints
	case modeDiagnostics:
		return navigationDiagnostics
	case modeTimeline:
		return navigationTimeline
	case modeExceptions:
		return navigationExceptions
	case modeJobConfig:
		return navigationJobConfig
	case modeActions:
		return navigationActions
	case modeMetricExplorer:
		return navigationMetrics
	case modeAccumulators:
		return navigationAccumulators
	default:
		return navigationGraph
	}
}

func (m *Model) jumpToPaletteVertex(vertexID string) tea.Cmd {
	if _, ok := m.layout.Rects[vertexID]; !ok {
		m.setNotice("That vertex is no longer present in the current job.")
		return nil
	}
	m.clearRouteHistory()
	m.selected = vertexID
	switch m.mode {
	case modeSubtasks:
		return m.openDiagnostics()
	case modeFlameGraph:
		return m.openFlameGraph()
	case modeMetricExplorer:
		return m.openMetricExplorer()
	case modeAccumulators:
		return m.openAccumulators()
	case modeGraph:
		m.centerSelection()
	default:
		m.syncJobDetailContext()
	}
	return nil
}

func (m *Model) jumpToPaletteTaskManager(taskManagerID string) tea.Cmd {
	if taskManagerID == "" {
		return nil
	}
	m.clearRouteHistory()
	return m.openTaskManagerByID(taskManagerID, modeTaskManagers)
}

func (m *Model) refreshCurrent() tea.Cmd { return m.activeScreen().refresh(m) }

func (m *Model) toggleMinimap() {
	m.graphViewport.ToggleMinimap(m.graphWidth())
	m.centerSelection()
}

func (m Model) renderCommandPalette(width, height int) string {
	return m.palette.Render(width, height, m.paletteCommands())
}
