package coordinator

import (
	tea "charm.land/bubbletea/v2"

	"github.com/nikitasavinov/flink-tui/internal/flink"
	"github.com/nikitasavinov/flink-tui/internal/graph"
	checkpointmodule "github.com/nikitasavinov/flink-tui/internal/ui/checkpoints"
	jobdetailmodule "github.com/nikitasavinov/flink-tui/internal/ui/jobdetail"
	joblistmodule "github.com/nikitasavinov/flink-tui/internal/ui/joblist"
	"github.com/nikitasavinov/flink-tui/internal/ui/shared"
)

func (m *Model) configureCheckpoints() {
	m.checkpoints = checkpointmodule.New(m.client, m.requests.context)
	m.syncCheckpointContext()
}

func (m *Model) checkpointContext() checkpointmodule.Context {
	return checkpointmodule.Context{
		JobID: m.snapshot.JobID, Summary: m.snapshot.Checkpoints, Nodes: m.snapshot.Nodes,
		Order: m.layout.Order, UpdatedAt: m.snapshot.UpdatedAt, RefreshErr: m.err,
		Generation: m.generation, BodyHeight: m.bodyHeight(),
	}
}

func (m *Model) syncCheckpointContext() { m.checkpoints.Sync(m.checkpointContext()) }

func (m *Model) syncCheckpointMode() {
	switch m.checkpoints.CurrentView() {
	case checkpointmodule.ViewOperators:
		m.mode = modeCheckpointOperators
	case checkpointmodule.ViewSubtasks:
		m.mode = modeCheckpointSubtasks
	default:
		m.mode = modeCheckpoints
	}
}

func (m *Model) applyCheckpointMessage(message checkpointmodule.Message) tea.Cmd {
	m.syncCheckpointContext()
	result := m.checkpoints.Apply(message)
	// Background replies may update cached data, but only the active workspace
	// may change the screen or its route history when a comparison falls back.
	switch m.mode {
	case modeCheckpoints, modeCheckpointOperators, modeCheckpointSubtasks:
		return m.applyCheckpointResult(result)
	default:
		return result.Command
	}
}

func (m *Model) applyCheckpointResult(result checkpointmodule.Result) tea.Cmd {
	if result.Notice != "" {
		m.setNotice(result.Notice)
	}
	if result.PeerFallback {
		if crumb, ok := m.history.Peek(); ok && crumb.mode == modeCheckpointOperators {
			_, _ = m.history.Pop()
		}
	}
	switch result.Intent {
	case checkpointmodule.IntentOverview:
		return m.openJobPicker()
	case checkpointmodule.IntentGraph:
		if result.Vertex != "" {
			if _, ok := m.layout.Rects[result.Vertex]; !ok {
				return result.Command
			}
			m.selected = result.Vertex
		}
		m.mode = modeGraph
		m.centerSelection()
		return result.Command
	default:
		m.syncCheckpointMode()
		return result.Command
	}
}

func (m *Model) openCheckpoints() {
	if m.snapshot.JobID == "" {
		return
	}
	m.checkpoints.Open(m.checkpointContext())
	m.syncCheckpointMode()
}

func (m *Model) handleCheckpointKey(key string) tea.Cmd {
	m.syncCheckpointContext()
	m.checkpoints.Activate(checkpointmodule.ViewHistory)
	origin := m.routeCrumb()
	command := m.applyCheckpointResult(m.checkpoints.HandleKey(key, m.bodyHeight()))
	if key == "enter" {
		m.pushDrill(origin)
	}
	return command
}

func (m *Model) handleCheckpointOperatorKey(key string) tea.Cmd {
	m.syncCheckpointContext()
	m.checkpoints.Activate(checkpointmodule.ViewOperators)
	origin := m.routeCrumb()
	command := m.applyCheckpointResult(m.checkpoints.HandleKey(key, m.bodyHeight()))
	if key == "enter" {
		m.pushDrill(origin)
	}
	return command
}

func (m *Model) handleCheckpointSubtaskKey(key string) tea.Cmd {
	m.syncCheckpointContext()
	m.checkpoints.Activate(checkpointmodule.ViewSubtasks)
	return m.applyCheckpointResult(m.checkpoints.HandleKey(key, m.bodyHeight()))
}

func (m *Model) handleCheckpointMouseClick(event tea.Mouse) tea.Cmd {
	m.syncCheckpointContext()
	m.checkpoints.Activate(checkpointmodule.ViewHistory)
	return m.applyCheckpointResult(m.checkpoints.HandleClick(event, m.bodyHeight()))
}

func (m *Model) handleCheckpointOperatorMouseClick(event tea.Mouse) tea.Cmd {
	m.syncCheckpointContext()
	m.checkpoints.Activate(checkpointmodule.ViewOperators)
	return m.applyCheckpointResult(m.checkpoints.HandleClick(event, m.bodyHeight()))
}

func (m *Model) handleCheckpointSubtaskMouseClick(event tea.Mouse) tea.Cmd {
	m.syncCheckpointContext()
	m.checkpoints.Activate(checkpointmodule.ViewSubtasks)
	return m.applyCheckpointResult(m.checkpoints.HandleClick(event, m.bodyHeight()))
}

func (m *Model) moveCheckpointSelection(delta int) {
	m.syncCheckpointContext()
	m.checkpoints.Activate(checkpointmodule.ViewHistory)
	m.checkpoints.HandleWheel(delta, m.bodyHeight())
}

func (m *Model) moveCheckpointOperatorSelection(delta int) {
	m.syncCheckpointContext()
	m.checkpoints.Activate(checkpointmodule.ViewOperators)
	m.checkpoints.HandleWheel(delta, m.bodyHeight())
}

func (m *Model) moveCheckpointSubtaskSelection(delta int) {
	m.syncCheckpointContext()
	m.checkpoints.Activate(checkpointmodule.ViewSubtasks)
	m.checkpoints.HandleWheel(delta, m.bodyHeight())
}

func (m *Model) fetchCheckpointDetail() tea.Cmd {
	m.syncCheckpointContext()
	return m.checkpoints.RefreshOperators()
}

func (m *Model) fetchCheckpointSubtasks() tea.Cmd {
	m.syncCheckpointContext()
	return m.checkpoints.RefreshSubtasks()
}

func (m Model) renderCheckpoints(width, height int) string {
	m.checkpoints.Sync(m.checkpointContext())
	m.checkpoints.Activate(checkpointmodule.ViewHistory)
	return m.checkpoints.Render(width, height)
}

func (m Model) renderCheckpointOperators(width, height int) string {
	m.checkpoints.Sync(m.checkpointContext())
	m.checkpoints.Activate(checkpointmodule.ViewOperators)
	return m.checkpoints.Render(width, height)
}

func (m Model) renderCheckpointSubtasks(width, height int) string {
	m.checkpoints.Sync(m.checkpointContext())
	m.checkpoints.Activate(checkpointmodule.ViewSubtasks)
	return m.checkpoints.Render(width, height)
}

type diagnosticPage = jobdetailmodule.Page

const (
	diagnosticBackpressure = jobdetailmodule.PageBackpressure
	diagnosticSkew         = jobdetailmodule.PageSkew
	diagnosticMetrics      = jobdetailmodule.PageMetrics
)

func (m *Model) configureJobDetails() {
	m.jobDetails = jobdetailmodule.New(m.client, m.requests.context)
	m.syncJobDetailContext()
}

func (m Model) jobDetailContext() jobdetailmodule.Context {
	return jobdetailmodule.Context{
		Snapshot: m.snapshot, Selected: m.selected, Order: m.layout.Order,
		Generation: m.generation, BodyHeight: m.bodyHeight(),
		ContentWidth: m.contentWidthAt(max(40, m.width)), Width: m.width,
		History: m.graphTelemetry.History(),
	}
}

func (m *Model) syncJobDetailContext() { m.jobDetails.Sync(m.jobDetailContext()) }

func (m *Model) syncJobDetailMode(view jobdetailmodule.View) {
	switch view {
	case jobdetailmodule.ViewDiagnostics:
		m.mode = modeDiagnostics
	case jobdetailmodule.ViewTimeline:
		m.mode = modeTimeline
	default:
		m.mode = modeSubtasks
	}
}

func (m *Model) applyJobDetailResult(result jobdetailmodule.Result) tea.Cmd {
	origin := m.routeCrumb()
	if result.Selected != "" {
		if _, ok := m.layout.Rects[result.Selected]; ok {
			m.selected = result.Selected
		}
	}
	if result.Notice != "" {
		m.setNotice(result.Notice)
	}
	switch result.Intent {
	case jobdetailmodule.IntentGraph:
		m.mode = modeGraph
		m.centerSelection()
	case jobdetailmodule.IntentOverview:
		return tea.Batch(result.Command, m.openJobPicker())
	case jobdetailmodule.IntentCheckpoints:
		m.openCheckpoints()
		m.pushDrill(origin)
	case jobdetailmodule.IntentThreadDump:
		command := m.openThreadDumpWithFocus(result.Process, modeSubtasks, result.Focus)
		m.pushDrill(origin)
		return tea.Batch(result.Command, command)
	default:
		m.syncJobDetailMode(result.View)
		m.pushDrill(origin)
	}
	return result.Command
}

func (m *Model) openDiagnostics() tea.Cmd {
	if m.snapshot.JobID == "" || m.selected == "" {
		return nil
	}
	m.mode = modeSubtasks
	return m.jobDetails.OpenSubtasks(m.jobDetailContext())
}

func (m *Model) openDiagnosticOverview(page diagnosticPage) {
	m.jobDetails.OpenDiagnostics(m.jobDetailContext(), page)
	m.mode = modeDiagnostics
}

func (m *Model) openTimeline() {
	m.jobDetails.OpenTimeline(m.jobDetailContext())
	m.mode = modeTimeline
}

func (m *Model) handleSubtaskKey(key string) tea.Cmd {
	m.syncJobDetailContext()
	m.jobDetails.Activate(jobdetailmodule.ViewSubtasks)
	return m.applyJobDetailResult(m.jobDetails.HandleKey(key))
}

func (m *Model) handleDiagnosticOverviewKey(key string) tea.Cmd {
	m.syncJobDetailContext()
	m.jobDetails.Activate(jobdetailmodule.ViewDiagnostics)
	return m.applyJobDetailResult(m.jobDetails.HandleKey(key))
}

func (m *Model) handleTimelineKey(key string) tea.Cmd {
	m.syncJobDetailContext()
	m.jobDetails.Activate(jobdetailmodule.ViewTimeline)
	return m.applyJobDetailResult(m.jobDetails.HandleKey(key))
}

func (m *Model) handleSubtaskMouseClick(event tea.Mouse) tea.Cmd {
	m.syncJobDetailContext()
	m.jobDetails.Activate(jobdetailmodule.ViewSubtasks)
	return m.applyJobDetailResult(m.jobDetails.HandleClick(event))
}

func (m *Model) handleDiagnosticOverviewMouseClick(event tea.Mouse) tea.Cmd {
	m.syncJobDetailContext()
	m.jobDetails.Activate(jobdetailmodule.ViewDiagnostics)
	return m.applyJobDetailResult(m.jobDetails.HandleClick(event))
}

func (m *Model) handleTimelineMouseClick(event tea.Mouse) tea.Cmd {
	m.syncJobDetailContext()
	m.jobDetails.Activate(jobdetailmodule.ViewTimeline)
	return m.applyJobDetailResult(m.jobDetails.HandleClick(event))
}

func (m *Model) moveSubtaskSelection(delta int) {
	m.jobDetails.Activate(jobdetailmodule.ViewSubtasks)
	m.jobDetails.HandleWheel(delta)
}

func (m *Model) moveDiagnosticSelection(delta int) {
	view := jobdetailmodule.ViewDiagnostics
	if m.mode == modeTimeline {
		view = jobdetailmodule.ViewTimeline
	}
	m.jobDetails.Activate(view)
	m.jobDetails.HandleWheel(delta)
	state := m.jobDetails.State()
	if state.Selected != "" {
		m.selected = state.Selected
	}
}

func (m *Model) fetchDiagnostics() tea.Cmd {
	m.jobDetails.Sync(m.jobDetailContext())
	return m.jobDetails.Poll()
}

func (m Model) renderSubtasks(width, height int) string {
	m.jobDetails.Sync(m.jobDetailContext())
	m.jobDetails.Activate(jobdetailmodule.ViewSubtasks)
	return m.jobDetails.Render(width, height)
}

func (m Model) renderDiagnosticOverview(width, height int) string {
	m.jobDetails.Sync(m.jobDetailContext())
	m.jobDetails.Activate(jobdetailmodule.ViewDiagnostics)
	return m.jobDetails.Render(width, height)
}

func (m Model) renderTimeline(width, height int) string {
	m.jobDetails.Sync(m.jobDetailContext())
	m.jobDetails.Activate(jobdetailmodule.ViewTimeline)
	return m.jobDetails.Render(width, height)
}

func (m *Model) configureJobList() {
	m.jobList = joblistmodule.New(m.client, m.requests.context)
	m.syncJobListContext()
}

func (m Model) jobListContext() joblistmodule.Context {
	return joblistmodule.Context{
		CurrentJobID: m.snapshot.JobID, PreferredJobID: m.preferredJobID,
		BodyHeight: m.bodyHeight(),
	}
}

func (m *Model) syncJobListContext() { m.jobList.Sync(m.jobListContext()) }

func (m *Model) applyJobListResult(result joblistmodule.Result) tea.Cmd {
	switch result.Intent {
	case joblistmodule.IntentGraph:
		m.navigation.ClearDeferredTarget()
		m.mode = modeGraph
	case joblistmodule.IntentOpenJob:
		target := m.jobPickerContinuation
		m.jobPickerContinuation = navigationGraph
		if target != navigationNone && target != navigationGraph {
			m.navigation.DeferTarget(target)
			job, _ := m.jobAtCursor()
			m.setNotice("Opening " + destination(target).label + " for " + shared.Fallback(job.Name, result.JobID) + "…")
		} else {
			m.navigation.ClearDeferredTarget()
		}
		return tea.Batch(result.Command, m.switchToJob(result.JobID))
	}
	return result.Command
}

func (m *Model) openJobPicker() tea.Cmd {
	m.jobPickerContinuation = jobPickerTargetForMode(m.mode)
	m.navigation.ClearDeferredTarget()
	command := m.jobList.Open(m.jobListContext())
	m.mode = modeJobs
	return command
}

func (m *Model) startJobsRefresh() tea.Cmd {
	m.syncJobListContext()
	return m.jobList.Poll()
}

func (m *Model) fetchJobs() tea.Cmd {
	m.jobList.Sync(m.jobListContext())
	return m.jobList.Poll()
}

func (m *Model) handleJobKey(key string) tea.Cmd {
	m.syncJobListContext()
	return m.applyJobListResult(m.jobList.HandleKey(key))
}

func (m *Model) handleJobMouseClick(event tea.Mouse) tea.Cmd {
	m.syncJobListContext()
	return m.applyJobListResult(m.jobList.HandleClick(event))
}

func (m *Model) moveJobCursor(delta int) { m.jobList.HandleWheel(delta) }

func (m Model) jobAtCursor() (flink.JobSummary, bool) { return m.jobList.SelectedJob() }

func (m Model) jobIDAtCursor() string {
	job, ok := m.jobList.SelectedJob()
	if !ok {
		return ""
	}
	return job.ID
}

func (m *Model) switchToSelectedJob() tea.Cmd {
	m.syncJobListContext()
	return m.applyJobListResult(m.jobList.HandleKey("enter"))
}

func (m *Model) switchToJob(jobID string) tea.Cmd {
	return m.beginJobSwitch(jobID, true)
}

func (m *Model) switchToPeerJob(jobID string) tea.Cmd {
	return m.beginJobSwitch(jobID, false)
}

func (m *Model) beginJobSwitch(jobID string, clearHistory bool) tea.Cmd {
	if jobID == "" {
		return nil
	}
	if clearHistory {
		m.clearRouteHistory()
	}
	m.generation++
	m.requests.snapshotPending = false
	m.resetJobScopedState()
	m.preferredJobID = jobID
	m.snapshot = flink.Snapshot{}
	m.layout = graph.Layout{}
	m.selected = ""
	m.loading = true
	m.err = nil
	m.mode = modeGraph
	m.syncJobListContext()
	return m.fetchSnapshot()
}

func (m Model) renderJobPicker(width, height int) string {
	m.jobList.Sync(m.jobListContext())
	return m.jobList.Render(width, height)
}
