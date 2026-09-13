package coordinator

import (
	tea "charm.land/bubbletea/v2"
	"github.com/nikitasavinov/flink-tui/internal/flink"
	inframodule "github.com/nikitasavinov/flink-tui/internal/ui/infrastructure"
	processmodule "github.com/nikitasavinov/flink-tui/internal/ui/processdiag"
	profilermodule "github.com/nikitasavinov/flink-tui/internal/ui/profiler"
	"strings"
)

const taskManagerBodyRowStart = 3

func (m *Model) configureInfrastructure() {
	m.infrastructure = inframodule.New(m.client, m.requests.context, conciseError)
}

func (m Model) infrastructureModeActive() bool {
	switch m.mode {
	case modeTaskManagers, modeTaskManagerDetail, modeJobManager:
		return true
	default:
		return false
	}
}

func (m *Model) syncInfrastructureMode() {
	switch m.infrastructure.View() {
	case inframodule.ViewTaskManagerDetail:
		m.mode = modeTaskManagerDetail
	case inframodule.ViewJobManager:
		m.mode = modeJobManager
	default:
		m.mode = modeTaskManagers
	}
}

func (m *Model) applyInfrastructureResult(result inframodule.Result) tea.Cmd {
	origin := m.routeCrumb()
	if result.Notice != "" {
		m.setNotice(result.Notice)
	}
	switch result.Intent {
	case inframodule.IntentOverview:
		return m.openJobPicker()
	case inframodule.IntentBack:
		if m.popRouteHistory() {
			return tea.Batch(result.Command, tea.ClearScreen)
		}
		if result.BackToken == int(modeTaskManagers) {
			m.infrastructure.Activate(inframodule.ViewTaskManagers)
		}
		if result.BackToken >= 0 && result.BackToken < int(modeCount) {
			m.mode = screenRegistryMode(result.BackToken)
		}
		return result.Command
	case inframodule.IntentLogs:
		return m.finishDrill(origin, m.openCurrentProcessLog(result.Process, m.mode))
	case inframodule.IntentLogFiles:
		return m.finishDrill(origin, m.openProcessLogs(result.Process, m.mode))
	case inframodule.IntentStdout:
		return m.finishDrill(origin, m.openStdout(result.Process, m.mode))
	case inframodule.IntentThreadDump:
		return m.finishDrill(origin, m.openThreadDump(result.Process, m.mode))
	case inframodule.IntentProfiler:
		return m.finishDrill(origin, m.openProfiler(result.Process, m.mode))
	case inframodule.IntentDocument:
		return m.finishDrill(origin, m.openLogDocument(result.Process, result.Log, m.mode))
	default:
		if m.infrastructureModeActive() {
			m.syncInfrastructureMode()
		}
		return result.Command
	}
}

func (m *Model) openTaskManagers() tea.Cmd {
	command := m.infrastructure.OpenTaskManagers(int(modeTaskManagers))
	m.syncInfrastructureMode()
	return command
}

func (m *Model) openTaskManagerByID(taskManagerID string, backMode screenMode) tea.Cmd {
	label := "Task Managers"
	if backMode == modeExceptions {
		label = "Exceptions"
	}
	result := m.infrastructure.OpenTaskManagerByID(strings.TrimSpace(taskManagerID), int(backMode), label)
	m.syncInfrastructureMode()
	return m.applyInfrastructureResult(result)
}

func (m *Model) openJobManager() tea.Cmd {
	command := m.infrastructure.OpenJobManager()
	m.syncInfrastructureMode()
	return command
}

func (m *Model) fetchInfrastructure() tea.Cmd { return m.infrastructure.Poll() }

func (m *Model) handleTaskManagerKey(key string) tea.Cmd {
	m.infrastructure.Activate(inframodule.ViewTaskManagers)
	m.infrastructure.SetBackTarget(int(modeTaskManagers), "Task Managers")
	origin := m.routeCrumb()
	command := m.applyInfrastructureResult(m.infrastructure.HandleKey(key, m.bodyHeight(), m.graphWidth()))
	if key == "enter" {
		m.pushDrill(origin)
	}
	return command
}

func (m *Model) handleTaskManagerDetailKey(key string) tea.Cmd {
	m.infrastructure.Activate(inframodule.ViewTaskManagerDetail)
	return m.applyInfrastructureResult(m.infrastructure.HandleKey(key, m.bodyHeight(), m.graphWidth()))
}

func (m *Model) handleJobManagerKey(key string) tea.Cmd {
	m.infrastructure.Activate(inframodule.ViewJobManager)
	return m.applyInfrastructureResult(m.infrastructure.HandleKey(key, m.bodyHeight(), m.graphWidth()))
}

func (m *Model) handleInfrastructureMouseClick(event tea.Mouse) tea.Cmd {
	origin := m.routeCrumb()
	if m.mode == modeTaskManagers {
		m.infrastructure.SetBackTarget(int(modeTaskManagers), "Task Managers")
	}
	result := m.infrastructure.HandleClick(event, event.Y-headerHeight, m.bodyHeight())
	command := m.applyInfrastructureResult(result)
	if origin.mode == modeTaskManagers && m.mode == modeTaskManagerDetail {
		m.pushDrill(origin)
	}
	return command
}

func (m *Model) moveTaskManagerSelection(delta int) {
	m.infrastructure.Activate(inframodule.ViewTaskManagers)
	m.infrastructure.HandleWheel(delta, m.bodyHeight(), m.graphWidth())
}

func (m *Model) moveTaskManagerDetail(delta int) {
	m.infrastructure.Activate(inframodule.ViewTaskManagerDetail)
	m.infrastructure.HandleWheel(delta, m.bodyHeight(), m.graphWidth())
}

func (m *Model) moveJobManagerConfigSelection(delta int) {
	m.infrastructure.Activate(inframodule.ViewJobManager)
	m.infrastructure.HandleWheel(delta, m.bodyHeight(), m.graphWidth())
}

func (m Model) selectedTaskManager() (flink.TaskManager, bool) {
	return m.infrastructure.SelectedTaskManager()
}

func (m Model) renderTaskManagers(width, height int) string {
	m.infrastructure.Activate(inframodule.ViewTaskManagers)
	return m.infrastructure.Render(width, height)
}

func (m Model) renderTaskManagerDetail(width, height int) string {
	m.infrastructure.Activate(inframodule.ViewTaskManagerDetail)
	return m.infrastructure.Render(width, height)
}

func (m Model) renderJobManager(width, height int) string {
	m.infrastructure.Activate(inframodule.ViewJobManager)
	return m.infrastructure.Render(width, height)
}

func (m *Model) configureProcessDiagnostics() {
	m.processDiagnostics = processmodule.New(m.client, m.requests.context)
}

func (m *Model) syncProcessDiagnosticMode() {
	switch m.processDiagnostics.CurrentView() {
	case processmodule.ViewDocument:
		m.mode = modeDocument
	case processmodule.ViewThreadDump:
		m.mode = modeThreadDump
	default:
		m.mode = modeProcessLogs
	}
}

func (m *Model) applyProcessDiagnosticResult(result processmodule.Result) tea.Cmd {
	origin := m.routeCrumb()
	switch result.Intent {
	case processmodule.IntentBack:
		if m.popRouteHistory() {
			return tea.Batch(result.Command, tea.ClearScreen)
		}
		if result.BackToken >= 0 && result.BackToken < int(modeCount) {
			m.mode = screenRegistryMode(result.BackToken)
		}
		return result.Command
	case processmodule.IntentProfiler:
		return m.finishDrill(origin, m.openProfiler(result.Process, m.mode))
	default:
		m.syncProcessDiagnosticMode()
		m.pushDrill(origin)
		return result.Command
	}
}

func (m *Model) openProcessLogs(process flink.ProcessRef, backMode screenMode) tea.Cmd {
	command := m.processDiagnostics.OpenLogs(process, int(backMode))
	m.syncProcessDiagnosticMode()
	return command
}

func (m *Model) openCurrentProcessLog(process flink.ProcessRef, backMode screenMode) tea.Cmd {
	command := m.processDiagnostics.OpenCurrentLog(process, int(backMode), m.bodyHeight())
	m.syncProcessDiagnosticMode()
	return command
}

func (m *Model) openLogDocument(process flink.ProcessRef, log flink.LogFile, backMode screenMode) tea.Cmd {
	command := m.processDiagnostics.OpenLog(process, log, int(backMode))
	m.syncProcessDiagnosticMode()
	return command
}

func (m *Model) openStdout(process flink.ProcessRef, backMode screenMode) tea.Cmd {
	command := m.processDiagnostics.OpenStdout(process, int(backMode))
	m.syncProcessDiagnosticMode()
	return command
}

func (m *Model) openStaticDocument(title, content string, backMode screenMode) {
	m.processDiagnostics.OpenStatic(title, content, int(backMode))
	m.syncProcessDiagnosticMode()
}

func (m *Model) openThreadDump(process flink.ProcessRef, backMode screenMode) tea.Cmd {
	return m.openThreadDumpWithFocus(process, backMode, processmodule.Focus{})
}

func (m *Model) openThreadDumpWithFocus(process flink.ProcessRef, backMode screenMode, focus processmodule.Focus) tea.Cmd {
	command := m.processDiagnostics.OpenThreadDump(process, int(backMode), focus)
	m.syncProcessDiagnosticMode()
	return command
}

func (m *Model) handleProcessLogKey(key string) tea.Cmd {
	m.processDiagnostics.Activate(processmodule.ViewLogs)
	return m.applyProcessDiagnosticResult(m.processDiagnostics.HandleKey(key, m.bodyHeight()))
}

func (m *Model) handleDocumentKey(key string) tea.Cmd {
	m.processDiagnostics.Activate(processmodule.ViewDocument)
	return m.applyProcessDiagnosticResult(m.processDiagnostics.HandleKey(key, m.bodyHeight()))
}

func (m *Model) handleThreadDumpKey(key string) tea.Cmd {
	m.processDiagnostics.Activate(processmodule.ViewThreadDump)
	return m.applyProcessDiagnosticResult(m.processDiagnostics.HandleKey(key, m.bodyHeight()))
}

func (m *Model) handleProcessLogMouseClick(event tea.Mouse) tea.Cmd {
	m.processDiagnostics.Activate(processmodule.ViewLogs)
	return m.applyProcessDiagnosticResult(m.processDiagnostics.HandleClick(event, m.bodyHeight()))
}

func (m *Model) handleThreadMouseClick(event tea.Mouse) tea.Cmd {
	m.processDiagnostics.Activate(processmodule.ViewThreadDump)
	return m.applyProcessDiagnosticResult(m.processDiagnostics.HandleClick(event, m.bodyHeight()))
}

func (m *Model) moveProcessLogSelection(delta int) {
	m.processDiagnostics.Activate(processmodule.ViewLogs)
	m.processDiagnostics.HandleWheel(delta, m.bodyHeight())
}

func (m *Model) moveDocumentVertical(delta int) {
	m.processDiagnostics.Activate(processmodule.ViewDocument)
	m.processDiagnostics.HandleWheel(delta, m.bodyHeight())
}

func (m *Model) moveThreadSelection(delta int) {
	m.processDiagnostics.Activate(processmodule.ViewThreadDump)
	m.processDiagnostics.HandleWheel(delta, m.bodyHeight())
}

func (m *Model) refreshProcessDiagnostics(view processmodule.View) tea.Cmd {
	m.processDiagnostics.Activate(view)
	return m.processDiagnostics.Refresh()
}

func (m Model) selectedThread() (flink.ThreadInfo, bool) {
	return m.processDiagnostics.SelectedThread()
}

func (m Model) renderProcessLogs(width, height int) string {
	m.processDiagnostics.Activate(processmodule.ViewLogs)
	return m.processDiagnostics.Render(width, height)
}

func (m Model) renderDocument(width, height int) string {
	m.processDiagnostics.Activate(processmodule.ViewDocument)
	return m.processDiagnostics.Render(width, height)
}

func (m Model) renderThreadDump(width, height int) string {
	m.processDiagnostics.Activate(processmodule.ViewThreadDump)
	return m.processDiagnostics.Render(width, height)
}

func (m *Model) configureProfiler() {
	m.profiler = profilermodule.New(m.client, m.requests.context)
	m.profiler.SetViewport(m.width, m.height)
}

func (m *Model) openProfiler(process flink.ProcessRef, backMode screenMode) tea.Cmd {
	m.mode = modeProfiler
	m.flameGraphs.ClearProfilerReport()
	m.profiler.SetViewport(m.width, m.height)
	return m.profiler.Open(process, int(backMode))
}

func (m *Model) applyProfilerResult(result profilermodule.Result) tea.Cmd {
	origin := m.routeCrumb()
	if result.Notice != "" {
		m.setNotice(result.Notice)
	}
	if result.ReportName != "" && result.Graph.Ready() {
		m.flameGraphs.LoadProfilerReport(result.Process, result.ReportName, result.Graph)
	}
	switch result.Intent {
	case profilermodule.IntentBack:
		if m.popRouteHistory() {
			return tea.Batch(result.Command, tea.ClearScreen)
		}
		if result.BackToken >= 0 && result.BackToken < int(modeCount) {
			m.mode = screenRegistryMode(result.BackToken)
		}
	case profilermodule.IntentLogs:
		return m.finishDrill(origin, tea.Batch(result.Command, m.openCurrentProcessLog(result.Process, modeProfiler)))
	case profilermodule.IntentStdout:
		return m.finishDrill(origin, tea.Batch(result.Command, m.openStdout(result.Process, modeProfiler)))
	case profilermodule.IntentThreadDump:
		return m.finishDrill(origin, tea.Batch(result.Command, m.openThreadDump(result.Process, modeProfiler)))
	case profilermodule.IntentReport:
		m.mode = modeProfilerFlameGraph
		m.pushDrill(origin)
	}
	return result.Command
}

func (m *Model) applyProfilerMessage(message profilermodule.Message) tea.Cmd {
	return m.applyProfilerResult(m.profiler.Apply(message, m.mode == modeProfiler))
}

func (m *Model) handleProfilerKey(key string) tea.Cmd {
	m.profiler.SetViewport(m.width, m.height)
	return m.applyProfilerResult(m.profiler.HandleKey(key))
}

func (m *Model) handleProfilerMouseClick(event tea.Mouse) tea.Cmd {
	m.profiler.SetViewport(m.width, m.height)
	return m.applyProfilerResult(m.profiler.HandleClick(event))
}

func (m *Model) moveProfilerSelection(delta int) { m.profiler.HandleWheel(delta) }

func (m *Model) fetchProfilerList() tea.Cmd { return m.profiler.Poll() }

func (m Model) renderProfiler(width, height int) string {
	m.profiler.SetViewport(m.width, m.height)
	return m.profiler.Render(width, height)
}
