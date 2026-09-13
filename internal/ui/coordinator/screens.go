package coordinator

import (
	"fmt"
	"strings"

	tea "charm.land/bubbletea/v2"

	checkpointmodule "github.com/nikitasavinov/flink-tui/internal/ui/checkpoints"
	flamegraphmodule "github.com/nikitasavinov/flink-tui/internal/ui/flamegraph"
	inframodule "github.com/nikitasavinov/flink-tui/internal/ui/infrastructure"
	jobdetailmodule "github.com/nikitasavinov/flink-tui/internal/ui/jobdetail"
	jobopsmodule "github.com/nikitasavinov/flink-tui/internal/ui/jobops"
	processmodule "github.com/nikitasavinov/flink-tui/internal/ui/processdiag"
	"github.com/nikitasavinov/flink-tui/internal/ui/shared"
)

// screenSpec is the single extension point for a full-screen mode. The root
// model still owns shared application state and asynchronous messages, while
// mode-specific interaction and presentation are declared here once.
type screenSpec struct {
	name                string
	navTarget           navigationTarget
	scope               screenScope
	upParent            screenMode
	title               func(Model) string
	render              func(Model, int, int) string
	key                 func(*Model, tea.KeyPressMsg) tea.Cmd
	click               func(*Model, tea.Mouse) tea.Cmd
	wheel               func(*Model, tea.Mouse) tea.Cmd
	refresh             func(*Model) tea.Cmd
	autoRefresh         func(*Model) tea.Cmd
	resize              func(*Model) tea.Cmd
	header              func(Model, int, string) string
	footer              func(Model, string, string) string
	backFallback        screenMode
	errors              func(Model) []displayedError
	capturesKeys        func(Model) bool
	leftOpensNavigation bool
}

var screenRegistry = [modeCount]screenSpec{
	modeGraph: {
		name: "Job Graph", navTarget: navigationGraph, scope: scopeJob, upParent: modeJobs, render: Model.renderGraphScreen, key: stringScreenKey((*Model).handleGraphKey),
		click: mouseWithoutCommand((*Model).handleMouseClick), wheel: mouseWithoutCommand((*Model).handleMouseWheel),
		refresh: refreshGraph, autoRefresh: refreshSnapshot, resize: noScreenCommand,
		header: jobScreenHeader, footer: graphScreenFooter, errors: jobScreenErrors,
		backFallback: modeJobs,
		capturesKeys: neverCapturesKeys,
	},
	modeJobs: {
		name: "Overview", navTarget: navigationOverview, scope: scopeOverview, upParent: modeCount, render: Model.renderJobPicker, key: stringScreenKey((*Model).handleJobKey),
		click: (*Model).handleJobMouseClick, wheel: wheelByDelta((*Model).moveJobCursor),
		refresh: refreshJobs, autoRefresh: tickJobs, resize: noScreenCommand,
		header: homeScreenHeader, footer: jobsScreenFooter, errors: homeScreenErrors,
		capturesKeys: jobListCapturesKeys, leftOpensNavigation: true,
	},
	modeSubtasks: {
		name: "Subtasks", navTarget: navigationSubtasks, scope: scopeVertex, upParent: modeGraph, render: Model.renderSubtasks, key: stringScreenKey((*Model).handleSubtaskKey),
		click: (*Model).handleSubtaskMouseClick, wheel: wheelByDelta((*Model).moveSubtaskSelection),
		refresh: refreshSubtasks, autoRefresh: tickSubtasks, resize: noScreenCommand,
		header: jobScreenHeader, footer: subtasksScreenFooter, errors: subtaskScreenErrors,
		backFallback: modeGraph,
		capturesKeys: subtaskCapturesKeys, leftOpensNavigation: true,
	},
	modeFlameGraph: {
		name: "Vertex Flame Graph", navTarget: navigationFlameGraph, scope: scopeVertex, upParent: modeCount, render: Model.renderFlameGraph, key: stringScreenKey((*Model).handleFlameGraphKey),
		click: (*Model).handleFlameGraphMouseClick, wheel: wheelByDelta((*Model).moveFlameGraphLinear),
		refresh: refreshFlameGraph, autoRefresh: tickFlameGraph, resize: noScreenCommand,
		header: jobScreenHeader, footer: flameGraphScreenFooter, errors: flameGraphScreenErrors,
		backFallback: modeGraph,
		capturesKeys: neverCapturesKeys,
	},
	modeCheckpoints: {
		name: "Checkpoints", navTarget: navigationCheckpoints, scope: scopeJob, upParent: modeJobs, render: Model.renderCheckpoints, key: stringScreenKey((*Model).handleCheckpointKey),
		click: (*Model).handleCheckpointMouseClick, wheel: wheelByDelta((*Model).moveCheckpointSelection),
		refresh: refreshSnapshot, autoRefresh: refreshSnapshot, resize: noScreenCommand,
		header: jobScreenHeader, footer: checkpointsScreenFooter, errors: jobScreenErrors,
		backFallback: modeGraph,
		capturesKeys: checkpointCapturesKeys, leftOpensNavigation: true,
	},
	modeCheckpointOperators: {
		name: "Checkpoint Operators", navTarget: navigationCheckpointOperators, scope: scopeJob, upParent: modeCheckpoints, render: Model.renderCheckpointOperators, key: stringScreenKey((*Model).handleCheckpointOperatorKey),
		click: (*Model).handleCheckpointOperatorMouseClick, wheel: wheelByDelta((*Model).moveCheckpointOperatorSelection),
		refresh: refreshCheckpointOperators, autoRefresh: refreshCheckpointOperators, resize: noScreenCommand,
		header: jobScreenHeader, footer: checkpointOperatorsScreenFooter, errors: checkpointOperatorScreenErrors,
		backFallback: modeCheckpoints,
		capturesKeys: checkpointCapturesKeys, leftOpensNavigation: true,
	},
	modeCheckpointSubtasks: {
		name: "Checkpoint Subtasks", navTarget: navigationCheckpointSubtasks, scope: scopeJob, upParent: modeCheckpointOperators, render: Model.renderCheckpointSubtasks, key: stringScreenKey((*Model).handleCheckpointSubtaskKey),
		click: (*Model).handleCheckpointSubtaskMouseClick, wheel: wheelByDelta((*Model).moveCheckpointSubtaskSelection),
		refresh: refreshCheckpointSubtasks, autoRefresh: refreshCheckpointSubtasks, resize: noScreenCommand,
		header: jobScreenHeader, footer: checkpointSubtasksScreenFooter, errors: checkpointSubtaskScreenErrors,
		backFallback: modeCheckpointOperators,
		capturesKeys: checkpointCapturesKeys, leftOpensNavigation: true,
	},
	modeDiagnostics: {
		name: "Diagnostics", navTarget: navigationDiagnostics, scope: scopeJob, upParent: modeJobs, render: Model.renderDiagnosticOverview, key: stringScreenKey((*Model).handleDiagnosticOverviewKey),
		click: (*Model).handleDiagnosticOverviewMouseClick, wheel: wheelByDelta((*Model).moveDiagnosticSelection),
		refresh: refreshSnapshot, autoRefresh: refreshSnapshot, resize: noScreenCommand,
		header: jobScreenHeader, footer: diagnosticsScreenFooter, errors: jobScreenErrors,
		backFallback: modeGraph,
		capturesKeys: subtaskCapturesKeys, leftOpensNavigation: true,
	},
	modeTimeline: {
		name: "Timeline", navTarget: navigationTimeline, scope: scopeJob, upParent: modeJobs, render: Model.renderTimeline, key: stringScreenKey((*Model).handleTimelineKey),
		click: (*Model).handleTimelineMouseClick, wheel: wheelByDelta((*Model).moveDiagnosticSelection),
		refresh: refreshSnapshot, autoRefresh: refreshSnapshot, resize: noScreenCommand,
		header: jobScreenHeader, footer: timelineScreenFooter, errors: jobScreenErrors,
		backFallback: modeGraph,
		capturesKeys: subtaskCapturesKeys, leftOpensNavigation: true,
	},
	modeExceptions: {
		name: "Exceptions", navTarget: navigationExceptions, scope: scopeJob, upParent: modeJobs, render: Model.renderExceptions, key: stringScreenKey((*Model).handleExceptionKey),
		click: (*Model).handleExceptionMouseClick, wheel: wheelByDelta((*Model).moveExceptionSelection),
		refresh: refreshSnapshot, autoRefresh: refreshSnapshot, resize: noScreenCommand,
		header: jobScreenHeader, footer: exceptionsScreenFooter, errors: jobScreenErrors,
		backFallback: modeGraph,
		capturesKeys: exceptionCapturesKeys, leftOpensNavigation: true,
	},
	modeJobConfig: {
		name: "Job Configuration", navTarget: navigationJobConfig, scope: scopeJob, upParent: modeJobs, render: Model.renderJobConfiguration, key: stringScreenKey((*Model).handleJobConfigKey),
		click: (*Model).handleJobConfigMouseClick, wheel: wheelByDelta((*Model).moveJobConfigSelection),
		refresh: refreshJobConfiguration, autoRefresh: tickJobConfiguration, resize: noScreenCommand,
		header: jobScreenHeader, footer: jobConfigScreenFooter, errors: jobConfigScreenErrors,
		backFallback: modeGraph,
		capturesKeys: actionCapturesKeys, leftOpensNavigation: true,
	},
	modeActions: {
		name: "Job Actions", navTarget: navigationActions, scope: scopeJob, upParent: modeJobs, render: Model.renderActions, key: stringScreenKey((*Model).handleActionKey),
		click: (*Model).handleActionMouseClick, wheel: wheelByDelta((*Model).moveActionSelection),
		refresh: refreshActions, autoRefresh: tickActions, resize: noScreenCommand,
		header: jobScreenHeader, footer: actionsScreenFooter, errors: actionScreenErrors,
		backFallback: modeGraph,
		capturesKeys: actionCapturesKeys, leftOpensNavigation: true,
	},
	modeTaskManagers: {
		name: "Task Managers", navTarget: navigationTaskManagers, scope: scopeCluster, upParent: modeJobs, render: Model.renderTaskManagers, key: stringScreenKey((*Model).handleTaskManagerKey),
		click: (*Model).handleInfrastructureMouseClick, wheel: wheelByDelta((*Model).moveTaskManagerSelection),
		refresh: refreshInfrastructure, autoRefresh: tickInfrastructure, resize: noScreenCommand,
		header: infrastructureScreenHeader, footer: taskManagersScreenFooter, errors: infrastructureScreenErrors,
		backFallback: modeJobs,
		capturesKeys: neverCapturesKeys, leftOpensNavigation: true,
	},
	modeTaskManagerDetail: {
		name: "Task Manager Detail", navTarget: navigationTaskManagerDetail, scope: scopeCluster, upParent: modeTaskManagers, render: Model.renderTaskManagerDetail, key: stringScreenKey((*Model).handleTaskManagerDetailKey),
		click: (*Model).handleInfrastructureMouseClick, wheel: wheelByDelta((*Model).moveTaskManagerDetail),
		refresh: refreshInfrastructure, autoRefresh: tickInfrastructure, resize: noScreenCommand,
		header: infrastructureScreenHeader, footer: taskManagerDetailScreenFooter, errors: infrastructureScreenErrors,
		backFallback: modeTaskManagers,
		capturesKeys: neverCapturesKeys, leftOpensNavigation: true,
	},
	modeJobManager: {
		name: "Job Manager", navTarget: navigationJobManager, scope: scopeCluster, upParent: modeJobs, render: Model.renderJobManager, key: stringScreenKey((*Model).handleJobManagerKey),
		click: (*Model).handleInfrastructureMouseClick, wheel: wheelByDelta((*Model).moveJobManagerConfigSelection),
		refresh: refreshInfrastructure, autoRefresh: tickInfrastructure, resize: noScreenCommand,
		header: infrastructureScreenHeader, footer: jobManagerScreenFooter, errors: infrastructureScreenErrors,
		backFallback: modeJobs,
		capturesKeys: infrastructureCapturesKeys, leftOpensNavigation: true,
	},
	modeMetricExplorer: {
		name: "Metrics", navTarget: navigationMetrics, scope: scopeVertex, upParent: modeGraph, render: Model.renderMetricExplorer, key: stringScreenKey((*Model).handleMetricExplorerKey),
		click: mouseWithoutCommand((*Model).handleMetricMouseClick), wheel: metricScreenWheel,
		refresh: refreshMetrics, autoRefresh: tickMetrics, resize: resizeMetrics,
		header: jobScreenHeader, footer: metricsScreenFooter, errors: metricScreenErrors,
		backFallback: modeGraph,
		capturesKeys: metricCapturesKeys, leftOpensNavigation: true,
	},
	modeAccumulators: {
		name: "Accumulators", navTarget: navigationAccumulators, scope: scopeVertex, upParent: modeGraph, render: Model.renderAccumulators, key: stringScreenKey((*Model).handleAccumulatorKey),
		click: mouseWithoutCommand((*Model).handleAccumulatorMouseClick), wheel: wheelByDelta((*Model).moveAccumulatorSelection),
		refresh: refreshAccumulators, autoRefresh: tickAccumulators, resize: noScreenCommand,
		header: jobScreenHeader, footer: accumulatorsScreenFooter, errors: accumulatorScreenErrors,
		backFallback: modeGraph,
		capturesKeys: accumulatorCapturesKeys, leftOpensNavigation: true,
	},
	modeProcessLogs: {
		name: "Logs", navTarget: navigationProcessLogs, scope: scopeProcess, upParent: modeCount, render: Model.renderProcessLogs, key: stringScreenKey((*Model).handleProcessLogKey),
		click: (*Model).handleProcessLogMouseClick, wheel: wheelByDelta((*Model).moveProcessLogSelection),
		refresh: refreshProcessLogs, autoRefresh: manualRefreshOnly, resize: noScreenCommand,
		header: infrastructureScreenHeader, footer: processLogsScreenFooter, errors: processLogsScreenErrors,
		backFallback: modeTaskManagerDetail,
		capturesKeys: neverCapturesKeys, leftOpensNavigation: true,
	},
	modeDocument: {
		name: "Document", navTarget: navigationDocument, scope: scopeDynamic, upParent: modeCount, title: documentScreenTitle, render: Model.renderDocument, key: stringScreenKey((*Model).handleDocumentKey),
		click: noScreenMouseCommand, wheel: wheelByDelta((*Model).moveDocumentVertical),
		refresh: refreshDocument, autoRefresh: manualRefreshOnly, resize: noScreenCommand,
		header: documentScreenHeader, footer: documentScreenFooter, errors: documentScreenErrors,
		backFallback: modeProcessLogs,
		capturesKeys: documentCapturesKeys, leftOpensNavigation: true,
	},
	modeThreadDump: {
		name: "Thread Dump", navTarget: navigationThreadDump, scope: scopeProcess, upParent: modeCount, render: Model.renderThreadDump, key: stringScreenKey((*Model).handleThreadDumpKey),
		click: (*Model).handleThreadMouseClick, wheel: wheelByDelta((*Model).moveThreadSelection),
		refresh: refreshThreadDump, autoRefresh: manualRefreshOnly, resize: noScreenCommand,
		header: infrastructureScreenHeader, footer: threadDumpScreenFooter, errors: threadDumpScreenErrors,
		backFallback: modeTaskManagerDetail,
		capturesKeys: threadCapturesKeys, leftOpensNavigation: true,
	},
	modeProfiler: {
		name: "Process Profiler", navTarget: navigationProfiler, scope: scopeProcess, upParent: modeCount, render: Model.renderProfiler, key: stringScreenKey((*Model).handleProfilerKey),
		click: (*Model).handleProfilerMouseClick, wheel: wheelByDelta((*Model).moveProfilerSelection),
		refresh: refreshProfiler, autoRefresh: tickProfiler, resize: noScreenCommand,
		header: infrastructureScreenHeader, footer: profilerScreenFooter, errors: profilerScreenErrors,
		backFallback: modeTaskManagerDetail,
		capturesKeys: neverCapturesKeys, leftOpensNavigation: true,
	},
	modeProfilerFlameGraph: {
		name: "Profile Flame Graph", navTarget: navigationProfilerFlameGraph, scope: scopeProcess, upParent: modeCount, render: Model.renderFlameGraph, key: stringScreenKey((*Model).handleFlameGraphKey),
		click: (*Model).handleFlameGraphMouseClick, wheel: wheelByDelta((*Model).moveFlameGraphLinear),
		refresh: immutableContent, autoRefresh: immutableContent, resize: noScreenCommand,
		header: infrastructureScreenHeader, footer: profilerFlameGraphScreenFooter, errors: profilerFlameGraphScreenErrors,
		backFallback: modeProfiler,
		capturesKeys: neverCapturesKeys,
	},
	modeSQL: {
		name: "SQL Workbench", navTarget: navigationSQL, scope: scopeCluster, upParent: modeJobs, render: renderSQLScreen, key: sqlScreenKey,
		click: sqlScreenClick, wheel: sqlScreenWheel,
		refresh: refreshSQL, autoRefresh: pollDrivenRefresh, resize: noScreenCommand,
		header: sqlScreenHeader, footer: sqlScreenFooter, errors: sqlScreenErrors,
		backFallback: modeJobs,
		capturesKeys: sqlCapturesKeys,
	},
}

func (m Model) activeScreen() screenSpec {
	if m.mode < modeCount {
		screen := screenRegistry[m.mode]
		if screen.name != "" {
			return screen
		}
	}
	return screenRegistry[modeGraph]
}

func screenRegistryMode(value int) screenMode {
	if value < 0 || value >= int(modeCount) {
		return modeGraph
	}
	return screenMode(value) //nolint:gosec // bounded against the complete registry above
}

func (screen screenSpec) displayTitle(m Model) string {
	if screen.title != nil {
		return screen.title(m)
	}
	return screen.name
}

func stringScreenKey(handler func(*Model, string) tea.Cmd) func(*Model, tea.KeyPressMsg) tea.Cmd {
	return func(m *Model, message tea.KeyPressMsg) tea.Cmd { return handler(m, message.String()) }
}

func mouseWithoutCommand(handler func(*Model, tea.Mouse)) func(*Model, tea.Mouse) tea.Cmd {
	return func(m *Model, event tea.Mouse) tea.Cmd {
		handler(m, event)
		return nil
	}
}

func wheelByDelta(handler func(*Model, int)) func(*Model, tea.Mouse) tea.Cmd {
	return func(m *Model, event tea.Mouse) tea.Cmd {
		handler(m, wheelDelta(event))
		return nil
	}
}

func noScreenCommand(*Model) tea.Cmd                 { return nil }
func noScreenMouseCommand(*Model, tea.Mouse) tea.Cmd { return nil }

// manualRefreshOnly keeps potentially large logs, documents, and thread dumps
// stable until the operator explicitly requests another fetch.
func manualRefreshOnly(m *Model) tea.Cmd { return noScreenCommand(m) }

// immutableContent keeps a captured profiler report fixed after it is loaded.
func immutableContent(m *Model) tea.Cmd { return noScreenCommand(m) }

// pollDrivenRefresh leaves SQL result polling to the server-provided
// nextResultUri rather than the application's regular refresh interval.
func pollDrivenRefresh(m *Model) tea.Cmd { return noScreenCommand(m) }

func neverCapturesKeys(Model) bool            { return false }
func sqlCapturesKeys(m Model) bool            { return m.sql.CapturesKeys() }
func jobListCapturesKeys(m Model) bool        { return m.jobList.CapturesKeys() }
func infrastructureCapturesKeys(m Model) bool { return m.infrastructure.CapturesKeys() }
func subtaskCapturesKeys(m Model) bool        { return m.jobDetails.CapturesKeys() }
func actionCapturesKeys(m Model) bool         { return m.jobOperations.CapturesKeys() }
func checkpointCapturesKeys(m Model) bool     { return m.checkpoints.CapturesKeys() }
func accumulatorCapturesKeys(m Model) bool    { return m.accumulators.CapturesKeys() }
func documentCapturesKeys(m Model) bool       { return m.processDiagnostics.State().DocumentSearch }
func metricCapturesKeys(m Model) bool         { return m.metrics.CapturesKeys() }
func threadCapturesKeys(m Model) bool         { return m.processDiagnostics.State().ThreadSearch }
func exceptionCapturesKeys(m Model) bool      { return m.exceptions.CapturesKeys() }

func metricScreenWheel(m *Model, event tea.Mouse) tea.Cmd {
	m.moveMetricSelection(wheelDelta(event))
	return m.refreshMetricWindow()
}

func refreshGraph(m *Model) tea.Cmd {
	m.loading = len(m.snapshot.Nodes) == 0
	return m.fetchSnapshot()
}

func refreshSnapshot(m *Model) tea.Cmd { return m.fetchSnapshot() }

func refreshJobs(m *Model) tea.Cmd {
	m.syncJobListContext()
	return m.jobList.Refresh()
}

func tickJobs(m *Model) tea.Cmd { return m.startJobsRefresh() }

func refreshSubtasks(m *Model) tea.Cmd {
	m.syncJobDetailContext()
	m.jobDetails.Activate(jobdetailmodule.ViewSubtasks)
	return tea.Batch(m.fetchSnapshot(), m.jobDetails.Refresh())
}

func tickSubtasks(m *Model) tea.Cmd {
	if m.jobDetails.State().DiagnosticsOpen == "" {
		return m.fetchSnapshot()
	}
	return tea.Batch(m.fetchSnapshot(), m.fetchDiagnostics())
}

func refreshFlameGraph(m *Model) tea.Cmd {
	m.syncFlameGraphContext()
	m.flameGraphs.Activate(flamegraphmodule.ViewVertex)
	return tea.Batch(m.fetchSnapshot(), m.flameGraphs.Refresh())
}

func tickFlameGraph(m *Model) tea.Cmd {
	return tea.Batch(m.fetchSnapshot(), m.fetchFlameGraph())
}

func refreshCheckpointOperators(m *Model) tea.Cmd {
	return tea.Batch(m.fetchSnapshot(), m.fetchCheckpointDetail())
}

func refreshCheckpointSubtasks(m *Model) tea.Cmd {
	return tea.Batch(m.fetchSnapshot(), m.fetchCheckpointDetail(), m.fetchCheckpointSubtasks())
}

func refreshJobConfiguration(m *Model) tea.Cmd {
	return tea.Batch(m.fetchSnapshot(), m.refreshJobOperations(jobopsmodule.ViewConfiguration))
}

// tickJobConfiguration refreshes the live job header without repeatedly
// fetching the immutable execution configuration shown in the body.
func tickJobConfiguration(m *Model) tea.Cmd { return m.fetchSnapshot() }

func refreshActions(m *Model) tea.Cmd {
	return tea.Batch(m.fetchSnapshot(), m.fetchActionOperationIfNeeded())
}

func tickActions(m *Model) tea.Cmd {
	return m.fetchSnapshot()
}

func refreshInfrastructure(m *Model) tea.Cmd {
	return m.infrastructure.Refresh()
}

func tickInfrastructure(m *Model) tea.Cmd { return m.infrastructure.Poll() }

func refreshMetrics(m *Model) tea.Cmd {
	m.syncMetricContext()
	return tea.Batch(m.fetchSnapshot(), m.metrics.Refresh())
}

func tickMetrics(m *Model) tea.Cmd {
	return tea.Batch(m.fetchSnapshot(), m.fetchCustomMetricValues())
}

func resizeMetrics(m *Model) tea.Cmd { return m.refreshMetricWindow() }

func refreshAccumulators(m *Model) tea.Cmd {
	m.syncAccumulatorContext()
	return tea.Batch(m.fetchSnapshot(), m.accumulators.Refresh())
}

func tickAccumulators(m *Model) tea.Cmd {
	return tea.Batch(m.fetchSnapshot(), m.fetchAccumulators())
}

func refreshProcessLogs(m *Model) tea.Cmd {
	return m.refreshProcessDiagnostics(processmodule.ViewLogs)
}

func refreshDocument(m *Model) tea.Cmd {
	return m.refreshProcessDiagnostics(processmodule.ViewDocument)
}

func refreshThreadDump(m *Model) tea.Cmd {
	return m.refreshProcessDiagnostics(processmodule.ViewThreadDump)
}

func refreshProfiler(m *Model) tea.Cmd {
	return m.profiler.Refresh()
}

func tickProfiler(m *Model) tea.Cmd { return m.fetchProfilerList() }

func jobScreenHeader(m Model, width int, title string) string { return m.renderJobHeader(title, width) }
func homeScreenHeader(m Model, width int, _ string) string    { return m.renderHomeHeader(width) }
func infrastructureScreenHeader(m Model, width int, title string) string {
	return m.renderInfrastructureHeader(title, width)
}
func documentScreenHeader(m Model, width int, title string) string {
	if documentUsesInfrastructure(m) {
		return m.renderInfrastructureHeader(title, width)
	}
	return m.renderJobHeader(title, width)
}

func documentScreenTitle(m Model) string {
	return shared.Fallback(m.processDiagnostics.State().DocumentTitle, "Document")
}

func documentUsesInfrastructure(m Model) bool {
	switch m.processDiagnostics.State().DocumentSource {
	case processmodule.SourceLog, processmodule.SourceCurrentLog, processmodule.SourceStdout:
		return true
	default:
		return false
	}
}

func graphScreenFooter(m Model, mouse, back string) string {
	flame := "F vertex flame"
	if node, ok := m.selectedNode(); ok {
		if m.width < 100 {
			flame = "F flame:" + shared.Truncate(node.Name, 16)
		} else {
			flame = "F flame for " + shared.Truncate(node.Name, 24)
		}
	}
	selection := "arrows vertex"
	if m.width >= 100 {
		selection = "arrows/hjkl vertex"
	}
	if m.width < 100 {
		return fmt.Sprintf("%s  f fit  enter st  :st/fl/mx/acc go  </> job  %s", flame, backHint("q/esc", back))
	}
	return fmt.Sprintf(":st/fl/mx/acc go  ctrl+n focus  %s  </> job  %s  enter subtasks  %s  f fit  +/- %s  z map  m mouse:%s",
		backHint("q/esc", back), selection, flame, m.graphZoomLabel(), mouse)
}

func jobsScreenFooter(m Model, mouse, _ string) string {
	if m.jobList.State().SearchOpen {
		return "filter jobs  type query  enter apply  esc cancel  ctrl+w clear"
	}
	// Overview is the root, so back stays here. "g" is the honest way out.
	forward := ""
	if m.snapshot.JobID != "" {
		forward = "  g graph"
	}
	return fmt.Sprintf("left nav  / filter  tab view:%s  up/down select  enter open%s  r reload  m mouse:%s", strings.ToLower(m.jobList.ViewLabel()), forward, mouse)
}

func subtasksScreenFooter(m Model, mouse, back string) string {
	state := m.jobDetails.State()
	if state.SubtaskSearch {
		return "filter subtasks  type query  enter apply  esc keep filter  ctrl+w clear  ctrl+n focus"
	}
	if state.SubtaskSortOpen {
		return "sort columns  #/r/b/p/i/o/I/O/w/t choose  left/right column  a asc  d desc  enter/esc apply"
	}
	view := "rates"
	if state.SubtaskTotals {
		view = "totals"
	}
	return fmt.Sprintf("left nav  </> vertex  up/down select  d thread dump  s sort  v rates/totals:%s  / filter  enter drill  %s  r refresh  m mouse:%s", view, backHint("q/esc", back), mouse)
}

func flameGraphScreenFooter(m Model, mouse, back string) string {
	back = backHint("q/esc", back)
	if m.flameGraphConsumesBack() {
		back = "q/esc/backspace out"
	}
	return fmt.Sprintf("ctrl+n nav  </> vertex  arrows frames  enter zoom  %s  [/] type  s/S subtask  home reset  r sample  m mouse:%s", back, mouse)
}

func checkpointsScreenFooter(m Model, mouse, back string) string {
	if m.checkpoints.State().HistorySearch {
		return "filter retained checkpoints  type query  enter open  esc keep filter  ctrl+w clear  ctrl+n focus"
	}
	view := "history"
	if m.checkpoints.State().Page == checkpointmodule.PageStatistics {
		view = "summary"
	}
	return fmt.Sprintf("left nav  </> job  tab/v history/summary:%s  up/down select  / filter  enter details  home latest  %s  r refresh  m mouse:%s", view, backHint("q/esc", back), mouse)
}

func checkpointOperatorsScreenFooter(m Model, mouse, back string) string {
	if m.checkpoints.State().OperatorSearch {
		return "filter checkpoint operators  type query  enter open  esc keep filter  ctrl+w clear  ctrl+n focus"
	}
	return fmt.Sprintf("left nav  </> checkpoint  up/down select  / filter  enter subtasks  s sort  i config  g graph  %s  r refresh  m mouse:%s", backHint("q/esc", back), mouse)
}

func checkpointSubtasksScreenFooter(_ Model, mouse, back string) string {
	return fmt.Sprintf("left nav  </> checkpoint  up/down select  s sort  g graph  %s  r refresh  m mouse:%s", backHint("q/esc", back), mouse)
}

func diagnosticsScreenFooter(m Model, mouse, back string) string {
	if m.jobDetails.State().DiagnosticSearch {
		return "filter diagnostics  type query  enter open  esc keep filter  ctrl+w clear  ctrl+n focus"
	}
	return fmt.Sprintf("left nav  </> job  %s  g graph  up/down select  / filter  [ ] page  b bp  enter subtasks  :mx metrics  r refresh  m mouse:%s", backHint("q/esc", back), mouse)
}

func timelineScreenFooter(m Model, mouse, back string) string {
	if m.jobDetails.State().TimelineSearch {
		return "filter timeline  type query  enter open  esc keep filter  ctrl+w clear  ctrl+n focus"
	}
	return fmt.Sprintf("left nav  </> job  %s  g graph  up/down select  / filter  enter subtasks  r refresh  m mouse:%s", backHint("q/esc", back), mouse)
}

func exceptionsScreenFooter(m Model, mouse, back string) string {
	if m.width >= 118 {
		return fmt.Sprintf("</> job  %s  enter vertex  T TM  d threads  l log tail  L more  ctrl+u/d trace", backHint("q/esc", back))
	}
	return fmt.Sprintf("</> job  %s  left nav  up/down incidents  enter vertex  T TM  d threads  l log tail  L more  ctrl+u/d trace  / filter  m:%s", backHint("q/esc", back), mouse)
}

func jobConfigScreenFooter(m Model, mouse, back string) string {
	if m.jobOperations.State().ConfigurationSearch {
		return "filter configuration  type query  enter apply  esc keep filter  ctrl+w clear  ctrl+n focus"
	}
	return fmt.Sprintf("left nav  </> job  %s  g graph  up/down select  / filter  home/end  r refresh  m mouse:%s", backHint("q/esc", back), mouse)
}

func actionsScreenFooter(m Model, mouse, back string) string {
	confirm, _ := m.jobOperations.ActionState()
	if confirm {
		return "CONFIRMATION OWNS INPUT  y execute  n/q/esc cancel  ctrl+c quit"
	}
	return fmt.Sprintf("left nav  </> job  %s  g graph  up/down select  enter review  r refresh  m mouse:%s", backHint("q/esc", back), mouse)
}

func taskManagersScreenFooter(m Model, mouse, back string) string {
	m.infrastructure.Activate(inframodule.ViewTaskManagers)
	return m.infrastructure.Footer(mouse, back)
}

func taskManagerDetailScreenFooter(m Model, mouse, back string) string {
	m.infrastructure.Activate(inframodule.ViewTaskManagerDetail)
	return m.infrastructure.Footer(mouse, back)
}

func jobManagerScreenFooter(m Model, mouse, back string) string {
	m.infrastructure.Activate(inframodule.ViewJobManager)
	return m.infrastructure.Footer(mouse, back)
}

func metricsScreenFooter(_ Model, mouse, back string) string {
	return fmt.Sprintf("left nav  </> vertex  %s  g graph  up/down metrics  enter track  / filter  s scope  a aggregate  [ ] history  x clear  r refresh  m mouse:%s", backHint("q/esc", back), mouse)
}

func accumulatorsScreenFooter(m Model, mouse, back string) string {
	if m.accumulators.State().SearchOpen {
		return "filter accumulators  type query  enter open  esc keep filter  ctrl+w clear  ctrl+n focus"
	}
	return fmt.Sprintf("left nav  </> vertex  %s  g graph  up/down select  / filter  enter full value  r refresh  m mouse:%s", backHint("q/esc", back), mouse)
}

func processLogsScreenFooter(_ Model, mouse, back string) string {
	return fmt.Sprintf("left nav  </> process  %s  up/down logs  enter/click content  p profiler  x stdout  d threads  r refresh  m mouse:%s", backHint("q/esc", back), mouse)
}

func documentScreenFooter(m Model, mouse, back string) string {
	if m.processDiagnostics.State().DocumentSearch {
		return "type search  enter find  esc cancel"
	}
	peer := ""
	if documentUsesInfrastructure(m) {
		peer = "</> process  "
	}
	return fmt.Sprintf("left nav  %s%s  up/down/pg scroll  h/l horizontal  / search  n/N next/prev  r reload  m mouse:%s", peer, backHint("q/esc", back), mouse)
}

func threadDumpScreenFooter(m Model, mouse, back string) string {
	if m.processDiagnostics.State().ThreadSearch {
		return "type thread filter  enter apply  esc cancel"
	}
	return fmt.Sprintf("left nav  </> process  %s  up/down threads  ctrl+u/d stack  enter full stack  / filter  p profiler  r refresh  m mouse:%s", backHint("q/esc", back), mouse)
}

func profilerScreenFooter(_ Model, mouse, back string) string {
	return fmt.Sprintf("left nav  </> process  %s  up/down runs  [/] mode  -/+ duration  p start  enter view process flame  l/x/d log tail/stdout/threads  r refresh  m mouse:%s", backHint("q/esc", back), mouse)
}

func profilerFlameGraphScreenFooter(m Model, _, back string) string {
	back = backHint("q/esc", back)
	if m.flameGraphConsumesBack() {
		back = "q/esc/backspace out"
	}
	return "ctrl+n nav  </> process  arrows frames  enter zoom  " + back + "  home reset"
}

func appendScreenError(errors []displayedError, label string, err error) []displayedError {
	if err == nil {
		return errors
	}
	return append(errors, displayedError{label: label, err: err})
}

func jobScreenErrors(m Model) []displayedError {
	errors := appendScreenError(nil, "Job snapshot", m.err)
	for _, issue := range m.snapshot.Issues {
		kind := string(issue.Kind)
		label := "Snapshot data"
		if kind != "" {
			label = strings.ToUpper(kind[:1]) + kind[1:]
		}
		if issue.VertexID != "" {
			label += " / " + shared.ShortID(issue.VertexID)
		}
		errors = appendScreenError(errors, label, issue.Err)
	}
	return errors
}

func homeScreenErrors(m Model) []displayedError {
	state := m.jobList.State()
	errors := appendScreenError(nil, "Cluster overview", state.ClusterErr)
	return appendScreenError(errors, "Job list", state.JobsErr)
}

func infrastructureScreenErrors(m Model) []displayedError {
	return appendScreenError(nil, "Infrastructure", m.infrastructure.Error())
}

func subtaskScreenErrors(m Model) []displayedError {
	return appendScreenError(jobScreenErrors(m), "Subtasks", m.jobDetails.Error())
}

func flameGraphScreenErrors(m Model) []displayedError {
	m.flameGraphs.Activate(flamegraphmodule.ViewVertex)
	return appendScreenError(jobScreenErrors(m), "Flame graph", m.flameGraphs.Error())
}

func checkpointOperatorScreenErrors(m Model) []displayedError {
	state := m.checkpoints.State()
	errors := appendScreenError(jobScreenErrors(m), "Checkpoint details", state.DetailErr)
	return appendScreenError(errors, "Checkpoint configuration", state.ConfigErr)
}

func checkpointSubtaskScreenErrors(m Model) []displayedError {
	return appendScreenError(jobScreenErrors(m), "Checkpoint subtasks", m.checkpoints.State().SubtasksErr)
}

func jobConfigScreenErrors(m Model) []displayedError {
	state := m.jobOperations.State()
	return appendScreenError(jobScreenErrors(m), "Job configuration", state.ConfigurationErr)
}

func metricScreenErrors(m Model) []displayedError {
	return appendScreenError(jobScreenErrors(m), "Metrics", m.metrics.Error())
}

func accumulatorScreenErrors(m Model) []displayedError {
	return appendScreenError(jobScreenErrors(m), "Accumulators", m.accumulators.Error())
}

func actionScreenErrors(m Model) []displayedError {
	return appendScreenError(jobScreenErrors(m), "Job action", m.jobOperations.State().ActionErr)
}

func processLogsScreenErrors(m Model) []displayedError {
	return appendScreenError(infrastructureScreenErrors(m), "Process logs", m.processDiagnostics.State().LogsErr)
}

// Document failures are intentionally visible. The legacy mode switch omitted
// modeDocument, which hid failed log and stdout loads from the error details UI.
func documentScreenErrors(m Model) []displayedError {
	errors := jobScreenErrors(m)
	if documentUsesInfrastructure(m) {
		errors = infrastructureScreenErrors(m)
	}
	return appendScreenError(errors, "Document", m.processDiagnostics.State().DocumentErr)
}

func threadDumpScreenErrors(m Model) []displayedError {
	return appendScreenError(infrastructureScreenErrors(m), "Thread dump", m.processDiagnostics.State().ThreadErr)
}

func profilerScreenErrors(m Model) []displayedError {
	return appendScreenError(infrastructureScreenErrors(m), "Profiler", m.profiler.Error())
}

func profilerFlameGraphScreenErrors(m Model) []displayedError {
	return appendScreenError(infrastructureScreenErrors(m), "Profiler report", m.profiler.Error())
}
