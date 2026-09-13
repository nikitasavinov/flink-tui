package coordinator

import (
	tea "charm.land/bubbletea/v2"

	checkpointmodule "github.com/nikitasavinov/flink-tui/internal/ui/checkpoints"
	flamegraphmodule "github.com/nikitasavinov/flink-tui/internal/ui/flamegraph"
	inframodule "github.com/nikitasavinov/flink-tui/internal/ui/infrastructure"
	jobdetailmodule "github.com/nikitasavinov/flink-tui/internal/ui/jobdetail"
	jobopsmodule "github.com/nikitasavinov/flink-tui/internal/ui/jobops"
	processmodule "github.com/nikitasavinov/flink-tui/internal/ui/processdiag"
	"github.com/nikitasavinov/flink-tui/internal/ui/shared"
	shellmodule "github.com/nikitasavinov/flink-tui/internal/ui/shell"
)

const maxRouteHistory = 16

// routeCrumb captures the shell-owned context that may change while drilling
// into another screen. Feature modules retain their own cursors and filters.
type routeCrumb struct {
	mode     screenMode
	selected string
	// jump records that the operator left this screen by naming a destination
	// rather than drilling into it. Back treats both alike, but a jump breaks
	// context inheritance: Task Managers reached from a job graph is still
	// cluster-scoped and must not wear that job's name.
	jump bool
}

// routeHistory is deliberately bounded: it is operator navigation state, not
// an audit log, and a fixed array avoids sharing a mutable slice across Bubble
// Tea's value-model copies.
type routeHistory struct {
	entries [maxRouteHistory]routeCrumb
	length  int
}

// parkedRoute is the single investigation suspended by an explicit Home
// glance. The leaf is stored separately because routeHistory contains only
// the screens behind the current one.
type parkedRoute struct {
	history routeHistory
	leaf    routeCrumb
	active  bool
}

// shellState groups cross-screen interaction state while preserving promoted
// access at the coordinator composition root.
type shellState struct {
	navigation            shellmodule.Navigation
	history               routeHistory
	parked                parkedRoute
	jobPickerContinuation navigationTarget
	palette               shellmodule.Palette
	help                  shellmodule.Help
}

func (history *routeHistory) Push(crumb routeCrumb) {
	if history.length > 0 && history.entries[history.length-1] == crumb {
		return
	}
	if history.length == len(history.entries) {
		copy(history.entries[:], history.entries[1:])
		history.length--
	}
	history.entries[history.length] = crumb
	history.length++
}

func (history *routeHistory) Pop() (routeCrumb, bool) {
	if history.length == 0 {
		return routeCrumb{}, false
	}
	history.length--
	crumb := history.entries[history.length]
	history.entries[history.length] = routeCrumb{}
	return crumb, true
}

func (history *routeHistory) Clear() {
	*history = routeHistory{}
}

func (history routeHistory) Len() int { return history.length }

func (history routeHistory) Peek() (routeCrumb, bool) {
	if history.length == 0 {
		return routeCrumb{}, false
	}
	return history.entries[history.length-1], true
}

func (history routeHistory) Crumbs() []routeCrumb {
	result := make([]routeCrumb, history.length)
	copy(result, history.entries[:history.length])
	return result
}

func (m Model) routeCrumb() routeCrumb {
	return routeCrumb{mode: m.mode, selected: m.selected}
}

func (m *Model) pushDrill(origin routeCrumb) {
	if origin.mode == m.mode {
		return
	}
	m.history.Push(origin)
}

func (m *Model) finishDrill(origin routeCrumb, command tea.Cmd) tea.Cmd {
	m.pushDrill(origin)
	return command
}

// clearRouteHistory abandons both the current and suspended investigations.
// Genuine context changes (notably selecting another job) use this path.
func (m *Model) clearRouteHistory() {
	m.history.Clear()
	m.parked = parkedRoute{}
}

func (m *Model) parkRouteHistory(leaf routeCrumb) {
	if leaf.mode == modeJobs {
		return
	}
	m.parked = parkedRoute{history: m.history, leaf: leaf, active: true}
	m.history.Clear()
}

func (m *Model) resumeParkedRoute() tea.Cmd {
	if !m.parked.active {
		return nil
	}
	parked := m.parked
	m.parked = parkedRoute{}
	m.history = parked.history
	m.restoreRouteCrumb(parked.leaf)
	m.navigation.DestinationOpened(max(40, m.width))
	return tea.ClearScreen
}

func (m Model) resumeDestinationLabel() string {
	if !m.parked.active {
		return ""
	}
	return screenModeName(m.parked.leaf.mode)
}

// backDestinationLabel names the screen q/esc will actually return to. Route
// history is authoritative; module-owned fallback tokens only matter when the
// operator did not arrive through a recorded shell route.
func (m Model) backDestinationLabel(fallback screenMode) string {
	if crumb, ok := m.history.Peek(); ok {
		return screenModeName(crumb.mode)
	}
	mode := fallback
	switch m.mode {
	case modeTaskManagerDetail:
		mode = validBackMode(m.infrastructure.State().BackToken, mode)
	case modeProcessLogs:
		mode = validBackMode(m.processDiagnostics.State().LogBackToken, mode)
	case modeDocument:
		mode = validBackMode(m.processDiagnostics.State().DocumentBackToken, mode)
	case modeThreadDump:
		mode = validBackMode(m.processDiagnostics.State().ThreadBackToken, mode)
	case modeProfiler:
		mode = validBackMode(m.profiler.State().BackToken, mode)
	}
	return screenModeName(mode)
}

func validBackMode(value int, fallback screenMode) screenMode {
	if value < 0 || value >= int(modeCount) {
		return fallback
	}
	return screenRegistryMode(value)
}

func backHint(keys, destination string) string {
	return keys + " " + shared.Truncate(destination, 22)
}

func (m *Model) popRouteHistory() bool {
	crumb, ok := m.history.Pop()
	if !ok {
		return false
	}
	m.restoreRouteCrumb(crumb)
	return true
}

func (m *Model) restoreRouteCrumb(crumb routeCrumb) {
	m.mode = crumb.mode
	m.restoreFeatureView(crumb.mode)
	if crumb.selected != "" {
		if _, exists := m.layout.Rects[crumb.selected]; exists {
			m.selected = crumb.selected
		}
	}
	if m.mode == modeGraph {
		m.centerSelection()
	}
}

// restoreFeatureView keeps a feature module's internal view aligned with the
// shell mode restored from history. Without this reconciliation, a delayed
// poll can project the feature's previous view back onto the shell and swallow
// a route pop.
func (m *Model) restoreFeatureView(mode screenMode) {
	switch mode {
	case modeSubtasks:
		m.jobDetails.Activate(jobdetailmodule.ViewSubtasks)
	case modeDiagnostics:
		m.jobDetails.Activate(jobdetailmodule.ViewDiagnostics)
	case modeTimeline:
		m.jobDetails.Activate(jobdetailmodule.ViewTimeline)
	case modeFlameGraph:
		m.flameGraphs.Activate(flamegraphmodule.ViewVertex)
	case modeCheckpoints:
		m.checkpoints.Activate(checkpointmodule.ViewHistory)
	case modeCheckpointOperators:
		m.checkpoints.Activate(checkpointmodule.ViewOperators)
	case modeCheckpointSubtasks:
		m.checkpoints.Activate(checkpointmodule.ViewSubtasks)
	case modeJobConfig:
		m.jobOperations.Activate(jobopsmodule.ViewConfiguration)
	case modeActions:
		m.jobOperations.Activate(jobopsmodule.ViewActions)
	case modeTaskManagers:
		m.infrastructure.Activate(inframodule.ViewTaskManagers)
	case modeTaskManagerDetail:
		m.infrastructure.Activate(inframodule.ViewTaskManagerDetail)
	case modeJobManager:
		m.infrastructure.Activate(inframodule.ViewJobManager)
	case modeProcessLogs:
		m.processDiagnostics.Activate(processmodule.ViewLogs)
	case modeDocument:
		m.processDiagnostics.Activate(processmodule.ViewDocument)
	case modeThreadDump:
		m.processDiagnostics.Activate(processmodule.ViewThreadDump)
	case modeProfilerFlameGraph:
		m.flameGraphs.Activate(flamegraphmodule.ViewProfilerReport)
	}
}

// handleScreenBack lets a screen consume an internal escape first (closing a
// search, zooming out a flame graph, and so on). If it actually leaves the
// screen, a recorded drill origin replaces that feature's fixed fallback.
func (m *Model) handleScreenBack() tea.Cmd {
	// A peer job can still be loading while Back restores a history crumb.
	// Its reply may populate the cache, but must not reopen the abandoned route.
	m.navigation.ClearDeferredTarget()
	if m.history.Len() > 0 && !m.flameGraphConsumesBack() {
		m.popRouteHistory()
		return tea.ClearScreen
	}
	// Overview is the root. Its feature fallback is the job graph, so without
	// this guard "esc" navigates forward from the root and the two screens
	// ping-pong forever.
	if m.mode == modeJobs {
		m.setNotice("Overview is the root screen; Ctrl+C quits Flink TUI.")
		return nil
	}
	origin := m.mode
	command := m.activeScreen().key(m, tea.KeyPressMsg(tea.Key{Code: tea.KeyEscape}))
	if m.mode == origin {
		return command
	}
	if m.popRouteHistory() {
		return tea.Batch(command, tea.ClearScreen)
	}
	return command
}

func (m Model) flameGraphConsumesBack() bool {
	if m.mode != modeFlameGraph && m.mode != modeProfilerFlameGraph {
		return false
	}
	state := m.flameGraphs.State()
	focus := state.LiveFocus
	if m.mode == modeProfilerFlameGraph {
		focus = state.ReportFocus
	}
	return focus != "" && focus != "0"
}
