package coordinator

import (
	"strings"

	tea "charm.land/bubbletea/v2"

	"github.com/nikitasavinov/flink-tui/internal/flink"
	checkpointmodule "github.com/nikitasavinov/flink-tui/internal/ui/checkpoints"
	inframodule "github.com/nikitasavinov/flink-tui/internal/ui/infrastructure"
)

// screenScope is the semantic object space a screen describes. Routing,
// sidebar context, and breadcrumbs all project from this one classification.
type screenScope uint8

const (
	scopeUnknown screenScope = iota
	scopeOverview
	scopeJob
	scopeVertex
	scopeCluster
	scopeProcess
	scopeDynamic
)

func (m Model) activeScope() screenScope {
	scope := m.activeScreen().scope
	if scope != scopeDynamic {
		return scope
	}
	if m.mode == modeDocument && documentUsesInfrastructure(m) {
		return scopeProcess
	}
	if m.mode == modeDocument {
		return scopeVertex
	}
	return scopeUnknown
}

func (m Model) scopeUpAvailable() bool {
	if m.mode == modeJobs || m.scopeUpReservedByScreen() {
		return false
	}
	return m.activeScreen().upParent < modeCount || m.activeScope() == scopeProcess || m.mode == modeDocument
}

func (m Model) scopeUpReservedByScreen() bool {
	return m.mode == modeFlameGraph || m.mode == modeProfilerFlameGraph
}

// scopeUp abandons the current drill branch and moves to its semantic parent.
// Clearing route history is intentional: a later q must not resurrect the
// object scope the operator explicitly left with Backspace.
func (m *Model) scopeUp() tea.Cmd {
	if m.scopeUpReservedByScreen() {
		return m.activeScreen().key(m, tea.KeyPressMsg{Code: tea.KeyBackspace})
	}
	if m.mode == modeJobs {
		m.setNotice("Overview is the top-level scope; Ctrl+C quits Flink TUI.")
		return nil
	}

	if m.activeScope() == scopeProcess {
		process := m.activeProcess()
		m.clearRouteHistory()
		m.navigation.DestinationOpened(max(40, m.width))
		if process.Kind == flink.ProcessTaskManager && strings.TrimSpace(process.TaskManagerID) != "" {
			return tea.Batch(m.openTaskManagerByID(process.TaskManagerID, modeTaskManagers), tea.ClearScreen)
		}
		return tea.Batch(m.openJobManager(), tea.ClearScreen)
	}

	parent := m.activeScreen().upParent
	if m.mode == modeDocument {
		state := m.processDiagnostics.State()
		parent = validBackMode(state.DocumentBackToken, modeGraph)
	}
	if parent >= modeCount {
		m.setNotice("No parent scope is available from this screen.")
		return nil
	}

	m.clearRouteHistory()
	m.navigation.DestinationOpened(max(40, m.width))
	var command tea.Cmd
	switch parent {
	case modeJobs:
		// Set the root first so Overview does not inherit a continuation back to
		// the screen whose scope was explicitly abandoned.
		m.mode = modeJobs
		command = m.openJobPicker()
	case modeGraph:
		m.mode = modeGraph
		m.centerSelection()
	case modeCheckpoints:
		m.checkpoints.Activate(checkpointmodule.ViewHistory)
		m.mode = modeCheckpoints
	case modeCheckpointOperators:
		m.checkpoints.Activate(checkpointmodule.ViewOperators)
		m.mode = modeCheckpointOperators
	case modeTaskManagers:
		m.infrastructure.Activate(inframodule.ViewTaskManagers)
		m.mode = modeTaskManagers
	default:
		m.restoreRouteCrumb(routeCrumb{mode: parent, selected: m.selected})
	}
	return tea.Batch(command, tea.ClearScreen)
}
