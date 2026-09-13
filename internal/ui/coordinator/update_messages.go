package coordinator

import (
	"time"

	tea "charm.land/bubbletea/v2"

	jobgraphmodule "github.com/nikitasavinov/flink-tui/internal/ui/jobgraph"
	"github.com/nikitasavinov/flink-tui/internal/ui/snapshotstate"
)

func (m *Model) applyWindowSize(message tea.WindowSizeMsg) tea.Cmd {
	m.width = message.Width
	m.height = message.Height
	m.navigation.Resize(max(40, m.width))
	autoFitted := m.autoFitGraphIfReady()
	if !autoFitted && m.mode == modeGraph {
		// Later resizes retain the selected semantic zoom and only recenter
		// the selection.
		m.centerSelection()
	}
	return m.activeScreen().resize(m)
}

func (m *Model) applySnapshot(message snapshotMsg) tea.Cmd {
	if message.generation != m.generation {
		return nil
	}
	m.requests.snapshotPending = false
	m.loading = false
	completedLimit := message.exceptionLimit
	if completedLimit <= 0 {
		completedLimit = m.exceptions.Limit()
	}
	m.exceptions.RefreshFinished(completedLimit)
	var command tea.Cmd
	if completedLimit < m.exceptions.Limit() {
		// Loading more history can change the request while an older snapshot
		// is in flight. Fulfill that intent even when auto-refresh is paused.
		command = m.fetchSnapshot()
	}
	m.err = message.err
	if message.err != nil {
		return command
	}

	needsCenter := len(m.snapshot.Nodes) == 0
	if m.snapshot.JobID != message.snapshot.JobID {
		m.resetJobScopedState()
		m.selected = ""
		needsCenter = true
	}

	nextSnapshot := snapshotstate.Merge(m.snapshot, message.snapshot)
	hasChanges := m.graphTelemetry.Record(m.snapshot, nextSnapshot)
	m.snapshot = nextSnapshot
	m.syncExceptionContext()
	m.exceptions.SnapshotApplied(m.exceptionContext())
	m.preferredJobID = nextSnapshot.JobID
	m.layout = m.buildGraphLayout(nextSnapshot.Nodes)
	m.syncCheckpointContext()
	if _, ok := m.layout.Rects[m.selected]; !ok && len(m.layout.Order) > 0 {
		m.selected = jobgraphmodule.HottestVertexID(nextSnapshot.Nodes, m.layout.Order)
		needsCenter = true
	}
	if !m.autoFitGraphIfReady() {
		if needsCenter {
			m.centerSelection()
		} else {
			m.pan(0, 0)
		}
	}
	if hasChanges {
		m.metricPulseGeneration++
		generation := m.metricPulseGeneration
		command = tea.Batch(command, tea.Tick(metricChangePulseDuration, func(time.Time) tea.Msg {
			return metricPulseClearMsg{generation: generation}
		}))
	}
	// Deferred routes use Graph as their loading screen. A direct process or
	// object jump may have already replaced it while this snapshot was loading.
	if target := m.navigation.TakeDeferredTarget(); target != navigationNone && m.mode == modeGraph {
		command = tea.Batch(command, m.navigateWithoutHistory(target))
	}
	return command
}

func (m *Model) resetJobScopedState() {
	m.graphTelemetry.Reset()
	m.graphViewport.Reset()
	m.metricPulseGeneration++
	m.jobDetails.Reset()
	m.flameGraphs.Reset()
	m.checkpoints.Reset()
	m.exceptions.Reset()
	m.jobOperations.Reset()
	m.metrics.Reset()
	m.accumulators.Reset()
}

func (m *Model) applyMetricPulseClear(message metricPulseClearMsg) tea.Cmd {
	if message.generation == m.metricPulseGeneration {
		m.graphTelemetry.ClearChanges()
	}
	return nil
}

func (m *Model) applyTick() tea.Cmd {
	commands := []tea.Cmd{m.nextTick()}
	if operation := m.fetchActionOperationIfNeeded(); operation != nil {
		commands = append(commands, operation)
	}
	if !m.paused {
		if command := m.activeScreen().autoRefresh(m); command != nil {
			commands = append(commands, command)
		}
	}
	return tea.Batch(commands...)
}
